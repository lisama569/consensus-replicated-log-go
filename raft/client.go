package raft

import (
	"context"
	"errors"
	"time"
)

// Client 是去重客户端：为每个 ClientID 维护单调递增的 Seq。
// 网络超时/领导权变更/分区时自动在节点间重试；
// 由于服务端按 (ClientID, Seq) 去重，重试绝不会造成重复写入。
type Client struct {
	id    string
	seq   uint64
	nodes []*Node
	hint  NodeID // 最近已知的领导者
	// cursor 用于在提示失效（例如领导者被分区）时轮询其余节点。
	cursor int
	retry  time.Duration
	// scanning=true 时不再相信自称的角色，严格轮询全部节点。
	scanning bool
	// attemptTimeout 是单次尝试的上限。超时后切换节点重试；
	// 因为请求携带固定的 (ClientID, Seq)，旧节点事后即使提交也会被去重，安全。
	attemptTimeout time.Duration
}

func NewClient(id string, nodes ...*Node) *Client {
	return &Client{
		id:             id,
		nodes:          nodes,
		retry:          5 * time.Millisecond,
		attemptTimeout: 100 * time.Millisecond,
	}
}

// Set 提交一个幂等写并等待多数派提交完成。
// 返回：提交日志索引；dup=true 表示服务端识别为重复请求（结果与首次一致）。
func (c *Client) Set(ctx context.Context, key, value string) (idx Index, dup bool, err error) {
	c.seq++
	cmd := Command{Op: OpSet, Key: key, Value: value, ClientID: c.id, Seq: c.seq}
	out, err := c.run(ctx, cmd, false, "")
	if err != nil {
		return 0, false, err
	}
	return out.index, out.duplicate, nil
}

// Read 线性一致读：服务端经一轮当前任期心跳确认领导权后返回状态机值。
func (c *Client) Read(ctx context.Context, key string) (string, bool, error) {
	o, err := c.run(ctx, Command{Op: OpNoop}, true, key)
	if err != nil {
		return "", false, err
	}
	if o.readValue != nil {
		return o.readValue.value, o.readValue.found, nil
	}
	return "", false, nil
}

// run 反复尝试直到成功或 ctx 结束。
// 每次失败后：更新领导提示；若提示无变化（典型为旧领导者被分区、
// 无法立即发现更高任期），则轮询下一个节点，避免被一个失效节点粘住。
func (c *Client) run(ctx context.Context, cmd Command, isRead bool, readKey string) (clientOutcome, error) {
	for {
		node := c.pickNode()
		out, err := c.submitOnce(ctx, node, cmd, isRead, readKey)
		if err == nil {
			c.scanning = false // 找到了可用领导者
			return out, nil
		}
		if errors.Is(err, context.Canceled) {
			return clientOutcome{}, err
		}
		// ErrStopped（节点崩溃/关闭）不是致命错误：换到其它节点重试同一请求。
		if errors.Is(err, ErrStopped) {
			c.hint = ""
			if !c.scanning {
				c.beginScan()
			}
			c.advance(node)
			if !sleepOrDone(ctx, c.retry) {
				return clientOutcome{}, ctx.Err()
			}
			continue
		}

		var lh *LeaderHintError
		hint := NodeID("")
		if errors.As(err, &lh) {
			hint = lh.Leader
		}
		switch {
		case hint != "" && hint != c.cfgID(node):
			// 明确指向另一个节点：跟随该提示，退出轮询。
			c.hint = hint
			c.scanning = false
			c.cursor = 0
		default:
			// 提示指向自己（过期领导者）或无提示（超时）：进入/继续严格轮询。
			c.hint = ""
			if !c.scanning {
				c.beginScan()
			}
			c.advance(node)
		}

		if !sleepOrDone(ctx, c.retry) {
			return clientOutcome{}, ctx.Err()
		}
	}
}

func (c *Client) submitOnce(parent context.Context, node *Node, cmd Command, isRead bool, readKey string) (clientOutcome, error) {
	ctx, cancel := context.WithTimeout(parent, c.attemptTimeout)
	defer cancel()
	if isRead {
		v, found, err := node.ReadIndex(ctx, readKey)
		if err != nil {
			return clientOutcome{}, err
		}
		return clientOutcome{readValue: &struct {
			value string
			found bool
		}{v, found}}, nil
	}
	return node.SubmitProposal(ctx, cmd)
}

// pickNode 返回下一次尝试的节点。
// 仅在没有可用提示、且处于新一轮扫描起点时才读取各节点 Status；
// 一旦开始轮询（被分区的旧领导者可能长期自称 Leader），就严格按游标走完所有节点，
// 避免反复被一个过期的“领导者”吸走。
func (c *Client) pickNode() *Node {
	if c.hint != "" {
		for _, n := range c.nodes {
			if n.cfg.ID == c.hint {
				return n
			}
		}
		c.hint = ""
	}
	if c.scanning {
		return c.nodes[c.cursor%len(c.nodes)]
	}
	// 首轮：询问谁是领导者。
	for _, n := range c.nodes {
		s := n.Status()
		if s.Role == RoleLeader && s.LeaderID != "" {
			c.hint = s.LeaderID
			return n
		}
	}
	c.scanning = true
	return c.nodes[0]
}

// beginScan 在确认当前提示不可信后，进入严格轮询模式。
func (c *Client) beginScan() {
	c.scanning = true
	c.cursor = 0
}

// advance 把游标移动到刚尝试节点之后。
func (c *Client) advance(tried *Node) {
	for i, n := range c.nodes {
		if n == tried {
			c.cursor = i + 1
			return
		}
	}
	c.cursor++
}

func (c *Client) cfgID(n *Node) NodeID { return n.cfg.ID }

// SetNodes 更新客户端可用节点视图（崩溃/重启场景下使用）。
func (c *Client) SetNodes(nodes []*Node) {
	c.nodes = nodes
	if c.cursor >= len(nodes) {
		c.cursor = 0
	}
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
