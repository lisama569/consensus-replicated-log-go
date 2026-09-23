package raft

import (
	"fmt"
	"testing"
)

// cluster 是确定性测试集群：逻辑时钟驱动，无真实睡眠，结果可复现。
type cluster struct {
	t        *testing.T
	net      *Network
	ids      []int
	nodes    map[int]*Node
	storages map[int]*MemoryStorage
	crashed  map[int]bool
	ticks    int
}

func newCluster(t *testing.T, ids ...int) *cluster {
	t.Helper()
	c := &cluster{
		t:        t,
		net:      NewNetwork(),
		ids:      ids,
		nodes:    make(map[int]*Node),
		storages: make(map[int]*MemoryStorage),
		crashed:  make(map[int]bool),
	}
	for _, id := range ids {
		c.storages[id] = NewMemoryStorage()
		c.net.Attach(id, func(m Message) {
			// 闭包动态查表：崩溃节点不收消息，重启后自动指向新节点。
			if c.crashed[m.To] {
				return
			}
			if n, ok := c.nodes[m.To]; ok {
				n.Step(m)
			}
		})
		c.spawn(id)
	}
	return c
}

func (c *cluster) spawn(id int) {
	cfg := Config{
		ID:               id,
		Peers:            c.ids,
		ElectionTimeout:  10,
		HeartbeatTimeout: 3,
		Storage:          c.storages[id],
		Transport:        c.net,
	}
	c.nodes[id] = NewNode(cfg)
}

// crash 模拟节点崩溃：内存状态丢失，持久存储保留。
func (c *cluster) crash(id int) {
	c.crashed[id] = true
	delete(c.nodes, id)
	c.t.Logf("[tick=%d] *** node %d CRASHED", c.ticks, id)
}

// restart 模拟节点重启：用同一份持久存储重建节点。
func (c *cluster) restart(id int) {
	c.crashed[id] = false
	c.spawn(id)
	c.t.Logf("[tick=%d] *** node %d RESTARTED -> %s", c.ticks, id, c.nodes[id].Describe())
}

// tick 推进一轮逻辑时钟：所有存活节点 Tick，然后网络投递到期消息。
func (c *cluster) tick(n int) {
	for i := 0; i < n; i++ {
		c.ticks++
		for _, id := range c.ids {
			if c.crashed[id] {
				continue
			}
			c.nodes[id].Tick()
		}
		c.net.Tick()
	}
}

// logStates 打印所有节点角色、任期、提交位置，作为判定依据。
func (c *cluster) logStates(prefix string) {
	for _, id := range c.ids {
		if c.crashed[id] {
			c.t.Logf("[tick=%d] %s node=%d CRASHED", c.ticks, prefix, id)
			continue
		}
		c.t.Logf("[tick=%d] %s %s", c.ticks, prefix, c.nodes[id].Describe())
	}
}

// leader 返回全集群唯一领导者；存在多个或零个时失败。
func (c *cluster) leader() *Node { return c.leaderIn(c.ids) }

// leaderIn 在给定节点集合内断言唯一领导者（用于分区场景）。
func (c *cluster) leaderIn(ids []int) *Node {
	c.t.Helper()
	var leaders []*Node
	for _, id := range ids {
		if c.crashed[id] {
			continue
		}
		if r, _, _ := c.nodes[id].State(); r == Leader {
			leaders = append(leaders, c.nodes[id])
		}
	}
	if len(leaders) != 1 {
		c.logStates("LEADER-CHECK")
		c.t.Fatalf("want exactly 1 leader in %v, got %d", ids, len(leaders))
	}
	return leaders[0]
}

// waitLeader 在 maxTicks 内等待全集群选出一个领导者。
func (c *cluster) waitLeader(maxTicks int) *Node { return c.waitLeaderIn(maxTicks, c.ids) }

// waitLeaderIn 在 maxTicks 内等待给定集合内选出一个领导者。
func (c *cluster) waitLeaderIn(maxTicks int, ids []int) *Node {
	c.t.Helper()
	for i := 0; i < maxTicks; i++ {
		c.tick(1)
		count := 0
		for _, id := range ids {
			if c.crashed[id] {
				continue
			}
			if r, _, _ := c.nodes[id].State(); r == Leader {
				count++
			}
		}
		if count == 1 {
			return c.leaderIn(ids)
		}
	}
	c.logStates("WAIT-LEADER")
	c.t.Fatalf("no stable leader in %v after %d ticks", ids, maxTicks)
	return nil
}

// propose 向领导者提交命令并等待其提交；超时则以 NO_QUORUM 失败。
func (c *cluster) propose(l *Node, cmd Command, maxTicks int) ApplyResult {
	c.t.Helper()
	_, reason := l.Propose(cmd)
	if reason != ReasonNone {
		c.t.Fatalf("propose rejected: reason=%s", reason)
	}
	for i := 0; i < maxTicks; i++ {
		c.tick(1)
		if res, r := l.Propose(cmd); r == ReasonDuplicate && res.Index > 0 {
			return res // 幂等表命中：说明已提交并应用
		}
	}
	c.logStates("PROPOSE-TIMEOUT")
	c.t.Fatalf("command %+v not committed within %d ticks (reason=%s)", cmd, maxTicks, ReasonNoQuorum)
	return ApplyResult{}
}

// assertCommitted 断言某节点的提交位置与状态机值。
func (c *cluster) assertCommitted(id int, wantCommit Index, key, wantVal string) {
	c.t.Helper()
	n := c.nodes[id]
	_, term, commit := n.State()
	got, ok := n.Read(key)
	c.t.Logf("[tick=%d] assert node=%d term=%d commit=%d applied=%d key=%q val=%q (want commit>=%d val=%q)",
		c.ticks, id, term, commit, n.Applied(), key, got, wantCommit, wantVal)
	if commit < wantCommit {
		c.t.Fatalf("node %d commit=%d, want >= %d", id, commit, wantCommit)
	}
	if !ok || got != wantVal {
		c.t.Fatalf("node %d read %q = %q,%v; want %q", id, key, got, ok, wantVal)
	}
}

func (c *cluster) String() string {
	s := ""
	for _, id := range c.ids {
		if c.crashed[id] {
			s += fmt.Sprintf("node=%d CRASHED; ", id)
		} else {
			s += c.nodes[id].Describe() + "; "
		}
	}
	return s
}
