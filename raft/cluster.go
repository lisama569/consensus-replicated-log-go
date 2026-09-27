package raft

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// Cluster 是测试与 demo 共用的本地模拟集群：
// 共享一个 MemoryTransport，支持节点崩溃（删除内存节点，保留磁盘）与重启，
// 支持分区隔离与故障注入。
type Cluster struct {
	Tr     *MemoryTransport
	dir    string
	seed   int64
	cfgs   map[NodeID]Config
	mu     sync.Mutex // 保护 nodes：测试的故障钩子 goroutine 可能与主 goroutine 并发 Crash/Restart
	nodes  map[NodeID]*Node
	logAll atomic.Bool
}

// NewCluster 创建 size 个节点。persistent=true 时使用临时目录做真实持久化，
// 可做崩溃重启；seed 固定时选主/故障注入序列可复现。
func NewCluster(size int, persistent bool, seed int64) (*Cluster, error) {
	dir := ""
	if persistent {
		var err error
		dir, err = os.MkdirTemp("", "raft-cluster-")
		if err != nil {
			return nil, err
		}
	}
	peers := make([]NodeID, size)
	for i := range peers {
		peers[i] = NodeID("n" + itoa(i+1))
	}
	tr := NewMemoryTransport()
	tr.Seed(seed)

	c := &Cluster{
		Tr:    tr,
		dir:   dir,
		seed:  seed,
		cfgs:  make(map[NodeID]Config),
		nodes: make(map[NodeID]*Node),
	}
	for _, id := range peers {
		ddir := ""
		if persistent {
			ddir = filepath.Join(dir, string(id))
		}
		cfg := DefaultConfig()
		cfg.ID = id
		cfg.Peers = peers
		cfg.DataDir = ddir
		cfg.RandSeed = seed
		c.cfgs[id] = cfg
		if err := c.spawn(id); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *Cluster) spawn(id NodeID) error {
	n, err := NewNode(c.cfgs[id], c.Tr)
	if err != nil {
		return err
	}
	if c.logAll.Load() {
		n.SetLogger(defaultLogger(id))
	}
	n.Start()
	c.mu.Lock()
	c.nodes[id] = n
	c.mu.Unlock()
	return nil
}

// SetSnapshotThreshold 对尚未创建的节点生效（在崩溃重启前调用）。
func (c *Cluster) SetSnapshotThreshold(id NodeID, n uint64) {
	cfg := c.cfgs[id]
	cfg.SnapshotThreshold = n
	c.cfgs[id] = cfg
}

// SetSnapshotChunkSize 配置分块快照的块大小（对尚未创建的节点生效，需重启节点加载）。
func (c *Cluster) SetSnapshotChunkSize(id NodeID, bytes int) {
	cfg := c.cfgs[id]
	cfg.SnapshotChunkSize = bytes
	c.cfgs[id] = cfg
}

// SetSnapshotChunkSizeAll 对集群全部节点设置块大小。
func (c *Cluster) SetSnapshotChunkSizeAll(bytes int) {
	for _, id := range c.IDs() {
		c.SetSnapshotChunkSize(id, bytes)
	}
}

// SetSnapshotHook 转发到传输层的测试故障钩子。
func (c *Cluster) SetSnapshotHook(h *SnapshotFaultHook) { c.Tr.SetSnapshotHook(h) }

func (c *Cluster) Nodes() map[NodeID]*Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[NodeID]*Node, len(c.nodes))
	for k, v := range c.nodes {
		out[k] = v
	}
	return out
}

func (c *Cluster) Node(id NodeID) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

func (c *Cluster) IDs() []NodeID {
	out := make([]NodeID, 0, len(c.cfgs))
	for id := range c.cfgs {
		out = append(out, id)
	}
	return out
}

// Leader 轮询各节点状态返回当前领导者（无则返回空串）。
func (c *Cluster) Leader() NodeID {
	c.mu.Lock()
	snap := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		snap = append(snap, n)
	}
	c.mu.Unlock()
	for _, n := range snap {
		if s := n.Status(); s.Role == RoleLeader {
			return s.ID
		}
	}
	return ""
}

// Crash 停止节点并从内存移除（磁盘保留），模拟进程崩溃。
func (c *Cluster) Crash(id NodeID) error {
	c.mu.Lock()
	n, ok := c.nodes[id]
	if ok {
		delete(c.nodes, id)
	}
	c.mu.Unlock()
	if !ok {
		return nil
	}
	// Stop 在锁外执行（可能等待在途 RPC），避免与其它节点操作互锁；
	// 删除映射已在锁内完成，重复 Crash 只会看到 !ok。
	return n.Stop()
}

// Restart 用同一数据目录重建节点，模拟崩溃后恢复。
func (c *Cluster) Restart(id NodeID) error {
	c.mu.Lock()
	_, ok := c.nodes[id]
	c.mu.Unlock()
	if ok {
		return nil
	}
	return c.spawn(id)
}

// Isolate / Heal 控制分区。
func (c *Cluster) Isolate(id NodeID) { c.Tr.SetPartition(id, true) }
func (c *Cluster) Heal(id NodeID)    { c.Tr.SetPartition(id, false) }
func (c *Cluster) HealAll()          { c.Tr.HealAll() }

// Flaky 对 from->to 的某类 RPC 注入故障。
func (c *Cluster) Flaky(from, to NodeID, kind RPCKind, p FaultPolicy) {
	c.Tr.SetPolicy(from, to, kind, p)
}

// EnableLogs 对当前及未来重启的节点打印调试日志。
func (c *Cluster) EnableLogs() {
	c.logAll.Store(true)
	for _, n := range c.Nodes() {
		n.SetLogger(defaultLogger(n.cfg.ID))
	}
}

func (c *Cluster) Stop() error {
	var first error
	for _, id := range c.IDs() {
		if err := c.Crash(id); err != nil && first == nil {
			first = err
		}
	}
	if c.dir != "" {
		_ = os.RemoveAll(c.dir)
	}
	return first
}

// SnapshotExists 报告某节点磁盘上是否存在快照。
func (c *Cluster) SnapshotExists(id NodeID) bool {
	cfg := c.cfgs[id]
	if cfg.DataDir == "" {
		return false
	}
	s, err := OpenStore(cfg.DataDir)
	if err != nil {
		return false
	}
	defer s.Close()
	return s.HasSnapshot()
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}
