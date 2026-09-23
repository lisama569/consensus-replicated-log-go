package raft

import "sync"

// Config 节点配置。
type Config struct {
	ID              int
	Peers           []int // 全部节点 ID（含自己）
	ElectionTimeout  int  // tick 数
	HeartbeatTimeout int  // tick 数
	Storage         Storage
	Transport       Transport
}

// Node 是一个 Raft 节点。所有方法须在同一个 goroutine 中调用，
// 或通过 Lock 暴露的互斥保护（测试里用 Tick/Step 驱动，天然单线程）。
type Node struct {
	mu sync.Mutex

	id    int
	peers []int
	cfg   Config

	role Role
	term Term
	votedFor int

	log   *Log
	commitIndex Index
	lastApplied Index

	// leader 状态
	nextIndex  map[int]Index
	matchIndex map[int]Index

	// 幂等表：clientID -> requestID -> result
	dedup map[string]map[uint64]ApplyResult

	// 状态机：key -> value
	kv map[string]string

	storage   Storage
	transport Transport

	electionElapsed  int
	heartbeatElapsed int
}

func NewNode(cfg Config) *Node {
	return &Node{}
}

// Tick 推进逻辑时钟：驱动选举与心跳。
func (n *Node) Tick() {}

// Step 处理一条收到的消息。
func (n *Node) Step(m Message) {}

// Propose 提交一条客户端命令。返回 (结果, 拒绝原因)。
// 只有多数派确认后才会通过 WaitApply 观察到结果。
func (n *Node) Propose(cmd Command) (ApplyResult, RejectReason) {
	return ApplyResult{}, ReasonNotLeader
}

// Read 读取状态机（要求已应用到 commitIndex）。
func (n *Node) Read(key string) (string, bool) { return "", false }

// Snapshot 压缩 [.., index] 的日志并生成快照。
func (n *Node) Snapshot(to Index) error { return nil }

// State 返回当前角色、任期、提交位置（用于测试断言与日志）。
func (n *Node) State() (Role, Term, Index) { return Follower, 0, 0 }

// Applied 返回已应用位置。
func (n *Node) Applied() Index { return 0 }
