package raft

// NodeID 是集群中节点的唯一标识。
type NodeID string

// Term 是 Raft 任期，单调递增。
type Term uint64

// Index 是日志索引，从 1 开始。
type Index uint64

// Role 表示节点当前角色。
type Role int

const (
	RoleFollower  Role = iota // 跟随者
	RoleCandidate             // 候选人
	RoleLeader                // 领导者
)

func (r Role) String() string {
	switch r {
	case RoleFollower:
		return "Follower"
	case RoleCandidate:
		return "Candidate"
	case RoleLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Operation 是状态机支持的操作类型。
type Operation string

const (
	OpSet  Operation = "Set" // 幂等写：Set(key,value)
	OpNoop Operation = "Noop"
)

// Command 是客户端命令，按 Op 解释字段。
type Command struct {
	Op    Operation `json:"op"`
	Key   string    `json:"key,omitempty"`
	Value string    `json:"value,omitempty"`

	// ClientID / Seq 用于去重：同一 ClientID 下相同 Seq 视为重复请求。
	ClientID string `json:"client_id,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`
}

// LogEntry 是一条日志条目。
type LogEntry struct {
	Term    Term    `json:"term"`
	Index   Index   `json:"index"`
	Command Command `json:"command"`
}

// Status 是节点在某一时刻的可观测状态（测试日志打印用）。
type Status struct {
	ID            NodeID
	Role          Role
	Term          Term
	LeaderID      NodeID
	CommitIndex   Index
	LastLogIndex  Index
	LastLogTerm   Term
	VotedFor      NodeID
	AppliedIndex  Index
	SnapshotIndex Index
	Peers         []NodeID
}
