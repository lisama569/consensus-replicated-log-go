// Package raft 实现一套可本地运行的多节点复制日志一致性协议。
package raft

import "errors"

// Role 表示节点角色。
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "Unknown"
}

// Term 任期号，Index 日志下标（从 1 开始，0 表示哨兵）。
type Term uint64
type Index uint64

// Command 是一条客户端命令。ClientID+RequestID 构成幂等键。
type Command struct {
	ClientID  string
	RequestID uint64
	Key       string
	Value     string
}

// Entry 是一条日志项。
type Entry struct {
	Term    Term
	Index   Index
	Command Command
}

// RejectReason 用于区分请求被拒绝的原因。
type RejectReason string

const (
	ReasonNone           RejectReason = ""
	ReasonNotLeader      RejectReason = "NOT_LEADER"       // 当前节点不是领导者
	ReasonStaleTerm      RejectReason = "STALE_TERM"       // 任期过期（消息来自旧任期）
	ReasonAlreadyVoted   RejectReason = "ALREADY_VOTED"    // 本任期已投过票
	ReasonLogMismatch    RejectReason = "LOG_MISMATCH"     // 日志不一致，需要回退 nextIndex
	ReasonDuplicate      RejectReason = "DUPLICATE"        // 重复请求，返回首个结果
	ReasonNoQuorum       RejectReason = "NO_QUORUM"        // 无法取得多数派
	ReasonSnapshotBehind RejectReason = "SNAPSHOT_BEHIND"  // 日志已压缩，需要快照
)

// 协议级错误。
var (
	ErrNotLeader     = errors.New("raft: not leader")
	ErrNoQuorum      = errors.New("raft: no quorum")
	ErrLogMismatch   = errors.New("raft: log mismatch")
	ErrStaleTerm     = errors.New("raft: stale term")
	ErrAlreadyVoted  = errors.New("raft: already voted in this term")
	ErrCompacted     = errors.New("raft: entry compacted")
	ErrUnavailable   = errors.New("raft: entry not yet available")
)

// ApplyResult 是已提交命令应用到状态机后的结果。
type ApplyResult struct {
	Index   Index
	Term    Term
	Command Command
	Value   string // 状态机返回的旧值（写语义为覆盖写，返回前值）
}
