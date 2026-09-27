package raft

import "errors"

// RejectReason 是 RPC 被拒绝时的可区分原因码。
type RejectReason string

const (
	RejectNone           RejectReason = ""
	RejectStaleTerm      RejectReason = "stale_term"         // 请求任期落后于本节点当前任期
	RejectAlreadyVoted   RejectReason = "already_voted"      // 本任期已投票给其他候选人
	RejectLogNotUpToDate RejectReason = "log_not_up_to_date" // 候选人日志不如本节点新
	RejectNotLeader      RejectReason = "not_leader"         // 本节点不是领导者（客户端请求）
	RejectLeaderDeposed  RejectReason = "leader_deposed"     // 领导者在提交前发现更高任期，已被废黜
	RejectSteppedDown    RejectReason = "stepped_down"       // 节点已关闭/下台，无法处理
	RejectResync         RejectReason = "resync"             // 分块传输：无匹配的临时快照/偏移不符，要求从 0 重发
	RejectTransferFailed RejectReason = "transfer_failed"    // 分块传输：校验和不符/落盘失败，临时快照已丢弃
	RejectCancelled      RejectReason = "cancelled"          // 追赶已被用户取消
)

// 可用于 errors.Is 的哨兵错误。
var (
	ErrNotLeader      = errors.New("raft: node is not the leader")
	ErrStaleTerm      = errors.New("raft: request has stale term")
	ErrAlreadyVoted   = errors.New("raft: already voted in this term")
	ErrLogNotUpToDate = errors.New("raft: candidate log is not up-to-date")
	ErrLeaderDeposed  = errors.New("raft: leader deposed by higher term before commit")
	ErrSteppedDown    = errors.New("raft: node stepped down")
	ErrStopped        = errors.New("raft: node stopped")
	ErrCompacted      = errors.New("raft: requested index is older than snapshot")
)

// LeaderHintError 在 ErrNotLeader 之上附带当前已知的领导者 ID，便于客户端重试。
type LeaderHintError struct {
	Err    error
	Leader NodeID
}

func (e *LeaderHintError) Error() string {
	if e.Leader == "" {
		return e.Err.Error() + " (leader unknown)"
	}
	return e.Err.Error() + " (leader=" + string(e.Leader) + ")"
}

func (e *LeaderHintError) Unwrap() error { return e.Err }

func errWithLeader(err error, leader NodeID) error {
	return &LeaderHintError{Err: err, Leader: leader}
}
