package raft

// RPC 消息定义：所有 RPC 都是请求/响应成对出现，网络层可丢、可重、可乱序。

// ---- AppendEntries（日志复制 + 心跳） ----

type AppendEntriesRequest struct {
	Term         Term
	LeaderID     NodeID
	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit Index
}

type AppendEntriesResponse struct {
	Term    Term
	Success bool
	Reason  RejectReason // Success=false 时给出可区分原因
	// XTerm: 冲突 term；XIndex: 该 term 的第一条索引（冲突快速回退优化）
	XTerm  Term
	XIndex Index
	// Peer 自身最后一条日志索引，供领导者推进匹配/回退
	PeerLastIndex Index
	Peer          NodeID
}

// ---- RequestVote（选举） ----

type RequestVoteRequest struct {
	Term         Term
	CandidateID  NodeID
	LastLogIndex Index
	LastLogTerm  Term
}

type RequestVoteResponse struct {
	Term        Term
	VoteGranted bool
	Reason      RejectReason
	Peer        NodeID
}

// ---- InstallSnapshot（快照追赶） ----

type InstallSnapshotRequest struct {
	Term              Term
	LeaderID          NodeID
	LastIncludedIndex Index
	LastIncludedTerm  Term
	Data              []byte
}

type InstallSnapshotResponse struct {
	Term    Term
	Success bool
	Reason  RejectReason
	Peer    NodeID
}

// RPCKind 标识消息种类，便于网络层分类注入故障。
type RPCKind int

const (
	RPCAppendEntries RPCKind = iota
	RPCRequestVote
	RPCInstallSnapshot
)

// ---- 出站响应的内部包装：网络 goroutine 收到响应后投递回事件循环 ----

type voteResponse struct {
	peer NodeID
	resp *RequestVoteResponse
}

type appendResponse struct {
	peer NodeID
	resp *AppendEntriesResponse
}

// installResult 是 InstallSnapshot 的内部结果（携带快照末索引）。
type installResult struct {
	term         Term
	success      bool
	reason       RejectReason
	lastIncluded Index
}

type snapshotResponse struct {
	peer NodeID
	resp *installResult
}
