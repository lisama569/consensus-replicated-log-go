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

// ---- InstallSnapshotChunk（大快照分块可续传） ----

// InstallSnapshotChunkRequest 携带快照的一个分块。
// 同一次传输由 SessionID 标识：会话 ID 由快照内容派生，
// 因此不同领导者持有相同快照文件时会话 ID 一致，接收端可跨领导者续传。
type InstallSnapshotChunkRequest struct {
	Term              Term
	LeaderID          NodeID
	SessionID         string // 传输会话 ID（由快照元数据+内容校验和派生）
	LastIncludedIndex Index
	LastIncludedTerm  Term
	Offset            uint64 // 本块在快照字节流中的偏移
	TotalSize         uint64 // 快照总字节数
	CRC               uint32 // 整个快照字节的 CRC32 校验和（每块都携带）
	Data              []byte // 本块数据（除最后一块外长度均为块大小）
	Done              bool   // 是否为最后一块
}

type InstallSnapshotChunkResponse struct {
	Term    Term
	Success bool
	Reason  RejectReason
	// NextOffset 是接收端已确认（已持久化）的连续前缀长度，
	// 发送端据此续传/重发，实现断点续传与乱序恢复。
	NextOffset uint64
	Peer       NodeID
}

// RPCKind 标识消息种类，便于网络层分类注入故障。
type RPCKind int

const (
	RPCAppendEntries RPCKind = iota
	RPCRequestVote
	RPCInstallSnapshot
	RPCInstallSnapshotChunk
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

// snapshotChunkResponse 是分块快照发送结果的内部包装。
type snapshotChunkResponse struct {
	peer      NodeID
	sessionID string // 发送时的会话 ID，用于忽略过期会话的迟到响应
	resp      *InstallSnapshotChunkResponse
}
