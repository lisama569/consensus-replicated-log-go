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
	// PreVote=true 表示这是一次不递增任期的预投票探测。
	PreVote bool
}

type RequestVoteResponse struct {
	Term        Term
	VoteGranted bool
	Reason      RejectReason
	Peer        NodeID
	PreVote     bool
}

// ---- InstallSnapshot（快照追赶） ----

type InstallSnapshotRequest struct {
	Term              Term
	LeaderID          NodeID
	LastIncludedIndex Index
	LastIncludedTerm  Term
	Data              []byte

	// ---- 分块续传字段（Chunked=false 时全部为零值，走旧版整体安装路径） ----
	Chunked    bool   // 是否为分块快照请求
	TransferID uint64 // 本次传输的标识（同一 leader 任期内单调）
	Offset     uint64 // Data 在完整快照中的字节偏移
	Length     uint64 // 完整快照的字节总长
	Checksum   []byte // 完整快照的 SHA-256，每个分块都携带
	Done       bool   // 本分块是否为最后一块
}

type InstallSnapshotResponse struct {
	Term    Term
	Success bool
	Reason  RejectReason
	Peer    NodeID

	// ---- 分块续传字段 ----
	Chunked      bool
	TransferID   uint64
	LastIncluded Index  // 响应对应快照的末索引（会话相关性判定）
	AckOffset    uint64 // 已确认连续落盘的字节数（下一块期望偏移）
	Complete     bool   // 整份快照是否已安装完成
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
	chunked      bool
	transferID   uint64
	ackOffset    uint64
	complete     bool
}

type snapshotResponse struct {
	peer NodeID
	resp *installResult
}
