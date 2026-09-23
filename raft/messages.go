package raft

// MessageType 节点间消息类型。
type MessageType int

const (
	MsgRequestVote MessageType = iota
	MsgRequestVoteResp
	MsgAppend
	MsgAppendResp
	MsgInstallSnapshot
	MsgInstallSnapshotResp
)

// Message 是节点间传递的统一消息信封。
type Message struct {
	Type MessageType
	From int
	To   int

	Term Term // 发送方任期

	// RequestVote
	LastLogIndex Index
	LastLogTerm  Term

	// AppendEntries
	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []Entry
	LeaderCommit Index

	// InstallSnapshot
	Snapshot Snapshot

	// 响应字段
	Granted      bool         // 投票是否通过 / 追加是否成功
	RejectReason RejectReason // 拒绝原因（可区分）
	MatchIndex   Index        // AppendResp 成功时的 matchIndex
}
