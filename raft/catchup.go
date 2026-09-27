package raft

// CatchUpState 是快照追赶的用户可观测状态。
type CatchUpState string

const (
	// CatchUpIdle 没有进行中的快照追赶（尚未需要或未开始）。
	CatchUpIdle CatchUpState = "idle"
	// CatchUpSyncing 正在接收分块大快照。
	CatchUpSyncing CatchUpState = "syncing"
	// CatchUpCaughtUp 快照已完整安装，正在/已经通过常规日志复制追平提交点。
	CatchUpCaughtUp CatchUpState = "caught_up"
	// CatchUpFailed 传输失败（校验不符、落盘错误等），将在领导者重试后自动重新开始。
	CatchUpFailed CatchUpState = "failed"
	// CatchUpCancelled 被用户显式取消；ResumeCatchUp 后可重新开始。
	CatchUpCancelled CatchUpState = "cancelled"
)

// CatchUpProgress 是某个节点追赶进度的用户侧视图。
type CatchUpProgress struct {
	State        CatchUpState
	SnapshotIdx  Index  // 正在安装/已安装快照的末索引
	Received     uint64 // 已连续确认落盘的字节数
	Total        uint64 // 完整快照字节数
	Leader       NodeID // 来源领导者
	TransferID   uint64 // 传输标识
	FailedReason RejectReason
}

// Ready 报告快照追赶是否已完成：快照已安装且状态机包含其全部内容。
// Ready=true 后剩余进度由常规 AppendEntries 补齐，状态机不会再回退。
func (p CatchUpProgress) Ready() bool { return p.State == CatchUpCaughtUp }
