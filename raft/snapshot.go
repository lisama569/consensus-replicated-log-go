package raft

// Snapshot 是一次日志压缩的结果：包含压缩点及状态机镜像。
type Snapshot struct {
	Index Index  // 快照包含的最后一条日志下标
	Term  Term   // 该下标对应日志的任期
	Data  []byte // 状态机序列化镜像
}
