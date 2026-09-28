package raft

import (
	"errors"
	"time"
)

// Config 是节点的可配置项。
type Config struct {
	// ID 本节点 ID。
	ID NodeID
	// Peers 集群全部节点 ID（含本节点）。
	Peers []NodeID
	// DataDir 持久化目录（硬状态、日志段、快照）；空串表示纯内存（测试崩溃重启不可用）。
	DataDir string

	// TickInterval 逻辑时钟周期，同时也是心跳间隔。
	TickInterval time.Duration
	// ElectionMin / ElectionMax 选举超时范围（Tick 数），随机化以避免分票活锁。
	ElectionMin int
	ElectionMax int

	// SnapshotThreshold 快照之后增量日志达到该条数时触发一次压缩。
	SnapshotThreshold uint64

	// SnapshotChunkSize 分块快照每块的目标字节数；0 使用默认值（64KiB）。
	SnapshotChunkSize int
	// SnapshotRetryTicks 分块快照在未收到确认后重发当前块的 tick 间隔；0 使用默认值。
	SnapshotRetryTicks int
	// SnapshotMinGapTicks 两次压缩之间至少间隔的 tick 数（0 使用默认值）。
	// 生成快照（导出/序列化整个状态机）与状态机大小同阶，阈值很小时压缩会被
	// 高频触发、其 CPU/IO 成本超过写入本身；该下限把压缩成本均摊开，
	// 同时仍满足“增量日志达阈值后尽快压缩”的常规需求。
	SnapshotMinGapTicks int

	// RandSeed 随机种子：测试固定种子保证可复现；传 0 时使用时间随机。
	RandSeed int64
}

// DefaultConfig 返回一份默认配置（调用方仍需填写 ID/Peers/DataDir）。
func DefaultConfig() Config {
	return Config{
		TickInterval:       10 * time.Millisecond,
		ElectionMin:        10, // 100ms
		ElectionMax:        20, // 200ms
		SnapshotThreshold:  50,
		SnapshotChunkSize:  64 * 1024,
		SnapshotRetryTicks: 2, // 20ms：丢失块快速重传，停等传输不会因单次丢包停顿太久
		// 默认不做压缩时间节流（0）：只按增量阈值压缩。跟随者在突发复制时必须尽快
		// 压出快照，否则它被选为新主后无法向更落后的节点提供足够新的快照。
		SnapshotMinGapTicks: 0,
	}
}

// validate 校验配置并补默认值。
func (c *Config) validate() error {
	if c.ID == "" {
		return errors.New("raft: config.ID is required")
	}
	if len(c.Peers) < 1 {
		return errors.New("raft: config.Peers must contain at least the node itself")
	}
	found := false
	for _, p := range c.Peers {
		if p == c.ID {
			found = true
			break
		}
	}
	if !found {
		return errors.New("raft: config.ID must be included in config.Peers")
	}
	if c.TickInterval <= 0 {
		c.TickInterval = DefaultConfig().TickInterval
	}
	if c.ElectionMin <= 0 {
		c.ElectionMin = 10
	}
	if c.ElectionMax < c.ElectionMin {
		return errors.New("raft: config.ElectionMax must be >= ElectionMin")
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 50
	}
	if c.SnapshotChunkSize <= 0 {
		c.SnapshotChunkSize = 64 * 1024
	}
	if c.SnapshotRetryTicks <= 0 {
		c.SnapshotRetryTicks = 2
	}
	// 0 表示不做时间节流（仅按增量阈值压缩）；>0 时两次压缩至少间隔该 tick 数。
	if c.SnapshotMinGapTicks < 0 {
		c.SnapshotMinGapTicks = 0
	}
	return nil
}
