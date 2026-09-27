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

	// SnapshotChunkSize 大快照分块传输的块大小（字节）。
	// 快照字节数不超过该值时仍走一次性 InstallSnapshot（兼容旧路径）；
	// 超过则切换为可分块、可断点续传的 InstallSnapshotChunk。
	SnapshotChunkSize int

	// RandSeed 随机种子：测试固定种子保证可复现；传 0 时使用时间随机。
	RandSeed int64
}

// DefaultConfig 返回一份默认配置（调用方仍需填写 ID/Peers/DataDir）。
func DefaultConfig() Config {
	return Config{
		TickInterval:      10 * time.Millisecond,
		ElectionMin:       10, // 100ms
		ElectionMax:       20, // 200ms
		SnapshotThreshold: 50,
		SnapshotChunkSize: 64 * 1024,
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
	return nil
}
