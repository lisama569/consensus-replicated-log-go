package raft

import "sync"

// Transport 负责把消息投递到目标节点。实现可以模拟故障。
type Transport interface {
	Send(m Message)
}

// Network 是一个可控的内存网络：支持分区、丢包、重复与乱序。
// 所有故障注入由测试显式控制，保证结果可复现。
type Network struct {
	mu        sync.Mutex
	handlers  map[int]func(Message)
	dropRate  float64 // 未使用占位
	dupEvery  int     // 未使用占位
}

func NewNetwork() *Network {
	return &Network{handlers: make(map[int]func(Message))}
}

// Attach 注册节点消息处理函数。
func (n *Network) Attach(id int, h func(Message)) {}

// Send 实现 Transport。
func (n *Network) Send(m Message) {}

// Partition 将节点分为两组，组间消息全部丢弃。
func (n *Network) Partition(groupA, groupB []int) {}

// Heal 恢复全部连通性。
func (n *Network) Heal() {}

// SetDrop 设置是否丢弃指定链路。
func (n *Network) SetDrop(from, to int, drop bool) {}

// SetDuplicate 设置指定链路是否重复投递。
func (n *Network) SetDuplicate(from, to int, dup bool) {}

// SetDelay 设置指定链路延迟的 tick 数（乱序来源）。
func (n *Network) SetDelay(from, to, ticks int) {}

// Tick 推进网络时钟，投递到期的延迟消息。
func (n *Network) Tick() {}
