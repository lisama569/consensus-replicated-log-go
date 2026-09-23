package raft

import "sync"

// Transport 负责把消息投递到目标节点。实现可以模拟故障。
type Transport interface {
	Send(m Message)
}

type linkKey struct{ from, to int }

type delayedMsg struct {
	deliverAt int
	m         Message
}

// Network 是一个可控的内存网络：支持分区、丢包、重复与乱序（延迟）。
// 所有故障注入由测试显式控制，时钟由 Tick 显式推进，保证结果可复现。
type Network struct {
	mu       sync.Mutex
	handlers map[int]func(Message)
	drop     map[linkKey]bool
	dup      map[linkKey]bool
	delay    map[linkKey]int // 延迟 tick 数
	now      int
	pending  []delayedMsg
}

func NewNetwork() *Network {
	return &Network{
		handlers: make(map[int]func(Message)),
		drop:     make(map[linkKey]bool),
		dup:      make(map[linkKey]bool),
		delay:    make(map[linkKey]int),
	}
}

// Attach 注册节点消息处理函数。
func (n *Network) Attach(id int, h func(Message)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

// Send 实现 Transport。根据链路配置立即投递、丢弃、重复或延迟。
func (n *Network) Send(m Message) {
	n.mu.Lock()
	k := linkKey{m.From, m.To}
	if n.drop[k] {
		n.mu.Unlock()
		return
	}
	d := n.delay[k]
	dup := n.dup[k]
	if d > 0 {
		n.pending = append(n.pending, delayedMsg{deliverAt: n.now + d, m: m})
		if dup {
			n.pending = append(n.pending, delayedMsg{deliverAt: n.now + d, m: m})
		}
		n.mu.Unlock()
		return
	}
	h, ok := n.handlers[m.To]
	n.mu.Unlock()
	if !ok {
		return
	}
	h(m)
	if dup {
		h(m) // 重复投递：协议层必须幂等处理
	}
}

// Partition 将两组节点之间的消息全部丢弃（双向）。
func (n *Network) Partition(groupA, groupB []int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, a := range groupA {
		for _, b := range groupB {
			n.drop[linkKey{a, b}] = true
			n.drop[linkKey{b, a}] = true
		}
	}
}

// Heal 恢复全部连通性并清空延迟队列之外的故障配置。
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.drop = make(map[linkKey]bool)
}

// SetDrop 设置是否丢弃指定链路。
func (n *Network) SetDrop(from, to int, drop bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.drop[linkKey{from, to}] = drop
}

// SetDuplicate 设置指定链路是否重复投递。
func (n *Network) SetDuplicate(from, to int, dup bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dup[linkKey{from, to}] = dup
}

// SetDelay 设置指定链路延迟的 tick 数（乱序来源：不同链路不同延迟）。
func (n *Network) SetDelay(from, to, ticks int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ticks <= 0 {
		delete(n.delay, linkKey{from, to})
		return
	}
	n.delay[linkKey{from, to}] = ticks
}

// Tick 推进网络时钟，投递到期的延迟消息。
func (n *Network) Tick() {
	n.mu.Lock()
	n.now++
	var ready []Message
	var keep []delayedMsg
	for _, dm := range n.pending {
		if dm.deliverAt <= n.now {
			ready = append(ready, dm.m)
		} else {
			keep = append(keep, dm)
		}
	}
	n.pending = keep
	handlers := make(map[int]func(Message), len(n.handlers))
	for id, h := range n.handlers {
		handlers[id] = h
	}
	n.mu.Unlock()
	for _, m := range ready {
		if h, ok := handlers[m.To]; ok {
			h(m)
		}
	}
}
