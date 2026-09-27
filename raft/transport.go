package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// Handler 是节点注册到网络层的统一 RPC 处理器。
// 实现必须是并发安全的（会在不同的发送方 goroutine 中被调用）；
// 节点侧通过将消息投递进事件循环 channel 来保证串行化。
type Handler func(from NodeID, req any) any

// Transport 抽象节点之间的通信。实现可以是内存模拟网络，
// 也可以替换为真实网络；允许消息丢失、重复、乱序和延迟。
type Transport interface {
	SendAppendEntries(ctx context.Context, to NodeID, req *AppendEntriesRequest) *AppendEntriesResponse
	SendRequestVote(ctx context.Context, to NodeID, req *RequestVoteRequest) *RequestVoteResponse
	SendInstallSnapshot(ctx context.Context, to NodeID, req *InstallSnapshotRequest) *InstallSnapshotResponse
}

// FaultPolicy 描述模拟网络对 from->to 方向、某类 RPC 的故障注入策略。
type FaultPolicy struct {
	DropRate  float64 // 丢包/丢响应概率 [0,1]
	Duplicate bool    // 请求送达两次（模拟消息重复）
	Reorder   bool    // 注入随机延迟，使消息可能乱序到达
}

type policyKey struct {
	from, to NodeID
	kind     RPCKind
}

// SnapshotFaultHook 是测试用的快照消息钩子（仅对分块 InstallSnapshot 生效）。
//   - Drop 返回 true 时该消息（请求）被直接丢弃，模拟丢包/超时；
//   - Mutate 可在投递前原地修改请求（例如翻转字节模拟链路损坏）。
//
// 钩子用于测试与本地复现；生产环境不要设置。
type SnapshotFaultHook struct {
	Drop   func(from, to NodeID, req *InstallSnapshotRequest) bool
	Mutate func(from, to NodeID, req *InstallSnapshotRequest)
}

// MemoryTransport 是进程内模拟网络：同步调用 + 可注入故障。
type MemoryTransport struct {
	mu       sync.RWMutex
	handlers map[NodeID]Handler
	policies map[policyKey]FaultPolicy
	isolated map[NodeID]bool // 被分区隔离的节点（双向 blackhole）
	delayMax time.Duration
	rngMu    sync.Mutex
	rng      *rand.Rand

	hookMu sync.Mutex
	hook   *SnapshotFaultHook
}

func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{
		handlers: make(map[NodeID]Handler),
		policies: make(map[policyKey]FaultPolicy),
		isolated: make(map[NodeID]bool),
		delayMax: 15 * time.Millisecond,
		rng:      rand.New(rand.NewSource(1)),
	}
}

// Seed 重置故障随机源（测试固定种子时使用）。
func (t *MemoryTransport) Seed(seed int64) {
	t.rngMu.Lock()
	t.rng = rand.New(rand.NewSource(seed))
	t.rngMu.Unlock()
}

// Bind 将节点接入网络并注册其 RPC 处理器。
func (t *MemoryTransport) Bind(id NodeID, h Handler) {
	t.mu.Lock()
	t.handlers[id] = h
	t.mu.Unlock()
}

func (t *MemoryTransport) handler(id NodeID) (Handler, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.isolated[id] {
		return nil, false
	}
	h, ok := t.handlers[id]
	return h, ok
}

// SetPolicy 设置 from->to 方向上某类 RPC 的故障策略。
func (t *MemoryTransport) SetPolicy(from, to NodeID, kind RPCKind, p FaultPolicy) {
	t.mu.Lock()
	t.policies[policyKey{from, to, kind}] = p
	t.mu.Unlock()
}

func (t *MemoryTransport) policy(from, to NodeID, kind RPCKind) FaultPolicy {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.isolated[from] || t.isolated[to] {
		return FaultPolicy{DropRate: 1}
	}
	return t.policies[policyKey{from, to, kind}]
}

// SetPartition 对节点 id 做双向隔离（其所有 RPC 全部丢失）。
func (t *MemoryTransport) SetPartition(id NodeID, isolated bool) {
	t.mu.Lock()
	t.isolated[id] = isolated
	t.mu.Unlock()
}

// SetPartitionGroup 将一组节点与集群其余节点双向隔离（组内互通）。
func (t *MemoryTransport) SetPartitionGroup(members []NodeID, isolated bool) {
	set := map[NodeID]bool{}
	for _, m := range members {
		set[m] = true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !isolated {
		for _, m := range members {
			t.isolated[m] = false
		}
		return
	}
	// 隔离语义：组内成员只与组内通信 -> 把不在组内的节点对组内方向全部切断，
	// 这里简化为：对组内每个成员，将其与非组内节点之间的策略置为全丢，
	// 通过 isolated 标记无法表达“组内互通”，因此采用逐对策略实现。
	for from := range t.handlers {
		for to := range t.handlers {
			if from == to {
				continue
			}
			inA, inB := set[from], set[to]
			if inA != inB {
				for _, k := range []RPCKind{RPCAppendEntries, RPCRequestVote, RPCInstallSnapshot} {
					t.policies[policyKey{from, to, k}] = FaultPolicy{DropRate: 1}
				}
			}
		}
	}
}

// HealAll 清除分区与全部故障策略。
func (t *MemoryTransport) HealAll() {
	t.mu.Lock()
	t.policies = make(map[policyKey]FaultPolicy)
	t.isolated = make(map[NodeID]bool)
	t.mu.Unlock()
}

// clearPoliciesBetween 清除某方向全部 RPC 策略。
func (t *MemoryTransport) clearPoliciesBetween(from, to NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, k := range []RPCKind{RPCAppendEntries, RPCRequestVote, RPCInstallSnapshot} {
		delete(t.policies, policyKey{from, to, k})
	}
}

// SetSnapshotHook 安装/清除测试用快照故障钩子。
func (t *MemoryTransport) SetSnapshotHook(h *SnapshotFaultHook) {
	t.hookMu.Lock()
	t.hook = h
	t.hookMu.Unlock()
}

func (t *MemoryTransport) snapshotHook() *SnapshotFaultHook {
	t.hookMu.Lock()
	defer t.hookMu.Unlock()
	return t.hook
}

func (t *MemoryTransport) randf() float64 {
	t.rngMu.Lock()
	defer t.rngMu.Unlock()
	return t.rng.Float64()
}

func (t *MemoryTransport) delay(d time.Duration) {
	if d <= 0 {
		return
	}
	t.rngMu.Lock()
	ms := t.rng.Int63n(int64(d/time.Millisecond) + 1)
	t.rngMu.Unlock()
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

// deliver 应用故障策略后调用处理器；返回 nil 表示消息（或响应）被丢弃。
func (t *MemoryTransport) deliver(from, to NodeID, kind RPCKind, req any) any {
	h, ok := t.handler(to)
	if !ok {
		return nil // 节点离线（已停止/崩溃）或被隔离
	}
	p := t.policy(from, to, kind)
	if p.DropRate > 0 && t.randf() < p.DropRate {
		return nil
	}
	if p.Reorder {
		t.delay(t.delayMax)
	}
	if p.Duplicate {
		// 重复送达：两次都执行。第一次的结果作为正式响应返回。
		go h(from, req)
	}
	return h(from, req)
}

func (t *MemoryTransport) SendAppendEntries(ctx context.Context, to NodeID, req *AppendEntriesRequest) *AppendEntriesResponse {
	r := t.deliver(req.LeaderID, to, RPCAppendEntries, req)
	if r == nil {
		return nil
	}
	return r.(*AppendEntriesResponse)
}

func (t *MemoryTransport) SendRequestVote(ctx context.Context, to NodeID, req *RequestVoteRequest) *RequestVoteResponse {
	r := t.deliver(req.CandidateID, to, RPCRequestVote, req)
	if r == nil {
		return nil
	}
	return r.(*RequestVoteResponse)
}

func (t *MemoryTransport) SendInstallSnapshot(ctx context.Context, to NodeID, req *InstallSnapshotRequest) *InstallSnapshotResponse {
	// 测试钩子：可丢块或损坏块（仅对分块消息生效，避免影响旧版整包路径）。
	if req.Chunked {
		if h := t.snapshotHook(); h != nil {
			if h.Drop != nil && h.Drop(req.LeaderID, to, req) {
				return nil
			}
			if h.Mutate != nil {
				h.Mutate(req.LeaderID, to, req)
			}
		}
	}
	r := t.deliver(req.LeaderID, to, RPCInstallSnapshot, req)
	if r == nil {
		return nil
	}
	return r.(*InstallSnapshotResponse)
}
