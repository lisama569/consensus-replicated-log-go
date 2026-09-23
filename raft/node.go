package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
)

// internalEvent 是投递进节点事件循环的内部事件（RPC / 客户端请求 / 时钟）。
// 所有对节点可变状态的读写都经事件循环串行处理，杜绝数据竞争。
type internalEvent struct {
	kind eventKind
	rpc  any      // 入站 RPC 请求（eventRPC 时有效）
	from NodeID   // 入站 RPC 的发送方
	resp chan any // RPC 同步响应通道
}

type eventKind int

const (
	eventTick eventKind = iota
	eventRPC
	eventTimeout
)

// Node 是一个 Raft 节点。
type Node struct {
	cfg   Config
	store *FileStore
	sm    *KVStateMachine
	tr    Transport

	logf func(format string, args ...any)

	// ---- 仅事件循环访问的可变状态 ----
	role        Role
	term        Term
	votedFor    NodeID
	leaderID    NodeID
	log         []LogEntry // log[0] 为快照占位项，log[1:] 为日志后缀
	snapMeta    SnapshotMeta
	commitIndex Index
	lastApplied Index

	// 领导者可变状态
	nextIndex  map[NodeID]Index
	matchIndex map[NodeID]Index

	// 候选人得票
	grantedVotes map[NodeID]bool

	// 等待提交的提案：日志索引 -> 全部等待者（重复请求会共用一个等待列表）
	proposals map[Index][]chan clientOutcome

	// appliedSeq 记录每个客户端最近一次已应用的 (seq,index)（幂等快速路径，不持久化）。
	appliedSeq map[string]appliedSeqEntry
	// pendingReads 是领导者在途的线性一致读队列。
	pendingReads []*readState

	electionElapsed int
	rng             *rand.Rand

	// ---- 事件循环通信 ----
	events chan internalEvent

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}

	// WG 用于等待节点发出的后台 goroutine（RPC 等）全部退出。
	wg sync.WaitGroup
}

// NewNode 构造节点：打开持久化存储并恢复硬状态、快照与日志。
// 此时事件循环尚未启动（无并发），直接恢复状态机。
func NewNode(cfg Config, tr Transport) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	store, err := OpenStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:        cfg,
		store:      store,
		tr:         tr,
		sm:         NewKVStateMachine(),
		role:       RoleFollower,
		nextIndex:  make(map[NodeID]Index),
		matchIndex: make(map[NodeID]Index),
		proposals:  make(map[Index][]chan clientOutcome),
		appliedSeq: make(map[string]appliedSeqEntry),
		events:     make(chan internalEvent, 256),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
		logf:       func(string, ...any) {},
	}
	seed := cfg.RandSeed
	if seed == 0 {
		seed = int64(uint64(rand.Int63()) ^ uint64(hashID(cfg.ID)))
	}
	// 每个节点使用不同的随机流，避免所有节点同时超时。
	n.rng = rand.New(rand.NewSource(seed + int64(hashID(cfg.ID))))

	if err := n.restore(); err != nil {
		store.Close()
		return nil, err
	}
	return n, nil
}

// SetLogger 注入日志函数（默认丢弃）。需在 Start 前调用。
func (n *Node) SetLogger(f func(format string, args ...any)) {
	if f != nil {
		n.logf = f
	}
}

// restore 从持久化介质恢复硬状态、快照与日志后缀。
func (n *Node) restore() error {
	term, voted, err := n.store.LoadState()
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	n.term, n.votedFor = term, voted

	meta, data, err := n.store.LoadSnapshot()
	if err != nil {
		return fmt.Errorf("load snapshot: %w", err)
	}
	n.log = []LogEntry{{Index: 0, Term: 0}}
	if len(data) > 0 || meta.LastIncludedIndex > 0 {
		if err := n.sm.Restore(data); err != nil {
			return fmt.Errorf("restore state machine: %w", err)
		}
		n.snapMeta = meta
		n.log[0] = LogEntry{Index: meta.LastIncludedIndex, Term: meta.LastIncludedTerm}
		n.commitIndex = meta.LastIncludedIndex
		n.lastApplied = meta.LastIncludedIndex
	}

	entries, err := n.store.LoadLogEntries()
	if err != nil {
		return fmt.Errorf("load log: %w", err)
	}
	if len(entries) > 0 {
		// 校验日志后缀与快照连续。
		want := meta.LastIncludedIndex + 1
		if entries[0].Index != want {
			return fmt.Errorf("log suffix gap: want index %d, got %d", want, entries[0].Index)
		}
		n.log = append(n.log, entries...)
	}
	return nil
}

// Start 注册 RPC 处理器并启动事件循环与时钟。
func (n *Node) Start() {
	n.tr.(interface {
		Bind(NodeID, Handler)
	}).Bind(n.cfg.ID, n.rpcHandler)
	go n.run()
}

// Stop 优雅停止：关闭事件循环并等待后台 goroutine 退出。
func (n *Node) Stop() error {
	n.stopOnce.Do(func() { close(n.stopCh) })
	<-n.doneCh
	n.wg.Wait()
	return n.store.Close()
}

// SubmitProposal 客户端提交写请求；仅领导者接受，其余返回 ErrNotLeader。
func (n *Node) SubmitProposal(ctx context.Context, cmd Command) (clientOutcome, error) {
	return n.submit(ctx, cmd, false)
}

// ReadIndex 提交一次读：领导者确认领导权（一轮心跳多数派）后返回状态机值。
func (n *Node) ReadIndex(ctx context.Context, key string) (string, bool, error) {
	outcome, err := n.submit(ctx, Command{Op: OpNoop, Key: key}, true)
	if err != nil {
		return "", false, err
	}
	if outcome.readValue != nil {
		return outcome.readValue.value, outcome.readValue.found, nil
	}
	return "", false, nil
}

// appliedSeqEntry 是某客户端最近一次已应用提案的序列号与其日志索引。
type appliedSeqEntry struct {
	seq   uint64
	index Index
}

type clientOutcome struct {
	index     Index
	term      Term
	duplicate bool
	readValue *struct {
		value string
		found bool
	}
	err error
}

func (n *Node) submit(ctx context.Context, cmd Command, isRead bool) (clientOutcome, error) {
	// 缓冲为 1：事件循环在 apply 时同步通知，即使 ctx 已取消也不会卡死循环。
	ch := make(chan clientOutcome, 1)
	req := proposeReq{cmd: cmd, isRead: isRead, res: ch}
	select {
	case n.events <- internalEvent{kind: eventRPC, from: n.cfg.ID, rpc: clientCall{req: req}}:
	case <-ctx.Done():
		return clientOutcome{}, ctx.Err()
	case <-n.stopCh:
		return clientOutcome{}, ErrStopped
	}
	select {
	case o := <-ch:
		return o, o.err
	case <-ctx.Done():
		return clientOutcome{}, ctx.Err()
	case <-n.stopCh:
		return clientOutcome{}, ErrStopped
	}
}

// Status 返回节点当前状态快照（通过事件循环读取，保证一致视图）。
func (n *Node) Status() Status {
	ch := make(chan Status, 1)
	select {
	case n.events <- internalEvent{kind: eventRPC, from: n.cfg.ID, rpc: statusCall{res: ch}}:
	case <-n.stopCh:
		return Status{ID: n.cfg.ID}
	}
	select {
	case s := <-ch:
		return s
	case <-n.stopCh:
		return Status{ID: n.cfg.ID}
	}
}

// ---- 持久化辅助（仅事件循环调用） ----

func (n *Node) persistState() {
	if err := n.store.SaveState(n.term, n.votedFor); err != nil {
		n.logf("[%s] persist state error: %v", n.cfg.ID, err)
	}
}

func (n *Node) debugf(format string, args ...any) {
	n.logf("[%s %s t=%d commit=%d applied=%d last=%d] "+format,
		append([]any{n.cfg.ID, n.role, n.term, n.commitIndex, n.lastApplied, n.lastLogIndex()}, args...)...)
}

// ---- 仅类型占位，实际定义在各实现文件中 ----

type clientCall struct {
	req proposeReq
}

type statusCall struct {
	res chan<- Status
}

type proposeReq struct {
	cmd    Command
	isRead bool
	res    chan clientOutcome
}

func hashID(id NodeID) uint32 {
	var h uint32 = 2166136261
	for _, b := range []byte(id) {
		h ^= uint32(b)
		h *= 16777619
	}
	return h
}

// sendOutcome 非阻塞地把结果送回客户端：事件循环绝不能被慢/超时的客户端阻塞。
func sendOutcome(ch chan clientOutcome, o clientOutcome) {
	select {
	case ch <- o:
	default:
	}
}
