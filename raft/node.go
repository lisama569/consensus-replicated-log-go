package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
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
	eventChunkWritten
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
	// aeInflight：每个 peer 是否已有一个 AppendEntries RPC 在途。
	// 慢接收端 + 高频提交时，每个响应都会触发再次广播；不加窗口会向同一个
	// 卡住的对端无限堆积发送 goroutine（在同步模拟传输里表现为大量挂起调用）。
	aeInflight map[NodeID]bool

	// 领导者侧：每个 peer 的分块快照发送会话（仅事件循环访问）。
	snapXfers map[NodeID]*snapshotTransfer
	// 下一个分块传输标识。
	nextTransferID uint64

	// 跟随者侧：进行中的分块快照接收状态（仅事件循环访问；nil 表示没有）。
	incoming *incomingSnapshot
	// chunkEpoch 在开始一份全新临时快照时递增；用于丢弃身份已变更会话的迟到异步落盘结果。
	chunkEpoch uint64
	// logDirty：自上次后台落盘以来日志后缀有变更，等待下一个 tick 合并落盘一次。
	logDirty bool
	// compactBusy/pendingCompact：后台压缩（多次 fsync）单飞。慢盘上压缩可能慢于
	// 压缩触发频率；进行中时只保留“最新一份待压缩”，避免队列被一连串快照任务压满
	// 后反过来卡住事件循环。内存状态始终保持最新，磁盘快照稍后跟上即可。
	compactBusy    bool
	pendingCompact *chunkJob

	// 候选人得票
	grantedVotes map[NodeID]bool

	// Pre-Vote 状态：preVoting=true 时处于预投票探测阶段（尚未递增任期）；
	// preVotesGranted 记录已获得的预投票。追赶中的落后节点用 Pre-Vote 避免扰动领导者。
	preVoting       bool
	preVotesGranted map[NodeID]bool

	// 等待提交的提案：日志索引 -> 全部等待者（重复请求会共用一个等待列表）
	proposals map[Index][]chan clientOutcome

	// appliedSeq 记录每个客户端最近一次已应用的 (seq,index)（幂等快速路径，不持久化）。
	appliedSeq map[string]appliedSeqEntry
	// pendingReads 是领导者在途的线性一致读队列。
	pendingReads []*readState

	electionElapsed int
	// ticksSinceSnapshot 距上次压缩过去的 tick 数（用于压缩节流）。
	ticksSinceSnapshot int
	rng                *rand.Rand

	// ---- 事件循环通信 ----
	events chan internalEvent
	// chunkEvents 专收分块快照块。块是大流量的“数据面”消息，必须与心跳/投票等
	// “控制面”消息分流：否则大量块会占满事件队列，把心跳饿死，导致接收节点在
	// 追赶期间反复选举超时/领导权抖动。事件循环对两个队列做优先级选择。
	chunkEvents chan internalEvent

	// 接收侧分块快照后台落盘队列（每节点一个串行 worker，保证 reset/write/finalize/clear 严格有序）。
	// 接收侧分块落盘队列。快照数据块走高优先通道（chunkJobs），保证慢盘上追赶
	// 不被其它落盘拖死；正式快照/日志等落盘走低优先通道（persistJobs），由单独
	// worker 处理。两条通道各自串行；两类资源（staging 文件 vs 正式快照/日志）
	// 互不相交，因此无需跨通道排序。
	chunkJobs   chan chunkJob
	persistJobs chan chunkJob
	// formalWriteMu 把“正式快照落盘 + 日志后缀重写”绑成不可分割的一段；发送方
	// 读取快照（loadSnapshotForTransfer）取读锁，绝不读到两步之间的中间态。
	formalWriteMu sync.RWMutex
	// chunkIODelay：仅测试用，worker 每处理一个落盘任务前等待该时长（模拟慢盘）。
	chunkIODelay time.Duration

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
	// handlerGen：Bind 到传输层的处理函数代次。崩溃后用同一传输重建节点会重新
	// Bind 一个新处理函数；旧代次处理函数必须立即拒绝，否则发给“已死旧节点”的
	// 在途 RPC 会永远挂起（其发送 goroutine 持有 wg，导致 Stop 无法结束）。
	handlerGen uint64

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
		cfg:         cfg,
		store:       store,
		tr:          tr,
		sm:          NewKVStateMachine(),
		role:        RoleFollower,
		nextIndex:   make(map[NodeID]Index),
		matchIndex:  make(map[NodeID]Index),
		aeInflight:  make(map[NodeID]bool),
		snapXfers:   make(map[NodeID]*snapshotTransfer),
		proposals:   make(map[Index][]chan clientOutcome),
		appliedSeq:  make(map[string]appliedSeqEntry),
		events:      make(chan internalEvent, 256),
		chunkEvents: make(chan internalEvent, 64),
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
		chunkJobs:   make(chan chunkJob, 32),
		persistJobs: make(chan chunkJob, 64),
		logf:        func(string, ...any) {},
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
		// 日志后缀必须紧跟快照点。崩溃可能恰好发生在“新快照已落盘、对应日志
		// 后缀重写尚未完成”之间，留下一个起点超前的旧后缀文件；其条目已被新
		// 快照覆盖，丢弃即可（并在启动时把当前后缀重写回磁盘）。
		want := meta.LastIncludedIndex + 1
		switch {
		case entries[0].Index == want:
			n.log = append(n.log, entries...)
		case entries[0].Index > want:
			n.logDirty = true
		default:
			i := 0
			for i < len(entries) && entries[i].Index <= meta.LastIncludedIndex {
				i++
			}
			n.log = append(n.log, entries[i:]...)
		}
	}

	// 恢复上次未完成的分块快照接收状态（仅用于从断点续传，绝不直接安装）。
	if err := n.restoreIncoming(); err != nil {
		return fmt.Errorf("restore staging snapshot: %w", err)
	}
	return nil
}

// Start 注册 RPC 处理器并启动事件循环与时钟。
func (n *Node) Start() {
	n.handlerGen++
	h := n.makeRPCHandler()
	n.tr.(interface {
		Bind(NodeID, Handler)
	}).Bind(n.cfg.ID, h)
	n.startChunkWorker()
	n.startPersistWorker()
	go n.run()
}

// flushCall 请事件循环同步执行 flushDurability（停止/崩溃前保证已提交数据落盘）。
type flushCall struct{ done chan struct{} }

// Stop 优雅停止：先在事件循环中刷盘（保证此前已提交数据 fsync），再关闭事件循环
// 并等待后台 goroutine 退出。
func (n *Node) Stop() error {
	n.stopOnce.Do(func() {
		// 同步刷盘：即使控制面队列暂时被占满也要把 flush 投进去（阻塞投递），
		// 否则可能在仍有已提交日志未落盘时退出。事件循环一定会处理 flushCall，
		// 或在节点从未启动时 doneCh 已关闭，二者都保证这里能返回。
		done := make(chan struct{})
		select {
		case n.events <- internalEvent{kind: eventRPC, rpc: flushCall{done: done}}:
			<-done
		case <-n.doneCh:
		}
		close(n.stopCh)
	})
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

// catchupCall 在事件循环内查询/控制本机快照追赶状态。
type catchupCall struct {
	op   catchupOp
	res  chan CatchUpProgress
	done chan error
}

type catchupOp int

const (
	catchupProgress catchupOp = iota
	catchupCancel
	catchupResume
)

// transfersCall 查询领导者侧各 peer 的快照传输进度。
type transfersCall struct {
	res chan map[NodeID]CatchUpProgress
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
