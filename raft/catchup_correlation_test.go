package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// scriptedPeer 是脚本化的假对端：投票一律同意；AppendEntries / InstallSnapshot
// 的应答由测试回调决定，用于确定性地构造“旧会话迟到响应 / attempt 错配 /
// 同身份 resync 断点续传”等难以靠时序复现的场景。
func bindScriptedPeer(id NodeID, tr *MemoryTransport,
	onAppend func(*AppendEntriesRequest) *AppendEntriesResponse,
	onSnapshot func(*InstallSnapshotRequest) *InstallSnapshotResponse,
) {
	tr.Bind(id, func(from NodeID, req any) any {
		switch r := req.(type) {
		case *RequestVoteRequest:
			return &RequestVoteResponse{Term: r.Term, VoteGranted: true, Peer: id}
		case *AppendEntriesRequest:
			if onAppend != nil {
				return onAppend(r)
			}
			return &AppendEntriesResponse{Term: r.Term, Peer: id}
		case *InstallSnapshotRequest:
			if onSnapshot != nil {
				return onSnapshot(r)
			}
			return &InstallSnapshotResponse{Term: r.Term, Peer: id}
		}
		return nil
	})
}

type scriptedChunk struct {
	transfer uint64
	attempt  uint64
	offset   uint64
	length   uint64
	snapIdx  Index
}

// TestSnapshotTransferResponseCorrelation：发送侧「会话 + 发送序号」相关性与
// 同身份 resync 从 AckOffset 断点续传的确定性测试。
func TestSnapshotTransferResponseCorrelation(t *testing.T) {
	tr := NewMemoryTransport()
	const l, p, q NodeID = "l", "p", "q"

	var maxQEnd atomic.Int64 // q 作为完美跟随者：收到的 AE 末端（观察提交推进用）
	bindScriptedPeer(q, tr,
		func(r *AppendEntriesRequest) *AppendEntriesResponse {
			end := int64(r.PrevLogIndex) + int64(len(r.Entries))
			for {
				cur := maxQEnd.Load()
				if end <= cur || maxQEnd.CompareAndSwap(cur, end) {
					break
				}
			}
			return &AppendEntriesResponse{
				Term: r.Term, Success: true, Peer: q, PeerLastIndex: Index(end),
			}
		},
		nil,
	)

	var maxPEnd atomic.Int64
	rejectAppend := atomic.Bool{}
	rejectAppend.Store(true)
	bindScriptedPeer(p, tr,
		func(r *AppendEntriesRequest) *AppendEntriesResponse {
			if rejectAppend.Load() {
				// 拒绝并报 XIndex=1：把 nextIndex 夹到快照点，强制走分块快照。
				return &AppendEntriesResponse{Term: r.Term, Success: false, Peer: p, XIndex: 1}
			}
			end := int64(r.PrevLogIndex) + int64(len(r.Entries))
			for {
				cur := maxPEnd.Load()
				if end <= cur || maxPEnd.CompareAndSwap(cur, end) {
					break
				}
			}
			return &AppendEntriesResponse{
				Term: r.Term, Success: true, Peer: p, PeerLastIndex: Index(end),
			}
		},
		nil, // 快照回调在下方安装（需要引用 seen 等状态）
	)

	cfg := DefaultConfig()
	cfg.ID = l
	cfg.Peers = []NodeID{l, p, q}
	cfg.DataDir = ""
	cfg.RandSeed = 11
	cfg.SnapshotThreshold = 4
	cfg.SnapshotChunkSize = 32
	cfg.SnapshotRetryTicks = 3 // 30ms 超时重传，加快测试
	leader, err := NewNode(cfg, tr)
	if err != nil {
		t.Fatal(err)
	}
	leader.Start()
	t.Cleanup(func() { _ = leader.Stop() })

	waitForDur(t, 3*time.Second, func() bool { return leader.Status().Role == RoleLeader }, "leader elected")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 持续写入：整个测试期间领导者不断提交、不断压缩出更新的快照。
	const writes = 200
	writerDone := make(chan error, 1)
	go func() {
		for i := 0; i < writes; i++ {
			if _, err := leader.SubmitProposal(ctx, corrCmd(i)); err != nil {
				writerDone <- err
				return
			}
		}
		writerDone <- nil
	}()

	var seenMu sync.Mutex
	var seen []scriptedChunk

	var mu sync.Mutex
	var s1id uint64
	var s1idx Index
	var s1len uint64
	var seekAck uint64 // 异常 C 回报的对端 durable 偏移
	// phase：small（小快照直接确认）-> anomalyA -> anomalyB -> anomalyC -> finish
	phase := "small"
	bAnsweredAttempt := uint64(0) // 异常 B 已应答的 attempt，重复重传不能再触发
	cAnsweredAttempt := uint64(0)

	snapshotFn := func(r *InstallSnapshotRequest) *InstallSnapshotResponse {
		seenMu.Lock()
		seen = append(seen, scriptedChunk{r.TransferID, r.Attempt, r.Offset, r.Length, r.LastIncludedIndex})
		seenMu.Unlock()

		mu.Lock()
		defer mu.Unlock()

		if phase == "small" {
			if r.Length <= 128 {
				return chunkAck(r, false)
			}
			// 第一份“足够大”的快照即 S1：注入异常 A（旧 TransferID 的响应）。
			phase = "anomalyA"
			s1id, s1idx, s1len = r.TransferID, r.LastIncludedIndex, r.Length
			return &InstallSnapshotResponse{
				Term: r.Term, Peer: r.LeaderID, Chunked: true,
				TransferID: r.TransferID - 1, Attempt: r.Attempt,
				Success: false, Reason: RejectResync,
				LastIncluded: r.LastIncludedIndex,
			}
		}

		// S1 未完成前会话必须钉住：任何新 TransferID 都说明钉住失效。
		if phase != "finish" && r.TransferID != s1id {
			return chunkAck(r, true)
		}
		if phase == "finish" {
			return chunkAck(r, true)
		}

		switch phase {
		case "anomalyA":
			// 异常 A 之后：等首块的超时重传（attempt>=2）。
			if r.Offset != 0 || r.Attempt < 2 || bAnsweredAttempt == r.Attempt {
				return chunkAck(r, false)
			}
			// 异常 B：回一个 attempt 对不上的“成功 ACK”，必须被发送方忽略。
			phase = "anomalyB"
			bAnsweredAttempt = r.Attempt
			return &InstallSnapshotResponse{
				Term: r.Term, Peer: r.LeaderID, Chunked: true,
				TransferID: r.TransferID, Attempt: r.Attempt - 1,
				Success: true, AckOffset: uint64(cfg.SnapshotChunkSize),
				LastIncluded: r.LastIncludedIndex,
			}
		case "anomalyB":
			// 异常 B 之后到达的下一个请求：异常 C——同身份 resync，
			// 断点报成当前偏移 +2 块（不超过总长）。
			if cAnsweredAttempt == r.Attempt && r.Offset != 0 {
				return chunkAck(r, false)
			}
			phase = "anomalyC"
			cAnsweredAttempt = r.Attempt
			seekAck = r.Offset + 2*uint64(cfg.SnapshotChunkSize)
			if seekAck > r.Length {
				seekAck = r.Length
			}
			return &InstallSnapshotResponse{
				Term: r.Term, Peer: r.LeaderID, Chunked: true,
				TransferID: r.TransferID, Attempt: r.Attempt,
				Success: false, Reason: RejectResync,
				LastIncluded: r.LastIncludedIndex, AckOffset: seekAck,
			}
		case "anomalyC":
			// seek 完成：之后所有块正常确认，S1 随即完成。
			phase = "finish"
			return chunkAck(r, true)
		}
		return chunkAck(r, true)
	}
	tr.Bind(p, func(from NodeID, req any) any {
		// 复用已注册的处理器：直接替换绑定（本测试单线程脚本化，安全）。
		switch rq := req.(type) {
		case *InstallSnapshotRequest:
			return snapshotFn(rq)
		case *AppendEntriesRequest:
			if rejectAppend.Load() {
				return &AppendEntriesResponse{Term: rq.Term, Success: false, Peer: p, XIndex: 1}
			}
			end := int64(rq.PrevLogIndex) + int64(len(rq.Entries))
			for {
				cur := maxPEnd.Load()
				if end <= cur || maxPEnd.CompareAndSwap(cur, end) {
					break
				}
			}
			return &AppendEntriesResponse{Term: rq.Term, Success: true, Peer: p, PeerLastIndex: Index(end)}
		case *RequestVoteRequest:
			return &RequestVoteResponse{Term: rq.Term, VoteGranted: true, Peer: p}
		}
		return nil
	})

	// 等三个异常都送达（phase 推进到 anomalyC），并取走本次 resync 的目标断点。
	var s1idCopy, seekCopy uint64
	waitForDur(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		if phase == "anomalyC" || phase == "finish" {
			s1idCopy, seekCopy = s1id, seekAck
			return true
		}
		return false
	}, "all three scripted anomalies delivered")

	// 再等领导者真正从 seekAck 续传（seen 中出现偏移 == seekAck 的请求）。
	// resync 响应处理在事件循环内直接 seek 并 transmit，轮询 seen 即可。
	waitForDur(t, 5*time.Second, func() bool {
		seenMu.Lock()
		defer seenMu.Unlock()
		for _, c := range seen {
			if c.transfer == s1idCopy && c.offset == seekCopy {
				return true
			}
		}
		return false
	}, "leader retransmits from the resync ack offset")

	// 断言 1：S1 会话在持续压缩中保持钉住——S1 相关的每个请求身份完全一致。
	seenMu.Lock()
	{
		var offs []string
		for _, c0 := range seen {
			if c0.transfer == s1idCopy {
				offs = append(offs, fmt.Sprintf("%d(a%d)", c0.offset, c0.attempt))
			}
		}
		t.Logf("S1 chunks seen: %v", offs)
	}
	var maxAttemptS1 uint64
	sessionIDs := map[uint64]int{}
	for _, c := range seen {
		sessionIDs[c.transfer]++
		if c.transfer == s1id {
			if c.snapIdx != s1idx || c.length != s1len {
				seenMu.Unlock()
				t.Fatalf("pinned session identity changed mid-transfer: "+
					"got idx=%d len=%d want idx=%d len=%d", c.snapIdx, c.length, s1idx, s1len)
			}
			if c.attempt > maxAttemptS1 {
				maxAttemptS1 = c.attempt
			}
		}
	}
	seenMu.Unlock()
	if maxAttemptS1 < 3 {
		t.Fatalf("stale responses must be ignored and trigger retransmits, max attempt=%d", maxAttemptS1)
	}
	t.Logf("s1 pinned: id=%d idx=%d bytes=%d maxAttempt=%d sessionsSoFar=%d seek=%d",
		s1id, s1idx, s1len, maxAttemptS1, len(sessionIDs), seekAck)

	if err := <-writerDone; err != nil {
		t.Fatalf("writer: %v", err)
	}

	// 等领导者把全部写入复制到 q 并压缩静止。
	waitForDur(t, 5*time.Second, func() bool {
		s := leader.Status()
		return Index(maxQEnd.Load()) >= s.LastLogIndex
	}, "all entries replicated to q")

	// 切换 p：接受后续 AE（快照继续秒确认，兼容领导端可能又推进了的快照）。
	tr.Bind(p, func(from NodeID, req any) any {
		switch rq := req.(type) {
		case *InstallSnapshotRequest:
			return chunkAck(rq, true)
		case *AppendEntriesRequest:
			end := int64(rq.PrevLogIndex) + int64(len(rq.Entries))
			for {
				cur := maxPEnd.Load()
				if end <= cur || maxPEnd.CompareAndSwap(cur, end) {
					break
				}
			}
			return &AppendEntriesResponse{Term: rq.Term, Success: true, Peer: p, PeerLastIndex: Index(end)}
		case *RequestVoteRequest:
			return &RequestVoteResponse{Term: rq.Term, VoteGranted: true, Peer: p}
		}
		return nil
	})
	rejectAppend.Store(false)

	// p 必须靠常规 AE 追到领导者日志末端（修复前会卡在“快照点已过但 AE 永远拒绝”）。
	waitForDur(t, 5*time.Second, func() bool {
		return Index(maxPEnd.Load()) >= leader.Status().LastLogIndex
	}, "peer p catches log end by AppendEntries after snapshot install")

	// 再持续写入并压缩，p 的 AE 高水位必须持续追到领导者日志末端，绝不空转。
	endBefore := Index(maxPEnd.Load())
	for i := writes; i < writes+30; i++ {
		if _, err := leader.SubmitProposal(ctx, corrCmd(i)); err != nil {
			t.Fatal(err)
		}
	}
	waitForDur(t, 5*time.Second, func() bool {
		return Index(maxPEnd.Load()) >= leader.Status().LastLogIndex &&
			Index(maxQEnd.Load()) >= leader.Status().LastLogIndex
	}, "p and q follow new writes by log replication")

	if Index(maxPEnd.Load()) < endBefore {
		t.Fatalf("p replication high-water mark regressed: %d -> %d",
			endBefore, maxPEnd.Load())
	}
	// 持续压缩下脚本化对端会被反复安装更新的快照。关键不是“永远不再有快照”，
	// 而是“每次快照都能完成并立刻把 p 推到日志末端、不卡在 syncing 空转”。
	// 静止一拍后 p 的 AE 高水位必须等于领导者日志末端，且不存在进行中的快照会话。
	waitForDur(t, 5*time.Second, func() bool {
		if Index(maxPEnd.Load()) < leader.Status().LastLogIndex {
			return false
		}
		xp := leader.SnapshotTransfers()[p]
		return xp.State != CatchUpSyncing
	}, "p ends quiescent at log end with no in-flight snapshot")
}

func corrCmd(i int) Command {
	return Command{
		Op:       OpSet,
		Key:      fmt.Sprintf("corr-%04d", i),
		Value:    fmt.Sprintf("value-%010d-padding-padding", i),
		ClientID: "corr",
		Seq:      uint64(i) + 1,
	}
}

func chunkAck(r *InstallSnapshotRequest, complete bool) *InstallSnapshotResponse {
	return &InstallSnapshotResponse{
		Term: r.Term, Peer: r.LeaderID, Chunked: true,
		TransferID:   r.TransferID,
		Attempt:      r.Attempt,
		Success:      true,
		Complete:     complete,
		AckOffset:    r.Offset + uint64(len(r.Data)),
		LastIncluded: r.LastIncludedIndex,
	}
}

// TestCancelResumeEpochInvalidatesStaleSuccess：确定性验证取消语义——
// 取消前已经在途的“成功/完成”响应（旧代际 epoch=0）在 ResumeCatchUp 之后
// 才到达时，绝不能被领导者悄悄当成恢复后的成功；它必须被丢弃，领导者继续
// 重发并等到真正属于新代际的响应后才完成。
func TestCancelResumeEpochInvalidatesStaleSuccess(t *testing.T) {
	tr := NewMemoryTransport()
	const l, p, q NodeID = "l3", "p3", "q3"

	bindScriptedPeer(q, tr,
		func(r *AppendEntriesRequest) *AppendEntriesResponse {
			return &AppendEntriesResponse{Term: r.Term, Success: true, Peer: q,
				PeerLastIndex: r.PrevLogIndex + Index(len(r.Entries))}
		}, nil)

	cfg := DefaultConfig()
	cfg.ID = l
	cfg.Peers = []NodeID{l, p, q}
	cfg.DataDir = ""
	cfg.RandSeed = 77
	cfg.SnapshotThreshold = 4
	cfg.SnapshotChunkSize = 32
	cfg.SnapshotRetryTicks = 2
	leader, err := NewNode(cfg, tr)
	if err != nil {
		t.Fatal(err)
	}
	leader.Start()
	t.Cleanup(func() { _ = leader.Stop() })
	waitForDur(t, 3*time.Second, func() bool { return leader.Status().Role == RoleLeader }, "leader elected")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := 0; i < 40; i++ {
		if _, err := leader.SubmitProposal(ctx, corrCmd(500+i)); err != nil {
			t.Fatal(err)
		}
	}
	waitForDur(t, 3*time.Second, func() bool {
		return leader.Status().SnapshotIndex >= 20
	}, "leader compacts a snapshot")

	// 真实节点 p（仅用于 CancelCatchUp/ResumeCatchUp/进度查询），但其快照 RPC
	// 由测试脚本接管，以精确控制响应时序。
	pcfg := DefaultConfig()
	pcfg.ID = p
	pcfg.Peers = cfg.Peers
	pcfg.DataDir = ""
	pcfg.RandSeed = 78
	pcfg.SnapshotThreshold = 4
	pcfg.SnapshotChunkSize = 32
	follower, err := NewNode(pcfg, tr)
	if err != nil {
		t.Fatal(err)
	}

	// 阶段控制（必须在 follower.Start 前装好脚本：Start 会绑定 p 的真实处理器，
	// 这里在 Start 之后立刻替换；先构造好脚本，替换与启动之间不主动让出调度）：
	//  hold     —— 放第一个块（建立 syncing 并落盘），之后持续丢块把 p 稳在中途；
	//  observe  —— 放行，取消后让真实节点把 cancelled（新 epoch）回报给领导者；
	//  stale    —— 第一个请求回 epoch=0 的“伪完成”，随后立即转 normal（真实处理）；
	//  normal   —— 正常转发给真实节点处理。
	var phase atomic.Value
	phase.Store("hold")
	var staleServed atomic.Bool

	realHandler := Handler(func(from NodeID, req any) any { return follower.rpcHandler(from, req) })
	script := func(from NodeID, req any) any {
		rq, ok := req.(*InstallSnapshotRequest)
		if !ok {
			return realHandler(from, req)
		}
		if phase.Load() == "hold" {
			// 确定性地只放前两块（offset 0、chunkSize），其余（含其重传）全丢：
			// follower 必然进入 syncing 且收到 2*chunkSize 字节但远未传完，
			// 不依赖任何时序或对 follower 状态的读取。
			if rq.Offset <= uint64(cfg.SnapshotChunkSize) {
				return realHandler(from, req)
			}
			return nil
		}
		// stale 阶段只对“恢复后发出的第一个块”（offset=0）注入旧代际伪完成；
		// 其余请求正常处理，避免持续伪响应把会话拖入无尽重试。
		if phase.Load() == "stale" && rq.Offset == 0 && !staleServed.Swap(true) {
			return &InstallSnapshotResponse{
				Term: rq.Term, Peer: p, Chunked: true,
				TransferID: rq.TransferID, Attempt: rq.Attempt,
				Epoch:   0, // 取消前的旧代际（真实节点此刻 epoch 已 >=1）
				Success: true, Complete: true,
				AckOffset: rq.Length, LastIncluded: rq.LastIncludedIndex,
			}
		}
		return realHandler(from, req)
	}

	// 在 follower 启动前先把脚本绑定到 p：follower.Start() 内部会再绑定一次
	// 真实处理器并覆盖脚本，因此启动后必须立刻重新绑定脚本；两次绑定之间到达的
	// 块走真实节点（首块会正常建立 syncing，后续块即使漏掉也由超时重传补回，
	// 最终状态仍被脚本的 hold 规则稳定在 syncing 中途）。
	tr.Bind(p, script)
	follower.Start()
	t.Cleanup(func() { _ = follower.Stop() })
	tr.Bind(p, script)

	waitForDur(t, 10*time.Second, func() bool {
		st := follower.CatchUpProgress()
		return st.State == CatchUpSyncing && st.Received >= uint64(2*cfg.SnapshotChunkSize)
	}, "p held mid-transfer in syncing")

	if err := follower.CancelCatchUp(); err != nil {
		t.Fatal(err)
	}
	// 放行：领导者的重发被真实节点以 cancelled（携带新 epoch）拒绝。
	phase.Store("observe")
	waitForDur(t, 5*time.Second, func() bool {
		x, ok := leader.SnapshotTransfers()[p]
		return ok && x.State == CatchUpCancelled
	}, "leader observes cancelled and learns the new epoch")
	waitProgressState(t, follower, CatchUpCancelled, "p reports cancelled")

	if err := follower.ResumeCatchUp(); err != nil {
		t.Fatal(err)
	}
	// 恢复后第一个 offset=0 的块注入旧代际伪完成。
	phase.Store("stale")

	// 给领导者几轮重发，让伪完成被消费。
	waitForDur(t, 3*time.Second, func() bool { return staleServed.Load() }, "stale response delivered")

	// 关键不变式：旧代际伪完成绝不能让领导者把会话标记为 caught_up。
	pr := leader.SnapshotTransfers()[p]
	if pr.State == CatchUpCaughtUp {
		t.Fatalf("stale pre-cancel success was silently accepted: %+v", pr)
	}

	// 伪完成只发一次，stale 阶段其余请求已按真实处理；转 normal 后全速完成。
	phase.Store("normal")
	waitForDur(t, 10*time.Second, func() bool {
		return follower.CatchUpProgress().State == CatchUpCaughtUp
	}, "p catches up with genuine new-epoch responses")
	waitForDur(t, 5*time.Second, func() bool {
		x, ok := leader.SnapshotTransfers()[p]
		return ok && x.State == CatchUpCaughtUp
	}, "leader eventually sees caught_up from new epoch")
}

// TestCatchupEpochSurvivesRestart：接收代际必须跨重启保留——
// 取消过追赶的跟随者崩溃重启后，其 epoch 不回 0；旧代际的“伪完成”
// 由领导者侧仍然记得的 epoch 作废，而真实新会话最终能正常完成。
// （使用真实持久化目录 + 真实集群，直接验证状态落盘。）
func TestCatchupEpochSurvivesRestart(t *testing.T) {
	c := newTestCluster(t, 3, true, 424242)
	for _, id := range c.IDs() {
		c.SetSnapshotThreshold(id, 4)
		c.SetSnapshotChunkSize(id, 48)
		// laggard 用更长的选举超时，避免它崩溃重启后短暂失联就自己参选换任期。
		cfg := c.cfgs[id]
		cfg.ElectionMin, cfg.ElectionMax = 100, 200
		c.cfgs[id] = cfg
	}
	restartAllWithConfig(t, c)
	waitForDur(t, 5*time.Second, func() bool { return c.Leader() != "" }, "leader")
	leader := c.Leader()

	var laggard NodeID
	for _, id := range c.IDs() {
		if id != leader {
			laggard = id
			break
		}
	}
	if err := c.Crash(laggard); err != nil {
		t.Fatal(err)
	}
	cli := NewClient("epoch-restart", nodesOf(c)...)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writeBulk(t, cli, ctx, 40)
	waitForDur(t, 8*time.Second, func() bool { return c.SnapshotExists(leader) },
		"leader compacted")

	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	ln := c.Node(laggard)
	waitForDur(t, 8*time.Second, func() bool {
		return ln.CatchUpProgress().State == CatchUpSyncing
	}, "laggard starts syncing")
	if err := ln.CancelCatchUp(); err != nil {
		t.Fatal(err)
	}
	waitProgressState(t, ln, CatchUpCancelled, "cancelled")
	if err := ln.ResumeCatchUp(); err != nil {
		t.Fatal(err)
	}
	// 立即崩溃重启：epoch=1 必须随硬状态落盘并被恢复。
	if err := c.Crash(laggard); err != nil {
		t.Fatal(err)
	}
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	ln = c.Node(laggard)

	// 重启后到追赶完成这段时间，持续让 laggard 能收到领导者心跳（避免它因
	// 选举超时自己参选换任期，那样会合法重置领导者侧 epoch 记忆、绕过本用例
	// 要验证的“同一领导者跨重启仍认得持久化 epoch”）。快照块照常由它处理。
	// 等待期内确认：同一个领导者任内，跟随者正常完成安装。
	leaderBefore := c.Leader()
	waitForDur(t, 15*time.Second, func() bool {
		if c.Leader() != leaderBefore {
			return false // 领导者必须保持不变
		}
		return ln.CatchUpProgress().State == CatchUpCaughtUp
	}, "restarted laggard installs the snapshot under the same leader (epoch durable)")

	waitForDur(t, 10*time.Second, func() bool {
		lid := c.Leader()
		if lid == "" {
			return false
		}
		lc := c.Node(lid).Status().CommitIndex
		s := ln.Status()
		return s.CommitIndex >= lc && s.AppliedIndex >= lc
	}, "restarted laggard catches leader commit")
	for i := 0; i < 40; i++ {
		k, v := bulkKV(i)
		if got, ok := ln.sm.Lookup(k); !ok || got != v {
			t.Fatalf("after restart missing %s=%q ok=%v", k, got, ok)
		}
	}
}
