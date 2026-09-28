package raft

// 事件循环：节点所有可变状态只在 run() 一个 goroutine 中被修改。
// 网络层回调、时钟、客户端请求都通过 events channel 串行化。

import "time"

// rpcHandler 由网络层在发送方 goroutine 中调用：
// 把 RPC 转成事件投递给事件循环，同步等待串行处理结果。
// rpcHandler 工厂：每次 Start 用新的代次生成处理函数。传输层只会保留最近一次
// Bind 的处理函数，但旧处理函数可能正被在途发送方持有并调用；它们用代次自检，
// 发现自己已被替换就立即返回 nil，绝不挂住发送方。
func (n *Node) makeRPCHandler() Handler {
	gen := n.handlerGen
	return func(from NodeID, req any) any {
		if gen != n.handlerGen {
			return nil // 已被重启后的新节点取代
		}
		ch := make(chan any, 1)
		ev := internalEvent{kind: eventRPC, from: from, rpc: req, resp: ch}
		// 分块快照块走独立的数据面队列，避免淹没心跳/投票/客户端等控制面消息。
		q := n.events
		if _, ok := req.(*InstallSnapshotRequest); ok {
			q = n.chunkEvents
		}
		select {
		case q <- ev:
		case <-n.stopCh:
			return nil
		}
		select {
		case r := <-ch:
			return r
		case <-n.stopCh:
			return nil
		}
	}
}

func (n *Node) rpcHandler(from NodeID, req any) any {
	return n.makeRPCHandler()(from, req)
}

func (n *Node) run() {
	defer close(n.doneCh)
	n.resetElectionTimer()

	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ticker.C:
				select {
				case n.events <- internalEvent{kind: eventTick}:
				case <-n.doneCh:
					return
				case <-n.stopCh:
					return
				}
			case <-n.doneCh:
				return
			case <-n.stopCh:
				return
			}
		}
	}()

	// 控制面（events：tick/心跳/投票/客户端/内部结果）优先于数据面（chunkEvents）。
	// 为避免控制消息 trickle 时块被永久饿死，每次“控制面恰好为空”时批量处理
	// 至多 chunkBurst 个块，再回头重新优先检查控制面。
	const chunkBurst = 16
	for {
		// 先非阻塞排空所有已就绪的控制面事件。
		drained := 0
		for drained < chunkBurst {
			select {
			case <-n.stopCh:
				return
			case ev := <-n.events:
				n.handleEvent(ev)
				drained++
			default:
				drained = chunkBurst // 控制面暂时为空
			}
		}
		// 控制面为空：阻塞等待下一条消息；若有块则最多连续处理 chunkBurst 个。
		select {
		case <-n.stopCh:
			return
		case ev := <-n.events:
			n.handleEvent(ev)
		case ev := <-n.chunkEvents:
			n.handleEvent(ev)
			for i := 1; i < chunkBurst; i++ {
				select {
				case <-n.stopCh:
					return
				case cev := <-n.events: // 块处理期间到达的控制面立即抢占
					n.handleEvent(cev)
					i = chunkBurst
				case cev := <-n.chunkEvents:
					n.handleEvent(cev)
				default:
					i = chunkBurst
				}
			}
		}
	}
}

func (n *Node) handleEvent(ev internalEvent) {
	switch ev.kind {
	case eventTick:
		n.tick()
	case eventRPC:
		n.dispatch(ev)
	case eventChunkWritten:
		if r, ok := ev.rpc.(chunkJobResult); ok {
			n.handleChunkJobResult(r)
		}
	}
}

func (n *Node) dispatch(ev internalEvent) {
	switch r := ev.rpc.(type) {
	// ---- 入站 Raft RPC ----
	case *AppendEntriesRequest:
		ev.resp <- n.handleAppendEntries(r)
	case *RequestVoteRequest:
		ev.resp <- n.handleRequestVote(r)
	case *InstallSnapshotRequest:
		if r.Chunked {
			ev.resp <- n.handleChunkedInstallSnapshot(r)
		} else {
			ev.resp <- n.handleInstallSnapshot(r)
		}

	// ---- 出站 RPC 的异步响应 ----
	case voteResponse:
		n.handleVoteResponse(r.peer, r.resp)
	case appendResponse:
		n.handleAppendResponse(r.peer, r.resp)
	case aeTimeout:
		n.aeInflight[r.peer] = false
	case snapshotResponse:
		n.handleSnapshotResponse(r.peer, r.resp)

	// ---- 节点内调用 ----
	case clientCall:
		n.handleClientCall(r.req)
	case statusCall:
		r.res <- n.snapshotStatus()
	case catchupCall:
		switch r.op {
		case catchupProgress:
			r.res <- n.catchUpProgressLocked()
		case catchupCancel:
			r.done <- n.cancelCatchUpLocked()
		case catchupResume:
			r.done <- n.resumeCatchUpLocked()
		}
	case transfersCall:
		r.res <- n.snapshotTransfersLocked()
	case chunkTimeout:
		// 某在途块请求/响应丢失：从窗口在途表移除该偏移，下一次 transmit/tick
		// 会按 durable ACK 的窗口基址重新补发，不需要调用方做额外处理。
		if tr := n.snapXfers[r.peer]; tr != nil {
			delete(tr.inflight, r.offset)
		}
	case thresholdCall:
		n.cfg.SnapshotThreshold = r.threshold
		r.done <- struct{}{}
	case chunkSizeCall:
		if r.bytes > 0 {
			n.cfg.SnapshotChunkSize = r.bytes
		}
		r.done <- struct{}{}
	case logRangeCall:
		r.res <- indexRange{snap: n.snapMeta.LastIncludedIndex, last: n.lastLogIndex()}
	case ioDelayCall:
		n.chunkIODelay = r.d
		r.done <- struct{}{}
	case quietCall:
		r.res <- n.catchUpProgressQuiet()
	case flushCall:
		n.flushDurability()
		close(r.done)
	}
}

func (n *Node) tick() {
	n.electionElapsed++
	n.ticksSinceSnapshot++
	// 每个 tick 把累积的日志变更合并落盘一次（领导者与跟随者都可能追加日志）。
	n.flushDirtyLog()
	switch n.role {
	case RoleLeader:
		// 每个时钟周期发送心跳（force：即使上一条 AE 在途也发，保证心跳/窗口
		// 在丢包或旧领导者崩溃后能自愈，不会因在途标记卡死某个 peer）。
		n.broadcastHeartbeat()
		n.tickSnapshotTransfers()
	default:
		timeout := n.electionTimeout()
		if n.electionElapsed >= timeout {
			n.electionElapsed = 0
			n.startElection()
		}
	}
}

func (n *Node) electionTimeout() int {
	span := n.cfg.ElectionMax - n.cfg.ElectionMin + 1
	return n.cfg.ElectionMin + n.rng.Intn(span)
}

func (n *Node) resetElectionTimer() { n.electionElapsed = 0 }

func (n *Node) becomeFollower(t Term) {
	if t > n.term {
		n.term = t
		n.votedFor = ""
		n.persistState()
	}
	n.role = RoleFollower
	n.leaderID = ""
	n.grantedVotes = nil
	n.preVoting = false
	n.preVotesGranted = nil
	// 失去领导权：全部在途分块发送会话作废（成为新领导者/重新成为领导者后重建）。
	n.snapXfers = make(map[NodeID]*snapshotTransfer)
	n.resetElectionTimer()
}

func (n *Node) becomeCandidate() {
	n.term++
	n.role = RoleCandidate
	n.votedFor = n.cfg.ID
	n.persistState()
	n.grantedVotes = map[NodeID]bool{n.cfg.ID: true}
	n.preVoting = false
	n.preVotesGranted = nil
	n.leaderID = ""
	n.resetElectionTimer()
	n.debugf("became candidate, requesting votes from peers")
	n.requestVotes()
}

func (n *Node) becomeLeader() {
	n.role = RoleLeader
	n.leaderID = n.cfg.ID
	n.resetElectionTimer()

	// 初始化每个 peer 的 next/match：
	// nextIndex 乐观地从自己最后一条之后开始，冲突时通过拒绝信息快速回退。
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		n.nextIndex[p] = n.lastLogIndex() + 1
		n.matchIndex[p] = 0
	}
	// 全新领导任期：旧的分块快照会话全部失效（becomeFollower 已清空，
	// 这里也兜底重建，避免向已清空的 map 写入）。
	n.snapXfers = make(map[NodeID]*snapshotTransfer)
	n.aeInflight = make(map[NodeID]bool)
	n.debugf("became leader for term %d", n.term)

	// 上任空日志：提交后即可证明本任期领导权（供线性一致读使用）。
	n.appendClientEntry(Command{Op: OpNoop}, nil)
	n.broadcastAppendEntries()
}

// snapshotStatus 收集当前状态（仅事件循环调用）。
func (n *Node) snapshotStatus() Status {
	peers := make([]NodeID, len(n.cfg.Peers))
	copy(peers, n.cfg.Peers)
	return Status{
		ID:            n.cfg.ID,
		Role:          n.role,
		Term:          n.term,
		LeaderID:      n.leaderID,
		CommitIndex:   n.commitIndex,
		LastLogIndex:  n.lastLogIndex(),
		LastLogTerm:   n.lastLogTerm(),
		VotedFor:      n.votedFor,
		AppliedIndex:  n.lastApplied,
		SnapshotIndex: n.snapMeta.LastIncludedIndex,
		Peers:         peers,
		CatchUp:       n.catchUpProgressLocked(),
	}
}
