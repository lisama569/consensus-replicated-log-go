package raft

// 事件循环：节点所有可变状态只在 run() 一个 goroutine 中被修改。
// 网络层回调、时钟、客户端请求都通过 events channel 串行化。

import "time"

// rpcHandler 由网络层在发送方 goroutine 中调用：
// 把 RPC 转成事件投递给事件循环，同步等待串行处理结果。
func (n *Node) rpcHandler(from NodeID, req any) any {
	ch := make(chan any, 1)
	ev := internalEvent{kind: eventRPC, from: from, rpc: req, resp: ch}
	select {
	case n.events <- ev:
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

	for {
		select {
		case <-n.stopCh:
			return
		case ev := <-n.events:
			n.handleEvent(ev)
		}
	}
}

func (n *Node) handleEvent(ev internalEvent) {
	switch ev.kind {
	case eventTick:
		n.tick()
	case eventRPC:
		n.dispatch(ev)
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
		// 分块请求/响应丢失：立即按断点重传当前块（transmitChunk 会递增
		// attempt，因此即使“丢失”的响应稍后才到，也会因 attempt 不符被丢弃）。
		if tr := n.snapXfers[r.peer]; tr != nil && tr.inflight && !tr.done && tr.backoff == 0 {
			tr.inflight = false
			tr.elapsed = 0
			n.transmitChunk(r.peer, tr)
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
	}
}

func (n *Node) tick() {
	n.electionElapsed++
	switch n.role {
	case RoleLeader:
		// 每个时钟周期发送心跳。
		n.broadcastAppendEntries()
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
	// 失去领导权：全部在途分块发送会话作废（成为新领导者/重新成为领导者后重建）。
	n.snapXfers = make(map[NodeID]*snapshotTransfer)
	n.knownEpoch = make(map[NodeID]uint64)
	n.knownEpochOK = make(map[NodeID]bool)
	n.resetElectionTimer()
}

func (n *Node) becomeCandidate() {
	n.term++
	n.role = RoleCandidate
	n.votedFor = n.cfg.ID
	n.persistState()
	n.grantedVotes = map[NodeID]bool{n.cfg.ID: true}
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
	n.knownEpoch = make(map[NodeID]uint64)
	n.knownEpochOK = make(map[NodeID]bool)
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
