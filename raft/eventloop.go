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
		ev.resp <- n.handleInstallSnapshot(r)
	case *InstallSnapshotChunkRequest:
		ev.resp <- n.handleInstallSnapshotChunk(r)

	// ---- 出站 RPC 的异步响应 ----
	case voteResponse:
		n.handleVoteResponse(r.peer, r.resp)
	case appendResponse:
		n.handleAppendResponse(r.peer, r.resp)
	case snapshotResponse:
		// 快照发送结果复用 AppendEntries 的推进逻辑。
		if r.resp.success {
			n.matchIndex[r.peer] = r.resp.lastIncluded
			n.nextIndex[r.peer] = r.resp.lastIncluded + 1
			n.broadcastAppendEntries()
		} else if r.resp.term > n.term {
			n.becomeFollower(r.resp.term)
		}
	case snapshotChunkResponse:
		n.handleSnapshotChunkResponse(r.peer, r.sessionID, r.resp)

	// ---- 节点内调用 ----
	case clientCall:
		n.handleClientCall(r.req)
	case statusCall:
		r.res <- n.snapshotStatus()
	case thresholdCall:
		n.cfg.SnapshotThreshold = r.threshold
		r.done <- struct{}{}
	case chunkSizeCall:
		n.cfg.SnapshotChunkSize = r.size
		r.done <- struct{}{}
	case logRangeCall:
		r.res <- indexRange{snap: n.snapMeta.LastIncludedIndex, last: n.lastLogIndex()}
	case catchupStatusCall:
		r.res <- n.catchup
	case cancelCatchupCall:
		if n.incoming == nil {
			r.res <- false
		} else {
			n.cancelIncoming(CatchupCancelled, "cancelled by user")
			r.res <- true
		}
	}
}

func (n *Node) tick() {
	n.tickCount++
	n.electionElapsed++
	switch n.role {
	case RoleLeader:
		// 每个时钟周期发送心跳。
		n.broadcastAppendEntries()
		// 分块快照块在途期间，接收端可能长时间收不到任何消息
		// （块超时重传窗口大于选举超时）：补一个快照边界心跳，
		// 避免大快照传输引发无谓的选举。只从 tick 路径发送，
		// 拒绝回退路径不补，避免事件风暴。
		for _, p := range n.cfg.Peers {
			if p == n.cfg.ID {
				continue
			}
			if out := n.snapOut[p]; out != nil && out.InFlight {
				n.sendSnapshotHeartbeat(p)
			}
		}
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
	// 下台即放弃全部在途的分块快照发送（响应会因角色/任期不匹配被忽略）。
	n.snapOut = make(map[NodeID]*outgoingSnapshot)
	n.resetElectionTimer()
}

func (n *Node) becomeCandidate() {
	n.term++
	n.role = RoleCandidate
	n.votedFor = n.cfg.ID
	n.persistState()
	n.grantedVotes = map[NodeID]bool{n.cfg.ID: true}
	n.leaderID = ""
	n.snapOut = make(map[NodeID]*outgoingSnapshot)
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
	}
}
