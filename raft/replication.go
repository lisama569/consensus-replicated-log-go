package raft

import "context"

// readState 是一个待完成的线性一致读（ReadIndex）。
type readState struct {
	barrier Index  // 登记时的 commitIndex
	key     string // 读取的键
	res     chan clientOutcome
	acks    map[NodeID]bool // 当前任期心跳确认
}

// ---- 客户端入口 ----

func (n *Node) notLeaderErr() error {
	return errWithLeader(ErrNotLeader, n.leaderID)
}

func (n *Node) handleClientCall(req proposeReq) {
	if n.role != RoleLeader {
		sendOutcome(req.res, clientOutcome{err: n.notLeaderErr()})
		return
	}

	// 线性一致读：登记读屏障，用一轮当前任期心跳确认领导权后再读状态机。
	if req.isRead {
		rs := &readState{barrier: n.commitIndex, key: req.cmd.Key, res: req.res, acks: map[NodeID]bool{}}
		n.pendingReads = append(n.pendingReads, rs)
		n.broadcastAppendEntries()
		return
	}

	cmd := req.cmd

	// 幂等快速路径 1：同一客户端的同一序列号此前已应用（此前已提交）。
	if cmd.ClientID != "" {
		if last, ok := n.appliedSeq[cmd.ClientID]; ok && cmd.Seq == last.seq {
			n.debugf("proposal client=%s seq=%d already applied at index %d -> idempotent reply",
				cmd.ClientID, cmd.Seq, last.index)
			sendOutcome(req.res, clientOutcome{index: last.index, term: n.term, duplicate: true})
			return
		}
		// 幂等快速路径 2：同序列号请求已在日志中（尚未提交），复用其等待队列。
		for i := n.physicalIndex(n.commitIndex) + 1; i < len(n.log); i++ {
			c := n.log[i].Command
			if c.ClientID == cmd.ClientID && c.Seq == cmd.Seq {
				n.proposals[n.log[i].Index] = append(n.proposals[n.log[i].Index], req.res)
				n.debugf("proposal client=%s seq=%d already in-flight at index %d -> join waiters",
					cmd.ClientID, cmd.Seq, n.log[i].Index)
				n.broadcastAppendEntries()
				return
			}
		}
	}

	idx := n.appendClientEntry(cmd, req.res)
	n.debugf("appended new entry index=%d term=%d client=%s seq=%d",
		idx, n.term, cmd.ClientID, cmd.Seq)
	n.broadcastAppendEntries()
}

// appendClientEntry 在日志尾部追加一条客户端命令并登记提案。
func (n *Node) appendClientEntry(cmd Command, res chan clientOutcome) Index {
	idx := n.lastLogIndex() + 1
	e := LogEntry{Term: n.term, Index: idx, Command: cmd}
	if err := n.appendSuffix([]LogEntry{e}); err != nil {
		n.logf("[%s] append log error: %v", n.cfg.ID, err)
	}
	if res != nil {
		n.proposals[idx] = append(n.proposals[idx], res)
	}
	return idx
}

// ---- 领导者：发送 ----

func (n *Node) broadcastAppendEntries() {
	if n.role != RoleLeader {
		return
	}
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		n.sendAppendEntries(p)
	}
}

func (n *Node) sendAppendEntries(peer NodeID) {
	if n.role != RoleLeader {
		return
	}
	ni := n.nextIndex[peer]

	// peer 落后到快照覆盖范围之前：改用 InstallSnapshot 追赶。
	if ni <= n.snapMeta.LastIncludedIndex {
		n.sendSnapshot(peer)
		return
	}

	prevIndex := ni - 1
	prevTerm := n.termAt(prevIndex)
	entries := n.entriesAfter(prevIndex)
	req := &AppendEntriesRequest{
		Term:         n.term,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		resp := n.tr.SendAppendEntries(context.Background(), peer, req)
		if resp == nil {
			return
		}
		select {
		case n.events <- internalEvent{kind: eventRPC, rpc: appendResponse{peer: peer, resp: resp}}:
		case <-n.stopCh:
		}
	}()
}

// ---- 跟随者：接收 ----

func (n *Node) handleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	resp := &AppendEntriesResponse{Term: n.term, Peer: n.cfg.ID}

	// 任期落后：拒绝并给出 RejectStaleTerm。
	if req.Term < n.term {
		resp.Reason = RejectStaleTerm
		return resp
	}
	if req.Term > n.term {
		n.becomeFollower(req.Term)
	}
	// 同任期收到合法领导者心跳：候选人/领导者身份都让回跟随者。
	if n.role != RoleFollower {
		n.becomeFollower(req.Term)
	}
	resp.Term = n.term
	n.leaderID = req.LeaderID
	n.resetElectionTimer()

	// 一致性检查：prevLogIndex 落在快照覆盖范围内但 term 对不上，
	// 说明领导者视图与本快照冲突（正常不会发生；要求快照同步）。
	if req.PrevLogIndex < n.snapMeta.LastIncludedIndex {
		resp.Reason = RejectLogNotUpToDate
		resp.XIndex = n.snapMeta.LastIncludedIndex
		return resp
	}

	// 冲突检查（Raft 论文的 conflictTerm/conflictIndex 快速回退）。
	// prevLogIndex=0 表示日志起点哨兵（无快照时的占位项），天然匹配。
	if req.PrevLogIndex == 0 && n.snapMeta.LastIncludedIndex == 0 {
		// 落到正常追加流程。
	} else {
		prev, ok := n.entryAt(req.PrevLogIndex)
		if !ok {
			// 缺少 prevLogIndex：告知领导者本节点最后一条之后的位置。
			resp.Success = false
			resp.PeerLastIndex = n.lastLogIndex()
			resp.XTerm = 0
			resp.XIndex = n.lastLogIndex() + 1
			n.debugf("append rejected: missing prevIndex=%d (local last=%d)",
				req.PrevLogIndex, n.lastLogIndex())
			return resp
		}
		if prev.Term != req.PrevLogTerm {
			resp.Success = false
			resp.PeerLastIndex = n.lastLogIndex()
			resp.XTerm = prev.Term
			resp.XIndex = n.firstIndexOfTerm(prev.Term)
			n.debugf("append rejected: term mismatch at index=%d localTerm=%d wantTerm=%d (xIndex=%d)",
				req.PrevLogIndex, prev.Term, req.PrevLogTerm, resp.XIndex)
			return resp
		}
	}

	// 追加新条目：遇到 term 冲突则截断之后再追加。
	if len(req.Entries) > 0 {
		appended := false
		for _, e := range req.Entries {
			if e.Index <= n.snapMeta.LastIncludedIndex {
				continue
			}
			existing, exists := n.entryAt(e.Index)
			switch {
			case !exists:
				// 从 e 开始（含）连续追加后续全部条目。
				rest := collectFrom(req.Entries, e.Index)
				if err := n.appendSuffix(rest); err != nil {
					n.logf("[%s] append suffix error: %v", n.cfg.ID, err)
				}
				appended = true
			case existing.Term != e.Term:
				if err := n.truncateTail(e.Index); err != nil {
					n.logf("[%s] truncate error: %v", n.cfg.ID, err)
				}
				rest := collectFrom(req.Entries, e.Index)
				if err := n.appendSuffix(rest); err != nil {
					n.logf("[%s] append suffix error: %v", n.cfg.ID, err)
				}
				appended = true
			}
			if appended {
				break
			}
		}
		if appended {
			n.debugf("appended entries up to lastIndex=%d from leader t=%d",
				n.lastLogIndex(), req.Term)
		}
	}

	// 推进本节点提交位置（不得超过本日志最后一条，也不得超过领导者的提交位置）。
	if req.LeaderCommit > n.commitIndex {
		target := req.LeaderCommit
		if last := n.lastLogIndex(); target > last {
			target = last
		}
		if target > n.commitIndex {
			n.commitIndex = target
		}
	}

	n.applyCommitted()
	n.maybeSnapshot()

	resp.Success = true
	resp.PeerLastIndex = n.lastLogIndex()
	return resp
}

// ---- 领导者：处理跟随者响应 ----

func (n *Node) handleAppendResponse(peer NodeID, resp *AppendEntriesResponse) {
	if resp.Term > n.term {
		n.debugf("append response from %s carries higher term %d -> step down", peer, resp.Term)
		n.becomeFollower(resp.Term)
		return
	}
	if n.role != RoleLeader || resp.Term != n.term {
		return // 过期响应
	}
	if !resp.Success {
		n.backoffPeer(peer, resp)
		return
	}

	// 成功：单调推进 match/next（乱序响应也不能让匹配位置倒退）。
	if resp.PeerLastIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = resp.PeerLastIndex
		n.debugf("peer %s matched up to %d", peer, resp.PeerLastIndex)
	}
	if next := n.matchIndex[peer] + 1; next > n.nextIndex[peer] {
		n.nextIndex[peer] = next
	}

	// 当前任期心跳确认：为所有在途读屏障记账。
	for _, rs := range n.pendingReads {
		rs.acks[peer] = true
	}

	n.advanceCommitIndex()
	n.tryServeReads()

	// peer 可能还落后（本次只发了窗口或之前是冲突重试），继续推送。
	if n.matchIndex[peer] < n.lastLogIndex() {
		n.sendAppendEntries(peer)
	}
}

// backoffPeer 根据冲突提示回退 nextIndex（论文 §5.3 优化）。
func (n *Node) backoffPeer(peer NodeID, resp *AppendEntriesResponse) {
	n.debugf("peer %s rejected append reason=%s xTerm=%d xIndex=%d (old next=%d)",
		peer, resp.Reason, resp.XTerm, resp.XIndex, n.nextIndex[peer])
	switch {
	case resp.XTerm != 0:
		if last := n.lastIndexOfTerm(resp.XTerm); last > 0 {
			// 本节点日志仍保留该任期：跳过整个冲突任期。
			n.nextIndex[peer] = last + 1
		} else {
			// 本节点已没有该任期：直接采用跟随者给出的该任期起点。
			n.nextIndex[peer] = resp.XIndex
		}
	case resp.XIndex > 0:
		n.nextIndex[peer] = resp.XIndex
	default:
		n.nextIndex[peer] = 1
	}
	// 快照边界兜底。
	if min := n.snapMeta.LastIncludedIndex + 1; n.nextIndex[peer] < min {
		n.nextIndex[peer] = min
	}
	n.sendAppendEntries(peer)
}

// advanceCommitIndex 仅提交当前任期的多数派条目（Figure 8 安全性）。
func (n *Node) advanceCommitIndex() {
	for candidate := n.lastLogIndex(); candidate > n.commitIndex; candidate-- {
		e, ok := n.entryAt(candidate)
		if !ok || e.Term != n.term {
			continue
		}
		// 统计复制到 >= candidate 的节点数（含领导者自身）。
		replicated := 1
		for _, p := range n.cfg.Peers {
			if p == n.cfg.ID {
				continue
			}
			if n.matchIndex[p] >= candidate {
				replicated++
			}
		}
		if replicated >= n.majority() {
			n.commitIndex = candidate
			n.debugf("advance commit to %d (term %d, %d/%d replicated)",
				candidate, n.term, replicated, len(n.cfg.Peers))
			n.applyCommitted()
			n.maybeSnapshot()
			// 提交位置前进，立即向跟随者广播新的 commit。
			n.broadcastAppendEntries()
			break
		}
	}
}

// applyCommitted 把 (lastApplied, commitIndex] 区间的日志顺序应用到状态机，
// 并通知等待该索引的全部提案。
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e, ok := n.entryAt(n.lastApplied)
		if !ok {
			// 理论上不可能：提交位置不应超出日志。回到提交前位置防止死循环。
			n.lastApplied--
			return
		}
		applied, err := n.sm.Apply(e.Command)
		if err != nil {
			n.logf("[%s] apply index=%d error: %v", n.cfg.ID, err)
		}
		if e.Command.ClientID != "" {
			n.appliedSeq[e.Command.ClientID] = appliedSeqEntry{seq: e.Command.Seq, index: e.Index}
		}

		if waiters := n.proposals[e.Index]; len(waiters) > 0 {
			out := clientOutcome{index: e.Index, term: e.Term, duplicate: !applied}
			for _, w := range waiters {
				sendOutcome(w, out)
			}
			delete(n.proposals, e.Index)
		}
	}
	n.tryServeReads()
}

// tryServeReads 尝试完成已经获得多数派心跳确认且状态机追平屏障的读。
func (n *Node) tryServeReads() {
	for len(n.pendingReads) > 0 {
		rs := n.pendingReads[0]
		if n.role != RoleLeader {
			sendOutcome(rs.res, clientOutcome{err: ErrLeaderDeposed})
			n.pendingReads = n.pendingReads[1:]
			continue
		}
		if len(rs.acks)+1 < n.majority() {
			break // 队头未获确认，后续读必须排队，保持读顺序
		}
		if n.lastApplied < rs.barrier {
			break
		}
		v, found := n.sm.Lookup(rs.key)
		sendOutcome(rs.res, clientOutcome{readValue: &struct {
			value string
			found bool
		}{v, found}})
		n.pendingReads = n.pendingReads[1:]
	}
}

// ---- 纯函数辅助 ----

// firstIndexOfTerm 返回日志中 term=t 的第一条索引（含快照占位扫描）。
func (n *Node) firstIndexOfTerm(t Term) Index {
	for i := 1; i < len(n.log); i++ {
		if n.log[i].Term == t {
			return n.log[i].Index
		}
	}
	return n.snapMeta.LastIncludedIndex + 1
}

// lastIndexOfTerm 返回本节点日志中 term=t 的最后一条索引；不存在返回 0。
func (n *Node) lastIndexOfTerm(t Term) Index {
	var last Index
	for i := 1; i < len(n.log); i++ {
		if n.log[i].Term == t {
			last = n.log[i].Index
		}
	}
	return last
}

// collectFrom 从 entries 中挑出 Index >= from 的连续后缀。
func collectFrom(entries []LogEntry, from Index) []LogEntry {
	for i, e := range entries {
		if e.Index == from {
			return append([]LogEntry(nil), entries[i:]...)
		}
	}
	return nil
}
