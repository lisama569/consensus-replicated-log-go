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
		n.sendAppendEntriesForce(p, false)
	}
}

// broadcastHeartbeat 每个 tick 调用：强制重发心跳，使 AE 在途窗口在丢包/领导权
// 切换后能自愈（不被残留的在途标记永久卡住某个 peer）。
func (n *Node) broadcastHeartbeat() {
	if n.role != RoleLeader {
		return
	}
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		n.sendAppendEntriesForce(p, true)
	}
}

func (n *Node) sendAppendEntries(peer NodeID) { n.sendAppendEntriesForce(peer, false) }

// sendAppendEntriesForce 发送一条 AppendEntries（或在落后时走快照）。
// force=true（每 tick 的心跳）忽略在途窗口，保证心跳不会因丢失的在途标记停发。
func (n *Node) sendAppendEntriesForce(peer NodeID, force bool) {
	if n.role != RoleLeader {
		return
	}
	ni := n.nextIndex[peer]

	// peer 落后到快照覆盖范围之前：改用分块 InstallSnapshot 追赶。
	if n.matchIndex[peer] < n.snapMeta.LastIncludedIndex {
		if tr := n.snapXfers[peer]; tr != nil && tr.backoff == 0 && !tr.done {
			return
		}
		n.sendSnapshot(peer)
		return
	}

	// 停等窗口：上一条 AppendEntries 还在途时不重复发送。新提交会在该响应
	// 回来后立即补推（handleAppendResponse 末尾再 sendAppendEntries），
	// 心跳也会每个 tick 重发，因此不损失新鲜度，只去掉重复洪泛。
	if !force && n.aeInflight[peer] {
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
	n.aeInflight[peer] = true
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		resp := n.tr.SendAppendEntries(context.Background(), peer, req)
		if resp == nil {
			// 丢包/对端不可达：没有响应事件，这里直接释放窗口，下个 tick 重发。
			select {
			case n.events <- internalEvent{kind: eventRPC, rpc: aeTimeout{peer: peer}}:
			case <-n.stopCh:
			}
			return
		}
		select {
		case n.events <- internalEvent{kind: eventRPC, rpc: appendResponse{peer: peer, resp: resp}}:
		case <-n.stopCh:
		}
	}()
}

// aeTimeout 表示某 peer 的 AppendEntries 请求/响应丢失，仅用于释放停等窗口。
type aeTimeout struct{ peer NodeID }

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

	// 一致性检查：prevLogIndex 落在本节点快照覆盖范围内（<= 快照点）。
	// 领导者携带的可能是一份更新的快照/日志——这种“本地缺日志”不是冲突，
	// 必须明确要求快照同步（log_not_up_to_date 且 XIndex=本地快照点）；
	// 领导者据此走 InstallSnapshot，而不是反复用同一 prevIndex 撞拒绝。
	if req.PrevLogIndex < n.snapMeta.LastIncludedIndex {
		resp.Success = false
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
			// 缺少 prevLogIndex：告知领导者本节点日志的真实起点。
			// XIndex 必须是本节点仍可能保留的第一条索引：
			// 有快照时为快照点之后（领导者据此直接走 InstallSnapshot），
			// 绝不能回填到已被压缩的占位索引。
			first := n.snapMeta.LastIncludedIndex + 1
			resp.Success = false
			resp.PeerLastIndex = n.lastLogIndex()
			resp.XTerm = 0
			resp.XIndex = first
			n.debugf("append rejected: missing prevIndex=%d (local last=%d, first=%d)",
				req.PrevLogIndex, n.lastLogIndex(), first)
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

	// 追加新条目：跳过快照点之前的；本地缺失则必须与本地末尾连续后追加，任期冲突
	// 则截断后覆盖；索引/任期都相同则幂等跳过。遍历“全部”携带条目。
	// 任何一条出现“本地缺失但又不接在末尾之后”的空洞都视为致命冲突——prevLog 检查
	// 已保证请求前缀与本地匹配，正常的 AE 条目必然在 prevLogIndex 之后连续；出现
	// 空洞只能说明本节点日志与领导者分叉，必须整体拒绝并回报真实起点，由领导者经
	// 快速回退/快照补齐，绝不静默跳过（否则会丢掉空洞之后的条目）。
	if len(req.Entries) > 0 {
		changed := false
		conflict := false
		for _, e := range req.Entries {
			if e.Index <= n.snapMeta.LastIncludedIndex {
				continue
			}
			existing, exists := n.entryAt(e.Index)
			switch {
			case !exists:
				if e.Index != n.lastLogIndex()+1 {
					resp.Success = false
					resp.PeerLastIndex = n.lastLogIndex()
					resp.XTerm = 0
					resp.XIndex = n.snapMeta.LastIncludedIndex + 1
					n.debugf("append gap at entry %d (local last=%d snap=%d) -> reject",
						e.Index, n.lastLogIndex(), n.snapMeta.LastIncludedIndex)
					conflict = true
					break
				}
				n.log = append(n.log, e)
				changed = true
			case existing.Term != e.Term:
				if err := n.truncateTail(e.Index); err != nil {
					n.logf("[%s] truncate error: %v", n.cfg.ID, err)
					continue
				}
				n.log = append(n.log, e)
				changed = true
			}
			if conflict {
				break
			}
		}
		if conflict {
			return resp
		}
		if changed {
			if err := n.persistLog(); err != nil {
				n.logf("[%s] append suffix persist error: %v", n.cfg.ID, err)
			}
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
	n.aeInflight[peer] = false
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
	// 已完成的快照会话记录保留在 snapXfers 中用于进度查询，
	// 下一次需要快照追赶时 startSnapshotTransfer 会比较快照身份并替换。
	if n.matchIndex[peer] < n.lastLogIndex() {
		n.sendAppendEntries(peer)
	}
}

// backoffPeer 根据冲突提示回退 nextIndex（论文 §5.3 优化）。
func (n *Node) backoffPeer(peer NodeID, resp *AppendEntriesResponse) {
	// 跟随者的冲突点 XIndex 若不小于本节点快照点，说明它可能仍保留所需日志，
	// 直接采用；否则它已落后到本节点快照覆盖范围（或持有更新的快照点），
	// nextIndex 必须夹到“本节点快照点之后”，下一条消息改走 InstallSnapshot。
	if resp.XTerm != 0 {
		if last := n.lastIndexOfTerm(resp.XTerm); last > 0 {
			n.nextIndex[peer] = last + 1
		} else {
			n.nextIndex[peer] = resp.XIndex
		}
	} else if resp.XIndex > n.snapMeta.LastIncludedIndex {
		// XIndex 是跟随者能接受的第一条索引（其快照点之后）。下一条 AE 的
		// prevLogIndex = nextIndex-1 必须等于跟随者快照点，因此 nextIndex
		// 直接设为 XIndex（即“要发送的第一条”），不能设成 XIndex 本身再
		// 当作 prev——那会让 prevLogIndex 落到跟随者已压缩的空洞里被无限拒绝。
		n.nextIndex[peer] = resp.XIndex
	} else {
		n.nextIndex[peer] = n.snapMeta.LastIncludedIndex + 1
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
