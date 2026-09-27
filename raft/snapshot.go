package raft

import "context"

// maybeSnapshot 检查日志增量是否达到阈值，达到则在当前提交位置做压缩。
// 压缩在事件循环内执行：状态机快照、快照元数据与日志截断是串行的，
// 与并发读（Lookup 仅经 RWMutex 访问状态机）和日志追加不竞争。
func (n *Node) maybeSnapshot() {
	threshold := n.cfg.SnapshotThreshold
	if threshold == 0 {
		return
	}
	suffixLen := uint64(n.lastLogIndex() - n.snapMeta.LastIncludedIndex)
	if suffixLen < threshold {
		return
	}
	if n.commitIndex <= n.snapMeta.LastIncludedIndex {
		return
	}
	n.takeSnapshot(n.commitIndex)
}

// takeSnapshot 在 index 处执行一次压缩：
//  1. 状态机导出字节（与 Apply/Lookup 通过 RWMutex 同步）；
//  2. 原子落盘新快照（tmp+fsync+rename）；
//  3. 用新的后缀重写日志文件并替换内存日志。
//
// 顺序保证：快照先持久化、日志后截断。任何崩溃时刻下，
// “快照内的状态”都不会多于已提交状态，重启恢复不会丢失或回退已提交内容。
func (n *Node) takeSnapshot(index Index) {
	meta, ok := n.snapshotMetaAt(index)
	if !ok {
		return
	}
	data, err := n.sm.Snapshot()
	if err != nil {
		n.logf("[%s] state machine snapshot error: %v", n.cfg.ID, err)
		return
	}
	if err := n.store.SaveSnapshot(meta, data); err != nil {
		n.logf("[%s] persist snapshot error: %v", n.cfg.ID, err)
		return
	}

	oldLast := n.snapMeta.LastIncludedIndex
	// 必须在切换 snapMeta 之前、按旧快照点计算物理下标：
	// index 处的条目将成为新占位项（新日志下标 0），其后为保留后缀。
	pi := n.physicalIndex(index)
	placeholder := n.log[pi]
	var suffix []LogEntry
	if pi+1 < len(n.log) {
		suffix = append([]LogEntry(nil), n.log[pi+1:]...)
	}
	n.snapMeta = meta
	n.log = append([]LogEntry{placeholder}, suffix...)
	if err := n.persistLog(); err != nil {
		n.logf("[%s] rewrite log after compaction error: %v", n.cfg.ID, err)
	}

	// 本地压缩点推进后，目标不超过新快照点的在途分块接收已无意义，取消它。
	if n.incoming != nil && n.incoming.Meta.LastIncludedIndex <= n.snapMeta.LastIncludedIndex {
		n.cancelIncoming(CatchupCancelled, "superseded by local compaction")
	}
	n.debugf("compacted: snapshot index %d->%d, retained suffix entries=%d",
		oldLast, meta.LastIncludedIndex, len(n.log)-1)
}

func (n *Node) snapshotMetaAt(index Index) (SnapshotMeta, bool) {
	if index <= n.snapMeta.LastIncludedIndex {
		return SnapshotMeta{}, false
	}
	e, ok := n.entryAt(index)
	if !ok || index > n.commitIndex {
		return SnapshotMeta{}, false
	}
	return SnapshotMeta{LastIncludedIndex: index, LastIncludedTerm: e.Term}, true
}

// loadSnapshotBytes 读取本地快照字节，供领导者向落后节点发送。
func (n *Node) loadSnapshotBytes() ([]byte, bool) {
	meta, data, err := n.store.LoadSnapshot()
	if err != nil || meta.LastIncludedIndex == 0 {
		return nil, false
	}
	return data, true
}

// sendSnapshot 领导者向 peer 发送快照，返回本轮是否真正发出了 RPC。
// 快照不超过配置块大小时走一次性 InstallSnapshot（兼容旧路径）；
// 大快照切换为可分块、可断点续传的 InstallSnapshotChunk。
func (n *Node) sendSnapshot(peer NodeID) bool {
	// 分块会话在途且未超时：本轮无块可发。
	// 提前返回，避免每个 tick/每次拒绝回退都把整份快照从磁盘读进内存。
	if out := n.snapOut[peer]; out != nil && out.InFlight &&
		n.tickCount-out.LastSend < chunkResendTicks {
		return false
	}
	data, ok := n.loadSnapshotBytes()
	if !ok {
		return false
	}
	if len(data) > n.cfg.SnapshotChunkSize {
		return n.sendSnapshotChunked(peer, data)
	}
	req := &InstallSnapshotRequest{
		Term:              n.term,
		LeaderID:          n.cfg.ID,
		LastIncludedIndex: n.snapMeta.LastIncludedIndex,
		LastIncludedTerm:  n.snapMeta.LastIncludedTerm,
		Data:              data,
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		rpcResp := n.tr.SendInstallSnapshot(context.Background(), peer, req)
		if rpcResp == nil {
			return
		}
		result := &installResult{
			term:         rpcResp.Term,
			success:      rpcResp.Success,
			reason:       rpcResp.Reason,
			lastIncluded: req.LastIncludedIndex,
		}
		select {
		case n.events <- internalEvent{kind: eventRPC, rpc: snapshotResponse{peer: peer, resp: result}}:
		case <-n.stopCh:
		}
	}()
	return true
}

// sendSnapshotHeartbeat 在分块快照传输的静默窗口内向 peer 发一个空 AppendEntries
// （prevLogIndex 取快照边界）：跟随者即使日志不匹配而拒绝，也会重置选举计时器，
// 避免大快照传输期间触发无谓的选举。
func (n *Node) sendSnapshotHeartbeat(peer NodeID) {
	req := &AppendEntriesRequest{
		Term:         n.term,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: n.snapMeta.LastIncludedIndex,
		PrevLogTerm:  n.snapMeta.LastIncludedTerm,
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

// handleInstallSnapshot 在事件循环内处理快照安装（论文 §5.7）。
func (n *Node) handleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	resp := &InstallSnapshotResponse{Term: n.term, Peer: n.cfg.ID}

	if req.Term < n.term {
		resp.Reason = RejectStaleTerm
		return resp
	}
	if req.Term > n.term {
		n.becomeFollower(req.Term)
	}
	if n.role != RoleFollower {
		n.becomeFollower(req.Term)
	}
	resp.Term = n.term
	n.leaderID = req.LeaderID
	n.resetElectionTimer()

	// 收到的快照比本地还旧：忽略（幂等，重复投递也安全）。
	if req.LastIncludedIndex <= n.snapMeta.LastIncludedIndex {
		n.debugf("ignore stale snapshot index=%d (local snapshot=%d, commit=%d)",
			req.LastIncludedIndex, n.snapMeta.LastIncludedIndex, n.commitIndex)
		resp.Success = true
		return resp
	}

	// 整份快照取代任何在途的分块会话。
	if n.incoming != nil {
		n.cancelIncoming(CatchupCancelled, "superseded by whole-snapshot install")
	}

	meta := SnapshotMeta{LastIncludedIndex: req.LastIncludedIndex, LastIncludedTerm: req.LastIncludedTerm}
	if n.installSnapshot(meta, req.Data) {
		resp.Success = true
	}
	return resp
}

// installSnapshot 把一份完整快照安装到本节点（整份 RPC 与分块组装完成共用）：
//  1. 先落盘快照；
//  2. 重置内存日志：占位项 + 快照点之后仍连续匹配的后缀（论文规则）；
//  3. 恢复状态机到快照内容；
//  4. 提交/应用位置至少追到快照点，并继续应用保留的已提交后缀。
//
// 调用前必须保证 meta.LastIncludedIndex > snapMeta.LastIncludedIndex。
func (n *Node) installSnapshot(meta SnapshotMeta, data []byte) bool {
	// 1. 先落盘快照。
	if err := n.store.SaveSnapshot(meta, data); err != nil {
		n.logf("[%s] save received snapshot error: %v", n.cfg.ID, err)
		return false
	}

	// 2. 重置内存日志：必须先用旧 snapMeta 计算要保留的后缀，再切换 snapMeta。
	kept := []LogEntry{{Index: meta.LastIncludedIndex, Term: meta.LastIncludedTerm}}
	if n.lastLogIndex() >= meta.LastIncludedIndex+1 {
		if e, ok := n.entryAt(meta.LastIncludedIndex + 1); ok {
			pi := n.physicalIndex(e.Index)
			if pi >= 1 && pi < len(n.log) {
				kept = append(kept, n.log[pi:]...)
			}
		}
	}
	n.snapMeta = meta
	n.log = kept
	if err := n.persistLog(); err != nil {
		n.logf("[%s] rewrite log after install snapshot error: %v", n.cfg.ID, err)
	}

	// 3. 恢复状态机到快照内容。
	if err := n.sm.Restore(data); err != nil {
		n.logf("[%s] restore snapshot error: %v", n.cfg.ID, err)
		return false
	}

	// 4. 提交位置/应用位置至少追到快照点。
	if meta.LastIncludedIndex > n.commitIndex {
		n.commitIndex = meta.LastIncludedIndex
	}
	if meta.LastIncludedIndex > n.lastApplied {
		n.lastApplied = meta.LastIncludedIndex
	}
	// 若保留了快照点之后的已提交条目，继续应用。
	n.applyCommitted()

	n.debugf("installed snapshot through index=%d term=%d (commit=%d applied=%d suffix=%d)",
		meta.LastIncludedIndex, meta.LastIncludedTerm, n.commitIndex, n.lastApplied, len(n.log)-1)
	return true
}
