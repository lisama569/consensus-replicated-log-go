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
	// 压缩节流：生成快照与状态机大小同阶（导出+序列化），高频触发会让其 CPU/IO
	// 成本超过正常复制。即使阈值很小，也保证两次压缩之间至少间隔 GapTicks。
	if n.ticksSinceSnapshot < n.cfg.SnapshotMinGapTicks {
		return
	}
	n.takeSnapshot(n.commitIndex)
}

// takeSnapshot 在 index 处安排一次压缩。
//
// 快照字节在事件循环内、与 Apply 互斥地导出，保证字节内容恰好覆盖到快照点 index；
// 真正的多次 fsync（原子落盘快照 + 重写日志后缀）放到后台持久化 worker，不阻塞
// 协议心跳与快照块收发。压缩由 SnapshotMinGapTicks 节流（默认不节流）。
func (n *Node) takeSnapshot(index Index) {
	meta, ok := n.snapshotMetaAt(index)
	if !ok {
		return
	}
	// 与 Apply 串行（事件循环内），字节内容严格对应 index，不会把快照点之后的
	// 状态与“index 之后的日志后缀”错误地重复重放。
	data, err := n.sm.Snapshot()
	if err != nil {
		n.logf("[%s] state machine snapshot error: %v", n.cfg.ID, err)
		return
	}
	n.ticksSinceSnapshot = 0

	oldLast := n.snapMeta.LastIncludedIndex
	pi := n.physicalIndex(index)
	placeholder := n.log[pi]
	var suffix []LogEntry
	if pi+1 < len(n.log) {
		suffix = append([]LogEntry(nil), n.log[pi+1:]...)
	}
	logCopy := append([]LogEntry{placeholder}, suffix...)

	n.snapMeta = meta
	n.log = logCopy
	n.logDirty = false
	n.debugf("compacted: snapshot index %d->%d (fsync in background)",
		oldLast, meta.LastIncludedIndex)

	// 后台仅负责 fsync（快照字节已在事件循环内确定）。单飞：同时只有一份压缩
	// 在落盘，期间产生的更新压缩只保留最新一份。
	job := chunkJob{kind: jobPersistCompact, meta: meta,
		data: append([]byte(nil), data...), log: logCopy}
	if n.compactBusy {
		n.pendingCompact = &job
	} else {
		n.compactBusy = true
		if !n.enqueuePersistAck(job) {
			// 队列暂满（磁盘跟不上）：内存已压缩，下个 tick/再次压缩会重做落盘；
			// flushDurability 停止前也会再写，保证不丢。
		}
	}
}

// onCompactPersisted 由 store worker 的压缩完成事件调用：若期间又有更新的压缩，
// 立即投递最新一份；否则解除单飞。
func (n *Node) onCompactPersisted() {
	if n.pendingCompact != nil {
		job := n.pendingCompact
		n.pendingCompact = nil
		n.enqueuePersistAck(*job)
		return
	}
	n.compactBusy = false
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

// loadSnapshotForTransfer 读取本地快照字节并计算校验和，供分块发送使用。
// 取 formalWriteMu 读锁：避免读到“快照文件已更新、日志后缀尚未重写”的中间态
// （那两步在 persist worker 里持写锁原子完成）。
func (n *Node) loadSnapshotForTransfer() ([]byte, SnapshotMeta, []byte, bool) {
	n.formalWriteMu.RLock()
	defer n.formalWriteMu.RUnlock()
	meta, data, err := n.store.LoadSnapshot()
	if err != nil || meta.LastIncludedIndex == 0 {
		return nil, SnapshotMeta{}, nil, false
	}
	sum := sha256Sum(data)
	return data, meta, sum, true
}

// sendSnapshot 领导者向 peer 发送快照：优先使用分块可续传传输；
// 读取本地快照失败时回退为空操作（下一轮心跳会再次尝试）。
func (n *Node) sendSnapshot(peer NodeID) {
	n.startSnapshotTransfer(peer)
}

// sendSnapshotChunk 在独立 goroutine 中发送一个分块快照请求，响应投递回事件循环。
func (n *Node) sendSnapshotChunk(peer NodeID, req *InstallSnapshotRequest) {
	// 防御性拷贝：内存传输会把请求指针直接交给对端处理器，
	// 不能让对端（或测试钩子）通过共享底层数组改到本节点的快照内存。
	out := *req
	out.Data = append([]byte(nil), req.Data...)
	out.Checksum = append([]byte(nil), req.Checksum...)
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		rpcResp := n.tr.SendInstallSnapshot(context.Background(), peer, &out)
		if rpcResp == nil {
			select {
			case n.events <- internalEvent{kind: eventRPC, rpc: chunkTimeout{peer: peer, offset: req.Offset}}:
			case <-n.stopCh:
			}
			return
		}
		result := &installResult{
			term:         rpcResp.Term,
			success:      rpcResp.Success,
			reason:       rpcResp.Reason,
			lastIncluded: req.LastIncludedIndex,
			chunked:      true,
			transferID:   rpcResp.TransferID,
			ackOffset:    rpcResp.AckOffset,
			complete:     rpcResp.Complete,
		}
		select {
		case n.events <- internalEvent{kind: eventRPC,
			rpc: snapshotResponse{peer: peer, resp: result}}:
		case <-n.stopCh:
		}
	}()
}

// chunkTimeout 表示某个分块的请求或响应丢失，offset 为该块起始偏移。
type chunkTimeout struct {
	peer   NodeID
	offset uint64
}

// handleInstallSnapshot 在事件循环内处理旧版（一次性整体）快照安装（论文 §5.7）。
// 保留该路径用于与旧版本节点的互操作；大快照默认走分块续传路径。
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

	meta := SnapshotMeta{LastIncludedIndex: req.LastIncludedIndex, LastIncludedTerm: req.LastIncludedTerm}
	if err := n.commitSnapshotInstall(meta, req.Data); err != nil {
		n.logf("[%s] install snapshot error: %v", n.cfg.ID, err)
		return resp
	}
	n.debugf("installed snapshot through index=%d term=%d (commit=%d applied=%d suffix=%d)",
		meta.LastIncludedIndex, meta.LastIncludedTerm, n.commitIndex, n.lastApplied, len(n.log)-1)
	resp.Success = true
	return resp
}

// commitSnapshotInstall 把一份完整快照提升为节点当前状态（接收方路径）。
// 磁盘部分（原子落盘快照 + 重写日志后缀）由后台 store worker 串行执行，
// 保证不与常规日志/压缩落盘乱序；完成后在调用上下文中完成内存与状态机切换。
// 分块路径在收到 worker 完成事件后调用 applyInstalledSnapshot；旧版一次性路径
// 通过 installSnapshotAndWait 同步等待 worker 完成后再返回 RPC。
func (n *Node) commitSnapshotInstall(meta SnapshotMeta, data []byte) error {
	kept := []LogEntry{{Index: meta.LastIncludedIndex, Term: meta.LastIncludedTerm}}
	if n.lastLogIndex() >= meta.LastIncludedIndex+1 {
		if e, ok := n.entryAt(meta.LastIncludedIndex + 1); ok {
			pi := n.physicalIndex(e.Index)
			if pi >= 1 && pi < len(n.log) {
				kept = append(kept, n.log[pi:]...)
			}
		}
	}
	if err := n.installSnapshotSync(meta, data, kept); err != nil {
		return err
	}
	return n.applyInstalledSnapshot(meta, data)
}

// installSnapshotSync 把快照+日志后缀任务交给 store worker 并等待其 fsync 完成。
func (n *Node) installSnapshotSync(meta SnapshotMeta, data []byte, kept []LogEntry) error {
	done := make(chan error, 1)
	job := chunkJob{
		kind: jobPersistInstall, meta: meta,
		data: append([]byte(nil), data...), log: kept,
		ack: true, syncDone: done,
	}
	select {
	case n.chunkJobs <- job:
	case <-n.stopCh:
		return ErrStopped
	}
	select {
	case err := <-done:
		return err
	case <-n.stopCh:
		return ErrStopped
	}
}

// applyInstalledSnapshot 在事件循环内完成快照安装的内存部分（worker 落盘成功后）。
//
// 单调安全：绝不因安装快照而让状态机内容回退。分两种情况：
//  1. 快照覆盖到的位置已不超过本地“已应用”位置：本地状态机已是它的超集，
//     只更新快照点/日志边界，不 Restore（避免把已应用的键清掉）。
//  2. 快照严格领先本地已应用位置：正常 Restore，再重放快照点之后保留的后缀。
func (n *Node) applyInstalledSnapshot(meta SnapshotMeta, data []byte) error {
	kept := []LogEntry{{Index: meta.LastIncludedIndex, Term: meta.LastIncludedTerm}}
	if n.lastLogIndex() >= meta.LastIncludedIndex+1 {
		if e, ok := n.entryAt(meta.LastIncludedIndex + 1); ok {
			pi := n.physicalIndex(e.Index)
			if pi >= 1 && pi < len(n.log) {
				kept = append(kept, n.log[pi:]...)
			}
		}
	}

	if meta.LastIncludedIndex <= n.lastApplied {
		// 本地已应用到更靠前的位置：当前状态机已经是快照内容的超集。只把快照/
		// 日志边界前移（取更靠后的），绝不 Restore 旧数据。
		n.debugf("snapshot at %d <= applied %d: keep state, advance bounds",
			meta.LastIncludedIndex, n.lastApplied)
		if meta.LastIncludedIndex > n.snapMeta.LastIncludedIndex {
			n.snapMeta = meta
			n.log = kept
		}
		if meta.LastIncludedIndex > n.commitIndex {
			n.commitIndex = meta.LastIncludedIndex
		}
		return nil
	}

	// 快照领先已应用位置：安全恢复到快照点并重放后缀。
	n.snapMeta = meta
	n.log = kept
	if err := n.sm.Restore(data); err != nil {
		return err
	}
	if meta.LastIncludedIndex > n.commitIndex {
		n.commitIndex = meta.LastIncludedIndex
	}
	n.lastApplied = meta.LastIncludedIndex
	n.applyCommitted()
	return nil
}
