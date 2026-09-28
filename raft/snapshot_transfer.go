package raft

import (
	"bytes"
	"crypto/sha256"
)

// 可恢复的大快照传输（分块 InstallSnapshot）：
//
//   - 领导者把整份快照切成固定大小的块，每块携带完整快照的身份
//     （LastIncludedIndex/Term/Length/Checksum）与字节偏移；
//   - 跟随者把已连续确认的块追加写入临时文件并持久化元数据，
//     丢包/超时/重启/领导权变化后都可以从 AckOffset 继续；
//   - 只有收齐全部字节且 SHA-256 校验通过，临时快照才会被原子提升为
//     正式快照并恢复状态机；半份数据永远不会被当成可用状态；
//   - 重复块幂等忽略，乱序块（偏移不等于已收长度）拒绝并要求从断点重发；
//     传输过程中领导者压缩出新快照时，身份变化，旧会话自然作废、从 0 重发。
//
// 所有可变状态仍只在节点事件循环中访问。

func sha256Sum(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

// incomingSnapshot 是跟随者侧唯一一份进行中的临时快照状态。
type incomingSnapshot struct {
	meta     StagingMeta
	received uint64
	state    CatchUpState
	reason   RejectReason
}

// snapshotTransfer 是领导者面向某个 peer 的一个快照发送会话。
// 会话一旦建立就“钉住”自己的快照身份：追赶期间领导者持续压缩只会产生
// 更新的快照，而不会打断进行中的传输，避免接收端字节进度反复掉 0。
// 当前快照安装完成（或确认无法继续）后，下一轮心跳再用最新快照开新会话。
type snapshotTransfer struct {
	id       uint64
	term     Term
	meta     SnapshotMeta
	length   uint64
	checksum []byte
	data     []byte

	offset    uint64 // 下一个待发送块的起始偏移
	inflight  bool   // 当前是否有在途块
	attempt   uint64 // 当前块的发送序号（每次发送递增），用于丢弃过期/重复响应
	elapsed   int    // 在途块已经等待的 tick 数
	backoff   int    // 对端要求等待（如被取消）时的剩余 tick
	done      bool   // 对端已完整安装
	epoch     uint64 // 对端最近一次响应中的接收代际（取消后旧代际成功响应必须作废）
	haveEpoch bool   // 是否已从对端收到过 epoch
}

// ---- 领导者：发送侧 ----

// startSnapshotTransfer 为 peer 创建、复用或续传一个分块快照会话。
// 每个心跳（sendAppendEntries 发现 nextIndex<=snapLast）都会调用：
// 因此创建过程必须幂等，并且在等待退避时不能发块。
func (n *Node) startSnapshotTransfer(peer NodeID) {
	if n.role != RoleLeader {
		return
	}
	data, meta, sum, ok := n.loadSnapshotForTransfer()
	if !ok {
		return
	}
	cur := snapshotIdentity{
		lastIncluded: meta.LastIncludedIndex,
		term:         meta.LastIncludedTerm,
		length:       uint64(len(data)),
		checksum:     string(sum),
	}

	tr := n.snapXfers[peer]
	if tr != nil {
		id := snapshotIdentity{
			lastIncluded: tr.meta.LastIncludedIndex,
			term:         tr.meta.LastIncludedTerm,
			length:       tr.length,
			checksum:     string(tr.checksum),
		}
		switch {
		case id != cur && !tr.done:
			// 会话钉住创建时的快照身份：追赶期间领导者压缩出的更新快照
			// 不会打断进行中的传输（否则接收端会反复从 0 重来）。
			// 当前快照安装完成后，本会话被移除，再由下方逻辑用最新快照开新会话。
			n.debugf("peer %s transfer pinned at snapshot %d (newer %d available)",
				peer, tr.meta.LastIncludedIndex, meta.LastIncludedIndex)
			// 落到函数末尾统一按“在途/退避/空闲”决定是否发块。
		case id != cur && tr.done:
			// 对端已完整安装旧快照：丢弃旧会话记录，按最新快照决定下一步。
			n.dropTransfer(peer)
			tr = nil
		case tr.done:
			// 对端已完整安装：常规 AppendEntries 会接管；
			// 若它仍落后于快照点（理论上不会），下面的推进逻辑兜底。
			if tr.meta.LastIncludedIndex > n.matchIndex[peer] {
				n.matchIndex[peer] = tr.meta.LastIncludedIndex
			}
			n.nextIndex[peer] = tr.meta.LastIncludedIndex + 1
			n.sendAppendEntries(peer)
			return
		case tr.backoff > 0:
			return // 对端明确要求等待（如被用户取消）。
		case tr.inflight:
			return // 有在途块，超时由 tickSnapshotTransfers 重传。
		}
	}

	if tr == nil {
		n.nextTransferID++
		tr = &snapshotTransfer{
			id:        n.nextTransferID,
			term:      n.term,
			meta:      meta,
			length:    uint64(len(data)),
			checksum:  sum,
			data:      data,
			haveEpoch: n.knownEpochOK[peer],
			epoch:     n.knownEpoch[peer],
		}
		n.snapXfers[peer] = tr
		n.debugf("start chunked snapshot transfer to %s: id=%d index=%d bytes=%d chunk=%d epoch=%d",
			peer, tr.id, meta.LastIncludedIndex, tr.length, n.cfg.SnapshotChunkSize, tr.epoch)
	}
	n.transmitChunk(peer, tr)
}

// transmitChunk 发送会话当前 offset 处的一块。
func (n *Node) transmitChunk(peer NodeID, tr *snapshotTransfer) {
	if tr.inflight || tr.backoff > 0 || tr.done {
		return
	}
	size := uint64(n.cfg.SnapshotChunkSize)
	if size == 0 {
		size = 64 * 1024
	}
	end := tr.offset + size
	if end > tr.length {
		end = tr.length
	}
	chunk := tr.data[tr.offset:end]
	tr.attempt++
	req := &InstallSnapshotRequest{
		Term:              n.term,
		LeaderID:          n.cfg.ID,
		LastIncludedIndex: tr.meta.LastIncludedIndex,
		LastIncludedTerm:  tr.meta.LastIncludedTerm,
		Chunked:           true,
		TransferID:        tr.id,
		Attempt:           tr.attempt,
		Offset:            tr.offset,
		Length:            tr.length,
		Checksum:          tr.checksum,
		Data:              append([]byte(nil), chunk...),
		Done:              end == tr.length,
	}
	tr.inflight = true
	tr.elapsed = 0
	n.sendSnapshotChunk(peer, req)
}

// tickSnapshotTransfers 重传超时未确认的块，并推进退避计时。
func (n *Node) tickSnapshotTransfers() {
	if n.role != RoleLeader {
		n.snapXfers = make(map[NodeID]*snapshotTransfer)
		return
	}
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		peer := p
		tr := n.snapXfers[peer]
		if tr != nil {
			if !tr.done {
				n.tickOneTransfer(peer, tr)
			}
			continue
		}
		// 没有会话且 peer 的 match 尚未到快照点：发起/续传追赶。
		if n.matchIndex[peer] < n.snapMeta.LastIncludedIndex {
			n.sendSnapshot(peer)
		}
	}
}

// tickOneTransfer 推进单个快照会话的超时/退避计时。
func (n *Node) tickOneTransfer(peer NodeID, tr *snapshotTransfer) {
	if tr.backoff > 0 {
		tr.backoff--
		if tr.backoff == 0 {
			// 退避结束（例如对端已 ResumeCatchUp）：立即续传探测。
			n.startSnapshotTransfer(peer)
		}
		return
	}
	if tr.inflight {
		tr.elapsed++
		if tr.elapsed >= n.cfg.SnapshotRetryTicks {
			n.debugf("snapshot chunk timeout peer=%s offset=%d attempt=%d, retransmit",
				peer, tr.offset, tr.attempt)
			tr.inflight = false
			tr.elapsed = 0
			n.transmitChunk(peer, tr) // transmitChunk 会把 attempt+1，旧响应因此失效
		}
	}
}

// dropTransfer 丢弃某 peer 的发送会话。
func (n *Node) dropTransfer(peer NodeID) { delete(n.snapXfers, peer) }

// handleSnapshotResponse 统一处理快照 RPC 的异步响应（旧版整体 + 分块）。
func (n *Node) handleSnapshotResponse(peer NodeID, r *installResult) {
	if r.term > n.term {
		n.becomeFollower(r.term)
		return
	}
	if !r.chunked {
		// 旧版整体快照响应：复用 AppendEntries 的推进逻辑。
		if n.role == RoleLeader && r.term == n.term && r.success {
			n.matchIndex[peer] = r.lastIncluded
			n.nextIndex[peer] = r.lastIncluded + 1
			n.broadcastAppendEntries()
		}
		return
	}
	if n.role != RoleLeader || r.term != n.term {
		return // 过期响应
	}
	tr := n.snapXfers[peer]
	if tr == nil || tr.done {
		return
	}
	// 会话相关性：领导权变化、压缩换会话、取消重开都会产生新 TransferID；
	// 旧会话迟到的响应绝不能改动当前会话（否则会用旧快照的进度“顶掉”新快照，
	// 造成 matchIndex 虚报、随后陷入 AppendEntries 拒绝风暴）。
	if r.transferID != tr.id {
		n.debugf("drop stale snapshot response peer=%s respTransfer=%d curTransfer=%d",
			peer, r.transferID, tr.id)
		return
	}
	// 发送相关性：正常流程下收到响应时必然有一个在途块（请求超时会立即重传）。
	// 重复投递（网络 Duplicate）或超时重传后的迟到响应，其 attempt 与当前在途
	// 块不符时一律忽略，避免同一个成功被应用两次、或用旧块进度错误推进 offset。
	if !tr.inflight || r.attempt != tr.attempt {
		n.debugf("drop non-correlated chunk response peer=%s resp=%d cur=%d inflight=%v",
			peer, r.attempt, tr.attempt, tr.inflight)
		return
	}

	// 接收代际：对端 CancelCatchUp 之后 epoch 递增。任何响应（成功、resync、
	// cancelled…）都先做代际校验与学习——取消通知本身必须把领导者已知代际推到
	// 新值，随后晚到的取消前“成功/完成”才会因 epoch 更小而被丢弃，
	// 不会被静默当成恢复后的进展。新会话的第一个响应负责学习当前代际。
	if tr.haveEpoch && r.epoch < tr.epoch {
		n.debugf("drop snapshot response from stale epoch peer=%s respEpoch=%d curEpoch=%d",
			peer, r.epoch, tr.epoch)
		return
	}
	tr.haveEpoch = true
	tr.epoch = r.epoch
	// 跨会话记忆：之后即便压缩换了新会话，也仍然认得对端最新代际。
	n.knownEpochOK[peer] = true
	n.knownEpoch[peer] = r.epoch

	if r.success {
		tr.inflight = false
		tr.elapsed = 0
		// 对端可能刚刚 ResumeCatchUp：正常 ACK 立即解除退避。
		tr.backoff = 0
		if r.complete {
			// 整份快照已安装：推进 match/next 后，会话记录标记完成并释放字节。
			// 若领导者在此期间又压缩出了更新的快照（对 peer 而言仍落后），
			// 不能把会话保留为 done（那会让后续心跳用旧快照点错误地转 AE），
			// 而要移除会话，由下一轮以最新快照开新会话继续追赶。
			tr.done = true
			tr.inflight = false
			tr.offset = tr.length // 进度视图显示满字节数
			tr.data = nil         // 会话仅保留用于进度查询，释放整份快照字节
			if r.lastIncluded > n.matchIndex[peer] {
				n.matchIndex[peer] = r.lastIncluded
			}
			n.nextIndex[peer] = r.lastIncluded + 1
			n.debugf("peer %s installed snapshot through %d (%d bytes), resume log replication",
				peer, r.lastIncluded, tr.length)
			n.advanceCommitIndex()
			if n.matchIndex[peer] < n.snapMeta.LastIncludedIndex {
				// 钉住的旧快照已装完，但领导者又有了更新的快照：立刻开新会话，
				// 不保留 done 记录（否则进度视图与真实状态不符）。
				n.dropTransfer(peer)
				n.startSnapshotTransfer(peer)
				return
			}
			n.sendAppendEntries(peer)
			return
		}
		// 已确认到 ackOffset：单调推进发送位置后立即发下一块。
		// ackOffset 不能超过快照总长（防御异常对端）。
		if r.ackOffset > tr.length {
			r.ackOffset = tr.length
		}
		if r.ackOffset > tr.offset {
			tr.offset = r.ackOffset
		}
		n.transmitChunk(peer, tr)
		return
	}

	// 失败响应：根据原因决定重试策略。
	tr.inflight = false
	tr.elapsed = 0
	switch r.reason {
	case RejectResync:
		if r.lastIncluded == tr.meta.LastIncludedIndex {
			// 同一快照身份（丢包/乱序/重启后会话丢失，但磁盘上的连续字节还在）：
			// 直接跳到对端已 durable 的 AckOffset 续传，绝不再从头重发。
			seek := r.ackOffset
			if seek > tr.length {
				seek = tr.length
			}
			n.debugf("peer %s resync snapshot %d: seek to ackOffset=%d (keep %d bytes)",
				peer, r.lastIncluded, seek, seek)
			tr.offset = seek
			n.transmitChunk(peer, tr)
		} else {
			// 对端的临时快照是另一个身份（例如旧领导留下的半份）：
			// 丢弃本会话，用当前最新快照从 0 开新会话。
			n.debugf("peer %s resync for other snapshot %d (mine %d): restart from 0",
				peer, r.lastIncluded, tr.meta.LastIncludedIndex)
			n.dropTransfer(peer)
			n.startSnapshotTransfer(peer)
		}
	case RejectTransferFailed:
		// 对端丢弃了临时快照（校验/落盘失败）：身份不变，从 0 重发本快照。
		n.debugf("peer %s snapshot transfer failed (%s), restart from 0", peer, r.reason)
		tr.offset = 0
		n.transmitChunk(peer, tr)
	case RejectCancelled:
		// 用户取消：有限退避。退避结束自动探测；对端若已 ResumeCatchUp，
		// 首个正常 ACK 会立刻清零退避并全速续传。
		// 恢复后对端会清空 staging，因此发送位置回到 0。
		// 关键：保留从 cancelled 响应学到的 epoch——取消前在途的旧代际
		// “成功/完成”响应晚到时必须因 epoch 更小而被丢弃，不能被静默当成成功。
		tr.offset = 0
		tr.backoff = n.cfg.SnapshotRetryTicks * 4
		n.debugf("peer %s catch-up cancelled (epoch=%d), backing off %d ticks",
			peer, tr.epoch, tr.backoff)
	case RejectStaleTerm:
		// 已在开头处理过高任期；同任期不会返回 stale，忽略等下一轮。
	default:
		// 其它拒绝：下一 tick 重发当前块。
	}
}

// checksumEqual 在事件循环内比较校验和。
func checksumEqual(a, b []byte) bool { return bytes.Equal(a, b) }

// ---- 接收侧与用户视图在文件后半部分实现（见下方接收者方法） ----

// ---- 跟随者：接收侧 ----

// restoreIncoming 在启动恢复阶段重建未完成的接收会话状态。
// 仅恢复进度用于续传；无论磁盘上临时文件处于什么状态，都绝不把它安装为正式快照。
func (n *Node) restoreIncoming() error {
	m, ok, err := n.store.LoadStagingMeta()
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// 崩溃窗口：快照可能已经原子提升为正式快照、但 staging 尚未清理。
	// 此时临时快照已经过时（甚至已安装），直接清理，绝不能据此报告 syncing。
	if m.LastIncludedIndex <= n.snapMeta.LastIncludedIndex {
		_ = n.store.ClearStaging()
		return nil
	}
	size, err := n.store.StagingSize()
	if err != nil {
		return err
	}
	if size > m.Length {
		// 异常数据：直接丢弃，等领导者重新传输。
		_ = n.store.ClearStaging()
		return nil
	}
	n.incoming = &incomingSnapshot{
		meta:     m,
		received: size,
		state:    CatchUpSyncing,
	}
	n.logf("[%s] recovered staged snapshot index=%d bytes=%d/%d",
		n.cfg.ID, m.LastIncludedIndex, size, m.Length)
	return nil
}

// identityMatches 判断请求快照身份与当前临时快照是否完全一致。
func (in *incomingSnapshot) identityMatches(req *InstallSnapshotRequest) bool {
	return in.meta.LastIncludedIndex == req.LastIncludedIndex &&
		in.meta.LastIncludedTerm == req.LastIncludedTerm &&
		in.meta.Length == req.Length &&
		checksumEqual(in.meta.Checksum, req.Checksum)
}

// ack 用当前断点成功应答。注意：TransferID/Attempt/Epoch 必须回传“请求”里的
// 值（resp 在入口已按 req 初始化），不能回写 staging 元数据里的旧会话 ID——
// 接收端重启或领导权切换后，本地暂存的 TransferID 可能属于上一世代，
// 回写它会让新领导者把本应有效的断点 ACK 当成过期响应永久丢弃。
func (in *incomingSnapshot) ack(resp *InstallSnapshotResponse) {
	resp.Success = true
	resp.Chunked = true
	resp.LastIncluded = in.meta.LastIncludedIndex
	resp.AckOffset = in.received
}

// discardIncoming 丢弃临时快照（磁盘 + 内存），进入失败态。
func (n *Node) discardIncoming(reason RejectReason) {
	_ = n.store.ClearStaging()
	if n.incoming != nil {
		n.incoming.received = 0
		n.incoming.state = CatchUpFailed
		n.incoming.reason = reason
	}
}

// handleChunkedInstallSnapshot 在事件循环内处理一个分块快照请求。
func (n *Node) handleChunkedInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	resp := &InstallSnapshotResponse{Term: n.term, Peer: n.cfg.ID, Chunked: true,
		LastIncluded: req.LastIncludedIndex, TransferID: req.TransferID,
		Attempt: req.Attempt, Epoch: n.catchupEpoch}

	// 任期检查与角色让位（与整体快照路径一致）。
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

	// 网络回调与本事件循环共享请求对象：先复制块字节，
	// 后续任何处理都不会（通过共享底层数组）改到发送方的快照数据。
	if len(req.Data) > 0 {
		req.Data = append([]byte(nil), req.Data...)
	}

	// 快照不比本地新：幂等应答“已完成”，领导者据此转常规日志复制。
	// 注意：不能因为本地有更新的快照就拒绝——领导权刚切换时这是正常路径。
	if req.LastIncludedIndex <= n.snapMeta.LastIncludedIndex {
		resp.Success = true
		resp.Complete = true
		resp.AckOffset = req.Length
		return resp
	}

	in := n.incoming
	identityMatch := in != nil && in.identityMatches(req)

	// 已结束的本地会话（取消/失败）：只有同一快照的首块可以开启新一轮。
	if in != nil && (in.state == CatchUpCancelled || in.state == CatchUpFailed) {
		if in.state == CatchUpCancelled {
			// ResumeCatchUp 之前，任何块都拒绝；恢复后通常已清空 incoming，
			// 这里兜底处理“取消期间领导者仍在重发”的竞态。
			resp.Reason = RejectCancelled
			return resp
		}
		if !identityMatch || req.Offset != 0 {
			// 失败态：等领导者从 0 重发同一快照；身份变化也必须从首块开始。
			resp.Reason = RejectResync
			if identityMatch {
				in.ack(resp)
				resp.Success = false
			}
			return resp
		}
		// 同一快照首块：丢弃失败的临时数据，落到下方“新会话首块”流程。
		_ = n.store.ClearStaging()
		in = nil
	}

	newIdentity := in == nil || !identityMatch

	switch {
	case newIdentity && req.Offset != 0:
		// 身份变了（领导者压缩/本端重启后会话已清），但对方发的不是首块：
		// 明确要求从 0 重发，绝不拼接不同快照的字节。
		// LastIncluded/AckOffset 回报本端旧临时快照的断点，供领导者诊断与续传决策。
		n.debugf("snapshot identity mismatch at offset=%d (want restart at 0)", req.Offset)
		resp.Reason = RejectResync
		if in != nil {
			resp.LastIncluded = in.meta.LastIncludedIndex
			resp.AckOffset = in.received
		}
		return resp
	case newIdentity:
		// 全新快照的首块：丢弃旧临时数据，从头开始。
		if n.incoming != nil {
			_ = n.store.ClearStaging()
		}
		meta := StagingMeta{
			TransferID:        req.TransferID,
			LastIncludedIndex: req.LastIncludedIndex,
			LastIncludedTerm:  req.LastIncludedTerm,
			Length:            req.Length,
			Checksum:          append([]byte(nil), req.Checksum...),
			Leader:            req.LeaderID,
		}
		if err := n.store.StartStagingData(); err != nil {
			n.logf("[%s] start staging data error: %v", n.cfg.ID, err)
			n.incoming = &incomingSnapshot{meta: meta, state: CatchUpFailed, reason: RejectTransferFailed}
			resp.Reason = RejectTransferFailed
			return resp
		}
		if err := n.store.SaveStagingMeta(meta); err != nil {
			n.logf("[%s] save staging meta error: %v", n.cfg.ID, err)
			resp.Reason = RejectTransferFailed
			return resp
		}
		in = &incomingSnapshot{meta: meta, state: CatchUpSyncing}
		n.incoming = in
	}

	// 身份一致：处理重复 / 乱序。
	end := req.Offset + uint64(len(req.Data))
	if end > req.Length {
		n.debugf("snapshot chunk overruns length: offset=%d size=%d length=%d",
			req.Offset, len(req.Data), req.Length)
		n.discardIncoming(RejectTransferFailed)
		resp.Reason = RejectTransferFailed
		return resp
	}
	if req.Offset < in.received {
		// 重复块（完全或部分落在已收区间）：幂等 ACK 当前断点即可。
		// 若与已收区间重叠且跨过断点，则截断出新部分再追加；通常重传的是同一块，直接忽略。
		if req.Offset+uint64(len(req.Data)) <= in.received {
			in.ack(resp)
			return resp
		}
		req.Data = req.Data[in.received-req.Offset:]
		req.Offset = in.received
	}
	if req.Offset != in.received {
		// 乱序块（出现空洞）：不缓存、不写入，要求从断点重发。
		n.debugf("snapshot gap: got offset=%d want=%d -> resync", req.Offset, in.received)
		resp.Reason = RejectResync
		in.ack(resp)
		resp.Success = false
		return resp
	}

	// 落盘这一块（fsync 后才应答）。
	if err := n.store.AppendStagingChunk(req.Offset, req.Data); err != nil {
		n.logf("[%s] append staging chunk error: %v", n.cfg.ID, err)
		n.discardIncoming(RejectTransferFailed)
		resp.Reason = RejectTransferFailed
		return resp
	}
	in.received = req.Offset + uint64(len(req.Data))
	in.state = CatchUpSyncing

	if !req.Done {
		in.ack(resp)
		return resp
	}

	// 末块：必须收齐，否则（Done 提前）按失败处理，要求重发。
	if in.received != req.Length {
		n.debugf("snapshot done before complete: got=%d length=%d -> resync",
			in.received, req.Length)
		resp.Reason = RejectResync
		in.ack(resp)
		resp.Success = false
		return resp
	}

	// 整体校验：重新读出临时文件计算 SHA-256（重启后也是同一条路径）。
	data, err := n.store.LoadStagingData()
	if err != nil {
		n.logf("[%s] load staging data error: %v", n.cfg.ID, err)
		n.discardIncoming(RejectTransferFailed)
		resp.Reason = RejectTransferFailed
		return resp
	}
	if uint64(len(data)) != req.Length || !checksumEqual(sha256Sum(data), req.Checksum) {
		n.logf("[%s] staging checksum mismatch, discarding %d bytes", n.cfg.ID, len(data))
		n.discardIncoming(RejectTransferFailed)
		resp.Reason = RejectTransferFailed
		return resp
	}

	// 校验通过：原子提升为正式快照并恢复状态机。
	meta := SnapshotMeta{LastIncludedIndex: req.LastIncludedIndex, LastIncludedTerm: req.LastIncludedTerm}
	if err := n.commitSnapshotInstall(meta, data); err != nil {
		n.logf("[%s] commit snapshot install error: %v", n.cfg.ID, err)
		n.discardIncoming(RejectTransferFailed)
		resp.Reason = RejectTransferFailed
		return resp
	}
	// 安装成功后才清理临时数据。
	_ = n.store.ClearStaging()
	in.state = CatchUpCaughtUp
	in.reason = RejectNone

	n.debugf("installed chunked snapshot through index=%d (%d bytes), resume log replication",
		meta.LastIncludedIndex, req.Length)
	resp.Success = true
	resp.Complete = true
	resp.AckOffset = in.received
	resp.LastIncluded = meta.LastIncludedIndex
	return resp
}

// ---- 仅事件循环调用的视图 ----

func (n *Node) catchUpProgressLocked() CatchUpProgress {
	if n.incoming == nil {
		// 已安装过快照且没有新的追赶：对用户呈现 idle。
		return CatchUpProgress{State: CatchUpIdle, SnapshotIdx: n.snapMeta.LastIncludedIndex}
	}
	in := n.incoming
	return CatchUpProgress{
		State:        in.state,
		SnapshotIdx:  in.meta.LastIncludedIndex,
		Received:     in.received,
		Total:        in.meta.Length,
		Leader:       in.meta.Leader,
		TransferID:   in.meta.TransferID,
		FailedReason: in.reason,
	}
}

func (n *Node) snapshotTransfersLocked() map[NodeID]CatchUpProgress {
	out := make(map[NodeID]CatchUpProgress, len(n.snapXfers))
	for peer, tr := range n.snapXfers {
		state := CatchUpSyncing
		switch {
		case tr.done:
			state = CatchUpCaughtUp
		case tr.backoff > 0:
			state = CatchUpCancelled
		}
		out[peer] = CatchUpProgress{
			State:       state,
			SnapshotIdx: tr.meta.LastIncludedIndex,
			Received:    tr.offset,
			Total:       tr.length,
			TransferID:  tr.id,
			Leader:      n.cfg.ID,
		}
	}
	return out
}

// cancelCatchUpLocked 取消本机进行中的接收。临时数据保留（恢复后可继续清理），
// 但领导者之后的块一律被拒绝，直到 resumeCatchUpLocked。
func (n *Node) cancelCatchUpLocked() error {
	n.catchupEpoch++ // 取消/恢复构成新的接收代际（持久化，跨重启仍有效）
	n.persistState()
	if n.incoming == nil {
		// 没有进行中的传输：记录一个取消标记，使紧接着到来的首块也被拒绝。
		n.incoming = &incomingSnapshot{state: CatchUpCancelled}
		return nil
	}
	n.incoming.state = CatchUpCancelled
	n.incoming.reason = RejectCancelled
	n.debugf("snapshot catch-up cancelled by user at %d/%d bytes (epoch=%d)",
		n.incoming.received, n.incoming.meta.Length, n.catchupEpoch)
	return nil
}

// resumeCatchUpLocked 解除取消：丢弃可能残留的失败/取消会话，让领导者从 0 重新开始。
// 取消时递增的 epoch 不回退：取消期间发出的旧代际成功响应在领导者侧仍然作废，
// 不会把“取消前的在途成功”悄悄当成“恢复后的成功”。
func (n *Node) resumeCatchUpLocked() error {
	if n.incoming != nil && n.incoming.state == CatchUpCancelled {
		_ = n.store.ClearStaging()
		n.incoming = nil
		n.debugf("snapshot catch-up resumed by user (epoch=%d retained)", n.catchupEpoch)
	}
	return nil
}

// ---- 用户侧 API（经事件循环读取，保证一致视图） ----

func (n *Node) CatchUpProgress() CatchUpProgress {
	ch := make(chan CatchUpProgress, 1)
	select {
	case n.events <- internalEvent{kind: eventRPC, from: n.cfg.ID,
		rpc: catchupCall{op: catchupProgress, res: ch}}:
	case <-n.stopCh:
		return CatchUpProgress{State: CatchUpIdle}
	}
	select {
	case p := <-ch:
		return p
	case <-n.stopCh:
		return CatchUpProgress{State: CatchUpIdle}
	}
}

func (n *Node) SnapshotTransfers() map[NodeID]CatchUpProgress {
	ch := make(chan map[NodeID]CatchUpProgress, 1)
	select {
	case n.events <- internalEvent{kind: eventRPC, from: n.cfg.ID, rpc: transfersCall{res: ch}}:
	case <-n.stopCh:
		return map[NodeID]CatchUpProgress{}
	}
	select {
	case m := <-ch:
		return m
	case <-n.stopCh:
		return map[NodeID]CatchUpProgress{}
	}
}

func (n *Node) controlCatchUp(op catchupOp) error {
	done := make(chan error, 1)
	select {
	case n.events <- internalEvent{kind: eventRPC, from: n.cfg.ID,
		rpc: catchupCall{op: op, done: done}}:
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

// CancelCatchUp 取消本机进行中的快照追赶（被取消后领导者的块会被拒绝，直到 ResumeCatchUp）。
func (n *Node) CancelCatchUp() error { return n.controlCatchUp(catchupCancel) }

// ResumeCatchUp 解除取消状态；领导者下一次重发即从断点继续。
func (n *Node) ResumeCatchUp() error { return n.controlCatchUp(catchupResume) }
