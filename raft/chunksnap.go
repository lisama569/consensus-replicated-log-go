package raft

import (
	"context"
	"fmt"
	"hash/crc32"
)

// 本文件实现「可恢复的大快照分块追赶」：
//
// 设计要点：
//   - 大快照被切分为固定大小的块，逐块发送（InstallSnapshotChunk RPC）；
//   - 接收端把「会话检查点 + 已确认字节前缀」持久化，崩溃/重启/换主后可从
//     已确认位置续传，不从头重来；
//   - 重复块、乱序块、会话被取代/取消都只影响在途会话，半份数据永远不会被安装；
//   - 只有收齐全部字节且 CRC 校验通过，才原子地安装快照（复用整份安装路径）。

// CatchupState 是用户可查询的快照追赶状态。
type CatchupState int

const (
	CatchupIdle      CatchupState = iota // 无追赶任务（节点就绪）
	CatchupReceiving                     // 正在接收快照块
	CatchupDone                          // 最近一次追赶已完成（已追平）
	CatchupFailed                        // 最近一次追赶失败（如校验错误）
	CatchupCancelled                     // 最近一次追赶被取消/被更新进度取代
)

func (s CatchupState) String() string {
	switch s {
	case CatchupIdle:
		return "Idle"
	case CatchupReceiving:
		return "Receiving"
	case CatchupDone:
		return "Done"
	case CatchupFailed:
		return "Failed"
	case CatchupCancelled:
		return "Cancelled"
	default:
		return "Unknown"
	}
}

// CatchupStatus 是接收端一次快照追赶的可观测进度。
type CatchupStatus struct {
	State     CatchupState // 当前状态
	SessionID string       // 在途/最近会话 ID
	Target    Index        // 追赶目标快照点（LastIncludedIndex）
	Received  uint64       // 已确认接收（已持久化）的字节数
	Total     uint64       // 快照总字节数
	LastError string       // 失败原因（State=Failed 时有效）
}

// Ready 报告节点是否不处于追赶中（空闲或已追平）。
func (s CatchupStatus) Ready() bool {
	return s.State == CatchupIdle || s.State == CatchupDone
}

// incomingSnapshot 是接收端的在途分块会话（检查点会持久化）。
type incomingSnapshot struct {
	SessionID string
	Meta      SnapshotMeta
	Term      Term   // 创建会话的领导者任期（同任期才比较进度新旧）
	Total     uint64 // 快照总字节
	Received  uint64 // 已确认连续前缀长度
	CRC       uint32 // 发送端给出的整体 CRC32 校验和
}

// outgoingSnapshot 是领导者对某个 peer 的在途分块发送状态（易失，不持久化）。
type outgoingSnapshot struct {
	SessionID  string
	Meta       SnapshotMeta
	Data       []byte // 会话期间缓存的快照字节
	Total      uint64
	CRC        uint32
	NextOffset uint64 // 下一个待发送偏移（按接收端确认推进）
	InFlight   bool   // 是否有一块在途（每 peer 至多一块）
	LastSend   int    // 最近一次发送时的逻辑时钟（用于重传超时）
}

// ---- 接收端 ----

// handleInstallSnapshotChunk 在事件循环内处理一个快照块。
func (n *Node) handleInstallSnapshotChunk(req *InstallSnapshotChunkRequest) *InstallSnapshotChunkResponse {
	resp := &InstallSnapshotChunkResponse{Term: n.term, Peer: n.cfg.ID}

	// 任期落后：拒绝（与其他入站 RPC 一致）。
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

	// 本地快照已覆盖该会话目标（可能追赶期间本地压缩推进了）：
	// 幂等确认全部字节让发送端正常收尾，并清理已无意义的在途会话。
	if req.LastIncludedIndex <= n.snapMeta.LastIncludedIndex {
		if n.incoming != nil && n.incoming.Meta.LastIncludedIndex <= n.snapMeta.LastIncludedIndex {
			n.cancelIncoming(CatchupCancelled, "superseded by local snapshot")
		}
		resp.Success = true
		resp.NextOffset = req.TotalSize
		return resp
	}

	// 收到不同会话：仅在「更新的任期」或「同任期快照点更新/相同」时取代，
	// 旧领导者的过期会话不得覆盖正在接收的更新进度。
	if n.incoming != nil && n.incoming.SessionID != req.SessionID {
		newer := req.Term > n.incoming.Term ||
			(req.Term == n.incoming.Term && req.LastIncludedIndex >= n.incoming.Meta.LastIncludedIndex)
		if !newer {
			resp.Success = false
			resp.Reason = RejectSnapshotStale
			resp.NextOffset = n.incoming.Received
			return resp
		}
		n.cancelIncoming(CatchupCancelled, "superseded by newer snapshot session")
	}

	// 新会话：先清掉可能残留的孤儿部分数据，再登记并持久化检查点。
	if n.incoming == nil {
		if err := n.store.ClearIncoming(); err != nil {
			n.logf("[%s] clear incoming error: %v", n.cfg.ID, err)
		}
		n.incoming = &incomingSnapshot{
			SessionID: req.SessionID,
			Meta: SnapshotMeta{
				LastIncludedIndex: req.LastIncludedIndex,
				LastIncludedTerm:  req.LastIncludedTerm,
			},
			Term:  n.term,
			Total: req.TotalSize,
			CRC:   req.CRC,
		}
		n.catchup = CatchupStatus{
			State:     CatchupReceiving,
			SessionID: req.SessionID,
			Target:    req.LastIncludedIndex,
			Total:     req.TotalSize,
		}
		n.persistIncoming()
	}

	inc := n.incoming
	// 同会话但关键参数不一致（协议违规，防御性处理）：判失败并丢弃部分数据。
	if inc.Total != req.TotalSize || inc.CRC != req.CRC ||
		inc.Meta.LastIncludedIndex != req.LastIncludedIndex ||
		inc.Meta.LastIncludedTerm != req.LastIncludedTerm {
		n.failIncoming("inconsistent chunk parameters in same session")
		resp.Success = false
		resp.Reason = RejectSnapshotGap
		resp.NextOffset = 0
		return resp
	}
	resp.NextOffset = inc.Received

	// 重复块（偏移落在已确认前缀内）：幂等确认，不重复写盘。
	if req.Offset < inc.Received {
		resp.Success = true
		return resp
	}
	// 乱序/空洞：告诉发送端期望的连续偏移，等待重发，不丢弃已有前缀。
	if req.Offset > inc.Received {
		resp.Success = false
		resp.Reason = RejectSnapshotGap
		return resp
	}

	// 连续追加：先 fsync 数据，再更新检查点（崩溃后至多重写一块，绝不少收）。
	if err := n.store.AppendIncomingChunk(req.Offset, req.Data); err != nil {
		n.failIncoming("persist chunk: " + err.Error())
		resp.Success = false
		resp.Reason = RejectSnapshotGap
		resp.NextOffset = 0
		return resp
	}
	inc.Received += uint64(len(req.Data))
	n.persistIncoming()
	n.catchup.Received = inc.Received
	resp.Success = true
	resp.NextOffset = inc.Received

	// 最后一块：收齐后做整体 CRC 校验，通过才安装；半份数据永远不会被当成可用状态。
	if req.Done {
		n.completeIncoming(resp)
	}
	return resp
}

// completeIncoming 在收齐全部字节后校验并安装快照。
func (n *Node) completeIncoming(resp *InstallSnapshotChunkResponse) {
	inc := n.incoming
	meta := inc.Meta
	total := inc.Total

	if inc.Received != total {
		// Done 已到但字节不全（正常不会发生：连续接收保证 Done 即末尾）。
		n.failIncoming("done flag with incomplete data")
		resp.Success = false
		resp.Reason = RejectSnapshotGap
		resp.NextOffset = 0
		return
	}
	_, data, err := n.store.LoadIncoming()
	if err != nil {
		n.failIncoming("reload partial data: " + err.Error())
		resp.Success = false
		resp.Reason = RejectSnapshotGap
		resp.NextOffset = 0
		return
	}
	if got := crc32.ChecksumIEEE(data); got != inc.CRC {
		n.failIncoming("crc mismatch")
		resp.Success = false
		resp.Reason = RejectSnapshotGap
		resp.NextOffset = 0
		return
	}

	// 安装前最后一次防过期检查：追赶期间本地压缩可能已推进到更新位置。
	if meta.LastIncludedIndex <= n.snapMeta.LastIncludedIndex {
		n.cancelIncoming(CatchupCancelled, "superseded by local snapshot before install")
		resp.Success = true
		resp.NextOffset = total
		return
	}

	n.incoming = nil
	if err := n.store.ClearIncoming(); err != nil {
		n.logf("[%s] clear incoming after completion error: %v", n.cfg.ID, err)
	}
	if !n.installSnapshot(meta, data) {
		n.catchup = CatchupStatus{
			State:     CatchupFailed,
			SessionID: inc.SessionID,
			Target:    meta.LastIncludedIndex,
			Received:  total,
			Total:     total,
			LastError: "install snapshot failed",
		}
		resp.Success = false
		resp.Reason = RejectSnapshotGap
		resp.NextOffset = 0
		return
	}
	n.catchup = CatchupStatus{
		State:     CatchupDone,
		SessionID: inc.SessionID,
		Target:    meta.LastIncludedIndex,
		Received:  total,
		Total:     total,
	}
	n.debugf("chunked snapshot installed: session=%s index=%d bytes=%d",
		inc.SessionID, meta.LastIncludedIndex, total)
	resp.Success = true
	resp.NextOffset = total
}

// persistIncoming 把当前在途会话写为检查点。
func (n *Node) persistIncoming() {
	inc := n.incoming
	if err := n.store.SaveIncomingCheckpoint(persistedIncoming{
		SessionID:         inc.SessionID,
		LastIncludedIndex: inc.Meta.LastIncludedIndex,
		LastIncludedTerm:  inc.Meta.LastIncludedTerm,
		Term:              inc.Term,
		Total:             inc.Total,
		Received:          inc.Received,
		CRC:               inc.CRC,
	}); err != nil {
		n.logf("[%s] persist incoming checkpoint error: %v", n.cfg.ID, err)
	}
}

// cancelIncoming 放弃在途会话并清理磁盘上的部分数据（取消/被取代时）。
func (n *Node) cancelIncoming(state CatchupState, reason string) {
	if n.incoming == nil {
		return
	}
	inc := n.incoming
	n.incoming = nil
	if err := n.store.ClearIncoming(); err != nil {
		n.logf("[%s] clear incoming on cancel error: %v", n.cfg.ID, err)
	}
	n.catchup = CatchupStatus{
		State:     state,
		SessionID: inc.SessionID,
		Target:    inc.Meta.LastIncludedIndex,
		Received:  inc.Received,
		Total:     inc.Total,
		LastError: reason,
	}
	n.debugf("incoming snapshot cancelled: session=%s state=%s reason=%s",
		inc.SessionID, state, reason)
}

// failIncoming 将在途会话标记为失败并丢弃部分数据（校验/落盘错误）。
func (n *Node) failIncoming(reason string) {
	n.cancelIncoming(CatchupFailed, reason)
}

// ---- 用户侧查询与控制 ----

type catchupStatusCall struct{ res chan<- CatchupStatus }

type cancelCatchupCall struct{ res chan<- bool }

// CatchupStatus 返回本节点快照追赶的可观测进度（经事件循环读取，视图一致）。
// State 区分：Idle（无追赶）、Receiving（正在追赶）、Done（已追平）、
// Failed（失败）、Cancelled（被取消/被更新进度取代）。
func (n *Node) CatchupStatus() CatchupStatus {
	ch := make(chan CatchupStatus, 1)
	select {
	case n.events <- internalEvent{kind: eventRPC, from: n.cfg.ID, rpc: catchupStatusCall{res: ch}}:
	case <-n.stopCh:
		return CatchupStatus{State: CatchupIdle}
	}
	select {
	case s := <-ch:
		return s
	case <-n.stopCh:
		return CatchupStatus{State: CatchupIdle}
	}
}

// CancelCatchup 取消当前在途的快照接收：丢弃部分数据并把状态置为 Cancelled。
// 返回 false 表示当前没有在途会话。
// 注意：取消是一次性的本地中止，不阻止领导者之后重新发起追赶；
// 半份数据在任何情况下都不会被安装。
func (n *Node) CancelCatchup() bool {
	ch := make(chan bool, 1)
	select {
	case n.events <- internalEvent{kind: eventRPC, from: n.cfg.ID, rpc: cancelCatchupCall{res: ch}}:
	case <-n.stopCh:
		return false
	}
	select {
	case ok := <-ch:
		return ok
	case <-n.stopCh:
		return false
	}
}

// ---- 发送端（领导者） ----

// chunkResendTicks 是块在途超时：超时后按当前 NextOffset 重发（覆盖丢包/丢响应）。
// 静默窗口由 sendSnapshotHeartbeat 兜底，不会触发接收端选举。
const chunkResendTicks = 15

// snapshotSessionID 由快照内容派生稳定会话 ID：
// 不同领导者持有同一份快照（同 index/term 且状态机确定性 => 内容一致）时
// 会话 ID 相同，因此换主后接收端仍可续传，而不必从头重传。
func snapshotSessionID(meta SnapshotMeta, crc uint32) string {
	return fmt.Sprintf("%d:%d:%08x", meta.LastIncludedIndex, meta.LastIncludedTerm, crc)
}

// sendSnapshotChunked 向 peer 发起或继续一次分块快照传输，返回本轮是否发出了块。
// data 是当前快照字节；若已有在途会话则沿用其（可能更旧的）快照，
// 当前快照更新时由后续传输轮次自然切换。
func (n *Node) sendSnapshotChunked(peer NodeID, data []byte) bool {
	if n.role != RoleLeader {
		return false
	}
	out := n.snapOut[peer]
	// 本地快照已比在途会话更新、且接收端尚未确认任何字节（会话是在对端
	// 离线期间空转的）：放弃过期会话，直接用新快照重新开始，避免传输
	// 注定要被取代的旧数据。已有确认进度的会话则传完为止——持续写入下
	// 快照不断推进，反复重启未完成的传输会导致追赶活锁。
	if out != nil && out.NextOffset == 0 &&
		n.snapMeta.LastIncludedIndex > out.Meta.LastIncludedIndex {
		n.debugf("drop stale chunked session %s to %s (snapshot advanced to %d)",
			out.SessionID, peer, n.snapMeta.LastIncludedIndex)
		delete(n.snapOut, peer)
		out = nil
	}
	if out == nil {
		if len(data) == 0 {
			return false
		}
		crc := crc32.ChecksumIEEE(data)
		out = &outgoingSnapshot{
			SessionID:  snapshotSessionID(n.snapMeta, crc),
			Meta:       n.snapMeta,
			Data:       data,
			Total:      uint64(len(data)),
			CRC:        crc,
			NextOffset: 0,
		}
		n.snapOut[peer] = out
		n.debugf("start chunked snapshot to %s: session=%s bytes=%d chunk=%d",
			peer, out.SessionID, out.Total, n.cfg.SnapshotChunkSize)
	}

	// 一块在途且未超时：等响应，不并发发送第二块。
	if out.InFlight && n.tickCount-out.LastSend < chunkResendTicks {
		return false
	}

	off := out.NextOffset
	if off > out.Total {
		off = out.Total
	}
	end := off + uint64(n.cfg.SnapshotChunkSize)
	if end > out.Total {
		end = out.Total
	}
	done := end == out.Total
	chunk := append([]byte(nil), out.Data[off:end]...)

	req := &InstallSnapshotChunkRequest{
		Term:              n.term,
		LeaderID:          n.cfg.ID,
		SessionID:         out.SessionID,
		LastIncludedIndex: out.Meta.LastIncludedIndex,
		LastIncludedTerm:  out.Meta.LastIncludedTerm,
		Offset:            off,
		TotalSize:         out.Total,
		CRC:               out.CRC,
		Data:              chunk,
		Done:              done,
	}
	out.InFlight = true
	out.LastSend = n.tickCount

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		resp := n.tr.SendInstallSnapshotChunk(context.Background(), peer, req)
		if resp == nil {
			// 丢包/对端崩溃：不投递任何事件，InFlight 保持，超时后重发。
			return
		}
		select {
		case n.events <- internalEvent{
			kind: eventRPC,
			rpc:  snapshotChunkResponse{peer: peer, sessionID: out.SessionID, resp: resp},
		}:
		case <-n.stopCh:
		}
	}()
	return true
}

// handleSnapshotChunkResponse 在事件循环内处理块应答（断点续传核心）。
func (n *Node) handleSnapshotChunkResponse(peer NodeID, sessionID string, resp *InstallSnapshotChunkResponse) {
	if resp.Term > n.term {
		n.debugf("chunk response from %s carries higher term %d -> step down", peer, resp.Term)
		n.becomeFollower(resp.Term)
		return
	}
	out := n.snapOut[peer]
	if n.role != RoleLeader || resp.Term != n.term || out == nil || out.SessionID != sessionID {
		return // 已下台/过期会话的迟到响应：忽略
	}
	out.InFlight = false

	if !resp.Success {
		switch resp.Reason {
		case RejectStaleTerm:
			// 同任期拒绝不应出现；防御性放弃，等待下一轮复制重试。
			delete(n.snapOut, peer)
		case RejectSnapshotStale:
			// 对端正在接收更新的快照（通常来自前任更新任期的领导者），
			// 本会话无法完成：放弃，由后续压缩/AppendEntries 重新驱动。
			n.debugf("peer %s rejects stale chunked session %s (has newer incoming)",
				peer, sessionID)
			delete(n.snapOut, peer)
		default:
			// 空洞/乱序：接收端给出已确认偏移，从那里续发（断点续传）。
			if resp.NextOffset < out.NextOffset {
				out.NextOffset = resp.NextOffset
			}
			n.sendSnapshotChunked(peer, nil)
		}
		return
	}

	// 成功：确认偏移单调推进（乱序/迟到的确认不得让进度倒退）。
	if resp.NextOffset > out.NextOffset {
		out.NextOffset = resp.NextOffset
	}
	if out.NextOffset < out.Total {
		n.sendSnapshotChunked(peer, nil)
		return
	}

	// 全部字节已确认并安装：推进 match/next，删除会话，回到正常日志复制。
	delete(n.snapOut, peer)
	n.matchIndex[peer] = out.Meta.LastIncludedIndex
	n.nextIndex[peer] = out.Meta.LastIncludedIndex + 1
	n.debugf("chunked snapshot to %s complete: session=%s index=%d",
		peer, out.SessionID, out.Meta.LastIncludedIndex)
	n.broadcastAppendEntries()
}
