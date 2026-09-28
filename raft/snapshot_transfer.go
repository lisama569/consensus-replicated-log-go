package raft

import (
	"bytes"
	"crypto/sha256"
	"time"
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

// ---- 接收侧异步落盘 ----
//
// 每个分块都要 fsync，若在事件循环里同步执行，慢盘/每块注入延迟时会把整个
// 事件循环卡住：心跳无法处理 -> 选举超时 -> 跟随者在追赶期间反复参选/让位，
// 领导权不断变化，传输会话随之一遍遍从头再来。
//
// 因此采用「事件循环内校验入队 -> 单 worker 串行落盘 -> 结果投回事件循环提交」：
// 协议判定与内存状态修改仍只在事件循环内发生，磁盘 I/O 不再阻塞心跳/选举处理。
// 单 worker 严格按入队顺序执行 reset->write->...->finalize->clear，杜绝并发写。

type chunkJobKind int

const (
	jobReset          chunkJobKind = iota // 截断重建临时数据文件（开始新身份）
	jobSaveMeta                           // 原子写临时快照元数据
	jobWrite                              // 在 offset 处追加一块
	jobFinalize                           // 全部收齐：读出整文件、校验长度与 SHA-256
	jobClear                              // 丢弃临时快照
	jobPersistCompact                     // 领导者压缩：原子落盘正式快照 + 重写日志后缀
	jobPersistInstall                     // 接收方安装：原子落盘正式快照 + 重写日志后缀（在 worker 内完成 fsync）
	jobPersistLog                         // 持久化日志后缀（常规追加/截断）
	jobBarrier                            // 刷盘屏障：worker 处理到它时关闭 barrierCh，代表此前任务都已 fsync
)

// chunkJob 是投递给串行落盘 worker 的任务。
type chunkJob struct {
	kind        chunkJobKind
	epoch       uint64
	offset      uint64
	data        []byte
	length      uint64
	sum         []byte
	meta        SnapshotMeta
	stagingMeta *StagingMeta
	log         []LogEntry    // jobPersistCompact/jobPersistInstall/jobPersistLog 的日志内容
	ack         bool          // 结果需要投回事件循环（false=纯后台写）
	syncDone    chan error    // 非 nil：worker 落盘后直接把错误投回此通道（同步等待路径）
	barrierCh   chan struct{} // jobBarrier：worker 执行到该任务（此前任务都已 fsync）时关闭
}

// chunkJobResult 是 worker 投回事件循环的结果。
type chunkJobResult struct {
	kind   chunkJobKind
	epoch  uint64
	offset uint64
	size   uint64
	length uint64
	data   []byte
	meta   SnapshotMeta
	log    []LogEntry
	err    error
}

// startChunkWorker 启动「快照数据块」高优先 worker：只处理 staging 文件的
// reset/meta/append/verify/clear。慢盘上这是追赶速度的关键路径，必须与
// 正式快照/日志的落盘互不排队。
func (n *Node) startChunkWorker() {
	n.wg.Add(1)
	go n.runStoreWorker(n.chunkJobs)
}

// startPersistWorker 启动「正式快照/日志」低优先 worker：处理
// compact/install/log 落盘。两个 worker 操作互不相交的文件集合，各自串行即可。
func (n *Node) startPersistWorker() {
	n.wg.Add(1)
	go n.runStoreWorker(n.persistJobs)
}

// runStoreWorker 串行执行落盘任务并按任务要求回送结果。
//
// 队列用“无缓冲投递 + 每节点单 worker”保证顺序，因此关闭时队列里不会积压大量
// 已入队任务；stopCh 关闭后至多完成当前一个 fsync 即退出，不会因排空大缓冲队列
// 而长时间拖住 Stop。
func (n *Node) runStoreWorker(q <-chan chunkJob) {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case job := <-q:
			r := chunkJobResult{kind: job.kind, epoch: job.epoch,
				offset: job.offset, length: job.length, meta: job.meta, log: job.log}
			// 慢盘延迟仅注入到数据块相关任务。
			if n.chunkIODelay > 0 && q == n.chunkJobs && (job.kind == jobWrite ||
				job.kind == jobReset || job.kind == jobSaveMeta || job.kind == jobFinalize) {
				select {
				case <-time.After(n.chunkIODelay):
				case <-n.stopCh:
					return
				}
			}
			switch job.kind {
			case jobReset:
				r.err = n.store.StartStagingData()
			case jobSaveMeta:
				if job.stagingMeta != nil {
					r.err = n.store.SaveStagingMeta(*job.stagingMeta)
				}
			case jobWrite:
				r.err = n.store.AppendStagingChunk(job.offset, job.data)
				r.size = uint64(len(job.data))
			case jobFinalize:
				r.data, r.err = n.store.LoadStagingData()
				if r.err == nil && (uint64(len(r.data)) != job.length ||
					!checksumEqual(sha256Sum(r.data), job.sum)) {
					r.err = errSnapshotChecksum
				}
			case jobClear:
				r.err = n.store.ClearStaging()
			case jobPersistCompact, jobPersistInstall:
				// 快照字节已在事件循环内确定（严格对应快照点）；worker 只做 fsync。
				// 关键的崩溃/并发安全：正式快照与其日志后缀必须作为一对原子地落到
				// 磁盘（先写快照并 fsync，再写日志后缀）。在这两步之间，不能让
				// 另一个正式快照写任务（更新的压缩）插进来，否则可能留下“新快照
				// 字节 + 旧后缀”或“旧快照 + 已截断后缀”的不自洽组合。由于所有
				// 这类任务都走本单一 worker，它们本就严格串行；这里再用一个本地
				// 互斥把 snapshot+log 两次 store 调用绑成不可分割的一段，防止
				// 将来有人把读路径（LoadSnapshot 供发送）并发引入时读到中间态。
				n.formalWriteMu.Lock()
				if err := n.store.SaveSnapshot(job.meta, job.data); err != nil {
					r.err = err
				} else {
					suffix := []LogEntry{}
					if len(job.log) > 1 {
						suffix = job.log[1:]
					}
					r.err = n.store.SaveLogSuffix(suffix)
				}
				n.formalWriteMu.Unlock()
				if job.kind == jobPersistInstall && r.err == nil {
					r.data = job.data
				}
			case jobPersistLog:
				r.err = n.store.SaveLogSuffix(job.log)
			case jobBarrier:
				// 此前所有 persistJobs 都已按序 fsync 完成；通知等待方。
				close(job.barrierCh)
				continue
			}
			// syncDone（同步等待路径）直接回传错误。
			if job.syncDone != nil {
				select {
				case job.syncDone <- r.err:
				case <-n.stopCh:
					return
				}
				continue
			}
			if !job.ack {
				if r.err != nil {
					n.logf("[%s] background persist %s error: %v", n.cfg.ID, job.kind, r.err)
				}
				continue
			}
			select {
			case n.events <- internalEvent{kind: eventChunkWritten, rpc: r}:
			case <-n.stopCh:
				return
			}
		}
	}
}

// enqueueChunkJob 非阻塞投递接收侧数据块任务（高优先通道）；结果回事件循环。
func (n *Node) enqueueChunkJob(job chunkJob) bool {
	job.ack = true
	select {
	case n.chunkJobs <- job:
		return true
	default:
		return false
	}
}

// enqueuePersist 投递后台正式快照/日志落盘任务（低优先通道、不要回执）。
// 非阻塞：队列满（磁盘一时跟不上）时丢弃该任务——日志持久化是“最新整份后缀”
// 的幂等重写，后续 tick/flush 会再写更新的一份；绝不因此卡住事件循环。
func (n *Node) enqueuePersist(job chunkJob) {
	job.ack = false
	select {
	case n.persistJobs <- job:
	case <-n.stopCh:
	default:
		// 队列满：让下一个 tick 重新标记脏并重试（仅日志类可安全丢弃）。
		if job.kind == jobPersistLog {
			n.logDirty = true
		}
	}
}

// enqueueLogPersist 将日志后缀整体持久化（后台、有序、不阻塞事件循环）。
func (n *Node) enqueueLogPersist(suffix []LogEntry) {
	n.enqueuePersist(chunkJob{kind: jobPersistLog, log: suffix})
}

// flushDurability 确保所有已提交的后台持久化（可能在途的压缩、正式快照、日志
// 后缀）都已 fsync。优雅停止/模拟崩溃前在事件循环中调用。
//
// 关键：必须用“阻塞投递”把当前日志后缀同步写盘。日志在事件循环里是按 tick
// 合并、非阻塞投递的（队列满时会丢一次、留给下个 tick）；停止时没有“下个 tick”，
// 因此这里直接把任务排在屏障前并等待，绝不允许最后一份已提交后缀丢失。
func (n *Node) flushDurability() {
	// 1) 当前内存中的日志后缀（权威最新形态，含被非阻塞投递跳过的尾部）。
	//    排在屏障前，保证它先于屏障完成；与此前任务的顺序由单 worker 保证。
	suffix := []LogEntry{}
	if len(n.log) > 1 {
		suffix = append([]LogEntry(nil), n.log[1:]...)
	}
	n.logDirty = false
	barrier := make(chan struct{})
	select {
	case n.persistJobs <- chunkJob{kind: jobPersistLog, log: suffix, ack: false}:
	case <-n.stopCh:
		return
	}
	// 2) 屏障：此前所有 persistJobs（含可能在途的压缩快照）都 fsync 后才关闭。
	select {
	case n.persistJobs <- chunkJob{kind: jobBarrier, barrierCh: barrier}:
	case <-n.stopCh:
		return
	}
	select {
	case <-barrier:
	case <-n.stopCh:
	}
}

// enqueuePersistAck 投递需要完成回执的压缩任务。压缩单飞保证同时只有一份，
// 队列足够容纳；非阻塞，入队失败时解除单飞并记录（下次压缩会重做）。
func (n *Node) enqueuePersistAck(job chunkJob) bool {
	job.ack = true
	select {
	case n.persistJobs <- job:
		return true
	default:
		n.compactBusy = false
		n.pendingCompact = nil
		return false
	}
}

// incomingSnapshot 是跟随者侧唯一一份进行中的临时快照状态。
type incomingSnapshot struct {
	meta     StagingMeta
	received uint64 // 已 fsync durable 的连续字节数（对外 ACK 的断点）
	frontier uint64 // 已投递给落盘 worker 的连续字节上界（received <= frontier <= Length）
	// pending 缓存发送窗口内、到达早于连续点的小块：key=块起始偏移。
	// 容量有界（snapshotWindow 块），空洞被补齐时按序投给 worker。
	pending    map[uint64]pendingChunk
	busy       bool
	finalizing bool
	installing bool
	state      CatchUpState
	reason     RejectReason
	epoch      uint64
}

// pendingChunk 是因空洞暂存的一个已校验小块。
type pendingChunk struct {
	data []byte
	end  uint64
	done bool
}

// snapshotTransfer 是领导者面向某个 peer 的一个快照发送会话。
//
// 采用有界滑动窗口：最多 snapshotWindow 个块同时在途。对端 ACK 回报“已连续
// fsync durable”的字节断点，发送方据此把窗口基址前移并补充新块；窗口内丢失的
// 块由超时机制重传（同偏移可重复送达，对端按偏移幂等去重）。
type snapshotTransfer struct {
	id       uint64
	term     Term
	meta     SnapshotMeta
	length   uint64
	checksum []byte
	data     []byte

	windowBase uint64         // 窗口中尚未被 durable ACK 覆盖的最小偏移
	windowEnd  uint64         // 已发送到的连续偏移上界（<= length）
	inflight   map[uint64]int // offset -> 已等待 tick 数（仅窗口内的块）
	backoff    int            // 对端要求等待（如被取消）时的剩余 tick
	done       bool           // 对端已完整安装
}

// snapshotWindow 是单个快照会话的在途块数上限。
// 太大在慢盘接收端堆积重复块，太小退化为停等；8 在小分块/慢 RTT 下足够填满
// fsync 流水线，又不会淹没接收端数据面队列。
const snapshotWindow = 8

// ---- 领导者：发送侧 ----

// startSnapshotTransfer 为 peer 创建、复用或续传一个分块快照会话。
// 每个心跳（sendAppendEntries 发现 nextIndex<=snapLast）都会调用：
// 因此创建过程必须幂等，并且在等待退避时不能发块。
//
// 关键语义：进行中的会话会“钉住”发起时的那份快照——即使领导者随后压缩出
// 更新的快照，也必须先把当前这份传完，再开新会话。否则在持续写入+持续压缩
// 下，会话会被不断作废、跟随者反复从 0 重传，负载不停就永远追不平。
func (n *Node) startSnapshotTransfer(peer NodeID) {
	if n.role != RoleLeader {
		return
	}
	if tr := n.snapXfers[peer]; tr != nil {
		switch {
		case tr.done:
			if n.matchIndex[peer] >= n.snapMeta.LastIncludedIndex {
				// 对端已完整安装且已不落后于当前快照点：常规 AppendEntries 接管。
				n.sendAppendEntries(peer)
				return
			}
			// 对端装完的是旧的钉住快照，而领导者又压出了更新的快照：
			// 丢弃已完成的旧会话，落到下方用最新快照开新会话。
			n.debugf("peer %s installed pinned snapshot %d but leader advanced to %d, next transfer",
				peer, tr.meta.LastIncludedIndex, n.snapMeta.LastIncludedIndex)
			delete(n.snapXfers, peer)
		case tr.backoff > 0:
			return // 对端明确要求等待（如被用户取消）。
		default:
			// 会话仍在进行：用滑动窗口继续发送钉住的那份快照，绝不因领导者压出
			// 更新快照而作废（这是持续负载下能追平的关键）。
			n.transmitChunk(peer, tr)
			return
		}
	}

	data, meta, sum, ok := n.loadSnapshotForTransfer()
	if !ok {
		return
	}
	n.nextTransferID++
	tr := &snapshotTransfer{
		id:       n.nextTransferID,
		term:     n.term,
		meta:     meta,
		length:   uint64(len(data)),
		checksum: sum,
		data:     data,
		inflight: map[uint64]int{},
	}
	n.snapXfers[peer] = tr
	n.debugf("start chunked snapshot transfer to %s: id=%d index=%d bytes=%d chunk=%d",
		peer, tr.id, meta.LastIncludedIndex, tr.length, n.cfg.SnapshotChunkSize)
	n.transmitChunk(peer, tr)
}

// chunkSize 返回配置的分块大小（带默认值）。
func (n *Node) chunkSize() uint64 {
	size := uint64(n.cfg.SnapshotChunkSize)
	if size == 0 {
		return 64 * 1024
	}
	return size
}

// sendOneChunk 发送 [off,end) 这一块并登记在途。
func (n *Node) sendOneChunk(peer NodeID, tr *snapshotTransfer, off, end uint64) {
	req := &InstallSnapshotRequest{
		Term:              n.term,
		LeaderID:          n.cfg.ID,
		LastIncludedIndex: tr.meta.LastIncludedIndex,
		LastIncludedTerm:  tr.meta.LastIncludedTerm,
		Chunked:           true,
		TransferID:        tr.id,
		Offset:            off,
		Length:            tr.length,
		Checksum:          tr.checksum,
		Data:              append([]byte(nil), tr.data[off:end]...),
		Done:              end == tr.length,
	}
	if tr.inflight == nil {
		tr.inflight = map[uint64]int{}
	}
	tr.inflight[off] = 0
	n.sendSnapshotChunk(peer, req)
}

// transmitChunk 填满发送窗口：从 windowEnd 起连续发送，直到窗口达到
// snapshotWindow 个在途块或快照末尾。窗口基址（windowBase）由 durable ACK 推进。
func (n *Node) transmitChunk(peer NodeID, tr *snapshotTransfer) {
	if tr.backoff > 0 || tr.done {
		return
	}
	size := n.chunkSize()
	for len(tr.inflight) < snapshotWindow && tr.windowEnd < tr.length {
		end := tr.windowEnd + size
		if end > tr.length {
			end = tr.length
		}
		n.sendOneChunk(peer, tr, tr.windowEnd, end)
		tr.windowEnd = end
	}
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
		if n.matchIndex[peer] < n.snapMeta.LastIncludedIndex {
			n.sendSnapshot(peer)
		}
	}
}

// tickOneTransfer 推进窗口内每个在途块的计时，重传超时块，退避结束后续传。
func (n *Node) tickOneTransfer(peer NodeID, tr *snapshotTransfer) {
	if tr.backoff > 0 {
		tr.backoff--
		if tr.backoff == 0 {
			n.startSnapshotTransfer(peer)
		}
		return
	}
	// 重传超时块：每个在途块累计等待 tick，达到阈值后重发并把计时清零，
	// 避免重发后计数仍停在阈值上导致每个 tick 都重复重发同一块（洪泛）。
	for off, waited := range tr.inflight {
		waited++
		if waited >= n.cfg.SnapshotRetryTicks {
			size := n.chunkSize()
			end := off + size
			if end > tr.length {
				end = tr.length
			}
			n.sendOneChunk(peer, tr, off, end)
			waited = 0
		}
		tr.inflight[off] = waited
	}
	// 空洞修复：窗口已发到 windowEnd，但某些偏移因请求/响应丢失其在途登记已被
	// chunkTimeout 删除、而 durable 基址尚未越过它们（inflight 里没有、却落在
	// [base,windowEnd) 之间），重新登记并重发这些缺失块。
	if tr.windowEnd > tr.windowBase {
		size := n.chunkSize()
		for off := tr.windowBase; off < tr.windowEnd; {
			end := off + size
			if end > tr.length {
				end = tr.length
			}
			if _, ok := tr.inflight[off]; !ok {
				n.sendOneChunk(peer, tr, off, end)
			}
			off = end
		}
	}
	// 窗口数据已全部发完且当前无在途块，却尚未 done：重发末块拿 complete 短路。
	// 两种情形：(a) 仍有字节没被 durable ACK（windowBase<length）；
	// (b) 字节都 durable 了但对端还在做末块校验/提升（complete 尚未回来）。
	// sendOneChunk 的 inflight 登记使该重发自限频，不会每 tick 洪泛。
	if len(tr.inflight) == 0 && tr.windowEnd >= tr.length {
		size := n.chunkSize()
		off := uint64(0)
		if tr.length > size {
			off = tr.length - size
		}
		// 即使 windowBase 已到 length（对端字节全 durable、只欠 complete），仍重发末块。
		if off > tr.windowBase {
			off = tr.windowBase
		}
		n.sendOneChunk(peer, tr, off, tr.length)
	}
	// 若窗口还没填满（例如对端只 ACK 了部分），补充新块。
	n.transmitChunk(peer, tr)
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
	// 会话相关性：响应必须属于当前钉住会话对应的那份快照。
	// 领导权变化/旧 goroutine 延迟投递的过期响应一律忽略，绝不能用旧进度推进新会话。
	if r.lastIncluded != tr.meta.LastIncludedIndex {
		n.debugf("ignore snapshot response for %d, current pinned snapshot is %d",
			r.lastIncluded, tr.meta.LastIncludedIndex)
		return
	}

	if r.success {
		// 对端可能刚刚 ResumeCatchUp：正常 ACK 立即解除退避。
		tr.backoff = 0
		if r.complete {
			tr.done = true
			tr.inflight = nil
			tr.windowBase = tr.length
			tr.windowEnd = tr.length
			tr.data = nil
			// 对端回报的 lastIncluded 是它真正的进度点（可能高于本会话快照点：
			// 它本地已有更新快照/日志）。本节点只要保留该索引的日志，就从该点
			// 之后续传；下一条 AppendEntries 的 prevLog 检查会在任期不一致时
			// 自动冲突回退。若本节点已没有该索引（它领先得更多），保持 nextIndex
			// 不动并让正常复制的拒绝路径处理。
			if _, ok := n.entryAt(r.lastIncluded); ok {
				if r.lastIncluded > n.matchIndex[peer] {
					// 仅在能确定任期一致时才直接抬高 matchIndex；否则保守地交给
					// 下一条 AE 确认（AE 成功才抬 matchIndex），避免虚报匹配。
					n.matchIndex[peer] = r.lastIncluded
				}
				n.nextIndex[peer] = r.lastIncluded + 1
			}
			n.debugf("peer %s snapshot complete at %d, resume log replication", peer, r.lastIncluded)
			n.advanceCommitIndex()
			n.sendAppendEntries(peer)
			return
		}
		// 滑动窗口推进：对端回报连续 durable 断点 ack。丢弃所有起始偏移 < ack
		// 的在途登记，窗口基址前移到 ack，再填满窗口发新块。
		if r.ackOffset > tr.windowBase && r.ackOffset <= tr.length {
			for off := range tr.inflight {
				if off < r.ackOffset {
					delete(tr.inflight, off)
				}
			}
			tr.windowBase = r.ackOffset
			// resync 回退场景下 windowEnd 可能小于新基址（已发区间被作废），纠正之。
			if tr.windowEnd < tr.windowBase {
				tr.windowEnd = tr.windowBase
			}
		}
		// 全部字节都已 durable：清掉所有残留的在途登记（重复发送/超时重传可能
		// 留下与 durable 区间重叠的偏移），然后重发末块轮询 complete。
		if r.ackOffset >= tr.length {
			tr.inflight = nil
			size := n.chunkSize()
			off := uint64(0)
			if tr.length > size {
				off = tr.length - size
			}
			n.sendOneChunk(peer, tr, off, tr.length)
			return
		}
		n.transmitChunk(peer, tr)
		return
	}

	// 失败响应：根据原因决定重试策略。
	switch r.reason {
	case RejectResync:
		if r.lastIncluded != tr.meta.LastIncludedIndex {
			n.dropTransfer(peer)
			return
		}
		// 对端回报了它仍持有的同身份断点：把窗口基址回退到该断点（绝不重头再来），
		// 清空在途表，从断点重新填满窗口。
		base := r.ackOffset
		if base > tr.length {
			base = tr.length
		}
		n.debugf("peer %s resync at breakpoint %d/%d for snapshot %d, resume window there",
			peer, base, tr.length, r.lastIncluded)
		tr.windowBase = base
		tr.windowEnd = base
		tr.inflight = map[uint64]int{}
		n.transmitChunk(peer, tr)
	case RejectTransferFailed:
		// 对端丢弃临时快照（校验/落盘失败）：从 0 重发同一份钉住快照。
		n.debugf("peer %s snapshot transfer failed (%s), retry from 0", peer, r.reason)
		tr.windowBase = 0
		tr.windowEnd = 0
		tr.inflight = map[uint64]int{}
		n.transmitChunk(peer, tr)
	case RejectCancelled:
		// 用户取消：有限退避。窗口与断点保持不变，退避后续传。
		tr.backoff = n.cfg.SnapshotRetryTicks * 8
		n.debugf("peer %s catch-up cancelled, backing off %d ticks (window stays)", peer, tr.backoff)
	case RejectStaleTerm:
		// 高任期已在开头让位；同任期不会 stale，忽略。
	default:
		// 其它拒绝：等下一个 tick 的窗口重传。
	}
}

// checksumEqual 在事件循环内比较校验和。
func checksumEqual(a, b []byte) bool { return bytes.Equal(a, b) }

// ---- 接收侧与用户视图在文件后半部分实现（见下方接收者方法） ----

// ---- 跟随者：接收侧（校验在事件循环；落盘/校验 I/O 在串行 worker） ----

// restoreIncoming 在启动恢复阶段重建未完成的接收会话状态。
// 仅恢复进度用于续传；无论磁盘上临时文件处于什么状态，都绝不把它安装为正式快照。
// 在 NewNode（事件循环启动前）调用，直接同步读盘。
func (n *Node) restoreIncoming() error {
	m, ok, err := n.store.LoadStagingMeta()
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// 崩溃窗口：快照可能已经原子提升为正式快照、但 staging 尚未清理。
	if m.LastIncludedIndex <= n.snapMeta.LastIncludedIndex {
		_ = n.store.ClearStaging()
		return nil
	}
	size, err := n.store.StagingSize()
	if err != nil {
		return err
	}
	if size > m.Length {
		_ = n.store.ClearStaging()
		return nil
	}
	n.chunkEpoch++
	n.incoming = &incomingSnapshot{
		meta:     m,
		received: size,
		frontier: size,
		pending:  map[uint64]pendingChunk{},
		epoch:    n.chunkEpoch,
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

// ackDurable 用「已 fsync durable」的断点填充响应。
func (in *incomingSnapshot) ackDurable(resp *InstallSnapshotResponse) {
	resp.Success = true
	resp.Chunked = true
	resp.TransferID = in.meta.TransferID
	resp.LastIncluded = in.meta.LastIncludedIndex
	resp.AckOffset = in.received
}

// failIncoming 标记会话失败并把 clear 投给 worker。旧身份迟到的结果因年代不符被忽略。
func (n *Node) failIncoming(reason RejectReason) {
	in := n.incoming
	if in == nil {
		return
	}
	in.state = CatchUpFailed
	in.reason = reason
	in.finalizing = false
	in.frontier = in.received
	in.busy = true
	n.chunkEpoch++ // 让此前已在队列中的 write/finalize 结果全部失效
	in.epoch = n.chunkEpoch
	n.enqueueChunkJob(chunkJob{kind: jobClear, epoch: in.epoch})
}

// beginNewIncoming 为全新身份的首块初始化会话，并把 reset->saveMeta->write(->finalize)
// 严格按序投给串行 worker。返回 false 表示 worker 暂不可用，调用方让对端重发首块。
func (n *Node) beginNewIncoming(req *InstallSnapshotRequest) bool {
	n.chunkEpoch++
	epoch := n.chunkEpoch
	meta := StagingMeta{
		TransferID:        req.TransferID,
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Length:            req.Length,
		Checksum:          append([]byte(nil), req.Checksum...),
		Leader:            req.LeaderID,
	}
	in := &incomingSnapshot{
		meta:    meta,
		pending: map[uint64]pendingChunk{},
		state:   CatchUpSyncing,
		epoch:   epoch,
		busy:    true,
	}
	jobs := []chunkJob{
		{kind: jobReset, epoch: epoch},
		{kind: jobSaveMeta, epoch: epoch, stagingMeta: &meta},
	}
	end := req.Offset + uint64(len(req.Data))
	jobs = append(jobs, chunkJob{kind: jobWrite, epoch: epoch,
		offset: req.Offset, data: append([]byte(nil), req.Data...)})
	if req.Done {
		jobs = append(jobs, chunkJob{kind: jobFinalize, epoch: epoch, length: req.Length,
			sum:  append([]byte(nil), req.Checksum...),
			meta: SnapshotMeta{LastIncludedIndex: req.LastIncludedIndex, LastIncludedTerm: req.LastIncludedTerm}})
	}
	for _, j := range jobs {
		if !n.enqueueChunkJob(j) {
			// 队列不足：一个都不承诺。已入队的 reset/saveMeta 属新身份，
			// 随后对端重发首块会再 bump 年代使其作废，安全。
			return false
		}
	}
	in.frontier = end
	if req.Done {
		in.finalizing = true
	}
	n.incoming = in
	return true
}

// handleChunkedInstallSnapshot 在事件循环内校验一个分块请求并把磁盘工作投给 worker。
func (n *Node) handleChunkedInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	resp := &InstallSnapshotResponse{Term: n.term, Peer: n.cfg.ID, Chunked: true,
		LastIncluded: req.LastIncludedIndex}

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

	// 拷贝块字节，避免通过共享底层数组改到发送方数据。
	if len(req.Data) > 0 {
		req.Data = append([]byte(nil), req.Data...)
	}

	// 快照不比本地新：幂等应答“已完成”，并把本节点“真正的进度点”（日志最后一条
	// 索引）通过 LastIncluded 回报给领导者——本节点持有的快照可能比领导者发送的
	// 更新（领导权切换后常见），领导者必须据回报点做日志对账，而不是把本节点的
	// 匹配位置错误地拉低到它自己较旧的快照点。
	if req.LastIncludedIndex <= n.snapMeta.LastIncludedIndex {
		resp.Success = true
		resp.Complete = true
		resp.AckOffset = req.Length
		resp.LastIncluded = n.lastLogIndex()
		resp.TransferID = req.TransferID
		return resp
	}

	in := n.incoming

	// 用户显式取消期间：一切块都拒绝，半成品保持不可见。
	if in != nil && in.state == CatchUpCancelled {
		resp.Reason = RejectCancelled
		return resp
	}

	identityMatch := in != nil && in.identityMatches(req)

	// 失败态：仅同一身份首块可开启新一轮；其余要求 resync。
	if in != nil && in.state == CatchUpFailed {
		if !identityMatch || req.Offset != 0 {
			resp.Reason = RejectResync
			if identityMatch {
				in.ackDurable(resp)
				resp.Success = false
			}
			return resp
		}
		in = nil
	}

	newIdentity := in == nil || !identityMatch
	if newIdentity {
		if req.Offset != 0 {
			n.debugf("snapshot identity mismatch at offset=%d (want restart at 0)", req.Offset)
			resp.Reason = RejectResync
			return resp
		}
		if !n.beginNewIncoming(req) {
			n.debugf("chunk worker busy at first chunk, ask resync")
			resp.Reason = RejectResync
			return resp
		}
		n.incoming.ackDurable(resp)
		return resp
	}

	// ---- 身份一致：重复 / 乱序 / 顺序块（带有界乱序缓存） ----
	end := req.Offset + uint64(len(req.Data))
	if end > req.Length {
		n.debugf("snapshot chunk overruns length: offset=%d size=%d length=%d",
			req.Offset, len(req.Data), req.Length)
		n.failIncoming(RejectTransferFailed)
		resp.Reason = RejectTransferFailed
		return resp
	}

	if in.finalizing || in.installing {
		in.ackDurable(resp)
		return resp
	}

	// 完全落在已 durable 区间：幂等 ACK。
	if end <= in.received {
		in.ackDurable(resp)
		return resp
	}

	// 落在已连续覆盖区间（durable..frontier）或已在乱序缓存中：幂等 ACK。
	if req.Offset >= in.received && end <= in.frontier {
		in.ackDurable(resp)
		return resp
	}
	if _, ok := in.pending[req.Offset]; ok && req.Offset >= in.frontier {
		in.ackDurable(resp)
		return resp
	}

	// 与已连续覆盖区间部分重叠：截断出尚未覆盖的新尾巴。
	if req.Offset < in.frontier {
		if end <= in.frontier {
			in.ackDurable(resp)
			return resp
		}
		req.Data = req.Data[in.frontier-req.Offset:]
		req.Offset = in.frontier
		end = req.Offset + uint64(len(req.Data))
	}

	// 空洞块（offset 超前连续点）：只在滑动窗口范围内缓存；超过窗口容量或
	// 距离连续点过远（>window 个块）都要求从断点重发，绝不无界缓存。
	if req.Offset > in.frontier {
		maxAhead := uint64(snapshotWindow) * n.chunkSize()
		tooFar := req.Offset-in.frontier > maxAhead
		if tooFar || len(in.pending) >= snapshotWindow {
			n.debugf("snapshot gap out of window at offset=%d frontier=%d -> resync",
				req.Offset, in.frontier)
			resp.Reason = RejectResync
			in.ackDurable(resp)
			resp.Success = false
			return resp
		}
		in.pending[req.Offset] = pendingChunk{
			data: append([]byte(nil), req.Data...), end: end, done: req.Done}
		in.ackDurable(resp)
		return resp
	}

	// req.Offset == frontier：顺序新块。投递给 worker，然后尝试排空乱序缓存。
	if !n.enqueueChunkJob(chunkJob{kind: jobWrite, epoch: in.epoch,
		offset: req.Offset, data: append([]byte(nil), req.Data...)}) {
		n.debugf("chunk worker queue full at offset=%d, ack durable=%d", req.Offset, in.received)
		in.ackDurable(resp)
		return resp
	}
	in.frontier = end
	in.busy = true
	lastDone := req.Done
	n.flushPendingChunks(in, req.Length, &lastDone)

	if lastDone {
		if in.frontier != req.Length {
			n.debugf("snapshot done before complete: frontier=%d length=%d -> resync",
				in.frontier, req.Length)
			resp.Reason = RejectResync
			in.ackDurable(resp)
			resp.Success = false
			return resp
		}
		in.finalizing = true
		n.enqueueChunkJob(chunkJob{kind: jobFinalize, epoch: in.epoch, length: req.Length,
			sum:  append([]byte(nil), req.Checksum...),
			meta: SnapshotMeta{LastIncludedIndex: req.LastIncludedIndex, LastIncludedTerm: req.LastIncludedTerm}})
	}

	in.ackDurable(resp)
	return resp
}

// flushPendingChunks 从连续点 frontier 起，把乱序缓存中首尾相接的小块按序投给
// 落盘 worker，直到遇到空洞或缓存耗尽。*sawDone 记录途中是否出现末块。
func (n *Node) flushPendingChunks(in *incomingSnapshot, total uint64, sawDone *bool) {
	for {
		pc, ok := in.pending[in.frontier]
		if !ok {
			return
		}
		if !n.enqueueChunkJob(chunkJob{kind: jobWrite, epoch: in.epoch,
			offset: in.frontier, data: pc.data}) {
			return // worker 队列暂满：frontier 停在此处，对端重发会再次驱动。
		}
		delete(in.pending, in.frontier)
		in.frontier = pc.end
		if pc.done {
			*sawDone = true
		}
		if in.frontier > total {
			return
		}
	}
}

// handleChunkJobResult 在事件循环内提交串行 worker 的结果。
func (n *Node) handleChunkJobResult(r chunkJobResult) {
	in := n.incoming
	// 压缩完成事件与具体接收会话无关（incoming 多为 nil）；只推进压缩单飞。
	if r.kind == jobPersistCompact {
		if r.err != nil {
			n.logf("[%s] background compaction persist error: %v", n.cfg.ID, r.err)
		}
		n.onCompactPersisted()
		return
	}
	// 年代不符：属于已作废身份（新快照/失败/重启）的迟到结果，一律忽略。
	if in == nil || r.epoch != in.epoch {
		return
	}
	switch r.kind {
	case jobReset, jobSaveMeta:
		if r.err != nil {
			n.logf("[%s] staging setup error: %v", n.cfg.ID, r.err)
			n.failIncoming(RejectTransferFailed)
		}
	case jobWrite:
		if r.err != nil {
			n.logf("[%s] append staging chunk error: %v", n.cfg.ID, r.err)
			n.failIncoming(RejectTransferFailed)
			return
		}
		if nr := r.offset + r.size; nr > in.received {
			in.received = nr
		}
		in.state = CatchUpSyncing
		// 末块落盘完成（连续点到全长）且尚未 finalize：在此自动触发整体校验。
		// 这样即使末块 Done 请求是被乱序缓存、或其直接 RPC 应答丢失，也不会漏掉
		// 完成信号——发送方重发末块时同样会短路，两条路径收敛到同一校验。
		if in.received >= in.meta.Length && in.frontier >= in.meta.Length && !in.finalizing && !in.installing {
			in.finalizing = true
			n.enqueueChunkJob(chunkJob{kind: jobFinalize, epoch: in.epoch, length: in.meta.Length,
				sum: append([]byte(nil), in.meta.Checksum...),
				meta: SnapshotMeta{LastIncludedIndex: in.meta.LastIncludedIndex,
					LastIncludedTerm: in.meta.LastIncludedTerm}})
		}
		if in.received >= in.frontier && !in.finalizing {
			in.busy = false
		}
	case jobFinalize:
		in.finalizing = false
		// 校验结果回来前用户已取消：绝不把半份快照提升为正式快照，直接丢弃。
		if in.state == CatchUpCancelled {
			in.busy = false
			n.enqueueChunkJob(chunkJob{kind: jobClear, epoch: in.epoch})
			return
		}
		if r.err != nil || uint64(len(r.data)) != r.length {
			n.logf("[%s] staging finalize failed: %v", n.cfg.ID, r.err)
			in.busy = false
			n.failIncoming(RejectTransferFailed)
			return
		}
		// 校验通过：把「原子落盘正式快照 + 重写日志」投给 worker（fsync 不阻塞）。
		// 计算保留后缀必须在切换 snapMeta 之前完成。
		kept := []LogEntry{{Index: r.meta.LastIncludedIndex, Term: r.meta.LastIncludedTerm}}
		if n.lastLogIndex() >= r.meta.LastIncludedIndex+1 {
			if e, ok := n.entryAt(r.meta.LastIncludedIndex + 1); ok {
				pi := n.physicalIndex(e.Index)
				if pi >= 1 && pi < len(n.log) {
					kept = append(kept, n.log[pi:]...)
				}
			}
		}
		// 正式快照的安装落盘必须走 persistJobs（与本节点自身压缩/日志后缀同一个
		// 串行 worker），绝不能走 chunkJobs——否则会与本节点的后台压缩并发写同一份
		// snapshot/log 文件，把两种快照内容交错写坏。只有 staging 数据块
		// (reset/write/finalize/clear) 才走高优先的 chunkJobs。
		in.installing = true
		in.busy = true
		installJob := chunkJob{kind: jobPersistInstall, epoch: in.epoch,
			meta: r.meta, data: r.data, log: kept, ack: true}
		select {
		case n.persistJobs <- installJob:
		case <-n.stopCh:
			in.installing = false
			in.busy = false
			n.failIncoming(RejectTransferFailed)
		default:
			in.installing = false
			in.busy = false
			// 队列暂满：保持 finalizing 语义，让对端重发末块后再次进入校验。
			n.failIncoming(RejectTransferFailed)
		}
	case jobPersistInstall:
		in.busy = false
		in.installing = false
		// 落盘期间用户取消：即使磁盘已写完也不暴露为可用快照，丢弃并保持 cancelled。
		if in.state == CatchUpCancelled {
			n.enqueueChunkJob(chunkJob{kind: jobClear, epoch: in.epoch})
			return
		}
		if r.err != nil {
			n.logf("[%s] persist snapshot install error: %v", n.cfg.ID, r.err)
			n.failIncoming(RejectTransferFailed)
			return
		}
		// 磁盘已 durable：在事件循环内完成内存状态切换与状态机恢复。
		if err := n.applyInstalledSnapshot(r.meta, r.data); err != nil {
			n.logf("[%s] apply installed snapshot error: %v", n.cfg.ID, err)
			n.failIncoming(RejectTransferFailed)
			return
		}
		in.state = CatchUpCaughtUp
		in.reason = RejectNone
		in.received = r.length
		in.frontier = r.length
		n.enqueueChunkJob(chunkJob{kind: jobClear, epoch: in.epoch})
		n.debugf("installed chunked snapshot through index=%d (%d bytes), resume log replication",
			r.meta.LastIncludedIndex, r.length)
	case jobClear:
		in.busy = false
	}
}

// catchUpQuiet 是测试/内部使用的细粒度接收状态（含后台落盘是否仍在途）。
type catchUpQuiet struct {
	state      CatchUpState
	received   uint64
	total      Index
	busy       bool
	finalizing bool
	installing bool
}

func (q catchUpQuiet) busySettling() bool {
	return q.busy || q.finalizing || q.installing
}

// catchUpProgressQuiet 在事件循环内读取细粒度接收状态（测试用）。
func (n *Node) catchUpProgressQuiet() catchUpQuiet {
	if n.incoming == nil {
		return catchUpQuiet{state: CatchUpIdle}
	}
	in := n.incoming
	return catchUpQuiet{
		state:      in.state,
		received:   in.received,
		total:      Index(in.meta.Length),
		busy:       in.busy,
		finalizing: in.finalizing,
		installing: in.installing,
	}
}

// ---- 仅事件循环调用的视图 ----

func (n *Node) catchUpProgressLocked() CatchUpProgress {
	if n.incoming == nil {
		return CatchUpProgress{State: CatchUpIdle, SnapshotIdx: n.snapMeta.LastIncludedIndex}
	}
	in := n.incoming
	received := in.received
	if in.state == CatchUpCaughtUp {
		// 安装完成即代表整份字节已收齐并提升；即使 clear 已清掉临时数据，
		// 对外进度也必须显示满字节（Ready()=接收并安装就绪）。
		received = in.meta.Length
	}
	return CatchUpProgress{
		State:        in.state,
		SnapshotIdx:  in.meta.LastIncludedIndex,
		Received:     received,
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
			Received:    tr.windowBase,
			Total:       tr.length,
			TransferID:  tr.id,
			Leader:      n.cfg.ID,
		}
	}
	return out
}

// cancelCatchUpLocked 取消本机进行中的接收。临时数据与断点保留（恢复后续传），
// 但取消期间领导者的块一律被拒绝。
func (n *Node) cancelCatchUpLocked() error {
	if n.incoming == nil {
		// 没有进行中的传输：记录取消标记，使紧接着到来的首块也被拒绝。
		n.incoming = &incomingSnapshot{state: CatchUpCancelled, reason: RejectCancelled}
		return nil
	}
	n.incoming.state = CatchUpCancelled
	n.incoming.reason = RejectCancelled
	n.debugf("snapshot catch-up cancelled by user at %d/%d bytes",
		n.incoming.received, n.incoming.meta.Length)
	return nil
}

// resumeCatchUpLocked 解除取消：保留已落盘的断点，把状态置回 syncing，
// 领导者重发后直接从已确认位置续传，不重头再来。
func (n *Node) resumeCatchUpLocked() error {
	in := n.incoming
	if in == nil || in.state != CatchUpCancelled {
		return nil
	}
	if in.meta.Length == 0 {
		// 取消发生在任何首块之前：仅清掉占位标记。
		n.incoming = nil
		n.debugf("snapshot catch-up resumed by user (no staging)")
		return nil
	}
	in.state = CatchUpSyncing
	in.reason = RejectNone
	in.busy = false
	in.finalizing = false
	n.debugf("snapshot catch-up resumed by user at %d/%d bytes", in.received, in.meta.Length)
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

// ResumeCatchUp 解除取消状态；保留断点，领导者重发后从已确认位置继续。
func (n *Node) ResumeCatchUp() error { return n.controlCatchUp(catchupResume) }
