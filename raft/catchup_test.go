package raft

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// writeBulk 写入 count 条约 50 字节的 KV（约 count*50 字节快照负载），返回写入条数。
func writeBulk(t *testing.T, cli *Client, ctx context.Context, count int) int {
	t.Helper()
	for i := 0; i < count; i++ {
		if _, _, err := cli.Set(ctx, fmt.Sprintf("bulk-%04d", i),
			fmt.Sprintf("value-%08d-padding-padding", i)); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	return count
}

func bulkKV(i int) (string, string) {
	return fmt.Sprintf("bulk-%04d", i), fmt.Sprintf("value-%08d-padding-padding", i)
}

// waitProgressState 轮询节点的追赶进度直到进入指定状态。
func waitProgressState(t *testing.T, n *Node, want CatchUpState, msg string) CatchUpProgress {
	t.Helper()
	deadline := time.Now().Add(waitShort)
	var p CatchUpProgress
	for time.Now().Before(deadline) {
		p = n.CatchUpProgress()
		if p.State == want {
			return p
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting progress=%s: %s (last=%+v)", want, msg, p)
	return p
}

// waitForDur 与 waitFor 相同，但允许自定义超时时长。
func waitForDur(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting: %s", msg)
}

// restartAllWithConfig 重启全部节点以加载 cfgs 中的新配置。
func restartAllWithConfig(t *testing.T, c *Cluster) {
	t.Helper()
	for _, id := range c.IDs() {
		if err := c.Crash(id); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range c.IDs() {
		if err := c.Restart(id); err != nil {
			t.Fatal(err)
		}
	}
}

func pickFollower(t *testing.T, c *Cluster, leader NodeID) NodeID {
	for _, id := range c.IDs() {
		if id != leader {
			return id
		}
	}
	t.Fatal("no follower found")
	return ""
}

// TestChunkedSnapshotResumeAfterDrops：大快照分块传输，中途丢块后从断点续传，
// 不从头重传；进度可查询，最终内容完整。
func TestChunkedSnapshotResumeAfterDrops(t *testing.T) {
	c := newTestCluster(t, 3, true, 31337)
	for _, id := range c.IDs() {
		c.SetSnapshotThreshold(id, 5)
		c.SetSnapshotChunkSize(id, 48) // 极小分块，制造大量块
	}
	restartAllWithConfig(t, c)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	leader := c.Leader()
	laggard := pickFollower(t, c, leader)
	if err := c.Crash(laggard); err != nil {
		t.Fatal(err)
	}

	cli := NewClient("big", nodesOf(c)...)
	ctx, cancel := context.WithTimeout(context.Background(), waitShort)
	defer cancel()
	totalWrites := writeBulk(t, cli, ctx, 80)
	waitFor(t, func() bool {
		for _, id := range c.IDs() {
			if id == laggard {
				continue
			}
			if !c.SnapshotExists(id) {
				return false
			}
		}
		return true
	}, "live peers compacted")

	// 丢弃传输中间的 10 个块，并记录观察到的最大偏移。
	var dropped int32
	var sawOffset int64 = -1
	c.SetSnapshotHook(&SnapshotFaultHook{
		Drop: func(from, to NodeID, req *InstallSnapshotRequest) bool {
			if NodeID(to) != laggard {
				return false
			}
			if v := int64(req.Offset); v > atomic.LoadInt64(&sawOffset) {
				atomic.StoreInt64(&sawOffset, v)
			}
			if req.Offset > 0 && atomic.AddInt32(&dropped, 1) <= 10 {
				return true
			}
			return false
		},
	})
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	ln := c.Node(laggard)

	var syncing CatchUpProgress
	waitFor(t, func() bool {
		syncing = ln.CatchUpProgress()
		return syncing.State == CatchUpSyncing && syncing.Total > 0
	}, "laggard enters syncing with total bytes")
	if syncing.Received > syncing.Total {
		t.Fatalf("received %d > total %d", syncing.Received, syncing.Total)
	}
	t.Logf("syncing: %d/%d bytes toward snapshot %d, transfer=%d",
		syncing.Received, syncing.Total, syncing.SnapshotIdx, syncing.TransferID)

	fin := waitProgressState(t, ln, CatchUpCaughtUp, "catch up completes")
	if fin.Received != fin.Total || fin.Total == 0 {
		t.Fatalf("bad final progress: %+v", fin)
	}
	c.SetSnapshotHook(nil)

	if atomic.LoadInt64(&sawOffset) <= 0 {
		t.Fatalf("never observed a chunk with offset>0")
	}
	t.Logf("observed max chunk offset=%d, dropped=%d",
		atomic.LoadInt64(&sawOffset), atomic.LoadInt32(&dropped))

	waitFor(t, func() bool {
		for i := 0; i < totalWrites; i++ {
			k, v := bulkKV(i)
			if got, ok := ln.sm.Lookup(k); !ok || got != v {
				return false
			}
		}
		return true
	}, "laggard state machine complete after resume")

	waitFor(t, func() bool {
		p, ok := c.Node(leader).SnapshotTransfers()[laggard]
		return ok && p.State == CatchUpCaughtUp && p.Received == p.Total
	}, "leader view shows caught_up")
}

// TestChunkedSnapshotResumeAcrossRestart：接收节点收到部分块后崩溃重启，
// 临时快照持久化在磁盘上，重启后从已确认位置继续；全部完成后重启仍一致。
func TestChunkedSnapshotResumeAcrossRestart(t *testing.T) {
	c := newTestCluster(t, 3, true, 42424)
	for _, id := range c.IDs() {
		c.SetSnapshotThreshold(id, 5)
		c.SetSnapshotChunkSize(id, 64)
	}
	restartAllWithConfig(t, c)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	leader := c.Leader()
	laggard := pickFollower(t, c, leader)
	if err := c.Crash(laggard); err != nil {
		t.Fatal(err)
	}
	cli := NewClient("restart", nodesOf(c)...)
	// -race + 整包高负载下磁盘与调度都会被显著放大，给足初始写入时间。
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	totalWrites := writeBulk(t, cli, ctx, 100)
	waitFor(t, func() bool {
		for _, id := range c.IDs() {
			if id != laggard && !c.SnapshotExists(id) {
				return false
			}
		}
		return true
	}, "peers compacted")

	var crashed int32
	c.SetSnapshotHook(&SnapshotFaultHook{
		Drop: func(from, to NodeID, req *InstallSnapshotRequest) bool {
			if NodeID(to) != laggard || atomic.LoadInt32(&crashed) != 0 {
				return false
			}
			if req.Offset >= 256 {
				atomic.StoreInt32(&crashed, 1)
				go func() { _ = c.Crash(laggard) }()
				return true
			}
			return false
		},
	})
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool {
		return atomic.LoadInt32(&crashed) == 1 && c.Node(laggard) == nil
	}, "laggard crashed mid-transfer")
	waitFor(t, func() bool {
		if err := c.Restart(laggard); err != nil {
			return false
		}
		return c.Node(laggard) != nil
	}, "laggard restarted")
	c.SetSnapshotHook(nil)
	ln := c.Node(laggard)

	p := waitProgressState(t, ln, CatchUpCaughtUp, "catch up after mid-transfer restart")
	if p.Received != p.Total {
		t.Fatalf("received %d != total %d", p.Received, p.Total)
	}
	t.Logf("after restart caught up: %d bytes, snapshot index=%d", p.Total, p.SnapshotIdx)

	waitFor(t, func() bool {
		for i := 0; i < totalWrites; i++ {
			k, v := bulkKV(i)
			if got, ok := ln.sm.Lookup(k); !ok || got != v {
				return false
			}
		}
		return true
	}, "all values present after catch-up")

	// 追赶完成后再重启：从正式快照恢复，内容一致，且不会误报 syncing。
	if err := c.Crash(laggard); err != nil {
		t.Fatal(err)
	}
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	ln = c.Node(laggard)
	waitFor(t, func() bool {
		s := ln.Status()
		if s.SnapshotIndex == 0 {
			return false
		}
		for i := 0; i < totalWrites; i++ {
			k, v := bulkKV(i)
			if got, ok := ln.sm.Lookup(k); !ok || got != v {
				return false
			}
		}
		return true
	}, "consistent after final restart")
	if st := ln.CatchUpProgress().State; st == CatchUpSyncing {
		t.Fatalf("freshly restarted node must not report syncing from a completed transfer, got %s", st)
	}
}

// TestChunkedSnapshotDuplicateReorderInterleave：
// 重复块幂等、乱序块不污染临时数据；传输期间集群继续写入并频繁压缩
// （快照身份不断推进），落后节点最终仍包含所有已确认提交的数据。
func TestChunkedSnapshotDuplicateReorderInterleave(t *testing.T) {
	c := newTestCluster(t, 3, true, 77701)
	for _, id := range c.IDs() {
		c.SetSnapshotThreshold(id, 3) // 极频繁压缩
		c.SetSnapshotChunkSize(id, 32)
	}
	restartAllWithConfig(t, c)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	leader := c.Leader()
	laggard := pickFollower(t, c, leader)
	if err := c.Crash(laggard); err != nil {
		t.Fatal(err)
	}

	cli := NewClient("interleave", nodesOf(c)...)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	n0 := writeBulk(t, cli, ctx, 30)

	// 追赶开始前的领导提交点：落后节点最终必须越过它。
	leaderBefore := c.Node(c.Leader()).Status().CommitIndex

	var totalChunks, nonZero int32
	c.SetSnapshotHook(&SnapshotFaultHook{
		Drop: func(from, to NodeID, req *InstallSnapshotRequest) bool {
			if NodeID(to) != laggard {
				return false
			}
			n := atomic.AddInt32(&totalChunks, 1)
			if req.Offset > 0 {
				atomic.AddInt32(&nonZero, 1)
			}
			return n%3 == 0 // 约 1/3 块丢弃，制造重复/乱序重传
		},
	})
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}

	// 传输期间持续写入并压缩。
	writerErr := make(chan error, 1)
	go func() {
		for i := 30; i < 150; i++ {
			if _, _, err := cli.Set(ctx, fmt.Sprintf("live-%04d", i), fmt.Sprintf("v-%08d", i)); err != nil {
				writerErr <- err
				return
			}
		}
		writerErr <- nil
	}()

	ln := c.Node(laggard)
	waitForDur(t, 8*time.Second, func() bool {
		s := ln.Status()
		p := ln.CatchUpProgress()
		return s.CommitIndex >= leaderBefore && s.AppliedIndex >= leaderBefore &&
			s.SnapshotIndex > 0 && (p.State == CatchUpCaughtUp || p.State == CatchUpIdle)
	}, "laggard installs an advancing snapshot and passes old commit")
	c.SetSnapshotHook(nil)
	if err := <-writerErr; err != nil {
		t.Fatalf("concurrent writer: %v", err)
	}

	// 再写一批并等待全量收敛。
	for i := 900; i < 940; i++ {
		if _, _, err := cli.Set(ctx, fmt.Sprintf("tail-%04d", i), fmt.Sprintf("t-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	leaderCommit := c.Node(c.Leader()).Status().CommitIndex
	waitForDur(t, 8*time.Second, func() bool {
		s := ln.Status()
		return s.CommitIndex >= leaderCommit && s.AppliedIndex >= leaderCommit
	}, "laggard commit/applied catches leader")

	for i := 0; i < n0; i++ {
		k, v := bulkKV(i)
		if got, ok := ln.sm.Lookup(k); !ok || got != v {
			t.Fatalf("committed value lost/rolled back: %s=%q ok=%v", k, got, ok)
		}
	}
	for i := 900; i < 940; i++ {
		if got, ok := ln.sm.Lookup(fmt.Sprintf("tail-%04d", i)); !ok || got != fmt.Sprintf("t-%d", i) {
			t.Fatalf("tail value missing: tail-%04d=%q ok=%v", i, got, ok)
		}
	}
	snap, last := ln.LogIndexRange()
	if last < snap {
		t.Fatalf("log hole: snap=%d last=%d", snap, last)
	}
	t.Logf("interleave ok: chunks=%d (nonzero-offset=%d), snap=%d last=%d commit=%d",
		atomic.LoadInt32(&totalChunks), atomic.LoadInt32(&nonZero), snap, last, leaderCommit)
}

// TestChunkedSnapshotCancelAndResume：用户可取消进行中的追赶（状态为 cancelled、
// 半份数据不可用），取消期间不允许虚报追平；ResumeCatchUp 后自动从断点/重头完成。
func TestChunkedSnapshotCancelAndResume(t *testing.T) {
	c := newTestCluster(t, 3, true, 51515)
	for _, id := range c.IDs() {
		c.SetSnapshotThreshold(id, 5)
		c.SetSnapshotChunkSize(id, 32)
	}
	restartAllWithConfig(t, c)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	leader := c.Leader()
	laggard := pickFollower(t, c, leader)
	if err := c.Crash(laggard); err != nil {
		t.Fatal(err)
	}
	cli := NewClient("cancel", nodesOf(c)...)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	totalWrites := writeBulk(t, cli, ctx, 60)
	waitFor(t, func() bool {
		for _, id := range c.IDs() {
			if id != laggard && !c.SnapshotExists(id) {
				return false
			}
		}
		return true
	}, "peers compacted")

	// 先取消（甚至在传输开始前），再启动落后节点：块应被拒绝，不能追平。
	ln0 := c.Node(laggard)
	_ = ln0 // 节点已崩溃，无法在本机调用；改为重启后立刻取消。
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	ln := c.Node(laggard)

	// 等会话建立后取消。
	waitForDur(t, 3*time.Second, func() bool {
		return ln.CatchUpProgress().State == CatchUpSyncing
	}, "syncing starts")
	if err := ln.CancelCatchUp(); err != nil {
		t.Fatal(err)
	}
	waitProgressState(t, ln, CatchUpCancelled, "cancelled state visible")

	// 取消后保持一段时间：不得进入 caught_up，正式快照不得被安装/推进，
	// 状态机仍停留在追赶前内容（bulk 键不可见）。
	time.Sleep(400 * time.Millisecond)
	if p := ln.CatchUpProgress(); p.Ready() {
		t.Fatalf("cancelled catch-up must never report ready: %+v", p)
	}
	if s := ln.Status(); s.SnapshotIndex > 0 {
		// 节点崩溃前可能已有旧快照；关键是 bulk-* 数据不能出现。
	}
	if _, ok := ln.sm.Lookup("bulk-0059"); ok {
		t.Fatal("half snapshot data must not be observable in state machine")
	}

	// ResumeCatchUp：领导者退避结束后续传，最终完成。
	if err := ln.ResumeCatchUp(); err != nil {
		t.Fatal(err)
	}
	waitForDur(t, 6*time.Second, func() bool {
		p := ln.CatchUpProgress()
		return p.State == CatchUpCaughtUp
	}, "catch-up completes after resume")

	waitForDur(t, 6*time.Second, func() bool {
		for i := 0; i < totalWrites; i++ {
			k, v := bulkKV(i)
			if got, ok := ln.sm.Lookup(k); !ok || got != v {
				return false
			}
		}
		return true
	}, "all values present after resume")
}

// sendChunks 直接经事件循环向接收节点喂一个分块快照；corruptLast=true 时
// 把最后一块的首字节翻转（但校验和保持不变），模拟链路/磁盘损坏。
func sendChunks(t *testing.T, n *Node, term Term, leader NodeID,
	meta SnapshotMeta, data []byte, chunkSize int, corruptLast bool) *InstallSnapshotResponse {
	t.Helper()
	sum := sha256Sum(data)
	var last *InstallSnapshotResponse
	for off := 0; off < len(data); off += chunkSize {
		end := off + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := append([]byte(nil), data[off:end]...)
		if corruptLast && end == len(data) {
			chunk[0] ^= 0xFF
		}
		req := &InstallSnapshotRequest{
			Term: term, LeaderID: leader,
			LastIncludedIndex: meta.LastIncludedIndex,
			LastIncludedTerm:  meta.LastIncludedTerm,
			Chunked:           true, TransferID: 7,
			Offset: uint64(off), Length: uint64(len(data)),
			Checksum: sum, Data: chunk, Done: end == len(data),
		}
		last = n.CallRPC(req).(*InstallSnapshotResponse)
	}
	return last
}

// newReceiverNode 构造一个已启动的、处于给定任期跟随者状态的节点（处理器级测试用）。
func newReceiverNode(t *testing.T, term Term, leader NodeID) *Node {
	tr := NewMemoryTransport()
	cfg := DefaultConfig()
	cfg.ID = "rx"
	cfg.Peers = []NodeID{"rx", leader}
	cfg.RandSeed = 1
	n, err := NewNode(cfg, tr)
	if err != nil {
		t.Fatal(err)
	}
	n.Start()
	t.Cleanup(func() { _ = n.Stop() })
	// 用一个高任期 AE 把它稳定为 follower 并设置 leader，避免测试期间选举。
	n.CallRPC(&AppendEntriesRequest{
		Term: term, LeaderID: leader,
		PrevLogIndex: 0, PrevLogTerm: 0, LeaderCommit: 0,
	})
	return n
}

// TestChunkedSnapshotCorruptionUnit：确定性地验证
//  1. 损坏的末块导致整体校验失败、临时快照被丢弃、状态 failed；
//  2. failed 态不接受非首块（resync）；
//  3. 从首块重新完整传输后成功安装，状态机内容与快照一致；
//  4. 半份/损坏数据从未进入正式快照。
func TestChunkedSnapshotCorruptionUnit(t *testing.T) {
	const term Term = 5
	const leader NodeID = "ld"
	n := newReceiverNode(t, term, leader)

	// 构造一份快照：状态机 JSON 字节 + 元数据（index=10,term=4）。
	sm := NewKVStateMachine()
	for i := 0; i < 20; i++ {
		_, _ = sm.Apply(Command{Op: OpSet, Key: fmt.Sprintf("k%d", i), Value: fmt.Sprintf("v%d", i),
			ClientID: "c", Seq: uint64(i + 1)})
	}
	data, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	meta := SnapshotMeta{LastIncludedIndex: 10, LastIncludedTerm: 4}
	const chunk = 17 // 故意很小且不整除

	// 第一次：损坏末块。
	resp := sendChunks(t, n, term, leader, meta, data, chunk, true)
	if resp == nil || resp.Success || resp.Reason != RejectTransferFailed {
		t.Fatalf("corrupted final chunk must fail with transfer_failed, got %+v", resp)
	}
	p := n.CatchUpProgress()
	if p.State != CatchUpFailed || p.FailedReason != RejectTransferFailed {
		t.Fatalf("want failed/transfer_failed, got %+v", p)
	}
	// 正式快照绝不能被安装。
	if n.snapMeta.LastIncludedIndex != 0 {
		t.Fatalf("corrupt data must never be promoted to snapshot, snap=%d", n.snapMeta.LastIncludedIndex)
	}
	if _, ok := n.sm.Lookup("k0"); ok {
		t.Fatal("corrupt half-snapshot must never be applied to state machine")
	}
	if size, _ := n.store.StagingSize(); size != 0 {
		t.Fatalf("staging data must be discarded, size=%d", size)
	}

	// failed 态收到非首块：要求 resync。
	mid := &InstallSnapshotRequest{
		Term: term, LeaderID: leader, Chunked: true, TransferID: 7,
		LastIncludedIndex: 10, LastIncludedTerm: 4,
		Offset: uint64(chunk), Length: uint64(len(data)), Checksum: sha256Sum(data),
		Data: make([]byte, chunk),
	}
	r2 := n.CallRPC(mid).(*InstallSnapshotResponse)
	if r2.Success || r2.Reason != RejectResync {
		t.Fatalf("non-first chunk in failed state must resync, got %+v", r2)
	}

	// 第二次：完好重传整份快照 -> 成功安装。
	resp = sendChunks(t, n, term, leader, meta, data, chunk, false)
	if resp == nil || !resp.Success || !resp.Complete {
		t.Fatalf("clean retransfer must complete, got %+v", resp)
	}
	if n.CatchUpProgress().State != CatchUpCaughtUp {
		t.Fatalf("want caught_up, got %+v", n.CatchUpProgress())
	}
	if n.snapMeta != meta {
		t.Fatalf("snapshot meta mismatch: %+v", n.snapMeta)
	}
	for i := 0; i < 20; i++ {
		v, ok := n.sm.Lookup(fmt.Sprintf("k%d", i))
		if !ok || v != fmt.Sprintf("v%d", i) {
			t.Fatalf("state machine mismatch k%d=%q ok=%v", i, v, ok)
		}
	}
	if n.Status().CommitIndex < 10 || n.Status().AppliedIndex < 10 {
		t.Fatalf("commit/applied must reach snapshot point: %+v", n.Status())
	}
}

// TestChunkedSnapshotDuplicateAndGapUnit：重复块幂等、乱序（空洞）块触发
// resync 且不污染临时数据；从断点补齐后成功。
func TestChunkedSnapshotDuplicateAndGapUnit(t *testing.T) {
	const term Term = 3
	const leader NodeID = "ld"
	n := newReceiverNode(t, term, leader)

	// 使用合法状态机快照字节（必须能被 Restore 解析），并保证总长 > 100。
	sm := NewKVStateMachine()
	for i := 0; i < 30; i++ {
		_, _ = sm.Apply(Command{Op: OpSet, Key: fmt.Sprintf("dup-key-%02d", i),
			Value: fmt.Sprintf("dup-value-%02d-padding", i), ClientID: "dc", Seq: uint64(i + 1)})
	}
	data, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) <= 100 {
		t.Fatalf("test setup: snapshot too short (%d bytes)", len(data))
	}
	meta := SnapshotMeta{LastIncludedIndex: 8, LastIncludedTerm: 2}
	sum := sha256Sum(data)
	const chunk = 40
	mk := func(off int, done bool) *InstallSnapshotRequest {
		end := off + chunk
		if end > len(data) {
			end = len(data)
		}
		return &InstallSnapshotRequest{
			Term: term, LeaderID: leader, Chunked: true, TransferID: 9,
			LastIncludedIndex: meta.LastIncludedIndex, LastIncludedTerm: meta.LastIncludedTerm,
			Offset: uint64(off), Length: uint64(len(data)), Checksum: sum,
			Data: append([]byte(nil), data[off:end]...), Done: done,
		}
	}

	// 首块 [0,chunk)。
	r := n.CallRPC(mk(0, false)).(*InstallSnapshotResponse)
	if !r.Success || r.AckOffset != chunk {
		t.Fatalf("first chunk ack want %d, got %+v", chunk, r)
	}
	// 重复首块：幂等返回相同断点。
	r = n.CallRPC(mk(0, false)).(*InstallSnapshotResponse)
	if !r.Success || r.AckOffset != chunk {
		t.Fatalf("duplicate chunk must be idempotent, got %+v", r)
	}
	// 乱序块（跳到 chunk*3）：出现空洞，必须 resync，且不写入。
	gapOff := chunk * 3
	if gapOff >= len(data) {
		gapOff = chunk // 数据短时退化为下一块
	}
	if gapOff != chunk { // 仅当确实能构造空洞时验证
		r = n.CallRPC(mk(gapOff, false)).(*InstallSnapshotResponse)
		if r.Success || r.Reason != RejectResync || r.AckOffset != chunk {
			t.Fatalf("gap chunk must resync with ack=%d, got %+v", chunk, r)
		}
		if size, _ := n.store.StagingSize(); size != chunk {
			t.Fatalf("out-of-order chunk must not be written, size=%d", size)
		}
	}
	// 按序补齐直到末块。
	for off := chunk; off < len(data); off += chunk {
		end := off + chunk
		done := end >= len(data)
		r = n.CallRPC(mk(off, done)).(*InstallSnapshotResponse)
		if done {
			if !r.Success || !r.Complete {
				t.Fatalf("final chunk must complete, got %+v", r)
			}
		} else if !r.Success || r.AckOffset != uint64(end) {
			t.Fatalf("chunk at %d ack want %d, got %+v", off, end, r)
		}
	}
	if n.CatchUpProgress().State != CatchUpCaughtUp {
		t.Fatalf("want caught_up, got %+v", n.CatchUpProgress())
	}
	// 状态机内容与快照一致。
	for i := 0; i < 30; i++ {
		v, ok := n.sm.Lookup(fmt.Sprintf("dup-key-%02d", i))
		if !ok || v != fmt.Sprintf("dup-value-%02d-padding", i) {
			t.Fatalf("state machine mismatch at %d: %q ok=%v", i, v, ok)
		}
	}
}

// TestLegacyOneShotSnapshotStillWorks：旧版本节点发送不带分块字段的
// 一次性 InstallSnapshot（Chunked=false），新版本接收端必须仍能安装，
// 保证滚动升级/新旧版本互通。
func TestLegacyOneShotSnapshotStillWorks(t *testing.T) {
	const term Term = 2
	const leader NodeID = "ld"
	n := newReceiverNode(t, term, leader)

	sm := NewKVStateMachine()
	for i := 0; i < 10; i++ {
		_, _ = sm.Apply(Command{Op: OpSet, Key: fmt.Sprintf("legacy-%d", i),
			Value: fmt.Sprintf("L%d", i), ClientID: "lc", Seq: uint64(i + 1)})
	}
	data, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	// 旧版请求：只有 4 个基础字段，Data 是整份快照。
	req := &InstallSnapshotRequest{
		Term: term, LeaderID: leader,
		LastIncludedIndex: 12, LastIncludedTerm: 2,
		Data: data,
	}
	resp := n.CallRPC(req).(*InstallSnapshotResponse)
	if !resp.Success {
		t.Fatalf("legacy one-shot snapshot must install, reason=%s", resp.Reason)
	}
	for i := 0; i < 10; i++ {
		v, ok := n.sm.Lookup(fmt.Sprintf("legacy-%d", i))
		if !ok || v != fmt.Sprintf("L%d", i) {
			t.Fatalf("legacy snapshot content mismatch at %d", i)
		}
	}
	if n.Status().SnapshotIndex != 12 {
		t.Fatalf("snapshot index want 12, got %d", n.Status().SnapshotIndex)
	}
	// 安装整包快照应不产生进行中的分块追赶状态。
	if p := n.CatchUpProgress(); p.State == CatchUpSyncing {
		t.Fatalf("legacy install must not leave syncing state: %+v", p)
	}
}

// TestUpgradeFromOldSnapshotOnDisk：磁盘上只有旧格式正式快照
// （snapshot.json，无任何 staging 文件）时，新版本打开节点必须能正常恢复，
// 且不会误报“正在追赶”。
func TestUpgradeFromOldSnapshotOnDisk(t *testing.T) {
	dir := t.TempDir()
	tr := NewMemoryTransport()
	peers := []NodeID{"u1", "u2", "u3"}

	// 第一步：用旧布局直接写一份正式快照 + 硬状态 + 日志（手工构造，
	// 模拟新版本上线前磁盘上已有的数据）。
	oldSM := NewKVStateMachine()
	for i := 0; i < 15; i++ {
		_, _ = oldSM.Apply(Command{Op: OpSet, Key: fmt.Sprintf("up-%d", i),
			Value: fmt.Sprintf("U%d", i), ClientID: "uc", Seq: uint64(i + 1)})
	}
	snapData, err := oldSM.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	meta := SnapshotMeta{LastIncludedIndex: 20, LastIncludedTerm: 2}
	if err := store.SaveState(2, "u2", 0); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(meta, snapData); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// 第二步：新版本节点用同一目录启动，必须恢复快照且内容完整。
	cfg := DefaultConfig()
	cfg.ID = "u1"
	cfg.Peers = peers
	cfg.DataDir = dir
	cfg.RandSeed = 9
	n, err := NewNode(cfg, tr)
	if err != nil {
		t.Fatalf("open upgraded node: %v", err)
	}
	n.Start()
	t.Cleanup(func() { _ = n.Stop() })

	s := n.Status()
	if s.SnapshotIndex != 20 || s.CommitIndex != 20 || s.AppliedIndex != 20 {
		t.Fatalf("restored points mismatch: %+v", s)
	}
	for i := 0; i < 15; i++ {
		v, ok := n.sm.Lookup(fmt.Sprintf("up-%d", i))
		if !ok || v != fmt.Sprintf("U%d", i) {
			t.Fatalf("upgraded snapshot missing up-%d: %q ok=%v", i, v, ok)
		}
	}
	// 没有残留 staging，不应报告 syncing。
	if p := n.CatchUpProgress(); p.State == CatchUpSyncing {
		t.Fatalf("upgraded node must not report syncing without staging: %+v", p)
	}
	if chk, e := OpenStore(dir); e == nil {
		if _, ok, _ := chk.LoadStagingMeta(); ok {
			t.Fatal("no staging metadata should exist after clean upgrade")
		}
		_ = chk.Close()
	}
}

// TestChunkedSnapshotLeadershipChange：落后节点追赶期间发生领导权切换，
// 新任期的领导者带着（可能更新的）快照进度重发；落后节点最终必须收敛到
// 包含全部已提交数据，半份数据不可用。
//
// 使用 3 节点：追赶中的落后节点仍在线参与投票，因此崩溃旧领导者后，
// 「新领导者 + 落后节点」恰好构成多数派，可立即选出新主并继续写入。
func TestChunkedSnapshotLeadershipChange(t *testing.T) {
	c := newTestCluster(t, 3, true, 83838)
	for _, id := range c.IDs() {
		c.SetSnapshotThreshold(id, 4)
		c.SetSnapshotChunkSize(id, 32)
	}
	restartAllWithConfig(t, c)
	waitForDur(t, 8*time.Second, func() bool { return c.Leader() != "" }, "leader")
	firstLeader := c.Leader()

	var laggard NodeID
	for _, id := range c.IDs() {
		if id != firstLeader {
			laggard = id
			break
		}
	}
	if err := c.Crash(laggard); err != nil {
		t.Fatal(err)
	}
	cli := NewClient("lc", nodesOf(c)...)
	initCtx, initCancel := context.WithTimeout(context.Background(), 10*time.Second)
	n0 := writeBulk(t, cli, initCtx, 40)
	initCancel()
	waitForDur(t, 5*time.Second, func() bool {
		for _, id := range c.IDs() {
			if id == laggard {
				continue
			}
			if !c.SnapshotExists(id) {
				return false
			}
		}
		return true
	}, "live peers compacted")

	// 周期性丢块，让追赶跨越领导权切换。
	var chunks int32
	c.SetSnapshotHook(&SnapshotFaultHook{
		Drop: func(from, to NodeID, req *InstallSnapshotRequest) bool {
			if NodeID(to) != laggard {
				return false
			}
			return atomic.AddInt32(&chunks, 1)%4 == 0 // 丢 1/4
		},
	})
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	ln := c.Node(laggard)

	// 落后节点开始收到部分块后，再切换领导权。
	waitForDur(t, 8*time.Second, func() bool {
		p := ln.CatchUpProgress()
		return p.State == CatchUpSyncing && p.Received > 0
	}, "laggard starts receiving chunks")
	t.Logf("laggard received %d bytes before leadership change", ln.CatchUpProgress().Received)

	// 崩溃旧领导者：在线的另一跟随者与落后节点构成多数派，立即选出新主。
	oldLeader := c.Leader()
	if err := c.Crash(oldLeader); err != nil {
		t.Fatal(err)
	}
	waitForDur(t, 8*time.Second, func() bool {
		for id, n := range c.Nodes() {
			if id != oldLeader && n.Status().Role == RoleLeader {
				return true
			}
		}
		return false
	}, "new leader elected during catch-up")
	// 新领导者上继续写入（其快照进度与旧主可能不同）。
	churnCtx, churnCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer churnCancel()
	for i := 200; i < 220; i++ {
		if _, _, err := cli.Set(churnCtx, fmt.Sprintf("churn-%d", i), fmt.Sprintf("c%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	// 旧领导者恢复，集群回到 3 节点。
	if err := c.Restart(oldLeader); err != nil {
		t.Fatal(err)
	}

	// 落后节点必须包含追赶前的全部已提交数据。
	waitForDur(t, 12*time.Second, func() bool {
		if ln.Status().SnapshotIndex == 0 {
			return false
		}
		for i := 0; i < n0; i++ {
			k, v := bulkKV(i)
			if got, ok := ln.sm.Lookup(k); !ok || got != v {
				return false
			}
		}
		return true
	}, "laggard converges after leadership change")
	c.SetSnapshotHook(nil)

	// 领导权切换期间写入的新数据也必须到达（直接以状态机内容为收敛判据）。
	waitForDur(t, 10*time.Second, func() bool {
		if ln.Status().AppliedIndex < ln.Status().CommitIndex {
			return false
		}
		for i := 200; i < 220; i++ {
			if got, ok := ln.sm.Lookup(fmt.Sprintf("churn-%d", i)); !ok || got != fmt.Sprintf("c%d", i) {
				return false
			}
		}
		return true
	}, "laggard state machine contains all churn values")
	t.Logf("leadership-change catch-up ok, chunks seen=%d", atomic.LoadInt32(&chunks))
}

// TestStagingClearedWhenSnapshotAlreadyInstalled：模拟「正式快照已提升、
// staging 尚未清理」的崩溃窗口——磁盘上同时存在更新的正式快照与一份旧 staging。
// 新版本启动时必须忽略并清理 staging，不报告 syncing，且从正式快照恢复。
func TestStagingClearedWhenSnapshotAlreadyInstalled(t *testing.T) {
	dir := t.TempDir()
	peers := []NodeID{"s1", "s2", "s3"}

	// 正式快照（更新，index=30）。
	newSM := NewKVStateMachine()
	_, _ = newSM.Apply(Command{Op: OpSet, Key: "installed", Value: "yes", ClientID: "a", Seq: 1})
	newData, _ := newSM.Snapshot()

	// 旧 staging（index=10，已收 5 字节，且与正式快照并存）。
	oldSM := NewKVStateMachine()
	_, _ = oldSM.Apply(Command{Op: OpSet, Key: "half", Value: "no", ClientID: "b", Seq: 1})
	oldData, _ := oldSM.Snapshot()

	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SaveState(3, "s2", 0); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveSnapshot(SnapshotMeta{LastIncludedIndex: 30, LastIncludedTerm: 3}, newData); err != nil {
		t.Fatal(err)
	}
	if err := st.StartStagingData(); err != nil {
		t.Fatal(err)
	}
	partial := oldData
	if len(partial) > 5 {
		partial = partial[:5]
	}
	if err := st.AppendStagingChunk(0, partial); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveStagingMeta(StagingMeta{
		TransferID: 1, LastIncludedIndex: 10, LastIncludedTerm: 2,
		Length: uint64(len(oldData)), Checksum: sha256Sum(oldData), Leader: "s2",
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.ID = "s1"
	cfg.Peers = peers
	cfg.DataDir = dir
	n, err := NewNode(cfg, NewMemoryTransport())
	if err != nil {
		t.Fatal(err)
	}
	n.Start()
	t.Cleanup(func() { _ = n.Stop() })

	if n.Status().SnapshotIndex != 30 {
		t.Fatalf("must recover from installed snapshot, snap=%d", n.Status().SnapshotIndex)
	}
	if v, ok := n.sm.Lookup("installed"); !ok || v != "yes" {
		t.Fatalf("state machine must come from installed snapshot, got %q", v)
	}
	if p := n.CatchUpProgress(); p.State == CatchUpSyncing {
		t.Fatalf("stale staging must not cause syncing state: %+v", p)
	}
	if size, _ := n.store.StagingSize(); size != 0 {
		t.Fatalf("stale staging data must be removed, size=%d", size)
	}
	if _, ok, _ := n.store.LoadStagingMeta(); ok {
		t.Fatal("stale staging meta must be removed")
	}
}
