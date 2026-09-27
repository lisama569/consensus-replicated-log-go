package raft

import (
	"context"
	"fmt"
	"hash/crc32"
	"strings"
	"testing"
	"time"
)

// bigVal 生成较大的值，让快照体积远超测试用的小块大小，强制走分块路径。
func bigVal(i int) string { return fmt.Sprintf("v-%d-%s", i, strings.Repeat("x", 180)) }

// waitForLong 与 waitFor 相同，但使用更长超时：
// 多分块传输在丢包重传下耗时秒级，-race 下 fsync/事件循环还会更慢。
func waitForLong(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting: %s", msg)
}

// setupChunkedCluster 建立 3 节点持久化集群（小压缩阈值 + 小块大小），
// 制造一个错过多次压缩的落后节点，返回集群与落后节点 ID。
// 返回时：存活两节点的快照已覆盖全部写入（快照内容稳定，不再推进），
// laggard 处于崩溃状态。
func setupChunkedCluster(t *testing.T, seed int64, chunkSize, writes int) (*Cluster, NodeID) {
	t.Helper()
	c := newTestCluster(t, 3, true, seed)
	for _, id := range c.IDs() {
		c.SetSnapshotThreshold(id, 5)
		c.SetSnapshotChunkSize(id, chunkSize)
	}
	for _, id := range c.IDs() {
		if err := c.Crash(id); err != nil {
			t.Fatal(err)
		}
		if err := c.Restart(id); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")

	leader := c.Leader()
	var laggard NodeID
	for _, id := range c.IDs() {
		if id != leader {
			laggard = id
			break
		}
	}
	if err := c.Crash(laggard); err != nil {
		t.Fatal(err)
	}

	cli := NewClient("chunk", nodesOf(c)...)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var lastIdx Index
	for i := 0; i < writes; i++ {
		idx, _, err := cli.Set(ctx, key(i), bigVal(i))
		if err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
		lastIdx = idx
	}
	// 等存活节点的快照推进到距日志末尾不足一个阈值：此后快照内容稳定，
	// 换主后新领导者持有同一份快照（同会话 ID），才能验证跨主续传。
	const threshold = 5 // 与上方 SetSnapshotThreshold 一致
	waitForLong(t, func() bool {
		for _, id := range c.IDs() {
			if id == laggard {
				continue
			}
			if c.Node(id).Status().SnapshotIndex+threshold < lastIdx {
				return false
			}
		}
		return true
	}, "live peers compacted a large stable snapshot")
	return c, laggard
}

// waitCatchup 等待节点追赶状态满足条件。
func waitCatchup(t *testing.T, n *Node, msg string, cond func(CatchupStatus) bool) CatchupStatus {
	t.Helper()
	var s CatchupStatus
	waitForLong(t, func() bool {
		s = n.CatchupStatus()
		return cond(s)
	}, msg)
	return s
}

// requireKeys 要求节点状态机包含 [0,n) 全部键且值正确。
func requireKeys(t *testing.T, n *Node, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		if v, ok := n.sm.Lookup(key(i)); !ok || v != bigVal(i) {
			t.Fatalf("node %s missing %s: %q ok=%v", n.cfg.ID, key(i), v, ok)
		}
	}
}

// TestChunkedSnapshotBasic：大快照分多块传输，落后节点追平，
// 进度状态经历 Receiving -> Done，且确认字节数等于总字节数。
func TestChunkedSnapshotBasic(t *testing.T) {
	c, laggard := setupChunkedCluster(t, 1101, 64, 30)

	// 落后节点重启前：无追赶任务。
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	n := c.Node(laggard)

	// 传输开始：应观察到 Receiving 且总量超过一个块（证明走了分块路径）。
	st := waitCatchup(t, n, "receiving chunked snapshot", func(s CatchupStatus) bool {
		return s.State == CatchupReceiving || s.State == CatchupDone
	})
	if st.Total <= 64 {
		t.Fatalf("expected multi-chunk snapshot, total=%d", st.Total)
	}

	// 完成：Done 且 Received == Total。
	st = waitCatchup(t, n, "chunked snapshot done", func(s CatchupStatus) bool {
		return s.State == CatchupDone
	})
	if st.Received != st.Total || st.Total == 0 {
		t.Fatalf("done but received=%d total=%d", st.Received, st.Total)
	}
	if !st.Ready() {
		t.Fatalf("node should be ready after done")
	}

	// 状态机包含全部已提交数据，且能继续跟随新写入。
	waitForLong(t, func() bool {
		for i := 0; i < 30; i++ {
			if v, ok := n.sm.Lookup(key(i)); !ok || v != bigVal(i) {
				return false
			}
		}
		return true
	}, "laggard state machine complete after chunked catchup")

	cli := NewClient("chunk2", nodesOf(c)...)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := 30; i < 40; i++ {
		if _, _, err := cli.Set(ctx, key(i), bigVal(i)); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	waitForLong(t, func() bool {
		for i := 30; i < 40; i++ {
			if v, ok := n.sm.Lookup(key(i)); !ok || v != bigVal(i) {
				return false
			}
		}
		return true
	}, "laggard follows new writes after catchup")
	t.Logf("laggard %s caught up via chunked snapshot: %+v", laggard, n.CatchupStatus())
}

// TestChunkedSnapshotResumeAfterReceiverRestart：传输中途接收端崩溃，
// 重启后从已确认偏移续传（检查点持久化），不从头重来。
func TestChunkedSnapshotResumeAfterReceiverRestart(t *testing.T) {
	c, laggard := setupChunkedCluster(t, 1102, 64, 40)
	leader := c.Leader()

	// 对 chunk 方向注入 50% 丢包，拉长传输窗口。
	c.Flaky(leader, laggard, RPCInstallSnapshotChunk, FaultPolicy{DropRate: 0.5})
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}

	// 等到至少确认了一块但未传完。
	n := c.Node(laggard)
	st := waitCatchup(t, n, "partial chunks received", func(s CatchupStatus) bool {
		return s.State == CatchupReceiving && s.Received > 0 && s.Received < s.Total
	})
	confirmed := st.Received
	t.Logf("crashing receiver with %d/%d bytes confirmed", confirmed, st.Total)

	// 先阻断后续块（至多再落地一个在途块），再崩溃重启，
	// 保证重启时传输必然未完成、检查点必然只是部分进度。
	c.Flaky(leader, laggard, RPCInstallSnapshotChunk, FaultPolicy{DropRate: 1})
	if err := c.Crash(laggard); err != nil {
		t.Fatal(err)
	}
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	n = c.Node(laggard)
	st = n.CatchupStatus()
	if st.State != CatchupReceiving || st.Received < confirmed {
		t.Fatalf("checkpoint lost across restart: state=%s received=%d (was %d)",
			st.State, st.Received, confirmed)
	}

	// 恢复网络：从已确认位置续传完成，数据完整。
	c.HealAll()
	st = waitCatchup(t, n, "resumed chunked snapshot done", func(s CatchupStatus) bool {
		return s.State == CatchupDone
	})
	if st.Received != st.Total {
		t.Fatalf("done with received=%d total=%d", st.Received, st.Total)
	}
	waitForLong(t, func() bool {
		for i := 0; i < 40; i++ {
			if v, ok := n.sm.Lookup(key(i)); !ok || v != bigVal(i) {
				return false
			}
		}
		return true
	}, "state machine complete after resume")
}

// TestChunkedSnapshotDuplicateAndReorder：chunk 方向注入重复+乱序+丢包，
// 接收端幂等处理，最终完整安装且数据一致。
func TestChunkedSnapshotDuplicateAndReorder(t *testing.T) {
	c, laggard := setupChunkedCluster(t, 1103, 64, 30)
	leader := c.Leader()
	c.Flaky(leader, laggard, RPCInstallSnapshotChunk,
		FaultPolicy{DropRate: 0.1, Duplicate: true, Reorder: true})

	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	n := c.Node(laggard)
	st := waitCatchup(t, n, "chunked snapshot done under dup/reorder", func(s CatchupStatus) bool {
		return s.State == CatchupDone
	})
	if st.Received != st.Total {
		t.Fatalf("done with received=%d total=%d", st.Received, st.Total)
	}
	waitForLong(t, func() bool {
		for i := 0; i < 30; i++ {
			if v, ok := n.sm.Lookup(key(i)); !ok || v != bigVal(i) {
				return false
			}
		}
		return true
	}, "state machine complete under dup/reorder")
}

// TestChunkedSnapshotResumeAcrossLeaderChange：传输中途领导者崩溃，
// 新领导者持有相同快照（同会话 ID），接收端从已确认位置续传完成。
func TestChunkedSnapshotResumeAcrossLeaderChange(t *testing.T) {
	c, laggard := setupChunkedCluster(t, 1104, 64, 40)
	oldLeader := c.Leader()

	// 拖慢传输：50% 丢包，确保传输窗口覆盖换主过程。
	c.Flaky(oldLeader, laggard, RPCInstallSnapshotChunk, FaultPolicy{DropRate: 0.5})
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	n := c.Node(laggard)
	st := waitCatchup(t, n, "partial chunks before leader crash", func(s CatchupStatus) bool {
		return s.State == CatchupReceiving && s.Received > 0 && s.Received < s.Total
	})
	session := st.SessionID
	confirmed := st.Received
	t.Logf("leader %s crashing mid-transfer: session=%s %d/%d", oldLeader, session, confirmed, st.Total)

	// 阻断旧方向的块并崩溃旧领导者，等待新领导者产生。
	c.Flaky(oldLeader, laggard, RPCInstallSnapshotChunk, FaultPolicy{DropRate: 1})
	if err := c.Crash(oldLeader); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		l := c.Leader()
		return l != "" && l != oldLeader
	}, "new leader elected")

	// 新领导者持有同一份快照文件：会话 ID 相同，接收端从检查点续传。
	c.HealAll()
	st = waitCatchup(t, n, "chunked snapshot done after leader change", func(s CatchupStatus) bool {
		return s.State == CatchupDone
	})
	if st.SessionID != session {
		t.Fatalf("expected resume of session %s, got new session %s", session, st.SessionID)
	}
	if st.Received != st.Total || st.Total < confirmed {
		t.Fatalf("bad final progress: %+v", st)
	}
	waitForLong(t, func() bool {
		for i := 0; i < 40; i++ {
			if v, ok := n.sm.Lookup(key(i)); !ok || v != bigVal(i) {
				return false
			}
		}
		return true
	}, "state machine complete after leader change")
}

// TestChunkedSnapshotConcurrentWritesAndCompaction：传输期间集群继续写入并压缩，
// 落后节点最终包含所有已确认提交的数据，不丢、不回退。
func TestChunkedSnapshotConcurrentWritesAndCompaction(t *testing.T) {
	c, laggard := setupChunkedCluster(t, 1105, 64, 30)
	leader := c.Leader()

	// 拉长传输窗口：丢包+乱序。
	c.Flaky(leader, laggard, RPCInstallSnapshotChunk,
		FaultPolicy{DropRate: 0.1, Reorder: true})
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	n := c.Node(laggard)
	waitCatchup(t, n, "receiving", func(s CatchupStatus) bool {
		return s.State == CatchupReceiving || s.State == CatchupDone
	})

	// 传输期间继续写入（触发领导者进一步压缩，快照进度与接收端交叠）。
	cli := NewClient("chunk3", nodesOf(c)...)
	// -race 下 fsync/事件循环较慢，给写入留出充足超时。
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := 30; i < 60; i++ {
		if _, _, err := cli.Set(ctx, key(i), bigVal(i)); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}

	// 最终：落后节点必须包含全部 60 个已确认提交的键。
	c.HealAll()
	waitForLong(t, func() bool {
		for i := 0; i < 60; i++ {
			if v, ok := n.sm.Lookup(key(i)); !ok || v != bigVal(i) {
				return false
			}
		}
		return true
	}, "laggard converges to all committed data during overlapping transfer/compaction")
	st := n.CatchupStatus()
	t.Logf("final catchup status: %+v commit=%d", st, n.Status().CommitIndex)
}

// TestChunkedSnapshotCancel：接收中取消：状态置为 Cancelled、部分数据被丢弃、
// 半份数据不会被安装；领导者重试后重新传输并最终完成。
func TestChunkedSnapshotCancel(t *testing.T) {
	c, laggard := setupChunkedCluster(t, 1106, 64, 30)
	leader := c.Leader()
	c.Flaky(leader, laggard, RPCInstallSnapshotChunk, FaultPolicy{DropRate: 0.5})
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	n := c.Node(laggard)
	st := waitCatchup(t, n, "partial receive before cancel", func(s CatchupStatus) bool {
		return s.State == CatchupReceiving && s.Received > 0 && s.Received < s.Total
	})

	// 阻断后续块后取消：状态 Cancelled，且快照内容不得出现在状态机中。
	c.Flaky(leader, laggard, RPCInstallSnapshotChunk, FaultPolicy{DropRate: 1})
	if !n.CancelCatchup() {
		t.Fatalf("cancel should report an in-flight session")
	}
	st = n.CatchupStatus()
	if st.State != CatchupCancelled {
		t.Fatalf("expected Cancelled, got %s", st.State)
	}
	if _, ok := n.sm.Lookup(key(0)); ok {
		t.Fatalf("partial snapshot must not be installed after cancel")
	}
	if n.CancelCatchup() {
		t.Fatalf("second cancel should report no in-flight session")
	}

	// 恢复网络：领导者重试，重新传输并完成（取消不阻止后续追赶）。
	c.HealAll()
	waitCatchup(t, n, "done after cancel+retry", func(s CatchupStatus) bool {
		return s.State == CatchupDone
	})
	waitForLong(t, func() bool {
		for i := 0; i < 30; i++ {
			if v, ok := n.sm.Lookup(key(i)); !ok || v != bigVal(i) {
				return false
			}
		}
		return true
	}, "state machine complete after cancel+retry")
}

// TestChunkedSnapshotRestartAfterCompletion：追赶完成后重启，状态机与提交位置一致。
func TestChunkedSnapshotRestartAfterCompletion(t *testing.T) {
	c, laggard := setupChunkedCluster(t, 1107, 64, 30)
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	n := c.Node(laggard)
	waitCatchup(t, n, "done", func(s CatchupStatus) bool { return s.State == CatchupDone })
	waitForLong(t, func() bool {
		for i := 0; i < 30; i++ {
			if v, ok := n.sm.Lookup(key(i)); !ok || v != bigVal(i) {
				return false
			}
		}
		return true
	}, "complete before restart")
	commitBefore := n.Status().CommitIndex

	if err := c.Crash(laggard); err != nil {
		t.Fatal(err)
	}
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	n = c.Node(laggard)
	// 重启后：无残留会话（Idle），提交位置不回退，数据完整。
	if st := n.CatchupStatus(); st.State != CatchupIdle {
		t.Fatalf("expected Idle after restart, got %s (%+v)", st.State, st)
	}
	waitFor(t, func() bool { return n.Status().CommitIndex >= commitBefore }, "commit not regressed")
	requireKeys(t, n, 30)
}

// TestSmallSnapshotStaysLegacy：小快照仍走一次性 InstallSnapshot，
// 不产生分块会话（向后兼容旧路径）。
func TestSmallSnapshotStaysLegacy(t *testing.T) {
	c := newTestCluster(t, 3, true, 1108)
	for _, id := range c.IDs() {
		c.SetSnapshotThreshold(id, 5)
		// 默认 64KB 块：小快照不会触发分块。
	}
	for _, id := range c.IDs() {
		if err := c.Crash(id); err != nil {
			t.Fatal(err)
		}
		if err := c.Restart(id); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	leader := c.Leader()
	var laggard NodeID
	for _, id := range c.IDs() {
		if id != leader {
			laggard = id
			break
		}
	}
	if err := c.Crash(laggard); err != nil {
		t.Fatal(err)
	}
	cli := NewClient("legacy", nodesOf(c)...)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := 0; i < 20; i++ {
		if _, _, err := cli.Set(ctx, key(i), val(i)); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	waitFor(t, func() bool {
		for _, id := range c.IDs() {
			if id != laggard && !c.SnapshotExists(id) {
				return false
			}
		}
		return true
	}, "live peers compacted")
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	n := c.Node(laggard)
	waitForLong(t, func() bool {
		for i := 0; i < 20; i++ {
			if v, ok := n.sm.Lookup(key(i)); !ok || v != val(i) {
				return false
			}
		}
		return true
	}, "laggard caught up via legacy whole snapshot")
	// 小快照走整份路径：不应出现分块接收状态。
	if st := n.CatchupStatus(); st.State == CatchupReceiving {
		t.Fatalf("small snapshot must not use chunked transfer: %+v", st)
	}
}

// ---- 单元级：直接驱动接收端处理器，确定性覆盖交错/乱序/重复/失败 ----

// makeSnapData 构造一份合法的状态机快照字节。
func makeSnapData(t *testing.T, kvs map[string]string) []byte {
	t.Helper()
	sm := NewKVStateMachine()
	for k, v := range kvs {
		if _, err := sm.Apply(Command{Op: OpSet, Key: k, Value: v}); err != nil {
			t.Fatal(err)
		}
	}
	data, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// chunk 构造一个快照块请求。
func chunk(term Term, session string, idx Index, off, total uint64, crc uint32, data []byte, done bool) *InstallSnapshotChunkRequest {
	return &InstallSnapshotChunkRequest{
		Term:              term,
		LeaderID:          "test-leader",
		SessionID:         session,
		LastIncludedIndex: idx,
		LastIncludedTerm:  1,
		Offset:            off,
		TotalSize:         total,
		CRC:               crc,
		Data:              data,
		Done:              done,
	}
}

// callChunk 在事件循环内同步投递一个块并返回响应。
func callChunk(n *Node, req *InstallSnapshotChunkRequest) *InstallSnapshotChunkResponse {
	return n.CallRPC(req).(*InstallSnapshotChunkResponse)
}

// TestChunkReceiverOutOfOrderAndDuplicate：乱序块被拒绝并指引重发位置；
// 重复块幂等确认；收齐后一次安装，半份数据从不对外可见。
func TestChunkReceiverOutOfOrderAndDuplicate(t *testing.T) {
	c := newTestCluster(t, 3, false, 1201)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	var n *Node
	for _, node := range c.Nodes() {
		if node.Status().Role != RoleLeader {
			n = node
			break
		}
	}
	term := n.Status().Term
	data := makeSnapData(t, map[string]string{"a": "1", "b": "2"})
	crc := crc32.ChecksumIEEE(data)
	total := uint64(len(data))
	session := "s-100"
	const idx = Index(100)

	// 乱序：第一块偏移不为 0 -> 拒绝并告知 NextOffset=0。
	mid := total / 2
	resp := callChunk(n, chunk(term, session, idx, mid, total, crc, data[mid:], false))
	if resp.Success || resp.Reason != RejectSnapshotGap || resp.NextOffset != 0 {
		t.Fatalf("out-of-order first chunk: %+v", resp)
	}
	// 状态机不得出现任何快照内容（半份数据不可用）。
	if _, ok := n.sm.Lookup("a"); ok {
		t.Fatalf("partial data must not be visible")
	}

	// 从头开始：第一半成功。
	resp = callChunk(n, chunk(term, session, idx, 0, total, crc, data[:mid], false))
	if !resp.Success || resp.NextOffset != mid {
		t.Fatalf("first half: %+v", resp)
	}
	// 重复第一半：幂等确认，进度不变。
	resp = callChunk(n, chunk(term, session, idx, 0, total, crc, data[:mid], false))
	if !resp.Success || resp.NextOffset != mid {
		t.Fatalf("duplicate first half: %+v", resp)
	}
	if st := n.CatchupStatus(); st.State != CatchupReceiving || st.Received != mid {
		t.Fatalf("receiving progress: %+v", st)
	}
	// 仍然乱序（跳过中间一段）：拒绝并保持进度。
	resp = callChunk(n, chunk(term, session, idx, mid+4, total, crc, data[mid+4:], false))
	if resp.Success || resp.Reason != RejectSnapshotGap || resp.NextOffset != mid {
		t.Fatalf("gap chunk: %+v", resp)
	}
	// 补上剩余部分（Done）：收齐校验通过后安装。
	resp = callChunk(n, chunk(term, session, idx, mid, total, crc, data[mid:], true))
	if !resp.Success || resp.NextOffset != total {
		t.Fatalf("final chunk: %+v", resp)
	}
	if v, ok := n.sm.Lookup("a"); !ok || v != "1" {
		t.Fatalf("snapshot not installed: a=%q ok=%v", v, ok)
	}
	st := n.CatchupStatus()
	if st.State != CatchupDone || st.Received != total || st.Target != idx {
		t.Fatalf("done status: %+v", st)
	}
}

// TestChunkReceiverInterleavedSessions：不同进度的会话交错到达：
// 更新的会话取代旧会话，旧会话迟到的块被明确拒绝，不得覆盖新进度。
func TestChunkReceiverInterleavedSessions(t *testing.T) {
	c := newTestCluster(t, 3, false, 1202)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	var n *Node
	for _, node := range c.Nodes() {
		if node.Status().Role != RoleLeader {
			n = node
			break
		}
	}
	term := n.Status().Term

	dataA := makeSnapData(t, map[string]string{"only-a": "1"})
	dataB := makeSnapData(t, map[string]string{"only-b": "2"})
	crcA := crc32.ChecksumIEEE(dataA)
	crcB := crc32.ChecksumIEEE(dataB)

	// 会话 A（快照点 100）传一半。
	midA := uint64(len(dataA)) / 2
	resp := callChunk(n, chunk(term, "A", 100, 0, uint64(len(dataA)), crcA, dataA[:midA], false))
	if !resp.Success {
		t.Fatalf("session A first half: %+v", resp)
	}

	// 会话 B（同任期、快照点 200，更新）到达：取代 A。
	resp = callChunk(n, chunk(term, "B", 200, 0, uint64(len(dataB)), crcB, dataB, true))
	if !resp.Success {
		t.Fatalf("session B should supersede A: %+v", resp)
	}
	if st := n.CatchupStatus(); st.State != CatchupDone || st.SessionID != "B" || st.Target != 200 {
		t.Fatalf("B installed: %+v", st)
	}
	if _, ok := n.sm.Lookup("only-b"); !ok {
		t.Fatalf("B content missing")
	}
	if _, ok := n.sm.Lookup("only-a"); ok {
		t.Fatalf("A content must not be installed")
	}

	// 旧会话 A 迟到的块：目标已被本地快照覆盖 -> 幂等确认全部，不破坏状态。
	resp = callChunk(n, chunk(term, "A", 100, midA, uint64(len(dataA)), crcA, dataA[midA:], true))
	if !resp.Success || resp.NextOffset != uint64(len(dataA)) {
		t.Fatalf("stale session chunk: %+v", resp)
	}
	if _, ok := n.sm.Lookup("only-b"); !ok {
		t.Fatalf("B content lost after stale chunk")
	}
}

// TestChunkReceiverSupersededMidSession：接收中途被更新会话取代，
// 状态转为 Cancelled 后可观察，随后新会话独立完成。
func TestChunkReceiverSupersededMidSession(t *testing.T) {
	c := newTestCluster(t, 3, false, 1203)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	var n *Node
	for _, node := range c.Nodes() {
		if node.Status().Role != RoleLeader {
			n = node
			break
		}
	}
	term := n.Status().Term
	dataA := makeSnapData(t, map[string]string{"a": "1"})
	dataB := makeSnapData(t, map[string]string{"b": "2"})

	// A 传一半后，B（更新快照点）到来 -> A 被取消。
	midA := uint64(len(dataA)) / 2
	callChunk(n, chunk(term, "A", 100, 0, uint64(len(dataA)), crc32.ChecksumIEEE(dataA), dataA[:midA], false))
	callChunk(n, chunk(term, "B", 200, 0, uint64(len(dataB)), crc32.ChecksumIEEE(dataB), dataB[:1], false))
	// A 的后续块（同任期、更旧快照点）-> 明确拒绝，不得覆盖 B 的进度。
	resp := callChunk(n, chunk(term, "A", 100, midA, uint64(len(dataA)), crc32.ChecksumIEEE(dataA), dataA[midA:], true))
	if resp.Success || resp.Reason != RejectSnapshotStale {
		t.Fatalf("stale session must be rejected: %+v", resp)
	}
	// B 继续并完成。
	resp = callChunk(n, chunk(term, "B", 200, 1, uint64(len(dataB)), crc32.ChecksumIEEE(dataB), dataB[1:], true))
	if !resp.Success {
		t.Fatalf("B completion: %+v", resp)
	}
	if _, ok := n.sm.Lookup("b"); !ok {
		t.Fatalf("B not installed")
	}
	if _, ok := n.sm.Lookup("a"); ok {
		t.Fatalf("A must not be installed")
	}
}

// TestChunkReceiverCRCFailure：校验和不匹配的完整传输被判失败：
// 状态 Failed、部分数据被丢弃、状态机不变，之后新会话可正常进行。
func TestChunkReceiverCRCFailure(t *testing.T) {
	c := newTestCluster(t, 3, false, 1204)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	var n *Node
	for _, node := range c.Nodes() {
		if node.Status().Role != RoleLeader {
			n = node
			break
		}
	}
	term := n.Status().Term
	data := makeSnapData(t, map[string]string{"x": "1"})
	badCRC := crc32.ChecksumIEEE(data) ^ 0xFFFF

	resp := callChunk(n, chunk(term, "bad", 100, 0, uint64(len(data)), badCRC, data, true))
	if resp.Success {
		t.Fatalf("corrupted snapshot must not be installed")
	}
	st := n.CatchupStatus()
	if st.State != CatchupFailed || st.LastError == "" {
		t.Fatalf("expected Failed with reason: %+v", st)
	}
	if _, ok := n.sm.Lookup("x"); ok {
		t.Fatalf("corrupted data must not be installed")
	}
	// 新会话（正确 CRC）正常完成。
	resp = callChunk(n, chunk(term, "good", 100, 0, uint64(len(data)), crc32.ChecksumIEEE(data), data, true))
	if !resp.Success {
		t.Fatalf("valid session after failure: %+v", resp)
	}
	if v, ok := n.sm.Lookup("x"); !ok || v != "1" {
		t.Fatalf("valid snapshot not installed after failure")
	}
}
