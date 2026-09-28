package raft

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestCatchUpUnderContinuousLoad：需求主验收场景。
//
//	落后节点用很大的状态机追赶；追赶期间领导者持续写入、持续压缩；
//	链路对快照分块持续丢包（并随机重复/延迟）；要求：
//	  1. 已经确认收下的字节不能反复掉回 0（同一份快照的传输只许单调前进）；
//	  2. 负载持续期间也能不断取得进展，最终完全追平领导者提交点；
//	  3. 已提交数据不丢不回退；不存在“进度追平但状态机内容不全”。
func TestCatchUpUnderContinuousLoad(t *testing.T) {
	c := newTestCluster(t, 5, true, 20240928)
	for _, id := range c.IDs() {
		c.SetSnapshotThreshold(id, 4) // 极频繁压缩：快照身份持续推进
		c.SetSnapshotChunkSize(id, 48)
	}
	restartAllWithConfig(t, c)
	waitForDur(t, 5*time.Second, func() bool { return c.Leader() != "" }, "leader")
	leader := c.Leader()

	// 5 节点中隔离 2 个跟随者：领导者 + 其余 2 个跟随者构成存活多数派继续提交，
	// 其中一个被隔离节点稍后带着大状态机缺口回归。
	var laggard, isolated2 NodeID
	picked := 0
	for _, id := range c.IDs() {
		if id == leader {
			continue
		}
		switch picked {
		case 0:
			laggard = id
		case 1:
			isolated2 = id
		}
		picked++
	}
	if err := c.Crash(laggard); err != nil {
		t.Fatal(err)
	}
	if err := c.Crash(isolated2); err != nil {
		t.Fatal(err)
	}

	cli := NewClient("stress", nodesOf(c)...)
	// 写客户端不设会过期的整体超时（-race 整包高负载下单次 Set 可能等待很久）；
	// 测试自身通过 defer cancel 与各阶段 waitForDur 兜底。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 大状态机初始负载。
	const baseWrites = 120
	writeBulk(t, cli, ctx, baseWrites)
	waitForDur(t, 5*time.Second, func() bool {
		for _, id := range c.IDs() {
			if id == laggard || id == isolated2 {
				continue
			}
			if !c.SnapshotExists(id) {
				return false
			}
		}
		return true
	}, "live majority compacted")

	// 快照链路：约 1/4 分块丢弃，持续整个追赶窗口（仅针对落后节点，不拖慢存活多数派）。
	var totalChunks int32
	c.SetSnapshotHook(&SnapshotFaultHook{
		Drop: func(from, to NodeID, req *InstallSnapshotRequest) bool {
			if NodeID(to) != laggard {
				return false
			}
			return atomic.AddInt32(&totalChunks, 1)%4 == 0
		},
	})

	// 弱网只注入到落后节点方向：存活多数派之间保持稳定（避免心跳丢包触发频繁
	// 选举，干扰“同一会话是否被压缩打断”的判定；领导权变化由独立用例覆盖）。
	// 链路丢包/超时全部落在落后节点的快照与 AE 通道上。
	liveSet := map[NodeID]bool{}
	for _, id := range c.IDs() {
		if id != laggard && id != isolated2 {
			liveSet[id] = true
		}
	}
	for _, from := range c.IDs() {
		for _, to := range c.IDs() {
			if from == to || to != laggard {
				continue
			}
			if liveSet[from] {
				c.Flaky(from, to, RPCAppendEntries, FaultPolicy{DropRate: 0.2, Reorder: true})
			}
		}
	}

	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	ln := c.Node(laggard)

	// 追赶期间持续写入（负载在整个追赶窗口内不停）。
	const liveWrites = 240
	writerDone := make(chan error, 1)
	go func() {
		for i := 0; i < liveWrites; i++ {
			if _, _, err := cli.Set(ctx, fmt.Sprintf("live-%05d", i),
				fmt.Sprintf("v-%012d-padding", i)); err != nil {
				writerDone <- err
				return
			}
		}
		writerDone <- nil
	}()

	// 进度观察者（2ms 轮询），两个与采样时机无关的不变式：
	//
	//  1. 同一快照身份 (SnapshotIdx,Total) 在 syncing 期间 Received 单调不减；
	//  2. 钉住语义：一份未传完（maxReceived<Total）的快照身份不得被另一份新身份
	//     顶替（旧实现每次领导者压缩都丢会话从 0 重传，会留下大量“半截身份”）。
	//
	// 已正常 caught_up 的身份（Received 曾到 Total）之后再开新会话是合法的。
	var regressions, abandoned int32
	observerStop := make(chan struct{})
	go func() {
		type snapKey struct {
			idx   Index
			total uint64
		}
		type prog struct {
			maxReceived uint64
			total       uint64
			completed   bool
		}
		rec := map[snapKey]*prog{}
		var lastSyncKey snapKey
		haveSync := false
		for {
			select {
			case <-observerStop:
				return
			case <-time.After(2 * time.Millisecond):
			}
			// 一次 Status 调用在事件循环内原子取得 (AppliedIndex, CatchUp)，
			// 避免分两次读取时“安装已完成但进度尚未刷新”的窗口造成误判。
			st := ln.Status()
			p := st.CatchUp
			applied := st.AppliedIndex
			k := snapKey{p.SnapshotIdx, p.Total}
			cur := rec[k]
			if cur == nil {
				cur = &prog{total: p.Total}
				rec[k] = cur
			}
			if p.Total > 0 {
				cur.total = p.Total
			}
			if p.Received > cur.maxReceived {
				cur.maxReceived = p.Received
			}
			if p.State == CatchUpCaughtUp || (k.idx > 0 && applied >= k.idx) {
				cur.completed = true
			}
			for kk, vv := range rec {
				if !vv.completed && kk.idx > 0 && applied >= kk.idx {
					vv.completed = true
				}
			}
			if p.State == CatchUpSyncing {
				if p.Received < cur.maxReceived {
					atomic.AddInt32(&regressions, 1)
				}
				if haveSync && k != lastSyncKey {
					// 新身份开始：上一份 syncing 的快照必须已传完或已 caught_up，
					// 否则就是“没传完就被新快照顶掉、字节从 0 重来”。
					if prev := rec[lastSyncKey]; prev != nil &&
						!prev.completed && prev.maxReceived < prev.total {
						atomic.AddInt32(&abandoned, 1)
					}
				}
				lastSyncKey = k
				haveSync = true
			}
		}
	}()

	if err := <-writerDone; err != nil {
		t.Fatalf("concurrent writer failed: %v", err)
	}

	// 负载停止后：落后节点必须完全追平领导者提交点并全部应用。
	var leaderCommit Index
	waitForDur(t, 40*time.Second, func() bool {
		leaderID := c.Leader()
		if leaderID == "" {
			return false
		}
		leaderCommit = c.Node(leaderID).Status().CommitIndex
		s := ln.Status()
		return s.CommitIndex >= leaderCommit && s.AppliedIndex >= leaderCommit
	}, "laggard fully catches leader commit under continuous compaction+drops")

	close(observerStop)

	if r := atomic.LoadInt32(&regressions); r != 0 {
		t.Fatalf("received bytes regressed %d times within the same snapshot identity", r)
	}
	if n := atomic.LoadInt32(&abandoned); n != 0 {
		t.Fatalf("leader abandoned %d incomplete snapshots for newer ones (bytes restarted from 0)", n)
	}

	// 数据完整性：基础负载 + 追赶期间全部写入都在状态机里。
	for i := 0; i < baseWrites; i++ {
		k, v := bulkKV(i)
		if got, ok := ln.sm.Lookup(k); !ok || got != v {
			t.Fatalf("committed base value lost/regressed: %s=%q ok=%v", k, got, ok)
		}
	}
	for i := 0; i < liveWrites; i++ {
		want := fmt.Sprintf("v-%012d-padding", i)
		if got, ok := ln.sm.Lookup(fmt.Sprintf("live-%05d", i)); !ok || got != want {
			t.Fatalf("live value missing: live-%05d=%q ok=%v", i, got, ok)
		}
	}

	// 主追赶完成：撤掉快照与 AE 故障，后续阶段只验证“正常复制”不再被弱网拖慢。
	c.SetSnapshotHook(nil)
	c.Tr.HealAll()

	// 追上之后再写一批，确认 laggard 正常参与常规复制。
	for i := 0; i < 20; i++ {
		if _, _, err := cli.Set(ctx, fmt.Sprintf("tail-%03d", i), fmt.Sprintf("t%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	waitForDur(t, 15*time.Second, func() bool {
		lc := c.Node(c.Leader()).Status().CommitIndex
		s := ln.Status()
		return s.CommitIndex >= lc && s.AppliedIndex >= lc
	}, "laggard follows new entries by log replication")
	for i := 0; i < 20; i++ {
		if got, ok := ln.sm.Lookup(fmt.Sprintf("tail-%03d", i)); !ok || got != fmt.Sprintf("t%d", i) {
			t.Fatalf("tail value missing: tail-%03d=%q ok=%v", i, got, ok)
		}
	}

	// 第二个被隔离节点恢复后也能追平（覆盖多个追赶会话并发存在）。
	if err := c.Restart(isolated2); err != nil {
		t.Fatal(err)
	}
	n2 := c.Node(isolated2)
	waitForDur(t, 20*time.Second, func() bool {
		lc := c.Node(c.Leader()).Status().CommitIndex
		s := n2.Status()
		return s.CommitIndex >= lc && s.AppliedIndex >= lc
	}, "second isolated node catches up after rejoin")

	snap, last := ln.LogIndexRange()
	if last < snap {
		t.Fatalf("log hole after catch-up: snap=%d last=%d", snap, last)
	}
	t.Logf("continuous-load catch-up ok: chunks=%d, leaderCommit=%d, snap=%d last=%d",
		atomic.LoadInt32(&totalChunks), leaderCommit, snap, last)
}
