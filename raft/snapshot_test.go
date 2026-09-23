package raft

import (
	"fmt"
	"sync"
	"testing"
)

// 完成压缩的节点重启后，状态机与提交位置必须保持一致。
func TestSnapshotRestartConsistency(t *testing.T) {
	c := newCluster(t, 0, 1, 2)
	l := c.waitLeader(30)
	leaderID := leaderless(c, l)
	var last Index
	for i := 1; i <= 8; i++ {
		res := c.propose(l, Command{ClientID: "s", RequestID: uint64(i), Key: fmt.Sprintf("k%d", i), Value: fmt.Sprintf("v%d", i)}, 60)
		last = res.Index
	}
	// 领导者压缩到 last。
	if err := c.nodes[leaderID].Snapshot(last); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	t.Logf("[tick=%d] snapshotted to %d: %s", c.ticks, last, c.nodes[leaderID].Describe())
	// 崩溃重启后：提交位置与状态机必须保持。
	c.crash(leaderID)
	c.restart(leaderID)
	n := c.nodes[leaderID]
	_, _, commit := n.State()
	if commit != last {
		t.Fatalf("commit=%d after snapshot+restart, want %d", commit, last)
	}
	for i := 1; i <= 8; i++ {
		if v, ok := n.Read(fmt.Sprintf("k%d", i)); !ok || v != fmt.Sprintf("v%d", i) {
			t.Fatalf("lost k%d after snapshot+restart", i)
		}
	}
	// 幂等表也必须存活：重启后重试旧请求仍返回原结果。
	res, reason := n.Propose(Command{ClientID: "s", RequestID: 1, Key: "k1", Value: "v1"})
	_ = res
	t.Logf("[tick=%d] retry-after-restart reason=%s (node not leader yet, expected); %s", c.ticks, reason, n.Describe())
	c.logStates("SNAP-RESTART")
}

// 严重落后的节点通过快照追上，而不是回放全部历史。
func TestSnapshotCatchUp(t *testing.T) {
	c := newCluster(t, 0, 1, 2)
	l := c.waitLeader(30)
	leaderID := leaderless(c, l)
	lagID := (leaderID + 1) % 3
	third := 3 - leaderID - lagID
	// 隔离 lagID，写入 10 条并在领导者上压缩。
	c.net.Partition([]int{lagID}, []int{leaderID, third})
	var last Index
	for i := 1; i <= 10; i++ {
		res := c.propose(l, Command{ClientID: "s", RequestID: uint64(i), Key: "k", Value: fmt.Sprintf("v%d", i)}, 60)
		last = res.Index
	}
	if err := c.nodes[leaderID].Snapshot(last); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	t.Logf("[tick=%d] leader snapshotted to %d; lagging node=%d commit far behind", c.ticks, last, lagID)
	// 恢复：lagID 必须通过 InstallSnapshot 一次追上。
	c.net.Heal()
	c.tick(50)
	n := c.nodes[lagID]
	_, _, commit := n.State()
	t.Logf("[tick=%d] after heal: %s", c.ticks, n.Describe())
	if commit < last {
		t.Fatalf("lagging node commit=%d, want >= %d (snapshot catch-up)", commit, last)
	}
	if v, ok := n.Read("k"); !ok || v != "v10" {
		t.Fatalf("lagging node k=%q,%v; want v10", v, ok)
	}
	// 追平后还能接收新写入。
	nl := c.leader()
	res := c.propose(nl, Command{ClientID: "s", RequestID: 11, Key: "k", Value: "v11"}, 60)
	c.tick(10)
	c.assertCommitted(lagID, res.Index, "k", "v11")
	c.logStates("SNAP-CATCHUP")
}

// 并发读写与压缩同时发生：不得有数据竞争或状态错乱（需 -race 运行）。
func TestConcurrentReadWriteCompact(t *testing.T) {
	c := newCluster(t, 0, 1, 2)
	l := c.waitLeader(30)
	leaderID := leaderless(c, l)
	ln := c.nodes[leaderID]

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// 并发读。
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					ln.Read("k")
					ln.State()
					ln.Describe()
				}
			}
		}()
	}
	// 串行写入与压缩（写路径本身由协议保证单点进入）。
	var committed Index
	for i := 1; i <= 30; i++ {
		res := c.propose(l, Command{ClientID: "c", RequestID: uint64(i), Key: "k", Value: fmt.Sprintf("v%d", i)}, 90)
		committed = res.Index
		if i%5 == 0 {
			if err := ln.Snapshot(committed); err != nil {
				t.Fatalf("snapshot at %d: %v", committed, err)
			}
		}
	}
	close(stop)
	wg.Wait()
	// 最终状态必须正确：最后一条写入可见，提交位置不回退。
	if v, ok := ln.Read("k"); !ok || v != "v30" {
		t.Fatalf("final k=%q,%v; want v30", v, ok)
	}
	_, _, commit := ln.State()
	if commit != committed {
		t.Fatalf("commit=%d, want %d", commit, committed)
	}
	t.Logf("[tick=%d] concurrent test done: %s", c.ticks, ln.Describe())
	// 压缩后的领导者重启，状态依旧一致。
	c.crash(leaderID)
	c.restart(leaderID)
	if v, ok := c.nodes[leaderID].Read("k"); !ok || v != "v30" {
		t.Fatalf("after restart k=%q,%v; want v30", v, ok)
	}
	c.logStates("CONCURRENT")
}
