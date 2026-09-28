package raft

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestSnapshotCompactionAndRestart：达到阈值后压缩；重启后状态机与提交位置一致。
func TestSnapshotCompactionAndRestart(t *testing.T) {
	c, err := NewCluster(3, true, 505)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	// 较小阈值，快速触发压缩。
	for _, id := range c.IDs() {
		c.SetSnapshotThreshold(id, 10)
	}
	// 阈值配置在新节点上生效，需要重启一次让所有节点加载新配置。
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
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	cli := NewClient("snap", nodesOf(c)...)
	ctx, cancel := context.WithTimeout(context.Background(), waitShort)
	defer cancel()
	for i := 0; i < 40; i++ {
		if _, _, err := cli.Set(ctx, key(i), val(i)); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}

	// 所有节点都应已产生快照，且日志后缀被截断。
	waitFor(t, func() bool {
		for _, id := range c.IDs() {
			if !c.SnapshotExists(id) {
				return false
			}
		}
		return true
	}, "snapshot file present on every node")

	// 提交位置刚到 40 时应用可能尚未追平（-race + 高负载下尤其明显），
	// 先等待各节点把已提交日志全部应用到状态机，再校验键值。
	leaderCommit := c.Node(c.Leader()).Status().CommitIndex
	waitForDur(t, 5*time.Second, func() bool {
		for _, n := range c.Nodes() {
			s := n.Status()
			if s.AppliedIndex < s.CommitIndex || s.CommitIndex < leaderCommit {
				return false
			}
		}
		return true
	}, "all nodes apply up to commit after compaction")

	for _, n := range c.Nodes() {
		s := n.Status()
		// 压缩后 lastLogIndex 可以仍包含部分后缀，但 applied 必须覆盖全部 40 个键。
		for i := 0; i < 40; i++ {
			if v, ok := n.sm.Lookup(key(i)); !ok || v != val(i) {
				t.Fatalf("node %s missing %s after compaction: %q ok=%v (commit=%d)",
					n.cfg.ID, key(i), v, ok, s.CommitIndex)
			}
		}
	}

	// 重启后：各节点从持久化位置恢复，已提交内容（多数派确认的 40 个键）
	// 必须全部存在且值不回退；状态机恢复点与提交/应用位置保持一致。
	commits := map[NodeID]Index{}
	for _, n := range c.Nodes() {
		s := n.Status()
		commits[n.cfg.ID] = s.CommitIndex
	}
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
	waitFor(t, func() bool { return c.Leader() != "" }, "leader after snapshot restart")
	// 给集群一点时间追平崩溃瞬间未同步的尾部日志。
	waitFor(t, func() bool {
		for _, n := range c.Nodes() {
			if n.Status().CommitIndex < commits[n.cfg.ID] {
				return false // 已持久化的提交位置不得回退
			}
		}
		return true
	}, "no node regresses below its persisted commit")

	for _, n := range c.Nodes() {
		s := n.Status()
		if s.AppliedIndex < s.CommitIndex {
			t.Fatalf("node %s applied %d < commit %d", n.cfg.ID, s.AppliedIndex, s.CommitIndex)
		}
		for i := 0; i < 40; i++ {
			if v, ok := n.sm.Lookup(key(i)); !ok || v != val(i) {
				t.Fatalf("node %s lost %s after snapshot restart: %q ok=%v (commit=%d snap=%d)",
					n.cfg.ID, key(i), v, ok, s.CommitIndex, s.SnapshotIndex)
			}
		}
		fmt.Printf("    [snapshot-restart] node=%s role=%s term=%d commit=%d applied=%d last=%d snap=%d\n",
			s.ID, s.Role, s.Term, s.CommitIndex, s.AppliedIndex, s.LastLogIndex, s.SnapshotIndex)
	}
}

// TestSnapshotCatchUp：严重落后（日志已被压缩删除）的节点通过快照追上。
func TestSnapshotCatchUp(t *testing.T) {
	c, err := NewCluster(3, true, 606)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	for _, id := range c.IDs() {
		c.SetSnapshotThreshold(id, 5)
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

	// 挑一个跟随者，在大量写入期间保持崩溃，使其错过日志压缩窗口。
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

	cli := NewClient("catch", nodesOf(c)...)
	ctx, cancel := context.WithTimeout(context.Background(), waitShort)
	defer cancel()
	for i := 0; i < 30; i++ {
		if _, _, err := cli.Set(ctx, key(i), val(i)); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	// 等待存活节点完成压缩。
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
	}, "live peers compacted their logs")

	// 落后节点重启：它需要的历史日志已不存在，必须经 InstallSnapshot 追上。
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		n := c.Node(laggard)
		for i := 0; i < 30; i++ {
			if v, ok := n.sm.Lookup(key(i)); !ok || v != val(i) {
				return false
			}
		}
		return n.Status().CommitIndex >= 1
	}, "laggard catches up via snapshot")

	// 继续写入新数据，确认追上后的节点能正常参与复制。
	for i := 30; i < 45; i++ {
		if _, _, err := cli.Set(ctx, key(i), val(i)); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	waitFor(t, func() bool {
		n := c.Node(laggard)
		for i := 0; i < 45; i++ {
			if v, ok := n.sm.Lookup(key(i)); !ok || v != val(i) {
				return false
			}
		}
		return true
	}, "laggard follows new entries after snapshot")
	s := c.Node(laggard).Status()
	t.Logf("laggard %s caught up: role=%s term=%d commit=%d applied=%d snap=%d",
		laggard, s.Role, s.Term, s.CommitIndex, s.AppliedIndex, c.Node(laggard).snapMeta.LastIncludedIndex)
}

// TestConcurrentReadWriteSnapshot：并发读写 + 压缩同时进行，race detector 下无竞争。
func TestConcurrentReadWriteSnapshot(t *testing.T) {
	c, err := NewCluster(3, false, 707)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	for _, id := range c.IDs() {
		c.SetSnapshotThreshold(id, 3)
	}
	for _, n := range c.Nodes() {
		n.SetSnapshotThreshold(3)
	}
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 多个写客户端。
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cli := NewClient(fmt.Sprintf("writer-%d", w), nodesOf(c)...)
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, _, err := cli.Set(ctx, fmt.Sprintf("w%d", w), fmt.Sprintf("%d", i)); err != nil {
					return
				}
				i++
			}
		}(w)
	}

	// 多个读客户端。
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cli := NewClient(fmt.Sprintf("reader-%d", r), nodesOf(c)...)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, _, err := cli.Read(ctx, "w0"); err != nil {
					return
				}
			}
		}()
	}

	// 直接并发调用状态机 Snapshot/Restore 不会发生（Restore 仅启动期），
	// 但 Lookup 与事件循环里的 Apply/Snapshot 并发由 -race 覆盖。
	time.Sleep(2000 * time.Millisecond)
	close(stop)
	cancel()
	wg.Wait()

	// 所有节点的状态机最终一致。-race + 整包高负载下给足收敛时间：
	// 以领导者提交点为准，要求每个节点 commit/apply 都追平，且状态机值一致
	// （仅比较某个键值在选举抖动时可能被“旧值恰好相同”掩盖）。
	waitForDur(t, 15*time.Second, func() bool {
		leaderID := c.Leader()
		if leaderID == "" {
			return false
		}
		leader := c.Node(leaderID)
		leaderCommit := leader.Status().CommitIndex
		v, _ := leader.sm.Lookup("w0")
		for _, n := range c.Nodes() {
			s := n.Status()
			if s.CommitIndex < leaderCommit || s.AppliedIndex < s.CommitIndex {
				return false
			}
			if got, _ := n.sm.Lookup("w0"); got != v {
				return false
			}
		}
		return true
	}, "all nodes converge after concurrent workload")
}
