package raft

import "testing"

// 客户端请求在多数派确认后提交，所有存活节点状态机一致。
func TestReplicationMajorityCommit(t *testing.T) {
	c := newCluster(t, 0, 1, 2)
	l := c.waitLeader(30)
	res := c.propose(l, Command{ClientID: "a", RequestID: 1, Key: "k1", Value: "v1"}, 60)
	t.Logf("[tick=%d] committed index=%d term=%d", c.ticks, res.Index, res.Term)
	c.tick(5)
	for _, id := range c.ids {
		c.assertCommitted(id, res.Index, "k1", "v1")
	}
	c.logStates("REPLICATED")
}

// 重复请求不得造成重复写入：相同 ClientID+RequestID 返回首个结果。
func TestIdempotentRetry(t *testing.T) {
	c := newCluster(t, 0, 1, 2)
	l := c.waitLeader(30)
	cmd := Command{ClientID: "a", RequestID: 1, Key: "k", Value: "v"}
	first := c.propose(l, cmd, 60)
	// 客户端重试同一请求（可能重试多次）。
	for i := 0; i < 3; i++ {
		res, reason := l.Propose(cmd)
		if reason != ReasonDuplicate {
			t.Fatalf("retry %d: reason=%s, want %s", i, reason, ReasonDuplicate)
		}
		if res.Index != first.Index {
			t.Fatalf("retry %d: index=%d, want first index %d (no duplicate write)", i, res.Index, first.Index)
		}
		t.Logf("[tick=%d] retry %d deduped: index=%d reason=%s", c.ticks, i, res.Index, reason)
	}
	// 不同 RequestID 是新的写入。
	c.propose(l, Command{ClientID: "a", RequestID: 2, Key: "k", Value: "v2"}, 60)
	if v, _ := l.Read("k"); v != "v2" {
		t.Fatalf("k=%q, want v2", v)
	}
	c.logStates("IDEMPOTENT")
}

// 落后节点（被分区后恢复）必须补齐缺失日志。
func TestLaggingNodeCatchUp(t *testing.T) {
	c := newCluster(t, 0, 1, 2)
	l := c.waitLeader(30)
	leaderID := leaderless(c, l)
	lagID := (leaderID + 1) % 3
	// 隔离 lagID，写入 5 条。
	c.net.Partition([]int{lagID}, []int{leaderID, 3 - leaderID - lagID})
	var last Index
	for i := 1; i <= 5; i++ {
		res := c.propose(l, Command{ClientID: "a", RequestID: uint64(i), Key: "k", Value: string(rune('0' + i))}, 60)
		last = res.Index
	}
	_, _, lagCommit := c.nodes[lagID].State()
	t.Logf("[tick=%d] lagging node=%d commit=%d, leader commit=%d", c.ticks, lagID, lagCommit, last)
	if lagCommit >= last {
		t.Fatalf("partitioned node unexpectedly up to date")
	}
	// 恢复分区：落后节点必须追上。
	c.net.Heal()
	c.tick(40)
	c.assertCommitted(lagID, last, "k", "5")
	c.logStates("CAUGHT-UP")
}

// 崩溃重启后已提交内容不得丢失或回退。
func TestCrashRestartDurability(t *testing.T) {
	c := newCluster(t, 0, 1, 2)
	l := c.waitLeader(30)
	r1 := c.propose(l, Command{ClientID: "a", RequestID: 1, Key: "x", Value: "1"}, 60)
	r2 := c.propose(l, Command{ClientID: "a", RequestID: 2, Key: "y", Value: "2"}, 60)
	c.tick(5)

	// 崩溃全部节点，再全部重启。
	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.restart(id)
	}
	// 重启后已提交内容必须立即可读（从持久存储重放）。
	for _, id := range c.ids {
		n := c.nodes[id]
		_, _, commit := n.State()
		t.Logf("[tick=%d] restarted %s", c.ticks, n.Describe())
		if commit < r2.Index {
			t.Fatalf("node %d commit=%d regressed below %d", id, commit, r2.Index)
		}
		if v, ok := n.Read("x"); !ok || v != "1" {
			t.Fatalf("node %d lost committed write x (commit was %d)", id, r1.Index)
		}
		if v, ok := n.Read("y"); !ok || v != "2" {
			t.Fatalf("node %d lost committed write y", id)
		}
	}
	// 重启后集群仍能选主并继续写入。
	nl := c.waitLeader(60)
	c.propose(nl, Command{ClientID: "a", RequestID: 3, Key: "z", Value: "3"}, 60)
	c.logStates("AFTER-RESTART")
}

// 消息重复与乱序不得破坏一致性。
func TestDuplicateAndReorderedMessages(t *testing.T) {
	c := newCluster(t, 0, 1, 2)
	l := c.waitLeader(30)
	leaderID := leaderless(c, l)
	// 所有链路开启重复投递；给部分链路加不同延迟制造乱序。
	for _, a := range c.ids {
		for _, b := range c.ids {
			if a != b {
				c.net.SetDuplicate(a, b, true)
			}
		}
	}
	c.net.SetDelay(leaderID, (leaderID+1)%3, 2)
	c.net.SetDelay((leaderID+1)%3, leaderID, 1)

	var last Index
	for i := 1; i <= 6; i++ {
		res := c.propose(l, Command{ClientID: "b", RequestID: uint64(i), Key: "n", Value: string(rune('0' + i))}, 90)
		last = res.Index
	}
	c.tick(20)
	for _, id := range c.ids {
		c.assertCommitted(id, last, "n", "6")
	}
	// 关闭故障注入，确认集群依旧稳定唯一领导者。
	for _, a := range c.ids {
		for _, b := range c.ids {
			c.net.SetDuplicate(a, b, false)
			c.net.SetDelay(a, b, 0)
		}
	}
	c.tick(30)
	c.leader()
	c.logStates("DUP-REORDER")
}

// 向非领导者写入必须被拒绝并给出 NOT_LEADER。
func TestNotLeaderRejected(t *testing.T) {
	c := newCluster(t, 0, 1, 2)
	l := c.waitLeader(30)
	var follower *Node
	for _, id := range c.ids {
		if c.nodes[id] != l {
			follower = c.nodes[id]
		}
	}
	_, reason := follower.Propose(Command{ClientID: "a", RequestID: 1, Key: "k", Value: "v"})
	if reason != ReasonNotLeader {
		t.Fatalf("reason=%s, want %s", reason, ReasonNotLeader)
	}
	t.Logf("[tick=%d] follower write rejected: reason=%s; %s", c.ticks, reason, follower.Describe())
}
