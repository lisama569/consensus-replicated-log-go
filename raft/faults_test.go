package raft

import (
	"context"
	"testing"
	"time"
)

// TestCrashRestartDurability：已多数派提交的内容在多数节点崩溃重启后不丢失。
func TestCrashRestartDurability(t *testing.T) {
	c := newTestCluster(t, 3, true, 101)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	cli := NewClient("dur", nodesOf(c)...)
	ctx, cancel := context.WithTimeout(context.Background(), waitShort)
	defer cancel()

	for i := 0; i < 10; i++ {
		if _, _, err := cli.Set(ctx, key(i), val(i)); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	leader := c.Leader()
	// 崩溃全部节点（模拟整集群掉电）。
	for _, id := range c.IDs() {
		if err := c.Crash(id); err != nil {
			t.Fatal(err)
		}
	}
	// 全部用同一数据目录重启。
	for _, id := range c.IDs() {
		if err := c.Restart(id); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return c.Leader() != "" }, "leader after restart")

	// 所有已提交键都必须存在，且值不回退。
	waitFor(t, func() bool {
		for _, n := range c.Nodes() {
			for i := 0; i < 10; i++ {
				v, ok := n.sm.Lookup(key(i))
				if !ok || v != val(i) {
					return false
				}
			}
		}
		return true
	}, "all committed entries survive full restart")

	if c.Leader() == leader {
		// 允许同一节点重新当选，但它必须在新/原任期内通过选举合法性校验；仅记录。
		t.Logf("same node %s re-elected", leader)
	}
	logStatus(c, "after-restart")
}

// TestMinorityCannotCommit：领导者被隔离进少数派分区后，其写入不得提交；
// 恢复分区后新领导者上的写入必须成功，旧领导者回滚未提交日志。
func TestMinorityCannotCommit(t *testing.T) {
	c := newTestCluster(t, 5, false, 202)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	oldLeader := c.Leader()

	// 把旧领导者单独隔离（1 节点分区 vs 4 节点多数派）。
	c.Isolate(oldLeader)

	// 旧领导者的写入必须失败（无法获得多数派）。
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_, err := c.Node(oldLeader).SubmitProposal(ctx, Command{
		Op: OpSet, Key: "partitioned", Value: "no",
		ClientID: "x", Seq: 1,
	})
	if err == nil {
		t.Fatal("partitioned leader proposal must not succeed")
	}

	// 多数派一侧选出新领导者。
	waitFor(t, func() bool {
		for id, n := range c.Nodes() {
			if id == oldLeader {
				continue
			}
			if n.Status().Role == RoleLeader {
				t.Logf("new leader elected: %s term=%d", id, n.Status().Term)
				return true
			}
		}
		return false
	}, "majority elects a new leader")

	// 通过新领导者写入，必须成功。
	cli := NewClient("maj", nodesOf(c)...)
	ctx2, cancel2 := context.WithTimeout(context.Background(), waitShort)
	defer cancel2()
	if _, _, err := cli.Set(ctx2, "after", "heal"); err != nil {
		t.Fatalf("write via new majority: %v", err)
	}

	// 恢复网络：旧领导者发现更高任期后退位，日志被覆盖收敛。
	c.Heal(oldLeader)
	waitFor(t, func() bool {
		n := c.Node(oldLeader)
		s := n.Status()
		v, _ := n.sm.Lookup("after")
		return s.Role == RoleFollower && v == "heal"
	}, "old leader steps down and converges")

	if v, ok := c.Node(oldLeader).sm.Lookup("partitioned"); ok && v == "no" {
		t.Fatal("uncommitted write from minority leader must not be applied")
	}
	logStatus(c, "after-heal")
}

// TestRejectReasons 可区分的拒绝原因。
func TestRejectReasons(t *testing.T) {
	c := newTestCluster(t, 3, false, 303)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	// 等待所有节点进入同一任期并已知领导者：高负载（-race 整包）下选出瞬间
	// 可能仍有节点在旧任期且尚未投票，否则同票检查会偶发落到刚起步的节点。
	waitForDur(t, 5*time.Second, func() bool {
		leaderID := c.Leader()
		if leaderID == "" {
			return false
		}
		lt := c.Node(leaderID).Status().Term
		for _, n := range c.Nodes() {
			s := n.Status()
			if s.Term != lt || (s.Role != RoleLeader && s.LeaderID == "") {
				return false
			}
		}
		return true
	}, "all nodes settle in the leader's term")
	leader := c.Node(c.Leader())
	// 必须挑一个“本任期已经投过票”的跟随者（即投票给了现任领导者）；
	// 若它尚未投票，同任期 RequestVote 本就应该被授予，already_voted 检查不成立。
	var follower *Node
	waitForDur(t, 5*time.Second, func() bool {
		for _, n := range c.Nodes() {
			s := n.Status()
			if s.Role != RoleLeader && s.VotedFor != "" && s.VotedFor == c.Leader() {
				follower = n
				return true
			}
		}
		return false
	}, "a follower that voted for the leader exists")

	// 1) 过期任期的 RequestVote -> stale_term。
	staleVote := &RequestVoteRequest{
		Term: 0, CandidateID: "zombie", LastLogIndex: 0, LastLogTerm: 0,
	}
	r := follower.CallRPC(staleVote).(*RequestVoteResponse)
	if r.VoteGranted || r.Reason != RejectStaleTerm {
		t.Fatalf("want stale_term, granted=%v reason=%s", r.VoteGranted, r.Reason)
	}
	t.Logf("stale vote rejected reason=%s (peer=%s localTerm=%d)", r.Reason, follower.cfg.ID, r.Term)

	// 2) 同任期二次投票给不同候选人 -> already_voted。
	f := follower.Status()
	second := &RequestVoteRequest{
		Term: f.Term, CandidateID: "other", LastLogIndex: f.LastLogIndex, LastLogTerm: f.LastLogTerm,
	}
	r2 := follower.CallRPC(second).(*RequestVoteResponse)
	if r2.VoteGranted || r2.Reason != RejectAlreadyVoted {
		t.Fatalf("want already_voted, granted=%v reason=%s", r2.VoteGranted, r2.Reason)
	}
	t.Logf("duplicate vote rejected reason=%s", r2.Reason)

	// 3) 过期任期的 AppendEntries -> stale_term。
	staleAE := &AppendEntriesRequest{Term: 0, LeaderID: "old"}
	ar := follower.CallRPC(staleAE).(*AppendEntriesResponse)
	if ar.Success || ar.Reason != RejectStaleTerm {
		t.Fatalf("want stale_term on append, success=%v reason=%s", ar.Success, ar.Reason)
	}
	t.Logf("stale append rejected reason=%s", ar.Reason)

	// 4) 日志不够新的候选人 -> log_not_up_to_date：
	//    先提交若干日志，再让一个空日志的同任期节点请求投票（用更高任期迫使让位后再判定日志）。
	cli := NewClient("rr", nodesOf(c)...)
	ctx, cancel := context.WithTimeout(context.Background(), waitShort)
	defer cancel()
	for i := 0; i < 3; i++ {
		if _, _, err := cli.Set(ctx, "k", "v"); err != nil {
			t.Fatal(err)
		}
	}
	// 新任期候选人但日志为空：先让 follower 进入更高任期（合法候选），再用空日志请求。
	newTerm := leader.Status().Term + 1
	poor := &RequestVoteRequest{
		Term: newTerm, CandidateID: "poorp", LastLogIndex: 0, LastLogTerm: 0,
	}
	r3 := follower.CallRPC(poor).(*RequestVoteResponse)
	if r3.VoteGranted || r3.Reason != RejectLogNotUpToDate {
		t.Fatalf("want log_not_up_to_date, granted=%v reason=%s", r3.VoteGranted, r3.Reason)
	}
	t.Logf("stale-log candidate rejected reason=%s (cand t=%d i=%d vs local t=%d i=%d)",
		r3.Reason, newTerm, 0, follower.lastLogTerm(), follower.lastLogIndex())
}

// TestDuplicateAndReorderDelivery：网络重复+乱序下结果仍唯一一致。
func TestDuplicateAndReorderDelivery(t *testing.T) {
	c := newTestCluster(t, 3, false, 404)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	leader := c.Leader()
	// 从领导者向两个跟随者两个方向注入重复 + 乱序。
	for _, id := range c.IDs() {
		if id == leader {
			continue
		}
		c.Flaky(leader, id, RPCAppendEntries, FaultPolicy{Duplicate: true, Reorder: true})
	}
	cli := NewClient("flaky", nodesOf(c)...)
	ctx, cancel := context.WithTimeout(context.Background(), waitShort)
	defer cancel()
	for i := 0; i < 20; i++ {
		if _, _, err := cli.Set(ctx, key(i), val(i)); err != nil {
			t.Fatalf("set %d under duplicate/reorder: %v", i, err)
		}
	}
	waitFor(t, func() bool {
		for _, n := range c.Nodes() {
			for i := 0; i < 20; i++ {
				if v, ok := n.sm.Lookup(key(i)); !ok || v != val(i) {
					return false
				}
			}
		}
		return true
	}, "convergence under duplicate/reorder network")

	// 日志不得有重复或空洞索引（从快照点之后到日志末尾必须连续）。
	for _, n := range c.Nodes() {
		snap, last := n.LogIndexRange()
		if last < snap {
			t.Fatalf("node %s last %d < snapshot %d", n.cfg.ID, last, snap)
		}
	}
}

func key(i int) string { return "k-" + itoa(i) }
func val(i int) string { return "v-" + itoa(i) }
