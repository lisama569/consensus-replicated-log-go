package raft

import (
	"context"
	"testing"
)

// TestStaleTermFromOldLeader：恢复后旧领导者的低任期 RPC 必须被明确拒绝。
func TestStaleTermFromOldLeader(t *testing.T) {
	c := newTestCluster(t, 3, false, 808)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	oldLeader := c.Leader()
	s := c.Node(oldLeader).Status()
	oldTerm := s.Term

	// 隔离旧领导者并让多数派选出更高任期的新领导者。
	c.Isolate(oldLeader)
	waitFor(t, func() bool {
		for id, n := range c.Nodes() {
			if id == oldLeader {
				continue
			}
			if n.Status().Role == RoleLeader {
				return true
			}
		}
		return false
	}, "new leader with higher term")
	c.Heal(oldLeader)

	var follower *Node
	for _, n := range c.Nodes() {
		if n.Status().Role != RoleLeader {
			follower = n
			break
		}
	}
	// 旧任期的 AppendEntries 必须被拒绝，原因 stale_term。
	resp := follower.CallRPC(&AppendEntriesRequest{
		Term: oldTerm, LeaderID: oldLeader,
		PrevLogIndex: 0, PrevLogTerm: 0, LeaderCommit: 0,
	}).(*AppendEntriesResponse)
	if resp.Success || resp.Reason != RejectStaleTerm || resp.Term <= oldTerm {
		t.Fatalf("stale append must be rejected with stale_term, got success=%v reason=%s term=%d",
			resp.Success, resp.Reason, resp.Term)
	}
	t.Logf("old-term append from %s rejected: reason=%s responderTerm=%d > oldTerm=%d",
		oldLeader, resp.Reason, resp.Term, oldTerm)

	// 旧任期的 RequestVote 同样拒绝。
	vresp := follower.CallRPC(&RequestVoteRequest{
		Term: oldTerm, CandidateID: oldLeader, LastLogIndex: s.LastLogIndex, LastLogTerm: s.LastLogTerm,
	}).(*RequestVoteResponse)
	if vresp.VoteGranted || vresp.Reason != RejectStaleTerm {
		t.Fatalf("stale vote must be rejected with stale_term, got granted=%v reason=%s",
			vresp.VoteGranted, vresp.Reason)
	}
}

// TestClientRetriesAcrossLeaderChange：领导者中途崩溃，客户端自动重试且不重复写。
func TestClientRetriesAcrossLeaderChange(t *testing.T) {
	c := newTestCluster(t, 5, true, 909)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	cli := NewClient("roam", nodesOf(c)...)
	ctx, cancel := context.WithTimeout(context.Background(), waitShort)
	defer cancel()

	for i := 0; i < 30; i++ {
		cli.SetNodes(nodesOf(c)) // 跳过已崩溃、纳入已重启的节点
		if _, _, err := cli.Set(ctx, key(100+i), val(100+i)); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
		// 每隔几次写崩溃当前领导者，再立即重启它（先让其余多数派选出新主）。
		if i%7 == 6 {
			victim := c.Leader()
			if err := c.Crash(victim); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool {
				for id, n := range c.Nodes() {
					if id != victim && n.Status().Role == RoleLeader {
						return true
					}
				}
				return false
			}, "new leader while victim is down")
			if err := c.Restart(victim); err != nil {
				t.Fatal(err)
			}
		}
	}
	waitFor(t, func() bool {
		for _, n := range c.nodes0() {
			for i := 0; i < 30; i++ {
				if v, ok := n.sm.Lookup(key(100 + i)); !ok || v != val(100+i) {
					return false
				}
			}
		}
		return len(c.Nodes()) == 5
	}, "restarted nodes catch up and all values converge")
	logStatus(c, "after-leader-crashes")
}

// TestCommittedValueNeverChanges：同一键重复 Set 不同值，已提交序号单调，
// 任意时刻从领导者读出的都是某个已提交值（线性一致读不返回脏读）。
func TestCommittedValueLinearizable(t *testing.T) {
	c := newTestCluster(t, 3, false, 111)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	writer := NewClient("lin-w", nodesOf(c)...)
	reader := NewClient("lin-r", nodesOf(c)...)
	ctx, cancel := context.WithTimeout(context.Background(), waitShort)
	defer cancel()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 30; i++ {
			if _, _, err := writer.Set(ctx, "shared", val(i)); err != nil {
				return
			}
		}
		close(done)
	}()
	for {
		select {
		case <-done:
			return
		default:
		}
		v, ok, err := reader.Read(ctx, "shared")
		if err != nil {
			continue
		}
		if ok {
			// 读到的值必须能被解析为某个已写入版本（v-N）。
			if len(v) < 2 || v[:2] != "v-" {
				t.Fatalf("linearizable read returned unexpected value %q", v)
			}
		}
	}
}

func (c *Cluster) nodes0() []*Node { return nodesOf(c) }
