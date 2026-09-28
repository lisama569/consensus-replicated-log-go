package raft

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

const (
	// 测试使用较小 tick 与固定种子，保证快且可复现。
	waitShort = 2 * time.Second
)

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(waitShort)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting: %s", msg)
}

func newTestCluster(t *testing.T, size int, persistent bool, seed int64) *Cluster {
	t.Helper()
	c, err := NewCluster(size, persistent, seed)
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop() })
	return c
}

func logStatus(c *Cluster, prefix string) {
	for _, id := range c.IDs() {
		if n, ok := c.Nodes()[id]; ok {
			s := n.Status()
			t_logf(c, prefix, s)
		}
	}
}

func t_logf(c *Cluster, prefix string, s Status) {
	fmt.Printf("    [%s] node=%s role=%s term=%d leader=%s commit=%d applied=%d last=%d voted=%s\n",
		prefix, s.ID, s.Role, s.Term, s.LeaderID, s.CommitIndex, s.AppliedIndex, s.LastLogIndex, s.VotedFor)
}

func TestLeaderElection(t *testing.T) {
	c := newTestCluster(t, 3, false, 42)
	waitFor(t, func() bool { return c.Leader() != "" }, "a leader to be elected")
	leader := c.Leader()
	s := c.Node(leader).Status()
	if s.Role != RoleLeader || s.Term < 1 {
		t.Fatalf("unexpected leader status: %+v", s)
	}
	// 恰好一个领导者。
	leaders := 0
	for _, n := range c.Nodes() {
		if n.Status().Role == RoleLeader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaders)
	}
	logStatus(c, "after-election")
}

func TestBasicReplication(t *testing.T) {
	c := newTestCluster(t, 3, false, 7)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	client := NewClient("client-a", nodesOf(c)...)

	ctx, cancel := context.WithTimeout(context.Background(), waitShort)
	defer cancel()
	idx, dup, err := client.Set(ctx, "k1", "v1")
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if dup {
		t.Fatalf("first write should not be duplicate")
	}
	if idx < 2 { // index 1 是领导者 Noop
		t.Fatalf("unexpected commit index %d", idx)
	}

	// 多数派（进而全部）提交且状态机一致。
	waitFor(t, func() bool {
		for _, n := range c.Nodes() {
			if n.Status().CommitIndex < idx {
				return false
			}
		}
		return true
	}, "all nodes commit")

	for _, n := range c.Nodes() {
		v, ok := n.sm.Lookup("k1")
		if !ok || v != "v1" {
			t.Fatalf("node %s state mismatch: %q ok=%v", n.cfg.ID, v, ok)
		}
	}

	// 线性一致读。
	v, ok, err := client.Read(ctx, "k1")
	if err != nil || !ok || v != "v1" {
		t.Fatalf("read got %q ok=%v err=%v", v, ok, err)
	}
	logStatus(c, "after-replication")
}

func TestIdempotentRetry(t *testing.T) {
	c := newTestCluster(t, 3, false, 99)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	_ = NewClient("client-b", nodesOf(c)...)
	ctx, cancel := context.WithTimeout(context.Background(), waitShort)
	defer cancel()

	cmd := Command{Op: OpSet, Key: "dup", Value: "x", ClientID: "manual", Seq: 1}
	leader := c.Node(c.Leader())
	o1, err := leader.SubmitProposal(ctx, cmd)
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	// 用完全相同的命令再次提交到同一领导者：必须复用等待路径/被去重，无新日志。
	o2, err := leader.SubmitProposal(ctx, cmd)
	if err != nil {
		t.Fatalf("duplicate submit: %v", err)
	}
	if !o2.duplicate {
		t.Fatalf("duplicate submit must be flagged duplicate")
	}
	if o2.index != o1.index {
		t.Fatalf("duplicate must land on same index: %d vs %d", o1.index, o2.index)
	}
	logStatus(c, "after-idempotent")
}

func TestNotLeaderReject(t *testing.T) {
	c := newTestCluster(t, 3, false, 5)
	waitFor(t, func() bool { return c.Leader() != "" }, "leader")
	var follower *Node
	for _, n := range c.Nodes() {
		if n.Status().Role != RoleLeader {
			follower = n
			break
		}
	}
	// 等待该跟随者至少收到一轮心跳、已知领导者（-race + 高负载下首拍可能尚未到达）。
	waitFor(t, func() bool { return follower.Status().LeaderID != "" }, "follower learns leader")
	ctx := context.Background()
	_, err := follower.SubmitProposal(ctx, Command{Op: OpSet, Key: "x", Value: "y"})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("want ErrNotLeader, got %v", err)
	}
	var lh *LeaderHintError
	if !errors.As(err, &lh) || lh.Leader == "" {
		t.Fatalf("want leader hint, got %v", err)
	}
}

func nodesOf(c *Cluster) []*Node {
	out := make([]*Node, 0, len(c.Nodes()))
	for _, n := range c.Nodes() {
		out = append(out, n)
	}
	return out
}
