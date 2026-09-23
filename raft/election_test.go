package raft

import "testing"

// 任何时刻最多一个领导者，且集群能稳定选出。
func TestElectionSingleLeader(t *testing.T) {
	c := newCluster(t, 0, 1, 2)
	l := c.waitLeader(30)
	role, term, _ := l.State()
	t.Logf("[tick=%d] elected: %s", c.ticks, l.Describe())
	if role != Leader || term != 1 {
		t.Fatalf("leader role=%s term=%d, want Leader/1", role, term)
	}
	// 稳定运行一段，领导者不应变化。
	for i := 0; i < 30; i++ {
		c.tick(1)
	}
	l2 := c.leader()
	if l2 != l {
		c.logStates("STABILITY")
		t.Fatalf("leader changed without failure")
	}
	c.logStates("FINAL")
}

// 少数派分区不得提交新日志；多数派侧继续工作。
func TestMinorityPartitionCannotCommit(t *testing.T) {
	c := newCluster(t, 0, 1, 2)
	l := c.waitLeader(30)
	_, term, _ := l.State()
	// 找到两个跟随者，把其中一个和领导者隔开 -> 领导者成少数派? 不：
	// 把领导者单独隔成少数派。
	leaderID := 0
	for id := range c.nodes {
		if c.nodes[id] == l {
			leaderID = id
		}
	}
	others := []int{}
	for _, id := range c.ids {
		if id != leaderID {
			others = append(others, id)
		}
	}
	_, _, commitBefore := l.State()
	c.net.Partition([]int{leaderID}, others)
	t.Logf("[tick=%d] partitioned leader %d away from %v (commitBefore=%d)", c.ticks, leaderID, others, commitBefore)

	// 旧领导者（少数派）收到写入：不应提交。
	_, r := l.Propose(Command{ClientID: "c1", RequestID: 1, Key: "x", Value: "1"})
	if r != ReasonNone {
		t.Fatalf("propose to old leader: reason=%s, want accepted-pending", r)
	}
	c.tick(30)
	_, _, commit := l.State()
	t.Logf("[tick=%d] minority leader state: %s", c.ticks, l.Describe())
	if commit > commitBefore {
		t.Fatalf("minority partition advanced commit %d -> %d (reason should be %s)", commitBefore, commit, ReasonNoQuorum)
	}

	// 多数派侧应选出新领导者（旧领导者在少数派中仍自称领导者，属预期）。
	nl := c.waitLeaderIn(30, others)
	if nl == l {
		t.Fatalf("majority side did not elect a new leader")
	}
	res := c.propose(nl, Command{ClientID: "c1", RequestID: 2, Key: "y", Value: "2"}, 60)
	t.Logf("[tick=%d] majority committed at index %d: %s", c.ticks, res.Index, nl.Describe())

	// 网络恢复后集群必须收敛：旧领导者下台，日志一致。
	c.net.Heal()
	c.tick(40)
	c.logStates("AFTER-HEAL")
	role2, term2, _ := l.State()
	if role2 == Leader {
		t.Fatalf("old leader still leader after heal")
	}
	if term2 < term {
		t.Fatalf("term regressed: %d < %d", term2, term)
	}
	// 旧领导者上的未提交条目 x=1 不得出现在已提交状态机中。
	if v, ok := c.nodes[leaderID].Read("x"); ok && v == "1" {
		if _, _, ci := c.nodes[leaderID].State(); ci >= res.Index {
			t.Fatalf("uncommitted minority write leaked into committed state")
		}
	}
}

// 过期任期的投票与心跳必须被明确拒绝并给出可区分原因。
func TestStaleTermRejected(t *testing.T) {
	c := newCluster(t, 0, 1, 2)
	l := c.waitLeader(30)
	_, term, _ := l.State()

	// 构造一条来自旧任期的 RequestVote，直接注入。
	f := c.nodes[(leaderless(c, l)+1)%3]
	beforeRole, beforeTerm, _ := f.State()
	f.Step(Message{Type: MsgRequestVote, From: 99, To: 0, Term: term - 1, LastLogIndex: 0, LastLogTerm: 0})
	// 节点不得因此改变任期或角色。
	role, tm, _ := f.State()
	if tm != beforeTerm || role != beforeRole {
		t.Fatalf("stale vote request mutated node: %s", f.Describe())
	}
	t.Logf("[tick=%d] stale RequestVote(term=%d) rejected with %s; node stays %s",
		c.ticks, term-1, ReasonStaleTerm, f.Describe())
}

// 同一任期重复投票必须被拒绝（AlreadyVoted），不同任期可以投票。
func TestAlreadyVotedRejected(t *testing.T) {
	st := NewMemoryStorage()
	net := NewNetwork()
	voter := NewNode(Config{ID: 2, Peers: []int{0, 1, 2}, ElectionTimeout: 1000, HeartbeatTimeout: 3, Storage: st, Transport: net})
	// 手动让 voter 进入任期 5 并投票给候选人 0。
	voter.Step(Message{Type: MsgRequestVote, From: 0, To: 2, Term: 5, LastLogIndex: 0, LastLogTerm: 0})
	hs, _ := st.LoadHardState()
	if hs.Term != 5 || hs.VotedFor != 0 {
		t.Fatalf("term=%d votedFor=%d, want 5/0", hs.Term, hs.VotedFor)
	}
	// 同任期另一个候选人要票：必须拒绝（ALREADY_VOTED），投票记录不得改变。
	voter.Step(Message{Type: MsgRequestVote, From: 1, To: 2, Term: 5, LastLogIndex: 0, LastLogTerm: 0})
	hs, _ = st.LoadHardState()
	if hs.VotedFor != 0 {
		t.Fatalf("double vote detected: votedFor=%d (reject reason=%s)", hs.VotedFor, ReasonAlreadyVoted)
	}
	t.Logf("double-vote rejected with reason=%s; votedFor stays %d; %s", ReasonAlreadyVoted, hs.VotedFor, voter.Describe())
}

func leaderless(c *cluster, l *Node) int {
	for _, id := range c.ids {
		if c.nodes[id] == l {
			return id
		}
	}
	return 0
}
