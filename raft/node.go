package raft

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Config 节点配置。
type Config struct {
	ID               int
	Peers            []int // 全部节点 ID（含自己）
	ElectionTimeout  int   // tick 数；实际超时为 ElectionTimeout+ID，确定性错峰
	HeartbeatTimeout int   // tick 数
	Storage          Storage
	Transport        Transport
}

// snapshotState 是快照中序列化的状态机镜像。
type snapshotState struct {
	KV    map[string]string
	Dedup map[string]map[uint64]ApplyResult
}

// Node 是一个 Raft 节点。
//
// 并发模型：所有公开方法加同一把互斥锁；对外发送统一走 outbox，
// 在锁释放后由 flush 投递，因此网络同步回调（Step 重入）不会死锁。
type Node struct {
	mu sync.Mutex

	id    int
	peers []int
	cfg   Config

	role     Role
	term     Term
	votedFor int // -1 表示未投票

	log         *Log
	commitIndex Index
	lastApplied Index

	// leader 状态
	nextIndex  map[int]Index
	matchIndex map[int]Index
	votes      map[int]bool

	// 幂等表：clientID -> requestID -> 已应用结果
	dedup map[string]map[uint64]ApplyResult
	// 已入日志但未提交的请求键
	pending map[string]map[uint64]bool

	// 状态机：key -> value
	kv map[string]string

	lastSnap Snapshot

	storage   Storage
	transport Transport

	electionElapsed  int
	heartbeatElapsed int

	outbox []Message
}

func NewNode(cfg Config) *Node {
	n := &Node{
		id:         cfg.ID,
		peers:      append([]int(nil), cfg.Peers...),
		cfg:        cfg,
		role:       Follower,
		votedFor:   -1,
		log:        NewLog(),
		nextIndex:  make(map[int]Index),
		matchIndex: make(map[int]Index),
		votes:      make(map[int]bool),
		dedup:      make(map[string]map[uint64]ApplyResult),
		pending:    make(map[string]map[uint64]bool),
		kv:         make(map[string]string),
		storage:    cfg.Storage,
		transport:  cfg.Transport,
	}
	// 从持久存储恢复：快照 -> 日志 -> 硬状态 -> 重放到提交点。
	hs, _ := n.storage.LoadHardState()
	snap, _ := n.storage.LoadSnapshot()
	entries, _ := n.storage.LoadEntries()
	n.term, n.votedFor, n.commitIndex = hs.Term, hs.VotedFor, hs.Commit
	if snap.Index > 0 {
		n.lastSnap = snap
		n.log.ResetToSnapshot(snap)
		var st snapshotState
		if err := json.Unmarshal(snap.Data, &st); err == nil {
			n.kv = st.KV
			n.dedup = st.Dedup
		}
		n.lastApplied = snap.Index
	}
	for _, e := range entries {
		n.log.Append(e)
	}
	n.applyTo(n.commitIndex)
	return n
}

// ---------- 公开方法 ----------

// Tick 推进逻辑时钟：驱动选举与心跳。
func (n *Node) Tick() {
	n.mu.Lock()
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTimeout {
			n.heartbeatElapsed = 0
			n.broadcastLocked()
		}
	} else {
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout() {
			n.campaignLocked()
		}
	}
	n.mu.Unlock()
	n.flush()
}

// Step 处理一条收到的消息。
func (n *Node) Step(m Message) {
	n.mu.Lock()
	switch m.Type {
	case MsgRequestVote:
		n.handleRequestVoteLocked(m)
	case MsgRequestVoteResp:
		n.handleRequestVoteRespLocked(m)
	case MsgAppend:
		n.handleAppendLocked(m)
	case MsgAppendResp:
		n.handleAppendRespLocked(m)
	case MsgInstallSnapshot:
		n.handleInstallSnapshotLocked(m)
	case MsgInstallSnapshotResp:
		n.handleInstallSnapshotRespLocked(m)
	}
	n.mu.Unlock()
	n.flush()
}

// Propose 提交一条客户端命令。
// 返回 (已应用结果, ReasonDuplicate) 表示重复请求；
// 返回 (零值, ReasonNone) 表示已接受，等待多数派确认后应用；
// 返回 (零值, ReasonNotLeader) 表示当前节点不是领导者。
func (n *Node) Propose(cmd Command) (ApplyResult, RejectReason) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return ApplyResult{}, ReasonNotLeader
	}
	if m, ok := n.dedup[cmd.ClientID]; ok {
		if res, ok := m[cmd.RequestID]; ok {
			n.mu.Unlock()
			return res, ReasonDuplicate
		}
	}
	if m, ok := n.pending[cmd.ClientID]; ok && m[cmd.RequestID] {
		n.mu.Unlock()
		return ApplyResult{}, ReasonDuplicate // 已在日志中待提交
	}
	n.log.Append(Entry{Term: n.term, Command: cmd})
	if n.pending[cmd.ClientID] == nil {
		n.pending[cmd.ClientID] = make(map[uint64]bool)
	}
	n.pending[cmd.ClientID][cmd.RequestID] = true
	n.persistLocked()
	idx := n.log.LastIndex()
	n.matchIndex[n.id] = idx
	n.nextIndex[n.id] = idx + 1
	n.broadcastLocked()
	n.maybeCommitLocked() // 单节点集群可立即提交
	n.mu.Unlock()
	n.flush()
	return ApplyResult{}, ReasonNone
}

// Read 读取状态机当前已应用的值。
func (n *Node) Read(key string) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	v, ok := n.kv[key]
	return v, ok
}

// Snapshot 压缩 (baseIndex, to] 的日志并生成快照。to 必须 <= commitIndex。
func (n *Node) Snapshot(to Index) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if to > n.commitIndex {
		return fmt.Errorf("raft: cannot snapshot uncommitted index %d (commit=%d)", to, n.commitIndex)
	}
	if to <= n.log.baseIndex {
		return nil // 已压缩过
	}
	term, err := n.log.TermAt(to)
	if err != nil {
		return err
	}
	data, err := json.Marshal(snapshotState{KV: n.kv, Dedup: n.dedup})
	if err != nil {
		return err
	}
	snap := Snapshot{Index: to, Term: term, Data: data}
	if err := n.storage.SaveSnapshot(snap); err != nil {
		return err
	}
	n.lastSnap = snap
	n.log.CompactTo(to, term)
	n.persistLocked()
	return nil
}

// State 返回当前角色、任期、提交位置（用于测试断言与日志）。
func (n *Node) State() (Role, Term, Index) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role, n.term, n.commitIndex
}

// Applied 返回已应用位置。
func (n *Node) Applied() Index {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

// Describe 输出节点状态行，供测试日志打印判定依据。
func (n *Node) Describe() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return fmt.Sprintf("node=%d role=%s term=%d commit=%d applied=%d lastLog=(%d,%d) votedFor=%d",
		n.id, n.role, n.term, n.commitIndex, n.lastApplied,
		n.log.LastIndex(), n.log.LastTerm(), n.votedFor)
}

// ---------- 内部：选举 ----------

func (n *Node) electionTimeout() int { return n.cfg.ElectionTimeout + n.id }

func (n *Node) campaignLocked() {
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.electionElapsed = 0
	n.votes = map[int]bool{n.id: true}
	n.persistLocked()
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.sendLocked(Message{
			Type: MsgRequestVote, From: n.id, To: p, Term: n.term,
			LastLogIndex: n.log.LastIndex(), LastLogTerm: n.log.LastTerm(),
		})
	}
	n.maybeWinLocked()
}

func (n *Node) handleRequestVoteLocked(m Message) {
	reply := Message{Type: MsgRequestVoteResp, From: n.id, To: m.From, Term: n.term}
	if m.Term < n.term {
		reply.Granted, reply.RejectReason = false, ReasonStaleTerm
		n.sendLocked(reply)
		return
	}
	if m.Term > n.term {
		n.becomeFollowerLocked(m.Term)
	}
	reply.Term = n.term
	upToDate := m.LastLogTerm > n.log.LastTerm() ||
		(m.LastLogTerm == n.log.LastTerm() && m.LastLogIndex >= n.log.LastIndex())
	if (n.votedFor == -1 || n.votedFor == m.From) && upToDate {
		n.votedFor = m.From
		n.electionElapsed = 0
		n.persistLocked()
		reply.Granted = true
	} else {
		reply.Granted = false
		if !upToDate {
			reply.RejectReason = ReasonLogMismatch // 候选人日志落后
		} else {
			reply.RejectReason = ReasonAlreadyVoted
		}
	}
	n.sendLocked(reply)
}

func (n *Node) handleRequestVoteRespLocked(m Message) {
	if m.Term > n.term {
		n.becomeFollowerLocked(m.Term)
		return
	}
	if n.role != Candidate || m.Term != n.term {
		return // 过期投票响应：忽略
	}
	if m.Granted {
		n.votes[m.From] = true
		n.maybeWinLocked()
	}
}

func (n *Node) maybeWinLocked() {
	if n.role != Candidate {
		return
	}
	if len(n.votes) > len(n.peers)/2 {
		n.becomeLeaderLocked()
	}
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.heartbeatElapsed = 0
	last := n.log.LastIndex()
	for _, p := range n.peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.matchIndex[n.id] = last
	// 追加一条当前任期的 no-op，使前任期未提交项可随之间接提交。
	n.log.Append(Entry{Term: n.term})
	n.matchIndex[n.id] = n.log.LastIndex()
	n.persistLocked()
	n.broadcastLocked()
	n.maybeCommitLocked()
}

func (n *Node) becomeFollowerLocked(term Term) {
	n.role = Follower
	n.term = term
	n.votedFor = -1
	n.electionElapsed = 0
	n.persistLocked()
}

// ---------- 内部：日志复制 ----------

func (n *Node) broadcastLocked() {
	for _, p := range n.peers {
		if p != n.id {
			n.sendToLocked(p)
		}
	}
}

func (n *Node) sendToLocked(p int) {
	next := n.nextIndex[p]
	if next <= n.log.baseIndex {
		// 跟随者严重落后：直接发快照而不是回放全部历史。
		n.sendLocked(Message{
			Type: MsgInstallSnapshot, From: n.id, To: p, Term: n.term,
			Snapshot: n.lastSnap,
		})
		return
	}
	prev := next - 1
	prevTerm, _ := n.log.TermAt(prev)
	n.sendLocked(Message{
		Type: MsgAppend, From: n.id, To: p, Term: n.term,
		PrevLogIndex: prev, PrevLogTerm: prevTerm,
		Entries:      n.log.Slice(next, n.log.LastIndex()),
		LeaderCommit: n.commitIndex,
	})
}

func (n *Node) handleAppendLocked(m Message) {
	reply := Message{Type: MsgAppendResp, From: n.id, To: m.From, Term: n.term}
	if m.Term < n.term {
		reply.Granted, reply.RejectReason = false, ReasonStaleTerm
		n.sendLocked(reply)
		return
	}
	if m.Term > n.term || n.role != Follower {
		n.becomeFollowerLocked(m.Term)
	}
	reply.Term = n.term
	n.electionElapsed = 0

	if m.PrevLogIndex < n.log.baseIndex {
		// 前序已压缩：让领导者从 baseIndex+1 重发。
		reply.Granted, reply.MatchIndex = true, n.log.baseIndex
		n.sendLocked(reply)
		return
	}
	t, err := n.log.TermAt(m.PrevLogIndex)
	if err != nil || t != m.PrevLogTerm {
		reply.Granted, reply.RejectReason = false, ReasonLogMismatch
		n.sendLocked(reply)
		return
	}
	// 冲突截断 + 追加（重复消息天然幂等：同任期同下标直接跳过）。
	lastNew := m.PrevLogIndex
	for _, e := range m.Entries {
		if e.Index <= n.log.baseIndex {
			lastNew = e.Index
			continue
		}
		if et, err := n.log.TermAt(e.Index); err == nil && et == e.Term {
			lastNew = e.Index
			continue
		}
		n.log.TruncateSuffix(e.Index)
		n.log.Append(Entry{Term: e.Term, Command: e.Command})
		lastNew = e.Index
	}
	if m.LeaderCommit > n.commitIndex {
		ci := m.LeaderCommit
		if ci > n.log.LastIndex() {
			ci = n.log.LastIndex()
		}
		n.commitIndex = ci
		n.applyTo(ci)
	}
	n.persistLocked()
	reply.Granted, reply.MatchIndex = true, lastNew
	n.sendLocked(reply)
}

func (n *Node) handleAppendRespLocked(m Message) {
	if m.Term > n.term {
		n.becomeFollowerLocked(m.Term)
		return
	}
	if n.role != Leader || m.Term != n.term {
		return // 过期响应：忽略
	}
	if m.Granted {
		if m.MatchIndex > n.matchIndex[m.From] {
			n.matchIndex[m.From] = m.MatchIndex
			n.nextIndex[m.From] = m.MatchIndex + 1
		}
		n.maybeCommitLocked()
		// 还有更多日志则立即补发，加速收敛。
		if n.nextIndex[m.From] <= n.log.LastIndex() {
			n.sendToLocked(m.From)
		}
		return
	}
	// 被拒绝：回退 nextIndex 重试（快照场景由 sendToLocked 判定）。
	if m.RejectReason == ReasonStaleTerm {
		return
	}
	if n.nextIndex[m.From] > 1 {
		n.nextIndex[m.From]--
	}
	n.sendToLocked(m.From)
}

func (n *Node) maybeCommitLocked() {
	if n.role != Leader {
		return
	}
	for i := n.log.LastIndex(); i > n.commitIndex; i-- {
		t, err := n.log.TermAt(i)
		if err != nil || t != n.term {
			continue // 只提交当前任期的条目（Raft 安全性）
		}
		count := 0
		for _, p := range n.peers {
			if n.matchIndex[p] >= i {
				count++
			}
		}
		if count > len(n.peers)/2 {
			n.commitIndex = i
			n.applyTo(i)
			n.persistLocked()
			n.broadcastLocked() // 通知提交位置推进
		}
		break
	}
}

// ---------- 内部：快照 ----------

func (n *Node) handleInstallSnapshotLocked(m Message) {
	reply := Message{Type: MsgInstallSnapshotResp, From: n.id, To: m.From, Term: n.term}
	if m.Term < n.term {
		reply.Granted, reply.RejectReason = false, ReasonStaleTerm
		n.sendLocked(reply)
		return
	}
	if m.Term > n.term || n.role != Follower {
		n.becomeFollowerLocked(m.Term)
	}
	reply.Term = n.term
	n.electionElapsed = 0
	s := m.Snapshot
	if s.Index <= n.commitIndex {
		reply.Granted, reply.MatchIndex = true, n.commitIndex
		n.sendLocked(reply)
		return
	}
	var st snapshotState
	if err := json.Unmarshal(s.Data, &st); err != nil {
		reply.Granted, reply.RejectReason = false, ReasonLogMismatch
		n.sendLocked(reply)
		return
	}
	n.storage.SaveSnapshot(s)
	n.lastSnap = s
	n.log.ResetToSnapshot(s)
	n.kv = st.KV
	n.dedup = st.Dedup
	n.commitIndex, n.lastApplied = s.Index, s.Index
	n.persistLocked()
	reply.Granted, reply.MatchIndex = true, s.Index
	n.sendLocked(reply)
}

func (n *Node) handleInstallSnapshotRespLocked(m Message) {
	if m.Term > n.term {
		n.becomeFollowerLocked(m.Term)
		return
	}
	if n.role != Leader || m.Term != n.term || !m.Granted {
		return
	}
	if m.MatchIndex > n.matchIndex[m.From] {
		n.matchIndex[m.From] = m.MatchIndex
		n.nextIndex[m.From] = m.MatchIndex + 1
	}
	n.maybeCommitLocked()
	if n.nextIndex[m.From] <= n.log.LastIndex() {
		n.sendToLocked(m.From)
	}
}

// ---------- 内部：应用与持久化 ----------

// applyTo 把 (lastApplied, to] 的日志应用到状态机，并登记幂等结果。
func (n *Node) applyTo(to Index) {
	for i := n.lastApplied + 1; i <= to; i++ {
		if i <= n.log.baseIndex {
			n.lastApplied = i
			continue
		}
		e, err := n.log.EntryAt(i)
		if err != nil {
			return
		}
		cmd := e.Command
		var res ApplyResult
		if cmd.ClientID != "" { // no-op 条目不进幂等表
			old := n.kv[cmd.Key]
			n.kv[cmd.Key] = cmd.Value
			res = ApplyResult{Index: i, Term: e.Term, Command: cmd, Value: old}
			if n.dedup[cmd.ClientID] == nil {
				n.dedup[cmd.ClientID] = make(map[uint64]ApplyResult)
			}
			n.dedup[cmd.ClientID][cmd.RequestID] = res
			if n.pending[cmd.ClientID] != nil {
				delete(n.pending[cmd.ClientID], cmd.RequestID)
			}
		}
		_ = res
		n.lastApplied = i
	}
}

// persistLocked 持久化硬状态与日志（调用方须持锁）。
func (n *Node) persistLocked() {
	n.storage.SaveHardState(HardState{Term: n.term, VotedFor: n.votedFor, Commit: n.commitIndex})
	n.storage.SaveEntries(n.log.Slice(n.log.baseIndex+1, n.log.LastIndex()))
}

// ---------- 内部：消息发送 ----------

func (n *Node) sendLocked(m Message) { n.outbox = append(n.outbox, m) }

// flush 在锁外投递 outbox，允许网络同步回调重入其他节点。
func (n *Node) flush() {
	n.mu.Lock()
	msgs := n.outbox
	n.outbox = nil
	n.mu.Unlock()
	for _, m := range msgs {
		n.transport.Send(m)
	}
}
