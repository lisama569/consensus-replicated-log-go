package raft

import "context"

// startElection 由选举超时触发：进入候选人任期并向所有 peer 拉票。
func (n *Node) startElection() {
	n.becomeCandidate()
	// 单节点集群直接当选。
	if n.countGranted() >= n.majority() {
		n.becomeLeader()
	}
}

// requestVotes 并行向所有 peer 发送 RequestVote；网络调用在独立 goroutine 中，
// 响应经事件循环串行处理。
func (n *Node) requestVotes() {
	req := &RequestVoteRequest{
		Term:         n.term,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
	}
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		peer := p
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			resp := n.tr.SendRequestVote(context.Background(), peer, req)
			if resp == nil {
				return // 丢包 / 分区 / 对端崩溃
			}
			select {
			case n.events <- internalEvent{kind: eventRPC, rpc: voteResponse{peer: peer, resp: resp}}:
			case <-n.stopCh:
			}
		}()
	}
}

// handleRequestVote 在事件循环内处理投票请求（Figure 3.1 / 4.2.3 的判定）。
func (n *Node) handleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	resp := &RequestVoteResponse{Term: n.term, Peer: n.cfg.ID}

	// 规则 1：任期落后，明确拒绝（RejectStaleTerm），并发现更高任期则下台。
	if req.Term < n.term {
		resp.Reason = RejectStaleTerm
		n.debugf("reject vote from %s: stale term req=%d local=%d",
			req.CandidateID, req.Term, n.term)
		return resp
	}
	if req.Term > n.term {
		// 更高任期：无论当前角色都转为跟随者，再继续判定是否可投。
		n.becomeFollower(req.Term)
		resp.Term = n.term
	}

	// 规则 2：本任期已经投过票且对象不是该候选人（RejectAlreadyVoted）。
	if n.votedFor != "" && n.votedFor != req.CandidateID {
		resp.Reason = RejectAlreadyVoted
		n.debugf("reject vote from %s: already voted for %s in term %d",
			req.CandidateID, n.votedFor, n.term)
		return resp
	}

	// 规则 3：候选人日志必须至少与本日志一样新（RejectLogNotUpToDate）。
	// 比较：最后日志 term 较大者新；term 相同则最后 index 较大者新。
	if !n.logAtLeastAsUpToDate(req.LastLogTerm, req.LastLogIndex) {
		resp.Reason = RejectLogNotUpToDate
		n.debugf("reject vote from %s: candidate log (t=%d,i=%d) older than local (t=%d,i=%d)",
			req.CandidateID, req.LastLogTerm, req.LastLogIndex, n.lastLogTerm(), n.lastLogIndex())
		return resp
	}

	// 满足全部条件：投票并重置选举计时器（这样避免投完票立刻又发起选举）。
	n.votedFor = req.CandidateID
	n.persistState()
	n.resetElectionTimer()
	resp.VoteGranted = true
	n.debugf("granted vote to %s for term %d (cand last t=%d i=%d)",
		req.CandidateID, n.term, req.LastLogTerm, req.LastLogIndex)
	return resp
}

// logAtLeastAsUpToDate 判断候选人的日志 (candTerm,candIndex) 是否不比本节点旧。
func (n *Node) logAtLeastAsUpToDate(candTerm Term, candIndex Index) bool {
	myTerm, myIndex := n.lastLogTerm(), n.lastLogIndex()
	if candTerm != myTerm {
		return candTerm > myTerm
	}
	return candIndex >= myIndex
}

// handleVoteResponse 在事件循环内处理拉票响应。
func (n *Node) handleVoteResponse(from NodeID, resp *RequestVoteResponse) {
	// 发现更高任期：立即退回跟随者。
	if resp.Term > n.term {
		n.debugf("got vote response from %s with higher term %d, step down", from, resp.Term)
		n.becomeFollower(resp.Term)
		return
	}
	// 过期响应（已不在候选人状态，或任期不匹配）忽略。
	if n.role != RoleCandidate || resp.Term != n.term {
		return
	}
	if !resp.VoteGranted {
		n.debugf("vote denied by %s reason=%s (local t=%d)", from, resp.Reason, n.term)
		return
	}
	n.grantedVotes[from] = true
	n.debugf("vote granted by %s (%d/%d)", from, n.countGranted(), n.majority())
	if n.countGranted() >= n.majority() {
		n.becomeLeader()
	}
}

func (n *Node) majority() int { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) countGranted() int { return len(n.grantedVotes) }
