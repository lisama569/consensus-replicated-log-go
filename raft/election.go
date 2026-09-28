package raft

import "context"

// startElection 由选举超时触发：先进行预投票（Pre-Vote）。
//
// 为什么需要 Pre-Vote：大快照追赶期间，落后节点可能因慢盘/链路抖动暂时收不全
// 心跳而选举超时。若它立刻 term+=1 并到处拉票，即使选不上，也会用更高任期
// 扰动在任领导者（心跳被 stale_term 拒绝、领导权频繁切换），追赶会话随之不断
// 作废。Pre-Vote 让节点先用“若我参选”的假任期试探多数派是否愿意投它，
// 得到多数预投票后才真正递增任期，避免一个明显追不上的节点把整个集群搅翻。
func (n *Node) startElection() {
	if n.preVoting {
		return
	}
	n.preVoting = true
	n.requestPreVotes()
}

// requestPreVotes 发送预投票请求（不递增本节点任期，不持久化投票）。
func (n *Node) requestPreVotes() {
	req := &RequestVoteRequest{
		Term:         n.term,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
		PreVote:      true,
	}
	n.preVotesGranted = map[NodeID]bool{}
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
				return
			}
			select {
			case n.events <- internalEvent{kind: eventRPC, rpc: voteResponse{peer: peer, resp: resp}}:
			case <-n.stopCh:
			}
		}()
	}
}

// beginRealElection 在 Pre-Vote 获得多数派后真正成为候选人并递增任期。
func (n *Node) beginRealElection() {
	n.becomeCandidate()
	if n.countGranted() >= n.majority() {
		n.becomeLeader()
	}
}

// requestVotes 并行向所有 peer 发送正式 RequestVote；网络调用在独立 goroutine 中，
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

// handleRequestVote 在事件循环内处理投票请求（Figure 3.1 / 4.2.3 + Pre-Vote）。
func (n *Node) handleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	resp := &RequestVoteResponse{Term: n.term, Peer: n.cfg.ID, PreVote: req.PreVote}

	// Pre-Vote：不改变本节点任何持久状态（不递增任期、不记录投票），
	// 只判断“假设它参选，它的日志够不够新、任期是否过期”。
	// 落后节点拿不到多数预投票，就不会用虚假高任期扰动在任领导者。
	if req.PreVote {
		if req.Term < n.term {
			resp.Reason = RejectStaleTerm
			return resp
		}
		if !n.logAtLeastAsUpToDate(req.LastLogTerm, req.LastLogIndex) {
			resp.Reason = RejectLogNotUpToDate
			return resp
		}
		resp.VoteGranted = true
		resp.Term = n.term // 回报当前任期，候选者发现更高任期会主动让位
		return resp
	}

	// 规则 1：任期落后，明确拒绝（RejectStaleTerm），并发现更高任期则下台。
	if req.Term < n.term {
		resp.Reason = RejectStaleTerm
		n.debugf("reject vote from %s: stale term req=%d local=%d",
			req.CandidateID, req.Term, n.term)
		return resp
	}
	if req.Term > n.term {
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
	if !n.logAtLeastAsUpToDate(req.LastLogTerm, req.LastLogIndex) {
		resp.Reason = RejectLogNotUpToDate
		n.debugf("reject vote from %s: candidate log (t=%d,i=%d) older than local (t=%d,i=%d)",
			req.CandidateID, req.LastLogTerm, req.LastLogIndex, n.lastLogTerm(), n.lastLogIndex())
		return resp
	}

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

// handleVoteResponse 在事件循环内处理拉票响应（区分 Pre-Vote 与正式投票）。
func (n *Node) handleVoteResponse(from NodeID, resp *RequestVoteResponse) {
	if resp.PreVote {
		n.handlePreVoteResponse(from, resp)
		return
	}
	if resp.Term > n.term {
		n.debugf("got vote response from %s with higher term %d, step down", from, resp.Term)
		n.becomeFollower(resp.Term)
		return
	}
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

// handlePreVoteResponse 处理预投票响应：拿到多数预投票才真正递增任期参选；
// 否则安静地留在跟随者状态，不扰动在任领导者，等待下次选举超时。
func (n *Node) handlePreVoteResponse(from NodeID, resp *RequestVoteResponse) {
	if !n.preVoting {
		return
	}
	if resp.Term > n.term {
		n.debugf("pre-vote: %s reports higher term %d, abort", from, resp.Term)
		n.preVoting = false
		n.preVotesGranted = nil
		n.becomeFollower(resp.Term)
		return
	}
	if !resp.VoteGranted {
		n.debugf("pre-vote denied by %s reason=%s", from, resp.Reason)
		return
	}
	n.preVotesGranted[from] = true
	if len(n.preVotesGranted)+1 >= n.majority() {
		n.debugf("pre-vote won (%d pre-grants), begin real election", len(n.preVotesGranted))
		n.preVoting = false
		n.preVotesGranted = nil
		n.beginRealElection()
	}
}

func (n *Node) majority() int { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) countGranted() int { return len(n.grantedVotes) }
