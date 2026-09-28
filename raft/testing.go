package raft

import "time"

// 本文件提供测试辅助入口：所有 RPC 都经事件循环串行执行，
// 避免测试代码直接调用处理器而与事件循环产生数据竞争。

// CallRPC 在节点事件循环内同步执行一次入站 RPC，返回其响应。
// req 必须是 *AppendEntriesRequest / *RequestVoteRequest / *InstallSnapshotRequest。
func (n *Node) CallRPC(req any) any {
	return n.rpcHandler("test-peer", req)
}

// SetSnapshotThreshold 在事件循环内修改压缩阈值（测试用，避免并发读写 Config）。
func (n *Node) SetSnapshotThreshold(threshold uint64) {
	ch := make(chan struct{}, 1)
	ev := internalEvent{kind: eventRPC, from: n.cfg.ID,
		rpc: thresholdCall{threshold: threshold, done: ch}}
	select {
	case n.events <- ev:
	case <-n.stopCh:
		return
	}
	select {
	case <-ch:
	case <-n.stopCh:
	}
}

type thresholdCall struct {
	threshold uint64
	done      chan struct{}
}

// SetSnapshotChunkSize 在事件循环内修改分块大小（测试用）。
func (n *Node) SetSnapshotChunkSize(bytes int) {
	ch := make(chan struct{}, 1)
	ev := internalEvent{kind: eventRPC, from: n.cfg.ID,
		rpc: chunkSizeCall{bytes: bytes, done: ch}}
	select {
	case n.events <- ev:
	case <-n.stopCh:
		return
	}
	select {
	case <-ch:
	case <-n.stopCh:
	}
}

type chunkSizeCall struct {
	bytes int
	done  chan struct{}
}

// indexRange 是日志覆盖范围。
type indexRange struct{ snap, last Index }

// LogIndexRange 返回 (快照最后索引, 日志最后索引)，经事件循环读取（测试用）。
func (n *Node) LogIndexRange() (Index, Index) {
	ch := make(chan indexRange, 1)
	ev := internalEvent{kind: eventRPC, from: n.cfg.ID,
		rpc: logRangeCall{res: ch}}
	select {
	case n.events <- ev:
	case <-n.stopCh:
		return 0, 0
	}
	select {
	case r := <-ch:
		return r.snap, r.last
	case <-n.stopCh:
		return 0, 0
	}
}

type logRangeCall struct{ res chan<- indexRange }

// SetChunkIODelay 仅测试用：让接收侧落盘 worker 每块额外等待 d（模拟慢盘 fsync）。
// 验证慢 I/O 不会阻塞事件循环（心跳/选举仍可处理）。
func (n *Node) SetChunkIODelay(d time.Duration) {
	ch := make(chan struct{}, 1)
	ev := internalEvent{kind: eventRPC, from: n.cfg.ID,
		rpc: ioDelayCall{d: d, done: ch}}
	select {
	case n.events <- ev:
	case <-n.stopCh:
		return
	}
	select {
	case <-ch:
	case <-n.stopCh:
	}
}

type ioDelayCall struct {
	d    time.Duration
	done chan struct{}
}

type quietCall struct{ res chan catchUpQuiet }

// CatchUpProgressQuiet 经事件循环读取细粒度接收状态（含后台落盘是否在途，测试用）。
func (n *Node) CatchUpProgressQuiet() catchUpQuiet {
	ch := make(chan catchUpQuiet, 1)
	ev := internalEvent{kind: eventRPC, from: n.cfg.ID, rpc: quietCall{res: ch}}
	select {
	case n.events <- ev:
	case <-n.stopCh:
		return catchUpQuiet{state: CatchUpIdle}
	}
	select {
	case q := <-ch:
		return q
	case <-n.stopCh:
		return catchUpQuiet{state: CatchUpIdle}
	}
}
