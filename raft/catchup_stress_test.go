package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// dropCounter 为丢包钩子提供一个简单的、无锁的伪随机来源：
// 每 denom 次调用约丢 1 次（概率性丢包，保证任何具体块最终都能送达）。
var dropCounter int64

func dropRand(denom int64) bool {
	return atomic.AddInt64(&dropCounter, 1)%denom == 0
}

// snapID 是一份快照的完整身份（末索引 + 字节总长）。
// 同一末索引在不同领导/不同时刻可能对应不同字节，字节断点只在同一身份内单调。
type snapID struct {
	idx   Index
	total uint64
	xfer  uint64
}

// 大状态机追赶压力测试：追赶期间领导者持续写入与压缩、链路持续丢包/超时，
// 落后节点必须最终追平并拿到全部已提交数据；重复跑和 -race 下都要稳定。

// stressSetup 构造一个 3 节点持久化集群：极小分块 + 极频繁压缩，
// 然后让 laggard 长期离线，由 writer 在集群上持续写入制造不断前进的快照。
func stressSetup(t *testing.T, seed int64, threshold uint64, chunk int) (*Cluster, NodeID, *Client) {
	atomic.StoreInt64(&dropCounter, 0)
	t.Helper()
	c := newTestCluster(t, 3, true, seed)
	for _, id := range c.IDs() {
		c.SetSnapshotThreshold(id, threshold)
		c.SetSnapshotChunkSize(id, chunk)
	}
	restartAllWithConfig(t, c)
	waitForDur(t, 5*time.Second, func() bool { return c.Leader() != "" }, "leader")
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
	return c, laggard, NewClient("stress", nodesOf(c)...)
}

// continuousWriter 持续写入编号 KV，直到 stop 关闭；返回已写入条数（原子读取）。
// pace 为每次成功写入后的额外间隔（<=0 不等待），用于把写入速率限制在追赶链路
// 可以追上的现实量级（真实系统中状态机追赶速度必须不低于压缩推进速度才能收敛）。
func continuousWriter(ctx context.Context, cli *Client, prefix string, stop <-chan struct{}, counter *int32, pace ...time.Duration) {
	var p time.Duration
	if len(pace) > 0 {
		p = pace[0]
	}
	seq := 0
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		default:
		}
		k := fmt.Sprintf("%s-%06d", prefix, seq)
		v := fmt.Sprintf("%s-value-%08d-padding-padding-padding", prefix, seq)
		if _, _, err := cli.Set(ctx, k, v); err != nil {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(2 * time.Millisecond)
			continue
		}
		atomic.AddInt32(counter, 1)
		seq++
		if p > 0 {
			select {
			case <-time.After(p):
			case <-stop:
				return
			case <-ctx.Done():
				return
			}
		}
	}
}

// writerPace 返回持续写入的节流间隔。-race 下 fsync/序列化慢一个数量级，
// 必须放慢写入，使状态机快照推进速度不超过追赶链路能追平的速度（否则任何
// 系统都无法在无限快写入下收敛——这不是被测协议要证明的性质）。
func writerPace(base time.Duration) time.Duration {
	if raceEnabled {
		return base * 8
	}
	return base
}

// TestChunkedSnapshotCatchUpUnderContinuousLoad 复现问题主场景：
// 大状态机落后节点在「领导者持续写入 + 持续压缩 + 链路持续丢块」下追赶，
// 必须能追平，且已收到字节不能对同一份快照反复掉回 0。
func TestChunkedSnapshotCatchUpUnderContinuousLoad(t *testing.T) {
	c, laggard, cli := stressSetup(t, 909091, 40, 2048)

	ctx, cancel := context.WithTimeout(context.Background(), wd(40*time.Second))
	defer cancel()

	stop := make(chan struct{})
	var written int32
	go continuousWriter(ctx, cli, "load", stop, &written, writerPace(4*time.Millisecond))

	// 先在 laggard 离线时写入一批，制造大状态机基础。
	waitForDur(t, 5*time.Second, func() bool { return atomic.LoadInt32(&written) >= 40 }, "bulk writes before laggard joins")
	waitForDur(t, 5*time.Second, func() bool {
		for _, id := range c.IDs() {
			if id == laggard {
				continue
			}
			if !c.SnapshotExists(id) {
				return false
			}
		}
		return true
	}, "live peers compacted")

	// 链路：持续丢块（约 1/5）；落后节点落盘每块额外延迟（模拟慢盘 fsync/超时），
	// 全程不恢复。慢 I/O 只拖慢后台 worker，绝不能卡住接收方心跳/选举。
	c.SetSnapshotHook(&SnapshotFaultHook{
		Drop: func(from, to NodeID, req *InstallSnapshotRequest) bool {
			return NodeID(to) == laggard && req.Chunked && req.Offset > 0 && dropRand(3)
		},
	})

	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	ln := c.Node(laggard)
	ln.SetChunkIODelay(3 * time.Millisecond)

	// 追赶持续一段时间：写入与压缩一直不停。
	// 关键回归判据：在负载持续期间，落后节点必须能完整装完一份又一份不断前进的快照
	// （缺陷行为是会话不断被更新快照作废、字节反复回 0，负载不停就一份都装不完）。
	loadDeadline := time.Now().Add(wd(10 * time.Second))
	maxInstalled := Index(0)
	installAdvances := 0
	lastProgress := CatchUpProgress{}
	for time.Now().Before(loadDeadline) {
		p := ln.CatchUpProgress()
		lastProgress = p
		if p.State == CatchUpFailed {
			t.Fatalf("catch-up entered failed state during load: %+v", p)
		}
		if s := ln.Status().SnapshotIndex; s > maxInstalled {
			installAdvances++
			maxInstalled = s
		}
		time.Sleep(5 * time.Millisecond)
	}
	if installAdvances < 3 {
		t.Fatalf("laggard could not finish advancing snapshots under continuous load: "+
			"only %d snapshot install advance(s), max=%d, last progress=%+v",
			installAdvances, maxInstalled, lastProgress)
	}
	t.Logf("installed %d advancing snapshots under load, max snapshot index=%d",
		installAdvances, maxInstalled)

	// 停止写入与丢包，laggard 必须最终追平领导者提交点且状态机内容齐全。
	close(stop)
	c.SetSnapshotHook(nil)
	n := atomic.LoadInt32(&written)
	if n == 0 {
		t.Fatal("writer produced nothing")
	}
	t.Logf("continuous writes: %d entries", n)

	waitForDur(t, 30*time.Second, func() bool {
		leaderNode := c.Node(c.Leader())
		if leaderNode == nil {
			return false
		}
		lc := leaderNode.Status().CommitIndex
		s := ln.Status()
		return s.CommitIndex >= lc && s.AppliedIndex >= lc
	}, "laggard catches leader commit after load stops")

	// 提交位置追平后，仍要确认状态机里每一条已提交写入可见（-race 下提交位置与
	// 状态机应用之间可能有短暂滞后），而不是立即下结论。
	waitForDur(t, 30*time.Second, func() bool {
		for i := int32(0); i < n; i++ {
			k := fmt.Sprintf("load-%06d", i)
			v := fmt.Sprintf("load-value-%08d-padding-padding-padding", i)
			if got, ok := ln.sm.Lookup(k); !ok || got != v {
				return false
			}
		}
		return true
	}, "laggard state machine contains every committed write")

	for i := int32(0); i < n; i++ {
		k := fmt.Sprintf("load-%06d", i)
		v := fmt.Sprintf("load-value-%08d-padding-padding-padding", i)
		if got, ok := ln.sm.Lookup(k); !ok || got != v {
			t.Fatalf("committed data missing/rolled back at %s: got %q ok=%v (n=%d)", k, got, ok, n)
		}
	}
	if ln.CatchUpProgress().State == CatchUpFailed {
		t.Fatal("must not end in failed state")
	}
}

// TestChunkedSnapshotStagingBytesMonotonicPerIdentity 在持续负载下轮询接收进度：
// 对同一个 (快照身份) ，Received 只能单调不减；看到身份推进时记录，禁止回到更旧身份。
func TestChunkedSnapshotStagingBytesMonotonicPerIdentity(t *testing.T) {
	c, laggard, cli := stressSetup(t, 313131, 40, 1024)

	ctx, cancel := context.WithTimeout(context.Background(), wd(30*time.Second))
	defer cancel()
	stop := make(chan struct{})
	var written int32
	go continuousWriter(ctx, cli, "mono", stop, &written, writerPace(4*time.Millisecond))
	waitForDur(t, 5*time.Second, func() bool { return atomic.LoadInt32(&written) >= 60 }, "bulk writes")

	var sawTransfer, sawNonZeroAck int32
	identityReceived := map[snapID]uint64{} // 完整身份 -> 最大 Received
	identityOrder := []snapID{}
	var mu sync.Mutex

	c.SetSnapshotHook(&SnapshotFaultHook{
		Drop: func(from, to NodeID, req *InstallSnapshotRequest) bool {
			return NodeID(to) == laggard && req.Chunked && dropRand(3)
		},
	})
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	ln := c.Node(laggard)
	ln.SetChunkIODelay(2 * time.Millisecond)

	observeStop := make(chan struct{})
	observeDone := make(chan struct{})
	go func() {
		defer close(observeDone)
		lastID := snapID{}
		lastState := CatchUpIdle
		lastRecv := uint64(0)
		for {
			select {
			case <-observeStop:
				return
			default:
			}
			p := ln.CatchUpProgress()
			id := snapID{idx: p.SnapshotIdx, total: p.Total, xfer: p.TransferID}
			if p.State == CatchUpSyncing {
				mu.Lock()
				// 仅在「同一会话连续 syncing」期间要求字节单调；进入新身份、或上一
				// 快照刚 caught_up（staging 已清理、新会话从 0 开始）都是合法边界。
				if lastState == CatchUpSyncing && id == lastID && p.Received < lastRecv {
					t.Errorf("staging bytes went backwards mid-session %+v: %d -> %d",
						id, lastRecv, p.Received)
				}
				if id != lastID || lastState != CatchUpSyncing {
					identityOrder = append(identityOrder, id)
				}
				if p.Received > 0 {
					atomic.StoreInt32(&sawNonZeroAck, 1)
				}
				atomic.StoreInt32(&sawTransfer, 1)
				if _, tracked := identityReceived[id]; !tracked {
					identityReceived[id] = 0
				}
				if p.Received > identityReceived[id] {
					identityReceived[id] = p.Received
				}
				mu.Unlock()
			}
			lastID = id
			lastState = p.State
			lastRecv = p.Received
			time.Sleep(2 * time.Millisecond)
		}
	}()

	time.Sleep(wd(6 * time.Second))
	close(observeStop)
	<-observeDone
	close(stop)
	c.SetSnapshotHook(nil)

	if atomic.LoadInt32(&sawTransfer) == 0 {
		t.Fatal("never observed a catch-up transfer")
	}
	if atomic.LoadInt32(&sawNonZeroAck) == 0 {
		t.Fatal("never observed non-zero byte progress")
	}
	n := atomic.LoadInt32(&written)
	waitForDur(t, 15*time.Second, func() bool {
		ld := c.Node(c.Leader())
		return ld != nil && ln.Status().AppliedIndex >= ld.Status().CommitIndex
	}, "final convergence")
	for i := int32(0); i < n; i++ {
		k := fmt.Sprintf("mono-%06d", i)
		if _, ok := ln.sm.Lookup(k); !ok {
			t.Fatalf("missing committed key %s", k)
		}
	}
	t.Logf("observed %d distinct snapshot identities during churn: %+v", len(identityOrder), identityOrder)
}

// TestChunkedSnapshotCatchUpDropTimeoutAndRestart 综合异常路径：
// 追赶中经历「持续丢块/超时 -> 接收方重启 -> 继续丢块 -> 再重启」，
// 每次都必须从已确认位置继续，最终追平。
func TestChunkedSnapshotCatchUpDropTimeoutAndRestart(t *testing.T) {
	c, laggard, cli := stressSetup(t, 626262, 40, 1024)
	ctx, cancel := context.WithTimeout(context.Background(), wd(40*time.Second))
	defer cancel()

	stop := make(chan struct{})
	var written int32
	go continuousWriter(ctx, cli, "rt", stop, &written, writerPace(4*time.Millisecond))
	waitForDur(t, 5*time.Second, func() bool { return atomic.LoadInt32(&written) >= 50 }, "bulk writes")

	c.SetSnapshotHook(&SnapshotFaultHook{
		Drop: func(from, to NodeID, req *InstallSnapshotRequest) bool {
			return NodeID(to) == laggard && req.Chunked && dropRand(2)
		},
	})
	if err := c.Restart(laggard); err != nil {
		t.Fatal(err)
	}
	ln := c.Node(laggard)

	// 两轮「收到部分 -> 崩溃 -> 重启续传」。
	for round := 0; round < 2; round++ {
		waitForDur(t, 8*time.Second, func() bool {
			p := ln.CatchUpProgress()
			return p.State == CatchUpSyncing && p.Received > 0
		}, fmt.Sprintf("round %d: laggard receives chunks", round))
		recv := ln.CatchUpProgress().Received
		total := ln.CatchUpProgress().Total
		if err := c.Crash(laggard); err != nil {
			t.Fatal(err)
		}
		if err := c.Restart(laggard); err != nil {
			t.Fatal(err)
		}
		ln = c.Node(laggard)
		// 重启后若 staging 仍匹配领导者当前快照，已收字节不得丢失。
		// （领导者在期间可能压出更新快照，则允许从新身份 0 开始——这属于身份推进，不是回退。）
		p := ln.CatchUpProgress()
		if p.State == CatchUpSyncing && p.Total == total && p.Received+uint64(64) < recv {
			t.Fatalf("round %d: staging bytes lost across restart: had %d now %d (total %d)",
				round, recv, p.Received, total)
		}
	}

	close(stop)
	c.SetSnapshotHook(nil)
	n := atomic.LoadInt32(&written)
	waitForDur(t, wd(30*time.Second), func() bool {
		ld := c.Node(c.Leader())
		return ld != nil && ln.Status().AppliedIndex >= ld.Status().CommitIndex
	}, "convergence after restarts")
	// 多轮崩溃重启 + -race 下状态机应用可能滞后于提交位置，再等一个有界窗口。
	waitForDur(t, wd(30*time.Second), func() bool {
		for i := int32(0); i < n; i++ {
			k := fmt.Sprintf("rt-%06d", i)
			v := fmt.Sprintf("rt-value-%08d-padding-padding-padding", i)
			if got, ok := ln.sm.Lookup(k); !ok || got != v {
				return false
			}
		}
		return true
	}, "state machine contains every committed rt write")
	for i := int32(0); i < n; i++ {
		k := fmt.Sprintf("rt-%06d", i)
		v := fmt.Sprintf("rt-value-%08d-padding-padding-padding", i)
		if got, ok := ln.sm.Lookup(k); !ok || got != v {
			t.Fatalf("missing %s=%q ok=%v", k, got, ok)
		}
	}
}
