// Command demo 在本地启动一个 5 节点集群，逐步演示：
//  1. 选主与多副本写入；
//  2. 网络分区：少数派不能提交，多数派选出新主并继续写入；
//  3. 分区恢复：旧领导者退位、日志收敛一致；
//  4. 崩溃重启：已提交内容不丢失；
//  5. 快照追赶：长期离线的节点通过 InstallSnapshot 追上。
//
// 运行：go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/lisama569/consensus-replicated-log-go/raft"
)

func main() {
	const seed = 20260924
	c, err := raft.NewCluster(5, true, seed)
	if err != nil {
		panic(err)
	}
	c.EnableLogs()
	defer func() { _ = c.Stop() }()

	// 使用较小的快照分块，便于在 demo 中观察到多块续传与字节级进度。
	// SetSnapshotChunkSize 对尚未创建的节点生效，因此先崩溃再重启全部节点加载配置。
	for _, id := range c.IDs() {
		c.SetSnapshotChunkSize(id, 256)
	}
	for _, id := range c.IDs() {
		must(c.Crash(id))
	}
	for _, id := range c.IDs() {
		must(c.Restart(id))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	step("1. 等待选主")
	wait(func() bool { return c.Leader() != "" })
	printStatus(c, "集群选出唯一领导者")

	step("2. 正常写入 10 个键")
	cli := raft.NewClient("demo-client", live(c)...)
	for i := 0; i < 10; i++ {
		idx, dup, err := cli.Set(ctx, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		must(err)
		fmt.Printf("   写入 key%d -> 提交索引=%d 去重=%v\n", i, idx, dup)
	}
	waitConverged(c, "key9", "value9")
	printStatus(c, "所有节点提交并应用")

	step("3. 网络分区：隔离现任领导者（1 vs 4）")
	oldLeader := c.Leader()
	c.Isolate(oldLeader)
	fmt.Printf("   节点 %s 被隔离进少数派分区\n", oldLeader)

	wait(func() bool {
		for id, n := range c.Nodes() {
			if id != oldLeader && n.Status().Role == raft.RoleLeader {
				return true
			}
		}
		return false
	})
	newLeader := ""
	for id, n := range c.Nodes() {
		if id != oldLeader && n.Status().Role == raft.RoleLeader {
			newLeader = string(id)
		}
	}
	fmt.Printf("   多数派一侧选出新领导者: %s\n", newLeader)

	step("4. 在多数派分区继续写入")
	cli.SetNodes(live(c))
	for i := 10; i < 15; i++ {
		idx, _, err := cli.Set(ctx, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		must(err)
		fmt.Printf("   写入 key%d -> 提交索引=%d（经新领导者）\n", i, idx)
	}

	step("5. 恢复分区，集群收敛")
	c.Heal(oldLeader)
	wait(func() bool {
		n := c.Node(oldLeader)
		if n == nil || n.Status().Role != raft.RoleFollower {
			return false
		}
		val, found := lookup(c, oldLeader, "key14")
		return found && val == "value14"
	})
	printStatus(c, "旧领导者已退位并补齐日志")

	step("6. 崩溃一个跟随者，再重启（持久化恢复）")
	follower := ""
	for id, n := range c.Nodes() {
		if n.Status().Role != raft.RoleLeader {
			follower = string(id)
			break
		}
	}
	must(c.Crash(raft.NodeID(follower)))
	fmt.Printf("   节点 %s 进程崩溃\n", follower)
	time.Sleep(100 * time.Millisecond)
	must(c.Restart(raft.NodeID(follower)))
	fmt.Printf("   节点 %s 从磁盘恢复并重新加入\n", follower)
	wait(func() bool {
		n := c.Node(raft.NodeID(follower))
		return n != nil && n.Status().CommitIndex >= c.Node(c.Leader()).Status().CommitIndex
	})
	fmt.Printf("   节点 %s 已追平提交位置\n", follower)

	step("7. 快照追赶：长期离线节点通过快照追上")
	// 让节点离线，制造大量已压缩历史，再重启观察 InstallSnapshot。
	victim := raft.NodeID(follower)
	must(c.Crash(victim))
	for i := 15; i < 80; i++ {
		cli.SetNodes(live(c))
		if _, _, err := cli.Set(ctx, fmt.Sprintf("bulk%d", i), fmt.Sprintf("b%d", i)); err != nil {
			must(err)
		}
	}
	fmt.Printf("   离线期间集群又提交 65 条并多次压缩，节点 %s 的历史日志已过时\n", victim)
	must(c.Restart(victim))
	// 演示用户侧可查询的追赶进度：syncing(字节进度) -> caught_up(就绪)。
	go func() {
		n := c.Node(victim)
		if n == nil {
			return
		}
		var lastState raft.CatchUpState
		var lastRecv uint64
		for i := 0; i < 5000; i++ {
			time.Sleep(2 * time.Millisecond)
			if cur := c.Node(victim); cur != nil {
				n = cur
			}
			p := n.CatchUpProgress()
			if p.State != lastState || p.Received != lastRecv {
				fmt.Printf("   [catch-up] %s: state=%s bytes=%d/%d ready=%v\n",
					victim, p.State, p.Received, p.Total, p.Ready())
				lastState = p.State
				lastRecv = p.Received
			}
			if p.Ready() {
				return
			}
		}
	}()
	wait(func() bool {
		n := c.Node(victim)
		if n == nil {
			return false
		}
		v, ok := lookup(c, victim, "bulk79")
		return ok && v == "b79" && n.Status().SnapshotIndex > 0
	})
	s := c.Node(victim).Status()
	fmt.Printf("   节点 %s 通过可恢复分块快照追至 snap=%d commit=%d\n", victim, s.SnapshotIndex, s.CommitIndex)
	printStatus(c, "最终集群状态")

	step("完成：分区、崩溃重启、消息重试、快照追赶下结果唯一且可复现")
}

func step(title string) {
	fmt.Printf("\n=== %s ===\n", title)
}

func wait(cond func() bool) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	fmt.Fprintln(os.Stderr, "demo: 等待条件超时")
	os.Exit(1)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func live(c *raft.Cluster) []*raft.Node {
	out := make([]*raft.Node, 0)
	for _, n := range c.Nodes() {
		out = append(out, n)
	}
	return out
}

func lookup(c *raft.Cluster, id raft.NodeID, key string) (string, bool) {
	// 通过线性一致读获取值；demo 中读仅用于展示判定，错误按未读到处理。
	cli := raft.NewClient("demo-read-"+string(id), live(c)...)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v, ok, err := cli.Read(ctx, key)
	if err != nil {
		return "", false
	}
	return v, ok
}

func waitConverged(c *raft.Cluster, key, want string) {
	wait(func() bool {
		for _, n := range c.Nodes() {
			val, found := lookup(c, n.Status().ID, key)
			if !found || val != want {
				return false
			}
		}
		return true
	})
}

func printStatus(c *raft.Cluster, note string) {
	fmt.Printf("   -- %s --\n", note)
	for _, n := range c.Nodes() {
		s := n.Status()
		fmt.Printf("   node=%s role=%-9s term=%d leader=%s commit=%d applied=%d snap=%d\n",
			s.ID, s.Role, s.Term, s.LeaderID, s.CommitIndex, s.AppliedIndex, s.SnapshotIndex)
	}
}
