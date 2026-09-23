# 多节点复制日志一致性协议（Raft 实现）

一个**零第三方依赖**、可在本地完整运行的 Raft 复制日志实现（Go 标准库 + 进程内模拟网络）。
覆盖选主、任期管理、日志复制、幂等写入、日志压缩/快照、快照追赶、崩溃恢复，
并提供可注入丢包/重复/乱序/分区的测试床与确定性随机种子，便于可复现地验证。

- 模块路径：`github.com/lisama569/consensus-replicated-log-go`
- Go 版本：见 `go.mod`（1.25）
- 纯标准库，无外部依赖

---

## 1. 快速开始

```bash
# 运行端到端演示：选主 -> 分区 -> 恢复 -> 崩溃重启 -> 快照追赶
go run ./cmd/demo

# 全部单元测试
go test ./...

# 竞争检测 + 重复多轮（推荐用于验收）
go test ./... -race -count=10

# 只看某个场景
go test ./raft/ -run TestMinorityCannotCommit -v
go test ./raft/ -run TestSnapshotCatchUp -v
```

测试默认使用很小的逻辑时钟（10ms/tick，选举超时 100~200ms），单测秒级完成。

---

## 2. 代码结构

| 文件 | 职责 |
| --- | --- |
| `raft/types.go` | 基础类型：NodeID / Term / Index / Role / Command / LogEntry / Status |
| `raft/errors.go` | 可区分的拒绝原因（`RejectReason`）与哨兵错误 |
| `raft/config.go` | 可配置项与默认值、校验 |
| `raft/node.go` | 节点生命周期、持久化恢复、客户端通道、串行事件循环骨架 |
| `raft/eventloop.go` | 时钟 tick、事件分发、角色转换（Follower/Candidate/Leader）、RPC 入口 |
| `raft/election.go` | 选举：拉票、投票判定、得票统计、更高任期让位 |
| `raft/replication.go` | AppendEntries 收发、冲突快速回退、多数派提交、状态机应用、读屏障 |
| `raft/log.go` | 日志布局（快照占位项 + 后缀）、索引换算与修改器 |
| `raft/snapshot.go` | 压缩（takeSnapshot）、快照安装（InstallSnapshot）、快照发送 |
| `raft/statemachine.go` | KV 状态机：幂等 Apply、Lookup、JSON 快照/恢复 |
| `raft/store.go` | 持久化：硬状态、日志后缀、快照；原子写（tmp+fsync+rename） |
| `raft/transport.go` | `MemoryTransport`：同步模拟网络，支持丢包/重复/乱序/分区 |
| `raft/client.go` | 去重客户端：ClientID+单调 Seq、自动找主/重试/轮询 |
| `raft/cluster.go` | 测试/demo 集群床：崩溃（保留磁盘）、重启、分区、故障注入 |
| `raft/*_test.go` | 分区、崩溃重启、重复/乱序、快照追赶、并发访问等场景测试 |
| `cmd/demo` | 本地端到端演示 |

---

## 3. 协议状态机

### 3.1 节点角色（同一时刻最多一个有效领导者）

```
            选举超时                 获得多数票
Follower ───────────────▶ Candidate ──────────────▶ Leader
   ▲                          │                        │
   │   发现更高任期            │ 发现更高任期            │ 发现更高任期
   └──────────────────────────┴────────────────────────┘
```

- **Follower（跟随者）**：被动接收 AppendEntries/InstallSnapshot/RequestVote；选举超时后成为候选人。
- **Candidate（候选人）**：`term += 1`、先投自己、并行拉票；得到多数票成为领导者。
- **Leader（领导者）**：每个 tick 发心跳；唯一可接受客户端写的节点；上任先追加一条 Noop。

### 3.2 持久状态（`raft/store.go`，任何应答前先落盘）

- `currentTerm`：当前任期，单调递增。
- `votedFor`：本任期投票对象（含空）。
- 日志后缀（严格位于快照点之后）。
- 快照：`lastIncludedIndex / lastIncludedTerm` + 状态机字节。

写入均采用 **临时文件 → fsync → 原子 rename → fsync 目录**，崩溃时不会出现半写文件。

### 3.3 易失状态

- Follower/Candidate：`commitIndex`、`lastApplied`、选举计时器、已知 `leaderID`。
- Leader：对每个 peer 的 `nextIndex` / `matchIndex`；在途提案表；在途线性一致读队列。

### 3.4 并发模型（无数据竞争的关键）

节点的**所有可变状态只在一个事件循环 goroutine 中被修改**：

- 网络回调（`rpcHandler`）、逻辑时钟 tick、客户端请求都被封装为 `internalEvent`
  投递进带缓冲的 `events` channel，由 `run()` 串行消费；
- 出站 RPC（AppendEntries/RequestVote/InstallSnapshot）在独立 goroutine 中执行，
  响应再投递回事件循环，因此慢网络/对端崩溃不会阻塞协议；
- 状态机 `Apply` 与 `Snapshot` 在事件循环内构造，与并发读 `Lookup` 之间用 `RWMutex` 同步；
- 客户端回执统一走**非阻塞发送**（`sendOutcome`），慢/已超时客户端绝不卡住事件循环。

---

## 4. 核心不变式（Invariants）

1. **任期单调（Election Safety）**：收到 `RPC.term > currentTerm` 立即转 Follower 并持久化新任期；
   低于本任期的 RPC 一律拒绝，原因 `stale_term`。
2. **任一任期最多一个领导者（Leader Safety）**：当选必须拿到多数票；多数派在同一任期只会投一个候选人。
3. **日志匹配（Log Matching）**：AppendEntries 带 `prevLogIndex/prevLogTerm` 做一致性检查；
   冲突时按论文的 `conflictTerm/conflictIndex` 快速回退，并截断本节点冲突后缀。
4. **多数派才提交（Leader Completeness）**：
   - 只有复制到多数节点、且**属于当前任期**的条目才可提升 `commitIndex`（避免 Figure 8 问题）；
   - 少数派分区中的领导者无法获得多数确认，其写入**绝不会提交或应用**；恢复后被覆盖回滚。
5. **已提交不丢不回退（State Machine Safety）**：
   - 提交位置只增不减；崩溃重启后由「快照 + 日志后缀」重放到提交点；
   - 压缩顺序为「先原子写快照 → 后截断日志」，任意崩溃点恢复都不丢已提交状态。
6. **幂等写入（Exactly-once 效果）**：客户端命令携带 `(ClientID, Seq)`：
   - 状态机记录每个客户端最大已应用 Seq，旧 Seq 跳过（返回 `duplicate=true`，不重复写）；
   - 同 Seq 已在日志中未提交时复用同一提案等待队列，不产生第二条日志；
   - 因此客户端在超时/领导权切换后**重试同一请求是安全的**。
7. **快照一致性**：压缩点 ≤ 提交点；快照恢复后 `snapMeta`、状态机、`commitIndex`、`lastApplied`
   四者对齐；落后到快照点之前的节点通过 `InstallSnapshot` 追赶，而不是回放全量历史。
8. **线性一致读**：领导者在当前任期经一轮心跳获得多数派确认、且状态机追平登记时的提交屏障后，
   才从本地状态机返回（ReadIndex 模式）。

### 拒绝原因（`raft/errors.go`，失败可区分）

| RejectReason | 触发条件 |
| --- | --- |
| `stale_term` | 请求任期落后于节点当前任期（过期投票/过期复制都会命中） |
| `already_voted` | 本任期已投给其他候选人 |
| `log_not_up_to_date` | 候选人日志不如本节点新（先比最后 term，再比最后 index） |
| `not_leader` | 向非领导者提交写（错误附带可能的领导提示） |
| `leader_deposed` | 领导者在完成读确认前被更高任期废黜 |
| `stepped_down` | 节点已关闭/下台 |

---

## 5. 可配置项（`raft/config.go`）

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `ID` | 必填 | 本节点 ID |
| `Peers` | 必填 | 集群全部节点（含自己），集群成员变更未实现，成员静态 |
| `DataDir` | 空=纯内存 | 持久化目录；崩溃重启测试必须非空 |
| `TickInterval` | 10ms | 逻辑时钟周期，即心跳间隔 |
| `ElectionMin` / `ElectionMax` | 10 / 20 ticks | 随机化选举超时范围，避免分票活锁 |
| `SnapshotThreshold` | 50 | 快照之后增量日志达到该条数即压缩 |
| `RandSeed` | 0=时间随机 | 固定种子可让选主/抖动序列**可复现** |

客户端侧（`raft/client.go`）：重试间隔 5ms、单次尝试超时 100ms；
同一逻辑请求在所有重试中保持相同 `(ClientID, Seq)`。

模拟网络（`raft/transport.go` 的 `FaultPolicy`）：

- `DropRate`：按概率丢弃请求；`Duplicate`：请求送达两次；`Reorder`：注入随机延迟造成乱序；
- `SetPartition(id,true)`：对某节点双向 blackhole；
- `MemoryTransport.Seed(seed)`：固定故障随机源。

---

## 6. 故障假设与覆盖范围

**假设成立的故障模型：**

- 节点崩溃-恢复（fail-stop / fail-recovery）：持久化数据不被拜占庭式篡改；
- 消息可以丢失、延迟、重复、乱序，但不会被伪造内容（无拜占庭错误）；
- 多数派节点存活且彼此可达时集群可用；多数派永久丢失时不可用（标准 Raft 折中）；
- 时钟仅用于选举超时与重传调度，不依赖绝对时间一致性（超时被显著放大也只是更慢，不会出错）。

**已测试覆盖：**

| 测试 | 场景 |
| --- | --- |
| `TestLeaderElection` | 固定种子下唯一领导者当选 |
| `TestBasicReplication` | 多数派提交、全节点应用、线性一致读 |
| `TestIdempotentRetry` | 同 `(ClientID,Seq)` 重复提交复用同一索引 |
| `TestNotLeaderReject` | 非领导者拒绝并返回领导提示 |
| `TestCrashRestartDurability` | 全集群掉电重启，已提交内容不丢 |
| `TestMinorityCannotCommit` | 1 对 4 分区：少数派写不提交，多数派选新主，恢复后旧主回滚收敛 |
| `TestStaleTermFromOldLeader` | 旧任期 AppendEntries/RequestVote 被 `stale_term` 明确拒绝 |
| `TestRejectReasons` | stale_term / already_voted / log_not_up_to_date 可区分 |
| `TestDuplicateAndReorderDelivery` | 重复+乱序网络下结果唯一、日志无重复索引 |
| `TestSnapshotCompactionAndRestart` | 压缩后重启，状态机/提交/应用位置一致 |
| `TestSnapshotCatchUp` | 长期离线节点经 InstallSnapshot 追上并继续复制 |
| `TestConcurrentReadWriteSnapshot` | 并发读写+压缩（`-race`）无数据竞争，最终一致 |
| `TestClientRetriesAcrossLeaderChange` | 领导者连续崩溃，客户端自动换主重试不丢不重 |
| `TestCommittedValueLinearizable` | 并发读不返回脏值 |

测试日志统一打印节点 **角色 / 任期 / 提交位置 / 应用位置 / 快照位置 / 判定依据**
（拒绝时会额外打印 `reason` 与冲突信息）。

**未覆盖 / 刻意简化：**

- 集群成员变更（membership change）：成员列表静态；
- 日志条目按 JSON 存储、整段重写（教学清晰优先，未做分段/批量 pipeline/真实网络）；
- 快照为一次性整体安装（未做分块 offset 续传），适合中小状态机；
- 无拜占庭容错、无预投票（Pre-Vote 不影响安全性，仅改善被隔离节点恢复时的扰动）。

---

## 7. 本地验证建议

```bash
# 1) 确定性：固定种子反复运行结果应一致
go test ./... -count=20

# 2) 并发安全：长时间 race
go test ./... -race -count=20

# 3) 端到端演示（使用真实临时目录做持久化，退出自动清理）
go run ./cmd/demo
```

若要手工实验：

```go
c, _ := raft.NewCluster(5, true /*持久化*/, 1234)
defer c.Stop()
// <- 等待 c.Leader()
cli := raft.NewClient("my-client", /*nodes...*/)
cli.Set(ctx, "k", "v")                 // 多数派提交后返回
c.Isolate(c.Leader())                  // 制造分区
c.Flaky("n1", "n2", raft.RPCAppendEntries, raft.FaultPolicy{DropRate: 0.5, Duplicate: true})
// ... 恢复：c.Heal(id) / c.HealAll()
```

