# 多节点复制日志一致性协议（Raft 实现）

一个**零第三方依赖**、可在本地完整运行的 Raft 复制日志实现（Go 标准库 + 进程内模拟网络）。
覆盖选主、任期管理、日志复制、幂等写入、日志压缩/快照、整份快照追赶、
**可恢复的大快照分块续传**、崩溃恢复，
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
go test ./raft/ -run 'TestChunkedSnapshot|TestChunkReceiver' -v
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
| `raft/chunksnap.go` | 大快照分块续传：InstallSnapshotChunk、接收检查点、断点续传、进度查询/取消 |
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
- 分块接收在途文件（仅大快照追赶期间存在，完成即删除）：
  `snapshot.incoming`（已确认字节前缀）与 `snapshot.incoming.json`（会话检查点）。

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
   四者对齐；落后到快照点之前的节点通过 `InstallSnapshot`（小快照整份）或
   `InstallSnapshotChunk`（大快照分块续传，见第 5 节）追赶，而不是回放全量历史；
   分块只有收齐且 CRC 通过才安装，半份数据永不进入状态机。
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

## 5. 大快照分块续传（InstallSnapshotChunk）

状态机很大时，整份 `InstallSnapshot` 要求一次传完：弱网丢包、传输超时、
接收节点重启或传输期间换主都会导致**整份重传**。本实现在保留整份路径
（小快照兼容旧行为）的同时，新增一层**分块、可恢复**的快照追赶。

### 5.1 协议形态

- 快照字节按 `Config.SnapshotChunkSize`（默认 64KB）切块，逐块发送
  `InstallSnapshotChunk{SessionID, LastIncludedIndex/Term, Offset, TotalSize, CRC, Data, Done}`。
- **会话 ID 由快照内容派生**：`LastIncludedIndex:Term:CRC32`。
  不同领导者持有同一份快照文件（确定性状态机）时会话 ID 相同，
  因此**换主后可在同一会话上续传**；快照内容不同（压缩推进）则自然换会话。
- 每个块的响应返回接收端**已确认（已 fsync）的连续前缀长度 `NextOffset`**：
  发送端一律从该偏移续发——丢包、丢响应、乱序、重启后均不从头重传。
- 发送端每对端最多一块在途；在途超时（15 个 tick）按确认偏移重发；
  在途静默期由领导者 tick 补一个快照边界心跳，避免接收端选举超时。

### 5.2 接收端持久化与安装（半份数据永不可用）

- 接收端维护在途会话并为每块落盘：`snapshot.incoming`（部分字节，追加+fsync）
  与 `snapshot.incoming.json`（检查点，tmp+fsync+rename 原子写）。
- 写入顺序是**先数据后检查点**：任意崩溃时刻，已确认前缀都已完整落盘；
  数据文件可能略长于检查点，重启加载时按检查点截断，绝不会少收。
- 只有满足全部条件才安装：`Done` 到达 **且** 收齐 `TotalSize` 字节
  **且** 整体 CRC32 校验通过。随后原子写入正式快照（复用整份安装路径：
  先快照、后截日志、再恢复状态机），并删除在途文件。
- 重复块（偏移在已确认前缀内）幂等确认；乱序/空洞块拒绝并回送期望偏移，
  已确认前缀保留；半份数据在任何时刻都不会进入状态机。
- 安装前再次检查快照点：追赶期间本地压缩/更新会话若已推进到更前位置，
  旧会话结果被忽略，已对外成功的数据不丢、不回退。

### 5.3 进度交叠与追赶活性

- 追赶期间集群正常读写与压缩，发送端快照可能不断推进。
  接收端只接受**同任期快照点不旧**或**更新任期**的会话；旧领导者迟到的
  旧会话块以 `snapshot_stale` 拒绝，不会覆盖更新进度。
- 发送端仅在对端**尚未确认任何字节**且本地快照已更新时放弃旧会话改用新快照；
  已有确认进度的会话坚持传完（安装后再由正常日志复制/下一轮快照补齐尾部），
  避免持续写入下"每次换到更新快照就从头"的追赶活锁。
- 分块会话完成后发送端推进 `match/next`，立刻回到 AppendEntries 路径。

### 5.4 用户侧 API

```go
// 查询追赶进度（经事件循环，视图一致）
st := node.CatchupStatus()
// st.State:
//   CatchupIdle      无追赶任务
//   CatchupReceiving 正在接收（st.Received/st.Total 为已确认字节进度）
//   CatchupDone      最近一次追赶完成、已追平
//   CatchupFailed    失败（如 CRC 不匹配/落盘错误，st.LastError 有原因）
//   CatchupCancelled 被取消或被更新进度取代
st.Ready() // Idle 或 Done 视为就绪

// 取消当前在途接收（丢弃部分数据、状态置 Cancelled）；返回是否存在在途会话。
// 取消只中止这一次，不阻止领导者之后重新发起；半份数据不会被安装。
node.CancelCatchup()
```

### 5.5 容量、兼容与异常行为边界

- **容量**：传输额外开销为对端一份在途会话内存（整份快照字节缓存，易失）
  与接收端一份部分数据磁盘文件；检查点每块一次原子写+fsync。
  块越小请求数越多、每块 fsync 开销越大；块越大单次重传越贵。默认 64KB。
- **兼容**：
  - 快照 ≤ `SnapshotChunkSize` 时仍走一次性 `InstallSnapshot`，旧路径与旧测试不变；
  - 升级节点若磁盘上只有旧格式正式快照，照常恢复；收到大快照时自动启用分块；
  - `snapshot.incoming*` 是新文件，旧版本地数据无需清理即可启动。
- **取消/失败**：`CancelCatchup` 或校验失败会删除部分数据并标记
  `Cancelled/Failed`；领导者随后重新发起整轮传输，不影响正确性。
- **换主**：新旧领导者快照一致则同会话续传；不一致则从新领导者的新会话重传，
  旧会话的已确认前缀不再复用（内容不同，不可复用）。

---

## 6. 可配置项（`raft/config.go`）

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `ID` | 必填 | 本节点 ID |
| `Peers` | 必填 | 集群全部节点（含自己），集群成员变更未实现，成员静态 |
| `DataDir` | 空=纯内存 | 持久化目录；崩溃重启测试必须非空 |
| `TickInterval` | 10ms | 逻辑时钟周期，即心跳间隔 |
| `ElectionMin` / `ElectionMax` | 10 / 20 ticks | 随机化选举超时范围，避免分票活锁 |
| `SnapshotThreshold` | 50 | 快照之后增量日志达到该条数即压缩 |
| `SnapshotChunkSize` | 64KB | 大快照分块传输的块大小；快照不超过该值走一次性 InstallSnapshot |
| `RandSeed` | 0=时间随机 | 固定种子可让选主/抖动序列**可复现** |

客户端侧（`raft/client.go`）：重试间隔 5ms、单次尝试超时 100ms；
同一逻辑请求在所有重试中保持相同 `(ClientID, Seq)`。

模拟网络（`raft/transport.go` 的 `FaultPolicy`）：

- `DropRate`：按概率丢弃请求；`Duplicate`：请求送达两次；`Reorder`：注入随机延迟造成乱序；
- `SetPartition(id,true)`：对某节点双向 blackhole；
- `MemoryTransport.Seed(seed)`：固定故障随机源。

---

## 7. 故障假设与覆盖范围

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
| `TestChunkedSnapshotBasic` | 大快照分多块传输，进度 Receiving→Done，收齐后才安装 |
| `TestChunkedSnapshotResumeAfterReceiverRestart` | 接收端传一半崩溃重启，从已确认偏移续传（检查点持久化） |
| `TestChunkedSnapshotDuplicateAndReorder` | 分块方向丢包/重复/乱序下幂等收敛、数据完整 |
| `TestChunkedSnapshotResumeAcrossLeaderChange` | 传输中领导者崩溃，新领导者同会话续传完成 |
| `TestChunkedSnapshotConcurrentWritesAndCompaction` | 传输中并发写入+持续压缩（进度交叠），最终包含全部已提交数据 |
| `TestChunkedSnapshotCancel` | 中途取消：半份数据不安装、状态 Cancelled、重试后完成 |
| `TestChunkedSnapshotRestartAfterCompletion` | 追赶完成后重启：无残留会话、提交不回退、数据完整 |
| `TestSmallSnapshotStaysLegacy` | 小快照仍走一次性路径，不分块（向后兼容） |
| `TestChunkReceiverOutOfOrderAndDuplicate` | 单元级：乱序/重复块处理、半份数据不可见 |
| `TestChunkReceiverInterleavedSessions` | 单元级：不同进度会话交错，旧会话不覆盖新进度 |
| `TestChunkReceiverSupersededMidSession` | 单元级：接收中途被更新会话取代，旧会话迟到块被拒 |
| `TestChunkReceiverCRCFailure` | 单元级：CRC 失败标记 Failed 且不安装，新会话可恢复 |
| `TestConcurrentReadWriteSnapshot` | 并发读写+压缩（`-race`）无数据竞争，最终一致 |
| `TestClientRetriesAcrossLeaderChange` | 领导者连续崩溃，客户端自动换主重试不丢不重 |
| `TestCommittedValueLinearizable` | 并发读不返回脏值 |

测试日志统一打印节点 **角色 / 任期 / 提交位置 / 应用位置 / 快照位置 / 判定依据**
（拒绝时会额外打印 `reason` 与冲突信息）。

**未覆盖 / 刻意简化：**

- 集群成员变更（membership change）：成员列表静态；
- 日志条目按 JSON 存储、整段重写（教学清晰优先，未做分段/批量 pipeline/真实网络）；
- 小快照为一次性整体安装；大快照已支持分块 offset 续传（见第 5 节），
  但状态机快照仍是一次性序列化的整份字节、发送端会话缓存整份快照；
- 无拜占庭容错、无预投票（Pre-Vote 不影响安全性，仅改善被隔离节点恢复时的扰动）。

---

## 8. 本地验证建议

```bash
# 1) 确定性：固定种子反复运行结果应一致
go test ./... -count=20

# 2) 并发安全：长时间 race
go test ./... -race -count=20

# 3) 只跑大快照分块续传相关用例（普通 + race）
go test ./raft/ -run 'TestChunkedSnapshot|TestChunkReceiver|TestSmallSnapshot' -v
go test ./raft/ -race -run 'TestChunkedSnapshot|TestChunkReceiver|TestSmallSnapshot'

# 4) 端到端演示（使用真实临时目录做持久化，退出自动清理；第 8 步演示分块进度）
go run ./cmd/demo
```

分块续传的可复现验证要点（均由上述测试自动化）：

- 用很小的 `SnapshotChunkSize`（测试中为 64 字节）+ 大值写入强制多块传输；
- 用 `Cluster.Flaky(leader, laggard, RPCInstallSnapshotChunk, FaultPolicy{...})`
  对分块方向单独注入丢包/重复/乱序；
- 轮询 `Node.CatchupStatus()` 观察 `Received/Total`，等到部分进度后
  `Crash`+`Restart` 接收端（或 `Crash` 领导者触发换主），再恢复网络验证续传；
- 最终断言：`CatchupStatus().State == CatchupDone`、`Received == Total`、
  状态机包含全部已确认提交的键；重启后状态为 `Idle` 且提交位置不回退。

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

