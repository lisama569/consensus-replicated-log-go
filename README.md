# 多节点复制日志一致性协议（Raft 实现）

一个**零第三方依赖**、可在本地完整运行的 Raft 复制日志实现（Go 标准库 + 进程内模拟网络）。
覆盖选主、任期管理、日志复制、幂等写入、日志压缩/快照、**可恢复的大快照分块续传追赶**、
崩溃恢复，并提供可注入丢包/重复/乱序/分区的测试床与确定性随机种子，便于可复现地验证。

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
| `raft/election.go` | 选举：Pre-Vote 预投票、拉票、投票判定、得票统计、更高任期让位 |
| `raft/replication.go` | AppendEntries 收发（停等窗口/强制心跳）、冲突快速回退、全条目对账、多数派提交、状态机应用、读屏障 |
| `raft/log.go` | 日志布局（快照占位项 + 后缀）、索引换算与修改器、按 tick 合并后台落盘 |
| `raft/snapshot.go` | 压缩（takeSnapshot）、快照单调安全安装、旧版整体安装兼容、快照分块发送 |
| `raft/snapshot_transfer.go` | 可恢复大快照：滑动窗口发送会话（钉住快照）、跟随者乱序缓存/校验/异步落盘/提升、后台 store worker、进度视图、取消/恢复 API |
| `raft/staging_store.go` | 接收端临时快照（staging）的原子元数据与分块追加落盘（fsync+读写锁），崩溃后续传 |
| `raft/statemachine.go` | KV 状态机：幂等 Apply、Lookup、JSON 快照/恢复（RWMutex） |
| `raft/store.go` | 持久化：硬状态、日志后缀、快照；原子写（tmp+fsync+rename）；读写锁串行化磁盘访问 |
| `raft/transport.go` | `MemoryTransport`：模拟网络，支持丢包/重复/乱序/延迟/分区，处理器代次防挂死 |
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
| `resync` | 分块快照：无匹配临时快照/出现空洞，要求从断点或 0 重发 |
| `transfer_failed` | 分块快照：校验和不符/落盘失败，临时快照已丢弃 |
| `cancelled` | 快照追赶已被用户取消 |

---

## 4A. 可恢复的大快照追赶（分块续传 InstallSnapshot）

旧实现的快照是「一次性整体安装」：状态机一大，弱网丢包、超时、节点重启或领导权变化
都会让整份快照重传。新实现把快照切成固定大小的块，支持**断点续传**，并且在追赶期间
集群仍可正常读写与压缩。

### 4A.1 协议

领导者把快照切成 `SnapshotChunkSize`（默认 64KiB）大小的块，每块都携带完整快照的
**身份**与偏移：

```
身份 = (LastIncludedIndex, LastIncludedTerm, Length, SHA-256(整份快照))
InstallSnapshotRequest{Chunked, TransferID, Offset, Length, Checksum, Data, Done}
InstallSnapshotResponse{Success, AckOffset, Complete, Reason(resync/transfer_failed/cancelled)}
```

- **断点续传（滑动窗口）**：领导者对每份快照维护一个有界滑动发送窗口
  （默认 `snapshotWindow=8` 块），块在途即可继续发送，不必逐块停等。跟随者把
  连续块 `O_APPEND + fsync` 追加到 `snapshot.staging.data`，元数据原子写入
  `snapshot.staging.json`；丢包/超时/重启后领导者据跟随者回报的连续 durable
  断点 `AckOffset` 推进窗口并补齐缺失块，绝不从头重来。
- **重复块**：偏移落在已收区间内，幂等 ACK 当前断点，不重复落盘。
- **乱序/空洞块**：滑动窗口内（最多 `snapshotWindow` 个块）的小块进入有界
  乱序缓存，空洞补齐后按序落盘；超出窗口或出现真正分叉则回报 `resync`，
  绝不无界缓存、绝不拼接两份不同快照。
- **身份变化**：领导者**钉住（pin）**正在发送的那份快照——即使追赶期间它又压出
  了更新的快照，也先把当前这份完整传完、跟随者装完后再开新会话，从根本上避免
  「负载不停 → 会话不断作废 → 字节反复掉回 0 → 永远追不平」。
  跟随者侧身份不同（末索引/任期/长度/校验和任一变化）则要求从新快照首块重发。
- **原子提升**：只有收齐全部字节且重新读出整文件、SHA-256 校验通过，临时快照才会被
  原子提升为正式快照并恢复状态机；此前**半份数据对读取完全不可见**。
  校验不符/落盘错误 -> 丢弃整份临时数据、状态 `failed`，领导者自动重传。
- **状态机单调不回退**：安装快照是「只前推」操作；领导权切换后若收到不新于本地
  已应用位置的旧快照，只前移快照/日志边界、绝不 `Restore` 旧字节，已提交数据不丢。

### 4A.2 追赶期间的一致性保证

- 已对客户端返回成功的数据**不丢、不回退**：快照点 ≤ 提交点，安装快照只前推
  commit/apply；快照点之后仍连续匹配的日志后缀会被保留并继续应用。
- 不存在「进度显示追上但内容不全」：`caught_up` 只在校验通过并完成状态机恢复后置位；
  临时快照在提升前，状态机仍是旧内容，任何读都看不到半成品。
- 领导权变化、重连后收到不同进度的重发内容：无论到达顺序如何，最终状态机包含所有
  已确认提交的数据（身份校验 + 提交点单调 + 快照后常规 AppendEntries 补齐）。
- 是否需要快照以领导者侧 `matchIndex < 快照点` 为准（而非易受回退影响的
  nextIndex），避免落后节点在空日志/旧前缀情况下陷入 AppendEntries 拒绝风暴。

### 4A.3 用户侧进度与控制 API

```go
// 本节点作为接收方的追赶进度（也包含在 Status().CatchUp 中）
p := node.CatchUpProgress()
// p.State:  idle | syncing | caught_up | failed | cancelled
// p.Received/p.Total 字节进度；p.Ready() 表示快照已完整安装、剩余由常规复制补齐

// 领导者侧：各落后 peer 的发送进度
transfers := leader.SnapshotTransfers() // map[NodeID]CatchUpProgress

node.CancelCatchUp() // 取消进行中的追赶（拒绝后续块，半份数据不可见）
node.ResumeCatchUp() // 解除取消；领导者重发后继续/重新完成
```

| 状态 | 含义 |
| --- | --- |
| `idle` | 没有进行中的追赶 |
| `syncing` | 正在接收分块（`Received/Total` 为字节进度） |
| `caught_up` | 快照已校验并安装就绪，剩余日志由常规复制补齐 |
| `failed` | 校验/落盘失败，临时数据已丢弃；领导者会自动重传，无需人工干预 |
| `cancelled` | 被 `CancelCatchUp` 取消；`ResumeCatchUp` 后重新开始 |

### 4A.4 异常与取消时的行为

- **节点崩溃/重启**：临时元数据与数据文件是 fsync 的；重启后恢复断点进度继续接收，
  但绝不直接安装未完成的临时快照。全部完成后再次重启，从正式快照恢复，状态一致。
- **取消**：取消只是本机状态位，幂等且可重复；被取消期间领导者收到 `cancelled` 会
  有限退避重试，不会永久卡死，`ResumeCatchUp` 后首个正常 ACK 立即全速续传。
- **磁盘空间/IO 错误、字节损坏**：进入 `failed`，临时数据被清理，等待自动重传。

### 4A.5 容量与兼容范围

- 快照大小不受单条 RPC 消息限制：分块用于传输与接收落盘；
  配置了 `DataDir`（落盘模式）时，接收端完整快照落在临时文件，追赶期间堆占用约为
  「一个块」。纯内存模式（`DataDir=""`，仅供不需要重启的测试）下临时快照仍在内存。
  另外本实现快照仍由领导者在内存中整体生成一次，这是发送端唯一与快照总大小同阶的内存项。
- 块越大 RPC 数越少但单次重传越粗；默认 64KiB，可按 `SnapshotChunkSize` 调整。
- **向后兼容，升级无需清库**：
  - 旧版 `snapshot.json` 正式快照可直接被新版本恢复；
  - 不带分块字段的旧版一次性 `InstallSnapshot`（`Chunked=false`）仍被支持，
    便于新旧版本节点在滚动升级期间互通；
  - 残留的损坏/旧版本 staging 文件会在启动时被忽略并清理，不影响正常恢复。

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
| `SnapshotChunkSize` | 64KiB | 大快照分块传输的每块字节数 |
| `SnapshotRetryTicks` | 2 ticks (20ms) | 未收到块确认时重发缺失/在途块的间隔（tick 数） |
| `SnapshotMinGapTicks` | 0（不节流） | 两次压缩之间的最小 tick 间隔；持续高频小写入+大状态机时可调大以限制压缩 CPU |
| `RandSeed` | 0=时间随机 | 固定种子可让选主/抖动序列**可复现** |

发送侧固定使用 `snapshotWindow=8` 的滑动窗口（块数，非字节），接收侧用同样大小的
有界乱序缓存；二者配合让丢包/乱序只重传缺失块而不是整份快照。

客户端侧（`raft/client.go`）：重试间隔 5ms、单次尝试超时 100ms；
同一逻辑请求在所有重试中保持相同 `(ClientID, Seq)`。

模拟网络（`raft/transport.go` 的 `FaultPolicy`）：

- `DropRate`：按概率丢弃请求；`Duplicate`：请求送达两次；`Reorder`：注入随机延迟造成乱序；
- 新增 `Delay`：可在发送前注入额外延迟（模拟慢链路），与 `DropRate/Reorder/Duplicate` 可叠加；
- `SetPartition(id,true)`：对某节点双向 blackhole；
- `MemoryTransport.Seed(seed)`：固定故障随机源。

---

## 5. 非阻塞磁盘 I/O 与事件循环

原始实现里每条日志追加、每次压缩都在 Raft 事件循环线程内同步执行多次 fsync
（tmp+fsync+rename+目录 fsync）。在真实慢盘上，一次 fsync 可能数毫秒，持续写入 +
压缩 + 大快照追赶时，事件循环会被 fsync 占满，导致：心跳/投票处理被饿死、追赶中的
跟随者反复选举超时、快照块收发停滞。新架构把磁盘 I/O 移出事件循环，同时保持
「所有可变状态只在事件循环上修改」的并发模型：

- **控制面 / 数据面双队列事件循环**：快照数据块 RPC 走独立的高优先 `chunkEvents`
  队列，心跳/投票/客户端请求等控制消息走低优先 `events` 队列；事件循环每轮优先排空
  控制面，再批量处理数据块。这样即使大量块在途，心跳与选举也不会被饿死。
- **每个节点两个串行落盘 worker**：
  - `chunkJobs`（高优先）：仅处理接收快照的 staging 文件
    （reset/append-fsync/finalize-verify/clear），保证慢盘上追赶不被普通落盘拖死；
  - `persistJobs`（低优先）：处理正式快照落盘、日志后缀重写，以及停止前的刷盘屏障。
  - 两类资源（staging 文件 vs 正式 snapshot/log）互不相交，因此两条队列无需跨序；
    正式快照「快照字节 + 日志后缀」在同一 worker 内严格先快照后日志地串行完成，
    并由 `formalWriteMu` 保护，发送方读快照在同一把读锁上，读不到两步之间的中间态。
- **协议判定留在事件循环，fsync 留在 worker**：接收方对每个块先在事件循环内完成
  校验/去重/窗口管理，只把磁盘追加交给 worker；ACK 的连续断点是「已 fsync」的位置。
  worker 完成事件再回到事件循环提交，状态修改因此仍然串行无锁。
- **日志按 tick 合并落盘**：高频写入时把「每条日志一次整文件重写+fsync」降为
  「每心跳最多一次」（`logDirty` 标记），避免写放大打爆 worker 反过来卡住循环。
- **Pre-Vote**：追赶中的落后节点用预投票探测多数派，日志明显追不上时不会先递增
  任期、不会用虚假高任期扰动在任领导者（详见 §4A 与 `election.go`）。
- **AppendEntries 停等窗口 + 强制心跳**：每条普通 AE 在途时不重复洪泛（
  `aeInflight`），但每个 tick 的心跳强制发送，使窗口在丢包/领导权切换后能自愈，
  不会因一个丢失的在途标记把某个 peer 永久卡住。
- **崩溃安全刷盘**：`Stop()`（测试中的模拟崩溃）先经事件循环投递屏障任务，等待
  此前所有已提交日志/快照 fsync 完成后再关闭；重启时若发现「新快照 + 尚未重写的
  旧后缀」这种崩溃窗口残留，会丢弃已被快照覆盖的旧后缀而不是报错。

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
| `TestSnapshotCatchUp` | 长期离线节点经 InstallSnapshot（分块路径）追上并继续复制 |
| `TestChunkedSnapshotResumeAfterDrops` | 大快照丢块后滑动窗口续传（不从头），字节进度与发送侧视图可查询 |
| `TestChunkedSnapshotResumeAcrossRestart` | 传一半节点崩溃重启，临时快照续传；完成后再重启仍一致 |
| `TestChunkedSnapshotDuplicateReorderInterleave` | 重复/乱序/丢块 + 传输期间并发写入与压缩，身份交错后仍收敛且不丢已提交数据 |
| `TestChunkedSnapshotLeadershipChange` | 追赶期间领导权切换、健康节点选新主、不同进度重发，最终包含全部已提交数据 |
| `TestChunkedSnapshotCancelAndResume` | 慢盘落盘下取消：cancelled 态、半成品不可见、不虚报就绪；恢复后续传完成 |
| `TestChunkedSnapshotCorruptionUnit` | 字节损坏触发校验失败、丢弃临时数据、failed 态、首块重传成功 |
| `TestChunkedSnapshotDuplicateAndGapUnit` | 处理器级：重复块幂等、窗口内乱序缓存、超窗空洞 resync、补齐后安装 |
| `TestChunkedSnapshotCatchUpUnderContinuousLoad` | **大状态机 + 持续写入压缩 + 持续概率丢块 + 慢盘 fsync**：负载期间连续装完多份前进快照，停写后追平且全部已提交数据齐全（核心回归） |
| `TestChunkedSnapshotStagingBytesMonotonicPerIdentity` | 持续丢块/压缩/重启下，同一会话身份内已 fsync 字节只增不减（禁止字节回 0） |
| `TestChunkedSnapshotCatchUpDropTimeoutAndRestart` | 多轮「收到部分块→崩溃→重启」，已确认断点保留、最终追平 |
| `TestLegacyOneShotSnapshotStillWorks` | 旧版一次性 InstallSnapshot 仍可安装（新旧版本互通） |
| `TestUpgradeFromOldSnapshotOnDisk` | 磁盘只有旧格式快照时新版本正常恢复，无需清库 |
| `TestConcurrentReadWriteSnapshot` | 并发读写+压缩（`-race`）无数据竞争，最终一致 |
| `TestClientRetriesAcrossLeaderChange` | 领导者连续崩溃，客户端自动换主重试不丢不重 |
| `TestCommittedValueLinearizable` | 并发读不返回脏值 |

测试日志统一打印节点 **角色 / 任期 / 提交位置 / 应用位置 / 快照位置 / 判定依据**
（拒绝时会额外打印 `reason` 与冲突信息）。

**未覆盖 / 刻意简化：**

- 集群成员变更（membership change）：成员列表静态；
- 日志条目按 JSON 存储、整段重写（教学清晰优先，未做分段/批量 pipeline/真实网络）；
- 无拜占庭容错、无预投票（Pre-Vote 不影响安全性，仅改善被隔离节点恢复时的扰动）。

---

## 7. 本地验证建议

```bash
# 1) 确定性：固定种子反复运行结果应一致
go test ./... -count=20

# 2) 并发安全：长时间 race
go test ./... -race -count=20

# 3) 端到端演示（使用真实临时目录做持久化，退出自动清理）：
#    末尾会打印大快照 syncing(字节进度) -> caught_up(就绪) 的追赶过程
go run ./cmd/demo

# 4) 只跑可恢复大快照续传相关用例（含中断续传、重启续传、重复/乱序/交错、
#    取消/恢复、损坏检测、领导权切换、旧版兼容、持续负载压力）
go test ./raft/ -run TestChunkedSnapshot -v
go test ./raft/ -run 'TestLegacyOneShotSnapshotStillWorks|TestUpgradeFromOldSnapshotOnDisk' -v
```

若要手工实验：

```go
c, _ := raft.NewCluster(5, true /*持久化*/, 1234)
defer c.Stop()
// <- 等待 c.Leader()
c.SetSnapshotChunkSizeAll(64) // 调小分块，便于在小状态机上观察多块/窗口续传
cli := raft.NewClient("my-client", /*nodes...*/)
cli.Set(ctx, "k", "v")                 // 多数派提交后返回
c.Isolate(c.Leader())                  // 制造分区
c.Flaky("n1", "n2", raft.RPCAppendEntries, raft.FaultPolicy{DropRate: 0.5, Duplicate: true})

// 对快照分块注入确定性故障（丢块/慢链路），复现窗口重传与断点续传：
c.SetSnapshotHook(&raft.SnapshotFaultHook{
    Drop: func(from, to raft.NodeID, req *raft.InstallSnapshotRequest) bool {
        return req.Chunked && req.Offset > 0 // 丢块，观察滑动窗口据 AckOffset 补齐
    },
    // Delay: func(...) time.Duration { return 30 * time.Millisecond }, // 模拟慢链路
})
// 模拟落后节点的慢盘 fsync（验证 fsync 不阻塞心跳/选举，追赶仍能推进）：
node.SetChunkIODelay(3 * time.Millisecond)
// ... 恢复：c.Heal(id) / c.HealAll() / c.SetSnapshotHook(nil)
// 查询追赶进度：node.CatchUpProgress()，或 node.CancelCatchUp()/ResumeCatchUp()
```

> 提示：追赶类压力测试在 `-race` 下会因 fsync/调度变慢而使用 race 感知的更宽松
> 超时（测试内部 `wd()/writerPace()` 自动放大），手工复现慢盘+持续写入时建议同样
> 给写入加节流（如每 4~8ms 一条），使快照推进不超过链路可追平的速度——这是任何
> 复制协议在「写入无限快、带宽/磁盘有限」下都成立的物理前提，而非协议缺陷。

