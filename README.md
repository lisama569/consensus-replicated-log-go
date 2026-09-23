# consensus-replicated-log-go

一个可本地运行的多节点复制日志一致性协议（Raft 风格）实现，位于 `raft/` 包。
集群在节点故障、网络分区与请求重试下仍能得到唯一且可复现的提交结果。

## 协议状态

每个节点维护：

| 状态 | 说明 | 持久化 |
|---|---|---|
| `term` / `votedFor` | 当前任期与本任期投票记录（`-1` 表示未投票） | 是（HardState） |
| `commitIndex` | 已提交位置 | 是（HardState） |
| `log` | 带快照基点 `(baseIndex, baseTerm)` 的日志序列 | 是（Entries + Snapshot） |
| `role` | Follower / Candidate / Leader | 否 |
| `nextIndex` / `matchIndex` | 领导者复制的进度 | 否 |
| `dedup` / `pending` | 幂等表：`clientID -> requestID -> 结果` | 是（随快照） |
| `kv` | 状态机（覆盖写语义，返回旧值） | 是（随快照） |

## 不变式

1. **选举安全**：同一任期最多一个领导者；赢得选举需多数派投票，且每节点每任期只投一票（重复投票以 `ALREADY_VOTED` 拒绝）。
2. **任期单调**：`term` 只增不减；来自旧任期的消息一律以 `STALE_TERM` 拒绝；发现更高任期立即退为跟随者。
3. **日志匹配**：`AppendEntries` 携带 `(prevLogIndex, prevLogTerm)`，不一致以 `LOG_MISMATCH` 拒绝并回退 `nextIndex` 重试；冲突条目先截断后追加。
4. **提交安全**：只有多数派复制且属于**当前任期**的条目才推进 `commitIndex`；少数派分区无法提交新日志。
5. **状态机安全**：`lastApplied <= commitIndex`，条目按下标顺序应用，已提交内容永不回退。
6. **幂等写入**：`clientID + requestID` 构成幂等键；重复请求以 `DUPLICATE` 拒绝并返回首个结果，不产生重复写入。
7. **快照一致**：快照只压缩 `<= commitIndex` 的前缀；快照点之后的状态机与提交位置在重启后保持一致。

## 可配置项（`raft.Config`）

| 字段 | 含义 |
|---|---|
| `ID`, `Peers` | 节点 ID 与集群成员 |
| `ElectionTimeout` | 选举超时（tick 数；实际为 `ElectionTimeout + ID`，确定性错峰避免分裂投票） |
| `HeartbeatTimeout` | 心跳间隔（tick 数） |
| `Storage` | 持久化接口（内置 `MemoryStorage`，崩溃重启后数据保留） |
| `Transport` | 消息投递接口（内置可控故障的 `Network`） |

## 故障假设与注入

- **崩溃重启**：`MemoryStorage` 模拟磁盘；崩溃 = 丢弃内存态，重启 = 用同一 `Storage` 调 `NewNode`，从快照 + 日志 + HardState 恢复。
- **网络分区**：`Network.Partition(a, b)` 双向丢弃组间消息；`Heal()` 恢复。
- **消息重复**：`SetDuplicate` 重复投递，协议层幂等处理。
- **消息乱序**：`SetDelay` 按链路延迟 tick 数，不同延迟产生乱序。
- **拒绝原因可区分**：`RejectReason` 枚举 —— `NOT_LEADER` / `STALE_TERM` / `ALREADY_VOTED` / `LOG_MISMATCH` / `DUPLICATE` / `NO_QUORUM` / `SNAPSHOT_BEHIND`。

## 本地验证

```sh
go test ./raft/ -v          # 全部功能测试（日志打印角色/任期/提交位置/判定依据）
go test ./raft/ -race       # 并发读写与压缩的数据竞争检测
go test ./raft/ -count=5    # 重复运行验证结果可复现
```

测试全部由逻辑时钟（`Tick`）驱动，无真实睡眠与随机数，结果完全可复现。覆盖场景：

- `election_test.go`：唯一领导者、少数派分区不得提交、过期任期拒绝、重复投票拒绝、分区恢复后收敛。
- `replication_test.go`：多数派提交、幂等重试、落后节点追赶、崩溃重启持久性、消息重复与乱序、非领导者拒绝。
- `snapshot_test.go`：快照后重启一致性、严重落后节点快照追赶、并发读写与压缩（需 `-race`）。
