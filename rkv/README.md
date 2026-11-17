# rkv — Raft 分布式键值存储

面向城市燃气远程抄表 / 阀控场景的一致性存储。单机库一旦宕机全城阀控停摆，
主从又可能脑裂发出相反阀门指令；本项目用 **Raft 多副本 + 范围分片** 替代单机库，
并自带**确定性故障模拟器**与**线性一致性检查器**，可以跑几万次随机故障自证正确。

## 八个需求点与实现位置

| # | 需求 | 实现 |
|---|------|------|
| 1 | 选主/日志复制/任期/投票按论文，分区时少数派不能写 | `src/raft/node.rs`：Pre-Vote 防断网节点扰动、论文 §5.2 投票资格（日志新旧比较）、§5.4 只提交本任期多数派条目、candidate 平票超时重选 |
| 2 | 日志快照、截断、落后节点装快照追赶 | `RaftNode::do_snapshot` / `handle_install_snapshot`，WAL 前缀截断，`src/storage.rs` 快照带 CRC |
| 3 | 成员变更用联合共识，加减节点全程可用 | `EnterJoint`/`LeaveJoint` 两条配置日志，`has_quorum` 在 joint 期要求 C_new 与 C_old 双多数派（`src/raft/node.rs`、`propose_conf_change`） |
| 4 | 线性一致读：ReadIndex 与 Lease 两种 | `client_read`：ReadIndex 走多数派确认轮并定格 readIndex、等 applied 追平；Lease 靠租约时钟，过期自动回退 ReadIndex。时钟前提见下节 |
| 5 | 范围分片、每片一个 Raft 组、自动分裂 | `src/shard/`：`ShardManager` 路由、`SplitPlan` 中位数分裂、meta Raft 组一致复制路由表、`driver.rs` 完整分裂编排 |
| 6 | 客户端自己找主，主切换期等待不报错 | `src/runner.rs::RaftClient` 与 `src/sim/mod.rs::retry_client_ops`：NotLeader 后按提示重试，超过总预算才报错 |
| 7 | 磁盘损坏可检测、不带病投票 | `src/storage.rs` 全量 CRC32（WAL 帧 / hardstate / snapshot），`verify_integrity` 失败即构造 `Role::Faulted` 节点，拒绝一切协议交互 |
| 8 | 页面看分片成员、任期历史、选主耗时 | `src/dashboard.rs` + `dashboard.html`，五个标签：节点状态 / 日志复制 / 分片 / 快照 / 一致性检查 |

## 租约读（LeaseRead）的时钟前提

ReadIndex 只依赖 Raft 任期与多数派，**没有任何时钟假设**。

Lease 读更快（跳过确认轮），但要求：

1. 每个节点的本地时钟**单调不减**（不能回拨，VM 迁移/NTP step 要禁用负跳变）；
2. 节点间时钟漂移有上界 Δ；
3. 配置满足
   `lease_duration + Δ < election_timeout_min − heartbeat_interval`。

这样旧 leader 感知到的租约，一定早于任何 follower 因选举超时而投票给新 leader 的时刻，
旧 leader 在租约内不可能与新 leader 并存服务写/读。代码里租约一旦过期，读请求
自动退回完整 ReadIndex，绝不用过期租约服务。生产环境若不能约束时钟，应只用 ReadIndex。

## 模拟层与可复现

`src/sim/` 用逻辑毫秒时钟 + 纯状态机驱动同一个 `RaftNode`（与真实 TCP 路径共用代码）：

- 可控网络：随机丢包、重复、延迟、网络分区、节点停机/重启；
- 磁盘故障：翻转 hardstate/snapshot、截断 WAL 尾帧、删除 CRC 旁车文件；
- 所有随机数来自一个 `Pcg64(seed)`，**同种子逐帧复现**；
- 在途消息按 `(deliver_at, seq)` 全局有序（`BTreeMap`），杜绝队首阻塞。

线性一致性检查器 `src/checker.rs` 是自写的 Wing–Gong 回溯 + 记忆化实现，
按 key 分组，支持并发在途请求（只对存在 happens-before 的同会话操作强制顺序），
带搜索预算保护，纯标准库。

## 命令行

```bash
# 构建
cargo build --release

# 本机 5 节点进程内集群 + 状态页面（http://127.0.0.1:8080）+ 冒烟读写
cargo run -- dev-cluster --nodes 5 --smoke

# 单个确定性故障场景（种子可复现），跑完做线性一致性裁决
cargo run -- sim --seed 42 --nodes 5 --duration-ms 20000

# 一次跑 200 个种子；任一失败会落 failure-seed-N.json 证据并以非零码退出
cargo run -- sim --seed 1 --count 200

# 租约读模式
cargo run -- sim --seed 1 --count 50 --lease

# 分片 + 自动分裂模拟，页面看分片与分裂历史
cargo run -- shard-sim --split-threshold 20 --ops 400

# 对导出的操作历史离线检查
cargo run -- check --file history.json
```

## 测试

```bash
cargo test                 # 单元 + 集成（含 12 个随机种子的线性一致性裁决）
```

集成测试 `tests/raft_core.rs` 覆盖：基础复制、少数派分区不可写、leader 宕机不丢
已提交数据、快照压缩与慢节点追快照、联合共识加减节点、磁盘损坏拒绝投票、
同种子确定性重放、ReadIndex/Lease 两种读；`tests/sharding.rs` 覆盖自动分裂、
路由不重叠覆盖、meta 组路由持久化。

## 目录结构

```
src/
  storage.rs          CRC 校验的 WAL / hardstate / snapshot
  raft/               Raft 纯状态机（node/types/state_machine）
  shard/              范围分片、分裂编排、meta 组
  sim/                确定性模拟网络 + 随机故障场景
  checker.rs          自写线性一致性检查器
  runner.rs           真实 TCP / 进程内传输 + 客户端
  cluster.rs          本地集群与 dashboard 数据聚合
  dashboard.rs(.html) 集群状态页面
```
