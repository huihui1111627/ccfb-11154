//! 确定性模拟层：可控网络（丢包/重复/延迟/分区）、节点重启、磁盘损坏注入。
//!
//! 所有随机数都来自集群创建时的一个 Pcg64（由 `seed` 播种），网络抽样、
//! 故障场景调度、各 Raft 节点内部随机源全部派生自它。因此**同一种子重放
//! 得到逐字节一致的执行轨迹**，出问题可用种子一键复现。
//!
//! 时间是逻辑毫秒，按固定 tick 推进；节点不持有线程，每个 tick 把到期消息
//! 喂给 [`crate::raft::RaftNode::advance`]，与真实 TCP 驱动共用同一份状态机。

pub mod scenario;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;

use crate::checker::Op;
use crate::raft::state_machine::KVStateMachine;
use crate::raft::{
    ClientId, Command, ConfState, Message, NodeId, RaftConfig, RaftNode, ReadMode, ReadRequest,
    Role, Term, TermEvent,
};
use crate::storage::{CorruptionKind, RaftStore, StoreError};

pub type GroupId = u64;
pub type NodeKey = (NodeId, GroupId);

#[derive(Debug, Clone)]
pub struct SimConfig {
    pub tick_ms: u64,
    pub loss_pct: u32,
    pub duplicate_pct: u32,
    pub delay_min_ms: u64,
    pub delay_max_ms: u64,
    pub seed: u64,
}

impl Default for SimConfig {
    fn default() -> Self {
        SimConfig {
            tick_ms: 10,
            loss_pct: 10,
            duplicate_pct: 2,
            delay_min_ms: 5,
            delay_max_ms: 40,
            seed: 1,
        }
    }
}

#[allow(dead_code)] // deliver_at/seq 与排序键冗余，保留以便调试可读
struct Inflight {
    deliver_at: u64,
    seq: u64,
    from: NodeId,
    to: NodeId,
    group: GroupId,
    msg: Message,
}

/// 在途消息的排序键：先按到期时间，同刻按全局序号（保证确定性 FIFO）。
type InflightKey = (u64, u64);

struct PendingClientOp {
    client_id: u64,
    group: GroupId,
    call_time: u64,
    msg: Message,
    /// 下一次尝试时间（主切换期退避，但不报错）
    next_try: u64,
    key: Vec<u8>,
    /// 已投递过的节点：同一请求不向同一节点重发，避免重复进日志。
    /// leader 切换（收到 NotLeader 提示）时清空，允许投给新主。
    tried: std::collections::HashSet<NodeId>,
}

/// 节点视图（页面/断言用的只读快照）。
#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeView {
    pub node_id: NodeId,
    pub group: GroupId,
    pub alive: bool,
    pub role: String,
    pub term: u64,
    pub leader: Option<NodeId>,
    pub commit: u64,
    pub applied: u64,
    pub last_log_index: u64,
    pub log_len: u64,
    pub snapshot_index: u64,
    pub in_joint: bool,
    pub voters: Vec<NodeId>,
    pub fault: Option<String>,
}

pub struct SimCluster {
    pub now: u64,
    cfg: SimConfig,
    raft_cfg: RaftConfig,
    rng: Pcg64,
    base_dir: PathBuf,
    nodes: BTreeMap<NodeKey, RaftNode>,
    alive: BTreeSet<NodeKey>,
    node_seeds: BTreeMap<NodeKey, u64>,
    dirs: BTreeMap<NodeKey, PathBuf>,
    inflight: BTreeMap<InflightKey, Inflight>,
    /// 在途消息全局自增序号，保证同刻到期消息的确定性顺序
    inflight_seq: u64,
    /// 分区：同一子列表内可达；列表外的节点完全隔离。None = 全互通。
    partitions: Option<Vec<Vec<NodeId>>>,
    loss_pct: u32,
    dup_pct: u32,
    delay_min: u64,
    delay_max: u64,

    // 客户端
    pending: BTreeMap<ClientId, PendingClientOp>,
    leader_hint: BTreeMap<GroupId, Option<NodeId>>,
    next_client_req: ClientId,
    next_internal_req: ClientId,
    history: Vec<Op>,
    event_log: Vec<String>,
    delivered_msgs: u64,
    dropped_msgs: u64,
}

impl SimCluster {
    pub fn new(base_dir: impl AsRef<Path>, cfg: SimConfig, raft_cfg: RaftConfig) -> Self {
        let rng = Pcg64::seed_from_u64(cfg.seed);
        let loss_pct = cfg.loss_pct;
        let dup_pct = cfg.duplicate_pct;
        let delay_min = cfg.delay_min_ms;
        let delay_max = cfg.delay_max_ms;
        SimCluster {
            now: 0,
            base_dir: base_dir.as_ref().to_path_buf(),
            rng,
            cfg,
            raft_cfg,
            nodes: BTreeMap::new(),
            alive: BTreeSet::new(),
            node_seeds: BTreeMap::new(),
            dirs: BTreeMap::new(),
            inflight: BTreeMap::new(),
            inflight_seq: 0,
            partitions: None,
            loss_pct,
            dup_pct,
            delay_min,
            delay_max,
            pending: BTreeMap::new(),
            leader_hint: BTreeMap::new(),
            next_client_req: 1,
            next_internal_req: 1_000_000,
            history: Vec::new(),
            event_log: Vec::new(),
            delivered_msgs: 0,
            dropped_msgs: 0,
        }
    }

    pub fn seed(&self) -> u64 {
        self.cfg.seed
    }

    pub fn history(&self) -> &[Op] {
        &self.history
    }

    pub fn event_log(&self) -> &[String] {
        &self.event_log
    }

    fn log_event(&mut self, s: String) {
        self.event_log.push(format!("t={} {s}", self.now));
    }

    // ---------------- 集群搭建 ----------------

    pub fn add_group(&mut self, group: GroupId, peers: Vec<NodeId>) {
        let conf = ConfState::new(peers.clone());
        for id in &peers {
            let key = (*id, group);
            let dir = self.base_dir.join(format!("node-{id}")).join(format!("group-{group}"));
            std::fs::create_dir_all(&dir).unwrap();
            let seed = self.rng.gen();
            self.node_seeds.insert(key, seed);
            let store = RaftStore::open(&dir, false).unwrap();
            let sm = Box::new(KVStateMachine::new());
            let node = RaftNode::bootstrap(
                *id,
                conf.clone(),
                store,
                sm,
                self.raft_cfg.clone(),
                seed,
                self.now,
            )
            .unwrap();
            self.nodes.insert(key, node);
            self.alive.insert(key);
            self.dirs.insert(key, dir);
        }
        self.leader_hint.insert(group, None);
        self.log_event(format!("group {group} bootstrapped with voters {peers:?}"));
    }

    // ---------------- 网络控制 ----------------

    pub fn set_loss(&mut self, pct: u32) {
        self.loss_pct = pct;
    }
    pub fn set_duplication(&mut self, pct: u32) {
        self.dup_pct = pct;
    }
    pub fn set_delay(&mut self, min_ms: u64, max_ms: u64) {
        self.delay_min = min_ms;
        self.delay_max = max_ms;
    }

    /// 施加分区：每个子列表是一个互通的孤岛。
    pub fn partition(&mut self, parts: Vec<Vec<NodeId>>) {
        self.log_event(format!("PARTITION {parts:?}"));
        self.partitions = Some(parts);
    }

    pub fn heal_partition(&mut self) {
        self.log_event("PARTITION HEALED".into());
        self.partitions = None;
    }

    fn reachable(&self, a: NodeId, b: NodeId) -> bool {
        match &self.partitions {
            None => true,
            Some(parts) => parts.iter().any(|p| p.contains(&a) && p.contains(&b)),
        }
    }

    fn route(&mut self, from: NodeId, to: NodeId, group: GroupId, msg: Message) {
        if !self.alive.contains(&(to, group)) || !self.alive.contains(&(from, group)) {
            self.dropped_msgs += 1;
            return;
        }
        if !self.reachable(from, to) {
            self.dropped_msgs += 1;
            return;
        }
        // 随机丢包
        if self.rng.gen_ratio(self.loss_pct.min(100) as u32, 100) {
            self.dropped_msgs += 1;
            return;
        }
        let span = if self.delay_max <= self.delay_min {
            self.delay_min
        } else {
            self.rng.gen_range(self.delay_min..=self.delay_max)
        };
        let deliver_at = self.now + span;
        self.inflight_seq += 1;
        let seq = self.inflight_seq;
        self.inflight.insert(
            (deliver_at, seq),
            Inflight { deliver_at, seq, from, to, group, msg: msg.clone() },
        );
        // 随机重复
        if self.dup_pct > 0 && self.rng.gen_ratio(self.dup_pct.min(100) as u32, 100) {
            let deliver2 = deliver_at + self.cfg.tick_ms;
            self.inflight_seq += 1;
            let seq2 = self.inflight_seq;
            self.inflight.insert(
                (deliver2, seq2),
                Inflight { deliver_at: deliver2, seq: seq2, from, to, group, msg },
            );
        }
    }

    // ---------------- 故障注入 ----------------

    /// 优雅停节点（保留磁盘）。
    pub fn shutdown_node(&mut self, id: NodeId) {
        let keys: Vec<NodeKey> = self
            .nodes
            .keys()
            .filter(|(n, _)| *n == id)
            .copied()
            .collect();
        for key in keys {
            if self.alive.remove(&key) {
                if let Some(node) = self.nodes.get(&key) {
                    self.log_event(format!(
                        "node {} group {} SHUTDOWN (term {}, role {:?})",
                        key.0, key.1, node.term, node.role
                    ));
                }
            }
        }
        // 在途消息清理：发给它的直接丢弃
        self.inflight.retain(|_, m| self.alive.contains(&(m.to, m.group)));
    }

    /// 节点必须已停机：直接破坏它的磁盘文件。
    pub fn corrupt_disk(
        &mut self,
        id: NodeId,
        group: GroupId,
        kind: CorruptionKind,
    ) -> std::result::Result<(), String> {
        let key = (id, group);
        if self.alive.contains(&key) {
            return Err("node must be shut down before corrupting its disk".into());
        }
        let dir = self.dirs.get(&key).cloned().ok_or("unknown node")?;
        let mut store = RaftStore::open(&dir, false).map_err(|e| e.to_string())?;
        store.inject_corruption(kind).map_err(|e| e.to_string())?;
        self.log_event(format!(
            "node {id} group {group} DISK CORRUPTED ({kind:?})"
        ));
        Ok(())
    }

    /// 重启：先做磁盘校验。损坏 -> Faulted 节点，拒绝参与投票。
    pub fn restart_node(&mut self, id: NodeId) {
        let keys: Vec<NodeKey> = self
            .dirs
            .keys()
            .filter(|(n, _)| *n == id)
            .copied()
            .collect();
        for key in keys {
            if self.alive.contains(&key) {
                continue;
            }
            let dir = self.dirs[&key].clone();
            let seed = self.node_seeds[&key];
            let store = match RaftStore::open(&dir, false) {
                Ok(s) => s,
                Err(e) => {
                    self.log_event(format!(
                        "node {} group {} RESTART FAULTED (open: {e})",
                        key.0, key.1
                    ));
                    self.nodes.insert(
                        key,
                        RaftNode::faulted(key.0, e.to_string(), self.raft_cfg.clone()),
                    );
                    self.alive.insert(key);
                    continue;
                }
            };
            let store = store;
            match store.verify_integrity() {
                Ok(()) => {
                    let sm = Box::new(KVStateMachine::new());
                    match RaftNode::recover(
                        key.0, store, sm, self.raft_cfg.clone(), seed, self.now,
                    ) {
                        Ok(node) => {
                            self.log_event(format!(
                                "node {} group {} RESTARTED ok (term {})",
                                key.0, key.1, node.term
                            ));
                            self.nodes.insert(key, node);
                        }
                        Err(e) => {
                            self.log_event(format!(
                                "node {} group {} RESTART FAULTED (recover: {e})",
                                key.0, key.1
                            ));
                            self.nodes.insert(
                                key,
                                RaftNode::faulted(key.0, e.to_string(), self.raft_cfg.clone()),
                            );
                        }
                    }
                }
                Err(StoreError::Corruption(reason)) => {
                    self.log_event(format!(
                        "node {} group {} RESTART REFUSED — corruption: {reason}",
                        key.0, key.1
                    ));
                    self.nodes.insert(
                        key,
                        RaftNode::faulted(key.0, reason, self.raft_cfg.clone()),
                    );
                }
                Err(e) => {
                    self.nodes.insert(
                        key,
                        RaftNode::faulted(key.0, e.to_string(), self.raft_cfg.clone()),
                    );
                }
            }
            self.alive.insert(key);
        }
    }

    /// 修复：抹掉损坏数据目录后以空白节点归队，靠 leader 的 InstallSnapshot 追平。
    pub fn repair_node(&mut self, id: NodeId, group: GroupId) {
        let key = (id, group);
        let dir = self.dirs[&key].clone();
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let seed = self.node_seeds[&key];
        let store = RaftStore::open(&dir, false).unwrap();
        let sm = Box::new(KVStateMachine::new());
        let node = RaftNode::recover_blank(
            id,
            store,
            sm,
            self.raft_cfg.clone(),
            seed,
            self.now,
        );
        self.log_event(format!("node {id} group {group} REPAIRED as blank, awaiting snapshot"));
        self.nodes.insert(key, node);
        self.alive.insert(key);
    }

    // ---------------- 客户端操作（需求 6：主切换期等待不报错） ----------------

    pub fn client_put(&mut self, client_id: u64, group: GroupId, key: Vec<u8>, value: Vec<u8>) {
        let req = self.next_client_req;
        self.next_client_req += 1;
        let msg = Message::ClientProposal {
            cmd: Command::Put { key: key.clone(), value },
            client_req: req,
        };
        self.pending.insert(
            req,
            PendingClientOp {
                client_id,
                group,
                call_time: self.now,
                msg,
                next_try: self.now,
                key,
                tried: std::collections::HashSet::new(),
            },
        );
    }

    pub fn client_delete(&mut self, client_id: u64, group: GroupId, key: Vec<u8>) {
        let req = self.next_client_req;
        self.next_client_req += 1;
        let msg = Message::ClientProposal {
            cmd: Command::Delete { key: key.clone() },
            client_req: req,
        };
        self.pending.insert(
            req,
            PendingClientOp {
                client_id,
                group,
                call_time: self.now,
                msg,
                next_try: self.now,
                key,
                tried: std::collections::HashSet::new(),
            },
        );
    }

    pub fn client_read(
        &mut self,
        client_id: u64,
        group: GroupId,
        key: Vec<u8>,
        mode: ReadMode,
    ) {
        let req = self.next_client_req;
        self.next_client_req += 1;
        let msg = Message::ClientRead(ReadRequest {
            key: key.clone(),
            mode,
            req_id: req,
        });
        self.pending.insert(
            req,
            PendingClientOp {
                client_id,
                group,
                call_time: self.now,
                msg,
                next_try: self.now,
                key,
                tried: std::collections::HashSet::new(),
            },
        );
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn inflight_count(&self) -> usize {
        self.inflight.len()
    }

    // ---------------- 主驱动循环 ----------------

    /// 推进到 `target_ms`。采用事件驱动时间跳跃：当某个 tick 既没有到期消息、
    /// 也没有节点要唤醒、也没有客户端要重试时，直接把逻辑时钟跳到最近的事件
    /// 时刻——不改变任何消息的相对顺序与投递时刻，只跳过"什么都不会发生"的
    /// 空 tick，避免心跳间隔远大于 tick 时的 O(空转次数) 开销。
    pub fn advance_to(&mut self, target_ms: u64) {
        while self.now < target_ms {
            // 下一条消息的到期时刻
            let next_msg = self
                .inflight
                .keys()
                .next()
                .map(|(deliver_at, _)| *deliver_at);
            // 任一存活节点的下一次唤醒（选举超时 / 心跳）
            let next_wake = self
                .alive
                .iter()
                .filter_map(|k| self.nodes.get(k))
                .map(|n| n.next_wakeup_ms())
                .min();
            // 下一次客户端重试
            let next_retry = self.pending.values().map(|o| o.next_try).min();

            let next_event = [next_msg, next_wake, next_retry, Some(target_ms)]
                .into_iter()
                .flatten()
                .filter(|t| *t > self.now)
                .min()
                .unwrap_or(target_ms)
                .min(target_ms);

            // 对齐到 tick 网格，保证确定性（所有事件时间本来就按 tick 取整）
            let mut jump_to = next_event;
            if jump_to > self.now + self.cfg.tick_ms {
                let steps = (jump_to - self.now + self.cfg.tick_ms - 1) / self.cfg.tick_ms;
                jump_to = self.now + steps * self.cfg.tick_ms;
            }
            self.now = jump_to.min(target_ms);

            self.deliver_due();
            self.retry_client_ops();
            self.step_all_nodes();
        }
    }

    fn deliver_due(&mut self) {
        // BTreeMap 按 (deliver_at, seq) 全局有序：split_off 取出所有到期消息，
        // 不再依赖"队首最早"的错误假设，彻底消除晚到期消息阻塞早到期消息的问题。
        let due = self.inflight.split_off(&(self.now + 1, 0));
        let due = std::mem::replace(&mut self.inflight, due);
        let mut queues: BTreeMap<NodeKey, Vec<(NodeId, Message)>> = BTreeMap::new();
        for (_, m) in due {
            if self.alive.contains(&(m.to, m.group)) && self.reachable(m.from, m.to) {
                queues.entry((m.to, m.group)).or_default().push((m.from, m.msg));
                self.delivered_msgs += 1;
            } else {
                self.dropped_msgs += 1;
            }
        }
        // 按 key 顺序喂消息，保证确定性
        for (key, msgs) in queues {
            let payload: Vec<Message> = msgs.into_iter().map(|(_, m)| m).collect();
            if let Some(node) = self.nodes.get_mut(&key) {
                let events = node.advance(self.now, payload);
                self.handle_events(key, events);
            }
        }
    }

    fn retry_client_ops(&mut self) {
        let reqs: Vec<ClientId> = self.pending.keys().copied().collect();
        for req in reqs {
            let Some(op) = self.pending.get(&req) else { continue };
            if op.next_try > self.now {
                continue;
            }
            let group = op.group;
            // 选目标：已知 leader 优先，否则在没投过的存活节点里轮询
            let hinted = self.leader_hint.get(&group).copied().flatten();
            let target = match hinted {
                Some(leader)
                    if self.alive.contains(&(leader, group))
                        && !self.pending[&req].tried.contains(&leader) =>
                {
                    leader
                }
                _ => {
                    let peers: Vec<NodeId> = self
                        .nodes
                        .keys()
                        .filter(|(n, g)| *g == group && !self.pending[&req].tried.contains(n))
                        .map(|(n, _)| *n)
                        .collect();
                    let pick = peers.get((req as usize + (self.now as usize)) % peers.len().max(1));
                    match pick {
                        Some(n) => *n,
                        None => {
                            // 所有节点都投过了：等一段时间后允许整体重试
                            let op = self.pending.get_mut(&req).unwrap();
                            op.next_try = self.now + 5 * self.cfg.tick_ms;
                            op.tried.clear();
                            continue;
                        }
                    }
                }
            };
            self.pending.get_mut(&req).unwrap().tried.insert(target);
            let key = (target, group);
            let msg = self.pending[&req].msg.clone();
            if let Some(node) = self.nodes.get_mut(&key) {
                let events = node.advance(self.now, vec![msg]);
                self.pending.get_mut(&req).unwrap().next_try =
                    self.now + self.cfg.tick_ms; // 主切换期等待，不报错
                self.handle_events(key, events);
            }
        }
    }

    fn step_all_nodes(&mut self) {
        let keys: Vec<NodeKey> = self.alive.iter().copied().collect();
        for key in keys {
            if let Some(node) = self.nodes.get_mut(&key) {
                let events = node.advance(self.now, vec![]);
                self.handle_events(key, events);
            }
        }
        self.assert_no_split_brain();
    }

    /// 安全不变量：同一任期、同一组内最多一个存活 leader。
    fn assert_no_split_brain(&self) {
        let mut leaders: BTreeMap<GroupId, Vec<(NodeId, Term)>> = BTreeMap::new();
        for ((id, group), node) in &self.nodes {
            if self.alive.contains(&(*id, *group)) && node.role == Role::Leader {
                leaders.entry(*group).or_default().push((*id, node.term));
            }
        }
        for (group, ls) in leaders {
            let terms: BTreeSet<Term> = ls.iter().map(|(_, t)| *t).collect();
            assert!(
                ls.len() <= 1 || terms.len() == ls.len(),
                "SPLIT BRAIN at t={}: group {group} has two leaders in same term: {ls:?}",
                self.now
            );
        }
    }

    fn handle_events(&mut self, key: NodeKey, events: Vec<crate::raft::Event>) {
        let (from, group) = key;
        for ev in events {
            match ev {
                crate::raft::Event::Send { to, msg } => {
                    self.route(from, to, group, msg);
                }
                crate::raft::Event::Committed { client_req, .. } => {
                    if let Some(op) = self.pending.remove(&client_req) {
                        self.history.push(Op {
                            client_id: op.client_id,
                            call_time: op.call_time,
                            return_time: self.now,
                            kind: match op.msg {
                                Message::ClientProposal {
                                    cmd: Command::Put { value, .. },
                                    ..
                                } => crate::checker::OpKind::Put { key: op.key, value },
                                Message::ClientProposal {
                                    cmd: Command::Delete { .. },
                                    ..
                                } => crate::checker::OpKind::Delete { key: op.key },
                                _ => unreachable!("committed non-proposal op"),
                            },
                        });
                    }
                }
                crate::raft::Event::ReadResult(r) => {
                    if let Some(op) = self.pending.remove(&r.req_id) {
                        self.history.push(Op {
                            client_id: op.client_id,
                            call_time: op.call_time,
                            return_time: self.now,
                            kind: crate::checker::OpKind::Read {
                                key: op.key,
                                observed: r.value,
                            },
                        });
                    }
                }
                crate::raft::Event::NotLeader { leader, .. } => {
                    // 更新 leader 提示；请求留在 pending 里继续等，不向客户端报错
                    if let Some(l) = leader {
                        let changed = self.leader_hint.get(&group).copied().flatten() != Some(l);
                        self.leader_hint.insert(group, Some(l));
                        if changed {
                            // 新主出现：允许把在途请求投给它
                            for op in self.pending.values_mut() {
                                if op.group == group {
                                    op.tried.clear();
                                }
                            }
                        }
                    }
                }
                crate::raft::Event::TermChanged(te) => {
                    if te.role == "leader" {
                        self.leader_hint.insert(group, te.leader);
                        for op in self.pending.values_mut() {
                            if op.group == group {
                                op.tried.clear();
                            }
                        }
                        self.log_event(format!(
                            "group {group} LEADER {} term {} (election {:?}ms)",
                            te.leader.unwrap_or(0),
                            te.term,
                            te.election_elapsed_ms
                        ));
                    }
                }
                crate::raft::Event::SnapshotCompacted { index } => {
                    self.log_event(format!(
                        "node {from} group {group} snapshot+truncate at {index}"
                    ));
                }
                crate::raft::Event::Applied { .. } => {}
                crate::raft::Event::Faulted { reason } => {
                    self.log_event(format!(
                        "node {from} group {group} FAULTED at runtime: {reason}"
                    ));
                }
            }
        }
    }

    // ---------------- 观测 ----------------

    pub fn node_views(&self) -> Vec<NodeView> {
        let mut out = Vec::new();
        for (key, node) in &self.nodes {
            out.push(NodeView {
                node_id: key.0,
                group: key.1,
                alive: self.alive.contains(key),
                role: format!("{:?}", node.role).to_lowercase(),
                term: node.term,
                leader: node.leader,
                commit: node.commit,
                applied: node.applied,
                last_log_index: node.last_log_index(),
                log_len: node.log_len() as u64,
                snapshot_index: node.snapshot_index(),
                in_joint: node.in_joint,
                voters: node.conf.voters.clone(),
                fault: node.fault_reason().map(|s| s.to_string()),
            });
        }
        out
    }

    pub fn term_events(&self, id: NodeId, group: GroupId) -> Vec<TermEvent> {
        self.nodes
            .get(&(id, group))
            .map(|n| n.term_events.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn leader_of(&self, group: GroupId) -> Option<NodeId> {
        self.nodes
            .iter()
            .find(|((_, g), n)| {
                *g == group && n.role == Role::Leader && self.alive.contains(&(n.id, group))
            })
            .map(|((id, _), _)| *id)
    }

    pub fn read_committed(&self, group: GroupId, key: &[u8]) -> Option<Vec<u8>> {
        // 从 leader 内存状态机直接读（测试断言用，不走客户端路径）
        let leader = self.leader_of(group)?;
        let node = self.nodes.get(&(leader, group))?;
        node.state_machine().get(key)
    }

    pub fn stats(&self) -> SimStats {
        SimStats {
            now_ms: self.now,
            inflight: self.inflight.len(),
            delivered: self.delivered_msgs,
            dropped: self.dropped_msgs,
            history_ops: self.history.len(),
            pending_ops: self.pending.len(),
        }
    }

    pub fn raft_config(&self) -> &RaftConfig {
        &self.raft_cfg
    }

    pub fn groups(&self) -> Vec<GroupId> {
        let g: BTreeSet<GroupId> = self.nodes.keys().map(|(_, g)| *g).collect();
        g.iter().copied().collect()
    }

    /// 找某物理节点当前所属的第一个组（单组场景直接用）。
    pub fn first_group_of(&self, id: NodeId) -> Option<GroupId> {
        self.nodes.keys().find(|(n, _)| *n == id).map(|(_, g)| *g)
    }

    /// 通过 leader 发起联合共识成员变更（需求 3）。
    pub fn propose_conf_change(
        &mut self,
        leader: NodeId,
        group: GroupId,
        change: crate::raft::ConfChange,
    ) -> Result<(), String> {
        let key = (leader, group);
        let node = self
            .nodes
            .get_mut(&key)
            .ok_or_else(|| format!("leader {leader} not in group {group}"))?;
        let mut events = Vec::new();
        node.propose_conf_change(&change, &mut events)?;
        self.handle_events(key, events);
        Ok(())
    }

    /// 成员变更前先把一个空白存储的新节点加进集群目录（不自动进 voters）。
    pub fn prepare_new_node_dir(&mut self, id: NodeId, group: GroupId) {
        let key = (id, group);
        if self.dirs.contains_key(&key) {
            return;
        }
        let dir = self.base_dir.join(format!("node-{id}")).join(format!("group-{group}"));
        std::fs::create_dir_all(&dir).unwrap();
        let seed = self.rng.gen();
        self.node_seeds.insert(key, seed);
        self.dirs.insert(key, dir);
    }

    /// 成员变更把新节点纳入 voters 后调用：以空白节点启动，等 leader 发快照。
    pub fn start_new_node_blank(&mut self, id: NodeId, group: GroupId) {
        let key = (id, group);
        let dir = self.dirs[&key].clone();
        let seed = self.node_seeds[&key];
        let store = RaftStore::open(&dir, false).unwrap();
        let sm = Box::new(KVStateMachine::new());
        let node = RaftNode::recover_blank(
            id,
            store,
            sm,
            self.raft_cfg.clone(),
            seed,
            self.now,
        );
        self.log_event(format!("new node {id} group {group} joined as blank, awaiting snapshot"));
        self.nodes.insert(key, node);
        self.alive.insert(key);
    }

    // ---- 分片驱动支持 ----

    pub fn node(&self, id: NodeId, group: GroupId) -> Option<&RaftNode> {
        self.nodes.get(&(id, group))
    }

    pub fn node_mut(&mut self, id: NodeId, group: GroupId) -> Option<&mut RaftNode> {
        self.nodes.get_mut(&(id, group))
    }

    /// 内部分片/路由请求号（不进客户端历史）。
    pub fn next_internal_req(&mut self) -> ClientId {
        let r = self.next_internal_req;
        self.next_internal_req += 1;
        r
    }

    /// 供分片驱动直接投递节点事件。
    pub fn handle_events_pub(
        &mut self,
        key: NodeKey,
        events: Vec<crate::raft::Event>,
    ) {
        self.handle_events(key, events);
    }

    /// 从某组 leader 的内存状态机导出 key 范围数据（分裂迁移用）。
    pub fn export_leader_range(
        &self,
        group: GroupId,
        range_start: &[u8],
    ) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let leader = self.leader_of(group).expect("group has no leader");
        let sm = self
            .nodes
            .get(&(leader, group))
            .unwrap()
            .state_machine();
        // KVStateMachine 是固定状态机，这里通过快照 JSON 中转导出
        let snap = sm.snapshot();
        let full: BTreeMap<Vec<u8>, Vec<u8>> = crate::storage::json_snapshot_decode(&snap).unwrap();
        full.range(range_start.to_vec()..)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// 给新组所有节点安装引导快照（分裂数据初始化）。
    pub fn bootstrap_group_with_data(
        &mut self,
        group: GroupId,
        voters: Vec<NodeId>,
        data: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) {
        let conf = ConfState::new(voters.clone());
        let bytes = crate::storage::json_snapshot_encode(data);
        for id in voters {
            let key = (id, group);
            let dir = self.base_dir.join(format!("node-{id}")).join(format!("group-{group}"));
            std::fs::create_dir_all(&dir).unwrap();
            let seed = self.rng.gen();
            self.node_seeds.insert(key, seed);
            self.dirs.insert(key, dir.clone());
            let store = RaftStore::open(&dir, false).unwrap();
            let sm = Box::new(KVStateMachine::new());
            let mut node = RaftNode::recover_blank(
                id,
                store,
                sm,
                self.raft_cfg.clone(),
                seed,
                self.now,
            );
            node.install_bootstrap_snapshot(conf.clone(), bytes.clone(), self.now).unwrap();
            self.log_event(format!(
                "group {group} node {id} bootstrapped with snapshot ({} keys)",
                data.len()
            ));
            self.nodes.insert(key, node);
            self.alive.insert(key);
        }
    }

    /// 在某组 leader 上提案删除 key 范围（分裂后老组清理）。
    /// 走内部请求号段，不进客户端历史、不与在途客户端请求撞号。
    pub fn delete_range_on_group(&mut self, group: GroupId, range_start: &[u8]) {
        let leader = self.leader_of(group).expect("group has no leader");
        let keys: Vec<Vec<u8>> = self.export_leader_range(group, range_start)
            .keys()
            .cloned()
            .collect();
        for key in keys {
            let req = self.next_internal_req();
            let node_key = (leader, group);
            let now = self.now;
            let msg = Message::ClientProposal {
                cmd: Command::Delete { key },
                client_req: req,
            };
            if let Some(node) = self.nodes.get_mut(&node_key) {
                let events = node.advance(now, vec![msg]);
                self.handle_events(node_key, events);
            }
        }
    }

    /// 收集某组 leader 状态机里的全部业务 key（分裂阈值判断用）。
    pub fn leader_business_keys(&self, group: GroupId) -> Vec<Vec<u8>> {
        let leader = self.leader_of(group).expect("group has no leader");
        let sm = self.nodes.get(&(leader, group)).unwrap().state_machine();
        let snap = sm.snapshot();
        let full: BTreeMap<Vec<u8>, Vec<u8>> = crate::storage::json_snapshot_decode(&snap).unwrap();
        full.keys()
            .filter(|k| !k.starts_with(b"\x00"))
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SimStats {
    pub now_ms: u64,
    pub inflight: usize,
    pub delivered: u64,
    pub dropped: u64,
    pub history_ops: usize,
    pub pending_ops: usize,
}
