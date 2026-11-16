//! 分片 KV 层:键空间按范围分片,每片一个 Raft 组;片太大自动分裂。
//! - 组 0 是元数据组(所有节点都跑),保存分片目录
//! - 数据组的分裂命令走自己的 Raft 日志,所有副本在同一下标确定性分裂
//! - 分裂完成后由旧组领导者向元数据组注册新目录,客户端靠 WrongShard 重试收敛
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

use crate::raft::{Conf, Envelope, EntryType, GroupId, NodeId, ProposeError, Raft, Role, Snapshot, META_GROUP};
use crate::stats::StatsHandle;
use crate::storage::StorageFactory;

pub type Key = Vec<u8>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardMeta {
    pub id: GroupId,
    pub start: Key,
    /// 空 = 正无穷
    pub end: Key,
    pub members: Vec<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KvCmd {
    Put {
        key: Key,
        val: Vec<u8>,
        /// 回执令牌 (client, req_id):应用时由提案者按身份响应,
        /// 防止领导下台后同一下标被覆盖产生"假成功"
        reply: Option<(u64, u64)>,
        /// 去重 ID (client_id, op_seq):同一逻辑操作重试共用,
        /// 状态机据此去重,防止重试导致同一操作生效两次
        op_id: (u64, u64),
    },
    Del {
        key: Key,
        reply: Option<(u64, u64)>,
        op_id: (u64, u64),
    },
    Split {
        split_key: Key,
        new_group: GroupId,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MetaCmd {
    RegisterSplit {
        old_group: GroupId,
        new_group: GroupId,
        split_key: Key,
        end: Key,
        members: Vec<NodeId>,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Consistency {
    /// ReadIndex:心跳确认法定多数后读
    Linearizable,
    /// 租约读:领导者租约有效期内直接本地读(时钟前提见 README)
    Lease,
    /// 任意节点本地读,可能读到旧值(不进线性一致性历史)
    Stale,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KvOp {
    Get,
    Put(Vec<u8>),
    Del,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientReq {
    Kv {
        req_id: u64,
        key: Key,
        op: KvOp,
        consistency: Consistency,
        /// 去重身份:客户端 ID + 操作序号(同一逻辑操作的重试共用)
        client_id: u64,
        op_seq: u64,
    },
    Directory {
        req_id: u64,
    },
    AskLeader {
        req_id: u64,
        group: GroupId,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KvError {
    NotLeader(Option<NodeId>),
    WrongShard,
    /// 领导者当前任期还没有提交条目,ReadIndex 暂不可用,重试即可
    NotReady,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientResp {
    Kv {
        req_id: u64,
        result: Result<Option<Vec<u8>>, KvError>,
    },
    Directory {
        req_id: u64,
        shards: Vec<ShardMeta>,
    },
    Leader {
        req_id: u64,
        leader: Option<NodeId>,
    },
}

// ---------------- 状态机 ----------------

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct KvSm {
    pub data: std::collections::BTreeMap<Key, Vec<u8>>,
    pub start: Key,
    pub end: Key,
    pub size: usize,
    /// 请求去重:client_id -> 已应用的最大 op_seq
    pub dedup: std::collections::BTreeMap<u64, u64>,
}

impl KvSm {
    /// 是否已应用过该操作(重试重复)
    pub fn is_dup(&self, op_id: (u64, u64)) -> bool {
        self.dedup.get(&op_id.0).map_or(false, |s| op_id.1 <= *s)
    }
    pub fn mark_applied(&mut self, op_id: (u64, u64)) {
        let e = self.dedup.entry(op_id.0).or_insert(0);
        if op_id.1 > *e {
            *e = op_id.1;
        }
    }
}

impl KvSm {
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && (self.end.is_empty() || key < self.end.as_slice())
    }
    fn put(&mut self, key: Key, val: Vec<u8>) {
        self.size += key.len() + val.len();
        if let Some(old) = self.data.insert(key.clone(), val) {
            self.size -= key.len() + old.len();
        }
    }
    fn del(&mut self, key: &[u8]) {
        if let Some(old) = self.data.remove(key) {
            self.size -= key.len() + old.len();
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MetaSm {
    pub shards: Vec<ShardMeta>,
}

impl MetaSm {
    pub fn lookup(&self, key: &[u8]) -> Option<&ShardMeta> {
        self.shards
            .iter()
            .find(|s| key >= s.start.as_slice() && (s.end.is_empty() || key < s.end.as_slice()))
    }
    fn apply(&mut self, cmd: MetaCmd) -> Option<(GroupId, GroupId, Key)> {
        match cmd {
            MetaCmd::RegisterSplit {
                old_group,
                new_group,
                split_key,
                end,
                members,
            } => {
                // 幂等:新组已注册过就忽略(多个节点可能重复注册)
                if self.shards.iter().any(|s| s.id == new_group) {
                    return None;
                }
                if let Some(pos) = self.shards.iter().position(|s| s.id == old_group) {
                    let start = self.shards[pos].start.clone();
                    self.shards[pos] = ShardMeta {
                        id: old_group,
                        start,
                        end: split_key.clone(),
                        members: members.clone(),
                    };
                    self.shards.insert(
                        pos + 1,
                        ShardMeta {
                            id: new_group,
                            start: split_key.clone(),
                            end,
                            members,
                        },
                    );
                    return Some((old_group, new_group, split_key));
                }
                None
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SmKind {
    Meta(MetaSm),
    Kv(KvSm),
}

impl SmKind {
    /// FNV-1a,用于跨副本一致性比对
    pub fn hash(&self) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        let mut mix = |bytes: &[u8]| {
            for b in bytes {
                h ^= *b as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
        };
        match self {
            SmKind::Meta(m) => {
                for s in &m.shards {
                    mix(&s.id.to_le_bytes());
                    mix(&s.start);
                    mix(&s.end);
                }
            }
            SmKind::Kv(sm) => {
                mix(&sm.start);
                mix(&sm.end);
                for (k, v) in &sm.data {
                    mix(k);
                    mix(v);
                }
                for (c, seq) in &sm.dedup {
                    mix(&c.to_le_bytes());
                    mix(&seq.to_le_bytes());
                }
            }
        }
        h
    }
}

// ---------------- 组宿主 ----------------

pub struct GroupHost {
    pub raft: Raft,
    pub sm: SmKind,
    pub applied: u64,
    /// 最近的应用下标→状态机哈希,供一致性检查页面
    pub hashes: VecDeque<(u64, u64)>,
    pub split_in_flight: bool,
}

// ---------------- 节点(分片管理器) ----------------

struct ParkedRead {
    ctx: u64,
    group: GroupId,
    client: u64,
    req_id: u64,
    key: Key,
    ready_index: Option<u64>,
}

pub struct Node {
    pub id: NodeId,
    pub groups: HashMap<GroupId, GroupHost>,
    pub factory: Box<dyn StorageFactory>,
    pub seed: u64,
    pub stats: Option<StatsHandle>,
    pub outbox: Vec<Envelope>,
    pub client_outbox: Vec<(u64, ClientResp)>,
    parked_reads: Vec<ParkedRead>,
    parked_writes: HashMap<(GroupId, u64), (u64, u64, Key)>,
    /// 已分裂但还没注册进元数据组的分裂(由元组领导者补注册)
    pending_split_regs: Vec<(GroupId, GroupId, Key, Key, Vec<NodeId>)>,
    pending_group_msgs: HashMap<GroupId, Vec<Envelope>>,
    pub snapshot_threshold: u64,
    pub split_threshold: usize,
}

impl Node {
    pub fn bootstrap(
        id: NodeId,
        voters: Vec<NodeId>,
        mut factory: Box<dyn StorageFactory>,
        seed: u64,
        stats: Option<StatsHandle>,
        snapshot_threshold: u64,
        split_threshold: usize,
    ) -> Node {
        let mut groups = HashMap::new();
        // 元数据组
        let meta_raft = Raft::new(
            id,
            META_GROUP,
            voters.clone(),
            factory.open(id, META_GROUP),
            seed ^ 0x5EED,
            stats.clone(),
        );
        let meta_sm = MetaSm {
            shards: vec![ShardMeta {
                id: 1,
                start: vec![],
                end: vec![],
                members: voters.clone(),
            }],
        };
        // 初始目录作为下标 1 的引导快照落盘,重启后能恢复
        let mut meta_raft = meta_raft;
        let seed_snap = crate::raft::Snapshot {
            index: 1,
            term: 0,
            conf: crate::raft::Conf {
                voters: voters.iter().copied().collect(),
                joint: None,
            },
            data: encode_sm(&SmKind::Meta(meta_sm.clone())),
        };
        meta_raft.seed_snapshot(seed_snap);
        groups.insert(
            META_GROUP,
            GroupHost {
                raft: meta_raft,
                sm: SmKind::Meta(meta_sm),
                applied: 1,
                hashes: VecDeque::new(),
                split_in_flight: false,
            },
        );
        // 初始数据组,覆盖整个键空间
        let data_raft = Raft::new(id, 1, voters, factory.open(id, 1), seed ^ 1, stats.clone());
        groups.insert(
            1,
            GroupHost {
                raft: data_raft,
                sm: SmKind::Kv(KvSm::default()),
                applied: 0,
                hashes: VecDeque::new(),
                split_in_flight: false,
            },
        );
        Node {
            id,
            groups,
            factory,
            seed,
            stats,
            outbox: Vec::new(),
            client_outbox: Vec::new(),
            parked_reads: Vec::new(),
            parked_writes: HashMap::new(),
            pending_group_msgs: HashMap::new(),
            pending_split_regs: Vec::new(),
            snapshot_threshold,
            split_threshold,
        }
    }

    /// 重启恢复:每组从存储恢复 Raft,状态机从快照 + 重放已提交日志重建
    pub fn recover(
        id: NodeId,
        factory: Box<dyn StorageFactory>,
        seed: u64,
        stats: Option<StatsHandle>,
        snapshot_threshold: u64,
        split_threshold: usize,
    ) -> Node {
        let mut node = Node {
            id,
            groups: HashMap::new(),
            factory,
            seed,
            stats,
            outbox: Vec::new(),
            client_outbox: Vec::new(),
            parked_reads: Vec::new(),
            parked_writes: HashMap::new(),
            pending_group_msgs: HashMap::new(),
            pending_split_regs: Vec::new(),
            snapshot_threshold,
            split_threshold,
        };
        let gids = node.factory.groups_for(id);
        for gid in gids {
            let storage = node.factory.open(id, gid);
            let raft = Raft::recover(id, gid, storage, seed ^ gid, node.stats.clone());
            let sm = match raft.latest_snap.as_ref() {
                Some(snap) => decode_sm(gid, &snap.data),
                None => {
                    if gid == META_GROUP {
                        SmKind::Meta(MetaSm::default())
                    } else {
                        SmKind::Kv(KvSm::default())
                    }
                }
            };
            let applied = raft.snap_index;
            if let Some(st) = &node.stats {
                crate::stats::push_capped_pub(&mut st.lock().unwrap().snap_log, (id, gid, applied, sm.hash()));
            }
            node.groups.insert(
                gid,
                GroupHost {
                    raft,
                    sm,
                    applied,
                    hashes: VecDeque::new(),
                    split_in_flight: false,
                },
            );
        }
        node.advance(); // 重放已提交日志
        node
    }

    pub fn tick(&mut self) {
        let mut gids: Vec<GroupId> = self.groups.keys().copied().collect();
        gids.sort_unstable();
        for gid in gids {
            self.groups.get_mut(&gid).unwrap().raft.tick();
        }
    }

    pub fn handle_envelope(&mut self, env: Envelope) {
        if !self.groups.contains_key(&env.group) {
            // 未知组按需创建空壳:新成员加入集群、或分裂消息先于分裂日志到达。
            // 空配置不会自我竞选,靠领导者复制/装快照追上。
            let storage = self.factory.open(self.id, env.group);
            let raft = Raft::new(
                self.id,
                env.group,
                vec![],
                storage,
                self.seed ^ env.group,
                self.stats.clone(),
            );
            let sm = if env.group == META_GROUP {
                SmKind::Meta(MetaSm::default())
            } else {
                SmKind::Kv(KvSm::default())
            };
            self.groups.insert(
                env.group,
                GroupHost {
                    raft,
                    sm,
                    applied: 0,
                    hashes: VecDeque::new(),
                    split_in_flight: false,
                },
            );
            if let Some(buf) = self.pending_group_msgs.remove(&env.group) {
                for m in buf {
                    self.groups.get_mut(&env.group).unwrap().raft.step(m);
                }
            }
        }
        let host = self.groups.get_mut(&env.group).unwrap();
        host.raft.step(env);
    }

    /// 每轮循环调用:应用提交、处理读就绪、快照、分裂、收集外发消息
    pub fn advance(&mut self) {
        let mut gids: Vec<GroupId> = self.groups.keys().copied().collect();
        gids.sort_unstable();
        // 1. 来自领导者的快照
        for gid in &gids {
            let host = self.groups.get_mut(gid).unwrap();
            if let Some(snap) = host.raft.pending_snapshot.take() {
                // 防御:快照不比已应用的更新就跳过(正常不会触发,Raft 层已拦截)
                if snap.index <= host.applied {
                    continue;
                }
                host.sm = decode_sm(*gid, &snap.data);
                host.applied = snap.index;
                if let Some(s) = &self.stats {
                    let mut st = s.lock().unwrap();
                    st.record_snapshot(*gid, self.id, snap.index, snap.data.len());
                    crate::stats::push_capped_pub(&mut st.snap_log, (self.id, *gid, snap.index, host.sm.hash()));
                }
                // 快照可能让 applied 跨过驻留读的等待点
                let gid = *gid;
                self.retry_parked_reads(gid);
            }
        }
        // 2. 应用新提交的日志
        for gid in gids.clone() {
            let entries: Vec<crate::raft::Entry> = {
                let host = self.groups.get_mut(&gid).unwrap();
                host.raft.newly_committed.drain(..).collect()
            };
            for e in entries {
                self.apply_entry(gid, e);
            }
        }
        // 3. ReadIndex 就绪
        for gid in gids.clone() {
            let reads: Vec<(u64, u64)> = {
                let host = self.groups.get_mut(&gid).unwrap();
                host.raft.ready_reads.drain(..).collect()
            };
            for (ctx, index) in reads {
                self.serve_read(gid, ctx, index);
            }
        }
        // 4. 维护:失去领导权时清驻留请求、快照、分裂、收集外发
        for gid in gids {
            let (role, lead) = {
                let host = self.groups.get(&gid).unwrap();
                (host.raft.role, host.raft.lead)
            };
            if role != Role::Leader {
                // 驻留的读/写请求改答 NotLeader,客户端立即换主重试
                let mut i = 0;
                while i < self.parked_reads.len() {
                    if self.parked_reads[i].group == gid {
                        let p = self.parked_reads.remove(i);
                        self.client_outbox.push((
                            p.client,
                            ClientResp::Kv {
                                req_id: p.req_id,
                                result: Err(KvError::NotLeader(lead)),
                            },
                        ));
                    } else {
                        i += 1;
                    }
                }
                let mut keys: Vec<(GroupId, u64)> =
                    self.parked_writes.keys().filter(|(g, _)| *g == gid).copied().collect();
                keys.sort_unstable(); // 固定顺序,保证可复现
                for k in keys {
                    let (client, req_id, _) = self.parked_writes.remove(&k).unwrap();
                    self.client_outbox.push((
                        client,
                        ClientResp::Kv {
                            req_id,
                            result: Err(KvError::NotLeader(lead)),
                        },
                    ));
                }
            }
            // 快照:日志过长时在 applied 处压缩
            let (need_snap, applied) = {
                let host = self.groups.get(&gid).unwrap();
                (
                    host.raft.log_len() as u64 > self.snapshot_threshold
                        && host.applied > host.raft.snap_index + self.snapshot_threshold / 2,
                    host.applied,
                )
            };
            if need_snap {
                let host = self.groups.get_mut(&gid).unwrap();
                let data = encode_sm(&host.sm);
                if let Some(snap) = host.raft.snapshot(applied, data) {
                    if let Some(s) = &self.stats {
                        s.lock().unwrap().record_snapshot(gid, self.id, snap.index, snap.data.len());
                    }
                }
            }
            // 分裂:数据组太大且是领导者时发起(走自己的 Raft 日志,确定性)
            let split_plan = {
                let host = self.groups.get(&gid).unwrap();
                if gid != META_GROUP
                    && host.raft.role == Role::Leader
                    && !host.split_in_flight
                    && host.raft.conf.joint.is_none()
                {
                    if let SmKind::Kv(sm) = &host.sm {
                        if sm.size > self.split_threshold && sm.data.len() >= 2 {
                            let mid_idx = sm.data.len() / 2;
                            let split_key = sm.data.keys().nth(mid_idx).unwrap().clone();
                            if split_key > sm.start && (sm.end.is_empty() || split_key < sm.end) {
                                Some((split_key, (gid << 32) | (host.applied + 1)))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            };
            if let Some((split_key, new_group)) = split_plan {
                let host = self.groups.get_mut(&gid).unwrap();
                let cmd = KvCmd::Split { split_key, new_group };
                if host.raft.propose(serde_json::to_vec(&cmd).unwrap()).is_ok() {
                    host.split_in_flight = true;
                }
            }
            // 收集外发消息
            let host = self.groups.get_mut(&gid).unwrap();
            self.outbox.extend(host.raft.outbox.drain(..));
        }
        // 4.5 元组领导者补注册未完成的分裂
        let is_meta_leader = self
            .groups
            .get(&META_GROUP)
            .map(|h| h.raft.role == Role::Leader)
            .unwrap_or(false);
        if is_meta_leader && !self.pending_split_regs.is_empty() {
            let registered: Vec<GroupId> = match &self.groups.get(&META_GROUP).unwrap().sm {
                SmKind::Meta(m) => m.shards.iter().map(|s| s.id).collect(),
                _ => vec![],
            };
            let pending = std::mem::take(&mut self.pending_split_regs);
            for (old_group, new_group, split_key, end, members) in pending {
                if registered.contains(&new_group) {
                    continue; // 已注册
                }
                let cmd = MetaCmd::RegisterSplit {
                    old_group,
                    new_group,
                    split_key: split_key.clone(),
                    end: end.clone(),
                    members: members.clone(),
                };
                let meta = self.groups.get_mut(&META_GROUP).unwrap();
                if meta.raft.propose(serde_json::to_vec(&cmd).unwrap()).is_err() {
                    // 不再是主了,放回队列
                    self.pending_split_regs
                        .push((old_group, new_group, split_key, end, members));
                }
            }
            let meta = self.groups.get_mut(&META_GROUP).unwrap();
            self.outbox.extend(meta.raft.outbox.drain(..));
        }

        // 5. 新组创建后重放缓冲的消息
        let mut pending: Vec<GroupId> = self
            .pending_group_msgs
            .keys()
            .filter(|g| self.groups.contains_key(g))
            .copied()
            .collect();
        pending.sort_unstable();
        for gid in pending {
            let msgs = self.pending_group_msgs.remove(&gid).unwrap();
            for env in msgs {
                self.handle_envelope(env);
            }
        }
    }

    fn apply_entry(&mut self, gid: GroupId, e: crate::raft::Entry) {
        let mut split: Option<(Key, GroupId, u64)> = None;
        let mut resp: Option<(u64, ClientResp)> = None;
        let mut split_stat: Option<(GroupId, GroupId, Key)> = None;
        let mut reply_token: Option<((u64, u64), bool)> = None;
        {
            let host = self.groups.get_mut(&gid).unwrap();
            if e.index <= host.applied {
                return;
            }
            host.applied = e.index;
            if e.etype == EntryType::Normal && !e.data.is_empty() {
                if gid == META_GROUP {
                    let cmd: MetaCmd = serde_json::from_slice(&e.data).unwrap();
                    if let SmKind::Meta(m) = &mut host.sm {
                        split_stat = m.apply(cmd);
                    }
                } else {
                    let cmd: KvCmd = serde_json::from_slice(&e.data).unwrap();
                    match cmd {
                        KvCmd::Put { key, val, reply, op_id } => {
                            if let Some(st) = &self.stats {
                                crate::stats::push_capped_pub(
                                    &mut st.lock().unwrap().put_log,
                                    (self.id, gid, e.index, e.term, key.clone()),
                                );
                            }
                            let in_range = match &mut host.sm {
                                SmKind::Kv(sm) => {
                                    let r = sm.contains(&key);
                                    if r && !sm.is_dup(op_id) {
                                        sm.put(key, val);
                                        sm.mark_applied(op_id);
                                    }
                                    r
                                }
                                _ => false,
                            };
                            reply_token = reply.map(|t| (t, in_range));
                        }
                        KvCmd::Del { key, reply, op_id } => {
                            if let Some(st) = &self.stats {
                                let mut k = b"DEL:".to_vec();
                                k.extend_from_slice(&key);
                                crate::stats::push_capped_pub(
                                    &mut st.lock().unwrap().put_log,
                                    (self.id, gid, e.index, e.term, k),
                                );
                            }
                            let in_range = match &mut host.sm {
                                SmKind::Kv(sm) => {
                                    let r = sm.contains(&key);
                                    if r && !sm.is_dup(op_id) {
                                        sm.del(&key);
                                        sm.mark_applied(op_id);
                                    }
                                    r
                                }
                                _ => false,
                            };
                            reply_token = reply.map(|t| (t, in_range));
                        }
                        KvCmd::Split { split_key, new_group } => {
                            split = Some((split_key, new_group, e.term));
                        }
                    }
                }
            }
            let hash = host.sm.hash();
            host.hashes.push_back((e.index, hash));
            if host.hashes.len() > 512 {
                host.hashes.pop_front();
            }
            if let Some(st) = &self.stats {
                crate::stats::push_capped_pub(&mut st.lock().unwrap().apply_log, (self.id, gid, e.index, hash));
            }
            // 只有提案者(本地驻留表里有这一条)且命令身份一致才响应
            if let Some(((client, req_id), in_range)) = reply_token {
                if let Some((pclient, preq, _)) = self.parked_writes.remove(&(gid, e.index)) {
                    if (pclient, preq) == (client, req_id) {
                        let result = if in_range { Ok(None) } else { Err(KvError::WrongShard) };
                        resp = Some((client, ClientResp::Kv { req_id, result }));
                    }
                }
            }
        }
        if let Some(r) = resp {
            self.client_outbox.push(r);
        }
        if let Some((old_g, new_g, key)) = split_stat {
            if let Some(s) = &self.stats {
                s.lock().unwrap().record_split(old_g, new_g, key);
            }
        }
        if let Some((split_key, new_group, split_term)) = split {
            self.execute_split(gid, split_key, new_group, split_term);
        }
        self.retry_parked_reads(gid);
    }

    /// 分裂:所有副本在同一下标执行,旧组截左半,新组带右半数据播种。
    /// 播种快照放在下标 1,迟到/新加入的副本靠 InstallSnapshot 拿数据。
    fn execute_split(&mut self, old_gid: GroupId, split_key: Key, new_gid: GroupId, split_term: u64) {
        let (members, end, hi, hi_size, dedup_clone) = {
            let host = self.groups.get_mut(&old_gid).unwrap();
            host.split_in_flight = false;
            let sm = match &mut host.sm {
                SmKind::Kv(sm) => sm,
                _ => return,
            };
            let hi = sm.data.split_off(&split_key);
            let hi_size: usize = hi.iter().map(|(k, v)| k.len() + v.len()).sum();
            sm.size -= hi_size;
            let end = std::mem::take(&mut sm.end);
            sm.end = split_key.clone();
            let members: Vec<NodeId> = host.raft.conf.all_voters().into_iter().collect();
            let dedup_clone = sm.dedup.clone();
            (members, end, hi, hi_size, dedup_clone)
        };
        let need_seed = match self.groups.get(&new_gid) {
            None => true,
            // 只有空壳(消息先到按需建的)才需要播种;已装过快照的跳过
            Some(h) => h.raft.latest_snap.is_none() && h.raft.log_len() == 0,
        };
        if need_seed {
            let new_sm = KvSm {
                data: hi,
                start: split_key.clone(),
                end: end.clone(),
                size: hi_size,
                dedup: dedup_clone,
            };
            let snap = Snapshot {
                index: 1,
                term: split_term,
                conf: Conf {
                    voters: members.iter().copied().collect(),
                    joint: None,
                },
                data: encode_sm(&SmKind::Kv(new_sm.clone())),
            };
            if let Some(host) = self.groups.get_mut(&new_gid) {
                host.raft.seed_snapshot(snap);
                host.sm = SmKind::Kv(new_sm);
                host.applied = 1;
            } else {
                let storage = self.factory.open(self.id, new_gid);
                let mut raft = Raft::new(
                    self.id,
                    new_gid,
                    members.clone(),
                    storage,
                    self.seed ^ new_gid,
                    self.stats.clone(),
                );
                raft.seed_snapshot(snap);
                self.groups.insert(
                    new_gid,
                    GroupHost {
                        raft,
                        sm: SmKind::Kv(new_sm),
                        applied: 1,
                        hashes: VecDeque::new(),
                        split_in_flight: false,
                    },
                );
            }
        }
        // 排队等元组领导者注册(谁是元组领导者谁在 advance 里补注册)
        self.pending_split_regs
            .push((old_gid, new_gid, split_key, end, members));
    }

    // ---------------- 客户端请求 ----------------

    pub fn handle_client_req(&mut self, client: u64, req: ClientReq) {
        match req {
            ClientReq::AskLeader { req_id, group } => {
                let leader = self.groups.get(&group).and_then(|h| h.raft.lead);
                self.client_outbox.push((client, ClientResp::Leader { req_id, leader }));
            }
            ClientReq::Directory { req_id } => {
                let shards = match self.groups.get(&META_GROUP).map(|h| &h.sm) {
                    Some(SmKind::Meta(m)) => m.shards.clone(),
                    _ => vec![],
                };
                self.client_outbox.push((client, ClientResp::Directory { req_id, shards }));
            }
            ClientReq::Kv {
                req_id,
                key,
                op,
                consistency,
                client_id,
                op_seq,
            } => self.handle_kv(client, req_id, key, op, consistency, client_id, op_seq),
        }
    }

    fn handle_kv(
        &mut self,
        client: u64,
        req_id: u64,
        key: Key,
        op: KvOp,
        consistency: Consistency,
        client_id: u64,
        op_seq: u64,
    ) {
        // 定位分片
        let gid = {
            match self.groups.get(&META_GROUP).map(|h| &h.sm) {
                Some(SmKind::Meta(m)) => match m.lookup(&key) {
                    Some(s) => s.id,
                    None => {
                        return self.reply_kv(client, req_id, Err(KvError::WrongShard));
                    }
                },
                _ => return self.reply_kv(client, req_id, Err(KvError::WrongShard)),
            }
        };
        let (role, lead, in_range) = match self.groups.get(&gid) {
            Some(h) => {
                let in_range = match &h.sm {
                    SmKind::Kv(sm) => sm.contains(&key),
                    _ => false,
                };
                (h.raft.role, h.raft.lead, in_range)
            }
            None => return self.reply_kv(client, req_id, Err(KvError::WrongShard)),
        };
        if !in_range {
            return self.reply_kv(client, req_id, Err(KvError::WrongShard));
        }
        match op {
            KvOp::Get => match consistency {
                Consistency::Stale => {
                    let v = match &self.groups.get(&gid).unwrap().sm {
                        SmKind::Kv(sm) => sm.data.get(&key).cloned(),
                        _ => None,
                    };
                    self.reply_kv(client, req_id, Ok(v));
                }
                Consistency::Lease => {
                    let host = self.groups.get(&gid).unwrap();
                    // 租约有效 且 状态机已应用到当前提交点,才能本地读;
                    // 否则退化为 ReadIndex(它会等 applied 追平)
                    if host.raft.lease_read_ok() && host.applied >= host.raft.commit {
                        if let Some(st) = &self.stats {
                            let now = st.lock().unwrap().now_ms;
                            crate::stats::push_capped_pub(
                                &mut st.lock().unwrap().lease_reads,
                                (self.id, gid, now, host.raft.term, host.raft.lease_expire()),
                            );
                        }
                        let v = match &host.sm {
                            SmKind::Kv(sm) => sm.data.get(&key).cloned(),
                            _ => None,
                        };
                        self.reply_kv(client, req_id, Ok(v));
                    } else {
                        self.handle_kv(client, req_id, key, KvOp::Get, Consistency::Linearizable, client_id, op_seq);
                    }
                }
                Consistency::Linearizable => {
                    if role != Role::Leader {
                        return self.reply_kv(client, req_id, Err(KvError::NotLeader(lead)));
                    }
                    let host = self.groups.get_mut(&gid).unwrap();
                    match host.raft.read_index() {
                        Some(ctx) => {
                            self.parked_reads.push(ParkedRead {
                                ctx,
                                group: gid,
                                client,
                                req_id,
                                key,
                                ready_index: None,
                            });
                            // 单节点组可能立即就绪
                            let ready: Vec<(u64, u64)> = host.raft.ready_reads.drain(..).collect();
                            let host = self.groups.get_mut(&gid).unwrap();
                            self.outbox.extend(host.raft.outbox.drain(..));
                            for (ctx, index) in ready {
                                self.serve_read(gid, ctx, index);
                            }
                        }
                        None => self.reply_kv(client, req_id, Err(KvError::NotReady)),
                    }
                }
            },
            KvOp::Put(val) => {
                if role != Role::Leader {
                    return self.reply_kv(client, req_id, Err(KvError::NotLeader(lead)));
                }
                let host = self.groups.get_mut(&gid).unwrap();
                let cmd = KvCmd::Put {
                    key: key.clone(),
                    val,
                    reply: Some((client, req_id)),
                    op_id: (client_id, op_seq),
                };
                match host.raft.propose(serde_json::to_vec(&cmd).unwrap()) {
                    Ok(index) => {
                        self.parked_writes.insert((gid, index), (client, req_id, key));
                        let host = self.groups.get_mut(&gid).unwrap();
                        self.outbox.extend(host.raft.outbox.drain(..));
                    }
                    Err(ProposeError::NotLeader(l)) => {
                        self.reply_kv(client, req_id, Err(KvError::NotLeader(l)))
                    }
                    _ => self.reply_kv(client, req_id, Err(KvError::NotReady)),
                }
            }
            KvOp::Del => {
                if role != Role::Leader {
                    return self.reply_kv(client, req_id, Err(KvError::NotLeader(lead)));
                }
                let host = self.groups.get_mut(&gid).unwrap();
                let cmd = KvCmd::Del {
                    key: key.clone(),
                    reply: Some((client, req_id)),
                    op_id: (client_id, op_seq),
                };
                match host.raft.propose(serde_json::to_vec(&cmd).unwrap()) {
                    Ok(index) => {
                        self.parked_writes.insert((gid, index), (client, req_id, key));
                        let host = self.groups.get_mut(&gid).unwrap();
                        self.outbox.extend(host.raft.outbox.drain(..));
                    }
                    Err(ProposeError::NotLeader(l)) => {
                        self.reply_kv(client, req_id, Err(KvError::NotLeader(l)))
                    }
                    _ => self.reply_kv(client, req_id, Err(KvError::NotReady)),
                }
            }
        }
    }

    fn reply_kv(&mut self, client: u64, req_id: u64, result: Result<Option<Vec<u8>>, KvError>) {
        self.client_outbox.push((client, ClientResp::Kv { req_id, result }));
    }

    fn serve_read(&mut self, gid: GroupId, ctx: u64, index: u64) {
        if let Some(pos) = self.parked_reads.iter().position(|p| p.ctx == ctx && p.group == gid) {
            let applied = self.groups.get(&gid).unwrap().applied;
            if applied >= index {
                let p = self.parked_reads.remove(pos);
                let result = match &self.groups.get(&gid).unwrap().sm {
                    SmKind::Kv(sm) => {
                        if sm.contains(&p.key) {
                            Ok(sm.data.get(&p.key).cloned())
                        } else {
                            Err(KvError::WrongShard)
                        }
                    }
                    _ => Err(KvError::WrongShard),
                };
                self.client_outbox.push((p.client, ClientResp::Kv { req_id: p.req_id, result }));
            } else {
                self.parked_reads[pos].ready_index = Some(index);
            }
        }
    }

    fn retry_parked_reads(&mut self, gid: GroupId) {
        let mut i = 0;
        while i < self.parked_reads.len() {
            let ready = self.parked_reads[i].group == gid
                && self.parked_reads[i]
                    .ready_index
                    .map_or(false, |idx| self.groups.get(&gid).unwrap().applied >= idx);
            if ready {
                let p = self.parked_reads.remove(i);
                let result = match &self.groups.get(&gid).unwrap().sm {
                    SmKind::Kv(sm) => {
                        if sm.contains(&p.key) {
                            Ok(sm.data.get(&p.key).cloned())
                        } else {
                            Err(KvError::WrongShard)
                        }
                    }
                    _ => Err(KvError::WrongShard),
                };
                self.client_outbox.push((p.client, ClientResp::Kv { req_id: p.req_id, result }));
            } else {
                i += 1;
            }
        }
    }

    // ---------------- 视图(供页面) ----------------

    pub fn views(&self) -> Vec<GroupView> {
        let mut out = Vec::new();
        let mut gids: Vec<GroupId> = self.groups.keys().copied().collect();
        gids.sort_unstable();
        for gid in gids {
            let h = &self.groups[&gid];
            let (size, range) = match &h.sm {
                SmKind::Kv(sm) => (sm.size, (sm.start.clone(), sm.end.clone())),
                SmKind::Meta(_) => (0, (vec![], vec![])),
            };
            out.push(GroupView {
                group: gid,
                role: format!("{:?}", h.raft.role),
                term: h.raft.term,
                lead: h.raft.lead,
                commit: h.raft.commit,
                applied: h.applied,
                log_len: h.raft.log_len(),
                snap_index: h.raft.snap_index,
                voters: h.raft.conf.voters.iter().copied().collect(),
                joint: h.raft.conf.joint.as_ref().map(|j| j.iter().copied().collect()),
                progress: h.raft.progress_snapshot(),
                sm_size: size,
                range,
                sm_hash: h.sm.hash(),
                storage_ok: h.raft.storage_ok(),
            });
        }
        out
    }

    pub fn directory(&self) -> Vec<ShardMeta> {
        match self.groups.get(&META_GROUP).map(|h| &h.sm) {
            Some(SmKind::Meta(m)) => m.shards.clone(),
            _ => vec![],
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct GroupView {
    pub group: GroupId,
    pub role: String,
    pub term: u64,
    pub lead: Option<NodeId>,
    pub commit: u64,
    pub applied: u64,
    pub log_len: usize,
    pub snap_index: u64,
    pub voters: Vec<NodeId>,
    pub joint: Option<Vec<NodeId>>,
    pub progress: Vec<(NodeId, u64, u64)>,
    pub sm_size: usize,
    pub range: (Key, Key),
    pub sm_hash: u64,
    pub storage_ok: bool,
}

// BTreeMap<Vec<u8>, _> 不能直接走 JSON(object key 必须是字符串),用数组代理
#[derive(Serialize, Deserialize)]
struct KvSmSer {
    data: Vec<(Key, Vec<u8>)>,
    start: Key,
    end: Key,
    size: usize,
    dedup: Vec<(u64, u64)>,
}

#[derive(Serialize, Deserialize)]
enum SmSer {
    Meta(MetaSm),
    Kv(KvSmSer),
}

fn encode_sm(sm: &SmKind) -> Vec<u8> {
    let ser = match sm {
        SmKind::Meta(m) => SmSer::Meta(m.clone()),
        SmKind::Kv(kv) => SmSer::Kv(KvSmSer {
            data: kv.data.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            start: kv.start.clone(),
            end: kv.end.clone(),
            size: kv.size,
            dedup: kv.dedup.iter().map(|(c, s)| (*c, *s)).collect(),
        }),
    };
    serde_json::to_vec(&ser).unwrap()
}

pub fn decode_sm_pub(gid: GroupId, data: &[u8]) -> SmKind {
    decode_sm(gid, data)
}

fn decode_sm(gid: GroupId, data: &[u8]) -> SmKind {
    match serde_json::from_slice::<SmSer>(data) {
        Ok(SmSer::Meta(m)) => SmKind::Meta(m),
        Ok(SmSer::Kv(k)) => SmKind::Kv(KvSm {
            data: k.data.into_iter().collect(),
            start: k.start,
            end: k.end,
            size: k.size,
            dedup: k.dedup.into_iter().collect(),
        }),
        Err(_) => {
            if gid == META_GROUP {
                SmKind::Meta(MetaSm::default())
            } else {
                SmKind::Kv(KvSm::default())
            }
        }
    }
}
