//! Raft 核心,严格按论文 (Ongaro & Ousterhout, "In Search of an Understandable
//! Consensus Algorithm") 实现:
//! - §5.1-5.3 选主、日志复制、任期与投票规则(一个任期最多投一票、日志至少一样新才投票)
//! - §5.4.1 选举限制:候选人日志必须不旧于投票者
//! - §5.4.2 只提交当前任期的日志条目(靠计数复制数),旧任期条目随之间接提交
//! - §6 成员变更:联合共识(joint consensus),单步加减节点,自动 LeaveJoint
//! - §7 快照:日志过长时压缩,落后太多的跟随者直接 InstallSnapshot
//! - §8 客户端交互:线性一致读用 ReadIndex(心跳确认轮)或租约读
//! 时间由宿主以 tick 注入,模拟器里 1 tick 是虚拟时间,真实集群 1 tick = 10ms。
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};

use crate::stats::StatsHandle;
use crate::storage::{HardState, Storage, StorageError};

pub type NodeId = u64;
pub type GroupId = u64;
pub type Term = u64;

pub const META_GROUP: GroupId = 0;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum EntryType {
    Normal,
    ConfChange,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entry {
    pub term: Term,
    pub index: u64,
    pub etype: EntryType,
    pub data: Vec<u8>,
}

/// 联合共识成员变更:Add/Remove 进入联合态,LeaveJoint 退出联合态。
/// 任何时刻最多一个变更在进行(单节点粒度变更,论文 §6 的建议)。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConfChange {
    AddNode(NodeId),
    RemoveNode(NodeId),
    LeaveJoint,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct Conf {
    /// 当前(或新)配置
    pub voters: BTreeSet<NodeId>,
    /// 联合共识期间为旧配置
    pub joint: Option<BTreeSet<NodeId>>,
}

impl Conf {
    pub fn all_voters(&self) -> BTreeSet<NodeId> {
        let mut s = self.voters.clone();
        if let Some(j) = &self.joint {
            s.extend(j.iter().copied());
        }
        s
    }
    /// 联合共识期间需要新旧两个配置各自过半(论文 §6)
    pub fn quorum_reached(&self, got: &BTreeSet<NodeId>) -> bool {
        fn maj(s: &BTreeSet<NodeId>) -> usize {
            s.len() / 2 + 1
        }
        fn count(s: &BTreeSet<NodeId>, got: &BTreeSet<NodeId>) -> usize {
            s.iter().filter(|id| got.contains(id)).count()
        }
        count(&self.voters, got) >= maj(&self.voters)
            && self.joint.as_ref().map_or(true, |j| count(j, got) >= maj(j))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub index: u64,
    pub term: Term,
    pub conf: Conf,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope {
    pub group: GroupId,
    pub from: NodeId,
    pub to: NodeId,
    pub msg: Msg,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Msg {
    RequestVote {
        term: Term,
        last_log_index: u64,
        last_log_term: Term,
    },
    RequestVoteResp {
        term: Term,
        granted: bool,
        reason: String,
    },
    Append {
        term: Term,
        prev_log_index: u64,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: u64,
        /// ReadIndex 确认上下文(§8,心跳带 ctx,法定多数确认后读才生效)
        read_ctx: Option<u64>,
    },
    AppendResp {
        term: Term,
        success: bool,
        match_index: u64,
        /// 冲突快速回退提示:冲突任期的第一条下标 / 日志长度+1
        hint: u64,
        /// 冲突处的任期(领导者据此用自己日志定位回退点)
        conflict_term: Term,
        read_ctx: Option<u64>,
    },
    Snapshot {
        term: Term,
        snap: Snapshot,
    },
    SnapshotResp {
        term: Term,
        success: bool,
        match_index: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
    /// 磁盘校验失败:不投票、不竞选(带病不参与)
    Refused,
}

#[derive(Debug)]
pub enum ProposeError {
    NotLeader(Option<NodeId>),
    ConfChangePending,
    Refused,
}

#[derive(Clone, Copy, Debug, Default)]
struct Progress {
    next: u64,
    matched: u64,
}

pub struct Raft {
    pub id: NodeId,
    pub group: GroupId,
    pub role: Role,
    pub term: Term,
    pub voted_for: Option<NodeId>,
    pub commit: u64,
    /// 快照之后的日志,log[0].index == snap_index + 1
    pub log: Vec<Entry>,
    pub snap_index: u64,
    pub snap_term: Term,
    pub latest_snap: Option<Snapshot>,
    pub conf: Conf,
    /// 最近一次设置 conf 的日志下标(用于截断后重算)
    conf_log_index: u64,
    /// 存储层持久化的 (conf, index),无快照时从此重建
    persisted_conf: (Conf, u64),
    storage: Box<dyn Storage>,

    pub election_timeout: u64,
    pub heartbeat_timeout: u64,
    election_elapsed: u64,
    heartbeat_elapsed: u64,
    randomized_timeout: u64,
    rng: StdRng,
    votes: HashMap<NodeId, bool>,
    pub lead: Option<NodeId>,
    progress: HashMap<NodeId, Progress>,

    // ReadIndex
    next_read_ctx: u64,
    pending_reads: HashMap<u64, (u64, BTreeSet<NodeId>)>,
    pub ready_reads: Vec<(u64, u64)>,

    // 租约读:法定多数心跳确认后,lease_expire 之前可以本地读。
    // 时钟前提:各节点时钟漂移有界且远小于 election_timeout(见 README)。
    tick_count: u64,
    lease_expire: u64,
    hb_acks: BTreeSet<NodeId>,

    campaign_start_tick: u64,

    // 输出,宿主取走
    pub outbox: Vec<Envelope>,
    pub newly_committed: Vec<Entry>,
    committed_pushed: u64,
    pub pending_snapshot: Option<Snapshot>,

    stats: Option<StatsHandle>,
}

impl Raft {
    /// 全新引导一个组
    pub fn new(
        id: NodeId,
        group: GroupId,
        voters: Vec<NodeId>,
        mut storage: Box<dyn Storage>,
        seed: u64,
        stats: Option<StatsHandle>,
    ) -> Raft {
        let conf = Conf {
            voters: voters.into_iter().collect(),
            joint: None,
        };
        storage.save_hardstate(&HardState::default());
        storage.save_conf(&conf, 0);
        let mut r = Self::bare(id, group, storage, seed, stats);
        r.persisted_conf = (conf.clone(), 0);
        r.conf = conf;
        r
    }

    /// 从存储恢复;存储损坏时以 Refused 角色恢复(不投票、不竞选)
    pub fn recover(id: NodeId, group: GroupId, mut storage: Box<dyn Storage>, seed: u64, stats: Option<StatsHandle>) -> Raft {
        match storage.load() {
            Ok(st) => {
                let (snap_index, snap_term, latest_snap) = match &st.snap {
                    Some(s) => (s.index, s.term, Some(s.clone())),
                    None => (0, 0, None),
                };
                let mut r = Self::bare(id, group, storage, seed, stats);
                r.term = st.hs.term;
                r.voted_for = st.hs.vote;
                r.commit = st.hs.commit;
                r.log = st.entries;
                if let Some(pc) = st.conf {
                    r.persisted_conf = pc;
                }
                r.snap_index = snap_index;
                r.snap_term = snap_term;
                r.latest_snap = latest_snap;
                r.recompute_conf();
                // 已提交未应用的条目交给宿主重放,重建状态机
                r.committed_pushed = snap_index;
                r.push_newly_committed();
                r
            }
            Err(StorageError::Corrupt(reason)) => {
                if let Some(s) = &stats {
                    s.lock()
                        .unwrap()
                        .record_crc_failure(format!("node {} group {}: {}", id, group, reason));
                }
                let mut r = Self::bare(id, group, storage, seed, stats);
                r.role = Role::Refused;
                r
            }
            Err(e) => panic!("storage io error: {}", e),
        }
    }

    fn bare(id: NodeId, group: GroupId, storage: Box<dyn Storage>, seed: u64, stats: Option<StatsHandle>) -> Raft {
        let mut rng = StdRng::seed_from_u64(seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(group));
        let election_timeout = 30;
        let randomized_timeout = rng.gen_range(election_timeout..election_timeout * 2);
        Raft {
            id,
            group,
            role: Role::Follower,
            term: 0,
            voted_for: None,
            commit: 0,
            log: Vec::new(),
            snap_index: 0,
            snap_term: 0,
            latest_snap: None,
            conf: Conf::default(),
            conf_log_index: 0,
            persisted_conf: (Conf::default(), 0),
            storage,
            election_timeout,
            heartbeat_timeout: 5,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            randomized_timeout,
            rng,
            votes: HashMap::new(),
            lead: None,
            progress: HashMap::new(),
            next_read_ctx: 1,
            pending_reads: HashMap::new(),
            ready_reads: Vec::new(),
            tick_count: 0,
            lease_expire: 0,
            hb_acks: BTreeSet::new(),
            campaign_start_tick: 0,
            outbox: Vec::new(),
            newly_committed: Vec::new(),
            committed_pushed: 0,
            pending_snapshot: None,
            stats,
        }
    }

    // ---------- 基础访问 ----------

    pub fn last_index(&self) -> u64 {
        self.snap_index + self.log.len() as u64
    }
    pub fn last_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(self.snap_term)
    }
    pub fn term_at(&self, i: u64) -> u64 {
        if i == self.snap_index {
            return self.snap_term;
        }
        if i < self.snap_index || i > self.last_index() {
            return 0;
        }
        self.log[(i - self.snap_index - 1) as usize].term
    }
    pub fn log_len(&self) -> usize {
        self.log.len()
    }
    pub fn is_voter(&self, id: NodeId) -> bool {
        self.conf.all_voters().contains(&id)
    }
    pub fn progress_snapshot(&self) -> Vec<(NodeId, u64, u64)> {
        let mut v: Vec<(NodeId, u64, u64)> = self
            .progress
            .iter()
            .map(|(id, p)| (*id, p.matched, p.next))
            .collect();
        v.sort_unstable();
        v
    }
    pub fn storage_ok(&self) -> bool {
        self.storage.verify()
    }
    pub fn tick_count(&self) -> u64 {
        self.tick_count
    }
    pub fn lease_expire(&self) -> u64 {
        self.lease_expire
    }

    fn persist_hardstate(&mut self) {
        self.storage.save_hardstate(&HardState {
            term: self.term,
            vote: self.voted_for,
            commit: self.commit,
        });
    }

    fn reset_randomized_timeout(&mut self) {
        self.randomized_timeout = self
            .rng
            .gen_range(self.election_timeout..self.election_timeout * 2);
    }

    fn send(&mut self, to: NodeId, msg: Msg) {
        self.outbox.push(Envelope {
            group: self.group,
            from: self.id,
            to,
            msg,
        });
    }

    // ---------- 配置(联合共识) ----------

    fn apply_conf_change(&mut self, cc: &ConfChange, index: u64) {
        match cc {
            ConfChange::AddNode(id) => {
                if self.conf.joint.is_none() && !self.conf.voters.contains(id) {
                    let old = self.conf.voters.clone();
                    self.conf.voters.insert(*id);
                    self.conf.joint = Some(old);
                }
            }
            ConfChange::RemoveNode(id) => {
                if self.conf.joint.is_none() && self.conf.voters.contains(id) {
                    let old = self.conf.voters.clone();
                    self.conf.voters.remove(id);
                    self.conf.joint = Some(old);
                }
            }
            ConfChange::LeaveJoint => {
                self.conf.joint = None;
            }
        }
        self.conf_log_index = index;
        self.persisted_conf = (self.conf.clone(), index);
        self.storage.save_conf(&self.conf, index);
        if self.role == Role::Leader {
            // 新成员补 progress,被移除的删掉
            let last = self.last_index();
            for vid in self.conf.all_voters() {
                self.progress.entry(vid).or_insert(Progress {
                    next: last + 1,
                    matched: 0,
                });
            }
            let voters = self.conf.all_voters();
            self.progress.retain(|id, _| voters.contains(id));
        }
    }

    /// 截断可能回滚未提交的配置变更,从快照/持久化配置 + 剩余日志重放重建
    fn recompute_conf(&mut self) {
        let (base_conf, base_index) = match &self.latest_snap {
            Some(s) => (s.conf.clone(), s.index),
            None => self.persisted_conf.clone(),
        };
        self.conf = base_conf;
        self.conf_log_index = base_index;
        let entries = self.log.clone();
        for e in &entries {
            if e.etype == EntryType::ConfChange && e.index > base_index {
                let cc: ConfChange = serde_json::from_slice(&e.data).unwrap();
                // 重放期间不维护 progress(角色未定)
                let role = self.role;
                self.apply_conf_change(&cc, e.index);
                self.role = role;
            }
        }
        // 重放结束后 persisted_conf 以最后一条配置日志为准
        self.persisted_conf = (self.conf.clone(), self.conf_log_index);
    }

    fn has_pending_confchange(&self) -> bool {
        self.log
            .iter()
            .any(|e| e.index > self.commit && e.etype == EntryType::ConfChange)
    }

    // ---------- 角色转换 ----------

    fn become_follower(&mut self, term: Term, lead: Option<NodeId>) {
        // 磁盘损坏的节点永久保持 Refused,不接受任何角色转换
        if self.role == Role::Refused {
            return;
        }
        if term > self.term {
            self.voted_for = None;
        }
        let changed = term != self.term;
        self.term = term;
        self.role = Role::Follower;
        self.lead = lead;
        self.election_elapsed = 0;
        self.reset_randomized_timeout();
        if changed {
            self.persist_hardstate();
        }
    }

    fn campaign(&mut self) {
        if !self.is_voter(self.id) {
            return;
        }
        self.role = Role::Candidate;
        self.term += 1;
        self.voted_for = Some(self.id);
        self.persist_hardstate();
        self.votes.clear();
        self.votes.insert(self.id, true);
        self.election_elapsed = 0;
        self.reset_randomized_timeout();
        self.campaign_start_tick = self.tick_count;
        self.lead = None;
        let mut got = BTreeSet::new();
        got.insert(self.id);
        if self.conf.quorum_reached(&got) {
            self.become_leader();
            return;
        }
        let (li, lt) = (self.last_index(), self.last_term());
        let term = self.term;
        for peer in self.conf.all_voters() {
            if peer != self.id {
                self.send(
                    peer,
                    Msg::RequestVote {
                        term,
                        last_log_index: li,
                        last_log_term: lt,
                    },
                );
            }
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.lead = Some(self.id);
        let last = self.last_index();
        self.progress = self
            .conf
            .all_voters()
            .iter()
            .map(|&id| {
                (
                    id,
                    Progress {
                        next: last + 1,
                        matched: if id == self.id { last } else { 0 },
                    },
                )
            })
            .collect();
        self.hb_acks = [self.id].into_iter().collect();
        self.lease_expire = 0;
        if let Some(s) = &self.stats {
            s.lock().unwrap().record_election(
                self.group,
                self.term,
                self.id,
                self.tick_count - self.campaign_start_tick,
            );
        }
        // 当选即追加一条当前任期的空条目:让旧任期条目随之间接提交(§5.4.2),
        // 也让 ReadIndex/租约读有"当前任期已提交条目"这个前提。
        let idx = self.last_index() + 1;
        let e = Entry {
            term: self.term,
            index: idx,
            etype: EntryType::Normal,
            data: vec![],
        };
        self.append_local(vec![e]);
        self.broadcast_append(None);
        self.maybe_commit();
    }

    // ---------- tick ----------

    pub fn tick(&mut self) {
        self.tick_count += 1;
        match self.role {
            Role::Leader => {
                // 被移出最终配置的领导者让位
                if !self.is_voter(self.id) && self.conf.joint.is_none() && self.conf_log_index <= self.commit {
                    self.become_follower(self.term, None);
                    return;
                }
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.heartbeat_timeout {
                    self.heartbeat_elapsed = 0;
                    self.hb_acks = [self.id].into_iter().collect();
                    self.broadcast_append(None);
                }
            }
            Role::Follower | Role::Candidate => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.randomized_timeout {
                    self.campaign();
                }
            }
            Role::Refused => {}
        }
    }

    // ---------- 提案 ----------

    pub fn propose(&mut self, data: Vec<u8>) -> Result<u64, ProposeError> {
        if self.role == Role::Refused {
            return Err(ProposeError::Refused);
        }
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader(self.lead));
        }
        let idx = self.last_index() + 1;
        self.append_local(vec![Entry {
            term: self.term,
            index: idx,
            etype: EntryType::Normal,
            data,
        }]);
        self.broadcast_append(None);
        self.maybe_commit();
        Ok(idx)
    }

    pub fn propose_conf_change(&mut self, cc: ConfChange) -> Result<u64, ProposeError> {
        if self.role == Role::Refused {
            return Err(ProposeError::Refused);
        }
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader(self.lead));
        }
        match cc {
            ConfChange::LeaveJoint => {
                if self.conf.joint.is_none() {
                    return Err(ProposeError::ConfChangePending);
                }
            }
            _ => {
                // 论文 §6:一次只处理一个成员变更
                if self.conf.joint.is_some() || self.has_pending_confchange() {
                    return Err(ProposeError::ConfChangePending);
                }
            }
        }
        let idx = self.last_index() + 1;
        self.append_local(vec![Entry {
            term: self.term,
            index: idx,
            etype: EntryType::ConfChange,
            data: serde_json::to_vec(&cc).unwrap(),
        }]);
        self.broadcast_append(None);
        self.maybe_commit();
        Ok(idx)
    }

    // ---------- 线性一致读 ----------

    /// ReadIndex(§8):返回 Some(ctx) 后,等 ready_reads 里出现该 ctx,
    /// 且状态机应用到对应下标,即可安全本地读。
    pub fn read_index(&mut self) -> Option<u64> {
        if self.role != Role::Leader {
            return None;
        }
        // 前提:当前任期已有提交条目(当选时的空条目保证很快满足)
        if self.term_at(self.commit) != self.term {
            return None;
        }
        let ctx = self.next_read_ctx;
        self.next_read_ctx += 1;
        let mut acks = BTreeSet::new();
        acks.insert(self.id);
        self.pending_reads.insert(ctx, (self.commit, acks));
        self.broadcast_append(Some(ctx));
        self.check_read(ctx);
        Some(ctx)
    }

    fn ack_read(&mut self, ctx: u64, from: NodeId) {
        if let Some((_, acks)) = self.pending_reads.get_mut(&ctx) {
            acks.insert(from);
        }
        self.check_read(ctx);
    }

    fn check_read(&mut self, ctx: u64) {
        if let Some((index, acks)) = self.pending_reads.get(&ctx) {
            if self.conf.quorum_reached(acks) {
                let index = *index;
                self.pending_reads.remove(&ctx);
                self.ready_reads.push((ctx, index));
            }
        }
    }

    /// 租约读:法定多数心跳确认后 election_timeout*4/5 内本地读安全。
    /// 前提:各节点时钟速率漂移有界,跟随者的选举计时不会比领导者
    /// 的租约计时先走完(详见 README 的时钟假设说明)。
    pub fn lease_read_ok(&self) -> bool {
        self.role == Role::Leader
            && self.tick_count < self.lease_expire
            && self.term_at(self.commit) == self.term
    }

    // ---------- 快照 ----------

    /// 宿主在 applied 处打快照并截断日志
    pub fn snapshot(&mut self, index: u64, data: Vec<u8>) -> Option<Snapshot> {
        if index <= self.snap_index || index > self.commit || self.conf_log_index > index {
            return None;
        }
        let term = self.term_at(index);
        let snap = Snapshot {
            index,
            term,
            conf: self.conf.clone(),
            data,
        };
        self.install_snapshot_meta(snap.clone());
        Some(snap)
    }

    /// 新组引导/分裂时用初始快照播种(index 可为 0)
    pub fn seed_snapshot(&mut self, snap: Snapshot) {
        self.install_snapshot_meta(snap);
    }

    fn install_snapshot_meta(&mut self, snap: Snapshot) {
        self.storage.save_snapshot(&snap);
        // 保留快照点之后的日志(快照只是压缩前缀)
        if self.last_index() > snap.index {
            let off = (snap.index + 1 - self.snap_index - 1) as usize;
            self.log = self.log.split_off(off);
        } else {
            self.log.clear();
        }
        self.storage.compact_to(snap.index);
        self.snap_index = snap.index;
        self.snap_term = snap.term;
        self.conf = snap.conf.clone();
        self.conf_log_index = snap.index;
        self.persisted_conf = (snap.conf.clone(), snap.index);
        self.storage.save_conf(&snap.conf, snap.index);
        self.latest_snap = Some(snap);
        if self.commit < self.snap_index {
            self.commit = self.snap_index;
        }
        if self.committed_pushed < self.snap_index {
            self.committed_pushed = self.snap_index;
        }
        self.persist_hardstate();
    }

    // ---------- 消息处理 ----------

    pub fn step(&mut self, env: Envelope) {
        let from = env.from;
        match env.msg {
            Msg::RequestVote {
                term,
                last_log_index,
                last_log_term,
            } => self.on_request_vote(from, term, last_log_index, last_log_term),
            Msg::RequestVoteResp { term, granted, .. } => self.step_vote_resp(from, term, granted),
            Msg::Append {
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_ctx,
            } => self.on_append(from, term, prev_log_index, prev_log_term, entries, leader_commit, read_ctx),
            Msg::AppendResp {
                term,
                success,
                match_index,
                hint,
                conflict_term,
                read_ctx,
            } => self.on_append_resp(from, term, success, match_index, hint, conflict_term, read_ctx),
            Msg::Snapshot { term, snap } => self.on_snapshot(from, term, snap),
            Msg::SnapshotResp {
                term,
                success,
                match_index,
            } => self.on_snapshot_resp(from, term, success, match_index),
        }
    }

    fn on_request_vote(&mut self, from: NodeId, term: Term, last_log_index: u64, last_log_term: Term) {
        if term > self.term {
            self.become_follower(term, None);
        }
        // 磁盘损坏的节点不投票(带病不参与)
        if self.role == Role::Refused {
            self.send(
                from,
                Msg::RequestVoteResp {
                    term: self.term,
                    granted: false,
                    reason: "storage-corrupt".into(),
                },
            );
            return;
        }
        if term < self.term {
            self.send(
                from,
                Msg::RequestVoteResp {
                    term: self.term,
                    granted: false,
                    reason: "stale-term".into(),
                },
            );
            return;
        }
        // §9.6 租约安全:刚收到现任领导者心跳(选举超时内)的投票者
        // 拒绝给他人投票——否则新主可能在旧主租约有效期内当选,
        // 使旧主的租约读返回旧值。
        if self.lead.is_some() && self.lead != Some(from) && self.election_elapsed < self.election_timeout {
            self.send(
                from,
                Msg::RequestVoteResp {
                    term: self.term,
                    granted: false,
                    reason: "leader-lease-sticky".into(),
                },
            );
            return;
        }
        // §5.4.1:候选人日志必须至少和投票者一样新
        let up_to_date = last_log_term > self.last_term()
            || (last_log_term == self.last_term() && last_log_index >= self.last_index());
        let can_vote = self.voted_for.is_none() || self.voted_for == Some(from);
        let candidate_ok = self.conf.all_voters().contains(&from);
        let granted = up_to_date && can_vote && candidate_ok;
        if granted {
            self.voted_for = Some(from);
            self.persist_hardstate();
            self.election_elapsed = 0;
        }
        self.send(
            from,
            Msg::RequestVoteResp {
                term: self.term,
                granted,
                reason: if granted {
                    String::new()
                } else if !up_to_date {
                    "log-not-up-to-date".into()
                } else if !candidate_ok {
                    "not-voter".into()
                } else {
                    "already-voted".into()
                },
            },
        );
    }

    fn on_append(
        &mut self,
        from: NodeId,
        term: Term,
        mut prev_log_index: u64,
        mut prev_log_term: Term,
        mut entries: Vec<Entry>,
        leader_commit: u64,
        read_ctx: Option<u64>,
    ) {
        if term < self.term {
            self.send(
                from,
                Msg::AppendResp {
                    term: self.term,
                    success: false,
                    match_index: 0,
                    hint: self.last_index() + 1,
                    conflict_term: 0,
                    read_ctx,
                },
            );
            return;
        }
        if self.role == Role::Refused {
            return; // 带病不参与:不响应复制请求
        }
        self.become_follower(term, Some(from));

        // 与本地快照重叠的部分裁掉
        if prev_log_index < self.snap_index {
            let skip = (self.snap_index - prev_log_index) as usize;
            if entries.len() <= skip {
                self.send(
                    from,
                    Msg::AppendResp {
                        term: self.term,
                        success: true,
                        match_index: self.snap_index,
                        hint: 0,
                        conflict_term: 0,
                        read_ctx,
                    },
                );
                return;
            }
            entries.drain(..skip);
            prev_log_index = self.snap_index;
            prev_log_term = self.snap_term;
        }

        if prev_log_index > self.last_index() {
            self.send(
                from,
                Msg::AppendResp {
                    term: self.term,
                    success: false,
                    match_index: 0,
                    hint: self.last_index() + 1,
                    conflict_term: 0,
                    read_ctx,
                },
            );
            return;
        }
        if self.term_at(prev_log_index) != prev_log_term {
            // 冲突优化:回退到冲突任期的第一条
            let conflict_term = self.term_at(prev_log_index);
            let mut hint = prev_log_index;
            while hint > self.snap_index + 1 && self.term_at(hint - 1) == conflict_term {
                hint -= 1;
            }
            self.send(
                from,
                Msg::AppendResp {
                    term: self.term,
                    success: false,
                    match_index: 0,
                    hint,
                    conflict_term,
                    read_ctx,
                },
            );
            return;
        }

        // 找到第一个冲突/缺失位置,截断后追加
        for (i, e) in entries.iter().enumerate() {
            let idx = prev_log_index + 1 + i as u64;
            if idx <= self.last_index() && self.term_at(idx) == e.term {
                continue;
            }
            if idx <= self.last_index() {
                self.truncate_from(idx);
            }
            self.append_entries(&entries[i..]);
            break;
        }
        let new_last = prev_log_index + entries.len() as u64;
        let new_commit = leader_commit.min(new_last);
        if new_commit > self.commit {
            self.commit = new_commit;
            self.push_newly_committed();
        }
        self.persist_hardstate();
        self.send(
            from,
            Msg::AppendResp {
                term: self.term,
                success: true,
                match_index: new_last,
                hint: 0,
                conflict_term: 0,
                read_ctx,
            },
        );
    }

    fn on_append_resp(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        match_index: u64,
        hint: u64,
        conflict_term: Term,
        read_ctx: Option<u64>,
    ) {
        if term > self.term {
            self.become_follower(term, None);
            return;
        }
        if self.role != Role::Leader || term != self.term {
            return;
        }
        if success {
            let match_index = match_index.min(self.last_index());
            let p = match self.progress.get_mut(&from) {
                Some(p) => p,
                None => return,
            };
            if match_index > p.matched {
                p.matched = match_index;
                p.next = match_index + 1;
            }
            if let Some(ctx) = read_ctx {
                self.ack_read(ctx, from);
            }
            self.hb_acks.insert(from);
            if self.conf.quorum_reached(&self.hb_acks) {
                // 租约:留 1/5 余量吸收有界时钟漂移
                self.lease_expire = self.tick_count + self.election_timeout * 4 / 5;
            }
            self.maybe_commit();
        } else {
            // 冲突任期规则(论文 §5.3 末尾的优化):
            // 领导者日志里若有该任期,回退到自己该任期的最后一条+1;
            // 否则回退到跟随者给的该任期第一条。
            let mut term_next = None;
            if conflict_term != 0 {
                let mut i = self.last_index();
                loop {
                    let t = self.term_at(i);
                    if t == conflict_term {
                        term_next = Some(i + 1);
                        break;
                    }
                    if t < conflict_term || i <= self.snap_index {
                        break;
                    }
                    i -= 1;
                }
            }
            {
                let p = match self.progress.get_mut(&from) {
                    Some(p) => p,
                    None => return,
                };
                let fallback = hint.max(1).min(p.next.saturating_sub(1).max(1)).max(1);
                p.next = term_next.unwrap_or(fallback).max(1);
            }
            self.send_append(from, None);
        }
    }

    fn on_snapshot(&mut self, from: NodeId, term: Term, snap: Snapshot) {
        if term < self.term {
            self.send(
                from,
                Msg::SnapshotResp {
                    term: self.term,
                    success: false,
                    match_index: 0,
                },
            );
            return;
        }
        if self.role == Role::Refused {
            return;
        }
        self.become_follower(term, Some(from));
        // 不新于本地 commit 的快照没有意义:接受会让状态机回退且不再重放。
        // 告知真实进度,让领导者改用 Append。
        if snap.index <= self.commit {
            self.send(
                from,
                Msg::SnapshotResp {
                    term: self.term,
                    success: true,
                    match_index: self.last_index(),
                },
            );
            return;
        }
        // 若本地在快照点之后有匹配条目则保留,否则整段丢弃
        let keep_suffix = self.last_index() > snap.index && self.term_at(snap.index) == snap.term;
        self.storage.save_snapshot(&snap);
        if keep_suffix {
            let off = (snap.index + 1 - self.snap_index - 1) as usize;
            self.log = self.log.split_off(off);
        } else {
            self.log.clear();
            self.storage.truncate_from(self.snap_index + 1);
        }
        self.storage.compact_to(snap.index);
        self.snap_index = snap.index;
        self.snap_term = snap.term;
        self.latest_snap = Some(snap.clone());
        if self.commit < snap.index {
            self.commit = snap.index;
        }
        if self.committed_pushed < snap.index {
            self.committed_pushed = snap.index;
        }
        self.recompute_conf();
        self.persist_hardstate();
        // 交给宿主应用到状态机
        self.pending_snapshot = Some(snap.clone());
        self.send(
            from,
            Msg::SnapshotResp {
                term: self.term,
                success: true,
                match_index: snap.index,
            },
        );
    }

    fn on_snapshot_resp(&mut self, from: NodeId, term: Term, success: bool, match_index: u64) {
        if term > self.term {
            self.become_follower(term, None);
            return;
        }
        if self.role != Role::Leader || !success {
            return;
        }
        let match_index = match_index.min(self.last_index());
        if let Some(p) = self.progress.get_mut(&from) {
            if match_index > p.matched {
                p.matched = match_index;
            }
            // 快照响应后必须推进 next,否则 next 卡在快照点反复发同一个快照
            p.next = p.matched + 1;
        }
        self.maybe_commit();
    }

    // ---------- 复制内部 ----------

    fn append_local(&mut self, entries: Vec<Entry>) {
        let last = entries.last().unwrap().index;
        self.append_entries(&entries);
        if let Some(p) = self.progress.get_mut(&self.id) {
            p.matched = last;
            p.next = last + 1;
        }
    }

    fn append_entries(&mut self, entries: &[Entry]) {
        if entries.is_empty() {
            return;
        }
        for e in entries {
            if e.etype == EntryType::ConfChange {
                let cc: ConfChange = serde_json::from_slice(&e.data).unwrap();
                self.apply_conf_change(&cc, e.index);
            }
        }
        self.storage.append(entries);
        self.log.extend_from_slice(entries);
    }

    fn truncate_from(&mut self, index: u64) {
        if let Some(st) = &self.stats {
            let removed: Vec<(u64, u64)> = self
                .log
                .iter()
                .filter(|e| e.index >= index)
                .map(|e| (e.index, e.term))
                .collect();
            if !removed.is_empty() {
                crate::stats::push_capped_pub(
                    &mut st.lock().unwrap().truncate_log,
                    (self.id, self.group, index, removed),
                );
            }
        }
        self.log.truncate((index - self.snap_index - 1) as usize);
        self.storage.truncate_from(index);
        if self.conf_log_index >= index {
            self.recompute_conf();
        }
    }

    fn broadcast_append(&mut self, read_ctx: Option<u64>) {
        let peers: Vec<NodeId> = self.conf.all_voters().into_iter().filter(|id| *id != self.id).collect();
        for p in peers {
            self.send_append(p, read_ctx);
        }
    }

    fn send_append(&mut self, to: NodeId, read_ctx: Option<u64>) {
        let next = match self.progress.get(&to) {
            Some(p) => p.next.min(self.last_index() + 1),
            None => return,
        };
        if next <= self.snap_index {
            // 落后太多,直接装快照(论文 §7)
            if let Some(snap) = self.latest_snap.clone() {
                let term = self.term;
                self.send(to, Msg::Snapshot { term, snap });
            }
            return;
        }
        let prev = next - 1;
        let prev_term = self.term_at(prev);
        let off = (next - self.snap_index - 1) as usize;
        let entries: Vec<Entry> = self.log[off..].iter().take(64).cloned().collect();
        let term = self.term;
        let commit = self.commit;
        self.send(
            to,
            Msg::Append {
                term,
                prev_log_index: prev,
                prev_log_term: prev_term,
                entries,
                leader_commit: commit,
                read_ctx,
            },
        );
    }

    fn maybe_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        // §5.4.2:只按复制计数提交当前任期的条目
        let mut n = self.commit;
        for idx in (self.commit + 1..=self.last_index()).rev() {
            if self.term_at(idx) != self.term {
                continue;
            }
            let got: BTreeSet<NodeId> = self
                .progress
                .iter()
                .filter(|(_, p)| p.matched >= idx)
                .map(|(id, _)| *id)
                .collect();
            if self.conf.quorum_reached(&got) {
                n = idx;
                break;
            }
        }
        if n > self.commit {
            self.commit = n;
            if let Some(st) = &self.stats {
                let quorum: Vec<NodeId> = self
                    .progress
                    .iter()
                    .filter(|(_, p)| p.matched >= n)
                    .map(|(id, _)| *id)
                    .collect();
                crate::stats::push_capped_pub(
                    &mut st.lock().unwrap().commit_log,
                    (self.id, self.group, n, self.term, quorum),
                );
            }
            self.persist_hardstate();
            self.push_newly_committed();
            self.broadcast_append(None);
        }
        // 联合共识:联合条目提交后自动追加 LeaveJoint(论文 §6)
        if self.conf.joint.is_some() && self.conf_log_index <= self.commit && !self.has_pending_confchange() {
            let _ = self.propose_conf_change(ConfChange::LeaveJoint);
        }
    }

    fn push_newly_committed(&mut self) {
        let commit = self.commit;
        let pushed = self.committed_pushed;
        for e in &self.log {
            if e.index > pushed && e.index <= commit {
                self.newly_committed.push(e.clone());
            }
        }
        if commit > self.committed_pushed {
            self.committed_pushed = commit;
        }
    }
}

// step 里需要 from 的投票响应处理(签名拆出来,保持 on_vote_resp 简单)
impl Raft {
    pub fn step_vote_resp(&mut self, from: NodeId, term: Term, granted: bool) {
        if term > self.term {
            self.become_follower(term, None);
            return;
        }
        if self.role != Role::Candidate || term != self.term || !granted {
            return;
        }
        self.votes.insert(from, true);
        let got: BTreeSet<NodeId> = self
            .votes
            .iter()
            .filter(|(_, g)| **g)
            .map(|(id, _)| *id)
            .collect();
        if self.conf.quorum_reached(&got) {
            self.become_leader();
        }
    }
}
