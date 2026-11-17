//! Raft 节点纯状态机。
//!
//! # 驱动方式
//! 节点不持有线程与网络。驱动层（sim / runner）在每个时刻调用
//! [`RaftNode::advance`]，传入当前时间（毫秒）和收到的消息，节点返回一批
//! [`Event`]（发消息、客户端响应、任期事件等）。模拟层用逻辑时钟、真实层用
//! `Instant` 换算的毫秒，因此同一份代码既可确定性重放也可线上运行。
//!
//! # 线性一致读的两种实现
//! - [`ReadMode::ReadIndex`]（论文 §6.4.1）：leader 先向全体发一轮确认心跳，
//!   拿到多数派 ack 后得到 readIndex（当时的 commit 索引），等状态机 applied
//!   追平 readIndex 再本地读。**不依赖任何时钟假设**，只靠 Raft 任期与多数派。
//! - [`ReadMode::Lease`]（论文 §6.4.2）：leader 收到多数派心跳 ack 时记下租约
//!   到期时刻；租约内的读请求跳过确认轮直接读。**时钟前提**：各节点本地时钟
//!   必须单调且相互漂移有界（设漂移上界为 Δ），且配置必须满足
//!   `lease_duration + Δ < election_timeout_min − heartbeat_interval`，
//!   即旧 leader 感知到的租约一定早于任何 follower 投票给新 leader 而失效。
//!   不满足该前提（比如虚拟机时钟回拨、未做 NTP 约束）时不能开租约读；
//!   本实现里租约过期会自动回退到 ReadIndex，不会用过期租约服务读。

use std::collections::{BTreeMap, HashSet, VecDeque};

use rand::{Rng, RngCore, SeedableRng};
use rand_pcg::Pcg64;

use crate::raft::state_machine::StateMachine;
use crate::raft::types::*;
use crate::raft::{LogIndex, NodeId, Term};
use crate::storage::{HardState, RaftStore, SnapshotFile, StoreError};

pub const ELECTION_EVENT_RING: usize = 64;

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
    /// 租约时长，必须满足模块文档中的时钟不等式
    pub lease_duration_ms: u64,
    /// 快照后保留在 WAL 里的日志条数阈值
    pub snapshot_threshold: u64,
    /// 每次 AppendEntries 最多带多少条
    pub max_entries_per_msg: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        RaftConfig {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
            lease_duration_ms: 100,
            snapshot_threshold: 64,
            max_entries_per_msg: 256,
        }
    }
}

struct PendingRead {
    read_id: u64,
    key: Vec<u8>,
    req_id: u64,
    acks: HashSet<NodeId>,
    /// 发起读时的任期：只接受同任期 ack，旧任期残留一律不计
    term: Term,
    /// 多数派确认时刻的 commitIndex（论文里的 readIndex）
    read_index: Option<LogIndex>,
}

/// 可注入的随机源。模拟层传固定种子的 Pcg64 保证可复现。
pub trait RaftRng: RngCore + Send {}
impl<T: RngCore + Send> RaftRng for T {}

pub struct RaftNode {
    pub id: NodeId,
    pub role: Role,
    pub term: Term,
    pub voted_for: Option<NodeId>,
    pub commit: LogIndex,
    pub applied: LogIndex,
    pub conf: ConfState,
    pub conf_old: Option<ConfState>,
    pub in_joint: bool,
    pub leader: Option<NodeId>,

    snap_index: LogIndex,
    snap_term: Term,
    /// snap_index 之后的日志，log[0].index == snap_index + 1
    log: Vec<Entry>,

    // leader 易失状态
    next_index: BTreeMap<NodeId, LogIndex>,
    match_index: BTreeMap<NodeId, LogIndex>,
    last_heartbeat_sent: u64,
    lease_expire_ms: u64,
    lease_acks: HashSet<NodeId>,
    pub lease_reads: u64,
    pub readindex_reads: u64,

    // 选举
    votes: HashSet<NodeId>,
    pre_votes: HashSet<NodeId>,
    pre_voting: bool,
    election_start_ms: Option<u64>,
    election_deadline_ms: u64,

    // 读
    pending_reads: VecDeque<PendingRead>,
    read_counter: u64,

    pub term_events: VecDeque<TermEvent>,
    pending_conf_change: bool,
    /// 已提交提案 index -> 客户端请求 id
    proposal_clients: BTreeMap<u64, ClientId>,

    pub cfg: RaftConfig,
    now_ms: u64,
    rng: Box<dyn RaftRng>,
    fault_reason: Option<String>,

    store: RaftStore,
    sm: Box<dyn StateMachine>,
}

impl RaftNode {
    // ---------------- 构造 / 恢复 ----------------

    /// 全新集群引导：所有初始成员各自写入同一条 index=1 的配置条目。
    pub fn bootstrap(
        id: NodeId,
        initial: ConfState,
        store: RaftStore,
        sm: Box<dyn StateMachine>,
        cfg: RaftConfig,
        seed: u64,
        now_ms: u64,
    ) -> Result<Self, StoreError> {
        if store.load_hard_state()?.is_some() || store.load_snapshot()?.is_some() {
            return Err(StoreError::Corruption(
                "bootstrap called on non-empty data dir".into(),
            ));
        }
        let marker = Entry {
            term: 0,
            index: 1,
            cmd: Command::LeaveJoint { conf: initial.clone() },
        };
        let mut store = store;
        store.save_hard_state(&HardState { term: 0, vote: None, commit: 1 })?;
        store.append(&[marker.clone()])?;
        let mut node = Self::fresh(id, initial, cfg, seed, now_ms, store, sm);
        node.snap_index = 0;
        node.snap_term = 0;
        node.log = vec![marker];
        node.commit = 1;
        node.applied = 1;
        node.sm.apply(1, 0, &Command::LeaveJoint { conf: node.conf.clone() });
        node.reset_election_timer(now_ms);
        node.push_term_event(now_ms, None);
        Ok(node)
    }

    /// 重启恢复：调用方必须先跑过 `RaftStore::verify_integrity`。
    pub fn recover(
        id: NodeId,
        mut store: RaftStore,
        mut sm: Box<dyn StateMachine>,
        cfg: RaftConfig,
        seed: u64,
        now_ms: u64,
    ) -> Result<Self, StoreError> {
        let hs = store.load_hard_state()?.unwrap_or_default();
        let snapshot = store.load_snapshot()?;
        let entries = store.entries(1, usize::MAX)?;

        let (snap_index, snap_term, conf, conf_old) = match &snapshot {
            Some(s) => {
                sm.restore(&s.data)?;
                (s.index, s.term, s.conf.clone(), s.conf_old.clone())
            }
            None => (0, 0, ConfState::default(), None),
        };

        let mut log = Vec::new();
        let mut applied = snap_index;
        for e in entries {
            if e.index <= snap_index {
                continue;
            }
            sm.apply(e.index, e.term, &e.cmd);
            applied = e.index;
            log.push(e);
        }
        let mut node = Self::fresh(
            id,
            conf,
            cfg,
            seed,
            now_ms,
            store,
            sm,
        );
        node.snap_index = snap_index;
        node.snap_term = snap_term;
        node.conf_old = conf_old;
        node.in_joint = node.conf_old.is_some();
        node.log = log;
        node.term = hs.term;
        node.voted_for = hs.vote;
        node.commit = hs.commit.max(applied);
        node.applied = applied;
        node.recover_conf_from_log();
        node.reset_election_timer(now_ms);
        node.term_events.push_back(TermEvent {
            term: hs.term,
            role: "follower(recovered)".into(),
            election_elapsed_ms: None,
            leader: None,
            at_ms: now_ms,
        });
        Ok(node)
    }

    /// 磁盘损坏、无法安全参与集群时构造一个 Faulted 节点：它拒绝一切消息。
    pub fn faulted(id: NodeId, reason: String, cfg: RaftConfig) -> Self {
        let tmp = std::env::temp_dir().join(format!("rkv-faulted-{id}-{}", std::process::id()));
        let store = RaftStore::open(&tmp, false).expect("temp dir store");
        let mut n = Self::fresh(
            id,
            ConfState::default(),
            cfg,
            0,
            0,
            store,
            Box::new(crate::raft::KVStateMachine::new()),
        );
        n.role = Role::Faulted;
        n.election_deadline_ms = u64::MAX;
        n.fault_reason = Some(reason);
        n.term_events.push_back(TermEvent {
            term: 0,
            role: "faulted".into(),
            election_elapsed_ms: None,
            leader: None,
            at_ms: 0,
        });
        n
    }

    /// 磁盘被清空后的空白节点：不参与选举，收到 leader 快照后恢复成员身份。
    pub fn recover_blank(
        id: NodeId,
        store: RaftStore,
        sm: Box<dyn StateMachine>,
        cfg: RaftConfig,
        seed: u64,
        now_ms: u64,
    ) -> Self {
        let mut node = Self::fresh(
            id,
            ConfState::default(),
            cfg,
            seed,
            now_ms,
            store,
            sm,
        );
        node.reset_election_timer(now_ms);
        node.term_events.push_back(TermEvent {
            term: 0,
            role: "follower(blank)".into(),
            election_elapsed_ms: None,
            leader: None,
            at_ms: now_ms,
        });
        node
    }

    /// 分片分裂引导：把外部导出的状态机数据作为 index=1 的初始快照安装。
    /// 调用方保证这是一个尚未参与过协议的空白组节点。
    pub fn install_bootstrap_snapshot(
        &mut self,
        conf: ConfState,
        data: Vec<u8>,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        let snap = SnapshotFile {
            index: 1,
            term: 0,
            conf: conf.clone(),
            conf_old: None,
            data,
        };
        self.sm.restore(&snap.data)?;
        self.store.write_snapshot(&snap)?;
        self.snap_index = 1;
        self.snap_term = 0;
        self.conf = conf;
        self.conf_old = None;
        self.in_joint = false;
        self.commit = 1;
        self.applied = 1;
        self.store.save_hard_state(&HardState { term: 0, vote: None, commit: 1 })?;
        self.reset_election_timer(now_ms);
        Ok(())
    }

    fn fresh(
        id: NodeId,
        conf: ConfState,
        cfg: RaftConfig,
        seed: u64,
        now_ms: u64,
        store: RaftStore,
        sm: Box<dyn StateMachine>,
    ) -> Self {
        Self {
            id,
            role: Role::Follower,
            term: 0,
            voted_for: None,
            commit: 0,
            applied: 0,
            conf,
            conf_old: None,
            in_joint: false,
            leader: None,
            snap_index: 0,
            snap_term: 0,
            log: Vec::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            last_heartbeat_sent: 0,
            lease_expire_ms: 0,
            lease_acks: HashSet::new(),
            lease_reads: 0,
            readindex_reads: 0,
            votes: HashSet::new(),
            pre_votes: HashSet::new(),
            pre_voting: false,
            election_start_ms: None,
            election_deadline_ms: 0,
            pending_reads: VecDeque::new(),
            read_counter: 0,
            term_events: VecDeque::new(),
            pending_conf_change: false,
            proposal_clients: BTreeMap::new(),
            cfg,
            now_ms,
            rng: Box::new(Pcg64::seed_from_u64(seed)),
            fault_reason: None,
            store,
            sm,
        }
    }

    pub fn fault_reason(&self) -> Option<&str> {
        self.fault_reason.as_deref()
    }

    pub fn state_machine(&self) -> &dyn StateMachine {
        self.sm.as_ref()
    }

    fn recover_conf_from_log(&mut self) {
        let mut conf = self.conf.clone();
        let mut old = self.conf_old.clone();
        let mut joint = self.in_joint;
        for e in &self.log {
            match &e.cmd {
                Command::EnterJoint { conf: new } => {
                    old = Some(conf.clone());
                    conf = new.clone();
                    joint = true;
                }
                Command::LeaveJoint { conf: new } => {
                    conf = new.clone();
                    old = None;
                    joint = false;
                }
                _ => {}
            }
        }
        self.conf = conf;
        self.conf_old = old;
        self.in_joint = joint;
    }

    // ---------------- 基础工具 ----------------

    pub fn last_log_index(&self) -> LogIndex {
        self.snap_index + self.log.len() as u64
    }

    pub fn last_log_term(&self) -> Term {
        match self.log.last() {
            Some(e) => e.term,
            None => self.snap_term,
        }
    }

    fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snap_index {
            return Some(self.snap_term);
        }
        if index < self.snap_index {
            return None;
        }
        self.log
            .get((index - self.snap_index - 1) as usize)
            .map(|e| e.term)
    }

    fn entry_at(&self, index: LogIndex) -> Option<&Entry> {
        if index <= self.snap_index {
            return None;
        }
        self.log.get((index - self.snap_index - 1) as usize)
    }

    fn effective_voters(&self) -> Vec<NodeId> {
        let mut v = self.conf.voters.clone();
        if let Some(old) = &self.conf_old {
            for id in &old.voters {
                if !v.contains(id) {
                    v.push(*id);
                }
            }
        }
        v
    }

    /// 联合共识期间需要两套多数派同时满足。
    fn has_quorum(&self, grants: &HashSet<NodeId>) -> bool {
        let ok = |c: &ConfState| {
            let yes = c.voters.iter().filter(|id| grants.contains(id)).count();
            yes >= c.quorum()
        };
        if let Some(old) = &self.conf_old {
            ok(&self.conf) && ok(old)
        } else {
            ok(&self.conf)
        }
    }

    fn reset_election_timer(&mut self, now: u64) {
        let span = self.rng.gen_range(
            self.cfg.election_timeout_min_ms..=self.cfg.election_timeout_max_ms,
        );
        self.election_deadline_ms = now + span;
    }

    fn persist_hard_state(&mut self) -> Result<(), StoreError> {
        self.store.save_hard_state(&HardState {
            term: self.term,
            vote: self.voted_for,
            commit: self.commit,
        })
    }

    fn push_term_event(&mut self, now: u64, elapsed: Option<u64>) {
        let role = match self.role {
            Role::Follower => "follower",
            Role::Candidate => "candidate",
            Role::Leader => "leader",
            Role::Faulted => "faulted",
        };
        self.term_events.push_back(TermEvent {
            term: self.term,
            role: role.into(),
            election_elapsed_ms: elapsed,
            leader: self.leader,
            at_ms: now,
        });
        while self.term_events.len() > ELECTION_EVENT_RING {
            self.term_events.pop_front();
        }
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        let changed = self.term != term || self.role != Role::Follower;
        let was_leader = self.role == Role::Leader;
        self.role = Role::Follower;
        self.term = term;
        self.voted_for = None;
        self.leader = leader;
        self.votes.clear();
        self.pre_votes.clear();
        self.pre_voting = false;
        self.pending_reads.clear();
        self.election_start_ms = None;
        // 下台即丢弃 leader 易失状态。关键：未提交提案的 client 映射必须清掉，
        // 否则这些 index 将来被新 leader 的日志覆盖并提交时会发出幽灵 Committed。
        if was_leader {
            self.proposal_clients.clear();
            self.next_index.clear();
            self.match_index.clear();
            self.lease_acks.clear();
        }
        if let Err(e) = self.persist_hard_state() {
            self.become_faulted(format!("persist hard state failed: {e}"));
            return;
        }
        if changed {
            self.push_term_event(self.now_ms, None);
        }
    }

    fn become_faulted(&mut self, reason: String) {
        self.role = Role::Faulted;
        self.fault_reason = Some(reason.clone());
        self.term_events.push_back(TermEvent {
            term: self.term,
            role: "faulted".into(),
            election_elapsed_ms: None,
            leader: None,
            at_ms: self.now_ms,
        });
    }

    // ---------------- 主入口 ----------------

    /// 推进一个时间点：先处理超时，再处理消息，最后推进提交/应用/读。
    pub fn advance(&mut self, now_ms: u64, incoming: Vec<Message>) -> Vec<Event> {
        let mut events = Vec::new();
        self.now_ms = now_ms;
        if self.role == Role::Faulted {
            // 带病节点：不参与任何协议交互，只回 NotLeader，让客户端去找别人
            for m in incoming {
                if matches!(m, Message::ClientProposal { .. } | Message::ClientRead(_)) {
                    events.push(Event::NotLeader { leader: None, client_req: None });
                }
            }
            return events;
        }

        // 1. 定时器
        match self.role {
            Role::Leader => {
                if now_ms.saturating_sub(self.last_heartbeat_sent)
                    >= self.cfg.heartbeat_interval_ms
                {
                    self.broadcast_append(&mut events);
                }
            }
            Role::Candidate => {
                // 平票/丢票后 candidate 必须能超时重选（论文 §5.2），
                // 否则两个 candidate 各持一票会永久卡死。
                if now_ms >= self.election_deadline_ms {
                    self.start_pre_vote(&mut events);
                }
            }
            _ => {
                if now_ms >= self.election_deadline_ms {
                    self.start_pre_vote(&mut events);
                }
            }
        }

        // 2. 消息
        for msg in incoming {
            self.step(msg, &mut events);
            if self.role == Role::Faulted {
                break;
            }
        }

        // 3. 提交/应用/快照/读
        if self.role != Role::Faulted {
            self.advance_apply(&mut events);
            self.try_serve_reads(&mut events);
        }
        events
    }

    // ---------------- 选举 ----------------

    fn start_pre_vote(&mut self, events: &mut Vec<Event>) {
        self.pre_voting = true;
        self.election_start_ms = Some(self.now_ms);
        self.pre_votes.clear();
        self.pre_votes.insert(self.id);
        self.reset_election_timer(self.now_ms);
        let future_term = self.term + 1;
        for peer in self.conf.voters.iter().filter(|p| **p != self.id) {
            events.push(Event::Send {
                to: *peer,
                msg: Message::PreVoteRequest {
                    term: future_term,
                    candidate: self.id,
                    last_log_index: self.last_log_index(),
                    last_log_term: self.last_log_term(),
                },
            });
        }
        // 单节点集群：pre-vote 直接成功
        if self.has_quorum(&self.pre_votes) {
            self.start_campaign(events);
        }
    }

    fn start_campaign(&mut self, events: &mut Vec<Event>) {
        self.role = Role::Candidate;
        self.term += 1;
        self.voted_for = Some(self.id);
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.id);
        self.pre_voting = false;
        self.election_start_ms = Some(self.now_ms);
        self.reset_election_timer(self.now_ms);
        if let Err(e) = self.persist_hard_state() {
            self.become_faulted(format!("persist vote failed: {e}"));
            return;
        }
        self.push_term_event(self.now_ms, None);
        for peer in self.conf.voters.iter().filter(|p| **p != self.id) {
            events.push(Event::Send {
                to: *peer,
                msg: Message::VoteRequest {
                    term: self.term,
                    candidate: self.id,
                    last_log_index: self.last_log_index(),
                    last_log_term: self.last_log_term(),
                },
            });
        }
        if self.has_quorum(&self.votes) {
            self.become_leader(events);
        }
    }

    fn become_leader(&mut self, events: &mut Vec<Event>) {
        self.role = Role::Leader;
        self.leader = Some(self.id);
        let last = self.last_log_index();
        self.next_index.clear();
        self.match_index.clear();
        for peer in self.effective_voters().iter().filter(|p| **p != self.id) {
            self.next_index.insert(*peer, last + 1);
            self.match_index.insert(*peer, 0);
        }
        self.lease_acks.clear();
        self.lease_acks.insert(self.id);
        self.lease_expire_ms = self.now_ms;
        // 新 leader 上任先提交一条 Noop（论文 §8），append 内部已向全体推送
        self.append_entries(
            vec![Entry { term: self.term, index: last + 1, cmd: Command::Noop }],
            events,
        );
        let elapsed = self
            .election_start_ms
            .map(|s| self.now_ms.saturating_sub(s));
        self.push_term_event(self.now_ms, elapsed);
        self.last_heartbeat_sent = self.now_ms;
    }

    fn handle_pre_vote_req(
        &mut self,
        term: u64,
        candidate: NodeId,
        last_idx: u64,
        last_term: u64,
        events: &mut Vec<Event>,
    ) {
        // Pre-vote 不递增任期、不持久化投票，只做资格判断
        let granted = term > self.term
            && self.log_at_least_as_up_to_date(last_idx, last_term)
            && self.conf.contains(candidate);
        events.push(Event::Send {
            to: candidate,
            msg: Message::PreVoteResponse { term: self.term, from: self.id, granted },
        });
    }

    fn handle_vote_req(
        &mut self,
        term: u64,
        candidate: NodeId,
        last_idx: u64,
        last_term: u64,
        events: &mut Vec<Event>,
    ) {
        if term < self.term {
            events.push(Event::Send {
                to: candidate,
                msg: Message::VoteResponse { term: self.term, from: self.id, granted: false },
            });
            return;
        }
        if term > self.term {
            self.become_follower(term, None);
        }
        let can_vote = self.voted_for.is_none() || self.voted_for == Some(candidate);
        let granted = can_vote
            && self.conf.contains(candidate)
            && self.log_at_least_as_up_to_date(last_idx, last_term);
        if granted {
            self.voted_for = Some(candidate);
            if let Err(e) = self.persist_hard_state() {
                self.become_faulted(format!("persist vote failed: {e}"));
                return;
            }
            self.reset_election_timer(self.now_ms);
        }
        events.push(Event::Send {
            to: candidate,
            msg: Message::VoteResponse { term: self.term, from: self.id, granted },
        });
    }

    /// 论文 §5.4 候选日志新旧判断：先比最后一条 term，再比 index。
    fn log_at_least_as_up_to_date(&self, other_idx: u64, other_term: u64) -> bool {
        let my_term = self.last_log_term();
        if other_term != my_term {
            return other_term > my_term;
        }
        other_idx >= self.last_log_index()
    }

    // ---------------- 日志复制 ----------------

    /// leader 把新日志落盘并向所有 follower 推送。
    fn append_entries(&mut self, entries: Vec<Entry>, events: &mut Vec<Event>) {
        if entries.is_empty() {
            return;
        }
        if let Err(e) = self.store.append(&entries) {
            self.become_faulted(format!("wal append failed: {e}"));
            return;
        }
        self.log.extend(entries);
        self.replicate_to_all(events);
    }

    fn broadcast_append(&mut self, events: &mut Vec<Event>) {
        self.last_heartbeat_sent = self.now_ms;
        self.lease_acks.clear();
        self.lease_acks.insert(self.id);
        self.replicate_to_all(events);
    }

    fn replicate_to_all(&mut self, events: &mut Vec<Event>) {
        let peers: Vec<NodeId> = self
            .effective_voters()
            .into_iter()
            .filter(|p| *p != self.id)
            .collect();
        for peer in peers {
            self.replicate_to(peer, events);
        }
    }

    /// 向单个 peer 推送日志或心跳。
    fn replicate_to(&mut self, peer: NodeId, events: &mut Vec<Event>) {
        let last = self.last_log_index();
        // 游标可能因为快照推进而落在压缩区间内，统一先夹到合法范围
        let mut next = *self.next_index.entry(peer).or_insert(last + 1);
        if next <= self.snap_index {
            match self.store.load_snapshot() {
                Ok(Some(snap)) => {
                    events.push(Event::Send {
                        to: peer,
                        msg: Message::InstallSnapshot { term: self.term, leader: self.id, snapshot: snap },
                    });
                    return;
                }
                Ok(None) => {
                    // 没有快照但 next 指向被压缩区间，退到 snap_index+1 重试
                    next = self.snap_index + 1;
                    self.next_index.insert(peer, next);
                }
                Err(e) => {
                    self.become_faulted(format!("load snapshot failed: {e}"));
                    return;
                }
            }
        }
        if next > last + 1 {
            // follower 游标跑过头（不应该发生），夹回末尾后一条
            next = last + 1;
            self.next_index.insert(peer, next);
        }
        let prev_idx = next - 1;
        let prev_term = if prev_idx == self.snap_index {
            self.snap_term
        } else {
            self.term_at(prev_idx).unwrap_or(0)
        };
        let start = (next - self.snap_index - 1) as usize;
        let end = (start + self.cfg.max_entries_per_msg).min(self.log.len());
        if start > self.log.len() {
            // 快照刚截断、内存日志尚在重建：本轮跳过，下拍心跳再发
            return;
        }
        let entries = self.log[start..end].to_vec();
        events.push(Event::Send {
            to: peer,
            msg: Message::AppendEntries {
                term: self.term,
                leader: self.id,
                prev_log_index: prev_idx,
                prev_log_term: prev_term,
                entries,
                leader_commit: self.commit,
                conf: self.conf.clone(),
                conf_old: self.conf_old.clone(),
            },
        });
    }

    fn handle_append_req(&mut self, msg: Message, events: &mut Vec<Event>) {
        let (term, leader, prev_idx, prev_term, entries, leader_commit, conf, conf_old) = match msg
        {
            Message::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                conf,
                conf_old,
            } => (
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                conf,
                conf_old,
            ),
            _ => unreachable!(),
        };

        if term < self.term {
            events.push(Event::Send {
                to: leader,
                msg: Message::AppendResponse {
                    term: self.term,
                    from: self.id,
                    match_index: 0,
                    success: false,
                    conflict_term: None,
                    conflict_index: self.last_log_index() + 1,
                },
            });
            return;
        }
        if term > self.term || self.role == Role::Candidate {
            self.become_follower(term, Some(leader));
        }
        self.leader = Some(leader);
        self.reset_election_timer(self.now_ms);
        self.pre_voting = false;

        // 快照覆盖区间内的 prev 无法校验，等 leader 发 InstallSnapshot
        if prev_idx < self.snap_index {
            events.push(Event::Send {
                to: leader,
                msg: Message::AppendResponse {
                    term: self.term,
                    from: self.id,
                    match_index: 0,
                    success: false,
                    conflict_term: Some(self.snap_term),
                    conflict_index: self.snap_index + 1,
                },
            });
            return;
        }

        // prev 日志一致性检查
        if prev_idx > self.last_log_index() {
            events.push(Event::Send {
                to: leader,
                msg: Message::AppendResponse {
                    term: self.term,
                    from: self.id,
                    match_index: 0,
                    success: false,
                    conflict_term: None,
                    conflict_index: self.last_log_index() + 1,
                },
            });
            return;
        }
        let actual_prev_term = if prev_idx == 0 {
            0
        } else if prev_idx == self.snap_index {
            self.snap_term
        } else {
            self.term_at(prev_idx).unwrap_or(0)
        };
        if actual_prev_term != prev_term {
            // 论文 §5.3 冲突提示：给出该位置 term 的第一条 index
            let conflict_term = actual_prev_term;
            let mut conflict_index = prev_idx;
            for idx in (self.snap_index + 1..=prev_idx).rev() {
                if self.term_at(idx) == Some(conflict_term) {
                    conflict_index = idx;
                } else {
                    break;
                }
            }
            events.push(Event::Send {
                to: leader,
                msg: Message::AppendResponse {
                    term: self.term,
                    from: self.id,
                    match_index: 0,
                    success: false,
                    conflict_term: Some(conflict_term),
                    conflict_index,
                },
            });
            return;
        }

        // 合并：找第一条与本地不同的日志。
        // matched 必须精确等于"与 leader 一致的最后一条"：follower 可能带有
        // 未提交的分叉后缀，绝不能用自己的 last_log_index 冒充（否则 leader
        // 会把没复制过的日志当成已复制，错误推进 commitIndex —— 论文 §5.3）。
        let mut first_new = prev_idx + 1;
        let mut matched = prev_idx;
        for e in &entries {
            if let Some(local) = self.entry_at(e.index) {
                if local.term != e.term {
                    // Raft 安全不变量：已提交条目绝不允许被覆盖。真触发说明
                    // 上层（选举资格/提交规则）出了 bug，直接 Faulted 而不是静默截断。
                    if e.index <= self.commit {
                        self.become_faulted(format!(
                            "refusing to overwrite committed entry {} (commit={})",
                            e.index, self.commit
                        ));
                        return;
                    }
                    if let Err(err) = self.store.truncate_suffix(e.index) {
                        self.become_faulted(format!("truncate failed: {err}"));
                        return;
                    }
                    if e.index <= self.snap_index {
                        self.become_faulted("conflict inside compacted snapshot".into());
                        return;
                    }
                    let pos = (e.index - self.snap_index - 1) as usize;
                    self.log.truncate(pos);
                    break;
                }
                first_new = e.index + 1;
                matched = e.index; // 已存在且 term 一致
            } else {
                break;
            }
        }
        let fresh: Vec<Entry> = entries.into_iter().filter(|e| e.index >= first_new).collect();
        if !fresh.is_empty() {
            if let Err(e) = self.store.append(&fresh) {
                self.become_faulted(format!("wal append failed: {e}"));
                return;
            }
            matched = fresh.last().unwrap().index; // 新追加的全部与 leader 一致
            self.log.extend(fresh);
        }

        // leader 的配置视图是权威的
        self.conf = conf;
        self.conf_old = conf_old;
        self.in_joint = self.conf_old.is_some();

        if leader_commit > self.commit {
            self.commit = leader_commit.min(self.last_log_index());
        }
        if let Err(e) = self.persist_hard_state() {
            self.become_faulted(format!("persist commit failed: {e}"));
            return;
        }
        events.push(Event::Send {
            to: leader,
            msg: Message::AppendResponse {
                term: self.term,
                from: self.id,
                match_index: matched,
                success: true,
                conflict_term: None,
                conflict_index: 0,
            },
        });
    }

    fn handle_append_resp(&mut self, from: NodeId, success: bool, match_idx: u64,
        conflict_term: Option<u64>, conflict_index: u64, events: &mut Vec<Event>) {
        if success {
            let prev = self.match_index.entry(from).or_insert(0);
            if match_idx > *prev {
                *prev = match_idx;
            }
            self.next_index.insert(from, match_idx + 1);
            self.lease_acks.insert(from);
            if self.has_quorum(&self.lease_acks) {
                self.lease_expire_ms = self.now_ms + self.cfg.lease_duration_ms;
            }
            self.maybe_advance_commit(events);
            // ack 回环：peer 已完全追平时不再回发空 AppendEntries（下一拍定时
            // 心跳会强制发，租约与新鲜度不受影响）；仍落后则继续推日志。
            if match_idx < self.last_log_index() {
                self.replicate_to(from, events);
            }
        } else {
            let last = self.last_log_index();
            let next = self.next_index.entry(from).or_insert(last + 1);
            let new_next = match conflict_term {
                Some(ct) => {
                    // 找到本日志中该 term 的最后一条，next 跳到它后面
                    let mut last_of_term = None;
                    for e in self.log.iter() {
                        if e.term == ct {
                            last_of_term = Some(e.index);
                        }
                    }
                    match last_of_term {
                        Some(idx) => idx + 1,
                        None => conflict_index,
                    }
                }
                None => conflict_index.max(1).min(*next),
            };
            *next = new_next;
            self.replicate_to(from, events);
        }
    }

    fn maybe_advance_commit(&mut self, events: &mut Vec<Event>) {
        // 从后往前找本任期、多数派复制的 index
        for idx in (self.commit + 1..=self.last_log_index()).rev() {
            if self.term_at(idx) != Some(self.term) {
                continue;
            }
            let mut replicated = HashSet::new();
            replicated.insert(self.id);
            for (peer, m) in &self.match_index {
                if *m >= idx {
                    replicated.insert(*peer);
                }
            }
            if self.has_quorum(&replicated) {
                self.commit = idx;
                if let Err(e) = self.persist_hard_state() {
                    self.become_faulted(format!("persist commit failed: {e}"));
                    return;
                }
                break;
            }
        }
        self.advance_apply(events);
    }

    // ---------------- 应用 / 快照 ----------------

    fn advance_apply(&mut self, events: &mut Vec<Event>) {
        while self.applied < self.commit {
            let idx = self.applied + 1;
            let entry = match self.entry_at(idx) {
                Some(e) => e.clone(),
                None => {
                    self.become_faulted(format!("committed entry {idx} missing"));
                    return;
                }
            };
            self.sm.apply(idx, entry.term, &entry.cmd);
            self.applied = idx;

            // 配置条目
            match &entry.cmd {
                Command::EnterJoint { conf: new } => {
                    self.conf_old = Some(self.conf.clone());
                    self.conf = new.clone();
                    self.in_joint = true;
                    // 新加入的 peer 初始化复制游标
                    let last_idx = self.last_log_index();
                    for p in &new.voters {
                        self.next_index.entry(*p).or_insert(last_idx + 1);
                        self.match_index.entry(*p).or_insert(0);
                    }
                    // 联合配置提交后立即追加 LeaveJoint：C_new 在其提交后生效
                    if self.role == Role::Leader {
                        let idx = self.last_log_index() + 1;
                        let target = new.clone();
                        self.append_entries(
                            vec![Entry {
                                term: self.term,
                                index: idx,
                                cmd: Command::LeaveJoint { conf: target },
                            }],
                            events,
                        );
                    }
                }
                Command::LeaveJoint { conf: new } => {
                    self.conf = new.clone();
                    self.conf_old = None;
                    self.in_joint = false;
                    self.pending_conf_change = false;
                    // 被移除的节点不再收消息
                    let removed: Vec<u64> = self
                        .next_index
                        .keys()
                        .filter(|p| !self.conf.contains(**p))
                        .copied()
                        .collect();
                    for p in removed {
                        self.next_index.remove(&p);
                        self.match_index.remove(&p);
                    }
                    if !self.conf.contains(self.id) && self.role == Role::Leader {
                        // 自己被移除：安静下台
                        self.become_follower(self.term, None);
                        return;
                    }
                }
                _ => {}
            }

            if let Some(client_req) = self.proposal_clients.remove(&idx) {
                events.push(Event::Committed { client_req, index: idx });
            }
            events.push(Event::Applied { index: idx, term: entry.term, cmd: entry.cmd });
        }

        // 日志太长 -> 快照 + 截断
        if self.applied > 0 && self.applied - self.snap_index >= self.cfg.snapshot_threshold {
            self.do_snapshot(events);
        }
    }

    fn do_snapshot(&mut self, events: &mut Vec<Event>) {
        let index = self.applied;
        let term = self.term_at(index).unwrap_or(self.term);
        let snap = SnapshotFile {
            index,
            term,
            conf: self.conf.clone(),
            conf_old: self.conf_old.clone(),
            data: self.sm.snapshot(),
        };
        if let Err(e) = self.store.write_snapshot(&snap) {
            self.become_faulted(format!("write snapshot failed: {e}"));
            return;
        }
        if let Err(e) = self.store.truncate_prefix(index + 1) {
            self.become_faulted(format!("wal compaction failed: {e}"));
            return;
        }
        let keep: Vec<Entry> = self.log.iter().filter(|e| e.index > index).cloned().collect();
        self.log = keep;
        self.snap_index = index;
        self.snap_term = term;
        events.push(Event::SnapshotCompacted { index });
    }

    fn handle_install_snapshot(&mut self, msg: Message, events: &mut Vec<Event>) {
        let (term, leader, snap) = match msg {
            Message::InstallSnapshot { term, leader, snapshot } => (term, leader, snapshot),
            _ => unreachable!(),
        };
        if term < self.term {
            events.push(Event::Send {
                to: leader,
                msg: Message::SnapshotResponse { term: self.term, from: self.id, index: snap.index, success: false },
            });
            return;
        }
        if term > self.term || self.role == Role::Candidate {
            self.become_follower(term, Some(leader));
        }
        self.leader = Some(leader);
        self.reset_election_timer(self.now_ms);

        if snap.index <= self.snap_index {
            events.push(Event::Send {
                to: leader,
                msg: Message::SnapshotResponse { term: self.term, from: self.id, index: self.snap_index, success: true },
            });
            return;
        }

        if let Err(e) = self.sm.restore(&snap.data) {
            self.become_faulted(format!("snapshot restore failed: {e}"));
            return;
        }
        if let Err(e) = self.store.write_snapshot(&snap) {
            self.become_faulted(format!("snapshot persist failed: {e}"));
            return;
        }
        if let Err(e) = self.store.truncate_prefix(snap.index + 1) {
            self.become_faulted(format!("wal truncate failed: {e}"));
            return;
        }
        // 丢弃快照覆盖区间的内存日志，保留其后的（正常应为空）
        self.log.retain(|e| e.index > snap.index);
        self.snap_index = snap.index;
        self.snap_term = snap.term;
        self.conf = snap.conf;
        self.conf_old = snap.conf_old;
        self.in_joint = self.conf_old.is_some();
        self.applied = self.applied.max(snap.index);
        self.commit = self.commit.max(snap.index);
        if let Err(e) = self.persist_hard_state() {
            self.become_faulted(format!("persist after snapshot failed: {e}"));
            return;
        }
        events.push(Event::Send {
            to: leader,
            msg: Message::SnapshotResponse { term: self.term, from: self.id, index: snap.index, success: true },
        });
    }

    // ---------------- 成员变更（联合共识） ----------------

    /// 发起成员变更。leader 先写 EnterJoint（C_old,new），其提交后再写
    /// LeaveJoint（C_new），两阶段期间集群始终可用，选举与提交都要求双多数派。
    pub fn propose_conf_change(&mut self, change: &ConfChange, events: &mut Vec<Event>)
        -> Result<(), String> {
        if self.role != Role::Leader {
            return Err(format!("not leader (leader={:?})", self.leader));
        }
        if self.pending_conf_change || self.in_joint {
            return Err("another membership change is in flight".into());
        }
        let mut voters = self.conf.voters.clone();
        match change {
            ConfChange::AddVoter { node } => {
                if voters.contains(node) {
                    return Err("node already a voter".into());
                }
                voters.push(*node);
            }
            ConfChange::RemoveVoter { node } => {
                if !voters.contains(node) {
                    return Err("node not a voter".into());
                }
                if voters.len() == 1 {
                    return Err("cannot remove the last voter".into());
                }
                voters.retain(|v| v != node);
            }
        }
        let target = ConfState::new(voters);
        self.pending_conf_change = true;
        let idx = self.last_log_index() + 1;
        self.append_entries(
            vec![Entry { term: self.term, index: idx, cmd: Command::EnterJoint { conf: target } }],
            events,
        );
        Ok(())
    }

    // ---------------- 客户端入口 ----------------

    pub fn client_proposal(&mut self, cmd: Command, client_req: ClientId, events: &mut Vec<Event>) {
        if self.role != Role::Leader {
            events.push(Event::NotLeader { leader: self.leader, client_req: Some(client_req) });
            return;
        }
        let idx = self.last_log_index() + 1;
        self.proposal_clients.insert(idx, client_req);
        self.append_entries(
            vec![Entry { term: self.term, index: idx, cmd }],
            events,
        );
    }

    pub fn client_read(&mut self, req: ReadRequest, events: &mut Vec<Event>) {
        if self.role != Role::Leader {
            events.push(Event::NotLeader { leader: self.leader, client_req: None });
            return;
        }
        // 租约模式且租约仍有效：跳过确认轮直接读（时钟前提见模块文档）
        if req.mode == ReadMode::Lease && self.now_ms < self.lease_expire_ms {
            self.lease_reads += 1;
            let value = self.sm_get(&req.key);
            events.push(Event::ReadResult(ReadResponse { req_id: req.req_id, value }));
            return;
        }
        // ReadIndex（或租约过期后回退）：一轮多数派确认
        self.readindex_reads += 1;
        self.read_counter += 1;
        let read_id = self.read_counter;
        let mut acks = HashSet::new();
        acks.insert(self.id);
        self.pending_reads.push_back(PendingRead {
            read_id,
            key: req.key,
            req_id: req.req_id,
            acks,
            term: self.term,
            read_index: None,
        });
        for peer in self.effective_voters().into_iter().filter(|p| *p != self.id) {
            events.push(Event::Send {
                to: peer,
                msg: Message::ReadIndexHeartbeat { term: self.term, leader: self.id, read_id },
            });
        }
        // 单节点集群直接满足
        self.try_serve_reads(events);
    }

    fn sm_get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.sm.get(key)
    }

    fn try_serve_reads(&mut self, events: &mut Vec<Event>) {
        if self.role != Role::Leader {
            return;
        }
        let current_term = self.term;
        let current_commit = self.commit;
        let applied = self.applied;

        // 阶段 1：定格每个读的 readIndex（拿到多数派确认的那一刻 commitIndex）
        // quorum 用纯局部计算，避免与 iter_mut 的借用冲突
        let quorum_of = |c: &ConfState| c.quorum();
        let new_quorum = quorum_of(&self.conf);
        let old_quorum = self.conf_old.as_ref().map(quorum_of);
        for pr in self.pending_reads.iter_mut() {
            if pr.term != current_term {
                continue;
            }
            let yes_new = pr.acks.iter().filter(|n| self.conf.contains(**n)).count();
            let quorum_ok = match old_quorum {
                Some(oq) => {
                    let yes_old = pr
                        .acks
                        .iter()
                        .filter(|n| self.conf_old.as_ref().unwrap().contains(**n))
                        .count();
                    yes_new >= new_quorum && yes_old >= oq
                }
                None => yes_new >= new_quorum,
            };
            if pr.read_index.is_none() && quorum_ok {
                pr.read_index = Some(current_commit);
            }
        }

        // 阶段 2：applied 追平 readIndex 后本地读；任期不符的读直接丢弃
        let mut drop_indexes = Vec::new();
        let mut to_read: Vec<(usize, u64, Vec<u8>)> = Vec::new();
        for (i, pr) in self.pending_reads.iter().enumerate() {
            if pr.term != current_term {
                drop_indexes.push(i);
                continue;
            }
            if let Some(ri) = pr.read_index {
                if applied >= ri {
                    to_read.push((i, pr.req_id, pr.key.clone()));
                }
            }
        }
        let mut results = Vec::new();
        for (i, req_id, key) in to_read {
            let value = self.sm.get(&key);
            results.push((req_id, value));
            drop_indexes.push(i);
        }
        for (req_id, value) in results {
            events.push(Event::ReadResult(ReadResponse { req_id, value }));
        }
        drop_indexes.sort_unstable_by(|a, b| b.cmp(a));
        drop_indexes.dedup();
        for i in drop_indexes {
            self.pending_reads.remove(i);
        }
    }

    // ---------------- 消息分发 ----------------

    fn step(&mut self, msg: Message, events: &mut Vec<Event>) {
        // 任何 RPC 里看到更高任期都先下台（客户端消息除外）
        let rpc_term = match &msg {
            Message::PreVoteRequest { term, .. }
            | Message::VoteRequest { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::InstallSnapshot { term, .. } => Some(*term),
            Message::PreVoteResponse { term, .. }
            | Message::VoteResponse { term, .. }
            | Message::AppendResponse { term, .. }
            | Message::SnapshotResponse { term, .. }
            | Message::ReadIndexHeartbeat { term, .. }
            | Message::ReadIndexAck { term, .. } => Some(*term),
            Message::ClientProposal { .. } | Message::ClientRead(_) => None,
        };
        if let Some(t) = rpc_term {
            if t > self.term && !matches!(msg, Message::PreVoteRequest { .. } | Message::PreVoteResponse { .. }) {
                self.become_follower(t, None);
            }
        }

        match msg {
            Message::PreVoteRequest { term, candidate, last_log_index, last_log_term, .. } => {
                self.handle_pre_vote_req(term, candidate, last_log_index, last_log_term, events);
            }
            Message::PreVoteResponse { term, from, granted } => {
                // pre-vote 阶段可能是首次发起（follower），也可能是 candidate
                // 平票超时后的重选；两种身份都要接受响应。
                if !self.pre_voting || self.role == Role::Leader {
                    let _ = term;
                    return;
                }
                if granted {
                    self.pre_votes.insert(from);
                    if self.has_quorum(&self.pre_votes) {
                        self.start_campaign(events);
                    }
                }
            }
            Message::VoteRequest { term, candidate, last_log_index, last_log_term, .. } => {
                self.handle_vote_req(term, candidate, last_log_index, last_log_term, events);
            }
            Message::VoteResponse { from, term, granted } => {
                if self.role != Role::Candidate || term != self.term {
                    return;
                }
                if granted {
                    self.votes.insert(from);
                    if self.has_quorum(&self.votes) {
                        self.become_leader(events);
                    }
                }
            }
            Message::AppendEntries { .. } => self.handle_append_req(msg, events),
            Message::AppendResponse { term, from, match_index, success, conflict_term, conflict_index } => {
                if self.role != Role::Leader || term != self.term {
                    if term > self.term {
                        self.become_follower(term, None);
                    }
                    return;
                }
                self.handle_append_resp(from, success, match_index, conflict_term, conflict_index, events);
            }
            Message::InstallSnapshot { .. } => self.handle_install_snapshot(msg, events),
            Message::SnapshotResponse { term, from, index, success } => {
                if self.role != Role::Leader || term != self.term {
                    return;
                }
                if success {
                    self.match_index.insert(from, index);
                    self.next_index.insert(from, index + 1);
                    self.maybe_advance_commit(events);
                    self.replicate_to(from, events);
                } else {
                    self.replicate_to(from, events);
                }
            }
            Message::ReadIndexHeartbeat { term, leader, read_id } => {
                if term < self.term {
                    return;
                }
                events.push(Event::Send {
                    to: leader,
                    msg: Message::ReadIndexAck { term: self.term, from: self.id, read_id },
                });
            }
            Message::ReadIndexAck { from, term, read_id } => {
                if self.role != Role::Leader {
                    return;
                }
                for pr in self.pending_reads.iter_mut() {
                    // 只接受同任期、当前成员的确认
                    if pr.read_id == read_id && pr.term == term && self.conf.contains(from) {
                        pr.acks.insert(from);
                    }
                }
                self.try_serve_reads(events);
            }
            Message::ClientProposal { cmd, client_req } => {
                self.client_proposal(cmd, client_req, events);
            }
            Message::ClientRead(req) => self.client_read(req, events),
        }
    }

    // ---------------- 页面/调试观测 ----------------

    pub fn snapshot_index(&self) -> u64 {
        self.snap_index
    }
    pub fn log_len(&self) -> usize {
        self.log.len()
    }
    pub fn leader_hint(&self) -> Option<NodeId> {
        self.leader
    }
    pub fn pending_read_count(&self) -> usize {
        self.pending_reads.len()
    }
    pub fn next_index(&self) -> &BTreeMap<NodeId, LogIndex> {
        &self.next_index
    }
    pub fn match_index(&self) -> &BTreeMap<NodeId, LogIndex> {
        &self.match_index
    }
    pub fn lease_valid(&self, now: u64) -> bool {
        self.role == Role::Leader && now < self.lease_expire_ms
    }

    /// 下一次需要被驱动层唤醒的绝对时间（模拟层跳时钟用）。
    pub fn next_wakeup_ms(&self) -> u64 {
        match self.role {
            Role::Leader => self.last_heartbeat_sent + self.cfg.heartbeat_interval_ms,
            Role::Faulted => u64::MAX,
            _ => self.election_deadline_ms,
        }
    }

    /// 关闭节点并交出存储（模拟层重启/损坏注入用）。
    pub fn into_store(self) -> RaftStore {
        self.store
    }

    /// 主动破坏磁盘内容，下次 `verify_integrity` 必须能抓到。
    pub fn corrupt_disk(&mut self, kind: crate::storage::CorruptionKind) -> Result<(), StoreError> {
        self.store.inject_corruption(kind)
    }

    /// 租约到期时间（页面展示用）。
    pub fn lease_expire_ms(&self) -> u64 {
        self.lease_expire_ms
    }
}
