//! 确定性网络模拟层:所有随机性来自一个种子(丢包、延迟、分区、重启、
//! 磁盘损坏、客户端操作序列),同一种子完全可复现。
//! 跑完一个场景后,把客户端操作历史交给线性一致性检查器验证,
//! 并检查各副本状态机收敛。
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::checker::{check_linearizable, Op, OpKind};
use crate::node::{ClientReq, ClientResp, Consistency, KvOp, Node};
use crate::raft::{Envelope, GroupId, NodeId, Role, META_GROUP};
use crate::stats::{new_stats, StatsHandle};
use crate::storage::MemFactory;

// ---------------- 模拟网络 ----------------

enum SimEvent {
    Peer(Envelope),
    ClientReqEv { to: NodeId, from_client: u64, req: ClientReq },
    ClientRespEv { to_client: u64, resp: ClientResp },
}

pub struct SimNet {
    events: BinaryHeap<std::cmp::Reverse<(u64, u64, usize)>>,
    payloads: Vec<Option<SimEvent>>,
    seq: u64,
    pub drop_rate: f64,
    pub min_latency: u64,
    pub max_latency: u64,
    pub blocked: HashSet<(NodeId, NodeId)>,
}

impl SimNet {
    fn new() -> SimNet {
        SimNet {
            events: BinaryHeap::new(),
            payloads: Vec::new(),
            seq: 0,
            drop_rate: 0.0,
            min_latency: 1,
            max_latency: 3,
            blocked: HashSet::new(),
        }
    }

    fn send(&mut self, now: u64, rng: &mut StdRng, from: NodeId, to: NodeId, ev: SimEvent) {
        if self.blocked.contains(&(from, to)) {
            return;
        }
        if rng.gen::<f64>() < self.drop_rate {
            return;
        }
        let lat = self.min_latency + rng.gen_range(0..=(self.max_latency - self.min_latency));
        self.payloads.push(Some(ev));
        let idx = self.payloads.len() - 1;
        self.seq += 1;
        self.events.push(std::cmp::Reverse((now + lat, self.seq, idx)));
    }

    fn pop_due(&mut self, now: u64) -> Option<SimEvent> {
        if let Some(std::cmp::Reverse((t, _, _))) = self.events.peek() {
            if *t <= now {
                let std::cmp::Reverse((_, _, idx)) = self.events.pop().unwrap();
                return self.payloads[idx].take();
            }
        }
        None
    }
}

// ---------------- 模拟客户端 ----------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpState {
    NeedDirectory,
    NeedLeader,
    Waiting,
}

pub struct PendingOp {
    pub op: Op,
    pub state: OpState,
    pub req_id: u64,
    pub sent_to: NodeId,
    pub sent_at: u64,
    pub group: GroupId,
    pub consistency: Consistency,
    /// 去重序号:同一逻辑操作的所有重试共用
    pub op_seq: u64,
}

pub struct SimClient {
    pub id: u64,
    pub pending: Option<PendingOp>,
    pub directory: Vec<crate::node::ShardMeta>,
    pub leaders: HashMap<GroupId, NodeId>,
    pub next_req: u64,
    next_op_seq: u64,
    rng: StdRng,
}

const REQ_TIMEOUT: u64 = 40;

impl SimClient {
    fn new(id: u64, seed: u64) -> SimClient {
        SimClient {
            id,
            directory: vec![],
            leaders: HashMap::new(),
            next_req: 1,
            next_op_seq: 1,
            pending: None,
            rng: StdRng::seed_from_u64(seed),
        }
    }

    fn start_op(&mut self, now: u64, key_space: usize, op_seq: u64) {
        let key = format!("key-{:03}", self.rng.gen_range(0..key_space)).into_bytes();
        let r: f64 = self.rng.gen();
        let kind = if r < 0.4 {
            OpKind::Put(format!("val-{}-{}", self.id, op_seq).into_bytes())
        } else if r < 0.8 {
            OpKind::Get
        } else {
            OpKind::Del
        };
        let consistency = if self.rng.gen::<f64>() < 0.7 {
            Consistency::Linearizable
        } else {
            Consistency::Lease
        };
        let read_path = match consistency {
            Consistency::Linearizable => Some("readindex".to_string()),
            Consistency::Lease => Some("lease".to_string()),
            Consistency::Stale => Some("stale".to_string()),
        };
        let op_seq = self.next_op_seq;
        self.next_op_seq += 1;
        self.pending = Some(PendingOp {
            op_seq,
            op: Op {
                key,
                kind,
                resp: None,
                start: now,
                end: 0,
                read_path,
                indeterminate: false,
            },
            state: OpState::NeedDirectory,
            req_id: 0,
            sent_to: 0,
            sent_at: 0,
            group: 0,
            consistency,
        });
    }

    /// 主循环里每个 tick 驱动一次
    fn step(&mut self, sim: &mut Sim) {
        let (nodes, now) = (sim.node_ids.clone(), sim.now);
        let p = match &mut self.pending {
            Some(p) => p,
            None => return,
        };
        // 请求超时:换路重试(主切换期间不报错,等新主选出来再发)
        if p.req_id != 0 && now - p.sent_at > REQ_TIMEOUT {
            p.req_id = 0;
            if p.state == OpState::Waiting {
                p.state = OpState::NeedLeader;
            }
        }
        if p.req_id != 0 {
            return; // 有在途请求,等响应
        }
        match p.state {
            OpState::NeedDirectory => {
                if let Some(to) = pick(&nodes, &mut self.rng) {
                    let req_id = self.next_req;
                    self.next_req += 1;
                    sim.send_client_req(to, self.id, ClientReq::Directory { req_id });
                    p.req_id = req_id;
                    p.sent_to = to;
                    p.sent_at = now;
                }
            }
            OpState::NeedLeader => {
                // 用缓存目录找分片;没有就先刷目录
                let key = p.op.key.clone();
                let gid = self
                    .directory
                    .iter()
                    .find(|s| key >= s.start && (s.end.is_empty() || key < s.end))
                    .map(|s| s.id);
                match gid {
                    None => p.state = OpState::NeedDirectory,
                    Some(gid) => {
                        p.group = gid;
                        if let Some(&leader) = self.leaders.get(&gid) {
                            let req_id = self.next_req;
                            self.next_req += 1;
                            let op = match &p.op.kind {
                                OpKind::Get => KvOp::Get,
                                OpKind::Put(v) => KvOp::Put(v.clone()),
                                OpKind::Del => KvOp::Del,
                            };
                            sim.send_client_req(
                                leader,
                                self.id,
                                ClientReq::Kv {
                                    req_id,
                                    key: p.op.key.clone(),
                                    op,
                                    consistency: p.consistency,
                                    client_id: self.id,
                                    op_seq: p.op_seq,
                                },
                            );
                            p.req_id = req_id;
                            p.sent_to = leader;
                            p.sent_at = now;
                            p.state = OpState::Waiting;
                        } else if let Some(to) = pick(&nodes, &mut self.rng) {
                            let req_id = self.next_req;
                            self.next_req += 1;
                            sim.send_client_req(to, self.id, ClientReq::AskLeader { req_id, group: gid });
                            p.req_id = req_id;
                            p.sent_to = to;
                            p.sent_at = now;
                        }
                    }
                }
            }
            OpState::Waiting => {}
        }
    }

    fn on_resp(&mut self, sim: &mut Sim, resp: ClientResp) {
        let p = match &mut self.pending {
            Some(p) => p,
            None => return,
        };
        match resp {
            ClientResp::Directory { req_id, shards } => {
                if p.state == OpState::NeedDirectory && req_id == p.req_id {
                    if !shards.is_empty() {
                        self.directory = shards;
                        p.state = OpState::NeedLeader;
                        p.req_id = 0;
                    } else {
                        p.req_id = 0; // 对端还没目录,重试
                    }
                }
            }
            ClientResp::Leader { req_id, leader } => {
                if req_id == p.req_id {
                    match leader {
                        Some(l) => {
                            self.leaders.insert(p.group, l);
                            p.req_id = 0;
                        }
                        None => p.req_id = 0, // 还没选出来,下个 tick 再问
                    }
                }
            }
            ClientResp::Kv { req_id, result } => {
                if p.state != OpState::Waiting || req_id != p.req_id {
                    return;
                }
                match result {
                    Ok(v) => {
                        let mut done = self.pending.take().unwrap();
                        done.op.resp = v;
                        done.op.end = sim.now;
                        sim.history.push(done.op);
                    }
                    Err(crate::node::KvError::NotLeader(hint)) => {
                        if let Some(l) = hint {
                            self.leaders.insert(p.group, l);
                        } else {
                            self.leaders.remove(&p.group);
                        }
                        p.state = OpState::NeedLeader;
                        p.req_id = 0;
                    }
                    Err(crate::node::KvError::WrongShard) => {
                        p.state = OpState::NeedDirectory;
                        p.req_id = 0;
                    }
                    Err(crate::node::KvError::NotReady) => {
                        p.req_id = 0; // 留在 Waiting 外的状态重发
                        p.state = OpState::NeedLeader;
                    }
                }
            }
        }
    }
}

fn pick(v: &[NodeId], rng: &mut StdRng) -> Option<NodeId> {
    if v.is_empty() {
        None
    } else {
        Some(v[rng.gen_range(0..v.len())])
    }
}

// ---------------- 模拟器 ----------------

pub struct Sim {
    pub now: u64,
    pub rng: StdRng,
    pub nodes: HashMap<NodeId, SimNode>,
    pub node_ids: Vec<NodeId>,
    pub net: SimNet,
    pub factory: MemFactory,
    pub clients: Vec<SimClient>,
    pub history: Vec<Op>,
    pub stats: StatsHandle,
    pub seed: u64,
    pub snapshot_threshold: u64,
    pub split_threshold: usize,
    // 故障计划
    partitions: Vec<(u64, HashSet<(NodeId, NodeId)>)>, // (heal_at, pairs)
    restarts: Vec<(u64, NodeId)>,                      // (boot_at, node)
    /// 发给未注册客户端的响应(测试直接驱动节点时用)
    pub unclaimed_resps: Vec<(u64, ClientResp)>,
}

pub struct SimNode {
    pub node: Node,
    pub up: bool,
}

#[derive(Clone, Copy)]
pub struct ScenarioCfg {
    pub nodes: usize,
    pub ticks: u64,
    pub clients: usize,
    pub max_ops: usize,
    pub key_space: usize,
    pub faults: bool,
    pub split_threshold: usize,
    pub snapshot_threshold: u64,
}

impl Default for ScenarioCfg {
    fn default() -> Self {
        ScenarioCfg {
            nodes: 3,
            ticks: 3000,
            clients: 3,
            max_ops: 120,
            key_space: 12,
            faults: true,
            split_threshold: 4096,
            snapshot_threshold: 40,
        }
    }
}

pub struct ScenarioResult {
    pub seed: u64,
    pub ok: bool,
    pub ops: usize,
    pub failures: Vec<String>,
}

impl Sim {
    pub fn new(seed: u64, cfg: &ScenarioCfg) -> Sim {
        let mut rng = StdRng::seed_from_u64(seed);
        let stats = new_stats();
        let factory = MemFactory::default();
        let node_ids: Vec<NodeId> = (1..=cfg.nodes as u64).collect();
        let mut nodes = HashMap::new();
        for &id in &node_ids {
            let node_seed = rng.gen::<u64>();
            let node = Node::bootstrap(
                id,
                node_ids.clone(),
                Box::new(factory.clone()),
                node_seed,
                Some(stats.clone()),
                cfg.snapshot_threshold,
                cfg.split_threshold,
            );
            nodes.insert(id, SimNode { node, up: true });
        }
        let clients = (0..cfg.clients)
            .map(|i| SimClient::new(10_000 + i as u64, rng.gen()))
            .collect();
        Sim {
            now: 0,
            rng,
            nodes,
            node_ids,
            net: SimNet::new(),
            factory,
            clients,
            history: vec![],
            stats,
            seed,
            snapshot_threshold: cfg.snapshot_threshold,
            split_threshold: cfg.split_threshold,
            partitions: vec![],
            restarts: vec![],
            unclaimed_resps: vec![],
        }
    }

    pub fn send_client_req(&mut self, to: NodeId, from_client: u64, req: ClientReq) {
        let rng = &mut self.rng;
        let now = self.now;
        self.net.send(now, rng, from_client, to, SimEvent::ClientReqEv { to, from_client, req });
    }

    /// 主循环推进一步
    pub fn step(&mut self) {
        self.now += 1;
        self.stats.lock().unwrap().now_ms = self.now;
        // 1. 所有存活节点 tick + advance
        let ids = self.node_ids.clone();
        for id in &ids {
            let up = self.nodes.get(id).map(|n| n.up).unwrap_or(false);
            if !up {
                continue;
            }
            let sim_node = self.nodes.get_mut(id).unwrap();
            sim_node.node.tick();
            sim_node.node.advance();
        }
        // 2. 收集外发消息进网络
        let mut envelopes = Vec::new();
        let mut client_resps = Vec::new();
        for id in &ids {
            let sim_node = self.nodes.get_mut(id).unwrap();
            envelopes.extend(sim_node.node.outbox.drain(..));
            client_resps.extend(sim_node.node.client_outbox.drain(..));
        }
        let now = self.now;
        for env in envelopes {
            let (from, to) = (env.from, env.to);
            self.net.send(now, &mut self.rng, from, to, SimEvent::Peer(env));
        }
        for (client, resp) in client_resps {
            self.net.send(now, &mut self.rng, *self.node_ids.first().unwrap(), client, SimEvent::ClientRespEv { to_client: client, resp });
        }
        // 3. 投递到期消息
        while let Some(ev) = self.net.pop_due(self.now) {
            match ev {
                SimEvent::Peer(env) => {
                    if let Some(sn) = self.nodes.get_mut(&env.to) {
                        if sn.up {
                            sn.node.handle_envelope(env);
                        }
                    }
                }
                SimEvent::ClientReqEv { to, from_client, req } => {
                    if let Some(sn) = self.nodes.get_mut(&to) {
                        if sn.up {
                            sn.node.handle_client_req(from_client, req);
                        }
                    }
                }
                SimEvent::ClientRespEv { to_client, resp } => {
                    let mut clients = std::mem::take(&mut self.clients);
                    let mut claimed = false;
                    if let Some(c) = clients.iter_mut().find(|c| c.id == to_client) {
                        c.on_resp(self, resp.clone());
                        claimed = true;
                    }
                    self.clients = clients;
                    if !claimed {
                        self.unclaimed_resps.push((to_client, resp));
                    }
                }
            }
        }
        // 4. 处理计划中的故障恢复
        let partitions = std::mem::take(&mut self.partitions);
        let (due, keep): (Vec<_>, Vec<_>) = partitions.into_iter().partition(|(at, _)| *at <= self.now);
        self.partitions = keep;
        for (_, pairs) in due {
            for p in pairs {
                self.net.blocked.remove(&p);
            }
        }
        let restarts = std::mem::take(&mut self.restarts);
        let (due, keep): (Vec<_>, Vec<_>) = restarts.into_iter().partition(|(at, _)| *at <= self.now);
        self.restarts = keep;
        for (_, id) in due {
            self.boot_node(id);
        }
    }

    pub fn boot_node(&mut self, id: NodeId) {
        let seed = self.rng.gen::<u64>();
        let node = Node::recover(
            id,
            Box::new(self.factory.clone()),
            seed,
            Some(self.stats.clone()),
            self.snapshot_threshold,
            self.split_threshold,
        );
        self.nodes.insert(id, SimNode { node, up: true });
    }

    pub fn crash_node(&mut self, id: NodeId) {
        if let Some(sn) = self.nodes.get_mut(&id) {
            sn.up = false;
        }
        let boot_at = self.now + 20 + self.rng.gen_range(0..50);
        self.restarts.push((boot_at, id));
    }

    /// 随机分区:把节点切成两块,双向断流,持续一段时间
    pub fn inject_partition(&mut self) {
        let mut shuffled = self.node_ids.clone();
        for i in (1..shuffled.len()).rev() {
            let j = self.rng.gen_range(0..=i);
            shuffled.swap(i, j);
        }
        let cut = self.rng.gen_range(1..shuffled.len());
        let (a, b) = shuffled.split_at(cut);
        let mut pairs = HashSet::new();
        for x in a {
            for y in b {
                pairs.insert((*x, *y));
                pairs.insert((*y, *x));
            }
        }
        for p in &pairs {
            self.net.blocked.insert(*p);
        }
        let heal_at = self.now + 100 + self.rng.gen_range(0..200);
        self.partitions.push((heal_at, pairs));
    }

    pub fn heal_all(&mut self) {
        self.net.blocked.clear();
        self.partitions.clear();
    }

    /// 注入磁盘损坏并重启该节点:恢复后对应组必须拒绝投票
    pub fn inject_corruption(&mut self, id: NodeId) {
        let mut groups: Vec<GroupId> = self.nodes.get(&id).map(|sn| sn.node.groups.keys().copied().collect()).unwrap_or_default();
        groups.sort_unstable(); // 固定顺序,保证种子可复现
        if groups.is_empty() {
            return;
        }
        let g = groups[self.rng.gen_range(0..groups.len())];
        self.factory.corrupt(id, g, "injected bitrot: crc mismatch");
        self.crash_node(id);
    }

    pub fn leader_of(&self, group: GroupId) -> Option<NodeId> {
        self.node_ids.iter().copied().find(|id| {
            self.nodes
                .get(id)
                .map(|sn| sn.up && sn.node.groups.get(&group).map(|h| h.raft.role == Role::Leader).unwrap_or(false))
                .unwrap_or(false)
        })
    }

    /// 驱动所有客户端一步:发新操作、推进在途操作
    pub fn drive_clients(&mut self, max_ops: usize, key_space: usize, ops_issued: &mut usize, op_seq: &mut u64) {
        let mut clients = std::mem::take(&mut self.clients);
        let now = self.now;
        for c in clients.iter_mut() {
            if c.pending.is_none() && *ops_issued < max_ops {
                c.start_op(now, key_space, *op_seq);
                *op_seq += 1;
                *ops_issued += 1;
            }
            c.step(self);
        }
        self.clients = clients;
    }
}

/// 跑一个完整随机故障场景
pub fn run_scenario(seed: u64, cfg: &ScenarioCfg) -> ScenarioResult {
    let mut sim = Sim::new(seed, cfg);
    let mut failures = Vec::new();
    let mut ops_issued = 0usize;
    let mut op_seq = 0u64;

    // 预热:等到选出稳定的领导者(分裂投票可能多轮,给足余量)
    let mut warmup = 0u64;
    while sim.leader_of(META_GROUP).is_none() && warmup < 2000 {
        sim.step();
        warmup += 1;
    }
    if sim.leader_of(META_GROUP).is_none() {
        failures.push("no meta leader after 2000-tick warmup".into());
    }
    for _ in 0..50 {
        sim.step();
    }

    for _ in 0..cfg.ticks {
        sim.step();
        // 客户端驱动
        sim.drive_clients(cfg.max_ops, cfg.key_space, &mut ops_issued, &mut op_seq);
        // 随机故障注入
        if cfg.faults {
            let r: f64 = sim.rng.gen();
            if r < 0.002 {
                sim.inject_partition();
            } else if r < 0.005 {
                let id = sim.node_ids[sim.rng.gen_range(0..sim.node_ids.len())];
                sim.crash_node(id);
            } else if r < 0.0052 {
                let id = sim.node_ids[sim.rng.gen_range(0..sim.node_ids.len())];
                sim.inject_corruption(id);
            }
        }
    }

    // 收尾:愈合分区、停止发新操作,让在途操作完成、副本收敛
    sim.heal_all();
    for _ in 0..400 {
        sim.step();
        sim.drive_clients(0, cfg.key_space, &mut ops_issued, &mut op_seq);
    }

    // 没完成的操作:已发起但客户端没等到回执,效果可能已提交也可能没有。
    // 记为不确定操作交给检查器(两种可能都尝试)。
    for c in &sim.clients {
        if let Some(p) = &c.pending {
            let mut op = p.op.clone();
            op.end = sim.now;
            op.indeterminate = true;
            sim.history.push(op);
        }
    }

    // 检查 1:线性一致性
    match check_linearizable(&sim.history) {
        Ok(()) => {}
        Err(key) => failures.push(format!(
            "linearizability violation on key {}",
            String::from_utf8_lossy(&key)
        )),
    }

    // 检查 2:副本状态机收敛(同组所有健康节点最终状态一致)
    let mut group_states: HashMap<GroupId, Vec<(NodeId, u64)>> = HashMap::new();
    for id in &sim.node_ids {
        let sn = match sim.nodes.get(id) {
            Some(sn) if sn.up => sn,
            _ => continue,
        };
        for (gid, host) in &sn.node.groups {
            let refused = host.raft.role == Role::Refused;
            if !refused {
                group_states.entry(*gid).or_default().push((*id, host.sm.hash()));
            }
        }
    }
    for (gid, states) in &group_states {
        let hashes: HashSet<u64> = states.iter().map(|(_, h)| *h).collect();
        if hashes.len() > 1 {
            failures.push(format!(
                "group {} replicas diverged: {:?}",
                gid,
                states.iter().map(|(n, h)| format!("n{}:{:x}", n, h)).collect::<Vec<_>>()
            ));
        }
    }

    ScenarioResult {
        seed,
        ok: failures.is_empty(),
        ops: sim.history.len(),
        failures,
    }
}

/// 批量跑场景,返回失败的种子
pub fn run_soak(base_seed: u64, scenarios: usize, cfg: &ScenarioCfg, threads: usize) -> (usize, Vec<ScenarioResult>) {
    let mut handles = Vec::new();
    let chunk = (scenarios + threads - 1) / threads;
    let mut total_ops = 0usize;
    for t in 0..threads {
        let start = t * chunk;
        let end = ((t + 1) * chunk).min(scenarios);
        if start >= end {
            break;
        }
        let cfg = ScenarioCfg { ..*cfg };
        handles.push(std::thread::spawn(move || {
            let mut fails = Vec::new();
            let mut ops = 0usize;
            for i in start..end {
                let seed = base_seed + i as u64;
                let r = run_scenario(seed, &cfg);
                ops += r.ops;
                if !r.ok {
                    fails.push(r);
                }
            }
            (ops, fails)
        }));
    }
    let mut failures = Vec::new();
    for h in handles {
        let (ops, fails) = h.join().unwrap();
        total_ops += ops;
        failures.extend(fails);
    }
    (total_ops, failures)
}
