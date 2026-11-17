//! 随机故障场景：按种子生成一整套"客户端压力 + 网络/节点/磁盘故障"剧本，
//! 跑完用线性一致性检查器裁决。失败时落盘证据，可用同种子完整复现。

use std::fs;
use std::path::Path;

use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;

use crate::checker::{check, CheckReport};
use crate::raft::{ConfChange, ReadMode};
use crate::sim::{GroupId, SimCluster, SimConfig, SimStats};
use crate::storage::CorruptionKind;

#[derive(Debug, Clone, serde::Serialize)]
pub struct ScenarioConfig {
    pub node_count: u64,
    pub group: GroupId,
    pub duration_ms: u64,
    pub clients: u64,
    /// 不同 key 的数量（key = "key-0007"）
    pub key_space: u32,
    pub write_pct: u32,
    pub read_mode: ReadMode,
    pub fault_interval_ms: u64,
    pub enable_partitions: bool,
    pub enable_restarts: bool,
    pub enable_corruption: bool,
    pub loss_pct: u32,
}

impl Default for ScenarioConfig {
    fn default() -> Self {
        ScenarioConfig {
            node_count: 5,
            group: 1,
            duration_ms: 20_000,
            clients: 4,
            key_space: 12,
            write_pct: 40,
            read_mode: ReadMode::ReadIndex,
            fault_interval_ms: 800,
            enable_partitions: true,
            enable_restarts: true,
            enable_corruption: true,
            loss_pct: 8,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ScenarioResult {
    pub seed: u64,
    pub linearizable: bool,
    pub inconclusive: bool,
    pub violation_count: usize,
    pub states_explored: usize,
    pub stats: SimStats,
    pub fault_events: usize,
    pub evidence_file: Option<String>,
}

/// 调度器执行的原子事件，全部带绝对时间点，按序执行。
#[derive(Debug, Clone)]
enum FaultEvent {
    Partition(Vec<Vec<u64>>),
    Heal,
    Stop(u64),
    Start(u64),
    Corrupt(u64, CorruptionKind),
    Repair(u64, GroupId),
}

fn pick_corruption(rng: &mut Pcg64) -> CorruptionKind {
    match rng.gen_range(0..4) {
        0 => CorruptionKind::FlipHardState,
        1 => CorruptionKind::FlipSnapshot,
        2 => CorruptionKind::TruncateWalTail,
        _ => CorruptionKind::RemoveCrc,
    }
}

/// 由种子确定性地生成故障剧本。
fn build_schedule(rng: &mut Pcg64, sc: &ScenarioConfig, peers: &[u64]) -> Vec<(u64, FaultEvent)> {
    let mut schedule: Vec<(u64, FaultEvent)> = Vec::new();
    let mut t = sc.fault_interval_ms;
    while t < sc.duration_ms {
        let roll: u32 = rng.gen_range(0..100);
        if sc.enable_corruption && roll < 20 {
            // 停机 -> 破坏磁盘 -> 重启（被拒，Faulted）-> 修复（空白节点靠快照追上）
            let node = rng.gen_range(1..=sc.node_count);
            let kind = pick_corruption(rng);
            schedule.push((t, FaultEvent::Stop(node)));
            schedule.push((t + 100, FaultEvent::Corrupt(node, kind)));
            schedule.push((t + 400, FaultEvent::Start(node)));
            schedule.push((t + 1100, FaultEvent::Repair(node, sc.group)));
        } else if sc.enable_partitions && roll < 55 {
            let mut pool = peers.to_vec();
            let mut left = Vec::new();
            while left.len() < pool.len() - 1 && rng.gen_bool(0.5) {
                let idx = rng.gen_range(0..pool.len());
                left.push(pool.remove(idx));
            }
            if left.is_empty() {
                left.push(pool.remove(0));
            }
            let heal_at = t + rng.gen_range(400..1500);
            schedule.push((t, FaultEvent::Partition(vec![left, pool])));
            schedule.push((heal_at, FaultEvent::Heal));
        } else if sc.enable_restarts {
            let node = rng.gen_range(1..=sc.node_count);
            let down = rng.gen_range(300..1200);
            schedule.push((t, FaultEvent::Stop(node)));
            schedule.push((t + down, FaultEvent::Start(node)));
        }
        t += sc.fault_interval_ms.max(1000);
    }
    schedule.sort_by_key(|(at, _)| *at);
    schedule
}

/// 核心执行：构造集群、跑剧本、施加压力、收尾。所有随机源都来自种子。
pub(crate) fn execute_cluster(
    base_dir: &Path,
    seed: u64,
    sc: &ScenarioConfig,
    raft_cfg: crate::raft::RaftConfig,
) -> SimCluster {
    // 场景随机源与集群网络随机源分离但都派生自种子，保证同种子逐帧可复现
    let mut rng = Pcg64::seed_from_u64(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1));
    let sim_cfg = SimConfig {
        seed,
        loss_pct: sc.loss_pct,
        duplicate_pct: 2,
        delay_min_ms: 5,
        delay_max_ms: 35,
        tick_ms: 10,
    };
    let mut cluster = SimCluster::new(base_dir, sim_cfg, raft_cfg);
    let peers: Vec<u64> = (1..=sc.node_count).collect();
    cluster.add_group(sc.group, peers.clone());

    let schedule = build_schedule(&mut rng, sc, &peers);
    let fault_events = schedule.len();
    let mut remaining = schedule;

    let tick = 100u64;
    let mut now = 0u64;
    let mut op_seq = 0u64;
    while now < sc.duration_ms {
        now += tick;
        cluster.advance_to(now);

        while let Some((at, _)) = remaining.first() {
            if *at > now {
                break;
            }
            let (_, ev) = remaining.remove(0);
            apply(&mut cluster, ev);
        }

        // 每个客户端每轮发 1~2 个操作
        for c in 0..sc.clients {
            for _ in 0..rng.gen_range(1..=2) {
                let key_idx = rng.gen_range(0..sc.key_space);
                let key = format!("key-{key_idx:04}").into_bytes();
                if rng.gen_ratio(sc.write_pct.min(100), 100) {
                    op_seq += 1;
                    if rng.gen_bool(0.85) {
                        let value = format!("v{seed}-{op_seq}").into_bytes();
                        cluster.client_put(c, sc.group, key, value);
                    } else {
                        cluster.client_delete(c, sc.group, key);
                    }
                } else {
                    cluster.client_read(c, sc.group, key, sc.read_mode);
                }
            }
        }
    }

    // 收尾：愈合分区、拉起所有节点，等 pending 全部完成
    cluster.heal_partition();
    for n in 1..=sc.node_count {
        cluster.restart_node(n);
    }
    cluster.advance_to(now + 8_000);
    let mut extra = 0;
    while cluster.pending_count() > 0 && extra < 20 {
        cluster.advance_to(cluster.now + 1_000);
        extra += 1;
    }
    let _ = fault_events;
    cluster
}

fn apply(cluster: &mut SimCluster, ev: FaultEvent) {
    match ev {
        FaultEvent::Partition(parts) => cluster.partition(parts),
        FaultEvent::Heal => cluster.heal_partition(),
        FaultEvent::Stop(node) => cluster.shutdown_node(node),
        FaultEvent::Start(node) => cluster.restart_node(node),
        FaultEvent::Corrupt(node, kind) => {
            if let Some(g) = cluster.first_group_of(node) {
                let _ = cluster.corrupt_disk(node, g, kind);
            }
        }
        FaultEvent::Repair(node, g) => cluster.repair_node(node, g),
    }
}/// 跑单个场景，返回裁决。
pub fn run_scenario(
    base_dir: &Path,
    seed: u64,
    sc: &ScenarioConfig,
    raft_cfg: crate::raft::RaftConfig,
) -> ScenarioResult {
    let cluster = execute_cluster(base_dir, seed, sc, raft_cfg);
    let report = check(cluster.history());
    let stats = cluster.stats();
    let fault_events = cluster.event_log().iter().filter(|l| {
        l.contains("PARTITION") || l.contains("SHUTDOWN") || l.contains("CORRUPTED")
            || l.contains("REPAIRED") || l.contains("RESTART")
    }).count();

    let mut evidence_file = None;
    if !report.ok {
        let path = base_dir.join(format!("failure-seed-{seed}.json"));
        let evidence = serde_json::json!({
            "seed": seed,
            "scenario": serde_json::to_value(sc).unwrap_or(serde_json::Value::Null),
            "violations": report.violations,
            "stats": stats,
            "event_log_tail": cluster.event_log().iter().rev().take(200).collect::<Vec<_>>(),
            "history_len": cluster.history().len(),
        });
        fs::write(&path, serde_json::to_string_pretty(&evidence).unwrap()).unwrap();
        evidence_file = Some(path.display().to_string());
    }

    ScenarioResult {
        seed,
        linearizable: report.ok,
        inconclusive: report.inconclusive,
        violation_count: report.violations.len(),
        states_explored: report.states_explored,
        stats,
        fault_events,
        evidence_file,
    }
}

/// 跑场景并返回跑完的集群（dashboard 复现用，确定性）。
pub fn run_scenario_return_cluster(
    base_dir: &Path,
    seed: u64,
    sc: &ScenarioConfig,
    raft_cfg: crate::raft::RaftConfig,
) -> SimCluster {
    execute_cluster(base_dir, seed, sc, raft_cfg)
}

/// 调试/复现：跑场景并同时返回完整操作历史（与 run_scenario 共用同一执行核心）。
pub fn run_scenario_collect_history(
    base_dir: &Path,
    seed: u64,
    sc: &ScenarioConfig,
    raft_cfg: crate::raft::RaftConfig,
) -> (ScenarioResult, Vec<crate::checker::Op>) {
    let cluster = execute_cluster(base_dir, seed, sc, raft_cfg);
    let history = cluster.history().to_vec();
    let report: CheckReport = check(&history);
    let stats = cluster.stats();
    let result = ScenarioResult {
        seed,
        linearizable: report.ok,
        inconclusive: report.inconclusive,
        violation_count: report.violations.len(),
        states_explored: report.states_explored,
        stats,
        fault_events: 0,
        evidence_file: None,
    };
    (result, history)
}

/// 批量跑 N 个种子场景，返回每个场景的裁决。
pub fn run_many(
    base_dir: &Path,
    start_seed: u64,
    count: u64,
    sc: &ScenarioConfig,
    raft_cfg: crate::raft::RaftConfig,
) -> Vec<ScenarioResult> {
    let mut out = Vec::new();
    for i in 0..count {
        let seed = start_seed + i;
        let dir = base_dir.join(format!("seed-{seed}"));
        fs::create_dir_all(&dir).unwrap();
        out.push(run_scenario(&dir, seed, sc, raft_cfg.clone()));
    }
    out
}

/// 成员变更测试专用：向某组的 leader 发起联合共识变更。
pub fn propose_conf_change(
    cluster: &mut SimCluster,
    group: GroupId,
    change: ConfChange,
) -> Result<(), String> {
    let leader = cluster.leader_of(group).ok_or("no leader yet")?;
    cluster.propose_conf_change(leader, group, change)
}
