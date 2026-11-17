//! rkv 命令行入口。
//!
//! 子命令：
//! - `dev-cluster`  本机起 N 个进程内节点 + dashboard，可直接 curl 读写
//! - `sim`          跑确定性随机故障场景（支持上万种子），线性一致性裁决，种子可复现
//! - `shard-sim`    带自动分裂的分片集群模拟，页面看分片/分裂历史
//! - `node`         单节点真实 TCP 模式（多进程部署）
//! - `check`        对历史 JSON 文件离线跑线性一致性检查器

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use rkv::checker::{check, Op};
use rkv::cluster::LocalCluster;
use rkv::dashboard::SharedSnapshot;
use rkv::raft::{RaftConfig, ReadMode};
use rkv::sim::scenario::{run_many, run_scenario, ScenarioConfig};
use rkv::sim::SimConfig;

#[derive(Parser)]
#[command(name = "rkv", version, about = "Raft 分布式 KV：分片 + 确定性故障模拟 + 线性一致性检查")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// 本机进程内 N 节点集群 + 状态页面
    DevCluster {
        #[arg(long, default_value_t = 5)]
        nodes: u64,
        #[arg(long, default_value = "./data/dev")]
        dir: PathBuf,
        #[arg(long, default_value_t = 8080)]
        port: u16,
        /// 跑完自检（连续读写 + 一次读校验）后不退出，继续挂页面
        #[arg(long)]
        smoke: bool,
    },
    /// 确定性随机故障场景
    Sim {
        #[arg(long, default_value_t = 1)]
        seed: u64,
        /// 连续跑多少个种子（从 seed 开始递增）
        #[arg(long, default_value_t = 1)]
        count: u64,
        #[arg(long, default_value_t = 5)]
        nodes: u64,
        #[arg(long, default_value_t = 20_000)]
        duration_ms: u64,
        #[arg(long, default_value = "./data/sim")]
        dir: PathBuf,
        #[arg(long)]
        lease: bool,
        /// 跑完把最后一个场景的 dashboard 挂在这个端口（0 = 不挂）
        #[arg(long, default_value_t = 0)]
        dashboard_port: u16,
    },
    /// 分片 + 自动分裂模拟
    ShardSim {
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 5)]
        nodes: u64,
        /// 每个分片多少 key 触发分裂
        #[arg(long, default_value_t = 20)]
        split_threshold: usize,
        #[arg(long, default_value_t = 400)]
        ops: u64,
        #[arg(long, default_value = "./data/shard-sim")]
        dir: PathBuf,
        #[arg(long, default_value_t = 8081)]
        dashboard_port: u16,
    },
    /// 真实 TCP 单节点
    Node {
        #[arg(long)]
        id: u64,
        #[arg(long)]
        bind: SocketAddr,
        /// 同伴列表，格式 id=host:port（可重复）
        #[arg(long = "peer")]
        peers: Vec<String>,
        #[arg(long)]
        dir: PathBuf,
        #[arg(long, default_value_t = 8090)]
        dashboard_port: u16,
    },
    /// 离线检查历史文件（JSON: [{client_id,call_time,return_time,kind...}]）
    Check {
        #[arg(long)]
        file: PathBuf,
    },
}

fn raft_cfg() -> RaftConfig {
    // 模拟用更快的选举节奏，便于在短时间内制造大量选举轮次
    RaftConfig {
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        heartbeat_interval_ms: 40,
        lease_duration_ms: 90,
        snapshot_threshold: 64,
        max_entries_per_msg: 256,
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::DevCluster { nodes, dir, port, smoke } => {
            std::fs::create_dir_all(&dir).unwrap();
            let ids: Vec<u64> = (1..=nodes).collect();
            let cluster = LocalCluster::start(&dir, ids.clone(), raft_cfg(), 42).await;
            cluster.clone().serve_dashboard(port).await;
            println!("dev cluster up: http://127.0.0.1:{port}  (nodes {ids:?})");

            if smoke {
                let mut client = cluster.client();
                for i in 0..20u64 {
                    let idx = client
                        .put(format!("k{i}").into_bytes(), format!("v{i}").into_bytes())
                        .await
                        .expect("put");
                    println!("put k{i} -> log index {idx}");
                }
                let v = client
                    .get(b"k7".to_vec(), ReadMode::ReadIndex)
                    .await
                    .expect("get");
                assert_eq!(v, Some(b"v7".to_vec()));
                println!("smoke ok: k7 = {:?}", v.as_deref().map(String::from_utf8_lossy));
            }
            tokio::signal::ctrl_c().await.ok();
        }

        Cmd::Sim { seed, count, nodes, duration_ms, dir, lease, dashboard_port } => {
            // 模拟是一次性确定性运行：清掉旧数据，避免 bootstrap 撞上非空目录
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            let sc = ScenarioConfig {
                node_count: nodes,
                duration_ms,
                read_mode: if lease { ReadMode::Lease } else { ReadMode::ReadIndex },
                ..Default::default()
            };
            if count == 1 {
                let one_dir = dir.join(format!("seed-{seed}"));
                std::fs::create_dir_all(&one_dir).unwrap();
                let r = run_scenario(&one_dir, seed, &sc, raft_cfg());
                println!(
                    "seed={seed} linearizable={} violations={} explored={} faults={} ops={}",
                    r.linearizable,
                    r.violation_count,
                    r.states_explored,
                    r.fault_events,
                    r.stats.history_ops
                );
                if let Some(f) = &r.evidence_file {
                    println!("evidence written: {f}");
                }
                if dashboard_port != 0 {
                    serve_sim_dashboard(&one_dir, seed, &sc, dashboard_port, lease).await;
                }
                if !r.linearizable {
                    std::process::exit(2);
                }
            } else {
                let results = run_many(&dir, seed, count, &sc, raft_cfg());
                let ok = results.iter().filter(|r| r.linearizable).count();
                let bad: Vec<_> = results.iter().filter(|r| !r.linearizable).collect();
                for r in &results {
                    println!(
                        "seed={} {} (violations={}, faults={}, ops={}, explored={})",
                        r.seed,
                        if r.linearizable { "OK" } else { "FAIL" },
                        r.violation_count,
                        r.fault_events,
                        r.stats.history_ops,
                        r.states_explored
                    );
                }
                println!("== {ok}/{} linearizable, {} failures", results.len(), bad.len());
                if !bad.is_empty() {
                    if let Some(f) = bad[0].evidence_file.as_ref() {
                        println!("first failure evidence: {f}");
                    }
                    std::process::exit(2);
                }
            }
        }

        Cmd::ShardSim { seed, nodes, split_threshold, ops, dir, dashboard_port } => {
            // 一次性模拟：清旧数据后重建
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            run_shard_sim(seed, nodes, split_threshold, ops, &dir, dashboard_port).await;
        }

        Cmd::Node { id, bind, peers, dir, dashboard_port, .. } => {
            std::fs::create_dir_all(&dir).unwrap();
            let mut peer_map: HashMap<u64, SocketAddr> = HashMap::new();
            for p in peers {
                let (pid, addr) = p.split_once('=').expect("peer format id=host:port");
                peer_map.insert(pid.parse().unwrap(), addr.parse().unwrap());
            }
            let replies = Arc::new(rkv::runner::ReplyRegistry::default());
            let (status_tx, mut status_rx) = tokio::sync::mpsc::unbounded_channel();
            let params = rkv::runner::NodeParams {
                id,
                group: 1,
                data_dir: dir,
                peers: peer_map,
                cfg: raft_cfg(),
                seed: id.wrapping_mul(7919),
                status_tx: Some(status_tx),
                replies,
            };
            rkv::runner::spawn_tcp_node(params, bind);

            // 聚合本节点状态并挂 dashboard
            let snapshot: rkv::dashboard::SharedSnapshot =
                Arc::new(std::sync::RwLock::new(rkv::dashboard::ClusterSnapshot {
                    source: "live".into(),
                    ..Default::default()
                }));
            let snap2 = snapshot.clone();
            tokio::spawn(async move {
                use rkv::dashboard::*;
                let mut by_id: HashMap<u64, NodeStatus> = HashMap::new();
                while let Some((node_id, st)) = status_rx.recv().await {
                    by_id.insert(node_id, st);
                    let nodes: Vec<NodeStatus> = {
                        let mut v: Vec<NodeStatus> = by_id.values().cloned().collect();
                        v.sort_by_key(|n| n.node_id);
                        v
                    };
                    let replication = nodes
                        .iter()
                        .map(|n| ReplicationView {
                            node_id: n.node_id,
                            role: n.role.clone(),
                            term: n.term,
                            commit: n.commit,
                            applied: n.applied,
                            last_log_index: n.last_log_index,
                            match_index: n.match_index.clone(),
                            next_index: n.next_index.clone(),
                        })
                        .collect();
                    let snapshots = nodes
                        .iter()
                        .map(|n| SnapshotView {
                            node_id: n.node_id,
                            group: 1,
                            snapshot_index: n.snapshot_index,
                            log_len: n.log_len,
                        })
                        .collect();
                    let now = nodes.first().map(|n| n.uptime_ms).unwrap_or(0);
                    *snap2.write().unwrap() = ClusterSnapshot {
                        source: "live".into(),
                        now_ms: now,
                        nodes,
                        replication,
                        snapshots,
                        ..Default::default()
                    };
                }
            });
            let app = rkv::dashboard::app(snapshot);
            serve_or_warn(app, dashboard_port, &format!("node {id}")).await;
            println!("node {id} listening on {bind}");
            tokio::signal::ctrl_c().await.ok();
        }

        Cmd::Check { file } => {
            let bytes = std::fs::read(&file).unwrap();
            let history: Vec<Op> = serde_json::from_slice(&bytes).unwrap();
            let report = check(&history);
            println!(
                "ops={} keys={} explored={} ok={} inconclusive={}",
                report.total_ops,
                report.keys_checked,
                report.states_explored,
                report.ok,
                report.inconclusive
            );
            for v in &report.violations {
                println!("VIOLATION key={:?}: {}", String::from_utf8_lossy(&v.key), v.detail);
            }
            if !report.ok {
                std::process::exit(2);
            }
        }
    }
}

/// 重放单个场景并把 dashboard 挂起来（同种子确定性复现）。
async fn serve_sim_dashboard(
    base_dir: &std::path::Path,
    seed: u64,
    sc: &ScenarioConfig,
    port: u16,
    lease: bool,
) {
    use rkv::cluster::snapshot_from_simulation;
    use rkv::sim::scenario::run_scenario_return_cluster;
    // 用同一执行核心完整重放，拿到含故障轨迹的集群对象
    let cluster = run_scenario_return_cluster(base_dir, seed, sc, raft_cfg());
    let report = rkv::checker::check(cluster.history());
    let snap = snapshot_from_simulation(
        &cluster,
        report,
        Some(seed),
        if lease { "lease" } else { "read-index" },
    );
    let shared: SharedSnapshot = Arc::new(std::sync::RwLock::new(snap));
    let app = rkv::dashboard::app(shared);
    match tokio::net::TcpListener::bind(("127.0.0.1", port)).await {
        Ok(listener) => {
            println!("simulation dashboard: http://127.0.0.1:{port}");
            axum::serve(listener, app).await.unwrap();
        }
        Err(e) => eprintln!("dashboard port {port} unavailable ({e}); simulation result already printed above"),
    }
}

/// 绑定 dashboard 端口，失败时返回 false 而不是 panic。
async fn serve_or_warn(
    app: axum::Router,
    port: u16,
    label: &str,
) -> bool {
    match tokio::net::TcpListener::bind(("127.0.0.1", port)).await {
        Ok(listener) => {
            println!("{label} dashboard: http://127.0.0.1:{port}");
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            true
        }
        Err(e) => {
            eprintln!("warning: dashboard port {port} unavailable ({e}); {label} keeps running without UI");
            false
        }
    }
}

async fn run_shard_sim(
    seed: u64,
    nodes: u64,
    split_threshold: usize,
    ops: u64,
    dir: &std::path::Path,
    dashboard_port: u16,
) {
    use rand::{Rng, SeedableRng};
    use rand_pcg::Pcg64;
    use rkv::shard::driver::{ShardCluster, META_GROUP};

    let sim_cfg = SimConfig { seed, loss_pct: 5, duplicate_pct: 1, ..Default::default() };
    let voters: Vec<u64> = (1..=nodes).collect();
    let mut cluster =
        ShardCluster::bootstrap(dir, sim_cfg, raft_cfg(), voters, split_threshold);
    cluster.sim.advance_to(2_000); // 等两个组选主、路由写入 meta

    let mut rng = Pcg64::seed_from_u64(seed ^ 0xABCDEF);
    for seq in 0..ops {
        let key_idx = rng.gen_range(0..(split_threshold as u32 * 3));
        let key = format!("item-{key_idx:05}").into_bytes();
        if rng.gen_ratio(70, 100) {
            let _g = cluster.put(seq % 4, key, format!("val-{seq}").into_bytes());
        } else {
            let _g = cluster.read(seq % 4, key, ReadMode::ReadIndex);
        }
        if seq % 25 == 0 {
            cluster.advance(500);
        }
    }
    cluster.advance(3_000);
    let _ = META_GROUP;

    let report = rkv::checker::check(cluster.sim.history());
    println!(
        "shard-sim: shards={} splits={} history_ops={} linearizable={} explored={}",
        cluster.shard_views().len(),
        cluster.split_history.len(),
        report.total_ops,
        report.ok,
        report.states_explored
    );
    for s in &cluster.split_history {
        println!(
            "  split: shard {}->{} group {}->{} at key={:?} migrated={}",
            s.old_shard,
            s.new_shard,
            s.old_group,
            s.new_group,
            String::from_utf8_lossy(&s.split_key),
            s.migrated_keys
        );
    }

    // 灌 dashboard
    let mut snap = rkv::cluster::snapshot_from_simulation(
        &cluster.sim,
        report,
        Some(seed),
        "read-index",
    );
    snap.shards = cluster
        .shard_views()
        .iter()
        .map(|m| rkv::dashboard::ShardView {
            shard_id: m.shard_id,
            group: m.group,
            start: String::from_utf8_lossy(&m.start).to_string(),
            end: m.end.as_ref().map(|e| String::from_utf8_lossy(e).to_string()),
            voters: m.voters.clone(),
            splitting: m.splitting,
        })
        .collect();
    snap.splits = cluster
        .split_history
        .iter()
        .map(|s| rkv::dashboard::SplitView {
            old_shard: s.old_shard,
            new_shard: s.new_shard,
            old_group: s.old_group,
            new_group: s.new_group,
            split_key: String::from_utf8_lossy(&s.split_key).to_string(),
            at_ms: s.at_ms,
            migrated_keys: s.migrated_keys,
        })
        .collect();
    let shared: SharedSnapshot = Arc::new(std::sync::RwLock::new(snap));
    let app = rkv::dashboard::app(shared);
    match tokio::net::TcpListener::bind(("127.0.0.1", dashboard_port)).await {
        Ok(listener) => {
            println!("shard dashboard: http://127.0.0.1:{dashboard_port}");
            axum::serve(listener, app).await.unwrap();
        }
        Err(e) => eprintln!("dashboard port {dashboard_port} unavailable ({e}); shard result already printed above"),
    }
}
