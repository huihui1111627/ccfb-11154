//! 本机多节点集群（进程内传输）+ dashboard 状态聚合。
//!
//! 用于 `rkv dev-cluster` 命令和集成测试：一个进程里跑 N 个 Raft 节点，
//! 节点间走内存路由（与 TCP 共用同一状态机与驱动循环），状态定时汇总到
//! [`ClusterSnapshot`] 供页面展示。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};

use crate::dashboard::{
    app, ClusterSnapshot, NodeStatus, ReplicationView, SharedSnapshot,
};
use crate::raft::{ConfState, NodeId, RaftConfig};
use crate::runner::{InprocRouter, NodeParams, RaftClient, ReplyRegistry};

pub struct LocalCluster {
    pub ids: Vec<NodeId>,
    pub router: InprocRouter,
    pub replies: Arc<ReplyRegistry>,
    pub snapshot: SharedSnapshot,
    latest: Arc<RwLock<HashMap<NodeId, NodeStatus>>>,
}

impl LocalCluster {
    /// 拉起一个 N 节点集群（单 Raft 组），各自独立数据目录。
    pub async fn start(
        base_dir: &Path,
        ids: Vec<NodeId>,
        cfg: RaftConfig,
        seed: u64,
    ) -> Arc<Self> {
        let router = InprocRouter::new();
        let replies = Arc::new(ReplyRegistry::default());
        let (status_tx, status_rx) = mpsc::unbounded_channel();
        let conf = ConfState::new(ids.clone());
        let dummy: SocketAddr = "127.0.0.1:0".parse().unwrap();

        for (i, &id) in ids.iter().enumerate() {
            let dir: PathBuf = base_dir.join(format!("node-{id}"));
            std::fs::create_dir_all(&dir).unwrap();
            let params = NodeParams {
                id,
                group: 1,
                data_dir: dir,
                peers: ids.iter().map(|p| (*p, dummy)).collect(),
                cfg: cfg.clone(),
                seed: seed.wrapping_add(i as u64 * 1000 + 1),
                status_tx: Some(status_tx.clone()),
                replies: replies.clone(),
            };
            crate::runner::spawn_inproc_node(params, router.clone(), Some(conf.clone())).await;
        }

        let cluster = Arc::new(LocalCluster {
            ids: ids.clone(),
            router,
            replies,
            snapshot: Arc::new(std::sync::RwLock::new(ClusterSnapshot {
                source: "live".into(),
                ..Default::default()
            })),
            latest: Arc::new(RwLock::new(HashMap::new())),
        });

        // 后台：收集状态 -> 生成快照（receiver 随任务 move 走）
        {
            let c = cluster.clone();
            tokio::spawn(async move {
                c.collect_loop(status_rx).await;
            });
        }
        // 等第一个 leader 选出来
        cluster.wait_for_leader(5_000).await;
        cluster
    }

    async fn collect_loop(
        self: Arc<Self>,
        mut status_rx: mpsc::UnboundedReceiver<(NodeId, NodeStatus)>,
    ) {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(200));
        loop {
            ticker.tick().await;
            while let Ok((id, st)) = status_rx.try_recv() {
                self.latest.write().await.insert(id, st);
            }
            let map = self.latest.read().await.clone();
            let mut nodes: Vec<NodeStatus> = map.into_values().collect();
            nodes.sort_by_key(|n| n.node_id);

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
                .map(|n| crate::dashboard::SnapshotView {
                    node_id: n.node_id,
                    group: 1,
                    snapshot_index: n.snapshot_index,
                    log_len: n.log_len,
                })
                .collect();

            let snap = ClusterSnapshot {
                source: "live".into(),
                now_ms: nodes.first().map(|n| n.uptime_ms).unwrap_or(0),
                replication,
                snapshots,
                nodes,
                ..Default::default()
            };
            *self.snapshot.write().unwrap() = snap;
        }
    }

    pub async fn wait_for_leader(&self, timeout_ms: u64) -> Option<NodeId> {
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_millis(timeout_ms);
        loop {
            {
                let map = self.latest.read().await;
                if let Some((id, _)) = map.iter().find(|(_, s)| s.role == "leader") {
                    return Some(*id);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// 挂 dashboard HTTP 服务；端口被占用时打印警告但不影响集群运行。
    pub async fn serve_dashboard(self: Arc<Self>, port: u16) {
        let app = app(self.snapshot.clone());
        match tokio::net::TcpListener::bind(("127.0.0.1", port)).await {
            Ok(listener) => {
                tokio::spawn(async move {
                    axum::serve(listener, app).await.unwrap();
                });
            }
            Err(e) => eprintln!("warning: dashboard port {port} unavailable ({e})"),
        }
    }

    pub fn client(&self) -> RaftClient {
        RaftClient::inproc(
            self.router.clone(),
            self.replies.clone(),
            self.ids.clone(),
        )
    }

    pub fn snapshot_now(&self) -> ClusterSnapshot {
        self.snapshot.read().unwrap().clone()
    }
}

/// 模拟运行把结果灌进同一份 dashboard 模型（页面数据源切换为 simulation）。
pub fn snapshot_from_simulation(
    cluster: &crate::sim::SimCluster,
    checker: crate::checker::CheckReport,
    seed: Option<u64>,
    read_mode: &str,
) -> ClusterSnapshot {
    use crate::dashboard::*;
    let views = cluster.node_views();
    let nodes = views
        .iter()
        .map(|v| {
            let events = cluster.term_events(v.node_id, v.group);
            NodeStatus {
                node_id: v.node_id,
                role: v.role.clone(),
                term: v.term,
                leader: v.leader,
                commit: v.commit,
                applied: v.applied,
                last_log_index: v.last_log_index,
                log_len: v.log_len,
                snapshot_index: v.snapshot_index,
                in_joint: v.in_joint,
                voters: v.voters.clone(),
                old_voters: None,
                fault: v.fault.clone(),
                lease_valid: false,
                lease_expire_ms: 0,
                lease_reads: 0,
                readindex_reads: 0,
                pending_reads: 0,
                next_index: vec![],
                match_index: vec![],
                term_events: events
                    .iter()
                    .map(|te| TermEventView {
                        term: te.term,
                        role: te.role.clone(),
                        election_elapsed_ms: te.election_elapsed_ms,
                        leader: te.leader,
                        at_ms: te.at_ms,
                    })
                    .collect(),
                uptime_ms: cluster.now,
            }
        })
        .collect();
    let stats = cluster.stats();
    ClusterSnapshot {
        source: "simulation".into(),
        now_ms: cluster.now,
        nodes,
        replication: views
            .iter()
            .map(|v| ReplicationView {
                node_id: v.node_id,
                role: v.role.clone(),
                term: v.term,
                commit: v.commit,
                applied: v.applied,
                last_log_index: v.last_log_index,
                match_index: vec![],
                next_index: vec![],
            })
            .collect(),
        shards: vec![],
        splits: vec![],
        snapshots: views
            .iter()
            .map(|v| SnapshotView {
                node_id: v.node_id,
                group: v.group,
                snapshot_index: v.snapshot_index,
                log_len: v.log_len,
            })
            .collect(),
        checker: CheckerView {
            ok: checker.ok,
            inconclusive: checker.inconclusive,
            total_ops: checker.total_ops,
            keys_checked: checker.keys_checked,
            states_explored: checker.states_explored,
            violations: checker
                .violations
                .iter()
                .map(|v| format!("key={:?}: {}", String::from_utf8_lossy(&v.key), v.detail))
                .collect(),
            seed,
            read_mode: read_mode.into(),
        },
        net: NetView {
            inflight: stats.inflight,
            delivered: stats.delivered,
            dropped: stats.dropped,
            pending_ops: stats.pending_ops,
        },
        events: cluster.event_log().to_vec(),
    }
}
