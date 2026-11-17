//! 集群状态页面：统一数据模型 + axum 路由 + 单页前端。
//!
//! 同一份 [`ClusterSnapshot`] 模型有两个数据来源：
//! - 真实集群（[`crate::runner`]）：每个节点定时上报的实时状态；
//! - 模拟运行（[`crate::sim`] / [`crate::shard::driver`]）：逻辑时钟下的完整轨迹，
//!   包含分片、分裂历史和线性一致性裁决。
//! 页面分五块：节点状态 / 日志复制 / 分片 / 快照 / 一致性检查。

use axum::{extract::State, response::Html, routing::get, Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeStatus {
    pub node_id: u64,
    pub role: String,
    pub term: u64,
    pub leader: Option<u64>,
    pub commit: u64,
    pub applied: u64,
    pub last_log_index: u64,
    pub log_len: u64,
    pub snapshot_index: u64,
    pub in_joint: bool,
    pub voters: Vec<u64>,
    pub old_voters: Option<Vec<u64>>,
    pub fault: Option<String>,
    pub lease_valid: bool,
    pub lease_expire_ms: u64,
    pub lease_reads: u64,
    pub readindex_reads: u64,
    pub pending_reads: usize,
    pub next_index: Vec<(u64, u64)>,
    pub match_index: Vec<(u64, u64)>,
    pub term_events: Vec<TermEventView>,
    pub uptime_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermEventView {
    pub term: u64,
    pub role: String,
    pub election_elapsed_ms: Option<u64>,
    pub leader: Option<u64>,
    pub at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShardView {
    pub shard_id: u64,
    pub group: u64,
    pub start: String,
    pub end: Option<String>,
    pub voters: Vec<u64>,
    pub splitting: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SplitView {
    pub old_shard: u64,
    pub new_shard: u64,
    pub old_group: u64,
    pub new_group: u64,
    pub split_key: String,
    pub at_ms: u64,
    pub migrated_keys: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SnapshotView {
    pub node_id: u64,
    pub group: u64,
    pub snapshot_index: u64,
    pub log_len: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CheckerView {
    pub ok: bool,
    pub inconclusive: bool,
    pub total_ops: usize,
    pub keys_checked: usize,
    pub states_explored: usize,
    pub violations: Vec<String>,
    pub seed: Option<u64>,
    pub read_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReplicationView {
    pub node_id: u64,
    pub role: String,
    pub term: u64,
    pub commit: u64,
    pub applied: u64,
    pub last_log_index: u64,
    pub match_index: Vec<(u64, u64)>,
    pub next_index: Vec<(u64, u64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterSnapshot {
    pub source: String, // "live" | "simulation"
    pub now_ms: u64,
    pub nodes: Vec<NodeStatus>,
    pub replication: Vec<ReplicationView>,
    pub shards: Vec<ShardView>,
    pub splits: Vec<SplitView>,
    pub snapshots: Vec<SnapshotView>,
    pub checker: CheckerView,
    pub net: NetView,
    pub events: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NetView {
    pub inflight: usize,
    pub delivered: u64,
    pub dropped: u64,
    pub pending_ops: usize,
}

pub type SharedSnapshot = Arc<RwLock<ClusterSnapshot>>;

pub fn app(state: SharedSnapshot) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/status", get(status))
        .with_state(state)
}

async fn status(State(s): State<SharedSnapshot>) -> Json<ClusterSnapshot> {
    let snap = s.read().unwrap().clone();
    Json(snap)
}

async fn index() -> Html<&'static str> {
    Html(PAGE_HTML)
}

const PAGE_HTML: &str = include_str!("dashboard.html");
