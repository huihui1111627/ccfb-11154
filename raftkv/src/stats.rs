//! 集群运行统计:选主历史、任期变化、快照、分裂、CRC 失败、一致性检查结果。
//! Raft 核心和节点层通过 StatsHandle 记录事件,页面和模拟器读取。
use serde::Serialize;
use std::sync::{Arc, Mutex};

use crate::checker::Op;
use crate::raft::{GroupId, NodeId, Term};

#[derive(Clone, Debug, Serialize)]
pub struct ElectionRecord {
    pub group: GroupId,
    pub term: Term,
    pub winner: NodeId,
    /// 从发起到当选花了多少 tick(真实集群 1 tick = 10ms)
    pub duration_ticks: u64,
    pub at_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct SnapshotRecord {
    pub group: GroupId,
    pub node: NodeId,
    pub index: u64,
    pub size: usize,
    pub at_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct SplitRecord {
    pub old_group: GroupId,
    pub new_group: GroupId,
    pub split_key: Vec<u8>,
    pub at_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ConsistencyReport {
    pub at_ms: u64,
    pub ok: bool,
    pub detail: String,
}

#[derive(Default)]
pub struct Stats {
    pub elections: Vec<ElectionRecord>,
    pub snapshots: Vec<SnapshotRecord>,
    pub splits: Vec<SplitRecord>,
    pub crc_failures: Vec<String>,
    pub consistency: Vec<ConsistencyReport>,
    /// 客户端操作历史(供线性一致性检查器)
    pub client_history: Vec<Op>,
    pub checker_results: Vec<ConsistencyReport>,
    /// 宿主环境当前时间(真实集群=wall ms,模拟=tick)
    pub now_ms: u64,
    /// 调试:每个节点每次应用的 (node, group, index, sm_hash)
    pub apply_log: Vec<(NodeId, GroupId, u64, u64)>,
    /// 调试:快照安装/恢复后的 (node, group, snap_index, sm_hash)
    pub snap_log: Vec<(NodeId, GroupId, u64, u64)>,
    /// 调试:日志截断事件 (node, group, from_index, 被截条目的 (index,term))
    pub truncate_log: Vec<(NodeId, GroupId, u64, Vec<(u64, u64)>)>,
    /// 调试:领导者提交推进 (node, group, index, term, quorum)
    pub commit_log: Vec<(NodeId, GroupId, u64, u64, Vec<NodeId>)>,
    /// 调试:Put 应用 (node, group, index, term, key)
    pub put_log: Vec<(NodeId, GroupId, u64, u64, Vec<u8>)>,
    /// 调试:租约读服务 (node, group, tick, term, lease_expire)
    pub lease_reads: Vec<(NodeId, GroupId, u64, u64, u64)>,
}

pub type StatsHandle = Arc<Mutex<Stats>>;

pub fn new_stats() -> StatsHandle {
    Arc::new(Mutex::new(Stats::default()))
}

const DEBUG_LOG_CAP: usize = 20000;

fn push_capped<T>(v: &mut Vec<T>, x: T) {
    if v.len() >= DEBUG_LOG_CAP {
        v.drain(..DEBUG_LOG_CAP / 2);
    }
    v.push(x);
}

#[doc(hidden)]
pub fn push_capped_pub<T>(v: &mut Vec<T>, x: T) {
    push_capped(v, x);
}

impl Stats {
    pub fn record_election(&mut self, group: GroupId, term: Term, winner: NodeId, duration_ticks: u64) {
        self.elections.push(ElectionRecord {
            group,
            term,
            winner,
            duration_ticks,
            at_ms: self.now_ms,
        });
    }
    pub fn record_snapshot(&mut self, group: GroupId, node: NodeId, index: u64, size: usize) {
        self.snapshots.push(SnapshotRecord {
            group,
            node,
            index,
            size,
            at_ms: self.now_ms,
        });
    }
    pub fn record_split(&mut self, old_group: GroupId, new_group: GroupId, split_key: Vec<u8>) {
        self.splits.push(SplitRecord {
            old_group,
            new_group,
            split_key,
            at_ms: self.now_ms,
        });
    }
    pub fn record_crc_failure(&mut self, detail: String) {
        self.crc_failures.push(detail);
    }
    pub fn record_consistency(&mut self, ok: bool, detail: String) {
        self.consistency.push(ConsistencyReport {
            at_ms: self.now_ms,
            ok,
            detail,
        });
        if self.consistency.len() > 200 {
            self.consistency.drain(..self.consistency.len() - 200);
        }
    }
    pub fn record_checker(&mut self, ok: bool, detail: String) {
        self.checker_results.push(ConsistencyReport {
            at_ms: self.now_ms,
            ok,
            detail,
        });
        if self.checker_results.len() > 50 {
            self.checker_results.drain(..self.checker_results.len() - 50);
        }
    }
}
