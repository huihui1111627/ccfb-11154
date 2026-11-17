//! Raft 之上的状态机抽象与默认 KV 状态机。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::raft::Command;
use crate::storage::{json_snapshot_decode, json_snapshot_encode, Result as StoreResult};

#[derive(Debug, Clone)]
pub struct ApplyOutcome {
    pub key_hint: Option<Vec<u8>>,
}

/// 每个 Raft 组挂一个状态机。
pub trait StateMachine: Send {
    fn apply(&mut self, index: u64, term: u64, cmd: &Command) -> ApplyOutcome;
    fn snapshot(&self) -> Vec<u8>;
    fn restore(&mut self, data: &[u8]) -> StoreResult<()>;
    /// 状态机内逻辑 key 数量（分片分裂阈值判断用）
    fn key_count(&self) -> usize;
    /// 只读查询（线性一致读最终在这里落盘外的内存读）
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
}

/// 键值状态机。逻辑上 `BTreeMap<key,value>`，快照即整表 JSON。
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct KVStateMachine {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    /// 已应用到的日志位置，restore 后由 raft 核心另外维护
    applied_index: u64,
    applied_term: u64,
}

impl KVStateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.get(key).cloned()
    }

    /// 分片分裂：把 >= range_start 的 key 导出给新组（调用方决定边界）。
    pub fn export_range(&self, range_start: &[u8]) -> BTreeMap<Vec<u8>, Vec<u8>> {
        self.data
            .range(range_start.to_vec()..)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// 分裂后老组删除已迁走的 key 范围。
    pub fn drop_range(&mut self, range_start: &[u8]) {
        let keep: BTreeMap<_, _> = self
            .data
            .range(..range_start.to_vec())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self.data = keep;
    }

    pub fn import(&mut self, data: BTreeMap<Vec<u8>, Vec<u8>>) {
        for (k, v) in data {
            self.data.insert(k, v);
        }
    }

    pub fn applied_index(&self) -> u64 {
        self.applied_index
    }
    pub fn applied_term(&self) -> u64 {
        self.applied_term
    }
}

impl StateMachine for KVStateMachine {
    fn apply(&mut self, index: u64, term: u64, cmd: &Command) -> ApplyOutcome {
        self.applied_index = index;
        self.applied_term = term;
        let mut hint = None;
        match cmd {
            Command::Put { key, value } => {
                self.data.insert(key.clone(), value.clone());
                hint = Some(key.clone());
            }
            Command::Delete { key } => {
                self.data.remove(key);
                hint = Some(key.clone());
            }
            Command::EnterJoint { .. }
            | Command::LeaveJoint { .. }
            | Command::ShardSplitDone { .. }
            | Command::Noop => {}
        }
        ApplyOutcome { key_hint: hint }
    }

    fn snapshot(&self) -> Vec<u8> {
        json_snapshot_encode(&self.data)
    }

    fn restore(&mut self, data: &[u8]) -> StoreResult<()> {
        self.data = json_snapshot_decode(data)?;
        Ok(())
    }

    fn key_count(&self) -> usize {
        self.data.len()
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.get(key).cloned()
    }
}
