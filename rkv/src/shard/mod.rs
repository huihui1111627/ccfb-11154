//! 范围分片：键空间按有序 range 切分，每个分片对应一个独立 Raft 组；
//! 分片超过阈值自动一分为二，路由表本身由 meta Raft 组一致复制。
//!
//! ## 路由模型
//! 每个分片持有 `[start, end)` 半开区间，按 start 有序排列，最后一个分片
//! `end` 为空哨兵表示到键空间末尾。路由表是 meta 组状态机里的特殊键：
//! - `\x00route/<shard_id>` -> JSON(ShardMeta)
//! 因此分片成员/边界变更同样走 Raft 日志，天然线性一致，客户端缓存旧路由时
//! 由数据组返回 "not my range"，管理器再让其刷新 meta。
//!
//! ## 自动分裂流程（编排器 + 两组 Raft 协同）
//! 1. 管理器发现某分片 key 数超过阈值，取中位数 key 作为分裂点；
//! 2. 在 meta 组提案新分片元数据（新 Raft 组 id、边界、初始成员=老组成员）；
//! 3. 新组以空白节点启动，leader 从老组 leader 导出 `[split_key, end)` 数据，
//!    作为初始快照安装（InstallSnapshot 通道，不落老组日志）；
//! 4. 新组就绪后，meta 组再提案把老分片 end 收缩到 split_key；
//! 5. 此后新 key 路由到新组，老组通过一次普通提案删除已迁走的 key 范围。

pub mod driver;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::raft::{ConfState, NodeId};

pub type ShardId = u64;
pub type GroupId = u64;

/// 路由表在 meta 组 KV 里的键前缀。
pub const ROUTE_PREFIX: &[u8] = b"\x00route/";
/// 分裂记账键（防止分裂半途重复触发）。
pub const SPLIT_LOCK_KEY: &[u8] = b"\x00split-lock";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardMeta {
    pub shard_id: ShardId,
    pub group: GroupId,
    /// 半开区间 [start, end)；end 为空表示到键空间末尾
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
    pub voters: Vec<NodeId>,
    /// 分裂进行中标记
    pub splitting: bool,
}

impl ShardMeta {
    pub fn contains(&self, key: &[u8]) -> bool {
        let after_start = key >= self.start.as_slice();
        let before_end = match &self.end {
            Some(e) => key < e.as_slice(),
            None => true,
        };
        after_start && before_end
    }
}

/// 分片编排器：持有路由表（从 meta 组同步），负责找片与分裂决策。
/// 它本身不跑 Raft——所有路由变更都通过 meta 组提案完成。
pub struct ShardManager {
    shards: BTreeMap<ShardId, ShardMeta>,
    next_shard_id: ShardId,
    next_group_id: GroupId,
    split_threshold: usize,
}

#[derive(Debug, Clone)]
pub struct SplitPlan {
    pub old_shard: ShardId,
    pub new_shard: ShardId,
    pub new_group: GroupId,
    pub split_key: Vec<u8>,
    pub voters: Vec<NodeId>,
}

impl ShardManager {
    /// 初始单分片，覆盖整个键空间。
    pub fn new(initial_voters: Vec<NodeId>, split_threshold: usize) -> Self {
        let meta = ShardMeta {
            shard_id: 1,
            group: 1,
            start: vec![],
            end: None,
            voters: initial_voters,
            splitting: false,
        };
        let mut shards = BTreeMap::new();
        shards.insert(1, meta);
        ShardManager {
            shards,
            next_shard_id: 2,
            next_group_id: 2,
            split_threshold,
        }
    }

    pub fn split_threshold(&self) -> usize {
        self.split_threshold
    }

    /// 从 meta 组状态机重建路由表（启动/路由刷新时调用）。
    pub fn load_from_meta(
        meta_kv: &BTreeMap<Vec<u8>, Vec<u8>>,
        split_threshold: usize,
    ) -> Option<Self> {
        let mut shards = BTreeMap::new();
        let mut max_shard = 0u64;
        let mut max_group = 0u64;
        for (k, v) in meta_kv {
            if k.starts_with(ROUTE_PREFIX) {
                let meta: ShardMeta = serde_json::from_slice(v).ok()?;
                max_shard = max_shard.max(meta.shard_id);
                max_group = max_group.max(meta.group);
                shards.insert(meta.shard_id, meta);
            }
        }
        if shards.is_empty() {
            return None;
        }
        Some(ShardManager {
            shards,
            next_shard_id: max_shard + 1,
            next_group_id: max_group + 1,
            split_threshold,
        })
    }

    /// 路由：key 落在哪个分片。
    pub fn route(&self, key: &[u8]) -> Option<&ShardMeta> {
        // 业务键不允许与内部前缀冲突
        self.shards.values().find(|m| m.contains(key))
    }

    pub fn shard_for_group(&self, group: GroupId) -> Option<&ShardMeta> {
        self.shards.values().find(|m| m.group == group)
    }

    pub fn all_shards(&self) -> Vec<&ShardMeta> {
        self.shards.values().collect()
    }

    /// 数据组收到不属于自己区间的写时用它判断。
    pub fn group_owns_key(&self, group: GroupId, key: &[u8]) -> bool {
        self.route(key).map(|m| m.group == group).unwrap_or(false)
    }

    /// 分裂决策：给出超阈值分片的分裂计划（取 key 列表中位数）。
    pub fn plan_split(
        &mut self,
        group: GroupId,
        keys_in_shard: &[Vec<u8>],
    ) -> Option<SplitPlan> {
        let meta = self.shards.values().find(|m| m.group == group)?.clone();
        if meta.splitting || keys_in_shard.len() < self.split_threshold {
            return None;
        }
        let mut sorted: Vec<&Vec<u8>> = keys_in_shard
            .iter()
            .filter(|k| meta.contains(k))
            .collect();
        sorted.sort();
        if sorted.len() < 2 {
            return None;
        }
        // 中位数作为分裂点，保证两边尽量均衡
        let split_key = sorted[sorted.len() / 2].clone();
        if split_key == meta.start {
            return None;
        }
        let new_shard = self.next_shard_id;
        let new_group = self.next_group_id;
        self.next_shard_id += 1;
        self.next_group_id += 1;
        Some(SplitPlan {
            old_shard: meta.shard_id,
            new_shard,
            new_group,
            split_key,
            voters: meta.voters.clone(),
        })
    }

    /// meta 组提案应用后调用：登记新分片（先标记 splitting，边界仍属老片）。
    pub fn apply_new_shard(&mut self, plan: &SplitPlan) {
        let meta = ShardMeta {
            shard_id: plan.new_shard,
            group: plan.new_group,
            start: plan.split_key.clone(),
            end: self
                .shards
                .get(&plan.old_shard)
                .and_then(|m| m.end.clone()),
            voters: plan.voters.clone(),
            splitting: true,
        };
        self.shards.insert(plan.new_shard, meta);
        if let Some(old) = self.shards.get_mut(&plan.old_shard) {
            old.splitting = true;
        }
    }

    /// 新组快照就绪后调用：老片收缩 end，新片解除 splitting，路由正式切换。
    pub fn apply_split_complete(&mut self, plan: &SplitPlan) {
        if let Some(old) = self.shards.get_mut(&plan.old_shard) {
            old.end = Some(plan.split_key.clone());
            old.splitting = false;
        }
        if let Some(new) = self.shards.get_mut(&plan.new_shard) {
            new.splitting = false;
        }
    }

    /// 成员变更后同步分片 voter 列表（meta 组提案驱动）。
    pub fn apply_membership(&mut self, shard: ShardId, conf: &ConfState) {
        if let Some(m) = self.shards.get_mut(&shard) {
            m.voters = conf.voters.clone();
        }
    }

    /// 序列化路由表条目（写 meta 组用）。
    pub fn route_entry(meta: &ShardMeta) -> (Vec<u8>, Vec<u8>) {
        let key = [ROUTE_PREFIX, meta.shard_id.to_string().as_bytes()].concat();
        let value = serde_json::to_vec(meta).unwrap();
        (key, value)
    }

    /// 完整路由表快照（页面展示）。
    pub fn snapshot(&self) -> Vec<ShardMeta> {
        let mut v: Vec<ShardMeta> = self.shards.values().cloned().collect();
        v.sort_by_key(|m| m.start.clone());
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(n: u8) -> Vec<u8> {
        vec![n]
    }

    #[test]
    fn routing_basic_and_split() {
        let mut mgr = ShardManager::new(vec![1, 2, 3], 4);
        assert!(mgr.route(b"a").is_some());
        let keys: Vec<Vec<u8>> = (1..=8).map(k).collect();
        let plan = mgr.plan_split(1, &keys).expect("split planned");
        assert_eq!(plan.new_shard, 2);
        assert_eq!(plan.new_group, 2);
        // 分裂点是中位数 key[4] = [5]
        assert_eq!(plan.split_key, k(5));

        mgr.apply_new_shard(&plan);
        // 切换前所有 key 仍路由老片（新片 splitting）
        assert_eq!(mgr.route(&k(7)).unwrap().group, 1);
        mgr.apply_split_complete(&plan);
        // 切换后 [5..) 走新组
        assert_eq!(mgr.route(&k(5)).unwrap().group, 2);
        assert_eq!(mgr.route(&k(4)).unwrap().group, 1);
        assert_eq!(mgr.route(&k(255)).unwrap().group, 2);
    }

    #[test]
    fn no_split_below_threshold() {
        let mut mgr = ShardManager::new(vec![1], 10);
        let keys: Vec<Vec<u8>> = (1..=5).map(k).collect();
        assert!(mgr.plan_split(1, &keys).is_none());
    }

    #[test]
    fn route_table_roundtrip_through_meta_kv() {
        let mut mgr = ShardManager::new(vec![1, 2, 3], 4);
        let keys: Vec<Vec<u8>> = (1..=8).map(k).collect();
        let plan = mgr.plan_split(1, &keys).unwrap();
        mgr.apply_new_shard(&plan);
        mgr.apply_split_complete(&plan);

        let mut meta_kv = BTreeMap::new();
        for m in mgr.all_shards() {
            let (key, val) = ShardManager::route_entry(m);
            meta_kv.insert(key, val);
        }
        let restored = ShardManager::load_from_meta(&meta_kv, 4).unwrap();
        assert_eq!(restored.route(&k(6)).unwrap().group, 2);
        assert_eq!(restored.all_shards().len(), 2);
    }
}
