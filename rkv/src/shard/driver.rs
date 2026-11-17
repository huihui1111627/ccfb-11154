//! 分片集群驱动：在模拟层之上编排 meta 组与多个数据 Raft 组，
//! 实现路由刷新、按片写入和"超阈值自动分裂"的完整生命周期。

use std::collections::BTreeMap;
use std::path::Path;

use crate::raft::{ReadMode, RaftConfig};
use crate::shard::{ShardManager, ShardMeta, ROUTE_PREFIX};
use crate::sim::{SimCluster, SimConfig, GroupId};

/// 路由元数据所在的 meta Raft 组。
pub const META_GROUP: GroupId = 0;
/// 第一个数据组。
pub const FIRST_DATA_GROUP: GroupId = 1;

pub struct ShardCluster {
    pub sim: SimCluster,
    pub manager: ShardManager,
    split_threshold: usize,
    /// 已完成的分裂记录（页面/断言用）
    pub split_history: Vec<SplitRecord>,
    /// 初始路由是否已写进 meta 组
    initial_routes_synced: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SplitRecord {
    pub old_shard: u64,
    pub new_shard: u64,
    pub old_group: GroupId,
    pub new_group: GroupId,
    pub split_key: Vec<u8>,
    pub at_ms: u64,
    pub migrated_keys: usize,
}

impl ShardCluster {
    /// 建集群：meta 组 + 一个覆盖全键空间的初始数据组，节点相同。
    pub fn bootstrap(
        base_dir: &Path,
        sim_cfg: SimConfig,
        raft_cfg: RaftConfig,
        voters: Vec<u64>,
        split_threshold: usize,
    ) -> Self {
        let mut sim = SimCluster::new(base_dir, sim_cfg, raft_cfg);
        sim.add_group(META_GROUP, voters.clone());
        sim.add_group(FIRST_DATA_GROUP, voters.clone());
        let manager = ShardManager::new(voters, split_threshold);
        ShardCluster {
            sim,
            manager,
            split_threshold,
            split_history: Vec::new(),
            initial_routes_synced: false,
        }
    }

    pub fn split_threshold(&self) -> usize {
        self.split_threshold
    }

    /// 把当前路由表写进 meta 组（普通提案，保证路由变更一致复制）。
    /// meta 组还没选主时跳过，由 advance 里的惰性同步重试。
    fn sync_routes_to_meta(&mut self) {
        if self.sim.leader_of(META_GROUP).is_none() {
            return;
        }
        let entries: Vec<(Vec<u8>, Vec<u8>)> = self
            .manager
            .snapshot()
            .iter()
            .map(ShardManager::route_entry)
            .collect();
        for (key, value) in entries {
            self.meta_put(key, value);
        }
    }

    /// meta 组写入（内部键，不进客户端历史）。
    fn meta_put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let Some(leader) = self.sim.leader_of(META_GROUP) else {
            return; // 等选主后由 sync_routes_to_meta 重试
        };
        let req = self.sim.next_internal_req();
        let node_key = (leader, META_GROUP);
        let now = self.sim.now;
        let msg = crate::raft::Message::ClientProposal {
            cmd: crate::raft::Command::Put { key, value },
            client_req: req,
        };
        if let Some(node) = self.sim.node_mut(leader, META_GROUP) {
            let events = node.advance(now, vec![msg]);
            self.sim.handle_events_pub(node_key, events);
        }
    }

    /// 从 meta 组 leader 读回路由表并重建管理器（路由刷新）。
    pub fn refresh_routes(&mut self) {
        let leader = match self.sim.leader_of(META_GROUP) {
            Some(l) => l,
            None => return,
        };
        let sm = self.sim.node(leader, META_GROUP).unwrap().state_machine();
        let snap = sm.snapshot();
        let kv: BTreeMap<Vec<u8>, Vec<u8>> = crate::storage::json_snapshot_decode(&snap).unwrap();
        if let Some(mgr) = ShardManager::load_from_meta(&kv, self.split_threshold) {
            self.manager = mgr;
        }
    }

    /// 路由写。返回实际落到的组。
    pub fn put(&mut self, client_id: u64, key: Vec<u8>, value: Vec<u8>) -> GroupId {
        let group = self.manager.route(&key).expect("key outside all shards").group;
        self.sim.client_put(client_id, group, key, value);
        group
    }

    pub fn delete(&mut self, client_id: u64, key: Vec<u8>) -> GroupId {
        let group = self.manager.route(&key).expect("key outside all shards").group;
        self.sim.client_delete(client_id, group, key);
        group
    }

    pub fn read(&mut self, client_id: u64, key: Vec<u8>, mode: ReadMode) -> GroupId {
        let group = self.manager.route(&key).expect("key outside all shards").group;
        self.sim.client_read(client_id, group, key, mode);
        group
    }

    pub fn advance(&mut self, ms: u64) {
        self.sim.advance_to(self.sim.now + ms);
        // meta 选主后惰性补一次初始路由
        if !self.initial_routes_synced && self.sim.leader_of(META_GROUP).is_some() {
            self.sync_routes_to_meta();
            self.initial_routes_synced = true;
        }
        self.check_and_split();
    }

    /// 检查每个数据组，超阈值就执行一次分裂编排。
    fn check_and_split(&mut self) {
        let groups: Vec<GroupId> = self
            .manager
            .all_shards()
            .iter()
            .filter(|m| !m.splitting)
            .map(|m| m.group)
            .collect();
        for group in groups {
            if self.sim.leader_of(group).is_none() {
                continue;
            }
            let keys = self.sim.leader_business_keys(group);
            let plan = match self.manager.plan_split(group, &keys) {
                Some(p) => p,
                None => continue,
            };
            self.execute_split(group, plan, &keys);
        }
    }

    fn execute_split(&mut self, old_group: GroupId, plan: crate::shard::SplitPlan, keys: &[Vec<u8>]) {
        // 1. 登记新分片（路由尚未切换）
        self.manager.apply_new_shard(&plan);
        self.sync_routes_to_meta();

        // 2. 从老组 leader 导出 [split_key, end) 数据
        let migrated = self.sim.export_leader_range(old_group, &plan.split_key);
        let migrated_count = migrated.len();

        // 3. 用导出数据引导新 Raft 组
        self.sim.bootstrap_group_with_data(plan.new_group, plan.voters.clone(), &migrated);
        self.sim.advance_to(self.sim.now + 2_000); // 等新组选主、内部复制稳定

        // 4. 路由正式切换：老片收缩 end，新片就绪
        self.manager.apply_split_complete(&plan);
        self.sync_routes_to_meta();

        // 5. 老组删除已迁走的 key 范围
        self.sim.delete_range_on_group(old_group, &plan.split_key);
        self.sim.advance_to(self.sim.now + 2_000);

        self.split_history.push(SplitRecord {
            old_shard: plan.old_shard,
            new_shard: plan.new_shard,
            old_group,
            new_group: plan.new_group,
            split_key: plan.split_key.clone(),
            at_ms: self.sim.now,
            migrated_keys: migrated_count,
        });
        let _ = keys;
    }

    /// 页面用：当前所有分片视图。
    pub fn shard_views(&self) -> Vec<ShardMeta> {
        self.manager.snapshot()
    }

    /// 从 meta 组读回的权威路由（验证 meta 持久化用）。
    pub fn meta_route_table(&self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let leader = self.sim.leader_of(META_GROUP).unwrap();
        let sm = self.sim.node(leader, META_GROUP).unwrap().state_machine();
        let snap = sm.snapshot();
        let kv: BTreeMap<Vec<u8>, Vec<u8>> = crate::storage::json_snapshot_decode(&snap).unwrap();
        kv.into_iter().filter(|(k, _)| k.starts_with(ROUTE_PREFIX)).collect()
    }
}
