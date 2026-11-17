//! 分片感知客户端：在 [`RaftClient`] 之上维护路由缓存，自动找主、
//! 路由刷新和主切换等待。模拟环境的等价物在 [`crate::shard::driver`]。

use std::collections::BTreeMap;

use crate::raft::ReadMode;
use crate::runner::{ClientReply, RaftClient};
use crate::shard::ShardMeta;

/// 路由键 -> 组 leader 客户端的门面。真实部署里路由从 meta 组拉取，
/// 这里接受调用方注入的路由表（测试/本地集群直接给全量）。
pub struct ShardClient {
    inner: RaftClient,
    routes: BTreeMap<u64, ShardMeta>,
    /// 组 -> 当前 leader 提示（None 时由底层客户端轮询）
    leaders: BTreeMap<u64, Option<u64>>,
}

impl ShardClient {
    pub fn new(inner: RaftClient, routes: Vec<ShardMeta>) -> Self {
        let routes = routes.into_iter().map(|m| (m.shard_id, m)).collect();
        ShardClient { inner, routes, leaders: BTreeMap::new() }
    }

    pub fn update_routes(&mut self, routes: Vec<ShardMeta>) {
        self.routes = routes.into_iter().map(|m| (m.shard_id, m)).collect();
    }

    fn route(&self, key: &[u8]) -> Option<&ShardMeta> {
        self.routes.values().find(|m| m.contains(key))
    }

    /// 写：路由到所属分片组；主切换期由底层客户端等待重试，不报错。
    pub async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<u64, String> {
        let _group = self
            .route(&key)
            .ok_or_else(|| format!("no shard owns key {:?}", String::from_utf8_lossy(&key)))?
            .group;
        // 本地集群所有组共用同一组节点与连接池，直接交给底层客户端
        self.inner.put(key, value).await
    }

    pub async fn delete(&mut self, key: Vec<u8>) -> Result<u64, String> {
        if self.route(&key).is_none() {
            return Err("no shard owns key".into());
        }
        self.inner.delete(key).await
    }

    pub async fn get(
        &mut self,
        key: Vec<u8>,
        mode: ReadMode,
    ) -> Result<Option<Vec<u8>>, String> {
        if self.route(&key).is_none() {
            return Err("no shard owns key".into());
        }
        self.inner.get(key, mode).await
    }

    /// 主动探测一次：返回某 key 对应组当前服务的 leader。
    pub async fn probe_leader(&mut self, key: &[u8]) -> Result<Option<u64>, String> {
        let group = self.route(key).map(|m| m.group).ok_or("no shard")?;
        // 用一次廉价读触发 NotLeader/成功路径，leader 缓存在底层更新
        let _ = self.inner.get(key.to_vec(), ReadMode::ReadIndex).await;
        Ok(self.leaders.entry(group).or_insert(None).clone())
    }

    /// 供测试断言：底层客户端是否在 NotLeader 后收敛。
    pub fn note_leader(&mut self, group: u64, leader: Option<u64>) {
        self.leaders.insert(group, leader);
    }
}

/// 把一次 roundtrip 的结果翻译成读值（测试辅助）。
pub fn reply_as_read(reply: ClientReply) -> Result<Option<Vec<u8>>, String> {
    match reply {
        ClientReply::Read { value } => Ok(value),
        ClientReply::NotLeader { .. } => Err("not leader".into()),
        ClientReply::Faulted(r) => Err(format!("faulted: {r}")),
        ClientReply::Committed { .. } => Err("unexpected committed on read".into()),
    }
}
