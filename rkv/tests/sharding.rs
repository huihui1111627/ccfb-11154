//! 分片与自动分裂集成测试。

use rkv::raft::{RaftConfig, ReadMode};
use rkv::shard::driver::ShardCluster;
use rkv::sim::SimConfig;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rkv-shard-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn fast_cfg() -> RaftConfig {
    RaftConfig {
        election_timeout_min_ms: 120,
        election_timeout_max_ms: 240,
        heartbeat_interval_ms: 30,
        lease_duration_ms: 70,
        snapshot_threshold: 40,
        max_entries_per_msg: 64,
    }
}

#[test]
fn shard_splits_automatically_and_routes_correctly() {
    let dir = temp_dir("split");
    let sim_cfg = SimConfig {
        seed: 11,
        loss_pct: 0,
        duplicate_pct: 0,
        delay_min_ms: 5,
        delay_max_ms: 10,
        tick_ms: 10,
    };
    let threshold = 15;
    let mut cluster =
        ShardCluster::bootstrap(&dir, sim_cfg, fast_cfg(), vec![1, 2, 3], threshold);
    cluster.advance(2_000); // meta + 数据组选主、初始路由落 meta

    // 写入超过阈值的不同 key，触发至少一次分裂
    let total_keys = (threshold * 4) as u64;
    for i in 0..total_keys {
        let key = format!("item-{i:05}").into_bytes();
        cluster.put(i % 3, key, format!("val-{i}").into_bytes());
        if i % 10 == 0 {
            cluster.advance(400);
        }
    }
    cluster.advance(4_000);

    assert!(
        cluster.split_history.len() >= 1,
        "expected at least one split, got {}",
        cluster.split_history.len()
    );
    let shards = cluster.shard_views();
    assert!(shards.len() >= 2);

    // 路由区间互不重叠且完整覆盖：每个写入的 key 都能且只能路由到一个片
    for i in 0..total_keys {
        let key = format!("item-{i:05}");
        let owners: Vec<_> = shards.iter().filter(|m| m.contains(key.as_bytes())).collect();
        assert_eq!(owners.len(), 1, "key {key} must belong to exactly one shard");
    }

    // 分裂历史里每组都有独立 Raft 组号
    let groups: std::collections::BTreeSet<u64> =
        cluster.shard_views().iter().map(|m| m.group).collect();
    assert!(groups.len() >= 2, "each shard needs its own raft group");

    // 历史线性一致
    let report = rkv::checker::check(cluster.sim.history());
    assert!(report.ok, "shard history not linearizable: {:?}", report.violations);
}

#[test]
fn route_table_persisted_in_meta_group() {
    let dir = temp_dir("meta");
    let sim_cfg = SimConfig {
        seed: 12,
        loss_pct: 0,
        duplicate_pct: 0,
        delay_min_ms: 5,
        delay_max_ms: 10,
        tick_ms: 10,
    };
    let mut cluster =
        ShardCluster::bootstrap(&dir, sim_cfg, fast_cfg(), vec![1, 2, 3], 10);
    cluster.advance(2_000);
    for i in 0..40u64 {
        cluster.put(i % 2, format!("k-{i:04}").into_bytes(), b"v".to_vec());
        if i % 8 == 0 {
            cluster.advance(400);
        }
    }
    cluster.advance(3_000);

    // 从 meta 组权威状态重建路由表，应与内存视图一致
    cluster.refresh_routes();
    let meta = cluster.meta_route_table();
    assert!(!meta.is_empty(), "route table must be replicated in meta group");
    assert_eq!(meta.len(), cluster.shard_views().len());

    // 重建出的管理器路由结果一致
    let mgr = rkv::shard::ShardManager::load_from_meta(
        &meta.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        10,
    )
    .expect("route table decodes");
    let key = b"k-0007";
    let g1 = cluster.shard_views().iter().find(|m| m.contains(key)).unwrap().group;
    let g2 = mgr.route(key).unwrap().group;
    assert_eq!(g1, g2);
}

#[test]
fn reads_go_through_correct_shard_group() {
    let dir = temp_dir("readroute");
    let sim_cfg = SimConfig {
        seed: 13,
        loss_pct: 0,
        duplicate_pct: 0,
        delay_min_ms: 5,
        delay_max_ms: 10,
        tick_ms: 10,
    };
    let mut cluster =
        ShardCluster::bootstrap(&dir, sim_cfg, fast_cfg(), vec![1, 2, 3], 12);
    cluster.advance(2_000);
    for i in 0..50u64 {
        cluster.put(0, format!("r-{i:04}").into_bytes(), format!("d{i}").into_bytes());
        if i % 10 == 0 {
            cluster.advance(400);
        }
    }
    cluster.advance(3_000);

    // 分裂后读仍然返回最新写入值
    for i in (0..50u64).step_by(7) {
        let key = format!("r-{i:04}").into_bytes();
        cluster.read(1, key.clone(), ReadMode::ReadIndex);
        cluster.advance(300);
    }
    cluster.advance(2_000);
    let report = rkv::checker::check(cluster.sim.history());
    assert!(report.ok, "{:?}", report.violations);
}
