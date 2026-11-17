//! Raft 核心集成测试（通过确定性模拟层驱动）。

use std::path::PathBuf;

use rkv::checker::check;
use rkv::raft::{ConfChange, ReadMode, RaftConfig, Role};
use rkv::sim::scenario::{run_scenario, ScenarioConfig};
use rkv::sim::{SimCluster, SimConfig};

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rkv-it-{}-{}-{}",
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

fn clean_sim(seed: u64) -> SimConfig {
    SimConfig {
        seed,
        loss_pct: 0,
        duplicate_pct: 0,
        delay_min_ms: 5,
        delay_max_ms: 10,
        tick_ms: 10,
    }
}

/// 等某组选出 leader。
fn wait_leader(c: &mut SimCluster, group: u64, until_ms: u64) -> u64 {
    c.advance_to(until_ms);
    c.leader_of(group).expect("a leader should be elected")
}

#[test]
fn clean_cluster_elects_and_replicates() {
    let dir = temp_dir("clean");
    let mut c = SimCluster::new(&dir, clean_sim(101), fast_cfg());
    c.add_group(1, vec![1, 2, 3]);
    let _leader = wait_leader(&mut c, 1, 2_000);

    for i in 0..30u64 {
        c.client_put(i % 3, 1, format!("k{i}").into_bytes(), format!("v{i}").into_bytes());
    }
    c.advance_to(4_000);
    assert_eq!(c.pending_count(), 0, "all writes must complete");

    // leader 状态机可读
    for i in 0..30u64 {
        assert_eq!(
            c.read_committed(1, format!("k{i}").as_bytes()),
            Some(format!("v{i}").into_bytes())
        );
    }
    // 所有存活节点 commit/applied 追平
    for id in 1..=3 {
        let n = c.node(id, 1).unwrap();
        assert_eq!(n.applied, n.commit);
        assert!(n.applied >= 31, "node {id} should replicate all entries, got {}", n.applied);
    }
    let report = check(c.history());
    assert!(report.ok, "history must be linearizable: {:?}", report.violations);
}

#[test]
fn minority_partition_cannot_write_majority_can() {
    let dir = temp_dir("partition");
    let mut c = SimCluster::new(&dir, clean_sim(202), fast_cfg());
    c.add_group(1, vec![1, 2, 3, 4, 5]);
    let _leader = wait_leader(&mut c, 1, 2_000);

    // 把两个节点隔成少数派（2/5），另外三个为多数派
    let minority: Vec<u64> = vec![1, 2];
    let majority: Vec<u64> = vec![3, 4, 5];
    c.partition(vec![minority.clone(), majority.clone()]);

    c.advance_to(4_000); // 多数派选出新 leader

    // 多数派写必须成功
    c.client_put(99, 1, b"majority-key".to_vec(), b"ok".to_vec());
    c.advance_to(5_500);
    assert_eq!(c.pending_count(), 0);
    assert_eq!(c.read_committed(1, b"majority-key"), Some(b"ok".to_vec()));

    let maj_leader = c.leader_of(1).expect("majority elects leader");
    assert!(majority.contains(&maj_leader), "new leader must be in majority");

    // 愈合后旧 leader 降级、日志追平到多数派水位
    c.heal_partition();
    c.advance_to(8_000);
    let majority_watermark = majority
        .iter()
        .map(|id| c.node(*id, 1).unwrap().applied)
        .min()
        .unwrap();
    for id in minority {
        let n = c.node(id, 1).unwrap();
        assert_ne!(n.role, Role::Leader, "old minority leader must step down: node {id}");
        assert!(
            n.applied >= majority_watermark,
            "minority node {id} must catch up to {majority_watermark}, applied={}",
            n.applied
        );
    }
    let report = check(c.history());
    assert!(report.ok, "{:?}", report.violations);
}

#[test]
fn leader_failure_keeps_committed_data() {
    let dir = temp_dir("leader-fail");
    let mut c = SimCluster::new(&dir, clean_sim(303), fast_cfg());
    c.add_group(1, vec![1, 2, 3]);
    let first = wait_leader(&mut c, 1, 2_000);

    for i in 0..20u64 {
        c.client_put(1, 1, format!("keep-{i}").into_bytes(), b"x".to_vec());
    }
    c.advance_to(3_500);
    assert_eq!(c.pending_count(), 0);

    c.shutdown_node(first);
    c.advance_to(6_000); // 重新选主
    let second = c.leader_of(1).expect("re-election");
    assert_ne!(second, first);

    // 已提交数据一条不丢
    for i in 0..20u64 {
        assert_eq!(
            c.read_committed(1, format!("keep-{i}").as_bytes()),
            Some(b"x".to_vec()),
            "committed entry {i} lost after leader failure"
        );
    }

    // 旧 leader 回来，靠日志/快照追平且不破坏一致性
    c.restart_node(first);
    c.advance_to(9_000);
    let n = c.node(first, 1).unwrap();
    assert_eq!(n.role, Role::Follower);
    assert!(n.applied >= 21);
    let report = check(c.history());
    assert!(report.ok, "{:?}", report.violations);
}

#[test]
fn snapshot_compaction_and_slow_follower_catchup() {
    let dir = temp_dir("snapshot");
    let mut cfg = fast_cfg();
    cfg.snapshot_threshold = 20; // 小阈值，尽快触发快照
    let mut c = SimCluster::new(&dir, clean_sim(404), cfg);
    c.add_group(1, vec![1, 2, 3]);
    wait_leader(&mut c, 1, 2_000);

    // 先停一个 follower，让它远远落后
    c.shutdown_node(3);
    for i in 0..120u64 {
        c.client_put(1, 1, format!("snap-{i:03}").into_bytes(), b"d".to_vec());
    }
    c.advance_to(6_000);
    assert_eq!(c.pending_count(), 0);

    // leader 应该已经打了多次快照并截断 WAL
    let leader = c.leader_of(1).unwrap();
    let snap_idx = c.node(leader, 1).unwrap().snapshot_index();
    assert!(snap_idx >= 40, "expected compaction, snapshot_index={snap_idx}");
    assert!(c.node(leader, 1).unwrap().log_len() < 60);

    // 落后节点重启：它的日志已经被 leader 压缩，必须走 InstallSnapshot
    c.restart_node(3);
    c.advance_to(10_000);
    let n3 = c.node(3, 1).unwrap();
    assert!(n3.applied >= 121, "slow follower must catch up via snapshot, got {}", n3.applied);
    assert!(n3.snapshot_index() > 0);
    for i in 0..120u64 {
        assert_eq!(
            c.node(3, 1).unwrap().state_machine().get(format!("snap-{i:03}").as_bytes()),
            Some(b"d".to_vec())
        );
    }
}

#[test]
fn joint_consensus_add_and_remove_node() {
    let dir = temp_dir("joint");
    let mut c = SimCluster::new(&dir, clean_sim(505), fast_cfg());
    c.add_group(1, vec![1, 2, 3]);
    wait_leader(&mut c, 1, 2_000);

    // 准备节点 4 的目录，先发起 AddVoter
    c.prepare_new_node_dir(4, 1);
    let leader = c.leader_of(1).unwrap();
    c.propose_conf_change(leader, 1, ConfChange::AddVoter { node: 4 }).unwrap();
    c.advance_to(3_000);
    // 变更提交期间集群应持续可用
    c.client_put(1, 1, b"during-add".to_vec(), b"1".to_vec());
    c.advance_to(3_500);
    assert_eq!(c.pending_count(), 0);

    // 节点 4 以空白身份加入，靠快照追上
    c.start_new_node_blank(4, 1);
    c.advance_to(7_000);
    let n4 = c.node(4, 1).unwrap();
    assert!(n4.applied >= 1, "new node must receive state");
    for id in 1..=4 {
        let voters = c.node(id, 1).unwrap().conf.voters.clone();
        assert!(voters.contains(&4), "node {id} should see node 4 in voters");
        assert_eq!(c.node(id, 1).unwrap().in_joint, false);
    }

    // 再移除节点 2
    let leader = c.leader_of(1).unwrap();
    c.propose_conf_change(leader, 1, ConfChange::RemoveVoter { node: 2 }).unwrap();
    c.advance_to(9_000);
    for id in [1, 3, 4] {
        let voters = c.node(id, 1).unwrap().conf.voters.clone();
        assert!(!voters.contains(&2), "node 2 should be removed");
        assert_eq!(voters.len(), 3);
    }
    // 变更全程写入仍然线性一致
    let report = check(c.history());
    assert!(report.ok, "{:?}", report.violations);
}

#[test]
fn disk_corruption_refuses_to_participate() {
    use rkv::storage::CorruptionKind;
    let dir = temp_dir("corrupt");
    let mut c = SimCluster::new(&dir, clean_sim(606), fast_cfg());
    c.add_group(1, vec![1, 2, 3]);
    wait_leader(&mut c, 1, 2_000);
    c.client_put(1, 1, b"x".to_vec(), b"1".to_vec());
    c.advance_to(3_000);

    // 停节点 2，破坏它的硬状态
    c.shutdown_node(2);
    c.corrupt_disk(2, 1, CorruptionKind::FlipHardState).unwrap();
    c.restart_node(2);
    let n2 = c.node(2, 1).unwrap();
    assert_eq!(n2.role, Role::Faulted, "corrupt node must be Faulted");
    assert!(n2.fault_reason().is_some());

    // 其余节点继续正常服务，多数派不受影响
    c.advance_to(5_000);
    c.client_put(1, 1, b"y".to_vec(), b"2".to_vec());
    c.advance_to(6_500);
    assert_eq!(c.pending_count(), 0);
    assert_eq!(c.read_committed(1, b"y"), Some(b"2".to_vec()));

    // 修复：清空重来，靠快照追上
    c.repair_node(2, 1);
    c.advance_to(10_000);
    let n2 = c.node(2, 1).unwrap();
    assert_ne!(n2.role, Role::Faulted);
    assert!(n2.applied >= 1);
}

#[test]
fn same_seed_replays_identically() {
    use rkv::sim::scenario::run_scenario;
    let sc = ScenarioConfig {
        node_count: 5,
        duration_ms: 8_000,
        clients: 3,
        key_space: 8,
        ..Default::default()
    };
    let d1 = temp_dir("det1");
    let d2 = temp_dir("det2");
    let r1 = run_scenario(&d1, 777, &sc, fast_cfg());
    let r2 = run_scenario(&d2, 777, &sc, fast_cfg());
    assert_eq!(r1.stats.history_ops, r2.stats.history_ops);
    assert_eq!(r1.states_explored, r2.states_explored);
    assert!(r1.linearizable && r2.linearizable);

    // 不同种子也应独立成立
    let d3 = temp_dir("det3");
    let r3 = run_scenario(&d3, 778, &sc, fast_cfg());
    assert!(r3.linearizable, "seed 778 violations: {:?}", r3.violation_count);
}

#[test]
fn random_scenarios_are_linearizable() {
    let sc = ScenarioConfig {
        node_count: 5,
        duration_ms: 12_000,
        clients: 4,
        key_space: 10,
        ..Default::default()
    };
    for seed in 1..=12 {
        let dir = temp_dir(&format!("rand-{seed}"));
        let r = run_scenario(&dir, seed, &sc, fast_cfg());
        assert!(
            r.linearizable,
            "seed {seed} broke linearizability ({} violations, evidence {:?})",
            r.violation_count,
            r.evidence_file
        );
    }
}

#[test]
fn lease_read_and_readindex_both_serve() {
    let dir = temp_dir("reads");
    let mut c = SimCluster::new(&dir, clean_sim(808), fast_cfg());
    c.add_group(1, vec![1, 2, 3]);
    wait_leader(&mut c, 1, 2_000);
    c.client_put(1, 1, b"rk".to_vec(), b"rv".to_vec());
    c.advance_to(3_000);

    c.client_read(2, 1, b"rk".to_vec(), ReadMode::ReadIndex);
    c.client_read(3, 1, b"rk".to_vec(), ReadMode::Lease);
    c.advance_to(4_500);
    assert_eq!(c.pending_count(), 0);

    let reads: Vec<_> = c
        .history()
        .iter()
        .filter_map(|op| match &op.kind {
            rkv::checker::OpKind::Read { observed, .. } => observed.clone(),
            _ => None,
        })
        .collect();
    assert_eq!(reads.len(), 2);
    assert!(reads.iter().all(|v| v.as_slice() == b"rv".as_slice()));
    assert!(check(c.history()).ok);
}
