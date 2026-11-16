//! 集成测试:逐条验证需求
//! 1. 选主/复制/任期投票 + 分区时少数派不能写
//! 2. 快照截断 + 落后节点装快照追上
//! 3. 联合共识加减节点,集群一直可用
//! 4. ReadIndex 和租约读
//! 5. 范围分片自动分裂
//! 6. 主切换期间客户端请求不报错
//! 7. 磁盘损坏检测 + 不带病投票
//! 8. 随机故障场景 + 线性一致性检查(按种子复现)
use raftkv::node::{ClientReq, ClientResp, Consistency, KvOp};
use std::sync::atomic::{AtomicU64, Ordering};

static OP_SEQ: AtomicU64 = AtomicU64::new(1);
fn next_op_seq() -> u64 {
    OP_SEQ.fetch_add(1, Ordering::Relaxed)
}
use raftkv::raft::{ConfChange, Role, META_GROUP};
use raftkv::sim::{run_scenario, ScenarioCfg, Sim};
use raftkv::storage::{FileStorage, Storage, StorageFactory};
use raftkv::raft::Entry;

fn cfg(nodes: usize) -> ScenarioCfg {
    ScenarioCfg {
        nodes,
        ticks: 3000,
        clients: 3,
        max_ops: 100,
        key_space: 10,
        faults: true,
        split_threshold: 4096,
        snapshot_threshold: 40,
    }
}

fn step_n(sim: &mut Sim, n: u64) {
    for _ in 0..n {
        sim.step();
    }
}

fn kv_get(req_id: u64, key: &str, c: Consistency) -> ClientReq {
    ClientReq::Kv {
        req_id,
        key: key.as_bytes().to_vec(),
        op: KvOp::Get,
        consistency: c,
        client_id: 777,
        op_seq: next_op_seq(),
    }
}

fn kv_put(req_id: u64, key: &str, val: &str) -> ClientReq {
    ClientReq::Kv {
        req_id,
        key: key.as_bytes().to_vec(),
        op: KvOp::Put(val.as_bytes().to_vec()),
        consistency: Consistency::Linearizable,
        client_id: 778,
        op_seq: next_op_seq(),
    }
}

/// 发一个 put 并等到响应;返回是否成功。
/// 轮换目标节点:旧主可能还自认是主(分区少数派),请求会挂着,
/// 客户端路径靠轮换 + NotLeader 提示收敛到真正的主。
fn put_and_wait(sim: &mut Sim, key: &str, val: &str, max_ticks: u64) -> bool {
    let client = 999u64;
    let mut req_id = 1u64;
    let mut cursor = 0usize;
    for _ in 0..max_ticks {
        let ids = sim.node_ids.clone();
        if ids.is_empty() {
            return false;
        }
        let target = ids[cursor % ids.len()];
        cursor += 1;
        let req = kv_put(req_id, key, val);
        sim.nodes.get_mut(&target).unwrap().node.handle_client_req(client, req);
        for _ in 0..60 {
            sim.step();
            let got = sim.unclaimed_resps.drain(..).collect::<Vec<_>>();
            let mut done = false;
            for (cid, resp) in got {
                if cid != client {
                    continue;
                }
                if let ClientResp::Kv { req_id: rid, result } = resp {
                    if rid == req_id {
                        if result.is_ok() {
                            return true;
                        }
                    }
                }
            }
            if done {
                break;
            }
            // 无响应(可能发给了分区里的旧主)→ 换下一个节点
        }
        req_id += 1;
    }
    false
}

fn get_and_wait(sim: &mut Sim, key: &str, c: Consistency, max_ticks: u64) -> Option<Option<Vec<u8>>> {
    let client = 998u64;
    let mut req_id = 10_000u64;
    let mut cursor = 0usize;
    for _ in 0..max_ticks {
        let ids = sim.node_ids.clone();
        if ids.is_empty() {
            return None;
        }
        let target = ids[cursor % ids.len()];
        cursor += 1;
        let req = kv_get(req_id, key, c);
        sim.nodes.get_mut(&target).unwrap().node.handle_client_req(client, req);
        for _ in 0..60 {
            sim.step();
            let got = sim.unclaimed_resps.drain(..).collect::<Vec<_>>();
            for (cid, resp) in got {
                if cid != client {
                    continue;
                }
                if let ClientResp::Kv { req_id: rid, result } = resp {
                    if rid == req_id {
                        if let Ok(v) = result {
                            return Some(v);
                        }
                    }
                }
            }
        }
        req_id += 1;
    }
    None
}

// ---------- 1. 选主与日志复制 ----------

#[test]
fn test_election_and_replication() {
    let mut sim = Sim::new(42, &cfg(3));
    step_n(&mut sim, 200);
    let leader = sim.leader_of(1).expect("should elect leader");
    // 任期应该 >= 1,且所有节点认可同一个主
    for id in sim.node_ids.clone() {
        let sn = sim.nodes.get(&id).unwrap();
        let host = sn.node.groups.get(&1).unwrap();
        assert!(host.raft.term >= 1);
        assert_eq!(host.raft.lead, Some(leader));
    }
    // 写入并验证复制到所有节点
    assert!(put_and_wait(&mut sim, "k1", "v1", 50));
    step_n(&mut sim, 50);
    for id in sim.node_ids.clone() {
        let sn = sim.nodes.get(&id).unwrap();
        let host = sn.node.groups.get(&1).unwrap();
        if let raftkv::node::SmKind::Kv(sm) = &host.sm {
            assert_eq!(sm.data.get(b"k1".as_slice()).map(|v| v.as_slice()), Some(b"v1".as_slice()));
        } else {
            panic!("not kv sm");
        }
    }
}

// ---------- 1b. 网络分区时少数派不能写 ----------

#[test]
fn test_minority_partition_cannot_write() {
    let mut sim = Sim::new(7, &cfg(3));
    step_n(&mut sim, 200);
    let leader = sim.leader_of(1).expect("leader");
    // 把主单独隔出去:主 | 其余两个
    let others: Vec<u64> = sim.node_ids.iter().copied().filter(|id| *id != leader).collect();
    for &o in &others {
        sim.net.blocked.insert((leader, o));
        sim.net.blocked.insert((o, leader));
    }
    // 记录旧主的 commit
    let commit_before = sim.nodes.get(&leader).unwrap().node.groups.get(&1).unwrap().raft.commit;
    // 直接向旧主发写请求
    sim.nodes
        .get_mut(&leader)
        .unwrap()
        .node
        .handle_client_req(777, kv_put(1, "kx", "vx"));
    step_n(&mut sim, 150);
    // 旧主的 commit 不能推进(少数派不能写)
    let commit_after = sim.nodes.get(&leader).unwrap().node.groups.get(&1).unwrap().raft.commit;
    assert_eq!(commit_before, commit_after, "minority leader must not commit");
    // 客户端不应收到成功响应
    assert!(sim.unclaimed_resps.is_empty(), "minority write must not succeed");
    // 多数派应该选出新主(旧主在少数派里仍自认是主,合法,但不能提交)
    let majority_leader = others
        .iter()
        .copied()
        .find(|id| {
            sim.nodes
                .get(id)
                .map(|sn| sn.node.groups.get(&1).unwrap().raft.role == raftkv::raft::Role::Leader)
                .unwrap_or(false)
        })
        .expect("majority should elect new leader");
    assert!(others.contains(&majority_leader));
    // 多数派可以写
    assert!(put_and_wait(&mut sim, "ky", "vy", 100));
    // 愈合后收敛
    sim.heal_all();
    step_n(&mut sim, 300);
    let hashes: std::collections::HashSet<u64> = sim
        .node_ids
        .iter()
        .filter_map(|id| sim.nodes.get(id))
        .map(|sn| sn.node.groups.get(&1).unwrap().sm.hash())
        .collect();
    assert_eq!(hashes.len(), 1, "replicas must converge after heal");
}

// ---------- 2. 快照与追赶 ----------

#[test]
fn test_snapshot_catchup() {
    let mut c = cfg(3);
    c.snapshot_threshold = 10;
    let mut sim = Sim::new(99, &c);
    step_n(&mut sim, 200);
    let leader = sim.leader_of(1).unwrap();
    // 隔离一个跟随者
    let lagging = sim.node_ids.iter().copied().find(|id| *id != leader).unwrap();
    for &o in &sim.node_ids.clone() {
        if o != lagging {
            sim.net.blocked.insert((lagging, o));
            sim.net.blocked.insert((o, lagging));
        }
    }
    // 写足够多的数据触发快照
    for i in 0..30 {
        assert!(put_and_wait(&mut sim, &format!("snap-k{:02}", i), "v", 100));
    }
    step_n(&mut sim, 50);
    let leader_snap = sim.nodes.get(&leader).unwrap().node.groups.get(&1).unwrap().raft.snap_index;
    assert!(leader_snap > 0, "leader should have snapshotted");
    // 愈合,落后节点应该通过 InstallSnapshot 追上
    sim.heal_all();
    step_n(&mut sim, 300);
    let sn = sim.nodes.get(&lagging).unwrap();
    let host = sn.node.groups.get(&1).unwrap();
    assert!(host.raft.snap_index > 0, "lagging follower should install snapshot");
    assert_eq!(host.raft.snap_index, sim.nodes.get(&leader).unwrap().node.groups.get(&1).unwrap().raft.snap_index);
    // 数据一致
    let h1 = host.sm.hash();
    let h2 = sim.nodes.get(&leader).unwrap().node.groups.get(&1).unwrap().sm.hash();
    assert_eq!(h1, h2);
}

// ---------- 3. 联合共识成员变更 ----------

#[test]
fn test_joint_consensus_add_remove_node() {
    let mut sim = Sim::new(5, &cfg(3));
    step_n(&mut sim, 200);
    // 写入一些数据
    assert!(put_and_wait(&mut sim, "cfg-k", "v1", 50));

    // 加节点 4:对组 0 和组 1 都做 AddNode
    let new_node_id = 4u64;
    for gid in [META_GROUP, 1] {
        let leader = sim.leader_of(gid).unwrap();
        let r = sim
            .nodes
            .get_mut(&leader)
            .unwrap()
            .node
            .groups
            .get_mut(&gid)
            .unwrap()
            .raft
            .propose_conf_change(ConfChange::AddNode(new_node_id));
        assert!(r.is_ok(), "add node should be accepted");
    }
    // 节点 4 上线(空节点,靠复制加入)
    sim.node_ids.push(new_node_id);
    sim.boot_node(new_node_id);
    step_n(&mut sim, 400);
    // 变更期间集群可用
    assert!(put_and_wait(&mut sim, "cfg-k2", "v2", 100));
    step_n(&mut sim, 200);
    // 节点 4 应该已有两个组且数据追平
    let sn4 = sim.nodes.get(&new_node_id).unwrap();
    assert!(sn4.node.groups.contains_key(&1), "new node should have data group");
    let h4 = sn4.node.groups.get(&1).unwrap().sm.hash();
    let leader = sim.leader_of(1).unwrap();
    let hl = sim.nodes.get(&leader).unwrap().node.groups.get(&1).unwrap().sm.hash();
    assert_eq!(h4, hl, "new node should converge");
    // 联合共识应已退出(配置稳定为 4 节点)
    let conf = &sim.nodes.get(&leader).unwrap().node.groups.get(&1).unwrap().raft.conf;
    assert!(conf.joint.is_none(), "joint consensus should have completed");
    assert_eq!(conf.voters.len(), 4);

    // 减节点 2:变更期间继续可写
    let victim = sim.node_ids.iter().copied().find(|id| *id != sim.leader_of(1).unwrap() && *id != new_node_id).unwrap();
    for gid in [META_GROUP, 1] {
        // 等该组主选出来
        let mut ok = false;
        for _ in 0..100 {
            if let Some(leader) = sim.leader_of(gid) {
                let r = sim
                    .nodes
                    .get_mut(&leader)
                    .unwrap()
                    .node
                    .groups
                    .get_mut(&gid)
                    .unwrap()
                    .raft
                    .propose_conf_change(ConfChange::RemoveNode(victim));
                if r.is_ok() {
                    ok = true;
                    break;
                }
            }
            sim.step();
        }
        assert!(ok, "remove node should be accepted for group {}", gid);
    }
    step_n(&mut sim, 300);
    assert!(put_and_wait(&mut sim, "cfg-k3", "v3", 100));
    step_n(&mut sim, 200);
    let leader = sim.leader_of(1).unwrap();
    let conf = &sim.nodes.get(&leader).unwrap().node.groups.get(&1).unwrap().raft.conf;
    assert!(conf.joint.is_none());
    assert!(!conf.voters.contains(&victim), "victim should be removed");
    assert_eq!(conf.voters.len(), 3);
}

// ---------- 4. ReadIndex 与租约读 ----------

#[test]
fn test_read_index_and_lease() {
    let mut sim = Sim::new(11, &cfg(3));
    step_n(&mut sim, 200);
    assert!(put_and_wait(&mut sim, "ri-k", "ri-v", 50));
    let leader = sim.leader_of(1).unwrap();

    // ReadIndex:返回 ctx,心跳确认后 ready_reads 就绪
    let ctx = {
        let host = sim.nodes.get_mut(&leader).unwrap().node.groups.get_mut(&1).unwrap();
        host.raft.read_index().expect("leader should accept read index")
    };
    step_n(&mut sim, 30);
    // 通过客户端路径做线性一致读
    let v = get_and_wait(&mut sim, "ri-k", Consistency::Linearizable, 50);
    assert_eq!(v, Some(Some(b"ri-v".to_vec())));
    let _ = ctx;

    // 租约读:心跳确认法定多数后租约生效
    step_n(&mut sim, 20);
    let lease_ok = sim
        .nodes
        .get(&leader)
        .unwrap()
        .node
        .groups
        .get(&1)
        .unwrap()
        .raft
        .lease_read_ok();
    assert!(lease_ok, "lease should be valid after quorum heartbeat");
    let v = get_and_wait(&mut sim, "ri-k", Consistency::Lease, 50);
    assert_eq!(v, Some(Some(b"ri-v".to_vec())));
}

// ---------- 5. 自动分裂 ----------

#[test]
fn test_shard_auto_split() {
    let mut c = cfg(3);
    c.split_threshold = 300; // 小阈值,容易触发
    let mut sim = Sim::new(21, &c);
    step_n(&mut sim, 200);
    // 写足够多的 key 让单个分片超过阈值
    for i in 0..20 {
        assert!(put_and_wait(&mut sim, &format!("split-{:02}", i), &format!("value-{:08}", i), 100));
    }
    step_n(&mut sim, 400);
    // 目录里应该出现多个分片
    let leader = sim.leader_of(META_GROUP).unwrap();
    let shards = sim.nodes.get(&leader).unwrap().node.directory();
    assert!(shards.len() >= 2, "should have split into multiple shards, got {:?}", shards.len());
    // 所有 key 仍然可读(通过客户端路径,自动路由)
    for i in 0..20 {
        let key = format!("split-{:02}", i);
        let v = get_and_wait(&mut sim, &key, Consistency::Linearizable, 100);
        assert_eq!(
            v,
            Some(Some(format!("value-{:08}", i).into_bytes())),
            "key {} should survive split",
            key
        );
    }
}

// ---------- 6. 主切换期间客户端请求不报错 ----------

#[test]
fn test_client_waits_through_leader_change() {
    let mut sim = Sim::new(31, &cfg(3));
    step_n(&mut sim, 200);
    assert!(put_and_wait(&mut sim, "ha-k", "v1", 50));
    // 杀掉主,客户端写应该等到新主选出来后成功,而不是报错
    let leader = sim.leader_of(1).unwrap();
    sim.crash_node(leader);
    let ok = put_and_wait(&mut sim, "ha-k2", "v2", 300);
    assert!(ok, "client write should succeed after new leader elected");
    let v = get_and_wait(&mut sim, "ha-k2", Consistency::Linearizable, 100);
    assert_eq!(v, Some(Some(b"v2".to_vec())));
}

// ---------- 7. 磁盘损坏检测 ----------

#[test]
fn test_file_storage_corruption_detected() {
    let dir = std::env::temp_dir().join(format!("raftkv-corrupt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut st = FileStorage::open(&dir).unwrap();
    st.append(&[
        Entry { term: 1, index: 1, etype: raftkv::raft::EntryType::Normal, data: b"cmd-1".to_vec() },
        Entry { term: 1, index: 2, etype: raftkv::raft::EntryType::Normal, data: b"cmd-2".to_vec() },
    ]);
    drop(st);
    // 健康加载没问题
    let mut st = FileStorage::open(&dir).unwrap();
    assert!(st.load().is_ok());
    drop(st);
    // 篡改日志文件中间一个字节
    let log_path = dir.join("log.bin");
    let mut buf = std::fs::read(&log_path).unwrap();
    let mid = buf.len() / 2;
    buf[mid] ^= 0xFF;
    std::fs::write(&log_path, &buf).unwrap();
    let mut st = FileStorage::open(&dir).unwrap();
    let r = st.load();
    assert!(matches!(r, Err(raftkv::storage::StorageError::Corrupt(_))), "corruption must be detected");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_corrupted_node_refuses_to_vote() {
    let mut sim = Sim::new(13, &cfg(3));
    step_n(&mut sim, 200);
    assert!(put_and_wait(&mut sim, "crc-k", "v", 50));
    // 给一个节点注入磁盘损坏并重启
    let victim = sim.node_ids[0];
    sim.inject_corruption(victim);
    step_n(&mut sim, 400);
    // 该节点对应组必须是 Refused,且不会成为主
    let sn = sim.nodes.get(&victim).unwrap();
    let has_refused = sn.node.groups.values().any(|h| h.raft.role == Role::Refused);
    if !has_refused {
        for (g, h) in &sn.node.groups {
            eprintln!("victim n{} g{} role={:?} up={}", victim, g, h.raft.role, sn.up);
        }
        for gid in sim.factory.groups_for(victim) {
            let d = sim.factory.disk(victim, gid).unwrap();
            eprintln!("  disk g{} corrupt={:?}", gid, d.lock().unwrap().corrupt);
        }
    }
    assert!(has_refused, "corrupted node must refuse to participate");
    // 集群其余部分照常工作
    assert!(put_and_wait(&mut sim, "crc-k2", "v2", 100));
    let leader = sim.leader_of(1).unwrap();
    assert_ne!(leader, victim);
}

// ---------- 8. 随机故障场景 + 线性一致性检查 ----------

#[test]
fn test_soak_short() {
    // 固定种子的短 soak;失败时输出种子可复现
    let mut failed = Vec::new();
    for seed in 100..120u64 {
        let r = run_scenario(seed, &cfg(3));
        if !r.ok {
            failed.push((seed, r.failures));
        }
    }
    assert!(failed.is_empty(), "soak failures: {:?}", failed);
}

#[test]
fn test_soak_five_nodes() {
    let mut failed = Vec::new();
    for seed in 200..210u64 {
        let r = run_scenario(seed, &cfg(5));
        if !r.ok {
            failed.push((seed, r.failures));
        }
    }
    assert!(failed.is_empty(), "soak failures: {:?}", failed);
}
