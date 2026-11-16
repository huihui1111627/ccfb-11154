use raftkv::node::SmKind;
use raftkv::raft::Role;
use raftkv::storage::StorageFactory;
use rand::Rng;
use raftkv::sim::{ScenarioCfg, Sim};

fn main() {
    let seed: u64 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(201);
    let nodes: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(3);
    let snap_threshold: u64 = std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(40);
    let cfg = ScenarioCfg {
        nodes,
        ticks: 3000,
        clients: 3,
        max_ops: 120,
        key_space: 12,
        faults: true,
        split_threshold: 4096,
        snapshot_threshold: snap_threshold,
    };
    // 与 run_scenario 完全相同的流程
    let mut sim = Sim::new(seed, &cfg);
    let mut ops_issued = 0usize;
    let mut op_seq = 0u64;
    let mut warmup = 0u64;
    while sim.leader_of(raftkv::raft::META_GROUP).is_none() && warmup < 2000 {
        sim.step();
        warmup += 1;
    }
    for _ in 0..50 {
        sim.step();
    }
    let mut diverge_reported = false;
    let mut last_up = std::collections::HashMap::new();
    for _ in 0..cfg.ticks {
        sim.step();
        if sim.now % 25 == 0 && sim.now > 480 && sim.now < 930 {
            let sn = sim.nodes.get(&3).unwrap();
            if sn.up {
                let h = &sn.node.groups[&1];
                if let SmKind::Kv(kv) = &h.sm {
                    println!("t={} n3 applied={} commit={} snap={} key-007={:?}", sim.now, h.applied,
                        h.raft.commit, h.raft.snap_index,
                        kv.data.get(b"key-007".as_slice()).map(|v| String::from_utf8_lossy(v).into_owned()));
                }
            }
        }
        if sim.now % 10 == 0 && sim.now > 550 && sim.now < 950 {
            let c = &sim.clients[0];
            if let Some(p) = &c.pending {
                println!("t={} c10000 state={:?} req_id={} sent_at={} kind={:?}", sim.now, p.state, p.req_id, p.sent_at, p.op.kind);
            } else {
                println!("t={} c10000 IDLE", sim.now);
            }
        }
        if sim.now % 5 == 0 && sim.now > 600 && sim.now < 700 {
            let sn = sim.nodes.get(&3).unwrap();
            if sn.up {
                let h = &sn.node.groups[&1];
                if let SmKind::Kv(kv) = &h.sm {
                    println!("t={} n3 applied={} commit={} key-003={:?} lease_ok={}", sim.now, h.applied,
                        h.raft.commit,
                        kv.data.get(b"key-003".as_slice()).map(|v| String::from_utf8_lossy(v).into_owned()),
                        h.raft.lease_read_ok());
                }
            }
        }
        // 检测 n3 重启恢复后的 sm
        for id in [1u64, 2, 3] {
            let sn = sim.nodes.get(&id).unwrap();
            let was = last_up.get(&id).copied().unwrap_or(true);
            if sn.up && !was {
                if let SmKind::Kv(kv) = &sn.node.groups[&1].sm {
                    println!("t={} n{} RECOVERED: applied={} key-007={:?} commit={} snap={}", sim.now, id,
                        sn.node.groups[&1].applied,
                        kv.data.get(b"key-007".as_slice()).map(|v| String::from_utf8_lossy(v).into_owned()),
                        sn.node.groups[&1].raft.commit,
                        sn.node.groups[&1].raft.snap_index);
                }
            }
            last_up.insert(id, sn.up);
        }
        if sim.now % 25 == 0 && !diverge_reported {
            // 比较 n1 和 n3 在相同下标处的 sm 哈希
            let h1 = &sim.nodes.get(&1).unwrap().node.groups[&1].hashes;
            let h3 = &sim.nodes.get(&3).unwrap().node.groups[&1].hashes;
            for (idx, ha) in h1 {
                if let Some((_, hb)) = h3.iter().find(|(i, _)| i == idx) {
                    if ha != hb {
                        println!("t={} FIRST DIVERGE at index {} n1={:x} n3={:x}", sim.now, idx, ha, hb);
                        diverge_reported = true;
                        break;
                    }
                }
            }
        }
        sim.drive_clients(cfg.max_ops, cfg.key_space, &mut ops_issued, &mut op_seq);
        if cfg.faults {
            let r: f64 = sim.rng.gen();
            if r < 0.002 {
                sim.inject_partition();
            } else if r < 0.005 {
                let id = sim.node_ids[sim.rng.gen_range(0..sim.node_ids.len())];
                println!("t={} crash n{}", sim.now, id);
                sim.crash_node(id);
            } else if r < 0.0052 {
                let id = sim.node_ids[sim.rng.gen_range(0..sim.node_ids.len())];
                println!("t={} corrupt n{}", sim.now, id);
                sim.inject_corruption(id);
            }
        }
    }
    sim.heal_all();
    for i in 0..400 {
        sim.step();
        sim.drive_clients(0, cfg.key_space, &mut ops_issued, &mut op_seq);
        if i % 25 == 0 {
            let l = sim.leader_of(1);
            if let Some(l) = l {
                let prog = sim.nodes.get(&l).unwrap().node.groups[&1].raft.progress_snapshot();
                let n3 = &sim.nodes.get(&3).unwrap().node.groups[&1].raft;
                println!("t={} leader=n{} prog={:?} | n3 commit={} last={} snap={}", sim.now, l, prog, n3.commit, n3.last_index(), n3.snap_index);
            }
        }
    }
    // 与 run_scenario 一致:未完成操作记为不确定
    for c in &sim.clients {
        if let Some(p) = &c.pending {
            let mut op = p.op.clone();
            op.end = sim.now;
            op.indeterminate = true;
            sim.history.push(op);
        }
    }
    println!("history ops = {}", sim.history.len());
    match raftkv::checker::check_linearizable(&sim.history) {
        Ok(()) => println!("linearizable: OK"),
        Err(key) => {
            println!("VIOLATION on key {}", String::from_utf8_lossy(&key));
            let mut ops: Vec<_> = sim.history.iter().filter(|o| o.key == key).collect();
            ops.sort_by_key(|o| o.start);
            for o in ops {
                println!("  [{}-{}] {:?} {:?} -> {:?}", o.start, o.end, o.kind, o.read_path,
                    o.resp.as_ref().map(|v| String::from_utf8_lossy(v).into_owned()));
            }
        }
    }
    {
        let st = sim.stats.lock().unwrap();
        let mut els: Vec<_> = st.elections.iter().filter(|e| e.group == 1).collect();
        els.sort_by_key(|e| e.at_ms);
        for e in els.iter().rev().take(12) {
            println!("election g1 term={} winner=n{} dur={} at={}", e.term, e.winner, e.duration_ticks, e.at_ms);
        }
    }
    for id in sim.node_ids.clone() {
        let sn = sim.nodes.get(&id).unwrap();
        let mut gids: Vec<u64> = sn.node.groups.keys().copied().collect();
        gids.sort();
        for gid in gids {
            let h = &sn.node.groups[&gid];
            let desc = match &h.sm {
                SmKind::Meta(m) => format!("meta shards={}", m.shards.len()),
                SmKind::Kv(kv) => format!("kv keys={} size={}", kv.data.len(), kv.size),
            };
            println!(
                "n{} g{} role={:?} term={} commit={} applied={} snap={} loglen={} hash={:x} {}",
                id, gid, h.raft.role, h.raft.term, h.raft.commit, h.applied, h.raft.snap_index,
                h.raft.log_len(), h.sm.hash(), desc
            );
        }
        // 磁盘上的状态
        for gid in sim.factory.groups_for(id) {
            let disk = sim.factory.disk(id, gid).unwrap();
            let d = disk.lock().unwrap();
            println!(
                "  disk n{} g{}: corrupt={:?} hs={:?} entries={} snap={:?}",
                id, gid, d.corrupt,
                d.hs.as_ref().map(|h| (h.term, h.vote, h.commit)),
                d.entries.len(),
                d.snap.as_ref().map(|s| (s.index, s.data.len()))
            );
        }
    }
    {
        let st = sim.stats.lock().unwrap();
        for (n, g, idx, h) in &st.snap_log {
            if *g == 1 {
                println!("SNAP n{} idx={} hash={:x}", n, idx, h);
            }
        }
    }
    // 用全局 apply_log 找 n1/n3 第一个分歧下标
    {
        let st = sim.stats.lock().unwrap();
        let mut by_node: std::collections::HashMap<u64, std::collections::HashMap<u64, u64>> = std::collections::HashMap::new();
        for (n, g, idx, h) in &st.apply_log {
            if *g == 1 {
                by_node.entry(*n).or_default().insert(*idx, *h);
            }
        }
        let n1 = by_node.get(&1).cloned().unwrap_or_default();
        let n3 = by_node.get(&3).cloned().unwrap_or_default();
        let mut idxs: Vec<u64> = n1.keys().filter(|i| n3.contains_key(i)).cloned().collect();
        idxs.sort();
        for i in idxs {
            if n1[&i] != n3[&i] {
                println!("APPLY-LOG DIVERGE at index {}: n1={:x} n3={:x}", i, n1[&i], n3[&i]);
                break;
            }
        }
    }
    // 找 g1 各节点 hashes 序列的第一个分歧点
    let series: Vec<Vec<(u64, u64)>> = sim
        .node_ids
        .iter()
        .filter_map(|id| sim.nodes.get(id))
        .filter(|sn| sn.up)
        .map(|sn| sn.node.groups[&1].hashes.iter().cloned().collect())
        .collect();
    if series.len() >= 2 {
        let base = &series[0];
        for (idx, h) in base {
            for (ni, s) in series.iter().enumerate().skip(1) {
                if let Some((_, h2)) = s.iter().find(|(i, _)| i == idx) {
                    if h2 != h {
                        println!("DIVERGE at index {}: n1={:x} n{}={:x}", idx, h, ni + 2, h2);
                    }
                }
            }
        }
        // 也直接对比最终 sm 内容
        let get = |ni: usize| -> &raftkv::node::KvSm {
            let id = sim.node_ids[ni];
            match &sim.nodes.get(&id).unwrap().node.groups[&1].sm {
                SmKind::Kv(kv) => kv,
                _ => panic!(),
            }
        };
        let a = get(0);
        for ni in 1..series.len() {
            let b = get(ni);
            for (k, v) in &a.data {
                if b.data.get(k) != Some(v) {
                    println!("key {:?}: n1={:?} n{}={:?}", String::from_utf8_lossy(k), String::from_utf8_lossy(v), ni+1, b.data.get(k).map(|x| String::from_utf8_lossy(x).into_owned()));
                }
            }
            for k in b.data.keys() {
                if !a.data.contains_key(k) {
                    println!("key {:?}: n1=MISSING n{}={:?}", String::from_utf8_lossy(k), ni+1, b.data.get(k).map(|x| String::from_utf8_lossy(x).into_owned()));
                }
            }
        }
    }
    // 对比 n2(leader)和 n3 的 g1 日志
    for id in [2u64, 3] {
        let h = &sim.nodes.get(&id).unwrap().node.groups[&1].raft;
        let terms: Vec<String> = h.log.iter().map(|e| format!("{}:t{}", e.index, e.term)).collect();
        println!("n{} snap=({},t{}) commit={} log=[{}]", id, h.snap_index, h.snap_term, h.commit, terms.join(" "));
    }
    {
        let st = sim.stats.lock().unwrap();
        println!("--- commits of g1 around idx of key-007 put ---");
        for (n, g, idx, term, q) in &st.commit_log {
            if *g == 1 && *idx >= 20 && *idx <= 70 {
                println!("commit n{} idx={} term={} quorum={:?}", n, idx, term, q);
            }
        }
        println!("--- truncations on g1 ---");
        for (n, g, from, removed) in &st.truncate_log {
            if *g == 1 {
                println!("truncate n{} from={} removed={:?}", n, from, removed);
            }
        }
    }
    {
        let st = sim.stats.lock().unwrap();
        for (n, g, idx, term, key) in &st.put_log {
            if *g == 1 && key == b"key-007" {
                println!("PUT-APPLIED n{} idx={} term={}", n, idx, term);
            }
        }
    }
    // 快照里有没有 key-007
    for id in sim.node_ids.clone() {
        let sn = sim.nodes.get(&id).unwrap();
        if let Some(snap) = &sn.node.groups[&1].raft.latest_snap {
            if let SmKind::Kv(kv) = raftkv::node::decode_sm_pub(1, &snap.data) {
                println!("n{} snap@{} key-007 = {:?}", id, snap.index,
                    kv.data.get(b"key-007".as_slice()).map(|v| String::from_utf8_lossy(v).into_owned()));
            }
        }
    }
    // 解码 n3 磁盘日志里 10..20 的命令
    {
        let disk = sim.factory.disk(3, 1).unwrap();
        let d = disk.lock().unwrap();
        for e in &d.entries {
            if e.index >= 43 && e.index <= 64 {
                let cmd: Result<raftkv::node::KvCmd, _> = serde_json::from_slice(&e.data);
                println!("n3 DISK idx={} term={} cmd={:?}", e.index, e.term, cmd.ok().map(|c| format!("{}", std::str::from_utf8(&format!("{:?}", c).into_bytes()).unwrap_or(""))));
            }
        }
    }
    // 解码 n3 g1 日志里 30..45 的命令
    {
        let h = &sim.nodes.get(&3).unwrap().node.groups[&1].raft;
        for e in &h.log {
            if e.index >= 30 && e.index <= 45 {
                let cmd: Result<raftkv::node::KvCmd, _> = serde_json::from_slice(&e.data);
                println!("n3 log idx={} term={} cmd={:?}", e.index, e.term, cmd.ok());
            }
        }
    }
    {
        let st = sim.stats.lock().unwrap();
        // 打印租约读和选举的交叉时间线
        let mut elections: Vec<(u64, u64, u64)> = st.elections.iter().filter(|e| e.group == 1).map(|e| (e.at_ms, e.term, e.winner)).collect();
        elections.sort();
        println!("--- lease reads on g1 (node, tick, term) vs elections ---");
        for (n, g, tick, term, _) in &st.lease_reads {
            if *g == 1 && *tick > 550 && *tick < 750 {
                println!("lease-read n{} tick={} term={}", n, tick, term);
            }
        }
        for (at, term, winner) in &elections {
            if *at > 500 && *at < 800 {
                println!("election term={} winner=n{} at={}", term, winner, at);
            }
        }
    }
    {
        let st = sim.stats.lock().unwrap();
        println!("put_log len = {}", st.put_log.len());
        println!("--- key-003 puts/dels applied ---");
        for (n, g, idx, term, key) in &st.put_log {
            if *g == 1 && (key == b"key-003" || key == b"DEL:key-003") {
                println!("apply n{} idx={} term={} {:?}", n, idx, term, String::from_utf8_lossy(key));
            }
        }
        println!("--- commits 550..700 ---");
        for (n, g, idx, term, q) in &st.commit_log {
            if *g == 1 {
                println!("commit n{} idx={} term={} q={:?}", n, idx, term, q);
            }
        }
    }
    // key-007 在各节点最终状态机里的值
    for id in sim.node_ids.clone() {
        let sn = sim.nodes.get(&id).unwrap();
        if !sn.up { continue; }
        if let SmKind::Kv(kv) = &sn.node.groups[&1].sm {
            println!("n{} key-007 = {:?}", id, kv.data.get(b"key-007".as_slice()).map(|v| String::from_utf8_lossy(v).into_owned()));
        }
    }
    let _ = Role::Follower;
}
