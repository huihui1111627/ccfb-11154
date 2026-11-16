//! raftkv —— 基于 Raft 的分布式分片键值存储
//!
//! 子命令:
//!   raftkv demo [--nodes 3] [--web 8000] [--base-port 9001] [--dir /tmp/raftkv-demo]
//!       起真实集群(TCP 互联)+ 状态页面 + 随机工作负载 + 一致性巡检
//!   raftkv sim [--scenarios 1000] [--seed 1] [--ticks 3000] [--threads 8]
//!       确定性模拟:随机丢包/延迟/分区/重启/磁盘损坏,
//!       每个场景结束用线性一致性检查器验证客户端历史,失败按种子复现
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use raftkv::checker::{check_linearizable, Op, OpKind};
use raftkv::client::Client;
use raftkv::node::Consistency;
use raftkv::realnode::{RealNode, TcpTransport};
use raftkv::sim::{run_soak, ScenarioCfg};
use raftkv::stats::new_stats;

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

fn arg_value(args: &[String], name: &str, default: &str) -> String {
    args.windows(2)
        .find(|w| w[0] == name)
        .map(|w| w[1].clone())
        .unwrap_or_else(|| default.to_string())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(|s| s.as_str()) {
        Some("sim") => cmd_sim(&args),
        Some("demo") => cmd_demo(&args),
        _ => {
            eprintln!("usage:");
            eprintln!("  raftkv demo [--nodes 3] [--web 8000] [--base-port 9001] [--dir /tmp/raftkv-demo]");
            eprintln!("  raftkv sim  [--scenarios 1000] [--seed 1] [--ticks 3000] [--threads 8] [--max-ops 120]");
            std::process::exit(1);
        }
    }
}

fn cmd_sim(args: &[String]) {
    let scenarios: usize = arg_value(args, "--scenarios", "1000").parse().unwrap();
    let seed: u64 = arg_value(args, "--seed", "1").parse().unwrap();
    let ticks: u64 = arg_value(args, "--ticks", "3000").parse().unwrap();
    let threads: usize = arg_value(args, "--threads", "8").parse().unwrap();
    let max_ops: usize = arg_value(args, "--max-ops", "120").parse().unwrap();
    let cfg = ScenarioCfg {
        ticks,
        max_ops,
        ..Default::default()
    };
    println!(
        "soak: {} scenarios, base seed {}, {} ticks/scenario, {} threads",
        scenarios, seed, ticks, threads
    );
    let t0 = Instant::now();
    let (total_ops, failures) = run_soak(seed, scenarios, &cfg, threads);
    println!(
        "done in {:?}: {} scenarios, {} client ops checked, {} failures",
        t0.elapsed(),
        scenarios,
        total_ops,
        failures.len()
    );
    for f in failures.iter().take(20) {
        println!("FAILED seed={} ops={} {:?}", f.seed, f.ops, f.failures);
        println!("  reproduce: cargo run --release -- sim --scenarios 1 --seed {}", f.seed);
    }
    if !failures.is_empty() {
        std::process::exit(1);
    }
}

fn cmd_demo(args: &[String]) {
    let n: usize = arg_value(args, "--nodes", "3").parse().unwrap();
    let web_port: u16 = arg_value(args, "--web", "8000").parse().unwrap();
    let base_port: u16 = arg_value(args, "--base-port", "9001").parse().unwrap();
    let dir = arg_value(args, "--dir", "/tmp/raftkv-demo");
    let _ = std::fs::remove_dir_all(&dir);

    let stats = new_stats();
    let ids: Vec<u64> = (1..=n as u64).collect();
    let addrs: HashMap<u64, String> = ids
        .iter()
        .map(|id| (*id, format!("127.0.0.1:{}", base_port + *id as u16 - 1)))
        .collect();

    let mut nodes = Vec::new();
    for &id in &ids {
        let node_dir = format!("{}/n{}", dir, id);
        let rn = RealNode::start(
            id,
            ids.clone(),
            Path::new(&node_dir),
            addrs.clone(),
            stats.clone(),
            50,    // snapshot_threshold: 50 条日志
            600,   // split_threshold: 600B 触发分裂,方便演示
        );
        nodes.push(rn);
    }
    let nodes = Arc::new(nodes);

    // 状态页面
    raftkv::web::start_web(web_port, nodes.as_ref().clone(), stats.clone());

    // 工作负载线程:随机读写,只把成功的操作记入历史供线性一致性检查
    for worker in 0..3u64 {
        let stats = stats.clone();
        let addrs = addrs.clone();
        let ids = ids.clone();
        thread::spawn(move || {
            let transport = TcpTransport { addrs };
            let mut client = Client::new(transport, ids);
            let mut seq = 0u64;
            let mut rng: u64 = 0x12345678 + worker * 0x9e3779b9;
            let mut next_rand = move || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng
            };
            loop {
                let key = format!("meter-{:03}", next_rand() % 20).into_bytes();
                let (kind, consistency) = match next_rand() % 10 {
                    0..=3 => (
                        OpKind::Put(format!("reading-{}-{}-{:016x}", worker, seq, next_rand()).into_bytes()),
                        Consistency::Linearizable,
                    ),
                    4..=7 => (OpKind::Get, Consistency::Linearizable),
                    8 => (OpKind::Get, Consistency::Lease),
                    _ => (OpKind::Del, Consistency::Linearizable),
                };
                let start = now_ms();
                // 先记为不确定操作(已发起,效果可能已提交),完成后再更新
                let idx = {
                    let mut s = stats.lock().unwrap();
                    s.client_history.push(Op {
                        key: key.clone(),
                        kind: kind.clone(),
                        resp: None,
                        start,
                        end: 0,
                        read_path: None,
                        indeterminate: true,
                    });
                    s.client_history.len() - 1
                };
                let resp = match &kind {
                    OpKind::Put(v) => client.put(&key, v).ok().map(|_| None),
                    OpKind::Get => client.get(&key, consistency).ok(),
                    OpKind::Del => client.del(&key).ok().map(|_| None),
                };
                let end = now_ms();
                {
                    let mut s = stats.lock().unwrap();
                    if let Some(resp) = resp {
                        // 客户端内部错误(传输失败)的操作保持不确定
                        s.client_history[idx].resp = resp;
                        s.client_history[idx].end = end;
                        s.client_history[idx].indeterminate = false;
                    }
                }
                seq += 1;
                thread::sleep(Duration::from_millis(20));
            }
        });
    }

    // 一致性巡检线程:副本哈希比对 + 存储 CRC + 线性一致性检查
    {
        let stats = stats.clone();
        let nodes = nodes.clone();
        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(2));
            let now = now_ms();
            {
                let mut s = stats.lock().unwrap();
                s.now_ms = now;
            }
            // 1. 同组各副本在相同 applied 下标处的状态机哈希必须一致
            let mut by_group: HashMap<u64, Vec<(u64, u64, u64, bool)>> = HashMap::new(); // group -> (node, applied, hash, storage_ok)
            for rn in nodes.iter() {
                for gv in rn.views.read().unwrap().iter() {
                    by_group
                        .entry(gv.group)
                        .or_default()
                        .push((rn.id, gv.applied, gv.sm_hash, gv.storage_ok));
                }
            }
            let mut s = stats.lock().unwrap();
            for (gid, replicas) in &by_group {
                for (_, _, _, ok) in replicas {
                    if !ok {
                        s.record_consistency(false, format!("group {} storage CRC failed", gid));
                    }
                }
                // 找 applied 相同的副本对,比哈希
                let mut seen: HashMap<u64, u64> = HashMap::new();
                for (node, applied, hash, _) in replicas {
                    if let Some(prev) = seen.insert(*applied, *hash) {
                        if prev != *hash {
                            s.record_consistency(
                                false,
                                format!("group {} node {} diverged at applied {}", gid, node, applied),
                            );
                        }
                    }
                }
            }
            // 2. 线性一致性检查器跑累积的客户端历史。
            // 历史不能按时间切片检查(切走更早的 Put 会误判),
            // 累积到上限后重置并记录,权威验证见模拟器 soak(完整历史)。
            if s.client_history.len() > 20_000 {
                s.client_history.clear();
                s.record_checker(true, "history window reset at 20000 ops (demo cap)".into());
            }
            if s.client_history.len() >= 50 {
                let history: Vec<Op> = s.client_history.clone();
                let n = history.len();
                match check_linearizable(&history) {
                    Ok(()) => s.record_checker(true, format!("{} ops linearizable", n)),
                    Err(key) => {
                        // 转储违规 key 的完整操作历史便于定位
                        let mut ops: Vec<String> = history
                            .iter()
                            .filter(|o| o.key == key)
                            .map(|o| {
                                format!(
                                    "[{}-{}] {:?} -> {:?}",
                                    o.start,
                                    o.end,
                                    o.kind,
                                    o.resp.as_ref().map(|v| String::from_utf8_lossy(v).into_owned())
                                )
                            })
                            .collect();
                        ops.sort();
                        s.record_checker(
                            false,
                            format!("violation on key {}: {}", String::from_utf8_lossy(&key), ops.join(" | ")),
                        );
                    }
                }
            }
        });
    }

    // 主线程:更新统计时钟
    loop {
        thread::sleep(Duration::from_millis(200));
        stats.lock().unwrap().now_ms = now_ms();
    }
}
