//! 集群状态页面:节点状态、日志复制、分片、快照、一致性检查、选主历史。
//! 单进程内直接从各节点的视图和共享统计读,无需额外 RPC。
use serde::Serialize;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

use crate::node::ShardMeta;
use crate::realnode::RealNode;
use crate::stats::{ConsistencyReport, ElectionRecord, SnapshotRecord, SplitRecord, StatsHandle};

#[derive(Serialize)]
struct NodeView {
    id: u64,
    groups: Vec<crate::node::GroupView>,
}

#[derive(Serialize)]
struct Status {
    now_ms: u64,
    nodes: Vec<NodeView>,
    shards: Vec<ShardMeta>,
    elections: Vec<ElectionRecordView>,
    snapshots: Vec<SnapshotRecord>,
    splits: Vec<SplitRecordView>,
    consistency: Vec<ConsistencyReport>,
    checker: Vec<ConsistencyReport>,
    crc_failures: Vec<String>,
}

#[derive(Serialize)]
struct ElectionRecordView {
    group: u64,
    term: u64,
    winner: u64,
    duration_ms: u64,
    at_ms: u64,
}

#[derive(Serialize)]
struct SplitRecordView {
    old_group: u64,
    new_group: u64,
    split_key: String,
    at_ms: u64,
}

fn key_str(k: &[u8]) -> String {
    if k.is_empty() {
        return "∅".into();
    }
    String::from_utf8_lossy(k).into_owned()
}

pub fn start_web(port: u16, nodes: Vec<Arc<RealNode>>, stats: StatsHandle) {
    thread::spawn(move || {
        let listener = TcpListener::bind(("0.0.0.0", port)).expect("bind web");
        println!("dashboard: http://127.0.0.1:{}", port);
        for conn in listener.incoming() {
            if let Ok(stream) = conn {
                let nodes = nodes.clone();
                let stats = stats.clone();
                thread::spawn(move || handle(stream, nodes, stats));
            }
        }
    });
}

fn handle(stream: std::net::TcpStream, nodes: Vec<Arc<RealNode>>, stats: StatsHandle) {
    let mut reader = BufReader::new(match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    });
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        return;
    }
    let path = line.split_whitespace().nth(1).unwrap_or("/").to_string();
    // 读完请求头
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                if line.trim().is_empty() {
                    break;
                }
            }
        }
    }
    let mut w = stream;
    if path == "/api/status" {
        let s = stats.lock().unwrap();
        let status = Status {
            now_ms: s.now_ms,
            nodes: nodes
                .iter()
                .map(|n| NodeView {
                    id: n.id,
                    groups: n.views.read().unwrap().clone(),
                })
                .collect(),
            shards: nodes.first().map(|n| n.directory.read().unwrap().clone()).unwrap_or_default(),
            elections: s
                .elections
                .iter()
                .rev()
                .take(100)
                .map(|e: &ElectionRecord| ElectionRecordView {
                    group: e.group,
                    term: e.term,
                    winner: e.winner,
                    duration_ms: e.duration_ticks * 10,
                    at_ms: e.at_ms,
                })
                .collect(),
            snapshots: s.snapshots.iter().rev().take(50).cloned().collect(),
            splits: s
                .splits
                .iter()
                .rev()
                .take(50)
                .map(|sp: &SplitRecord| SplitRecordView {
                    old_group: sp.old_group,
                    new_group: sp.new_group,
                    split_key: key_str(&sp.split_key),
                    at_ms: sp.at_ms,
                })
                .collect(),
            consistency: s.consistency.iter().rev().take(50).cloned().collect(),
            checker: s.checker_results.iter().rev().take(20).cloned().collect(),
            crc_failures: s.crc_failures.clone(),
        };
        let body = serde_json::to_vec(&status).unwrap();
        let _ = write!(
            w,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = w.write_all(&body);
    } else {
        let body = INDEX_HTML.as_bytes();
        let _ = write!(
            w,
            "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = w.write_all(body);
    }
}

// GroupView / ShardMeta / 记录类型需要 Serialize
const INDEX_HTML: &str = r##"<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>raftkv 集群状态</title>
<style>
body { font-family: -apple-system, "PingFang SC", monospace; margin: 24px; background: #0f1420; color: #dde3f0; }
h1 { font-size: 20px; } h2 { font-size: 15px; color: #7fb4ff; margin-top: 28px; border-bottom: 1px solid #26314a; padding-bottom: 4px; }
table { border-collapse: collapse; font-size: 12px; margin-top: 8px; }
th, td { border: 1px solid #26314a; padding: 3px 8px; text-align: left; }
th { background: #1a2236; }
.Leader { color: #7dffa8; font-weight: bold; } .Candidate { color: #ffd97f; } .Refused { color: #ff7f7f; font-weight: bold; }
.ok { color: #7dffa8; } .bad { color: #ff7f7f; }
.meta { color: #8b93a8; font-size: 12px; }
</style>
</head>
<body>
<h1>raftkv 集群状态 <span class="meta" id="now"></span></h1>
<div id="app">loading…</div>
<script>
function esc(s){ return String(s).replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
function keyOf(r){ return (r[0]||'∅') + ' ~ ' + (r[1]||'+∞'); }
async function refresh() {
  const s = await (await fetch('/api/status')).json();
  document.getElementById('now').textContent = 't=' + s.now_ms;
  let h = '';

  // 节点状态
  h += '<h2>节点状态</h2><table><tr><th>节点</th><th>组</th><th>角色</th><th>任期</th><th>主</th><th>commit</th><th>applied</th><th>日志长</th><th>快照点</th><th>存储CRC</th></tr>';
  for (const n of s.nodes) for (const g of n.groups) {
    h += `<tr><td>${n.id}</td><td>${g.group}</td><td class="${g.role}">${g.role}</td><td>${g.term}</td><td>${g.lead??'-'}</td><td>${g.commit}</td><td>${g.applied}</td><td>${g.log_len}</td><td>${g.snap_index}</td><td class="${g.storage_ok?'ok':'bad'}">${g.storage_ok?'✓':'✗'}</td></tr>`;
  }
  h += '</table>';

  // 日志复制
  h += '<h2>日志复制(领导者视角 match/next)</h2><table><tr><th>节点</th><th>组</th><th>角色</th><th>对等点 → matched / next</th></tr>';
  for (const n of s.nodes) for (const g of n.groups) {
    if (g.role !== 'Leader') continue;
    const peers = g.progress.map(p => `n${p[0]}: ${p[1]}/${p[2]}`).join(' &nbsp; ');
    h += `<tr><td>${n.id}</td><td>${g.group}</td><td class="Leader">Leader</td><td>${peers}</td></tr>`;
  }
  h += '</table>';

  // 分片
  h += '<h2>分片</h2><table><tr><th>分片(组)</th><th>键范围</th><th>成员</th><th>大小(B)</th><th>状态机哈希</th></tr>';
  for (const sh of s.shards) {
    let size = '', hash = '';
    for (const n of s.nodes) for (const g of n.groups) if (g.group === sh.id) { size = g.sm_size; hash = '0x'+g.sm_hash.toString(16); break; }
    h += `<tr><td>${sh.id}</td><td>${esc(sh.start||'∅')} ~ ${esc(sh.end||'+∞')}</td><td>[${sh.members.join(', ')}]</td><td>${size}</td><td>${hash}</td></tr>`;
  }
  h += '</table>';
  if (s.splits.length) {
    h += '<div class="meta">分裂历史: ' + s.splits.map(x => `g${x.old_group}→g${x.new_group} @${esc(x.split_key)}`).join(' | ') + '</div>';
  }

  // 快照
  h += '<h2>快照</h2><table><tr><th>组</th><th>节点</th><th>快照下标</th><th>大小(B)</th><th>时间</th></tr>';
  for (const sn of s.snapshots) h += `<tr><td>${sn.group}</td><td>${sn.node}</td><td>${sn.index}</td><td>${sn.size}</td><td>${sn.at_ms}</td></tr>`;
  h += '</table>';

  // 一致性检查
  h += '<h2>一致性检查</h2>';
  h += '<div class="meta">线性一致性检查器(客户端历史):</div><table><tr><th>时间</th><th>结果</th><th>详情</th></tr>';
  for (const c of s.checker) h += `<tr><td>${c.at_ms}</td><td class="${c.ok?'ok':'bad'}">${c.ok?'通过':'失败'}</td><td>${esc(c.detail)}</td></tr>`;
  h += '</table><div class="meta">副本状态机哈希比对 / 存储 CRC:</div><table><tr><th>时间</th><th>结果</th><th>详情</th></tr>';
  for (const c of s.consistency) h += `<tr><td>${c.at_ms}</td><td class="${c.ok?'ok':'bad'}">${c.ok?'通过':'失败'}</td><td>${esc(c.detail)}</td></tr>`;
  h += '</table>';
  if (s.crc_failures.length) h += '<div class="bad">CRC 失败: ' + s.crc_failures.map(esc).join(' | ') + '</div>';

  // 选主历史
  h += '<h2>选主历史(任期变化 / 选举耗时)</h2><table><tr><th>组</th><th>任期</th><th>当选者</th><th>耗时(ms)</th><th>时间</th></tr>';
  for (const e of s.elections) h += `<tr><td>${e.group}</td><td>${e.term}</td><td>n${e.winner}</td><td>${e.duration_ms}</td><td>${e.at_ms}</td></tr>`;
  h += '</table>';

  document.getElementById('app').innerHTML = h;
}
refresh(); setInterval(refresh, 1000);
</script>
</body>
</html>"##;
