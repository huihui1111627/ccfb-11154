//! 真实集群节点:TCP 互联(JSON 行协议),10ms 一个 tick。
//! 同一进程内跑多个节点,共享统计句柄,供状态页面读取。
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use crate::node::{ClientReq, ClientResp, GroupView, Node};
use crate::raft::{Envelope, NodeId};
use crate::stats::StatsHandle;
use crate::storage::{FileFactory, StorageFactory};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NetMsg {
    Peer(Envelope),
    ClientReq(u64, ClientReq),
    ClientResp(u64, ClientResp),
}

pub struct RealNode {
    pub id: NodeId,
    pub node: Mutex<Node>,
    pub views: RwLock<Vec<GroupView>>,
    pub directory: RwLock<Vec<crate::node::ShardMeta>>,
    inbox: mpsc::Sender<NetMsg>,
    peers: Mutex<HashMap<NodeId, TcpStream>>,
    pub peer_addrs: HashMap<NodeId, String>,
    client_responders: Mutex<HashMap<u64, mpsc::Sender<ClientResp>>>,
    next_conn: AtomicU64,
}

fn send_line(stream: &mut TcpStream, msg: &NetMsg) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(msg).unwrap();
    line.push(b'\n');
    stream.write_all(&line)?;
    stream.flush()
}

impl RealNode {
    pub fn start(
        id: NodeId,
        voters: Vec<NodeId>,
        data_dir: &Path,
        peer_addrs: HashMap<NodeId, String>,
        stats: StatsHandle,
        snapshot_threshold: u64,
        split_threshold: usize,
    ) -> Arc<RealNode> {
        let factory = FileFactory::new(data_dir);
        let known = factory.groups_for(id);
        let node = if known.is_empty() {
            Node::bootstrap(
                id,
                voters,
                Box::new(factory),
                id.wrapping_mul(0x9E3779B9),
                Some(stats),
                snapshot_threshold,
                split_threshold,
            )
        } else {
            Node::recover(
                id,
                Box::new(factory),
                id.wrapping_mul(0x9E3779B9),
                Some(stats),
                snapshot_threshold,
                split_threshold,
            )
        };
        let (tx, rx) = mpsc::channel::<NetMsg>();
        let rn = Arc::new(RealNode {
            id,
            node: Mutex::new(node),
            views: RwLock::new(vec![]),
            directory: RwLock::new(vec![]),
            inbox: tx,
            peers: Mutex::new(HashMap::new()),
            peer_addrs: peer_addrs.clone(),
            client_responders: Mutex::new(HashMap::new()),
            next_conn: AtomicU64::new(1),
        });

        // 监听线程
        let addr = peer_addrs[&id].clone();
        let rn2 = rn.clone();
        thread::spawn(move || {
            let listener = TcpListener::bind(&addr).expect("bind");
            for conn in listener.incoming() {
                if let Ok(stream) = conn {
                    let rn3 = rn2.clone();
                    thread::spawn(move || rn3.serve_conn(stream));
                }
            }
        });

        // 主循环:tick + 处理消息 + 刷外发
        let rn4 = rn.clone();
        thread::spawn(move || loop {
            let loop_start = Instant::now();
            // 收
            while let Ok(msg) = rx.try_recv() {
                match msg {
                    NetMsg::Peer(env) => rn4.node.lock().unwrap().handle_envelope(env),
                    NetMsg::ClientReq(conn, req) => rn4.node.lock().unwrap().handle_client_req(conn, req),
                    _ => {}
                }
            }
            // 推
            {
                let mut node = rn4.node.lock().unwrap();
                node.tick();
                node.advance();
                let envelopes: Vec<Envelope> = node.outbox.drain(..).collect();
                let client_resps: Vec<(u64, ClientResp)> = node.client_outbox.drain(..).collect();
                *rn4.views.write().unwrap() = node.views();
                *rn4.directory.write().unwrap() = node.directory();
                drop(node);
                for env in envelopes {
                    rn4.send_peer(env);
                }
                for (conn, resp) in client_resps {
                    let tx = rn4.client_responders.lock().unwrap().remove(&conn);
                    if let Some(tx) = tx {
                        let _ = tx.send(resp);
                    }
                }
            }
            let elapsed = loop_start.elapsed();
            if elapsed < Duration::from_millis(10) {
                thread::sleep(Duration::from_millis(10) - elapsed);
            }
        });

        rn
    }

    fn serve_conn(self: &Arc<RealNode>, stream: TcpStream) {
        let mut reader = BufReader::new(match stream.try_clone() {
            Ok(s) => s,
            Err(_) => return,
        });
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => return,
                Ok(_) => {
                    let msg: NetMsg = match serde_json::from_str(line.trim()) {
                        Ok(m) => m,
                        Err(_) => continue,
                    };
                    match msg {
                        NetMsg::Peer(_) | NetMsg::ClientResp(_, _) => {
                            let _ = self.inbox.send(msg);
                        }
                        NetMsg::ClientReq(_, req) => {
                            // 同步请求/响应:注册一次性 responder,等主循环处理后回写
                            let conn_id = self.next_conn.fetch_add(1, Ordering::Relaxed);
                            let (tx, rx) = mpsc::channel::<ClientResp>();
                            self.client_responders.lock().unwrap().insert(conn_id, tx);
                            let _ = self.inbox.send(NetMsg::ClientReq(conn_id, req));
                            let mut w = match stream.try_clone() {
                                Ok(s) => s,
                                Err(_) => return,
                            };
                            match rx.recv_timeout(Duration::from_secs(30)) {
                                Ok(resp) => {
                                    let _ = send_line(&mut w, &NetMsg::ClientResp(conn_id, resp));
                                }
                                Err(_) => {
                                    self.client_responders.lock().unwrap().remove(&conn_id);
                                }
                            }
                            let _ = stream.shutdown(Shutdown::Both);
                            return;
                        }
                    }
                }
            }
        }
    }

    fn send_peer(self: &Arc<RealNode>, env: Envelope) {
        let to = env.to;
        let msg = NetMsg::Peer(env);
        // 已有连接直接发,失败则重连一次
        {
            let mut peers = self.peers.lock().unwrap();
            if let Some(s) = peers.get_mut(&to) {
                if send_line(s, &msg).is_ok() {
                    return;
                }
                peers.remove(&to);
            }
        }
        if let Some(addr) = self.peer_addrs.get(&to) {
            if let Ok(mut s) = TcpStream::connect(addr) {
                let _ = s.set_nodelay(true);
                if send_line(&mut s, &msg).is_ok() {
                    self.peers.lock().unwrap().insert(to, s);
                }
            }
        }
    }
}

/// TCP 客户端传输
pub struct TcpTransport {
    pub addrs: HashMap<NodeId, String>,
}

impl crate::client::ClientTransport for TcpTransport {
    fn request(&mut self, node: NodeId, req: &ClientReq) -> Result<ClientResp, String> {
        let addr = self.addrs.get(&node).ok_or("unknown node")?;
        let mut stream = TcpStream::connect(addr).map_err(|e| e.to_string())?;
        stream.set_nodelay(true).ok();
        send_line(&mut stream, &NetMsg::ClientReq(0, req.clone())).map_err(|e| e.to_string())?;
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).map_err(|e| e.to_string())?;
        match serde_json::from_str::<NetMsg>(line.trim()) {
            Ok(NetMsg::ClientResp(_, resp)) => Ok(resp),
            _ => Err("bad response".into()),
        }
    }
}
