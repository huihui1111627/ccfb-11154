//! 实时驱动：用 tokio 任务 + 可插拔传输跑同一份 [`RaftNode`] 状态机。
//!
//! 传输有两种实现，节点逻辑只有一份：
//! - TCP（多进程部署）：4 字节 LE 长度前缀 + JSON 帧，每条客户端连接配一个写任务；
//! - 进程内路由（单机开发集群）：内存 mpsc，免端口。
//!
//! 节点主循环每 10ms 一个 tick（真实墙钟换算毫秒），把收到的消息批量喂给
//! `RaftNode::advance`。客户端回复统一走 [`ReplyRegistry`]：TCP 模式按
//! req_id 找回该连接的写通道，inproc 模式找回 oneshot。
//! leader 切换时由 [`RaftClient`] 按 NotLeader 提示等待重试，不向调用方报错。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::dashboard::{NodeStatus, TermEventView};
use crate::raft::state_machine::KVStateMachine;
use crate::raft::{Command, Event, Message, NodeId, RaftConfig, RaftNode, ReadMode, ReadRequest};
use crate::storage::RaftStore;

pub type GroupId = u64;
const TICK_MS: u64 = 10;

/// 节点间线路消息。
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Wire {
    pub group: GroupId,
    pub from: NodeId,
    pub msg: Message,
}

/// 客户端请求信封（与 Raft RPC 共用端口）。
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ClientRequest {
    pub req_id: u64,
    pub msg: Message,
}

/// 客户端回复（写请求拿到 Committed，读请求拿到 Read）。
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum ClientReply {
    Committed { index: u64 },
    Read { value: Option<Vec<u8>> },
    NotLeader { leader: Option<NodeId> },
    Faulted(String),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum Frame {
    Node(Wire),
    Client(ClientRequest),
    ClientReply(ClientReply),
}

// ---------------- 回复注册表 ----------------

/// 节点侧：req_id -> 回复通道。两种传输各填各的通道类型。
#[derive(Default)]
pub struct ReplyRegistry {
    tcp: Mutex<HashMap<u64, mpsc::UnboundedSender<Frame>>>,
    inproc: Mutex<HashMap<u64, oneshot::Sender<ClientReply>>>,
}

impl ReplyRegistry {
    async fn reply(&self, req_id: u64, reply: ClientReply) {
        if let Some(tx) = self.tcp.lock().await.remove(&req_id) {
            let _ = tx.send(Frame::ClientReply(reply));
        } else if let Some(tx) = self.inproc.lock().await.remove(&req_id) {
            let _ = tx.send(reply);
        }
    }
}

// ---------------- 传输抽象 ----------------

#[async_trait::async_trait] // 本地轻量替代：见文件末尾的手写 dyn 安全声明
trait Transport: Send + Sync {
    async fn send_frame(&self, to: NodeId, frame: Frame);
}

// ---- TCP ----

struct TcpTransport {
    pool: Mutex<HashMap<NodeId, Arc<Mutex<TcpStream>>>>,
    peers: HashMap<NodeId, SocketAddr>,
}

impl TcpTransport {
    fn new(peers: HashMap<NodeId, SocketAddr>) -> Arc<Self> {
        Arc::new(TcpTransport { pool: Mutex::new(HashMap::new()), peers })
    }

    async fn connect(&self, to: NodeId) -> Option<Arc<Mutex<TcpStream>>> {
        if let Some(s) = self.pool.lock().await.get(&to) {
            return Some(s.clone());
        }
        let addr = self.peers.get(&to)?;
        let stream = TcpStream::connect(addr).await.ok()?;
        stream.set_nodelay(true).ok();
        let s = Arc::new(Mutex::new(stream));
        self.pool.lock().await.insert(to, s.clone());
        Some(s)
    }
}

#[async_trait::async_trait]
impl Transport for TcpTransport {
    async fn send_frame(&self, to: NodeId, frame: Frame) {
        let Some(stream) = self.connect(to).await else { return };
        let bytes = match serde_json::to_vec(&frame) {
            Ok(b) => b,
            Err(_) => return,
        };
        let mut guard = stream.lock().await;
        if guard.write_all(&(bytes.len() as u32).to_le_bytes()).await.is_err()
            || guard.write_all(&bytes).await.is_err()
        {
            drop(guard);
            self.pool.lock().await.remove(&to); // 连接坏了，下次重连
        }
    }
}

// ---- 进程内 ----

#[derive(Clone)]
pub struct InprocRouter {
    routes: Arc<Mutex<HashMap<NodeId, mpsc::UnboundedSender<Frame>>>>,
}

impl InprocRouter {
    pub fn new() -> Self {
        InprocRouter { routes: Arc::new(Mutex::new(HashMap::new())) }
    }
    async fn register(&self, id: NodeId, tx: mpsc::UnboundedSender<Frame>) {
        self.routes.lock().await.insert(id, tx);
    }
}

struct InprocTransport {
    router: InprocRouter,
}

#[async_trait::async_trait]
impl Transport for InprocTransport {
    async fn send_frame(&self, to: NodeId, frame: Frame) {
        if let Some(tx) = self.router.routes.lock().await.get(&to) {
            let _ = tx.send(frame);
        }
    }
}

// ---------------- 节点运行参数 ----------------

pub struct NodeParams {
    pub id: NodeId,
    pub group: GroupId,
    pub data_dir: PathBuf,
    pub peers: HashMap<NodeId, SocketAddr>,
    pub cfg: RaftConfig,
    pub seed: u64,
    pub status_tx: Option<mpsc::UnboundedSender<(NodeId, NodeStatus)>>,
    pub replies: Arc<ReplyRegistry>,
}


/// 启动一个 TCP 节点任务。
pub fn spawn_tcp_node(params: NodeParams, bind: SocketAddr) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let started = Instant::now();
        let transport = TcpTransport::new(params.peers.clone());
        let listener = TcpListener::bind(bind).await.expect("bind raft port");
        let node = recover_or_fault(&params);
        let (inbox_tx, inbox_rx) = mpsc::unbounded_channel::<Frame>();
        let reg = params.replies.clone();
        tokio::spawn(async move { accept_loop(listener, inbox_tx, reg).await });
        node_loop(node, params, transport, inbox_rx, started).await;
    })
}

/// 启动一个进程内节点（单机集群，已有数据则恢复，空目录则带初始配置引导）。
pub async fn spawn_inproc_node(
    params: NodeParams,
    router: InprocRouter,
    initial: Option<crate::raft::ConfState>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    router.register(params.id, tx).await;
    let transport = Arc::new(InprocTransport { router });
    let started = Instant::now();
    let node = match initial {
        Some(conf) => bootstrap_or_fault(&params, conf),
        None => recover_or_fault(&params),
    };
    tokio::spawn(async move {
        node_loop(node, params, transport, rx, started).await;
    });
}

/// 启动先校验磁盘：损坏即 Faulted，绝不带病参与投票（需求 7）。
fn recover_or_fault(params: &NodeParams) -> RaftNode {
    let NodeParams { id, data_dir, cfg, seed, .. } = params;
    match RaftStore::open(data_dir, true) {
        Ok(store) => match store.verify_integrity() {
            Ok(()) => {
                let sm = Box::new(KVStateMachine::new());
                RaftNode::recover(*id, store, sm, cfg.clone(), *seed, 0)
                    .unwrap_or_else(|e| RaftNode::faulted(*id, e.to_string(), cfg.clone()))
            }
            Err(e) => RaftNode::faulted(*id, e.to_string(), cfg.clone()),
        },
        Err(e) => RaftNode::faulted(*id, e.to_string(), cfg.clone()),
    }
}

fn bootstrap_or_fault(params: &NodeParams, initial: crate::raft::ConfState) -> RaftNode {
    let NodeParams { id, data_dir, cfg, seed, .. } = params;
    let store = RaftStore::open(data_dir, true).expect("open store");
    let has_state = store.load_hard_state().ok().flatten().is_some()
        || store.load_snapshot().ok().flatten().is_some();
    if has_state {
        let sm = Box::new(KVStateMachine::new());
        RaftNode::recover(*id, store, sm, cfg.clone(), *seed, 0)
            .unwrap_or_else(|e| RaftNode::faulted(*id, e.to_string(), cfg.clone()))
    } else {
        let sm = Box::new(KVStateMachine::new());
        RaftNode::bootstrap(*id, initial, store, sm, cfg.clone(), *seed, 0)
            .unwrap_or_else(|e| RaftNode::faulted(*id, e.to_string(), cfg.clone()))
    }
}

async fn node_loop<T: Transport>(
    mut node: RaftNode,
    params: NodeParams,
    transport: Arc<T>,
    mut inbox_rx: mpsc::UnboundedReceiver<Frame>,
    started: Instant,
) {
    let NodeParams { id, group, status_tx, replies, .. } = params;
    let mut waiting: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(TICK_MS));
    let mut last_status = 0u64;

    loop {
        ticker.tick().await;
        let now_ms = started.elapsed().as_millis() as u64;

        let mut messages = Vec::new();
        while let Ok(frame) = inbox_rx.try_recv() {
            match frame {
                Frame::Node(wire) => messages.push(wire.msg),
                Frame::Client(req) => {
                    let cid = match &req.msg {
                        Message::ClientProposal { client_req, .. } => *client_req,
                        Message::ClientRead(r) => r.req_id,
                        _ => continue,
                    };
                    waiting.insert(cid);
                    messages.push(req.msg);
                }
                Frame::ClientReply(_) => {} // 节点不消费回复帧
            }
        }

        let events = node.advance(now_ms, messages);
        for ev in events {
            match ev {
                Event::Send { to, msg } => {
                    transport
                        .send_frame(to, Frame::Node(Wire { group, from: id, msg }))
                        .await;
                }
                Event::Committed { client_req, index } => {
                    waiting.remove(&client_req);
                    replies.reply(client_req, ClientReply::Committed { index }).await;
                }
                Event::ReadResult(r) => {
                    waiting.remove(&r.req_id);
                    replies.reply(r.req_id, ClientReply::Read { value: r.value }).await;
                }
                Event::NotLeader { leader, client_req } => {
                    if let Some(cid) = client_req {
                        waiting.remove(&cid);
                        replies.reply(cid, ClientReply::NotLeader { leader }).await;
                    }
                }
                Event::Faulted { reason } => {
                    for cid in waiting.drain() {
                        replies.reply(cid, ClientReply::Faulted(reason.clone())).await;
                    }
                }
                Event::TermChanged(_)
                | Event::SnapshotCompacted { .. }
                | Event::Applied { .. } => {}
            }
        }

        if now_ms.saturating_sub(last_status) >= 200 {
            last_status = now_ms;
            if let Some(tx) = &status_tx {
                let _ = tx.send((id, build_status(&node, now_ms)));
            }
        }
    }
}

async fn accept_loop(
    listener: TcpListener,
    tx: mpsc::UnboundedSender<Frame>,
    reg: Arc<ReplyRegistry>,
) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(x) => x,
            Err(_) => continue,
        };
        let tx = tx.clone();
        let reg = reg.clone();
        tokio::spawn(async move { connection_loop(stream, tx, reg).await });
    }
}

/// 每条连接：读任务解析帧；客户端帧登记写通道后入 inbox，写任务负责回帧。
async fn connection_loop(
    stream: TcpStream,
    inbox: mpsc::UnboundedSender<Frame>,
    reg: Arc<ReplyRegistry>,
) {
    stream.set_nodelay(true).ok();
    let (reader, writer) = stream.into_split();
    let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Frame>();
    let write_task = tokio::spawn(async move {
        let mut writer = writer;
        while let Some(frame) = write_rx.recv().await {
            let bytes = match serde_json::to_vec(&frame) {
                Ok(b) => b,
                Err(_) => return,
            };
            if writer.write_all(&(bytes.len() as u32).to_le_bytes()).await.is_err()
                || writer.write_all(&bytes).await.is_err()
            {
                return;
            }
        }
    });

    let mut reader = reader;
    loop {
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > 256 * 1024 * 1024 {
            break;
        }
        let mut buf = vec![0u8; len];
        if reader.read_exact(&mut buf).await.is_err() {
            break;
        }
        let frame: Frame = match serde_json::from_slice(&buf) {
            Ok(f) => f,
            Err(_) => break,
        };
        match frame {
            Frame::Client(req) => {
                let cid = req.req_id;
                reg.tcp.lock().await.insert(cid, write_tx.clone());
                if inbox.send(Frame::Client(req)).is_err() {
                    break;
                }
            }
            Frame::Node(wire) => {
                if inbox.send(Frame::Node(wire)).is_err() {
                    break;
                }
            }
            Frame::ClientReply(_) => {}
        }
    }
    drop(write_tx);
    let _ = write_task.await;
}

fn build_status(node: &RaftNode, now_ms: u64) -> NodeStatus {
    NodeStatus {
        node_id: node.id,
        role: format!("{:?}", node.role).to_lowercase(),
        term: node.term,
        leader: node.leader,
        commit: node.commit,
        applied: node.applied,
        last_log_index: node.last_log_index(),
        log_len: node.log_len() as u64,
        snapshot_index: node.snapshot_index(),
        in_joint: node.in_joint,
        voters: node.conf.voters.clone(),
        old_voters: node.conf_old.as_ref().map(|c| c.voters.clone()),
        fault: node.fault_reason().map(|s| s.to_string()),
        lease_valid: node.lease_valid(now_ms),
        lease_expire_ms: node.lease_expire_ms(),
        lease_reads: node.lease_reads,
        readindex_reads: node.readindex_reads,
        pending_reads: node.pending_read_count(),
        next_index: node.next_index().iter().map(|(k, v)| (*k, *v)).collect(),
        match_index: node.match_index().iter().map(|(k, v)| (*k, *v)).collect(),
        term_events: node
            .term_events
            .iter()
            .map(|te| TermEventView {
                term: te.term,
                role: te.role.clone(),
                election_elapsed_ms: te.election_elapsed_ms,
                leader: te.leader,
                at_ms: te.at_ms,
            })
            .collect(),
        uptime_ms: now_ms,
    }
}

// ---------------- 客户端（需求 6：主切换期等待不报错） ----------------

pub struct RaftClient {
    addrs: HashMap<NodeId, SocketAddr>,
    inproc: Option<(InprocRouter, Arc<ReplyRegistry>)>,
    leader: Option<NodeId>,
    req_seq: u64,
    retry_wait: Duration,
    timeout: Duration,
}

impl RaftClient {
    pub fn tcp(addrs: HashMap<NodeId, SocketAddr>) -> Self {
        RaftClient {
            addrs,
            inproc: None,
            leader: None,
            req_seq: 1,
            retry_wait: Duration::from_millis(50),
            timeout: Duration::from_secs(30),
        }
    }

    pub fn inproc(
        router: InprocRouter,
        replies: Arc<ReplyRegistry>,
        nodes: Vec<NodeId>,
    ) -> Self {
        let addrs = nodes
            .into_iter()
            .map(|n| (n, SocketAddr::from(([127, 0, 0, 1], 0))))
            .collect();
        RaftClient {
            addrs,
            inproc: Some((router, replies)),
            leader: None,
            req_seq: 1,
            retry_wait: Duration::from_millis(20),
            timeout: Duration::from_secs(30),
        }
    }

    fn next_req(&mut self) -> u64 {
        let r = self.req_seq;
        self.req_seq += 1;
        r
    }

    pub async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<u64, String> {
        self.propose(Command::Put { key, value }).await
    }

    pub async fn delete(&mut self, key: Vec<u8>) -> Result<u64, String> {
        self.propose(Command::Delete { key }).await
    }

    /// 写：主切换期间循环等待新主选出，只有超过总预算才报错。
    async fn propose(&mut self, cmd: Command) -> Result<u64, String> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        loop {
            let req_id = self.next_req();
            let req = ClientRequest {
                req_id,
                msg: Message::ClientProposal { cmd: cmd.clone(), client_req: req_id },
            };
            match self.roundtrip(req).await {
                Ok(ClientReply::Committed { index }) => return Ok(index),
                Ok(ClientReply::NotLeader { leader }) => {
                    self.leader = leader.or(self.leader);
                    if tokio::time::Instant::now() >= deadline {
                        return Err("proposal timed out waiting for a leader".into());
                    }
                    tokio::time::sleep(self.retry_wait).await;
                }
                Ok(ClientReply::Faulted(r)) => return Err(format!("node faulted: {r}")),
                Ok(other) => return Err(format!("unexpected reply: {other:?}")),
                Err(_) => {
                    self.leader = None;
                    if tokio::time::Instant::now() >= deadline {
                        return Err("proposal timed out (connections failing)".into());
                    }
                    tokio::time::sleep(self.retry_wait).await;
                }
            }
        }
    }

    /// 线性一致读，ReadIndex 或 Lease 由调用方选。
    pub async fn get(
        &mut self,
        key: Vec<u8>,
        mode: ReadMode,
    ) -> Result<Option<Vec<u8>>, String> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        loop {
            let req_id = self.next_req();
            let req = ClientRequest {
                req_id,
                msg: Message::ClientRead(ReadRequest { key: key.clone(), mode, req_id }),
            };
            match self.roundtrip(req).await {
                Ok(ClientReply::Read { value }) => return Ok(value),
                Ok(ClientReply::NotLeader { leader }) => {
                    self.leader = leader.or(self.leader);
                    if tokio::time::Instant::now() >= deadline {
                        return Err("read timed out waiting for a leader".into());
                    }
                    tokio::time::sleep(self.retry_wait).await;
                }
                Ok(ClientReply::Faulted(r)) => return Err(format!("node faulted: {r}")),
                Ok(other) => return Err(format!("unexpected reply: {other:?}")),
                Err(_) => {
                    self.leader = None;
                    if tokio::time::Instant::now() >= deadline {
                        return Err("read timed out".into());
                    }
                    tokio::time::sleep(self.retry_wait).await;
                }
            }
        }
    }

    async fn roundtrip(&mut self, req: ClientRequest) -> Result<ClientReply, String> {
        let target = self.pick_target();
        if let Some((router, replies)) = &self.inproc {
            let (tx, rx) = oneshot::channel();
            replies.inproc.lock().await.insert(req.req_id, tx);
            let routes = router.routes.lock().await;
            let inbox = routes
                .get(&target)
                .ok_or_else(|| format!("node {target} inbox missing"))?;
            inbox
                .send(Frame::Client(req))
                .map_err(|_| "node inbox closed".to_string())?;
            drop(routes);
            let reply = rx.await.map_err(|_| "reply channel closed".to_string())?;
            if let ClientReply::NotLeader { leader: Some(l) } = &reply {
                self.leader = Some(*l);
            }
            return Ok(reply);
        }

        let addr = self
            .addrs
            .get(&target)
            .copied()
            .ok_or_else(|| format!("no addr for node {target}"))?;
        let mut stream = TcpStream::connect(addr).await.map_err(|e| e.to_string())?;
        let bytes = serde_json::to_vec(&req).map_err(|e| e.to_string())?;
        stream.write_all(&(bytes.len() as u32).to_le_bytes()).await.map_err(|e| e.to_string())?;
        stream.write_all(&bytes).await.map_err(|e| e.to_string())?;
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.map_err(|e| e.to_string())?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await.map_err(|e| e.to_string())?;
        // 服务端回 Frame，先解外层再取 ClientReply
        let frame: Frame = serde_json::from_slice(&buf).map_err(|e| e.to_string())?;
        match frame {
            Frame::ClientReply(reply) => {
                if let ClientReply::NotLeader { leader: Some(l) } = &reply {
                    self.leader = Some(*l);
                }
                Ok(reply)
            }
            _ => Err("expected ClientReply frame".into()),
        }
    }

    fn pick_target(&self) -> NodeId {
        if let Some(l) = self.leader {
            return l;
        }
        let ids: Vec<NodeId> = self.addrs.keys().copied().collect();
        ids[(self.req_seq as usize) % ids.len().max(1)]
    }
}
