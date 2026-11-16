//! 客户端:自己定位分片和当前主节点;主切换期间不报错,
//! 退避后重新问主、等新主选出来再发(默认无限重试)。
use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

use crate::node::{ClientReq, ClientResp, Consistency, KvError, KvOp, ShardMeta};
use crate::raft::{GroupId, NodeId};

pub trait ClientTransport {
    fn request(&mut self, node: NodeId, req: &ClientReq) -> Result<ClientResp, String>;
}

pub struct Client<T: ClientTransport> {
    pub transport: T,
    pub nodes: Vec<NodeId>,
    pub directory: Vec<ShardMeta>,
    pub leaders: HashMap<GroupId, NodeId>,
    next_req: u64,
    rr: usize,
    /// 单次操作最长等待;None = 永远等新主(需求:主切换期间不报错)
    pub max_wait: Option<Duration>,
    /// 去重身份:客户端 ID(创建时随机)+ 每个逻辑操作一个序号,重试共用
    pub client_id: u64,
    op_seq: u64,
}

impl<T: ClientTransport> Client<T> {
    pub fn new(transport: T, nodes: Vec<NodeId>) -> Client<T> {
        let client_id = rand::random::<u64>();
        Client {
            transport,
            nodes,
            directory: vec![],
            leaders: HashMap::new(),
            next_req: 1,
            rr: 0,
            max_wait: None,
            client_id,
            op_seq: 0,
        }
    }

    pub fn get(&mut self, key: &[u8], consistency: Consistency) -> Result<Option<Vec<u8>>, String> {
        self.kv(key, KvOp::Get, consistency)
    }
    pub fn put(&mut self, key: &[u8], val: &[u8]) -> Result<(), String> {
        self.kv(key, KvOp::Put(val.to_vec()), Consistency::Linearizable)?;
        Ok(())
    }
    pub fn del(&mut self, key: &[u8]) -> Result<(), String> {
        self.kv(key, KvOp::Del, Consistency::Linearizable)?;
        Ok(())
    }

    fn next_req_id(&mut self) -> u64 {
        let id = self.next_req;
        self.next_req += 1;
        id
    }

    fn pick_node(&mut self) -> NodeId {
        let n = self.nodes[self.rr % self.nodes.len()];
        self.rr += 1;
        n
    }

    fn refresh_directory(&mut self) -> Result<(), String> {
        for _ in 0..self.nodes.len() {
            let node = self.pick_node();
            let req_id = self.next_req_id();
            if let Ok(ClientResp::Directory { shards, .. }) = self.transport.request(node, &ClientReq::Directory { req_id }) {
                if !shards.is_empty() {
                    self.directory = shards;
                    return Ok(());
                }
            }
        }
        Err("no node has directory yet".into())
    }

    fn ask_leader(&mut self, group: GroupId) {
        for _ in 0..self.nodes.len() {
            let node = self.pick_node();
            let req_id = self.next_req_id();
            if let Ok(ClientResp::Leader { leader, .. }) = self.transport.request(node, &ClientReq::AskLeader { req_id, group }) {
                if let Some(l) = leader {
                    self.leaders.insert(group, l);
                    return;
                }
            }
        }
    }

    fn lookup(&self, key: &[u8]) -> Option<&ShardMeta> {
        self.directory
            .iter()
            .find(|s| key >= s.start.as_slice() && (s.end.is_empty() || key < s.end.as_slice()))
    }

    fn kv(&mut self, key: &[u8], op: KvOp, consistency: Consistency) -> Result<Option<Vec<u8>>, String> {
        let started = std::time::Instant::now();
        let mut backoff = Duration::from_millis(20);
        // 同一逻辑操作的所有重试共用一个 op_seq,服务端据此去重
        self.op_seq += 1;
        let op_seq = self.op_seq;
        let client_id = self.client_id;
        loop {
            if let Some(max) = self.max_wait {
                if started.elapsed() > max {
                    return Err("client deadline exceeded".into());
                }
            }
            // 1. 定位分片
            let gid = match self.lookup(key) {
                Some(s) => s.id,
                None => {
                    let _ = self.refresh_directory();
                    sleep(backoff);
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                    continue;
                }
            };
            // 2. 定位主
            if !self.leaders.contains_key(&gid) {
                self.ask_leader(gid);
            }
            // 3. 发请求,按响应调整路由
            let target = match self.leaders.get(&gid) {
                Some(&t) => t,
                None => self.pick_node(), // 随便找一个,它会回 NotLeader 带提示
            };
            let req_id = self.next_req_id();
            let req = ClientReq::Kv {
                req_id,
                key: key.to_vec(),
                op: op.clone(),
                consistency,
                client_id,
                op_seq,
            };
            match self.transport.request(target, &req) {
                Ok(ClientResp::Kv { result, .. }) => match result {
                    Ok(v) => return Ok(v),
                    Err(KvError::NotLeader(hint)) => {
                        match hint {
                            Some(l) => {
                                self.leaders.insert(gid, l);
                            }
                            None => {
                                self.leaders.remove(&gid);
                                self.ask_leader(gid);
                            }
                        }
                    }
                    Err(KvError::WrongShard) => {
                        let _ = self.refresh_directory();
                    }
                    Err(KvError::NotReady) => {}
                },
                Ok(_) => {}
                Err(_) => {
                    // 主可能挂了:忘掉它,重新问,等新主选出来
                    self.leaders.remove(&gid);
                    self.ask_leader(gid);
                }
            }
            sleep(backoff);
            backoff = (backoff * 2).min(Duration::from_secs(1));
        }
    }
}
