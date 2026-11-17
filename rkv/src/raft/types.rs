//! Raft 消息、日志条目、成员配置等类型定义。

use serde::{Deserialize, Serialize};

use crate::raft::NodeId;
use crate::storage::SnapshotFile;

/// 客户端提案序列号（去重/关联响应）
pub type ClientId = u64;

/// 日志条目载荷。成员变更也以日志形式走 Raft，保证配置本身也是一致复制的。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Command {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    /// 联合共识阶段 1：日志提交后集群进入 `C_old,new`
    EnterJoint { conf: ConfState },
    /// 联合共识阶段 2：日志提交后集群切到 `C_new`
    LeaveJoint { conf: ConfState },
    /// 标记某个 key range 的分片分裂已完成（由分片管理器写入 meta 组）
    ShardSplitDone {
        shard_id: u64,
        new_shard_id: u64,
        range_end: Vec<u8>,
    },
    /// 无操作（如启动时的空提案、读确认后的占位）
    Noop,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub cmd: Command,
}

/// 成员配置。联合共识期间 raft 核心同时持有 current + old 两套。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ConfState {
    pub voters: Vec<NodeId>,
}

impl ConfState {
    pub fn new(mut voters: Vec<NodeId>) -> Self {
        voters.sort_unstable();
        voters.dedup();
        ConfState { voters }
    }
    pub fn contains(&self, id: NodeId) -> bool {
        self.voters.contains(&id)
    }
    pub fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }
}

/// 成员变更请求（由管理接口发起，leader 翻译成 EnterJoint/LeaveJoint 两条日志）。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConfChange {
    AddVoter { node: NodeId },
    RemoveVoter { node: NodeId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadMode {
    /// 走一轮 quorum 心跳确认领导权（ReadIndex，论文 §6.4.1），不依赖时钟
    ReadIndex,
    /// 依赖本地租约（论文 §6.4.2），前提见 node 模块文档
    Lease,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadRequest {
    pub key: Vec<u8>,
    pub mode: ReadMode,
    /// 关联用的请求 id
    pub req_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadResponse {
    pub req_id: u64,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
    /// 磁盘校验失败，永不参与投票/复制
    Faulted,
}

/// 投票 RPC 的处理结果，用于统计与页面展示。
#[derive(Debug, Clone)]
pub struct VoteResponse {
    pub from: NodeId,
    pub term: u64,
    pub granted: bool,
    pub pre_vote: bool,
}

/// 所有节点间 RPC。模拟层与真实 TCP 层传输同一套枚举。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    // ---- 选举 ----
    PreVoteRequest {
        term: u64,
        candidate: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    },
    PreVoteResponse {
        term: u64,
        from: NodeId,
        granted: bool,
    },
    VoteRequest {
        term: u64,
        candidate: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    },
    VoteResponse {
        term: u64,
        from: NodeId,
        granted: bool,
    },

    // ---- 日志复制 / 心跳 ----
    AppendEntries {
        term: u64,
        leader: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
        /// 发送方当前配置（joint 期间 followers 据此更新自己的配置视图）
        conf: ConfState,
        conf_old: Option<ConfState>,
    },
    AppendResponse {
        term: u64,
        from: NodeId,
        /// 与请求 prev_log_index 对齐；成功时为 follower 最后一条匹配的 index
        match_index: u64,
        success: bool,
        /// 冲突提示（论文 §5.3 优化）：follower 该 index 上的实际 term
        conflict_term: Option<u64>,
        conflict_index: u64,
    },

    // ---- 快照 ----
    InstallSnapshot {
        term: u64,
        leader: NodeId,
        snapshot: SnapshotFile,
    },
    SnapshotResponse {
        term: u64,
        from: NodeId,
        index: u64,
        success: bool,
    },

    // ---- 读（ReadIndex 确认轮）----
    ReadIndexHeartbeat {
        term: u64,
        leader: NodeId,
        read_id: u64,
    },
    ReadIndexAck {
        term: u64,
        from: NodeId,
        read_id: u64,
    },

    // ---- 客户端入口 ----
    ClientProposal {
        cmd: Command,
        client_req: ClientId,
    },
    ClientRead(ReadRequest),
}

/// 状态机推进后对外发出的事件，由驱动层处理。
#[derive(Debug, Clone)]
pub enum Event {
    Send { to: NodeId, msg: Message },
    /// 客户端提案被复制到多数派并已应用
    Committed { client_req: ClientId, index: u64 },
    /// 非 leader 收到客户端请求，带上当前已知 leader 提示（可能未知）
    NotLeader { leader: Option<NodeId>, client_req: Option<ClientId> },
    ReadResult(ReadResponse),
    /// 快照落盘完成，WAL 前缀已截断
    SnapshotCompacted { index: u64 },
    /// 任期/角色变化历史，供页面展示
    TermChanged(TermEvent),
    /// 状态机应用了一条命令（分片管理器监听它做分裂编排）
    Applied { index: u64, term: u64, cmd: Command },
    Faulted { reason: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct TermEvent {
    pub term: u64,
    pub role: String,
    /// 选主耗时（毫秒，模拟时间或真实时钟），未完成选举为 None
    pub election_elapsed_ms: Option<u64>,
    pub leader: Option<NodeId>,
    pub at_ms: u64,
}
