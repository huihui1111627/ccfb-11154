//! Raft 纯状态机：按 Diego Ongaro 博士论文（扩展版 Raft 论文）实现。
//!
//! 覆盖：
//! - 选主、任期、投票规则（论文 §5.2，含 Pre-Vote 防断网节点回归打断主节点）
//! - 日志复制与提交规则（§5.3/§5.4）
//! - 联合共识成员变更（§6，`C_old,new` 两阶段）
//! - 快照与日志截断、InstallSnapshot 追快照（§7）
//! - 线性一致读：ReadIndex（§6.4）与 LeaseRead（§6.4.2，时钟前提见 [`crate::raft::node`]）
//!
//! 本模块不碰网络、不碰真实时钟、不直接起线程：输入是 [`Message`]，输出是
//! [`Event`] 列表，由 `sim`（模拟）或 `runner`（真实 TCP）驱动，天然可确定性测试。

pub mod state_machine;
pub mod types;
pub mod node;

pub use state_machine::{ApplyOutcome, KVStateMachine, StateMachine};
pub use types::{
    ClientId, Command, ConfChange, ConfState, Entry, Event, Message, ReadMode, ReadRequest,
    ReadResponse, Role, TermEvent, VoteResponse,
};
pub use node::{RaftConfig, RaftNode, ELECTION_EVENT_RING};

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;
