//! rkv — Raft 分布式键值存储（范围分片 + 确定性故障模拟 + 线性一致性检查）
//!
//! 模块划分：
//! - [`storage`]：带 CRC 的 WAL / 硬状态 / 快照，损坏即拒启动
//! - [`raft`]：论文级 Raft 纯状态机（选主、日志复制、联合共识、快照、读索引/租约读）
//! - [`shard`]：范围分片、每片一个 Raft 组、自动分裂
//! - [`client`]：自动找主、主切换期等待重试
//! - [`sim`]：可控模拟网络（丢包/延迟/分区/重启/损坏），种子可复现
//! - [`checker`]：自写线性一致性检查器
//! - [`dashboard`]：集群状态页面
//! - [`runner`]：真实 TCP 传输下驱动 Raft 状态机
//! - [`cluster`]：本机多节点集群与模拟入口

pub mod checker;
pub mod client;
pub mod cluster;
pub mod dashboard;
pub mod raft;
pub mod runner;
pub mod shard;
pub mod sim;
pub mod storage;

pub use raft::{Command, ConfState, Entry, NodeId, RaftNode, Term};
