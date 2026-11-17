//! 自写线性一致性检查器（Linearizability checker）。
//!
//! # 问题定义
//! 给定一组客户端操作历史，每个操作有：
//! - 调用时间 `call_time`、返回时间 `return_time`（模拟逻辑时钟，全局一致）
//! - 操作类型与参数（Put/Delete/Read）
//! - 实际观测到的返回值
//! 检查是否存在一条合法的**全序**排列，使得：
//! 1. 每个操作的线性化点落在它的 `[call_time, return_time]` 区间内；
//! 2. 按该顺序应用操作，每个 Read 返回的值等于当时 KV 状态里的值；
//! 3. 同一客户端（client_id）的操作保持其调用顺序（会话顺序）。
//!
//! # 算法
//! Wing–Gong 线性化回溯 + 记忆化：
//! - 状态 = (已线性化操作的位图, 当前 KV 值)；
//! - 每一步候选操作必须满足 `call_time <= 剩余操作中最早的 return_time`
//!   （实时序剪枝：任何在它之后才调用的操作都不可能排到它前面），
//!   且同客户端先调用的操作都已线性化（会话序）；
//! - Read 立即校验观测值，不合法就剪枝；
//! - 到达过的 (位图, 值) 状态记忆化，失败路径不重复探索。
//!
//! 检查按 key 分组独立进行（不同 key 之间没有依赖），大幅缩小搜索空间。
//! 纯标准库实现，随机测试里用它当最终裁判。

use std::collections::{BTreeMap, HashSet};

/// 单条客户端操作。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Op {
    pub client_id: u64,
    pub call_time: u64,
    pub return_time: u64,
    pub kind: OpKind,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OpKind {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Read { key: Vec<u8>, observed: Option<Vec<u8>> },
}

impl OpKind {
    pub fn key(&self) -> &[u8] {
        match self {
            OpKind::Put { key, .. } | OpKind::Delete { key } | OpKind::Read { key, .. } => key,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Violation {
    pub key: Vec<u8>,
    pub detail: String,
    /// 涉及的全局操作下标
    pub ops: Vec<usize>,
}

#[derive(Debug, Default)]
pub struct CheckReport {
    pub ok: bool,
    /// true = 搜索预算耗尽，无法判定（不算 ok，也不武断地算违规）
    pub inconclusive: bool,
    pub violations: Vec<Violation>,
    pub total_ops: usize,
    pub keys_checked: usize,
    /// 搜索中访问过的状态数（规模/性能指标）
    pub states_explored: usize,
}

/// 单 key 搜索的状态数预算，超过则判 inconclusive。
const SEARCH_BUDGET: usize = 5_000_000;

pub fn check(history: &[Op]) -> CheckReport {
    let mut report = CheckReport { total_ops: history.len(), ..Default::default() };

    // 1. 基础合法性：call <= return
    for (i, op) in history.iter().enumerate() {
        if op.call_time > op.return_time {
            report.violations.push(Violation {
                key: op.kind.key().to_vec(),
                detail: format!("op {i}: call_time {} > return_time {}", op.call_time, op.return_time),
                ops: vec![i],
            });
        }
    }

    // 2. 按 key 分组（保留全局下标用于报错）
    let mut by_key: BTreeMap<Vec<u8>, Vec<(usize, &Op)>> = BTreeMap::new();
    for (i, op) in history.iter().enumerate() {
        by_key.entry(op.kind.key().to_vec()).or_default().push((i, op));
    }
    report.keys_checked = by_key.len();

    let mut explored = 0usize;
    for (key, ops) in &by_key {
        let sub: Vec<Op> = ops.iter().map(|(_, o)| (*o).clone()).collect();
        let mut solver = Solver { ops: sub, memo: HashSet::new(), explored: 0 };
        match solver.solve() {
            SolveResult::Linearizable => {}
            SolveResult::Violation => report.violations.push(Violation {
                key: key.clone(),
                detail: format!("no legal linearization for {} ops on key {:?}",
                    ops.len(), String::from_utf8_lossy(key)),
                ops: ops.iter().map(|(g, _)| *g).collect(),
            }),
            SolveResult::BudgetExceeded => {
                report.inconclusive = true;
                report.violations.push(Violation {
                    key: key.clone(),
                    detail: format!("search budget {SEARCH_BUDGET} exhausted, inconclusive"),
                    ops: ops.iter().map(|(g, _)| *g).collect(),
                });
            }
        }
        explored += solver.explored;
    }
    report.states_explored = explored;
    report.ok = report.violations.is_empty();
    report
}

enum SolveResult {
    Linearizable,
    Violation,
    BudgetExceeded,
}

struct Solver {
    ops: Vec<Op>,
    memo: HashSet<(Vec<u64>, Option<Vec<u8>>)>,
    explored: usize,
}

impl Solver {
    fn words(&self) -> usize {
        (self.ops.len() + 63) / 64
    }

    fn solve(&mut self) -> SolveResult {
        let done = vec![0u64; self.words()];
        if self.search(done, None) {
            SolveResult::Linearizable
        } else if self.explored >= SEARCH_BUDGET {
            SolveResult::BudgetExceeded
        } else {
            SolveResult::Violation
        }
    }

    fn is_set(&self, done: &[u64], i: usize) -> bool {
        done[i / 64] & (1 << (i % 64)) != 0
    }

    /// 回溯搜索。`done` 为已线性化操作位图，`value` 为当前 KV 值。
    fn search(&mut self, done: Vec<u64>, value: Option<Vec<u8>>) -> bool {
        self.explored += 1;
        if self.explored > SEARCH_BUDGET {
            return false;
        }
        let n = self.ops.len();
        let done_count: usize = done.iter().map(|w| w.count_ones() as usize).sum();
        if done_count == n {
            return true;
        }
        if self.memo.contains(&(done.clone(), value.clone())) {
            return false; // 同一状态之前到达过且失败
        }

        // 未线性化操作中最早的 return_time：线性化点不能越过它
        let mut earliest_return = u64::MAX;
        for (i, op) in self.ops.iter().enumerate() {
            if !self.is_set(&done, i) {
                earliest_return = earliest_return.min(op.return_time);
            }
        }

        // 会话序（happens-before）：同一客户端里，凡是"前一个操作已经返回之后
        // 才调用"的操作都必须先线性化。注意并发在途请求（前一个还没返回就调用）
        // 之间没有顺序约束——不能简单按历史下标排，否则会把合法并发误判成违规。
        // 对每个候选 op，收集它在会话里必须等待的前驱（同 client 且 return<=call）。
        let mut hb_preds: Vec<Vec<usize>> = vec![Vec::new(); self.ops.len()];
        for (i, op) in self.ops.iter().enumerate() {
            for (j, other) in self.ops.iter().enumerate() {
                if i != j
                    && other.client_id == op.client_id
                    && other.return_time <= op.call_time
                    && !self.is_set(&done, j)
                {
                    hb_preds[i].push(j);
                }
            }
        }

        for i in 0..n {
            if self.is_set(&done, i) {
                continue;
            }
            let op = &self.ops[i];
            // Wing–Gong 实时序剪枝：调用晚于"最早返回"的操作没资格排下一个
            if op.call_time > earliest_return {
                continue;
            }
            // 会话 happens-before：前驱未线性化则等待
            if !hb_preds[i].is_empty() {
                continue;
            }

            // 应用操作并校验观测
            let next_value = match &op.kind {
                OpKind::Put { value, .. } => Some(value.clone()),
                OpKind::Delete { .. } => None,
                OpKind::Read { observed, .. } => {
                    if observed != &value {
                        continue; // 读到的值与候选历史不符，剪枝
                    }
                    value.clone()
                }
            };

            let mut next_done = done.clone();
            next_done[i / 64] |= 1 << (i % 64);
            if self.search(next_done, next_value) {
                return true;
            }
        }

        self.memo.insert((done, value));
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(c: u64, t0: u64, t1: u64, key: &[u8], val: &[u8]) -> Op {
        Op {
            client_id: c,
            call_time: t0,
            return_time: t1,
            kind: OpKind::Put { key: key.to_vec(), value: val.to_vec() },
        }
    }
    fn read(c: u64, t0: u64, t1: u64, key: &[u8], obs: Option<&[u8]>) -> Op {
        Op {
            client_id: c,
            call_time: t0,
            return_time: t1,
            kind: OpKind::Read { key: key.to_vec(), observed: obs.map(|b| b.to_vec()) },
        }
    }
    fn delete(c: u64, t0: u64, t1: u64, key: &[u8]) -> Op {
        Op {
            client_id: c,
            call_time: t0,
            return_time: t1,
            kind: OpKind::Delete { key: key.to_vec() },
        }
    }

    #[test]
    fn accepts_consistent_history() {
        let h = vec![
            put(1, 0, 10, b"k", b"a"),
            read(2, 5, 15, b"k", Some(b"a")),
            put(1, 12, 20, b"k", b"b"),
            read(2, 21, 25, b"k", Some(b"b")),
        ];
        assert!(check(&h).ok);
    }

    #[test]
    fn rejects_stale_read() {
        let h = vec![
            put(1, 0, 5, b"k", b"a"),
            put(1, 6, 10, b"k", b"b"),
            read(2, 11, 15, b"k", Some(b"a")),
        ];
        let r = check(&h);
        assert!(!r.ok && !r.inconclusive);
        assert_eq!(r.violations.len(), 1);
    }

    #[test]
    fn concurrent_ops_can_reorder() {
        let h = vec![
            put(1, 0, 10, b"k", b"a"),
            put(2, 1, 9, b"k", b"b"),
            read(3, 2, 12, b"k", Some(b"b")),
        ];
        assert!(check(&h).ok);
    }

    #[test]
    fn respects_session_order() {
        // 同一客户端先写 b 后写 a（两次写不重叠，存在 happens-before），
        // 最后读到 b 不可能合法
        let h = vec![
            put(1, 0, 4, b"k", b"b"),
            put(1, 5, 9, b"k", b"a"),
            read(2, 10, 12, b"k", Some(b"b")),
        ];
        assert!(!check(&h).ok);
    }

    #[test]
    fn concurrent_inflight_same_client_has_no_session_order() {
        // 同一客户端两个并发在途写（调用区间重叠），之间没有 happens-before，
        // 读到任意一个都合法
        let h = vec![
            put(1, 0, 10, b"k", b"a"),
            put(1, 1, 9, b"k", b"b"),
            read(1, 11, 14, b"k", Some(b"a")),
        ];
        assert!(check(&h).ok);
        let h2 = vec![
            put(1, 0, 10, b"k", b"a"),
            put(1, 1, 9, b"k", b"b"),
            read(1, 11, 14, b"k", Some(b"b")),
        ];
        assert!(check(&h2).ok);
    }

    #[test]
    fn delete_is_observed() {
        let h = vec![
            put(1, 0, 5, b"k", b"a"),
            delete(1, 6, 8, b"k"),
            read(2, 9, 12, b"k", None),
        ];
        assert!(check(&h).ok);
        let bad = vec![
            put(1, 0, 5, b"k", b"a"),
            delete(1, 6, 8, b"k"),
            read(2, 9, 12, b"k", Some(b"a")),
        ];
        assert!(!check(&bad).ok);
    }

    #[test]
    fn independent_keys_checked_separately() {
        let h = vec![
            put(1, 0, 5, b"k1", b"a"),
            read(2, 6, 9, b"k2", None),
            read(3, 6, 9, b"k1", Some(b"a")),
        ];
        assert!(check(&h).ok);
    }
}
