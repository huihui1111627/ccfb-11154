//! 线性一致性检查器(porcupine 风格,自研)。
//! 利用线性一致性的局部性(Herlihy & Wing):整体可线性化 当且仅当
//! 每个 key 的操作历史各自可线性化。每个 key 建模为一个寄存器,
//! 对并发操作的所有可能顺序做带回溯的搜索,用 (寄存器值, 已完成操作集) 记忆化。
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpKind {
    Get,
    Put(Vec<u8>),
    Del,
}

#[derive(Clone, Debug)]
pub struct Op {
    pub key: Vec<u8>,
    pub kind: OpKind,
    /// Get 的观测值(None = key 不存在);Put/Del 恒为 None
    pub resp: Option<Vec<u8>>,
    /// 调用/返回时间(同一时钟域即可,模拟用 tick,真实集群用 ms)
    pub start: u64,
    pub end: u64,
    /// 调试:读路径(ReadIndex/Lease),写操作为 None
    pub read_path: Option<String>,
    /// 不确定操作:已发起但客户端没等到回执(可能已提交也可能没有),
    /// 检查器对它们尝试"发生/没发生"两种可能
    pub indeterminate: bool,
}

/// 检查整段历史是否可线性化;不可线性化时返回出问题的 key
pub fn check_linearizable(ops: &[Op]) -> Result<(), Vec<u8>> {
    let mut by_key: HashMap<&[u8], Vec<&Op>> = HashMap::new();
    for op in ops {
        by_key.entry(op.key.as_slice()).or_default().push(op);
    }
    for (key, key_ops) in by_key {
        if !check_register(&key_ops) {
            return Err(key.to_vec());
        }
    }
    Ok(())
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct BitSet(Vec<u64>);

impl BitSet {
    fn new(n: usize) -> BitSet {
        BitSet(vec![0; (n + 63) / 64])
    }
    fn get(&self, i: usize) -> bool {
        self.0[i / 64] >> (i % 64) & 1 == 1
    }
    fn set(&mut self, i: usize, v: bool) {
        if v {
            self.0[i / 64] |= 1 << (i % 64);
        } else {
            self.0[i / 64] &= !(1 << (i % 64));
        }
    }
    fn full(&self, n: usize) -> bool {
        (0..n).all(|i| self.get(i))
    }
    /// self 是否为 other 的子集(precede[i] & ~done == 0 的判断)
    fn subset_of(&self, other: &BitSet) -> bool {
        self.0.iter().zip(&other.0).all(|(a, b)| a & !b == 0)
    }
}

fn check_register(ops: &[&Op]) -> bool {
    // 分离确定操作和不确定操作;对不确定操作枚举"发生/没发生"
    let det: Vec<&Op> = ops.iter().filter(|o| !o.indeterminate).copied().collect();
    let indet: Vec<&Op> = ops.iter().filter(|o| o.indeterminate).copied().collect();
    if indet.len() > 20 {
        // 异常情况:不确定操作太多,全部当作发生(它们绝大多数确实发生了)
        let mut pool = det.clone();
        pool.extend(indet.iter().copied());
        return check_register_pool(&pool);
    }
    for mask in 0..(1u64 << indet.len()) {
        let mut pool = det.clone();
        for (i, op) in indet.iter().enumerate() {
            if mask >> i & 1 == 1 {
                pool.push(op);
            }
        }
        if check_register_pool(&pool) {
            return true;
        }
    }
    false
}

fn check_register_pool(ops: &[&Op]) -> bool {
    let n = ops.len();
    if n == 0 {
        return true;
    }
    // 实时偏序:a.end < b.start 则 a 必须先于 b 线性化;
    // 不确定操作的有效窗口是 [start, +∞)
    let mut precede = vec![BitSet::new(n); n];
    for i in 0..n {
        for j in 0..n {
            let end_j = if ops[j].indeterminate { u64::MAX } else { ops[j].end };
            if i != j && end_j < ops[i].start {
                precede[i].set(j, true);
            }
        }
    }
    let mut memo: HashMap<(Option<Vec<u8>>, BitSet), bool> = HashMap::new();
    let mut done = BitSet::new(n);
    search(ops, &precede, &mut done, &None, &mut memo)
}

fn search(
    ops: &[&Op],
    precede: &[BitSet],
    done: &mut BitSet,
    value: &Option<Vec<u8>>,
    memo: &mut HashMap<(Option<Vec<u8>>, BitSet), bool>,
) -> bool {
    let n = ops.len();
    if done.full(n) {
        return true;
    }
    let key = (value.clone(), done.clone());
    if let Some(&r) = memo.get(&key) {
        return r;
    }
    for i in 0..n {
        if done.get(i) || !precede[i].subset_of(done) {
            continue;
        }
        let next_value = match &ops[i].kind {
            OpKind::Get => {
                // Get 必须读到当前寄存器值
                if ops[i].resp.as_ref() != value.as_ref() {
                    continue;
                }
                value.clone()
            }
            OpKind::Put(v) => Some(v.clone()),
            OpKind::Del => None,
        };
        done.set(i, true);
        if search(ops, precede, done, &next_value, memo) {
            memo.insert(key, true);
            return true;
        }
        done.set(i, false);
    }
    memo.insert(key, false);
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get(key: &[u8], resp: Option<&[u8]>, start: u64, end: u64) -> Op {
        Op {
            key: key.to_vec(),
            kind: OpKind::Get,
            resp: resp.map(|v| v.to_vec()),
            start,
            end,
            read_path: None,
            indeterminate: false,
        }
    }
    fn put(key: &[u8], v: &[u8], start: u64, end: u64) -> Op {
        Op {
            key: key.to_vec(),
            kind: OpKind::Put(v.to_vec()),
            resp: None,
            start,
            end,
            read_path: None,
            indeterminate: false,
        }
    }

    #[test]
    fn accepts_linearizable_history() {
        let ops = vec![
            put(b"k", b"1", 1, 2),
            get(b"k", Some(b"1"), 3, 4),
            put(b"k", b"2", 5, 6),
            get(b"k", Some(b"2"), 7, 8),
        ];
        assert!(check_linearizable(&ops).is_ok());
    }

    #[test]
    fn accepts_concurrent_read_old_or_new() {
        // 读与写并发,读到旧值或新值都合法
        let ops = vec![put(b"k", b"1", 1, 10), get(b"k", None, 3, 4)];
        assert!(check_linearizable(&ops).is_ok());
        let ops = vec![put(b"k", b"1", 1, 10), get(b"k", Some(b"1"), 3, 4)];
        assert!(check_linearizable(&ops).is_ok());
    }

    #[test]
    fn rejects_stale_read_after_completed_write() {
        // 写已返回之后开始的读,必须读到新值
        let ops = vec![put(b"k", b"1", 1, 2), get(b"k", None, 3, 4)];
        assert!(check_linearizable(&ops).is_err());
    }

    #[test]
    fn rejects_impossible_value() {
        let ops = vec![get(b"k", Some(b"ghost"), 1, 2)];
        assert!(check_linearizable(&ops).is_err());
    }

    #[test]
    fn per_key_locality() {
        // k1 合法,k2 非法 → 整体非法
        let ops = vec![
            put(b"k1", b"1", 1, 2),
            get(b"k1", Some(b"1"), 3, 4),
            get(b"k2", Some(b"ghost"), 1, 2),
        ];
        assert_eq!(check_linearizable(&ops), Err(b"k2".to_vec()));
    }
}
