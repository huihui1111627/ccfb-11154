//! 持久化存储：WAL 日志、硬状态（term/vote）、快照，全部带 CRC32 校验。
//!
//! 磁盘布局（`dir/` 下）：
//! - `wal.log`        追加写日志，帧格式 `MAGIC | LEN | CRC32(payload) | payload`
//! - `hardstate.json` 当前 term / 投票 / commit 索引（原子 rename）
//! - `hardstate.crc`  hardstate.json 的 CRC32（hex）
//! - `snapshot.json`  快照（原子 rename）
//! - `snapshot.crc`   snapshot.json 的 CRC32（hex）
//!
//! 容错语义（对应用户需求 7）：
//! - 启动时 `verify_integrity()` 全量校验，任何不匹配一律报 [`StoreError::Corruption`]，
//!   上层节点进入 Faulted 状态，不参与投票、不响应请求，绝不"带病上岗"。
//! - 帧损坏 / CRC 缺失 / 尾部半帧同样按损坏处理（崩溃恢复由上层保证只在干净
//!   shutdown 后重启，断电半帧场景在这里显式暴露而不是悄悄丢弃）。

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::raft::{Command, ConfState, Entry};

const WAL_MAGIC: u32 = 0x5741_4C31; // "WAL1"
const WAL_NAME: &str = "wal.log";
const HS_NAME: &str = "hardstate.json";
const HS_CRC: &str = "hardstate.crc";
const SNAP_NAME: &str = "snapshot.json";
const SNAP_CRC: &str = "snapshot.crc";

#[derive(Debug)]
pub enum StoreError {
    Io(std::io::Error),
    Corruption(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Io(e) => write!(f, "io error: {e}"),
            StoreError::Corruption(msg) => write!(f, "on-disk corruption detected: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        StoreError::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;

/// Raft 必须持久化的硬状态（论文 Figure 2）。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HardState {
    pub term: u64,
    pub vote: Option<u64>,
    pub commit: u64,
}

impl Default for HardState {
    fn default() -> Self {
        HardState { term: 0, vote: None, commit: 0 }
    }
}

/// 快照文件内容。`data` 对存储层不透明，由状态机自己解释（KV 机用 JSON）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotFile {
    pub index: u64,
    pub term: u64,
    pub conf: ConfState,
    pub conf_old: Option<ConfState>,
    /// 应用层快照（KV 机序列化为 JSON 的字节）
    pub data: Vec<u8>,
}

#[derive(Clone, Copy)]
struct FrameLoc {
    offset: u64,
    len: u32,
}

pub struct RaftStore {
    dir: PathBuf,
    wal_path: PathBuf,
    wal: fs::File,
    /// 每条日志在 WAL 中的位置，按 index 升序
    frames: BTreeMap<u64, FrameLoc>,
    sync: bool,
    corrupted: bool,
}

impl RaftStore {
    pub fn open(dir: impl AsRef<Path>, sync: bool) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let wal_path = dir.join(WAL_NAME);
        let wal = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&wal_path)?;

        let mut store = RaftStore {
            dir,
            wal_path,
            wal,
            frames: BTreeMap::new(),
            sync,
            corrupted: false,
        };
        store.rebuild_index()?;
        Ok(store)
    }

    // ---------- 启动校验 ----------

    /// 全量校验磁盘数据。节点启动时、参与任何投票之前必须先调用。
    pub fn verify_integrity(&self) -> Result<()> {
        if self.corrupted {
            return Err(StoreError::Corruption("store already marked corrupt".into()));
        }
        // 1. WAL 全帧重读校验
        let mut f = fs::File::open(&self.wal_path)?;
        let file_len = f.metadata()?.len();
        let mut pos = 0u64;
        let mut count = 0u64;
        while pos < file_len {
            let mut header = [0u8; 12];
            read_exact_or_corrupt(&mut f, &mut header, &self.wal_path, pos)?;
            let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
            let len = u32::from_le_bytes(header[4..8].try_into().unwrap());
            let crc = u32::from_le_bytes(header[8..12].try_into().unwrap());
            if magic != WAL_MAGIC {
                return Err(StoreError::Corruption(format!(
                    "{}: bad frame magic at offset {pos}",
                    self.wal_path.display()
                )));
            }
            let mut payload = vec![0u8; len as usize];
            read_exact_or_corrupt(&mut f, &mut payload, &self.wal_path, pos + 12)?;
            if crc32fast::hash(&payload) != crc {
                return Err(StoreError::Corruption(format!(
                    "{}: crc mismatch at offset {pos}",
                    self.wal_path.display()
                )));
            }
            // 顺便校验 payload 本身能反序列化
            let entry: Entry = serde_json::from_slice(&payload).map_err(|e| {
                StoreError::Corruption(format!(
                    "{}: undecodable entry at offset {pos}: {e}",
                    self.wal_path.display()
                ))
            })?;
            if !self.frames.contains_key(&entry.index) {
                return Err(StoreError::Corruption(format!(
                    "{}: wal index {} not in rebuilt index",
                    self.wal_path.display(),
                    entry.index
                )));
            }
            pos += 12 + len as u64;
            count += 1;
        }
        if count as usize != self.frames.len() {
            return Err(StoreError::Corruption(format!(
                "{}: frame count {count} != index size {}",
                self.wal_path.display(),
                self.frames.len()
            )));
        }

        // 2. 硬状态校验
        let hs_path = self.dir.join(HS_NAME);
        if hs_path.exists() {
            let bytes = fs::read(&hs_path)?;
            let expect = read_crc_sidecar(&self.dir.join(HS_CRC), HS_NAME)?;
            let actual = crc32fast::hash(&bytes);
            if expect != actual {
                return Err(StoreError::Corruption(format!(
                    "{}: crc mismatch",
                    hs_path.display()
                )));
            }
            serde_json::from_slice::<HardState>(&bytes).map_err(|e| {
                StoreError::Corruption(format!("{}: undecodable: {e}", hs_path.display()))
            })?;
        }

        // 3. 快照校验
        let snap_path = self.dir.join(SNAP_NAME);
        if snap_path.exists() {
            let bytes = fs::read(&snap_path)?;
            let expect = read_crc_sidecar(&self.dir.join(SNAP_CRC), SNAP_NAME)?;
            let actual = crc32fast::hash(&bytes);
            if expect != actual {
                return Err(StoreError::Corruption(format!(
                    "{}: crc mismatch",
                    snap_path.display()
                )));
            }
            serde_json::from_slice::<SnapshotFile>(&bytes).map_err(|e| {
                StoreError::Corruption(format!("{}: undecodable: {e}", snap_path.display()))
            })?;
        }
        Ok(())
    }

    fn rebuild_index(&mut self) -> Result<()> {
        self.frames.clear();
        let file_len = self.wal.metadata()?.len();
        let mut pos = 0u64;
        while pos < file_len {
            let mut header = [0u8; 12];
            read_exact_or_corrupt(&mut self.wal, &mut header, &self.wal_path, pos)?;
            let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
            let len = u32::from_le_bytes(header[4..8].try_into().unwrap());
            let crc = u32::from_le_bytes(header[8..12].try_into().unwrap());
            if magic != WAL_MAGIC {
                return Err(StoreError::Corruption(format!(
                    "{}: bad frame magic at offset {pos}",
                    self.wal_path.display()
                )));
            }
            let mut payload = vec![0u8; len as usize];
            read_exact_or_corrupt(&mut self.wal, &mut payload, &self.wal_path, pos + 12)?;
            if crc32fast::hash(&payload) != crc {
                return Err(StoreError::Corruption(format!(
                    "{}: crc mismatch at offset {pos}",
                    self.wal_path.display()
                )));
            }
            let entry: Entry = serde_json::from_slice(&payload).map_err(|e| {
                StoreError::Corruption(format!(
                    "{}: undecodable entry at offset {pos}: {e}",
                    self.wal_path.display()
                ))
            })?;
            self.frames.insert(entry.index, FrameLoc { offset: pos, len });
            pos += 12 + len as u64;
        }
        Ok(())
    }

    // ---------- 硬状态 ----------

    pub fn save_hard_state(&mut self, hs: &HardState) -> Result<()> {
        let bytes = serde_json::to_vec(hs).unwrap();
        atomic_write_with_crc(&self.dir, HS_NAME, HS_CRC, &bytes, self.sync)?;
        Ok(())
    }

    pub fn load_hard_state(&self) -> Result<Option<HardState>> {
        let p = self.dir.join(HS_NAME);
        if !p.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&p)?;
        let expect = read_crc_sidecar(&self.dir.join(HS_CRC), HS_NAME)?;
        if crc32fast::hash(&bytes) != expect {
            return Err(StoreError::Corruption(format!("{}: crc mismatch", p.display())));
        }
        Ok(Some(serde_json::from_slice(&bytes).map_err(|e| {
            StoreError::Corruption(format!("{}: {e}", p.display()))
        })?))
    }

    // ---------- WAL ----------

    pub fn append(&mut self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.wal.seek(SeekFrom::End(0))?;
        for e in entries {
            let payload = serde_json::to_vec(e).unwrap();
            let crc = crc32fast::hash(&payload);
            let mut frame = Vec::with_capacity(12 + payload.len());
            frame.extend_from_slice(&WAL_MAGIC.to_le_bytes());
            frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            frame.extend_from_slice(&crc.to_le_bytes());
            frame.extend_from_slice(&payload);
            let offset = self.wal.stream_position()?;
            self.wal.write_all(&frame)?;
            self.frames.insert(e.index, FrameLoc { offset, len: payload.len() as u32 });
        }
        if self.sync {
            self.wal.sync_all()?;
        }
        Ok(())
    }

    fn read_entry(&mut self, index: u64) -> Result<Entry> {
        let loc = *self
            .frames
            .get(&index)
            .ok_or_else(|| StoreError::Corruption(format!("wal has no entry {index}")))?;
        self.wal.seek(SeekFrom::Start(loc.offset + 12))?;
        let mut payload = vec![0u8; loc.len as usize];
        self.wal.read_exact(&mut payload)?;
        if crc32fast::hash(&payload) != self.read_frame_crc(loc.offset)? {
            return Err(StoreError::Corruption(format!(
                "{}: crc mismatch at index {index}",
                self.wal_path.display()
            )));
        }
        serde_json::from_slice(&payload).map_err(|e| {
            StoreError::Corruption(format!("entry {index} undecodable: {e}"))
        })
    }

    fn read_frame_crc(&mut self, offset: u64) -> Result<u32> {
        self.wal.seek(SeekFrom::Start(offset + 8))?;
        let mut crc_bytes = [0u8; 4];
        self.wal.read_exact(&mut crc_bytes)?;
        Ok(u32::from_le_bytes(crc_bytes))
    }

    pub fn entry(&mut self, index: u64) -> Result<Option<Entry>> {
        if !self.frames.contains_key(&index) {
            return Ok(None);
        }
        Ok(Some(self.read_entry(index)?))
    }

    pub fn entries(&mut self, lo: u64, max: usize) -> Result<Vec<Entry>> {
        let indexes: Vec<u64> = self.frames.range(lo..).map(|(k, _)| *k).take(max).collect();
        let mut out = Vec::with_capacity(indexes.len());
        for idx in indexes {
            out.push(self.read_entry(idx)?);
        }
        Ok(out)
    }

    pub fn has_index(&self, index: u64) -> bool {
        self.frames.contains_key(&index)
    }

    pub fn first_index(&self) -> Option<u64> {
        self.frames.keys().next().copied()
    }

    pub fn last_index(&self) -> Option<u64> {
        self.frames.keys().next_back().copied()
    }

    /// 删除 index >= from 的所有日志（follower 冲突回退）。
    pub fn truncate_suffix(&mut self, from: u64) -> Result<()> {
        if let Some(&loc) = self.frames.get(&from) {
            self.wal.set_len(loc.offset)?;
            if self.sync {
                self.wal.sync_all()?;
            }
        }
        self.frames.split_off(&from);
        Ok(())
    }

    /// 删除 index < keep 的所有日志（打快照后截断前缀）。
    pub fn truncate_prefix(&mut self, keep: u64) -> Result<()> {
        // 重写 WAL，只保留 >= keep 的帧
        let indexes: Vec<u64> = self.frames.range(keep..).map(|(k, _)| *k).collect();
        let mut kept: Vec<Entry> = Vec::with_capacity(indexes.len());
        for idx in indexes {
            kept.push(self.read_entry(idx)?);
        }
        let tmp = self.dir.join(format!("{WAL_NAME}.tmp"));
        {
            let mut new = fs::File::create(&tmp)?;
            let mut new_frames = BTreeMap::new();
            let mut offset = 0u64;
            for e in &kept {
                let payload = serde_json::to_vec(e).unwrap();
                let crc = crc32fast::hash(&payload);
                let mut frame = Vec::with_capacity(12 + payload.len());
                frame.extend_from_slice(&WAL_MAGIC.to_le_bytes());
                frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                frame.extend_from_slice(&crc.to_le_bytes());
                frame.extend_from_slice(&payload);
                new.write_all(&frame)?;
                new_frames.insert(e.index, FrameLoc { offset, len: payload.len() as u32 });
                offset += frame.len() as u64;
            }
            if self.sync {
                new.sync_all()?;
            }
        }
        fs::rename(&tmp, &self.wal_path)?;
        self.wal = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.wal_path)?;
        self.frames.clear();
        self.rebuild_index()?;
        Ok(())
    }

    // ---------- 快照 ----------

    pub fn write_snapshot(&mut self, snap: &SnapshotFile) -> Result<()> {
        let bytes = serde_json::to_vec(snap).unwrap();
        atomic_write_with_crc(&self.dir, SNAP_NAME, SNAP_CRC, &bytes, self.sync)?;
        Ok(())
    }

    pub fn load_snapshot(&self) -> Result<Option<SnapshotFile>> {
        let p = self.dir.join(SNAP_NAME);
        if !p.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&p)?;
        let expect = read_crc_sidecar(&self.dir.join(SNAP_CRC), SNAP_NAME)?;
        if crc32fast::hash(&bytes) != expect {
            return Err(StoreError::Corruption(format!("{}: crc mismatch", p.display())));
        }
        Ok(Some(serde_json::from_slice(&bytes).map_err(|e| {
            StoreError::Corruption(format!("{}: {e}", p.display()))
        })?))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    // ---------- 故障注入（供模拟层使用） ----------

    /// 测试用：主动破坏磁盘内容，返回后节点应在下次重启时被 `verify_integrity` 拦住。
    pub fn inject_corruption(&mut self, kind: CorruptionKind) -> Result<()> {
        self.corrupted = true;
        match kind {
            CorruptionKind::FlipHardState => {
                let p = self.dir.join(HS_NAME);
                if p.exists() {
                    let mut b = fs::read(&p)?;
                    if !b.is_empty() {
                        b[0] ^= 0xFF;
                        fs::write(&p, b)?;
                    }
                }
            }
            CorruptionKind::FlipSnapshot => {
                let p = self.dir.join(SNAP_NAME);
                if p.exists() {
                    let mut b = fs::read(&p)?;
                    if !b.is_empty() {
                        b[0] ^= 0xFF;
                        fs::write(&p, b)?;
                    }
                }
            }
            CorruptionKind::TruncateWalTail => {
                let len = self.wal.metadata()?.len();
                if len > 4 {
                    self.wal.set_len(len - 4)?; // 砍掉最后 4 字节 -> 尾部半帧
                }
            }
            CorruptionKind::RemoveCrc => {
                let _ = fs::remove_file(self.dir.join(HS_CRC));
                let _ = fs::remove_file(self.dir.join(SNAP_CRC));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CorruptionKind {
    FlipHardState,
    FlipSnapshot,
    TruncateWalTail,
    RemoveCrc,
}

fn read_exact_or_corrupt(
    f: &mut fs::File,
    buf: &mut [u8],
    path: &Path,
    offset: u64,
) -> Result<()> {
    match f.read_exact(buf) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Err(StoreError::Corruption(
            format!("{}: truncated frame at offset {offset}", path.display()),
        )),
        Err(e) => Err(e.into()),
    }
}

fn read_crc_sidecar(path: &Path, owner: &str) -> Result<u32> {
    let s = fs::read_to_string(path).map_err(|e| {
        StoreError::Corruption(format!("missing crc sidecar for {owner}: {e}"))
    })?;
    u32::from_str_radix(s.trim(), 16).map_err(|e| {
        StoreError::Corruption(format!("bad crc sidecar {}: {e}", path.display()))
    })
}

fn atomic_write_with_crc(
    dir: &Path,
    name: &str,
    crc_name: &str,
    bytes: &[u8],
    sync: bool,
) -> Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    let crc_tmp = dir.join(format!("{crc_name}.tmp"));
    fs::write(&tmp, bytes)?;
    fs::write(&crc_tmp, format!("{:08x}", crc32fast::hash(bytes)))?;
    if sync {
        // 先刷数据再 rename，保证 crc 不可能先于数据可见
        fs::File::open(&tmp)?.sync_all()?;
    }
    fs::rename(&tmp, dir.join(name))?;
    fs::rename(&crc_tmp, dir.join(crc_name))?;
    Ok(())
}

/// 给状态机/测试用的通用快照编解码助手。
/// JSON 对象键必须是字符串，因此序列化为 `[[hex_key, hex_value], ...]` 数组。
pub fn json_snapshot_encode(map: &BTreeMap<Vec<u8>, Vec<u8>>) -> Vec<u8> {
    let pairs: Vec<(String, String)> = map
        .iter()
        .map(|(k, v)| (hex_encode(k), hex_encode(v)))
        .collect();
    serde_json::to_vec(&pairs).unwrap()
}

pub fn json_snapshot_decode(bytes: &[u8]) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let pairs: Vec<(String, String)> = serde_json::from_slice(bytes)
        .map_err(|e| StoreError::Corruption(format!("kv snapshot: {e}")))?;
    let mut map = BTreeMap::new();
    for (k, v) in pairs {
        map.insert(hex_decode(&k)?, hex_decode(&v)?);
    }
    Ok(map)
}

pub fn hex_encode(bytes: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(H[(b >> 4) as usize] as char);
        s.push(H[(b & 0x0f) as usize] as char);
    }
    s
}

pub fn hex_decode(s: &str) -> Result<Vec<u8>> {
    let bytes = s.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(StoreError::Corruption("hex string has odd length".into()));
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let val = |c: u8| -> Result<u8> {
        match c {
            b'0'..=b'9' => Ok(c - b'0'),
            b'a'..=b'f' => Ok(c - b'a' + 10),
            b'A'..=b'F' => Ok(c - b'A' + 10),
            _ => Err(StoreError::Corruption(format!("bad hex char {c}"))),
        }
    };
    for pair in bytes.chunks(2) {
        out.push((val(pair[0])? << 4) | val(pair[1])?);
    }
    Ok(out)
}

/// 仅供测试/模拟：把一条 Command 直接转 JSON 字节（日志载荷格式）。
pub fn encode_command(cmd: &Command) -> Vec<u8> {
    serde_json::to_vec(cmd).unwrap()
}
