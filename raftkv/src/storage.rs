//! 持久化存储层。所有落盘记录都带 CRC32 校验,加载时逐条验证,
//! 发现损坏即报错,节点进入 Refused 状态(不参与投票、不竞选)。
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::raft::{Conf, Entry, GroupId, NodeId, Snapshot};

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct HardState {
    pub term: u64,
    pub vote: Option<NodeId>,
    pub commit: u64,
}

#[derive(Debug)]
pub enum StorageError {
    Corrupt(String),
    Io(std::io::Error),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Corrupt(s) => write!(f, "storage corrupt: {}", s),
            StorageError::Io(e) => write!(f, "io error: {}", e),
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        StorageError::Io(e)
    }
}

pub struct StoredState {
    pub hs: HardState,
    pub conf: Option<(Conf, u64)>,
    pub entries: Vec<Entry>,
    pub snap: Option<Snapshot>,
}

pub trait Storage: Send {
    fn load(&mut self) -> Result<StoredState, StorageError>;
    fn save_hardstate(&mut self, hs: &HardState);
    /// 持久化当前配置及设置它的日志下标(重启后从此重建)
    fn save_conf(&mut self, conf: &Conf, index: u64);
    fn append(&mut self, entries: &[Entry]);
    /// 删除 index >= from 的日志(冲突截断)
    fn truncate_from(&mut self, from: u64);
    /// 删除 index <= to 的日志(快照压缩)
    fn compact_to(&mut self, to: u64);
    fn save_snapshot(&mut self, snap: &Snapshot);
    /// 重新读盘校验 CRC,用于一致性检查页面
    fn verify(&self) -> bool;
}

pub trait StorageFactory: Send {
    fn open(&mut self, node: NodeId, group: GroupId) -> Box<dyn Storage>;
    fn groups_for(&self, node: NodeId) -> Vec<GroupId>;
    fn wipe(&mut self, node: NodeId, group: GroupId);
}

// ---------------- MemStorage(模拟环境用,磁盘即内存对象,重启=换 Raft、保留 disk) ----------------

#[derive(Default)]
pub struct MemDisk {
    pub hs: Option<HardState>,
    pub conf: Option<(Conf, u64)>,
    pub entries: Vec<Entry>,
    pub snap: Option<Snapshot>,
    /// 模拟磁盘 bitrot:Some(原因) 时 load 报 Corrupt
    pub corrupt: Option<String>,
}

pub struct MemStorage(pub Arc<Mutex<MemDisk>>);

impl Storage for MemStorage {
    fn load(&mut self) -> Result<StoredState, StorageError> {
        let d = self.0.lock().unwrap();
        if let Some(reason) = &d.corrupt {
            return Err(StorageError::Corrupt(reason.clone()));
        }
        Ok(StoredState {
            hs: d.hs.clone().unwrap_or_default(),
            conf: d.conf.clone(),
            entries: d.entries.clone(),
            snap: d.snap.clone(),
        })
    }
    fn save_hardstate(&mut self, hs: &HardState) {
        self.0.lock().unwrap().hs = Some(hs.clone());
    }
    fn save_conf(&mut self, conf: &Conf, index: u64) {
        self.0.lock().unwrap().conf = Some((conf.clone(), index));
    }
    fn append(&mut self, entries: &[Entry]) {
        self.0.lock().unwrap().entries.extend_from_slice(entries);
    }
    fn truncate_from(&mut self, from: u64) {
        self.0.lock().unwrap().entries.retain(|e| e.index < from);
    }
    fn compact_to(&mut self, to: u64) {
        self.0.lock().unwrap().entries.retain(|e| e.index > to);
    }
    fn save_snapshot(&mut self, snap: &Snapshot) {
        self.0.lock().unwrap().snap = Some(snap.clone());
    }
    fn verify(&self) -> bool {
        self.0.lock().unwrap().corrupt.is_none()
    }
}

#[derive(Clone, Default)]
pub struct MemFactory {
    pub disks: Arc<Mutex<HashMap<(NodeId, GroupId), Arc<Mutex<MemDisk>>>>>,
}

impl MemFactory {
    pub fn corrupt(&self, node: NodeId, group: GroupId, reason: &str) {
        let disks = self.disks.lock().unwrap();
        if let Some(d) = disks.get(&(node, group)) {
            d.lock().unwrap().corrupt = Some(reason.to_string());
        }
    }
    pub fn disk(&self, node: NodeId, group: GroupId) -> Option<Arc<Mutex<MemDisk>>> {
        self.disks.lock().unwrap().get(&(node, group)).cloned()
    }
}

impl StorageFactory for MemFactory {
    fn open(&mut self, node: NodeId, group: GroupId) -> Box<dyn Storage> {
        let mut disks = self.disks.lock().unwrap();
        let disk = disks.entry((node, group)).or_insert_with(|| Arc::new(Mutex::new(MemDisk::default())));
        Box::new(MemStorage(disk.clone()))
    }
    fn groups_for(&self, node: NodeId) -> Vec<GroupId> {
        let disks = self.disks.lock().unwrap();
        let mut g: Vec<GroupId> = disks.keys().filter(|(n, _)| *n == node).map(|(_, g)| *g).collect();
        g.sort_unstable();
        g
    }
    fn wipe(&mut self, node: NodeId, group: GroupId) {
        self.disks
            .lock()
            .unwrap()
            .insert((node, group), Arc::new(Mutex::new(MemDisk::default())));
    }
}

// ---------------- FileStorage(真实部署用,每条记录 CRC32) ----------------

pub struct FileStorage {
    dir: PathBuf,
}

fn crc_of(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

impl FileStorage {
    pub fn open(dir: &Path) -> std::io::Result<FileStorage> {
        fs::create_dir_all(dir)?;
        Ok(FileStorage { dir: dir.to_path_buf() })
    }
    fn hs_path(&self) -> PathBuf {
        self.dir.join("hardstate.bin")
    }
    fn log_path(&self) -> PathBuf {
        self.dir.join("log.bin")
    }
    fn snap_path(&self) -> PathBuf {
        self.dir.join("snapshot.bin")
    }
    fn conf_path(&self) -> PathBuf {
        self.dir.join("conf.bin")
    }

    fn write_crc_file(path: &Path, payload: &[u8]) -> std::io::Result<()> {
        let tmp = path.with_extension("tmp");
        let mut buf = Vec::with_capacity(payload.len() + 4);
        buf.extend_from_slice(&crc_of(payload).to_le_bytes());
        buf.extend_from_slice(payload);
        fs::write(&tmp, &buf)?;
        fs::rename(&tmp, path)?;
        Ok(())
    }

    fn read_crc_file(path: &Path) -> Result<Option<Vec<u8>>, StorageError> {
        match fs::read(path) {
            Ok(buf) => {
                if buf.len() < 4 {
                    return Err(StorageError::Corrupt(format!("{:?} too short", path)));
                }
                let (crc_b, payload) = buf.split_at(4);
                let expect = u32::from_le_bytes(crc_b.try_into().unwrap());
                if crc_of(payload) != expect {
                    return Err(StorageError::Corrupt(format!("{:?} crc mismatch", path)));
                }
                Ok(Some(payload.to_vec()))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::Io(e)),
        }
    }

    fn read_log(&self) -> Result<Vec<Entry>, StorageError> {
        let mut entries = Vec::new();
        let buf = match fs::read(self.log_path()) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(entries),
            Err(e) => return Err(StorageError::Io(e)),
        };
        let mut cur = std::io::Cursor::new(&buf);
        loop {
            let mut len_b = [0u8; 4];
            match cur.read_exact(&mut len_b) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(StorageError::Io(e)),
            }
            let len = u32::from_le_bytes(len_b) as usize;
            let mut crc_b = [0u8; 4];
            cur.read_exact(&mut crc_b).map_err(|_| StorageError::Corrupt("log record truncated crc".into()))?;
            let mut payload = vec![0u8; len];
            cur.read_exact(&mut payload).map_err(|_| StorageError::Corrupt("log record truncated payload".into()))?;
            if crc_of(&payload) != u32::from_le_bytes(crc_b) {
                return Err(StorageError::Corrupt(format!(
                    "log record crc mismatch at offset {}",
                    cur.position() as usize - len
                )));
            }
            let e: Entry = serde_json::from_slice(&payload)
                .map_err(|err| StorageError::Corrupt(format!("log record decode: {}", err)))?;
            entries.push(e);
        }
        Ok(entries)
    }

    fn rewrite_log(&self, entries: &[Entry]) {
        let mut buf = Vec::new();
        for e in entries {
            let payload = serde_json::to_vec(e).unwrap();
            buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            buf.extend_from_slice(&crc_of(&payload).to_le_bytes());
            buf.extend_from_slice(&payload);
        }
        let tmp = self.log_path().with_extension("tmp");
        fs::write(&tmp, &buf).unwrap();
        fs::rename(&tmp, self.log_path()).unwrap();
    }
}

impl Storage for FileStorage {
    fn load(&mut self) -> Result<StoredState, StorageError> {
        let hs = match Self::read_crc_file(&self.hs_path())? {
            Some(p) => serde_json::from_slice(&p).map_err(|e| StorageError::Corrupt(format!("hardstate decode: {}", e)))?,
            None => HardState::default(),
        };
        let snap: Option<Snapshot> = match Self::read_crc_file(&self.snap_path())? {
            Some(p) => Some(serde_json::from_slice(&p).map_err(|e| StorageError::Corrupt(format!("snapshot decode: {}", e)))?),
            None => None,
        };
        let conf = match Self::read_crc_file(&self.conf_path())? {
            Some(p) => Some(serde_json::from_slice(&p).map_err(|e| StorageError::Corrupt(format!("conf decode: {}", e)))?),
            None => None,
        };
        let entries = self.read_log()?;
        // 连续性校验:日志必须紧接快照之后且下标连续
        let mut expect = snap.as_ref().map(|s| s.index + 1).unwrap_or(1);
        for e in &entries {
            if e.index != expect {
                return Err(StorageError::Corrupt(format!(
                    "log not contiguous: expect index {}, got {}",
                    expect, e.index
                )));
            }
            expect += 1;
        }
        Ok(StoredState { hs, conf, entries, snap })
    }

    fn save_hardstate(&mut self, hs: &HardState) {
        Self::write_crc_file(&self.hs_path(), &serde_json::to_vec(hs).unwrap()).unwrap();
    }

    fn save_conf(&mut self, conf: &Conf, index: u64) {
        Self::write_crc_file(&self.conf_path(), &serde_json::to_vec(&(conf, index)).unwrap()).unwrap();
    }

    fn append(&mut self, entries: &[Entry]) {
        if entries.is_empty() {
            return;
        }
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_path())
            .unwrap();
        for e in entries {
            let payload = serde_json::to_vec(e).unwrap();
            f.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
            f.write_all(&crc_of(&payload).to_le_bytes()).unwrap();
            f.write_all(&payload).unwrap();
        }
    }

    fn truncate_from(&mut self, from: u64) {
        let entries: Vec<Entry> = self.read_log().unwrap_or_default().into_iter().filter(|e| e.index < from).collect();
        self.rewrite_log(&entries);
    }

    fn compact_to(&mut self, to: u64) {
        let entries: Vec<Entry> = self.read_log().unwrap_or_default().into_iter().filter(|e| e.index > to).collect();
        self.rewrite_log(&entries);
    }

    fn save_snapshot(&mut self, snap: &Snapshot) {
        Self::write_crc_file(&self.snap_path(), &serde_json::to_vec(snap).unwrap()).unwrap();
    }

    fn verify(&self) -> bool {
        (|| -> Result<(), StorageError> {
            if let Some(p) = Self::read_crc_file(&self.hs_path())? {
                let _: HardState = serde_json::from_slice(&p).map_err(|e| StorageError::Corrupt(e.to_string()))?;
            }
            if let Some(p) = Self::read_crc_file(&self.snap_path())? {
                let _: Snapshot = serde_json::from_slice(&p).map_err(|e| StorageError::Corrupt(e.to_string()))?;
            }
            if let Some(p) = Self::read_crc_file(&self.conf_path())? {
                let _: (Conf, u64) = serde_json::from_slice(&p).map_err(|e| StorageError::Corrupt(e.to_string()))?;
            }
            self.read_log()?;
            Ok(())
        })()
        .is_ok()
    }
}

pub struct FileFactory {
    pub root: PathBuf,
}

impl FileFactory {
    pub fn new(root: &Path) -> FileFactory {
        fs::create_dir_all(root).unwrap();
        FileFactory { root: root.to_path_buf() }
    }
    fn group_dir(&self, node: NodeId, group: GroupId) -> PathBuf {
        self.root.join(format!("node-{}/group-{}", node, group))
    }
}

impl StorageFactory for FileFactory {
    fn open(&mut self, node: NodeId, group: GroupId) -> Box<dyn Storage> {
        Box::new(FileStorage::open(&self.group_dir(node, group)).unwrap())
    }
    fn groups_for(&self, node: NodeId) -> Vec<GroupId> {
        let dir = self.root.join(format!("node-{}", node));
        let mut out = Vec::new();
        if let Ok(rd) = fs::read_dir(dir) {
            for ent in rd.flatten() {
                if let Some(name) = ent.file_name().to_str() {
                    if let Some(g) = name.strip_prefix("group-").and_then(|s| s.parse::<GroupId>().ok()) {
                        out.push(g);
                    }
                }
            }
        }
        out.sort_unstable();
        out
    }
    fn wipe(&mut self, node: NodeId, group: GroupId) {
        let _ = fs::remove_dir_all(self.group_dir(node, group));
    }
}
