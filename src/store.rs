use bytes::Bytes;
use hashbrown::HashMap;
use parking_lot::RwLock;
use std::collections::{HashSet, VecDeque};
use std::hash::{BuildHasher, Hasher};
use std::time::{Duration, Instant};

#[derive(Clone, Default)]
pub(crate) struct FxBuildHasher;

impl BuildHasher for FxBuildHasher {
    type Hasher = FxHasher;
    fn build_hasher(&self) -> FxHasher {
        FxHasher(0xcbf29ce484222325)
    }
}

pub(crate) struct FxHasher(u64);

impl Hasher for FxHasher {
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= b as u64;
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }
    fn finish(&self) -> u64 {
        self.0
    }
}

pub fn num_shards() -> usize {
    static SHARDS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *SHARDS.get_or_init(|| {
        std::env::var("LUX_SHARDS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                let cpus = std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(4);
                (cpus * 16).next_power_of_two().max(16).min(1024)
            })
    })
}
pub const MAX_SHARDS: usize = 1024;

pub enum StoreValue {
    Str(Bytes),
    List(VecDeque<Bytes>),
    Hash(HashMap<String, Bytes>),
    Set(HashSet<String>),
}

impl StoreValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            StoreValue::Str(_) => "string",
            StoreValue::List(_) => "list",
            StoreValue::Hash(_) => "hash",
            StoreValue::Set(_) => "set",
        }
    }
}

pub struct Entry {
    pub value: StoreValue,
    pub expires_at: Option<Instant>,
}

impl Entry {
    #[inline(always)]
    pub fn is_expired_at(&self, now: Instant) -> bool {
        self.expires_at.map_or(false, |exp| now > exp)
    }
}

#[repr(align(128))]
pub(crate) struct Shard {
    pub(crate) data: HashMap<String, Entry, FxBuildHasher>,
}

pub struct Store {
    shards: Box<[RwLock<Shard>]>,
}

#[inline(always)]
pub(crate) fn fx_hash(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[inline(always)]
fn key_str(key: &[u8]) -> &str {
    std::str::from_utf8(key).unwrap_or("")
}

#[inline(always)]
fn key_string(key: &[u8]) -> String {
    String::from_utf8_lossy(key).into_owned()
}

impl Store {
    pub fn new() -> Self {
        let n = num_shards();
        let shards: Vec<RwLock<Shard>> = (0..n)
            .map(|_| {
                RwLock::new(Shard {
                    data: HashMap::with_hasher(FxBuildHasher),
                })
            })
            .collect();
        Self {
            shards: shards.into_boxed_slice(),
        }
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    #[inline(always)]
    fn shard_index(&self, key: &[u8]) -> usize {
        (fx_hash(key) % self.shards.len() as u64) as usize
    }

    pub fn shard_for_key(&self, key: &[u8]) -> usize {
        self.shard_index(key)
    }

    pub fn lock_read_shard(&self, idx: usize) -> parking_lot::RwLockReadGuard<'_, Shard> {
        self.shards[idx].read()
    }

    pub fn lock_write_shard(&self, idx: usize) -> parking_lot::RwLockWriteGuard<'_, Shard> {
        self.shards[idx].write()
    }

    #[inline(always)]
    pub fn get_from_shard(data: &HashMap<String, Entry, FxBuildHasher>, key: &[u8], now: Instant) -> Option<Bytes> {
        let ks = key_str(key);
        data.get(ks).and_then(|entry| {
            if entry.is_expired_at(now) { return None; }
            match &entry.value {
                StoreValue::Str(s) => Some(s.clone()),
                _ => None,
            }
        })
    }

    #[inline(always)]
    pub fn get_and_write(data: &HashMap<String, Entry, FxBuildHasher>, key: &[u8], now: Instant, out: &mut bytes::BytesMut) {
        let ks = key_str(key);
        match data.get(ks) {
            Some(entry) if !entry.is_expired_at(now) => {
                if let StoreValue::Str(s) = &entry.value {
                    crate::resp::write_bulk_raw(out, s);
                } else {
                    crate::resp::write_null(out);
                }
            }
            _ => crate::resp::write_null(out),
        }
    }

    #[inline(always)]
    pub fn set_on_shard(data: &mut HashMap<String, Entry, FxBuildHasher>, key: &[u8], value: &[u8], ttl: Option<Duration>, now: Instant) {
        let expires_at = ttl.map(|d| now + d);
        let ks = key_string(key);
        if let Some(entry) = data.get_mut(&ks) {
            entry.value = StoreValue::Str(Bytes::copy_from_slice(value));
            entry.expires_at = expires_at;
        } else {
            data.insert(ks, Entry {
                value: StoreValue::Str(Bytes::copy_from_slice(value)),
                expires_at,
            });
        }
    }

    pub fn get(&self, key: &[u8], now: Instant) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        Self::get_from_shard(&shard.data, key, now)
    }

    pub fn get_entry_type(&self, key: &[u8], now: Instant) -> Option<&'static str> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => Some(entry.value.type_name()),
            _ => None,
        }
    }

    pub fn set(&self, key: &[u8], value: &[u8], ttl: Option<Duration>, now: Instant) {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        Self::set_on_shard(&mut shard.data, key, value, ttl, now);
    }

    pub fn set_nx(&self, key: &[u8], value: &[u8], now: Instant) -> bool {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let ks = key_str(key);
        if let Some(entry) = shard.data.get(ks) {
            if !entry.is_expired_at(now) {
                return false;
            }
        }
        shard.data.insert(
            key_string(key),
            Entry {
                value: StoreValue::Str(Bytes::copy_from_slice(value)),
                expires_at: None,
            },
        );
        true
    }

    pub fn get_set(&self, key: &[u8], value: &[u8], now: Instant) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let ks = key_str(key);
        let old = shard.data.get(ks).and_then(|e| {
            if e.is_expired_at(now) {
                None
            } else {
                match &e.value {
                    StoreValue::Str(s) => Some(s.clone()),
                    _ => None,
                }
            }
        });
        shard.data.insert(
            key_string(key),
            Entry {
                value: StoreValue::Str(Bytes::copy_from_slice(value)),
                expires_at: None,
            },
        );
        old
    }

    pub fn strlen(&self, key: &[u8], now: Instant) -> i64 {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::Str(s) => s.len() as i64,
                _ => 0,
            },
            _ => 0,
        }
    }

    pub fn del(&self, keys: &[&[u8]]) -> i64 {
        let mut count = 0i64;
        for key in keys {
            let idx = self.shard_index(key);
            let mut shard = self.shards[idx].write();
            if shard.data.remove(key_str(key)).is_some() {
                count += 1;
            }
        }
        count
    }

    pub fn exists(&self, keys: &[&[u8]], now: Instant) -> i64 {
        let mut count = 0i64;
        for key in keys {
            let idx = self.shard_index(key);
            let shard = self.shards[idx].read();
            if let Some(entry) = shard.data.get(key_str(key)) {
                if !entry.is_expired_at(now) {
                    count += 1;
                }
            }
        }
        count
    }

    pub fn incr(&self, key: &[u8], delta: i64, now: Instant) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let ks = key_str(key);
        let (current, expires_at) = match shard.data.get(ks) {
            Some(e) if !e.is_expired_at(now) => match &e.value {
                StoreValue::Str(s) => {
                    let s = std::str::from_utf8(s).map_err(|_| {
                        "ERR value is not an integer or out of range".to_string()
                    })?;
                    let n = s.parse::<i64>().map_err(|_| {
                        "ERR value is not an integer or out of range".to_string()
                    })?;
                    (n, e.expires_at)
                }
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            },
            _ => (0, None),
        };
        let new_val = current + delta;
        shard.data.insert(
            key_string(key),
            Entry {
                value: StoreValue::Str(Bytes::from(new_val.to_string())),
                expires_at,
            },
        );
        Ok(new_val)
    }

    pub fn append(&self, key: &[u8], value: &[u8], now: Instant) -> i64 {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let ks = key_str(key);
        if let Some(entry) = shard.data.get_mut(ks) {
            if !entry.is_expired_at(now) {
                if let StoreValue::Str(s) = &entry.value {
                    let mut new_val = Vec::with_capacity(s.len() + value.len());
                    new_val.extend_from_slice(s);
                    new_val.extend_from_slice(value);
                    let len = new_val.len() as i64;
                    entry.value = StoreValue::Str(Bytes::from(new_val));
                    return len;
                }
            }
        }
        let val = Bytes::copy_from_slice(value);
        let len = val.len() as i64;
        shard.data.insert(
            key_string(key),
            Entry {
                value: StoreValue::Str(val),
                expires_at: None,
            },
        );
        len
    }

    pub fn keys(&self, pattern: &[u8], now: Instant) -> Vec<String> {
        let pat_str = key_str(pattern);
        let matcher = GlobMatcher::new(pat_str);
        let mut result = Vec::new();
        for shard in self.shards.iter() {
            let shard = shard.read();
            for (k, e) in shard.data.iter() {
                if e.expires_at.map_or(true, |exp| now < exp) && matcher.matches(k) {
                    result.push(k.clone());
                }
            }
        }
        result
    }

    pub fn scan(&self, cursor: usize, pattern: &[u8], count: usize, now: Instant) -> (usize, Vec<String>) {
        let all_keys = self.keys(pattern, now);
        let start = cursor.min(all_keys.len());
        let end = (start + count).min(all_keys.len());
        let next_cursor = if end >= all_keys.len() { 0 } else { end };
        (next_cursor, all_keys[start..end].to_vec())
    }

    pub fn ttl(&self, key: &[u8], now: Instant) -> i64 {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            None => -2,
            Some(entry) => match entry.expires_at {
                None => -1,
                Some(exp) => {
                    if now > exp { -2 } else { exp.duration_since(now).as_secs() as i64 }
                }
            },
        }
    }

    pub fn pttl(&self, key: &[u8], now: Instant) -> i64 {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            None => -2,
            Some(entry) => match entry.expires_at {
                None => -1,
                Some(exp) => {
                    if now > exp { -2 } else { exp.duration_since(now).as_millis() as i64 }
                }
            },
        }
    }

    pub fn expire(&self, key: &[u8], seconds: u64, now: Instant) -> bool {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        if let Some(entry) = shard.data.get_mut(key_str(key)) {
            if !entry.is_expired_at(now) {
                entry.expires_at = Some(now + Duration::from_secs(seconds));
                return true;
            }
        }
        false
    }

    pub fn pexpire(&self, key: &[u8], millis: u64, now: Instant) -> bool {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        if let Some(entry) = shard.data.get_mut(key_str(key)) {
            if !entry.is_expired_at(now) {
                entry.expires_at = Some(now + Duration::from_millis(millis));
                return true;
            }
        }
        false
    }

    pub fn persist(&self, key: &[u8], now: Instant) -> bool {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        if let Some(entry) = shard.data.get_mut(key_str(key)) {
            if !entry.is_expired_at(now) && entry.expires_at.is_some() {
                entry.expires_at = None;
                return true;
            }
        }
        false
    }

    pub fn rename(&self, key: &[u8], new_key: &[u8], now: Instant) -> Result<(), String> {
        let old_idx = self.shard_index(key);
        let entry = {
            let mut shard = self.shards[old_idx].write();
            match shard.data.remove(key_str(key)) {
                Some(e) if !e.is_expired_at(now) => e,
                _ => return Err("ERR no such key".to_string()),
            }
        };
        let new_idx = self.shard_index(new_key);
        let mut shard = self.shards[new_idx].write();
        shard.data.insert(key_string(new_key), entry);
        Ok(())
    }

    pub fn dbsize(&self, now: Instant) -> i64 {
        let mut total = 0i64;
        for shard in self.shards.iter() {
            let shard = shard.read();
            total += shard.data.values().filter(|e| e.expires_at.map_or(true, |exp| now < exp)).count() as i64;
        }
        total
    }

    pub fn flushdb(&self) {
        for shard in self.shards.iter() {
            let mut shard = shard.write();
            shard.data.clear();
        }
    }

    pub fn lpush(&self, key: &[u8], values: &[&[u8]], now: Instant) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let ks = key_string(key);
        let entry = shard.data.entry(ks).or_insert_with(|| Entry {
            value: StoreValue::List(VecDeque::new()),
            expires_at: None,
        });
        if entry.is_expired_at(now) {
            entry.value = StoreValue::List(VecDeque::new());
            entry.expires_at = None;
        }
        match &mut entry.value {
            StoreValue::List(list) => {
                for v in values { list.push_front(Bytes::copy_from_slice(v)); }
                Ok(list.len() as i64)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    pub fn rpush(&self, key: &[u8], values: &[&[u8]], now: Instant) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let ks = key_string(key);
        let entry = shard.data.entry(ks).or_insert_with(|| Entry {
            value: StoreValue::List(VecDeque::new()),
            expires_at: None,
        });
        if entry.is_expired_at(now) {
            entry.value = StoreValue::List(VecDeque::new());
            entry.expires_at = None;
        }
        match &mut entry.value {
            StoreValue::List(list) => {
                for v in values { list.push_back(Bytes::copy_from_slice(v)); }
                Ok(list.len() as i64)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    pub fn lpop(&self, key: &[u8], now: Instant) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        match shard.data.get_mut(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &mut entry.value {
                StoreValue::List(list) => list.pop_front(),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn rpop(&self, key: &[u8], now: Instant) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        match shard.data.get_mut(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &mut entry.value {
                StoreValue::List(list) => list.pop_back(),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn llen(&self, key: &[u8], now: Instant) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::List(list) => Ok(list.len() as i64),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(0),
        }
    }

    pub fn lrange(&self, key: &[u8], start: i64, stop: i64, now: Instant) -> Result<Vec<Bytes>, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::List(list) => {
                    let len = list.len() as i64;
                    let s = if start < 0 { (len + start).max(0) as usize } else { start.min(len) as usize };
                    let e = if stop < 0 { (len + stop + 1).max(0) as usize } else { (stop + 1).min(len) as usize };
                    if s >= e { Ok(vec![]) } else { Ok(list.iter().skip(s).take(e - s).cloned().collect()) }
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(vec![]),
        }
    }

    pub fn lindex(&self, key: &[u8], index: i64, now: Instant) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::List(list) => {
                    let i = if index < 0 { (list.len() as i64 + index) as usize } else { index as usize };
                    list.get(i).cloned()
                }
                _ => None,
            },
            _ => None,
        }
    }

    pub fn hset(&self, key: &[u8], pairs: &[(&[u8], &[u8])], now: Instant) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let ks = key_string(key);
        let entry = shard.data.entry(ks).or_insert_with(|| Entry {
            value: StoreValue::Hash(HashMap::new()),
            expires_at: None,
        });
        if entry.is_expired_at(now) {
            entry.value = StoreValue::Hash(HashMap::new());
            entry.expires_at = None;
        }
        match &mut entry.value {
            StoreValue::Hash(map) => {
                let mut added = 0i64;
                for (field, value) in pairs {
                    if map.insert(key_string(field), Bytes::copy_from_slice(value)).is_none() { added += 1; }
                }
                Ok(added)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    pub fn hget(&self, key: &[u8], field: &[u8], now: Instant) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::Hash(map) => map.get(key_str(field)).cloned(),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn hmget(&self, key: &[u8], fields: &[&[u8]], now: Instant) -> Vec<Option<Bytes>> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::Hash(map) => fields.iter().map(|f| map.get(key_str(f)).cloned()).collect(),
                _ => fields.iter().map(|_| None).collect(),
            },
            _ => fields.iter().map(|_| None).collect(),
        }
    }

    pub fn hdel(&self, key: &[u8], fields: &[&[u8]], now: Instant) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        match shard.data.get_mut(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &mut entry.value {
                StoreValue::Hash(map) => {
                    let mut removed = 0i64;
                    for f in fields { if map.remove(key_str(f)).is_some() { removed += 1; } }
                    Ok(removed)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(0),
        }
    }

    pub fn hgetall(&self, key: &[u8], now: Instant) -> Result<Vec<(String, Bytes)>, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::Hash(map) => Ok(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(vec![]),
        }
    }

    pub fn hkeys(&self, key: &[u8], now: Instant) -> Result<Vec<String>, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::Hash(map) => Ok(map.keys().cloned().collect()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(vec![]),
        }
    }

    pub fn hvals(&self, key: &[u8], now: Instant) -> Result<Vec<Bytes>, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::Hash(map) => Ok(map.values().cloned().collect()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(vec![]),
        }
    }

    pub fn hlen(&self, key: &[u8], now: Instant) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::Hash(map) => Ok(map.len() as i64),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(0),
        }
    }

    pub fn hexists(&self, key: &[u8], field: &[u8], now: Instant) -> Result<bool, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::Hash(map) => Ok(map.contains_key(key_str(field))),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(false),
        }
    }

    pub fn hincrby(&self, key: &[u8], field: &[u8], delta: i64, now: Instant) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let ks = key_string(key);
        let entry = shard.data.entry(ks).or_insert_with(|| Entry {
            value: StoreValue::Hash(HashMap::new()),
            expires_at: None,
        });
        if entry.is_expired_at(now) {
            entry.value = StoreValue::Hash(HashMap::new());
            entry.expires_at = None;
        }
        match &mut entry.value {
            StoreValue::Hash(map) => {
                let fs = key_str(field);
                let current: i64 = map.get(fs)
                    .map(|v| std::str::from_utf8(v).ok()
                        .and_then(|s| s.parse::<i64>().ok())
                        .ok_or_else(|| "ERR hash value is not an integer".to_string()))
                    .transpose()?
                    .unwrap_or(0);
                let new_val = current + delta;
                map.insert(fs.to_string(), Bytes::from(new_val.to_string()));
                Ok(new_val)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    pub fn sadd(&self, key: &[u8], members: &[&[u8]], now: Instant) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let ks = key_string(key);
        let entry = shard.data.entry(ks).or_insert_with(|| Entry {
            value: StoreValue::Set(HashSet::new()),
            expires_at: None,
        });
        if entry.is_expired_at(now) {
            entry.value = StoreValue::Set(HashSet::new());
            entry.expires_at = None;
        }
        match &mut entry.value {
            StoreValue::Set(set) => {
                let mut added = 0i64;
                for m in members { if set.insert(key_string(m)) { added += 1; } }
                Ok(added)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    pub fn srem(&self, key: &[u8], members: &[&[u8]], now: Instant) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        match shard.data.get_mut(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &mut entry.value {
                StoreValue::Set(set) => {
                    let mut removed = 0i64;
                    for m in members { if set.remove(key_str(m)) { removed += 1; } }
                    Ok(removed)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(0),
        }
    }

    pub fn smembers(&self, key: &[u8], now: Instant) -> Result<Vec<String>, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::Set(set) => Ok(set.iter().cloned().collect()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(vec![]),
        }
    }

    pub fn sismember(&self, key: &[u8], member: &[u8], now: Instant) -> Result<bool, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::Set(set) => Ok(set.contains(key_str(member))),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(false),
        }
    }

    pub fn scard(&self, key: &[u8], now: Instant) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::Set(set) => Ok(set.len() as i64),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(0),
        }
    }

    fn collect_set(&self, key: &[u8], now: Instant) -> Result<HashSet<String>, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::Set(set) => Ok(set.clone()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(HashSet::new()),
        }
    }

    pub fn sunion(&self, keys: &[&[u8]], now: Instant) -> Result<Vec<String>, String> {
        let mut result = HashSet::new();
        for key in keys { result.extend(self.collect_set(key, now)?); }
        Ok(result.into_iter().collect())
    }

    pub fn sinter(&self, keys: &[&[u8]], now: Instant) -> Result<Vec<String>, String> {
        if keys.is_empty() { return Ok(vec![]); }
        let mut result = self.collect_set(keys[0], now)?;
        for key in &keys[1..] { let set = self.collect_set(key, now)?; result.retain(|m| set.contains(m)); }
        Ok(result.into_iter().collect())
    }

    pub fn sdiff(&self, keys: &[&[u8]], now: Instant) -> Result<Vec<String>, String> {
        if keys.is_empty() { return Ok(vec![]); }
        let mut result = self.collect_set(keys[0], now)?;
        for key in &keys[1..] { let set = self.collect_set(key, now)?; result.retain(|m| !set.contains(m)); }
        Ok(result.into_iter().collect())
    }

    pub fn approximate_memory(&self) -> usize {
        let now = Instant::now();
        let mut total = 0usize;
        for shard in self.shards.iter() {
            let shard = shard.read();
            for (key, entry) in shard.data.iter() {
                if entry.is_expired_at(now) { continue; }
                total += key.len() + 64;
                total += match &entry.value {
                    StoreValue::Str(s) => s.len(),
                    StoreValue::List(l) => l.iter().map(|b| b.len() + 32).sum(),
                    StoreValue::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len() + 64).sum(),
                    StoreValue::Set(s) => s.iter().map(|m| m.len() + 32).sum(),
                };
            }
        }
        total
    }

    pub fn dump_all(&self, now: Instant) -> Vec<DumpEntry> {
        let mut entries = Vec::new();
        for shard in self.shards.iter() {
            let shard = shard.read();
            for (key, entry) in shard.data.iter() {
                if entry.is_expired_at(now) { continue; }
                let ttl_ms = entry.expires_at.map(|exp| exp.duration_since(now).as_millis() as i64).unwrap_or(0);
                entries.push(DumpEntry {
                    key: key.clone(),
                    value: match &entry.value {
                        StoreValue::Str(s) => DumpValue::Str(String::from_utf8_lossy(s).into_owned()),
                        StoreValue::List(l) => DumpValue::List(l.iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect()),
                        StoreValue::Hash(h) => DumpValue::Hash(h.iter().map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).into_owned())).collect()),
                        StoreValue::Set(s) => DumpValue::Set(s.iter().cloned().collect()),
                    },
                    ttl_ms,
                });
            }
        }
        entries
    }

    pub fn load_entry(&self, key: String, value: DumpValue, ttl: Option<Duration>) {
        let idx = self.shard_index(key.as_bytes());
        let mut shard = self.shards[idx].write();
        let store_value = match value {
            DumpValue::Str(s) => StoreValue::Str(Bytes::from(s)),
            DumpValue::List(l) => StoreValue::List(l.into_iter().map(Bytes::from).collect()),
            DumpValue::Hash(h) => StoreValue::Hash(h.into_iter().map(|(k, v)| (k, Bytes::from(v))).collect()),
            DumpValue::Set(s) => StoreValue::Set(s.into_iter().collect()),
        };
        let expires_at = ttl.map(|d| Instant::now() + d);
        shard.data.insert(key, Entry { value: store_value, expires_at });
    }

    pub fn getdel(&self, key: &[u8], now: Instant) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let ks = key_str(key);
        match shard.data.get(ks) {
            Some(entry) if !entry.is_expired_at(now) => {
                if let StoreValue::Str(s) = &entry.value {
                    let val = s.clone();
                    shard.data.remove(ks);
                    Some(val)
                } else { None }
            }
            _ => None,
        }
    }

    pub fn getex(&self, key: &[u8], ttl: Option<Duration>, persist: bool, now: Instant) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let ks = key_str(key);
        match shard.data.get_mut(ks) {
            Some(entry) if !entry.is_expired_at(now) => {
                if persist { entry.expires_at = None; }
                else if let Some(d) = ttl { entry.expires_at = Some(now + d); }
                match &entry.value {
                    StoreValue::Str(s) => Some(s.clone()),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    pub fn getrange(&self, key: &[u8], start: i64, end: i64, now: Instant) -> Bytes {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => {
                if let StoreValue::Str(s) = &entry.value {
                    let len = s.len() as i64;
                    let s_i = if start < 0 { (len + start).max(0) as usize } else { start.min(len) as usize };
                    let e_i = if end < 0 { (len + end).max(-1) as usize + 1 } else { (end + 1).min(len) as usize };
                    if s_i >= e_i { Bytes::new() } else { s.slice(s_i..e_i) }
                } else { Bytes::new() }
            }
            _ => Bytes::new(),
        }
    }

    pub fn setrange(&self, key: &[u8], offset: usize, value: &[u8], now: Instant) -> i64 {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let ks = key_string(key);
        let entry = shard.data.entry(ks).or_insert_with(|| Entry {
            value: StoreValue::Str(Bytes::new()),
            expires_at: None,
        });
        if entry.is_expired_at(now) {
            entry.value = StoreValue::Str(Bytes::new());
            entry.expires_at = None;
        }
        if let StoreValue::Str(s) = &entry.value {
            let mut buf = s.to_vec();
            let needed = offset + value.len();
            if buf.len() < needed { buf.resize(needed, 0); }
            buf[offset..offset + value.len()].copy_from_slice(value);
            let len = buf.len() as i64;
            entry.value = StoreValue::Str(Bytes::from(buf));
            len
        } else { 0 }
    }

    pub fn msetnx(&self, pairs: &[(&[u8], &[u8])], now: Instant) -> bool {
        for (key, _) in pairs {
            if self.get(key, now).is_some() { return false; }
        }
        for (key, value) in pairs {
            self.set(key, value, None, now);
        }
        true
    }

    pub fn unlink(&self, keys: &[&[u8]]) -> i64 {
        self.del(keys)
    }

    pub fn expireat(&self, key: &[u8], timestamp: u64, now: Instant) -> bool {
        let target = std::time::UNIX_EPOCH + Duration::from_secs(timestamp);
        let now_sys = std::time::SystemTime::now();
        if target <= now_sys { return false; }
        let dur = target.duration_since(now_sys).unwrap_or(Duration::ZERO);
        self.expire(key, dur.as_secs(), now)
    }

    pub fn pexpireat(&self, key: &[u8], timestamp_ms: u64, now: Instant) -> bool {
        let target = std::time::UNIX_EPOCH + Duration::from_millis(timestamp_ms);
        let now_sys = std::time::SystemTime::now();
        if target <= now_sys { return false; }
        let dur = target.duration_since(now_sys).unwrap_or(Duration::ZERO);
        self.pexpire(key, dur.as_millis() as u64, now)
    }

    pub fn expiretime(&self, key: &[u8], now: Instant) -> i64 {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            None => -2,
            Some(entry) if entry.is_expired_at(now) => -2,
            Some(entry) => match entry.expires_at {
                None => -1,
                Some(exp) => {
                    let remaining = exp.duration_since(now);
                    let now_unix = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                    (now_unix.as_secs() + remaining.as_secs()) as i64
                }
            },
        }
    }

    pub fn pexpiretime(&self, key: &[u8], now: Instant) -> i64 {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            None => -2,
            Some(entry) if entry.is_expired_at(now) => -2,
            Some(entry) => match entry.expires_at {
                None => -1,
                Some(exp) => {
                    let remaining = exp.duration_since(now);
                    let now_unix = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                    (now_unix.as_millis() + remaining.as_millis()) as i64
                }
            },
        }
    }

    pub fn lset(&self, key: &[u8], index: i64, value: &[u8], now: Instant) -> Result<(), String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        match shard.data.get_mut(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &mut entry.value {
                StoreValue::List(list) => {
                    let i = if index < 0 { (list.len() as i64 + index) as usize } else { index as usize };
                    if i >= list.len() { return Err("ERR index out of range".to_string()); }
                    list[i] = Bytes::copy_from_slice(value);
                    Ok(())
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Err("ERR no such key".to_string()),
        }
    }

    pub fn linsert(&self, key: &[u8], before: bool, pivot: &[u8], value: &[u8], now: Instant) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        match shard.data.get_mut(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &mut entry.value {
                StoreValue::List(list) => {
                    if let Some(pos) = list.iter().position(|v| v.as_ref() == pivot) {
                        let insert_at = if before { pos } else { pos + 1 };
                        list.insert(insert_at, Bytes::copy_from_slice(value));
                        Ok(list.len() as i64)
                    } else { Ok(-1) }
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(0),
        }
    }

    pub fn lrem(&self, key: &[u8], count: i64, value: &[u8], now: Instant) -> Result<i64, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        match shard.data.get_mut(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &mut entry.value {
                StoreValue::List(list) => {
                    let mut removed = 0i64;
                    if count > 0 {
                        let mut i = 0;
                        while i < list.len() && removed < count {
                            if list[i].as_ref() == value { list.remove(i); removed += 1; }
                            else { i += 1; }
                        }
                    } else if count < 0 {
                        let mut i = list.len();
                        while i > 0 && removed < count.abs() {
                            i -= 1;
                            if list[i].as_ref() == value { list.remove(i); removed += 1; }
                        }
                    } else {
                        list.retain(|v| { if v.as_ref() == value { removed += 1; false } else { true } });
                    }
                    Ok(removed)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(0),
        }
    }

    pub fn ltrim(&self, key: &[u8], start: i64, stop: i64, now: Instant) -> Result<(), String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        match shard.data.get_mut(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &mut entry.value {
                StoreValue::List(list) => {
                    let len = list.len() as i64;
                    let s = if start < 0 { (len + start).max(0) as usize } else { start.min(len) as usize };
                    let e = if stop < 0 { (len + stop + 1).max(0) as usize } else { (stop + 1).min(len) as usize };
                    if s >= e { list.clear(); } else {
                        let trimmed: VecDeque<Bytes> = list.drain(s..e).collect();
                        *list = trimmed;
                    }
                    Ok(())
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(()),
        }
    }

    pub fn lpushx(&self, key: &[u8], values: &[&[u8]], now: Instant) -> i64 {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        match shard.data.get_mut(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &mut entry.value {
                StoreValue::List(list) => {
                    for v in values { list.push_front(Bytes::copy_from_slice(v)); }
                    list.len() as i64
                }
                _ => 0,
            },
            _ => 0,
        }
    }

    pub fn rpushx(&self, key: &[u8], values: &[&[u8]], now: Instant) -> i64 {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        match shard.data.get_mut(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &mut entry.value {
                StoreValue::List(list) => {
                    for v in values { list.push_back(Bytes::copy_from_slice(v)); }
                    list.len() as i64
                }
                _ => 0,
            },
            _ => 0,
        }
    }

    pub fn lmove(&self, src: &[u8], dst: &[u8], from_left: bool, to_left: bool, now: Instant) -> Option<Bytes> {
        let src_idx = self.shard_index(src);
        let val = {
            let mut shard = self.shards[src_idx].write();
            match shard.data.get_mut(key_str(src)) {
                Some(entry) if !entry.is_expired_at(now) => match &mut entry.value {
                    StoreValue::List(list) => {
                        if from_left { list.pop_front() } else { list.pop_back() }
                    }
                    _ => None,
                },
                _ => None,
            }
        };
        if let Some(v) = &val {
            let dst_idx = self.shard_index(dst);
            let mut shard = self.shards[dst_idx].write();
            let ks = key_string(dst);
            let entry = shard.data.entry(ks).or_insert_with(|| Entry {
                value: StoreValue::List(VecDeque::new()),
                expires_at: None,
            });
            if entry.is_expired_at(now) {
                entry.value = StoreValue::List(VecDeque::new());
                entry.expires_at = None;
            }
            if let StoreValue::List(list) = &mut entry.value {
                if to_left { list.push_front(v.clone()); } else { list.push_back(v.clone()); }
            }
        }
        val
    }

    pub fn hsetnx(&self, key: &[u8], field: &[u8], value: &[u8], now: Instant) -> Result<bool, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let ks = key_string(key);
        let entry = shard.data.entry(ks).or_insert_with(|| Entry {
            value: StoreValue::Hash(HashMap::new()),
            expires_at: None,
        });
        if entry.is_expired_at(now) {
            entry.value = StoreValue::Hash(HashMap::new());
            entry.expires_at = None;
        }
        match &mut entry.value {
            StoreValue::Hash(map) => {
                let fs = key_str(field);
                if map.contains_key(fs) { Ok(false) }
                else { map.insert(fs.to_string(), Bytes::copy_from_slice(value)); Ok(true) }
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    pub fn hincrbyfloat(&self, key: &[u8], field: &[u8], delta: f64, now: Instant) -> Result<String, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        let ks = key_string(key);
        let entry = shard.data.entry(ks).or_insert_with(|| Entry {
            value: StoreValue::Hash(HashMap::new()),
            expires_at: None,
        });
        if entry.is_expired_at(now) {
            entry.value = StoreValue::Hash(HashMap::new());
            entry.expires_at = None;
        }
        match &mut entry.value {
            StoreValue::Hash(map) => {
                let fs = key_str(field);
                let current: f64 = map.get(fs)
                    .map(|v| std::str::from_utf8(v).ok()
                        .and_then(|s| s.parse::<f64>().ok())
                        .ok_or_else(|| "ERR hash value is not a valid float".to_string()))
                    .transpose()?
                    .unwrap_or(0.0);
                let new_val = current + delta;
                let s = format!("{}", new_val);
                map.insert(fs.to_string(), Bytes::from(s.clone()));
                Ok(s)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    pub fn hstrlen(&self, key: &[u8], field: &[u8], now: Instant) -> i64 {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::Hash(map) => map.get(key_str(field)).map(|v| v.len() as i64).unwrap_or(0),
                _ => 0,
            },
            _ => 0,
        }
    }

    pub fn spop(&self, key: &[u8], count: usize, now: Instant) -> Result<Vec<String>, String> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        match shard.data.get_mut(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &mut entry.value {
                StoreValue::Set(set) => {
                    let mut result = Vec::new();
                    for _ in 0..count {
                        if set.is_empty() { break; }
                        let member = set.iter().next().unwrap().clone();
                        set.remove(&member);
                        result.push(member);
                    }
                    Ok(result)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(vec![]),
        }
    }

    pub fn srandmember(&self, key: &[u8], count: i64, now: Instant) -> Result<Vec<String>, String> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::Set(set) => {
                    if count == 0 || set.is_empty() { return Ok(vec![]); }
                    let members: Vec<&String> = set.iter().collect();
                    let abs_count = count.unsigned_abs() as usize;
                    let result: Vec<String> = members.iter().take(abs_count).map(|s| (*s).clone()).collect();
                    Ok(result)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            },
            _ => Ok(vec![]),
        }
    }

    pub fn smove(&self, src: &[u8], dst: &[u8], member: &[u8], now: Instant) -> Result<bool, String> {
        let src_idx = self.shard_index(src);
        let removed = {
            let mut shard = self.shards[src_idx].write();
            match shard.data.get_mut(key_str(src)) {
                Some(entry) if !entry.is_expired_at(now) => match &mut entry.value {
                    StoreValue::Set(set) => set.remove(key_str(member)),
                    _ => return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                },
                _ => false,
            }
        };
        if !removed { return Ok(false); }
        let dst_idx = self.shard_index(dst);
        let mut shard = self.shards[dst_idx].write();
        let ks = key_string(dst);
        let entry = shard.data.entry(ks).or_insert_with(|| Entry {
            value: StoreValue::Set(HashSet::new()),
            expires_at: None,
        });
        if entry.is_expired_at(now) {
            entry.value = StoreValue::Set(HashSet::new());
            entry.expires_at = None;
        }
        match &mut entry.value {
            StoreValue::Set(set) => { set.insert(key_string(member)); Ok(true) }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    pub fn smismember(&self, key: &[u8], members: &[&[u8]], now: Instant) -> Vec<bool> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        match shard.data.get(key_str(key)) {
            Some(entry) if !entry.is_expired_at(now) => match &entry.value {
                StoreValue::Set(set) => members.iter().map(|m| set.contains(key_str(m))).collect(),
                _ => members.iter().map(|_| false).collect(),
            },
            _ => members.iter().map(|_| false).collect(),
        }
    }

    pub fn sdiffstore(&self, dst: &[u8], keys: &[&[u8]], now: Instant) -> Result<i64, String> {
        let result = self.sdiff(keys, now)?;
        let members: Vec<&[u8]> = result.iter().map(|s| s.as_bytes()).collect();
        self.del(&[dst]);
        if !members.is_empty() {
            let member_refs: Vec<&[u8]> = members;
            self.sadd(dst, &member_refs, now)?;
        }
        Ok(result.len() as i64)
    }

    pub fn sinterstore(&self, dst: &[u8], keys: &[&[u8]], now: Instant) -> Result<i64, String> {
        let result = self.sinter(keys, now)?;
        let members: Vec<&[u8]> = result.iter().map(|s| s.as_bytes()).collect();
        self.del(&[dst]);
        if !members.is_empty() {
            self.sadd(dst, &members, now)?;
        }
        Ok(result.len() as i64)
    }

    pub fn sunionstore(&self, dst: &[u8], keys: &[&[u8]], now: Instant) -> Result<i64, String> {
        let result = self.sunion(keys, now)?;
        let members: Vec<&[u8]> = result.iter().map(|s| s.as_bytes()).collect();
        self.del(&[dst]);
        if !members.is_empty() {
            self.sadd(dst, &members, now)?;
        }
        Ok(result.len() as i64)
    }

    pub fn expire_sweep(&self, now: Instant) {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        now.hash(&mut hasher);
        let seed = hasher.finish() as usize;

        for (i, shard) in self.shards.iter().enumerate() {
            let should_check = {
                let shard = shard.read();
                !shard.data.is_empty()
            };
            if !should_check { continue; }

            let mut shard = shard.write();
            let keys: Vec<String> = shard.data.keys()
                .enumerate()
                .filter(|(j, _)| (*j + seed + i) % 5 == 0)
                .take(20)
                .map(|(_, k)| k.clone())
                .collect();
            for key in keys {
                if let Some(entry) = shard.data.get(&key) {
                    if entry.is_expired_at(now) {
                        shard.data.remove(&key);
                    }
                }
            }
        }
    }
}

pub enum DumpValue {
    Str(String),
    List(Vec<String>),
    Hash(Vec<(String, String)>),
    Set(Vec<String>),
}

pub struct DumpEntry {
    pub key: String,
    pub value: DumpValue,
    pub ttl_ms: i64,
}

struct GlobMatcher {
    pattern: Vec<char>,
}

impl GlobMatcher {
    fn new(pattern: &str) -> Self {
        Self { pattern: pattern.chars().collect() }
    }

    fn matches(&self, s: &str) -> bool {
        if self.pattern.len() == 1 && self.pattern[0] == '*' { return true; }
        let s: Vec<char> = s.chars().collect();
        Self::do_match(&self.pattern, &s, 0, 0)
    }

    fn do_match(pattern: &[char], s: &[char], pi: usize, si: usize) -> bool {
        if pi == pattern.len() && si == s.len() { return true; }
        if pi == pattern.len() { return false; }
        if pattern[pi] == '*' {
            for i in si..=s.len() { if Self::do_match(pattern, s, pi + 1, i) { return true; } }
            return false;
        }
        if si == s.len() { return false; }
        if pattern[pi] == '?' || pattern[pi] == s[si] { return Self::do_match(pattern, s, pi + 1, si + 1); }
        false
    }
}
// appended at module level - will need to be moved inside impl Store
