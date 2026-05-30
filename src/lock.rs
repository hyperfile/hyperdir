//! S3-backed advisory file locks (the "global" lock mode).
//!
//! Two parts:
//!
//!   * A storage-agnostic byte-range lock engine ([`LockTable`]) with POSIX
//!     semantics (read/write compatibility, overlap, replace/split of the
//!     owner's own locks, group release, TTL expiry). Pure logic, unit-tested,
//!     no I/O.
//!   * A thin binding ([`HyperDir`] `fs_getlk` / `fs_setlk` / `fs_unlock_owner`
//!     / `fs_lock_renew`) that persists one table per file/dir in a single
//!     `<DIR|FILE/uuid>/_lock` object, updated with S3 conditional writes
//!     (create-once `If-None-Match:*`, CAS `If-Match:<etag>`) under an OCC
//!     retry loop — the same pattern as the compactor lease.
//!
//! The `_lock` object lives under the child's prefix, so it is reclaimed
//! automatically when the file/dir prefix is deleted (no GC change needed).
//!
//! Crash release is by TTL: every held lock carries an absolute expiry; an
//! expired entry is ignored and pruned on the next write. The holder must
//! renew (`fs_lock_renew`) well within the TTL. Blocking acquisition
//! (`F_SETLKW`) is not implemented here — the caller treats it as a single
//! non-blocking attempt.
//!
//! `owner` is an opaque single-line token (no tab/newline). The FUSE layer
//! folds its per-mount client id and the kernel `lock_owner` into it, so locks
//! are distinguishable across mounts.

use std::io::{Result, Error, ErrorKind};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::SdkBody;
use uuid::Uuid;
use crate::hyper::HyperDir;
use crate::layout::HyperDirLayout;
use crate::unix_now_ms;

/// Object name (under each child's prefix) holding its lock table.
const LOCK_OBJECT: &str = "_lock";
/// Default lock lease TTL. A holder that crashes frees its locks within this.
pub const DEFAULT_LOCK_TTL_MS: u64 = 60_000;
/// Max optimistic-concurrency retries for a `_lock` read-modify-write.
const LOCK_OCC_RETRIES: usize = 8;

/// A lock's mode. `Unlock` is only ever passed to [`LockTable::set`]; it is
/// never stored in an entry.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LockKind {
    Read,
    Write,
    Unlock,
}

impl LockKind {
    fn as_char(self) -> char {
        match self {
            LockKind::Read => 'R',
            LockKind::Write => 'W',
            LockKind::Unlock => 'U',
        }
    }
    fn from_char(c: char) -> Option<Self> {
        match c {
            'R' => Some(LockKind::Read),
            'W' => Some(LockKind::Write),
            _ => None,
        }
    }
}

/// An existing lock that conflicts with a requested one (returned by getlk /
/// a failed setlk). Carries enough to fill a FUSE getlk reply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conflict {
    pub start: u64,
    pub end: u64,
    pub kind: LockKind,
    pub pid: u32,
}

/// Outcome of a non-blocking setlk.
#[derive(Debug, PartialEq, Eq)]
pub enum SetLkOutcome {
    Granted,
    Conflict(Conflict),
}

#[derive(Clone, Debug)]
struct Entry {
    owner: String,
    kind: LockKind, // Read | Write only
    start: u64,
    end: u64, // inclusive
    pid: u32,
    expire_ms: i64,
}

/// The set of locks held on one file/dir. Ranges are inclusive `[start, end]`.
#[derive(Default, Debug)]
pub struct LockTable {
    entries: Vec<Entry>,
}

fn overlaps(a_start: u64, a_end: u64, b_start: u64, b_end: u64) -> bool {
    a_start <= b_end && b_start <= a_end
}

impl LockTable {
    fn purge_expired(&mut self, now_ms: i64) {
        self.entries.retain(|e| e.expire_ms > now_ms);
    }

    /// Return a conflicting lock held by a *different* owner, or `None`.
    /// `kind` is the requested mode (Read/Write). Expired entries are ignored.
    pub fn test(&self, owner: &str, kind: LockKind, start: u64, end: u64, now_ms: i64) -> Option<Conflict> {
        let want_write = kind == LockKind::Write;
        for e in &self.entries {
            if e.expire_ms <= now_ms || e.owner == owner {
                continue;
            }
            if overlaps(start, end, e.start, e.end) && (want_write || e.kind == LockKind::Write) {
                return Some(Conflict { start: e.start, end: e.end, kind: e.kind, pid: e.pid });
            }
        }
        None
    }

    /// Drop/split the given owner's own locks within `[start, end]`.
    fn remove_range(&mut self, owner: &str, start: u64, end: u64) {
        let mut out = Vec::with_capacity(self.entries.len());
        for e in std::mem::take(&mut self.entries) {
            if e.owner != owner || !overlaps(start, end, e.start, e.end) {
                out.push(e);
                continue;
            }
            if e.start < start {
                out.push(Entry { end: start - 1, ..e.clone() }); // start>=1 here
            }
            if e.end > end {
                out.push(Entry { start: end + 1, ..e.clone() }); // end<u64::MAX here
            }
            // middle overlap is dropped
        }
        self.entries = out;
    }

    /// Apply a lock/unlock for `owner`. On `Unlock`, removes the owner's locks
    /// in range. Otherwise, fails with the conflict if another owner holds an
    /// incompatible overlapping lock; on success replaces the owner's own
    /// locks in range with the new one.
    #[allow(clippy::too_many_arguments)]
    pub fn set(&mut self, owner: &str, kind: LockKind, start: u64, end: u64, pid: u32, expire_ms: i64, now_ms: i64)
        -> std::result::Result<(), Conflict>
    {
        self.purge_expired(now_ms);
        if kind == LockKind::Unlock {
            self.remove_range(owner, start, end);
            return Ok(());
        }
        if let Some(c) = self.test(owner, kind, start, end, now_ms) {
            return Err(c);
        }
        self.remove_range(owner, start, end);
        self.entries.push(Entry { owner: owner.to_string(), kind, start, end, pid, expire_ms });
        Ok(())
    }

    /// Remove all of an owner's locks (FUSE release/flush). Returns true if
    /// anything was removed.
    pub fn release_owner(&mut self, owner: &str) -> bool {
        let before = self.entries.len();
        self.entries.retain(|e| e.owner != owner);
        self.entries.len() != before
    }

    /// Refresh the expiry of all of an owner's locks. Returns true if the
    /// owner held any.
    pub fn renew(&mut self, owner: &str, expire_ms: i64) -> bool {
        let mut any = false;
        for e in &mut self.entries {
            if e.owner == owner {
                e.expire_ms = expire_ms;
                any = true;
            }
        }
        any
    }

    /// Encode as a line-based ASCII body: a `v1` header then one
    /// tab-separated record per entry. `owner` must contain no tab/newline.
    pub fn encode(&self) -> Vec<u8> {
        let mut s = String::from("v1\n");
        for e in &self.entries {
            s.push_str(&format!(
                "{}\t{}\t{}\t{}\t{}\t{}\n",
                e.owner, e.kind.as_char(), e.start, e.end, e.pid, e.expire_ms
            ));
        }
        s.into_bytes()
    }

    /// Decode [`encode`]'s format. Unparseable / unknown lines are skipped, so
    /// a partial or future-versioned body degrades to "fewer locks" rather
    /// than an error.
    pub fn decode(buf: &[u8]) -> Self {
        let mut t = LockTable::default();
        let Ok(s) = std::str::from_utf8(buf) else { return t; };
        for line in s.lines().skip(1) {
            // skip "v1" header
            let f: Vec<&str> = line.split('\t').collect();
            if f.len() != 6 {
                continue;
            }
            let (Some(kind), Ok(start), Ok(end), Ok(pid), Ok(expire_ms)) = (
                f[1].chars().next().and_then(LockKind::from_char),
                f[2].parse::<u64>(),
                f[3].parse::<u64>(),
                f[4].parse::<u32>(),
                f[5].parse::<i64>(),
            ) else {
                continue;
            };
            t.entries.push(Entry { owner: f[0].to_string(), kind, start, end, pid, expire_ms });
        }
        t
    }
}

/// S3 key of a child's lock-table object.
fn lock_key(layout: &HyperDirLayout, uuid: &Uuid, is_dir: bool) -> String {
    let base = if is_dir { layout.dir_key(uuid) } else { layout.file_key(uuid) };
    format!("{}/{}", base, LOCK_OBJECT)
}

/// Load the lock table and its current ETag (`None` if the object doesn't
/// exist yet).
async fn load_table(client: &Client, bucket: &str, key: &str) -> Result<(LockTable, Option<String>)> {
    match client.get_object().bucket(bucket).key(key).send().await {
        Ok(res) => {
            let etag = res.e_tag.clone().map(|t| t.replace('"', ""));
            let bytes = res.body.collect().await
                .map_err(|e| Error::other(format!("collect _lock s3://{}/{}: {}", bucket, key, e)))?
                .into_bytes();
            Ok((LockTable::decode(&bytes), etag))
        },
        Err(e) if e.as_service_error().is_some_and(|s| s.is_no_such_key()) => Ok((LockTable::default(), None)),
        Err(e) => Err(Error::other(format!("GetObject _lock s3://{}/{}: {}", bucket, key, e))),
    }
}

enum PutOutcome {
    Ok,
    Retry, // CAS miss (412/409): table changed under us
}

/// Conditional PUT of the table body: create-once when `etag` is `None`,
/// otherwise CAS on the ETag. A precondition failure is reported as `Retry`.
async fn cas_put(client: &Client, bucket: &str, key: &str, body: Vec<u8>, etag: Option<&str>) -> Result<PutOutcome> {
    let b = client.put_object().bucket(bucket).key(key).body(SdkBody::from(body).into());
    let b = match etag {
        Some(t) => b.if_match(t),
        None => b.if_none_match('*'),
    };
    match b.send().await {
        Ok(_) => Ok(PutOutcome::Ok),
        Err(sdk_err) => {
            if sdk_err.raw_response().map(|r| matches!(r.status().as_u16(), 412 | 409)).unwrap_or(false) {
                Ok(PutOutcome::Retry)
            } else {
                Err(Error::other(format!("PutObject _lock s3://{}/{}: {}", bucket, key, sdk_err)))
            }
        },
    }
}

impl HyperDir<'_> {
    /// Test for a conflicting lock without acquiring one (FUSE getlk).
    #[allow(clippy::too_many_arguments)]
    pub async fn fs_getlk(
        client: &Client, layout: &HyperDirLayout, bucket: &str, uuid: &Uuid, is_dir: bool,
        owner: &str, kind: LockKind, start: u64, end: u64,
    ) -> Result<Option<Conflict>> {
        let key = lock_key(layout, uuid, is_dir);
        let (table, _) = load_table(client, bucket, &key).await?;
        Ok(table.test(owner, kind, start, end, unix_now_ms()))
    }

    /// Acquire / change / release a byte-range lock (FUSE setlk, non-blocking).
    /// Returns `Granted` or `Conflict`; transient CAS races are retried.
    #[allow(clippy::too_many_arguments)]
    pub async fn fs_setlk(
        client: &Client, layout: &HyperDirLayout, bucket: &str, uuid: &Uuid, is_dir: bool,
        owner: &str, kind: LockKind, start: u64, end: u64, pid: u32, ttl_ms: u64,
    ) -> Result<SetLkOutcome> {
        let key = lock_key(layout, uuid, is_dir);
        for _ in 0..LOCK_OCC_RETRIES {
            let (mut table, etag) = load_table(client, bucket, &key).await?;
            // An unlock that finds no object has nothing to do.
            if kind == LockKind::Unlock && etag.is_none() {
                return Ok(SetLkOutcome::Granted);
            }
            let now = unix_now_ms();
            let expire = now.saturating_add(ttl_ms as i64);
            if let Err(c) = table.set(owner, kind, start, end, pid, expire, now) {
                return Ok(SetLkOutcome::Conflict(c));
            }
            match cas_put(client, bucket, &key, table.encode(), etag.as_deref()).await? {
                PutOutcome::Ok => return Ok(SetLkOutcome::Granted),
                PutOutcome::Retry => continue,
            }
        }
        Err(Error::new(ErrorKind::ResourceBusy, "fs_setlk: too many OCC conflicts"))
    }

    /// Release every lock held by `owner` on this file/dir (FUSE release/flush).
    pub async fn fs_unlock_owner(
        client: &Client, layout: &HyperDirLayout, bucket: &str, uuid: &Uuid, is_dir: bool, owner: &str,
    ) -> Result<()> {
        let key = lock_key(layout, uuid, is_dir);
        for _ in 0..LOCK_OCC_RETRIES {
            let (mut table, etag) = load_table(client, bucket, &key).await?;
            let Some(etag) = etag else { return Ok(()); }; // no object => nothing held
            if !table.release_owner(owner) {
                return Ok(()); // we held nothing
            }
            match cas_put(client, bucket, &key, table.encode(), Some(&etag)).await? {
                PutOutcome::Ok => return Ok(()),
                PutOutcome::Retry => continue,
            }
        }
        Err(Error::new(ErrorKind::ResourceBusy, "fs_unlock_owner: too many OCC conflicts"))
    }

    /// Refresh the TTL of every lock held by `owner` (maintenance renew).
    /// A no-op if the owner holds nothing.
    pub async fn fs_lock_renew(
        client: &Client, layout: &HyperDirLayout, bucket: &str, uuid: &Uuid, is_dir: bool,
        owner: &str, ttl_ms: u64,
    ) -> Result<()> {
        let key = lock_key(layout, uuid, is_dir);
        for _ in 0..LOCK_OCC_RETRIES {
            let (mut table, etag) = load_table(client, bucket, &key).await?;
            let Some(etag) = etag else { return Ok(()); };
            let expire = unix_now_ms().saturating_add(ttl_ms as i64);
            if !table.renew(owner, expire) {
                return Ok(());
            }
            match cas_put(client, bucket, &key, table.encode(), Some(&etag)).await? {
                PutOutcome::Ok => return Ok(()),
                PutOutcome::Retry => continue,
            }
        }
        Err(Error::new(ErrorKind::ResourceBusy, "fs_lock_renew: too many OCC conflicts"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FUTURE: i64 = i64::MAX;
    const NOW: i64 = 1_000;

    fn set(t: &mut LockTable, owner: &str, kind: LockKind, s: u64, e: u64) -> std::result::Result<(), Conflict> {
        t.set(owner, kind, s, e, 0, FUTURE, NOW)
    }

    #[test]
    fn read_read_compatible_write_excludes() {
        let mut t = LockTable::default();
        set(&mut t, "a", LockKind::Read, 0, 10).unwrap();
        // another reader on overlapping range is fine
        set(&mut t, "b", LockKind::Read, 5, 20).unwrap();
        // a writer overlapping a reader conflicts
        let c = set(&mut t, "c", LockKind::Write, 8, 9).unwrap_err();
        assert_eq!(c.kind, LockKind::Read);
        // a writer on a disjoint range is fine
        set(&mut t, "c", LockKind::Write, 100, 200).unwrap();
    }

    #[test]
    fn write_write_overlap_conflicts_disjoint_ok() {
        let mut t = LockTable::default();
        set(&mut t, "a", LockKind::Write, 0, 10).unwrap();
        assert!(set(&mut t, "b", LockKind::Write, 10, 10).is_err()); // touch at 10
        set(&mut t, "b", LockKind::Write, 11, 20).unwrap(); // adjacent, disjoint
    }

    #[test]
    fn same_owner_replaces_and_upgrades() {
        let mut t = LockTable::default();
        set(&mut t, "a", LockKind::Read, 0, 10).unwrap();
        // same owner upgrading its own range to write must not self-conflict
        set(&mut t, "a", LockKind::Write, 0, 10).unwrap();
        // and now another writer is excluded by the upgraded lock
        assert!(set(&mut t, "b", LockKind::Write, 0, 0).is_err());
    }

    #[test]
    fn unlock_splits_middle() {
        let mut t = LockTable::default();
        set(&mut t, "a", LockKind::Write, 0, 100).unwrap();
        // punch out the middle
        set(&mut t, "a", LockKind::Unlock, 40, 60).unwrap();
        // left and right remain locked, middle is free
        assert!(set(&mut t, "b", LockKind::Write, 50, 50).is_ok());
        assert!(set(&mut t, "c", LockKind::Write, 0, 0).is_err());
        assert!(set(&mut t, "d", LockKind::Write, 100, 100).is_err());
    }

    #[test]
    fn release_owner_and_renew() {
        let mut t = LockTable::default();
        set(&mut t, "a", LockKind::Write, 0, 10).unwrap();
        set(&mut t, "a", LockKind::Write, 20, 30).unwrap();
        assert!(t.renew("a", FUTURE));
        assert!(!t.renew("nobody", FUTURE));
        assert!(t.release_owner("a"));
        assert!(t.entries.is_empty());
        assert!(set(&mut t, "b", LockKind::Write, 0, 30).is_ok());
    }

    #[test]
    fn expired_locks_are_ignored() {
        let mut t = LockTable::default();
        // a's lock already expired (expire <= now)
        t.set("a", LockKind::Write, 0, 10, 0, NOW - 1, NOW).unwrap();
        // b can take it; the expired entry is pruned on this set
        t.set("b", LockKind::Write, 0, 10, 0, FUTURE, NOW).unwrap();
        assert!(t.test("c", LockKind::Read, 5, 5, NOW).is_some());
    }

    #[test]
    fn encode_decode_roundtrip() {
        let mut t = LockTable::default();
        set(&mut t, "client-1:42", LockKind::Read, 0, 10).unwrap();
        set(&mut t, "client-2:7", LockKind::Write, 100, u64::MAX).unwrap();
        let back = LockTable::decode(&t.encode());
        // semantics preserved: read region shared, write region exclusive
        assert!(back.test("x", LockKind::Read, 0, 10, NOW).is_none());
        assert!(back.test("x", LockKind::Write, 0, 0, NOW).is_some());
        assert!(back.test("x", LockKind::Read, 200, 200, NOW).is_some());
    }

    #[test]
    fn decode_tolerates_garbage() {
        assert!(LockTable::decode(b"").entries.is_empty());
        assert!(LockTable::decode(b"v1\ngarbage line\n\tbad\t\t\t\t").entries.is_empty());
    }
}
