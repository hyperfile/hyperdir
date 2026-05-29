use std::io::{Result, Error, ErrorKind};
use std::time::Duration;
use log::{debug, warn};
use aws_sdk_s3::Client;
use hyperfile::file::flags::{FileFlags, HyperFileFlags};
use hyperfile::file::mode::{FileMode, HyperFileMode};
use hyperfile::config::HyperFileConfigBuilder;
use hyperfile::config::HyperFileRuntimeConfig;
use hyperfile::staging::{Staging, config::StagingConfig, s3::S3Staging, StagingIntercept};
use hyperfile::file::HyperTrait;
use hyperfile::ondisk::InodeRaw;
use crate::hyper::HyperDir;
use crate::file::{EntryNameHash, DirFileEntry, CompactStats};
use crate::interceptor::ScatterFirstInterceptor;
use crate::{
    DirStaging, DirScatterInodeOp,
    build_tombstone_body, parse_tombstone_body, unix_now_ms,
};

impl<'a> HyperDir<'a>
{
    pub async fn fs_create(client: &Client, uri: &str, flags: FileFlags, mode: FileMode) -> Result<Self>
    {
        debug!("fs_create - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        let m = HyperFileMode::from_mode(mode);
        return Self::create(client.clone(), file_config, f, m).await;
    }

    pub async fn fs_create_with_interceptor(client: &Client, uri: &str, flags: FileFlags, mode: FileMode, interceptor: impl StagingIntercept<S3Staging> + 'static) -> Result<Self>
    {
        debug!("fs_create_with_interceptor - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        let m = HyperFileMode::from_mode(mode);
        return Self::create_with_interceptor(client.clone(), file_config, f, m, interceptor).await;
    }

    /// Convenience over `fs_create_with_interceptor` that installs the
    /// default scatter-first interceptor (`ScatterFirstInterceptor`).
    ///
    /// Use this when you want the standard hyperdir commit semantics:
    /// every flush of this file's inode first emits a scatter object into
    /// the parent directory's `!/` prefix as a conditional PUT, which is
    /// the durable commit point of the change. The subsequent file inode
    /// PUT is best-effort replication.
    pub async fn fs_create_default(client: &Client, uri: &str, flags: FileFlags, mode: FileMode) -> Result<Self>
    {
        debug!("fs_create_default - uri: {}, flags: {}", uri, flags);
        Self::fs_create_with_interceptor(client, uri, flags, mode, ScatterFirstInterceptor::new()).await
    }

    pub async fn fs_open(client: &Client, uri: &str, flags: FileFlags) -> Result<Self>
    {
        debug!("fs_open - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        return Self::open(client.clone(), file_config, f).await;
    }

    pub async fn fs_open_opt(client: &Client, uri: &str, flags: FileFlags, runtime_config: &HyperFileRuntimeConfig) -> Result<Self>
    {
        debug!("fs_open_opt - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .with_runtime_config(runtime_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        return Self::open(client.clone(), file_config, f).await;
    }

    pub async fn fs_open_or_create_with_default_opt(client: &Client, uri: &str, flags: FileFlags, mode: FileMode) -> Result<Self>
    {
        debug!("fs_open_or_create - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
            .with_staging_config(&staging_config)
            .build();
        let f = HyperFileFlags::from_flags(flags);
        let m = HyperFileMode::from_mode(mode);
        return Self::do_open_or_create(client.clone(), file_config, f, m, true).await;
    }

    /// Delete a file by emitting a tombstone scatter to its parent directory.
    ///
    /// This method does **not** physically delete the child's S3 prefix. It
    /// opens the child briefly to capture its current `InodeRaw`, builds a
    /// tombstone body (`TombstoneHeader || InodeRaw`), and emits a `Delete`
    /// scatter into the parent directory's `!/` namespace as a conditional
    /// PUT (`If-None-Match: *`). The next `compact` on the parent removes
    /// the directory-entry mapping but keeps the tombstone scatter in S3 as
    /// the authoritative deletion record. Physical reclamation of the child
    /// prefix is the job of [`fs_gc`], which honours `retention`.
    ///
    /// `retention`:
    /// - `None` => `retention_until_unix_ms = 0`. The next `fs_gc` may
    ///   immediately reclaim the child storage. From the user's perspective
    ///   this is "delete now (physical reclamation is asynchronous)".
    /// - `Some(d)` => the child storage is preserved for at least `d` past
    ///   `now`. Until the retention expires `fs_gc` will leave the child
    ///   prefix alone, which is what makes a future undelete possible.
    pub async fn fs_unlink(client: &Client, uri: &str, retention: Option<Duration>) -> Result<()>
    {
        debug!("fs_unlink - uri: {}, retention: {:?}", uri, retention);

        // Open the child staging just long enough to read its current inode.
        // We do not modify the child here; the prefix is left in place to be
        // reclaimed by `fs_gc` after the retention window.
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let runtime_config = HyperFileRuntimeConfig::default();
        let child_staging = S3Staging::from(client, staging_config, runtime_config).await?;

        let mut raw_inode: InodeRaw = unsafe {
            std::mem::MaybeUninit::zeroed().assume_init()
        };
        let _ = child_staging.load_inode(raw_inode.as_mut_u8_slice()).await?;

        // Compute retention deadline. 0 means "no retention" -> GC may run
        // immediately on the next pass.
        let now_ms = unix_now_ms();
        let retention_until_unix_ms = match retention {
            None => 0,
            Some(d) => {
                let dms = i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
                now_ms.saturating_add(dms)
            },
        };

        // Body = TombstoneHeader || raw inode bytes.
        let body = build_tombstone_body(now_ms, retention_until_unix_ms, raw_inode.as_u8_slice());

        // Derive the parent staging from the child path (current path-based
        // addressing; will switch to an explicit parent context once UUID
        // addressing lands) and commit the tombstone there.
        let parent_staging = <S3Staging as DirStaging>::to_dir_staging(&child_staging);
        parent_staging.emit_scatter_event(&body, DirScatterInodeOp::Delete).await
    }

    /// Reclaim physical storage for tombstoned children whose retention has
    /// expired.
    ///
    /// Lists scatter objects under the parent at `parent_uri`, picks out the
    /// `Delete` (tombstone) scatters, reads each one's `TombstoneHeader`, and
    /// for every tombstone whose `retention_until_unix_ms <= now_ms`:
    ///   1. resolves the child URI from the parent path + filename,
    ///   2. opens the child staging and calls `unlink()` to remove the
    ///      child's prefix in full (inode + segments + scatter folder),
    ///   3. deletes the tombstone scatter object.
    ///
    /// Tombstones whose retention has not yet passed are skipped silently;
    /// they remain in S3 and a future `fs_gc` call after expiration will
    /// process them. If a child prefix is already gone (e.g. because a
    /// previous partial `fs_gc` run died between the unlink and the
    /// tombstone delete), the tombstone scatter is still removed so the
    /// directory's scatter listing converges.
    ///
    /// `fs_gc` is intentionally separate from `fs_compact`. Compact is
    /// expected to run at high frequency; GC walks tombstones, issues
    /// physical deletes, and is appropriate for cron/admin cadence.
    pub async fn fs_gc(client: &Client, parent_uri: &str) -> Result<GcStats>
    {
        debug!("fs_gc - parent_uri: {}", parent_uri);
        let staging_config = StagingConfig::new_s3_uri(parent_uri, None);
        let runtime_config = HyperFileRuntimeConfig::default();
        let parent_staging = S3Staging::from(client, staging_config, runtime_config.clone()).await?;

        let scatters = parent_staging.list_scatter_inodes().await?;
        let now_ms = unix_now_ms();

        let mut stats = GcStats::default();

        for scatter in scatters {
            if !matches!(scatter.op, DirScatterInodeOp::Delete) {
                continue;
            }
            stats.tombstones_visited += 1;

            // Read the tombstone body to extract the retention deadline.
            let body = match get_object_bytes(&parent_staging, &scatter.key).await {
                Ok(b) => b,
                Err(e) => {
                    warn!("fs_gc: failed to GET tombstone body s3://{}/{}: {}",
                          parent_staging.bucket, scatter.key, e);
                    stats.errors += 1;
                    continue;
                },
            };
            let (header, _inode_raw) = match parse_tombstone_body(&body) {
                Ok(v) => v,
                Err(e) => {
                    warn!("fs_gc: malformed tombstone body s3://{}/{}: {}",
                          parent_staging.bucket, scatter.key, e);
                    stats.errors += 1;
                    continue;
                },
            };

            if header.retention_until_unix_ms > now_ms {
                stats.tombstones_skipped_retention += 1;
                continue;
            }

            // Reclaim physical storage of the child. Note: with path-based
            // addressing, the child URI is parent_path + "/" + filename. The
            // upcoming UUID-based layout will instead resolve via the
            // child_uuid encoded in the scatter key, but the rest of this
            // logic stays the same.
            let child_uri = format!("s3://{}/{}/{}",
                parent_staging.bucket, parent_staging.root_path, scatter.filename);
            match reclaim_child_prefix(client, &child_uri, runtime_config.clone()).await {
                Ok(_) => {},
                Err(e) => {
                    warn!("fs_gc: failed to reclaim child {}: {}", child_uri, e);
                    stats.errors += 1;
                    continue;
                },
            }

            // Tombstone scatter object goes last: a partial GC that crashes
            // here will leave the tombstone behind, and a future fs_gc will
            // observe a missing child prefix (handled as already-cleaned)
            // and finish the job.
            if let Err(e) = parent_staging.remove_scatter_inodes(vec![scatter.key.clone()]).await {
                warn!("fs_gc: failed to delete tombstone s3://{}/{}: {}",
                      parent_staging.bucket, scatter.key, e);
                stats.errors += 1;
                continue;
            }

            stats.tombstones_reclaimed += 1;
        }

        Ok(stats)
    }

    pub async fn fs_release(&mut self) -> Result<u64>
    {
        debug!("fs_release - ");
        self.inner.release().await
    }

    pub async fn fs_flush(&mut self) -> Result<u64>
    {
        debug!("fs_flush - ");
        self.inner.flush().await
    }

    pub fn fs_getattr(&self) -> Result<libc::stat>
    {
        debug!("fs_getattr - ");
        Ok(self.inner.stat())
    }

    pub async fn fs_getattr_fast(client: &Client, uri: &str) -> Result<libc::stat>
    {
        debug!("fs_getattr_fast - uri: {}", uri);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
            .with_staging_config(&staging_config)
            .build();
        return Self::stat_fast(client.clone(), file_config).await;
    }

    pub async fn fs_chmod(&mut self, mode: libc::mode_t) -> Result<libc::stat>
    {
        debug!("fs_chmod - mode: {:#o}", mode);
        let mut stat = self.inner.stat();
        // update permission part only, don't change file type part
        stat.st_mode = (stat.st_mode & libc::S_IFMT) | (mode & !libc::S_IFMT);
        self.inner.update_stat(&stat).await
    }

    pub async fn fs_chown(&mut self, uid: libc::uid_t, gid: libc::gid_t) -> Result<libc::stat> {
        debug!("fs_chown - uid: {}, gid: {}", uid, gid);
        let mut stat = self.inner.stat();
        stat.st_uid = uid;
        stat.st_gid = gid;
        self.inner.update_stat(&stat).await
    }

    pub async fn fs_setattr(&mut self, stat: &libc::stat) -> Result<libc::stat> {
        debug!("fs_setattr - mode: {}, uid: {}, gid: {}", stat.st_mode, stat.st_uid, stat.st_gid);
        self.inner.update_stat(stat).await
    }

    pub async fn fs_setattr_fast(client: &Client, uri: &str, stat: &libc::stat) -> Result<libc::stat>
    {
        debug!("fs_setattr_fast - uri: {}", uri);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .build();
        return Self::update_stat_fast(client.clone(), file_config, stat).await;
    }

    pub async fn fs_read_entry(&self, hash: &EntryNameHash) -> Result<DirFileEntry>
    {
        debug!("fs_read_entry - ");
        self.inner.read_entry(hash).await
    }

    pub async fn fs_read_dir(&self) -> Result<Vec<DirFileEntry>>
    {
        debug!("fs_readdir - ");
        self.inner.read_dir().await
    }

    /// Consolidate outstanding scatter objects into the persisted bmap.
    ///
    /// Acquires the directory's compactor leader lease before doing any
    /// work, so concurrent callers on the same directory typically see
    /// `Err(ErrorKind::ResourceBusy)` returned fast rather than racing
    /// through the consolidate-and-flush path; correctness also relies on
    /// hyperfile's per-inode OCC, but the lease avoids the duplicated I/O
    /// that OCC alone cannot. The lease is best-effort released after
    /// either success or failure; on holder crash the lease is reclaimed
    /// after `DEFAULT_COMPACT_LEASE_TTL_MS`.
    pub async fn fs_compact(&mut self) -> Result<CompactStats>
    {
        debug!("fs_compact - ");
        self.inner.compact().await
    }
}

/// Statistics returned from [`HyperDir::fs_gc`].
#[derive(Default, Debug, Clone, Copy)]
pub struct GcStats {
    /// Number of tombstone scatter objects examined this round.
    pub tombstones_visited: usize,
    /// Tombstones whose retention had not yet expired and were left in
    /// place for a future GC pass.
    pub tombstones_skipped_retention: usize,
    /// Tombstones whose child prefix was successfully unlinked and whose
    /// scatter object was successfully removed.
    pub tombstones_reclaimed: usize,
    /// Tombstones that hit a recoverable error (LIST/GET/PUT/DELETE failure,
    /// malformed body, etc.). These are left in S3 and a subsequent `fs_gc`
    /// call may retry them. The detailed reason is in the log at `warn` level.
    pub errors: usize,
}

/// GET a single scatter / tombstone object, returning its body bytes.
async fn get_object_bytes(staging: &S3Staging, key: &str) -> Result<bytes::Bytes> {
    let res = staging.client
        .get_object()
        .bucket(&staging.bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| Error::other(
            format!("GetObject s3://{}/{}: {}", staging.bucket, key, e)))?;
    let bytes = res.body.collect().await
        .map_err(|e| Error::other(
            format!("collect body s3://{}/{}: {}", staging.bucket, key, e)))?
        .into_bytes();
    Ok(bytes)
}

/// Open the child staging at `child_uri` and call hyperfile's `unlink()` to
/// remove its inode + segments. A `NotFound` from the open path is treated
/// as "already cleaned": that means a previous partial GC removed the child
/// prefix but failed to delete the tombstone scatter, and the current call
/// can finish the job.
async fn reclaim_child_prefix(
    client: &Client,
    child_uri: &str,
    runtime_config: HyperFileRuntimeConfig,
) -> Result<()> {
    let child_config = StagingConfig::new_s3_uri(child_uri, None);
    match S3Staging::from(client, child_config, runtime_config).await {
        Ok(child_staging) => child_staging.unlink().await,
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}
