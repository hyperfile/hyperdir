use std::io::{Result, Error, ErrorKind};
use std::time::Duration;
use log::{debug, warn};
use uuid::Uuid;
use aws_sdk_s3::Client;
use hyperfile::file::flags::{FileFlags, HyperFileFlags};
use hyperfile::file::mode::{FileMode, HyperFileMode};
use hyperfile::config::HyperFileConfigBuilder;
use hyperfile::config::HyperFileRuntimeConfig;
use hyperfile::staging::{Staging, config::StagingConfig, s3::S3Staging, StagingIntercept};
use hyperfile::file::HyperTrait;
use hyperfile::ondisk::InodeRaw;
use crate::hyper::HyperDir;
use crate::file::{DirFileEntry, CompactStats};
use crate::interceptor::ScatterFirstInterceptor;
use crate::layout::HyperDirLayout;
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

    /// Create a child directory under `parent_dir_uuid`, returning the new
    /// directory handle and the UUID allocated for it.
    ///
    /// hyperdir owns directory-identity allocation: a fresh v4 UUID is
    /// generated here and the directory's hyperfile is created at
    /// `layout.dir_uri(bucket, new_uuid)`. A [`ScatterFirstInterceptor`] is
    /// installed toward the parent so the first inode flush commits a scatter
    /// into the parent's `!/` namespace. The parent must already exist.
    ///
    /// The root directory has no parent; create it with [`HyperDir::create`]
    /// directly (no interceptor) at `layout.root_dir_uri(bucket)`.
    pub async fn fs_create_default(
        client: &Client,
        layout: &HyperDirLayout,
        bucket: &str,
        parent_dir_uuid: &Uuid,
        name: &str,
        flags: FileFlags,
        mode: FileMode,
    ) -> Result<(Self, Uuid)>
    {
        let uuid = Uuid::new_v4();
        let uri = layout.dir_uri(bucket, &uuid);
        let parent_dir_uri = layout.dir_uri(bucket, parent_dir_uuid);
        debug!("fs_create_default - uri: {}, parent: {}, name: {}", uri, parent_dir_uri, name);
        let parent_staging = S3Staging::from(
            client,
            StagingConfig::new_s3_uri(&parent_dir_uri, None),
            HyperFileRuntimeConfig::default(),
        ).await?;
        let interceptor = ScatterFirstInterceptor::new(parent_staging, name, uuid);
        let dir = Self::fs_create_with_interceptor(client, &uri, flags, mode, interceptor).await?;
        Ok((dir, uuid))
    }

    /// Create the root directory (`DIR/<nil-uuid>`).
    ///
    /// The root has no parent, so no scatter interceptor is installed: nothing
    /// observes the root's existence through a parent. Errors with
    /// `AlreadyExists` if the root already exists.
    pub async fn fs_create_root(
        client: &Client,
        layout: &HyperDirLayout,
        bucket: &str,
        flags: FileFlags,
        mode: FileMode,
    ) -> Result<Self>
    {
        let uri = layout.root_dir_uri(bucket);
        debug!("fs_create_root - uri: {}", uri);
        Self::fs_create(client, &uri, flags, mode).await
    }

    /// Open the root directory (`DIR/<nil-uuid>`).
    pub async fn fs_open_root(
        client: &Client,
        layout: &HyperDirLayout,
        bucket: &str,
        flags: FileFlags,
    ) -> Result<Self>
    {
        let uri = layout.root_dir_uri(bucket);
        debug!("fs_open_root - uri: {}", uri);
        Self::fs_open(client, &uri, flags).await
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

    /// Delete a child by emitting a tombstone scatter to its parent directory.
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
    /// `parent_dir_uuid` / `name` identify the entry in the parent;
    /// `child_uuid` is the child's own UUID and `child_is_dir` selects the
    /// `DIR/` vs `FILE/` namespace for the child's prefix.
    ///
    /// `retention`:
    /// - `None` => `retention_until_unix_ms = 0`. The next `fs_gc` may
    ///   immediately reclaim the child storage. From the user's perspective
    ///   this is "delete now (physical reclamation is asynchronous)".
    /// - `Some(d)` => the child storage is preserved for at least `d` past
    ///   `now`. Until the retention expires `fs_gc` will leave the child
    ///   prefix alone, which is what makes a future undelete possible.
    // Addressing context is passed as discrete components; this is a
    // provisional surface pending the consumer (hyperfs) driving it.
    #[allow(clippy::too_many_arguments)]
    pub async fn fs_unlink(
        client: &Client,
        layout: &HyperDirLayout,
        bucket: &str,
        parent_dir_uuid: &Uuid,
        name: &str,
        child_uuid: &Uuid,
        child_is_dir: bool,
        retention: Option<Duration>,
    ) -> Result<()>
    {
        let child_uri = if child_is_dir {
            layout.dir_uri(bucket, child_uuid)
        } else {
            layout.file_uri(bucket, child_uuid)
        };
        let parent_dir_uri = layout.dir_uri(bucket, parent_dir_uuid);
        debug!("fs_unlink - child: {}, parent: {}, name: {}, retention: {:?}",
               child_uri, parent_dir_uri, name, retention);

        // Open the child staging just long enough to read its current inode.
        // We do not modify the child here; the prefix is left in place to be
        // reclaimed by `fs_gc` after the retention window.
        let runtime_config = HyperFileRuntimeConfig::default();
        let child_staging = S3Staging::from(
            client,
            StagingConfig::new_s3_uri(&child_uri, None),
            runtime_config.clone(),
        ).await?;

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

        // Commit the tombstone into the parent directory's scatter namespace.
        let parent_staging = S3Staging::from(
            client,
            StagingConfig::new_s3_uri(&parent_dir_uri, None),
            runtime_config,
        ).await?;
        parent_staging.emit_scatter_event(name, child_uuid, &body, DirScatterInodeOp::Delete).await
    }

    /// Remove an empty child directory.
    ///
    /// Opens the child directory and verifies it has no entries (merging its
    /// bmap with any outstanding scatter via `read_dir`); returns
    /// `ErrorKind::DirectoryNotEmpty` otherwise. On success it emits a Delete
    /// tombstone into the parent exactly like [`fs_unlink`], so physical
    /// reclamation is deferred to [`fs_gc`] under `retention`.
    ///
    /// The emptiness check and the tombstone are not atomic: an entry created
    /// in the child between the two steps can be lost. A caller that needs
    /// strict semantics must serialize rmdir against creates under the child
    /// (a higher-layer concern).
    pub async fn fs_rmdir(
        client: &Client,
        layout: &HyperDirLayout,
        bucket: &str,
        parent_dir_uuid: &Uuid,
        name: &str,
        child_dir_uuid: &Uuid,
        retention: Option<Duration>,
    ) -> Result<()>
    {
        let child_uri = layout.dir_uri(bucket, child_dir_uuid);
        debug!("fs_rmdir - child: {}, name: {}", child_uri, name);

        let child = Self::fs_open(client, &child_uri, FileFlags::rdonly()).await?;
        let entries = child.fs_read_dir().await?;
        if !entries.is_empty() {
            return Err(Error::new(ErrorKind::DirectoryNotEmpty,
                format!("directory not empty: {} ({} entries)", name, entries.len())));
        }
        drop(child);

        Self::fs_unlink(client, layout, bucket, parent_dir_uuid, name, child_dir_uuid, true, retention).await
    }

    /// Reclaim physical storage for tombstoned children whose retention has
    /// expired.
    ///
    /// Lists scatter objects under the parent directory `parent_dir_uuid`,
    /// picks out the `Delete` (tombstone) scatters, reads each one's
    /// `TombstoneHeader`, and for every tombstone whose
    /// `retention_until_unix_ms <= now_ms`:
    ///   1. resolves the child URI from the child UUID carried in the scatter
    ///      and the file/dir mode read from the tombstone's `InodeRaw`,
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
    pub async fn fs_gc(
        client: &Client,
        layout: &HyperDirLayout,
        bucket: &str,
        parent_dir_uuid: &Uuid,
    ) -> Result<GcStats>
    {
        let parent_uri = layout.dir_uri(bucket, parent_dir_uuid);
        debug!("fs_gc - parent_uri: {}", parent_uri);
        let runtime_config = HyperFileRuntimeConfig::default();
        let parent_staging = S3Staging::from(
            client,
            StagingConfig::new_s3_uri(&parent_uri, None),
            runtime_config.clone(),
        ).await?;

        let scatters = parent_staging.list_scatter_inodes().await?;
        let now_ms = unix_now_ms();

        let mut stats = GcStats::default();

        for scatter in scatters {
            if !matches!(scatter.op, DirScatterInodeOp::Delete) {
                continue;
            }
            stats.tombstones_visited += 1;

            // Read the tombstone body to extract the retention deadline and
            // the child's inode (whose mode tells us DIR vs FILE namespace).
            let body = match get_object_bytes(&parent_staging, &scatter.key).await {
                Ok(b) => b,
                Err(e) => {
                    warn!("fs_gc: failed to GET tombstone body s3://{}/{}: {}",
                          parent_staging.bucket, scatter.key, e);
                    stats.errors += 1;
                    continue;
                },
            };
            let (header, inode_raw_bytes) = match parse_tombstone_body(&body) {
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

            // Resolve the child's prefix from its UUID and kind. The kind is
            // recovered from the inode mode stored in the tombstone.
            let inode_raw = InodeRaw::from_u8_slice(inode_raw_bytes);
            let child_uri = if is_dir_mode(inode_raw.i_mode) {
                layout.dir_uri(bucket, &scatter.uuid)
            } else {
                layout.file_uri(bucket, &scatter.uuid)
            };
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

    pub async fn fs_read_entry(&self, name: &str) -> Result<DirFileEntry>
    {
        debug!("fs_read_entry - name: {}", name);
        self.inner.read_entry(name).await
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

    /// Rename an entry within this (already-open, writable) directory.
    ///
    /// Same-directory rename keeps the child's UUID and storage; only this
    /// directory's name->entry mapping changes, committed by a single inode
    /// flush (atomic via hyperfile OCC). The destination must not exist
    /// (otherwise `AlreadyExists`); `old_name == new_name` is a no-op.
    pub async fn fs_rename(&mut self, old_name: &str, new_name: &str) -> Result<()>
    {
        debug!("fs_rename - {} -> {}", old_name, new_name);
        self.inner.rename_within(old_name, new_name).await
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

/// True if a Unix mode word denotes a directory (`S_IFDIR`).
fn is_dir_mode(mode: u32) -> bool {
    (mode & libc::S_IFMT) == libc::S_IFDIR
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
