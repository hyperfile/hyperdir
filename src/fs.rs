use std::io::{Result, Error, ErrorKind};
use std::time::Duration;
use log::{debug, warn};
use uuid::Uuid;
use ulid::Ulid;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::SdkBody;
use hyperfile::file::flags::{FileFlags, HyperFileFlags};
use hyperfile::file::mode::{FileMode, HyperFileMode};
use hyperfile::file::hyper::Hyper as HyperFileHandle;
use hyperfile::config::HyperFileConfigBuilder;
use hyperfile::config::HyperFileRuntimeConfig;
use hyperfile::staging::{Staging, config::StagingConfig, s3::S3Staging, StagingIntercept};
use hyperfile::meta_loader::s3_batch::S3BlockLoader;
use hyperfile::file::HyperTrait;
use hyperfile::ondisk::InodeRaw;
use hyperfile::inode::FlushInodeFlag;
use crate::hyper::HyperDir;
use crate::file::{DirFileEntry, CompactStats, HyperDirFile};
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

    /// Create a regular file `name` under `parent_dir_uuid`, returning an open
    /// hyperfile handle for byte I/O and the UUID allocated for it.
    ///
    /// Mirrors [`fs_create_default`] but creates a FILE (`layout.file_uri`)
    /// and hands back hyperfile's `Hyper`: the namespace (name -> uuid) is
    /// hyperdir's via the scatter interceptor, while byte content is
    /// hyperfile's. An initial flush is issued here so the file's inode object
    /// and the parent's Create scatter both exist before returning — the file
    /// is immediately stat-able and visible to `read_dir`.
    pub async fn fs_create_file(
        client: &Client,
        layout: &HyperDirLayout,
        bucket: &str,
        parent_dir_uuid: &Uuid,
        name: &str,
        flags: FileFlags,
        mode: FileMode,
    ) -> Result<(HyperFileHandle<'static>, Uuid)>
    {
        let uuid = Uuid::new_v4();
        let uri = layout.file_uri(bucket, &uuid);
        let parent_dir_uri = layout.dir_uri(bucket, parent_dir_uuid);
        debug!("fs_create_file - uri: {}, parent: {}, name: {}", uri, parent_dir_uri, name);
        let parent_staging = S3Staging::from(
            client,
            StagingConfig::new_s3_uri(&parent_dir_uri, None),
            HyperFileRuntimeConfig::default(),
        ).await?;
        let interceptor = ScatterFirstInterceptor::new(parent_staging, name, uuid);
        let mut file = HyperFileHandle::fs_create_with_interceptor(client, &uri, flags, mode, interceptor).await?;
        file.fs_flush().await?;
        Ok((file, uuid))
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
        parent_staging.emit_scatter_event(name, child_uuid, &body, DirScatterInodeOp::Delete).await?;

        // Decrement the file's authoritative link count, after the tombstone
        // so a crash in between over-counts (a storage leak fs_gc won't
        // reclaim) rather than under-counts (premature reclamation). Only
        // files carry hard links here; a directory has exactly one name and
        // is reclaimed by fs_gc unconditionally.
        if !child_is_dir {
            if let Err(e) = adjust_nlink(client, &child_uri, -1).await {
                warn!("fs_unlink: nlink decrement failed for {}: {}", child_uri, e);
            }
        }
        Ok(())
    }

    /// Create a hard link `new_name` in `parent_dir_uuid` to the existing file
    /// `target_file_uuid`.
    ///
    /// Bumps the target file's authoritative link count (stored in the file's
    /// own inode) and adds a directory entry pointing at the same UUID. Hard
    /// links to directories are rejected. Fails with `AlreadyExists` if
    /// `new_name` already exists in the parent.
    ///
    /// The link-count bump and the entry insert are not atomic: a crash
    /// between them over-counts (storage leak), never under-counts.
    pub async fn fs_link(
        client: &Client,
        layout: &HyperDirLayout,
        bucket: &str,
        parent_dir_uuid: &Uuid,
        new_name: &str,
        target_file_uuid: &Uuid,
    ) -> Result<()>
    {
        let child_uri = layout.file_uri(bucket, target_file_uuid);
        debug!("fs_link - parent: {}, name: {}, target: {}", parent_dir_uuid, new_name, child_uri);

        let mut parent = Self::fs_open_dir(client, layout, bucket, parent_dir_uuid, FileFlags::rdwr()).await?;
        if parent.inner.read_entry(new_name).await.is_ok() {
            return Err(Error::new(ErrorKind::AlreadyExists,
                format!("link target name exists: {}/{}", parent_dir_uuid, new_name)));
        }

        // Load the target inode; reject directory targets.
        let runtime_config = HyperFileRuntimeConfig::default();
        let target_staging = S3Staging::from(
            client,
            StagingConfig::new_s3_uri(&child_uri, None),
            runtime_config,
        ).await?;
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let _ = target_staging.load_inode(raw_inode.as_mut_u8_slice()).await?;
        if is_dir_mode(raw_inode.i_mode) {
            return Err(Error::new(ErrorKind::InvalidInput,
                format!("hard link to a directory is not allowed: {}", child_uri)));
        }

        // Bump the authoritative nlink (its own OCC retry), then add the new
        // entry. The insert flush can lose an OCC race with a concurrent
        // compactor; retry just the insert (re-opening to refresh the inode)
        // so nlink is bumped exactly once.
        let new_nlink = adjust_nlink(client, &child_uri, 1).await?;
        raw_inode.i_nlink = new_nlink;
        let entry = crate::ondisk::DirFileEntryRaw::from(&raw_inode, target_file_uuid.as_bytes(), new_name.as_bytes());
        for _ in 0..NLINK_RETRIES {
            match parent.inner.insert_entry(new_name, entry).await {
                Ok(()) => return Ok(()),
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                    parent = Self::fs_open_dir(client, layout, bucket, parent_dir_uuid, FileFlags::rdwr()).await?;
                },
                Err(e) => return Err(e),
            }
        }
        Err(Error::new(ErrorKind::ResourceBusy, "fs_link: too many OCC conflicts"))
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

    /// Reclaim a child that was displaced by a replace-over-existing rename.
    ///
    /// Unlike [`fs_unlink`], the displaced child has no name left in any
    /// directory (its entry was overwritten in place), so it cannot be
    /// reclaimed by the name-keyed tombstone/GC path. We reclaim it directly.
    ///
    /// This is **idempotent** so it is safe to run from both the foreground
    /// rename and crash recovery: a file is purged only when its stored
    /// `nlink <= 1` (the displaced name was the last/only one), never by a
    /// blind decrement. A still-hard-linked file (`nlink > 1`) is left in
    /// place — its `nlink` stays stale-high (a future leak on its final
    /// unlink, never data loss). Directories (single link) are purged.
    pub async fn fs_reclaim(
        client: &Client,
        layout: &HyperDirLayout,
        bucket: &str,
        child_uuid: &Uuid,
        child_is_dir: bool,
    ) -> Result<()>
    {
        if child_is_dir {
            let uri = layout.dir_uri(bucket, child_uuid);
            debug!("fs_reclaim - dir: {}", uri);
            return reclaim_child_prefix(client, &uri, HyperFileRuntimeConfig::default()).await;
        }
        let uri = layout.file_uri(bucket, child_uuid);
        debug!("fs_reclaim - file: {}", uri);
        match current_nlink(client, &uri).await? {
            None => Ok(()),                                                   // already reclaimed
            Some(n) if n <= 1 => reclaim_child_prefix(client, &uri, HyperFileRuntimeConfig::default()).await,
            Some(_) => Ok(()),                                                // still hard-linked elsewhere
        }
    }

    /// Durably record (commit point, `If-None-Match: *`) that `displaced` was
    /// replaced under `dst_parent/dst_name` by a rename, so a crash before the
    /// foreground reclaim doesn't orphan its storage. Returns the intent key,
    /// which the caller deletes once it has reclaimed. Left-behind intents are
    /// completed by [`fs_recover_renames`].
    pub async fn fs_emit_reclaim_intent(
        client: &Client,
        layout: &HyperDirLayout,
        bucket: &str,
        dst_parent: &Uuid,
        dst_name: &str,
        displaced: &Uuid,
        displaced_is_dir: bool,
    ) -> Result<String>
    {
        let intent = ReclaimIntent {
            dst_parent: *dst_parent,
            dst_name: dst_name.to_string(),
            displaced: *displaced,
            displaced_is_dir,
        };
        let key = layout.reclaim_key(&Ulid::new().to_string());
        client.put_object()
            .bucket(bucket)
            .key(&key)
            .body(SdkBody::from(intent.serialize().as_bytes()).into())
            .if_none_match('*')
            .send()
            .await
            .map_err(|e| Error::other(format!("PutObject reclaim s3://{}/{}: {}", bucket, key, e)))?;
        Ok(key)
    }

    /// Delete an intent object (best-effort cleanup of a committed reclaim).
    pub async fn fs_delete_intent(client: &Client, bucket: &str, key: &str) -> Result<()> {
        delete_object(client, bucket, key).await
    }

    /// Set an extended attribute (last-write-wins plain PUT). Stored as a
    /// sidecar object `<child prefix>/_xattr/<base64url(name)>`; it lives under
    /// the inode's prefix, so hyperfile's prefix-wide unlink reclaims it too.
    /// Namespace policy (e.g. user.* only) is enforced by the caller.
    pub async fn fs_setxattr(client: &Client, layout: &HyperDirLayout, bucket: &str, uuid: &Uuid, is_dir: bool, name: &str, value: &[u8]) -> Result<()> {
        let key = xattr_key(layout, uuid, is_dir, name);
        client.put_object()
            .bucket(bucket)
            .key(&key)
            .body(SdkBody::from(value.to_vec()).into())
            .send()
            .await
            .map_err(|e| Error::other(format!("PutObject xattr s3://{}/{}: {}", bucket, key, e)))?;
        Ok(())
    }

    /// Get an extended attribute's value, or `None` if it isn't set.
    pub async fn fs_getxattr(client: &Client, layout: &HyperDirLayout, bucket: &str, uuid: &Uuid, is_dir: bool, name: &str) -> Result<Option<Vec<u8>>> {
        let key = xattr_key(layout, uuid, is_dir, name);
        match client.get_object().bucket(bucket).key(&key).send().await {
            Ok(res) => {
                let bytes = res.body.collect().await
                    .map_err(|e| Error::other(format!("collect xattr s3://{}/{}: {}", bucket, key, e)))?
                    .into_bytes();
                Ok(Some(bytes.to_vec()))
            },
            Err(e) if e.as_service_error().is_some_and(|s| s.is_no_such_key()) => Ok(None),
            Err(e) => Err(Error::other(format!("GetObject xattr s3://{}/{}: {}", bucket, key, e))),
        }
    }

    /// List the names of all set extended attributes.
    pub async fn fs_listxattr(client: &Client, layout: &HyperDirLayout, bucket: &str, uuid: &Uuid, is_dir: bool) -> Result<Vec<String>> {
        let prefix = xattr_prefix(layout, uuid, is_dir);
        let mut names = Vec::new();
        let mut stream = client.list_objects_v2()
            .bucket(bucket)
            .prefix(&prefix)
            .delimiter("/")
            .into_paginator()
            .send();
        while let Some(page) = stream.next().await {
            let page = page.map_err(|e| Error::other(format!("ListObjectsV2 {}: {}", prefix, e)))?;
            for obj in page.contents() {
                let Some(seg) = obj.key().and_then(|k| k.strip_prefix(&prefix)) else { continue };
                if seg.is_empty() { continue; }
                if let Ok(bytes) = B64URL.decode(seg) {
                    if let Ok(s) = String::from_utf8(bytes) { names.push(s); }
                }
            }
        }
        Ok(names)
    }

    /// Remove an extended attribute. Returns `false` if it wasn't set.
    pub async fn fs_removexattr(client: &Client, layout: &HyperDirLayout, bucket: &str, uuid: &Uuid, is_dir: bool, name: &str) -> Result<bool> {
        if Self::fs_getxattr(client, layout, bucket, uuid, is_dir, name).await?.is_none() {
            return Ok(false);
        }
        delete_object(client, bucket, &xattr_key(layout, uuid, is_dir, name)).await?;
        Ok(true)
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

        // Read handle used to check whether a tombstone's delete has been
        // folded into the persisted bmap yet (see the per-tombstone guard
        // below). Opened once and reused.
        let dir = Self::fs_open(client, &parent_uri, FileFlags::rdonly()).await?;

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

            // Fold guard: only finalize a tombstone whose delete has been
            // folded out of the persisted bmap. If the name still resolves in
            // the bmap to this same uuid, a compact hasn't folded the delete
            // yet; deleting the tombstone now would leave the bmap entry as a
            // ghost (read_dir would show it with no tombstone to mask it).
            // Leave it for a future GC pass after a compact folds the delete.
            // (A name resolving to a *different* uuid means this delete was
            // already superseded/folded, so it is safe to finalize.)
            match dir.fs_read_entry(&scatter.filename).await {
                Ok(entry) if entry.uuid == scatter.uuid => {
                    stats.tombstones_skipped_unfolded += 1;
                    continue;
                },
                Ok(_) => {}, // name now maps elsewhere: this delete is folded/superseded
                Err(e) if e.kind() == ErrorKind::NotFound => {}, // folded out: safe
                Err(e) => {
                    warn!("fs_gc: bmap fold-check failed for {}: {}", scatter.filename, e);
                    stats.errors += 1;
                    continue;
                },
            }

            // Resolve the child's prefix from its UUID and kind. The kind is
            // recovered from the inode mode stored in the tombstone.
            let inode_raw = InodeRaw::from_u8_slice(inode_raw_bytes);
            let is_dir = is_dir_mode(inode_raw.i_mode);
            let child_uri = if is_dir {
                layout.dir_uri(bucket, &scatter.uuid)
            } else {
                layout.file_uri(bucket, &scatter.uuid)
            };

            // A file may still be reachable through other hard links; reclaim
            // only when its authoritative link count has reached zero. A
            // missing child inode means a previous GC already reclaimed it.
            // Directories carry exactly one name and are reclaimed outright.
            let should_reclaim = if is_dir {
                true
            } else {
                match current_nlink(client, &child_uri).await {
                    Ok(Some(n)) => n == 0,
                    Ok(None) => true,
                    Err(e) => {
                        warn!("fs_gc: failed to read nlink for {}: {}", child_uri, e);
                        stats.errors += 1;
                        continue;
                    },
                }
            };

            if should_reclaim {
                match reclaim_child_prefix(client, &child_uri, runtime_config.clone()).await {
                    Ok(_) => {},
                    Err(e) => {
                        warn!("fs_gc: failed to reclaim child {}: {}", child_uri, e);
                        stats.errors += 1;
                        continue;
                    },
                }
            }

            // Tombstone scatter object goes last: a partial GC that crashes
            // here will leave the tombstone behind, and a future fs_gc will
            // observe a missing child prefix (handled as already-cleaned)
            // and finish the job. The tombstone is removed whether or not the
            // child storage was reclaimed -- this name is gone either way.
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

    /// Scatter-aware single-name resolve (the cheap lookup path): see
    /// [`HyperDirFile::resolve_entry`]. Returns `None` if the name is absent.
    pub async fn fs_resolve_entry(&self, name: &str) -> Result<Option<DirFileEntry>>
    {
        debug!("fs_resolve_entry - name: {}", name);
        self.inner.resolve_entry(name).await
    }

    /// Handle-less single-name resolve: same result as [`fs_resolve_entry`] but
    /// without first opening a directory handle. The lookup hot path (one call
    /// per path component) thus avoids the open's redundant inode GET: zero
    /// inode GETs when the name still has pending scatter, one on the compacted
    /// fallback (vs. two for open-then-resolve).
    pub async fn fs_resolve_entry_fast(
        client: &Client, layout: &HyperDirLayout, bucket: &str, parent_uuid: &Uuid, name: &str,
    ) -> Result<Option<DirFileEntry>>
    {
        let uri = layout.dir_uri(bucket, parent_uuid);
        let staging_config = StagingConfig::new_s3_uri(&uri, None);
        let dir_config = S3Staging::to_dir_staging_config(&staging_config);
        let staging = S3Staging::from(client, dir_config, HyperFileRuntimeConfig::default()).await?;
        let loader = S3BlockLoader::new(client, &staging.bucket, staging.root_path());
        HyperDirFile::<S3Staging, S3BlockLoader>::resolve_fast(staging, loader, name).await
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

    /// Rename a child across two different directories.
    ///
    /// The child keeps its UUID and storage; only the two parents' mappings
    /// change. Because the two parent inode flushes cannot be made atomic
    /// together, a rename intent object is written first as the single commit
    /// point: once it exists the rename is logically committed and is
    /// completed forward (add to destination, remove from source) idempotently,
    /// then the intent is deleted. A crash at any point leaves the intent
    /// behind for [`fs_recover_renames`] to finish.
    ///
    /// Source must exist (`NotFound`) and destination must not
    /// (`AlreadyExists`); both are checked before the commit. The pre-checks
    /// and the commit are not atomic, so a destination created concurrently
    /// between the check and the apply can be overwritten (a higher-layer
    /// concern).
    #[allow(clippy::too_many_arguments)]
    pub async fn fs_rename_across(
        client: &Client,
        layout: &HyperDirLayout,
        bucket: &str,
        src_parent_uuid: &Uuid,
        src_name: &str,
        dst_parent_uuid: &Uuid,
        dst_name: &str,
    ) -> Result<()>
    {
        debug!("fs_rename_across - {}/{} -> {}/{}",
               src_parent_uuid, src_name, dst_parent_uuid, dst_name);

        let (key, intent) = emit_rename_intent(
            client, layout, bucket, src_parent_uuid, src_name, dst_parent_uuid, dst_name).await?;

        apply_rename_intent(client, layout, bucket, &intent).await?;

        // Best-effort intent cleanup; a leftover intent is harmless and will
        // be re-applied (idempotently) and removed by fs_recover_renames.
        let _ = delete_object(client, bucket, &key).await;
        Ok(())
    }

    /// Test-only: perform a cross-directory rename's pre-checks and write the
    /// commit-point intent object, then return WITHOUT applying it -- i.e.
    /// simulate a crash immediately after the commit. A subsequent
    /// [`fs_recover_renames`] must complete the move.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub async fn fs_rename_emit_intent_only(
        client: &Client,
        layout: &HyperDirLayout,
        bucket: &str,
        src_parent_uuid: &Uuid,
        src_name: &str,
        dst_parent_uuid: &Uuid,
        dst_name: &str,
    ) -> Result<()>
    {
        let _ = emit_rename_intent(
            client, layout, bucket, src_parent_uuid, src_name, dst_parent_uuid, dst_name).await?;
        Ok(())
    }

    /// Re-drive any rename intents left behind by an interrupted
    /// [`fs_rename_across`] (e.g. a crash between the commit and the intent
    /// delete). Each intent's forward steps are idempotent, so re-applying a
    /// fully-completed rename is a no-op.
    pub async fn fs_recover_renames(
        client: &Client,
        layout: &HyperDirLayout,
        bucket: &str,
    ) -> Result<usize>
    {
        let prefix = layout.txn_prefix();
        debug!("fs_recover_renames - prefix: {}", prefix);
        let mut recovered = 0;
        let mut stream = client.list_objects_v2()
            .bucket(bucket)
            .prefix(&prefix)
            .into_paginator()
            .send();
        while let Some(page) = stream.next().await {
            let page = page.map_err(|e| Error::other(format!("ListObjectsV2 {}: {}", prefix, e)))?;
            if let Some(objects) = page.contents {
                for obj in objects.iter() {
                    let Some(key) = obj.key() else { continue };
                    if key.ends_with(".intent") {
                        let body = get_object_raw(client, bucket, key).await?;
                        let intent = match RenameIntent::parse(&body) {
                            Ok(i) => i,
                            Err(e) => { warn!("fs_recover_renames: bad intent {}: {}", key, e); continue; }
                        };
                        apply_rename_intent(client, layout, bucket, &intent).await?;
                        delete_object(client, bucket, key).await?;
                        recovered += 1;
                    } else if key.ends_with(".reclaim") {
                        let body = get_object_raw(client, bucket, key).await?;
                        let ri = match ReclaimIntent::parse(&body) {
                            Ok(i) => i,
                            Err(e) => { warn!("fs_recover_renames: bad reclaim {}: {}", key, e); continue; }
                        };
                        // Is the displaced child still named dst_name? (scatter-aware)
                        let still = match Self::fs_open_dir(client, layout, bucket, &ri.dst_parent, FileFlags::rdonly()).await {
                            Ok(dir) => dir.fs_read_dir().await
                                .map(|es| es.iter().any(|e| e.name == ri.dst_name && e.uuid == ri.displaced))
                                .unwrap_or(false),
                            Err(_) => false,
                        };
                        if still {
                            // Rename hasn't completed: the displaced child is still validly
                            // named, so don't reclaim. Only clean up a clearly-abandoned
                            // (stale) intent, to avoid racing an in-flight rename.
                            if reclaim_intent_stale(key) { delete_object(client, bucket, key).await?; }
                        } else {
                            // Rename completed: the displaced child is orphaned. Idempotent.
                            Self::fs_reclaim(client, layout, bucket, &ri.displaced, ri.displaced_is_dir).await?;
                            delete_object(client, bucket, key).await?;
                            recovered += 1;
                        }
                    }
                }
            }
        }
        Ok(recovered)
    }

    /// Open a directory by its UUID.
    pub async fn fs_open_dir(client: &Client, layout: &HyperDirLayout, bucket: &str, uuid: &Uuid, flags: FileFlags) -> Result<Self> {
        Self::fs_open(client, &layout.dir_uri(bucket, uuid), flags).await
    }

    /// Enumerate every directory's UUID by listing the `DIR/` namespace with a
    /// `/` delimiter. Used by a maintenance pass to drive per-directory
    /// `fs_compact` / `fs_gc` across the whole tree.
    pub async fn fs_list_dir_uuids(client: &Client, layout: &HyperDirLayout, bucket: &str) -> Result<Vec<Uuid>> {
        list_namespace_uuids(client, bucket, &layout.dir_prefix()).await
    }

    /// Enumerate every file's UUID by listing the `FILE/` namespace.
    pub async fn fs_list_file_uuids(client: &Client, layout: &HyperDirLayout, bucket: &str) -> Result<Vec<Uuid>> {
        list_namespace_uuids(client, bucket, &layout.file_prefix()).await
    }

    /// Reclaim orphaned files: those no directory entry references anymore.
    ///
    /// The name-keyed tombstone GC ([`fs_gc`]) can't see a file that became
    /// nameless without a tombstone (e.g. the rare hard-linked child displaced
    /// by a replace-over-existing rename, left with a stale-high nlink). This
    /// is the backstop: mark every UUID referenced by any directory's
    /// `read_dir` (scatter-aware, so freshly created entries count), then purge
    /// any `FILE/<uuid>` not in that set whose inode is older than `grace`. The
    /// grace window avoids collecting a file whose creation is still in flight.
    pub async fn fs_gc_orphans(client: &Client, layout: &HyperDirLayout, bucket: &str, grace: Duration) -> Result<usize> {
        use std::collections::HashSet;
        let mut referenced: HashSet<Uuid> = HashSet::new();
        for d in Self::fs_list_dir_uuids(client, layout, bucket).await? {
            if let Ok(dir) = Self::fs_open_dir(client, layout, bucket, &d, FileFlags::rdonly()).await {
                if let Ok(entries) = dir.fs_read_dir().await {
                    for e in entries { referenced.insert(e.uuid); }
                }
            }
        }

        let now_ms = unix_now_ms();
        let grace_ms = i64::try_from(grace.as_millis()).unwrap_or(i64::MAX);
        let mut reclaimed = 0;
        for fu in Self::fs_list_file_uuids(client, layout, bucket).await? {
            if referenced.contains(&fu) { continue; }
            let uri = layout.file_uri(bucket, &fu);
            // Skip files whose inode is younger than the grace window (an
            // in-flight create not yet visible to read_dir).
            match Self::fs_getattr_fast(client, &uri).await {
                Ok(st) if now_ms - st.st_ctime.saturating_mul(1000) >= grace_ms => {
                    if reclaim_child_prefix(client, &uri, HyperFileRuntimeConfig::default()).await.is_ok() {
                        reclaimed += 1;
                    }
                },
                _ => {}, // young, or already gone
            }
        }
        Ok(reclaimed)
    }
}

/// List the UUIDs of the prefixes directly under `prefix` (e.g. `DIR/` or
/// `FILE/`), using a `/` delimiter to get one common prefix per UUID.
async fn list_namespace_uuids(client: &Client, bucket: &str, prefix: &str) -> Result<Vec<Uuid>> {
    debug!("list_namespace_uuids - prefix: {}", prefix);
    let mut out = Vec::new();
    let mut stream = client.list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .delimiter("/")
        .into_paginator()
        .send();
    while let Some(page) = stream.next().await {
        let page = page.map_err(|e| Error::other(format!("ListObjectsV2 {}: {}", prefix, e)))?;
        for cp in page.common_prefixes() {
            let Some(p) = cp.prefix() else { continue };
            let rest = p.strip_prefix(prefix).unwrap_or(p).trim_end_matches('/');
            if let Ok(u) = Uuid::parse_str(rest) {
                out.push(u);
            }
        }
    }
    Ok(out)
}

/// Statistics returned from [`HyperDir::fs_gc`].
#[derive(Default, Debug, Clone, Copy)]
pub struct GcStats {
    /// Number of tombstone scatter objects examined this round.
    pub tombstones_visited: usize,
    /// Tombstones whose retention had not yet expired and were left in
    /// place for a future GC pass.
    pub tombstones_skipped_retention: usize,
    /// Tombstones whose delete has not yet been folded into the persisted
    /// bmap (the name is still live there). Finalizing them now would orphan
    /// the bmap entry, so they are left for a future GC pass (after a compact
    /// folds the delete).
    pub tombstones_skipped_unfolded: usize,
    /// Tombstones whose child prefix was successfully unlinked and whose
    /// scatter object was successfully removed.
    pub tombstones_reclaimed: usize,
    /// Tombstones that hit a recoverable error (LIST/GET/PUT/DELETE failure,
    /// malformed body, etc.). These are left in S3 and a subsequent `fs_gc`
    /// call may retry them. The detailed reason is in the log at `warn` level.
    pub errors: usize,
}

/// A cross-directory rename, captured durably so the move can be completed
/// (or recovered) after the single intent-object commit.
struct RenameIntent {
    src_parent: Uuid,
    src_name: String,
    dst_parent: Uuid,
    dst_name: String,
    /// The child's UUID; preserved across the move (its storage never moves).
    child: Uuid,
    /// The source entry's raw inode bytes, moved verbatim to the destination.
    inode: Vec<u8>,
}

impl RenameIntent {
    /// key=value text body; names and inode are base64 (values are otherwise
    /// arbitrary bytes). Mirrors the lease body's no-dependency style.
    fn serialize(&self) -> String {
        format!(
            "src_parent={}\nsrc_name={}\ndst_parent={}\ndst_name={}\nchild={}\ninode={}\n",
            self.src_parent,
            B64.encode(self.src_name.as_bytes()),
            self.dst_parent,
            B64.encode(self.dst_name.as_bytes()),
            self.child,
            B64.encode(&self.inode),
        )
    }

    fn parse(buf: &[u8]) -> Result<Self> {
        let s = std::str::from_utf8(buf)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("intent not UTF-8: {}", e)))?;
        let mut src_parent = None;
        let mut src_name = None;
        let mut dst_parent = None;
        let mut dst_name = None;
        let mut child = None;
        let mut inode = None;
        let b64s = |v: &str| -> Result<String> {
            let bytes = B64.decode(v).map_err(|e| Error::new(ErrorKind::InvalidData, format!("intent b64: {}", e)))?;
            String::from_utf8(bytes).map_err(|e| Error::new(ErrorKind::InvalidData, format!("intent name: {}", e)))
        };
        let uuid = |v: &str| -> Result<Uuid> {
            Uuid::parse_str(v).map_err(|e| Error::new(ErrorKind::InvalidData, format!("intent uuid: {}", e)))
        };
        for line in s.lines() {
            if let Some(v) = line.strip_prefix("src_parent=") { src_parent = Some(uuid(v)?); }
            else if let Some(v) = line.strip_prefix("src_name=") { src_name = Some(b64s(v)?); }
            else if let Some(v) = line.strip_prefix("dst_parent=") { dst_parent = Some(uuid(v)?); }
            else if let Some(v) = line.strip_prefix("dst_name=") { dst_name = Some(b64s(v)?); }
            else if let Some(v) = line.strip_prefix("child=") { child = Some(uuid(v)?); }
            else if let Some(v) = line.strip_prefix("inode=") {
                inode = Some(B64.decode(v).map_err(|e| Error::new(ErrorKind::InvalidData, format!("intent inode b64: {}", e)))?);
            }
        }
        Ok(Self {
            src_parent: src_parent.ok_or_else(|| Error::new(ErrorKind::InvalidData, "intent missing src_parent"))?,
            src_name: src_name.ok_or_else(|| Error::new(ErrorKind::InvalidData, "intent missing src_name"))?,
            dst_parent: dst_parent.ok_or_else(|| Error::new(ErrorKind::InvalidData, "intent missing dst_parent"))?,
            dst_name: dst_name.ok_or_else(|| Error::new(ErrorKind::InvalidData, "intent missing dst_name"))?,
            child: child.ok_or_else(|| Error::new(ErrorKind::InvalidData, "intent missing child"))?,
            inode: inode.ok_or_else(|| Error::new(ErrorKind::InvalidData, "intent missing inode"))?,
        })
    }
}

/// A reclaim intent for a child displaced by a replace-over-existing rename.
/// The displaced child's name was overwritten in place (no tombstone), so it
/// is recorded here durably; recovery reclaims it if the rename completed.
struct ReclaimIntent {
    dst_parent: Uuid,
    dst_name: String,
    displaced: Uuid,
    displaced_is_dir: bool,
}

impl ReclaimIntent {
    fn serialize(&self) -> String {
        format!(
            "dst_parent={}\ndst_name={}\ndisplaced={}\nis_dir={}\n",
            self.dst_parent,
            B64.encode(self.dst_name.as_bytes()),
            self.displaced,
            self.displaced_is_dir as u8,
        )
    }

    fn parse(buf: &[u8]) -> Result<Self> {
        let s = std::str::from_utf8(buf)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("reclaim not UTF-8: {}", e)))?;
        let mut dst_parent = None;
        let mut dst_name = None;
        let mut displaced = None;
        let mut is_dir = false;
        for line in s.lines() {
            if let Some(v) = line.strip_prefix("dst_parent=") {
                dst_parent = Some(Uuid::parse_str(v).map_err(|e| Error::new(ErrorKind::InvalidData, format!("reclaim uuid: {}", e)))?);
            } else if let Some(v) = line.strip_prefix("dst_name=") {
                let bytes = B64.decode(v).map_err(|e| Error::new(ErrorKind::InvalidData, format!("reclaim b64: {}", e)))?;
                dst_name = Some(String::from_utf8(bytes).map_err(|e| Error::new(ErrorKind::InvalidData, format!("reclaim name: {}", e)))?);
            } else if let Some(v) = line.strip_prefix("displaced=") {
                displaced = Some(Uuid::parse_str(v).map_err(|e| Error::new(ErrorKind::InvalidData, format!("reclaim uuid: {}", e)))?);
            } else if let Some(v) = line.strip_prefix("is_dir=") {
                is_dir = v == "1";
            }
        }
        Ok(Self {
            dst_parent: dst_parent.ok_or_else(|| Error::new(ErrorKind::InvalidData, "reclaim missing dst_parent"))?,
            dst_name: dst_name.ok_or_else(|| Error::new(ErrorKind::InvalidData, "reclaim missing dst_name"))?,
            displaced: displaced.ok_or_else(|| Error::new(ErrorKind::InvalidData, "reclaim missing displaced"))?,
            displaced_is_dir: is_dir,
        })
    }
}

/// Whether a reclaim intent (keyed by `<ulid>.reclaim`) is old enough to be
/// considered abandoned rather than the in-flight tail of a live rename.
fn reclaim_intent_stale(key: &str) -> bool {
    const STALE_MS: i64 = 60_000;
    let stem = key.rsplit('/').next().unwrap_or(key);
    let stem = stem.strip_suffix(".reclaim").unwrap_or(stem);
    match Ulid::from_string(stem) {
        Ok(u) => unix_now_ms() - (u.timestamp_ms() as i64) > STALE_MS,
        Err(_) => true,
    }
}

/// Pre-check a cross-directory rename and write its commit-point intent
/// object (`If-None-Match: *`), returning the intent's S3 key and the parsed
/// intent. Source must exist (`NotFound`); destination must not
/// (`AlreadyExists`). Shared by `fs_rename_across` and the crash-injection
/// test entry.
async fn emit_rename_intent(
    client: &Client,
    layout: &HyperDirLayout,
    bucket: &str,
    src_parent_uuid: &Uuid,
    src_name: &str,
    dst_parent_uuid: &Uuid,
    dst_name: &str,
) -> Result<(String, RenameIntent)> {
    let src = HyperDir::fs_open_dir(client, layout, bucket, src_parent_uuid, FileFlags::rdonly()).await?;
    let entry_raw = src.inner.read_entry_raw(src_name).await?;
    drop(src);

    // An existing destination is overwritten: apply_rename_intent's
    // destination-add is an upsert, so the displaced entry's btree slot is
    // replaced in place (no tombstone). The caller reclaims the displaced
    // child's storage.
    let intent = RenameIntent {
        src_parent: *src_parent_uuid,
        src_name: src_name.to_string(),
        dst_parent: *dst_parent_uuid,
        dst_name: dst_name.to_string(),
        child: Uuid::from_bytes(entry_raw.uuid),
        inode: entry_raw.inode.as_u8_slice().to_vec(),
    };

    let txn_id = Ulid::new().to_string();
    let key = layout.txn_key(&txn_id);
    client.put_object()
        .bucket(bucket)
        .key(&key)
        .body(SdkBody::from(intent.serialize().as_bytes()).into())
        .if_none_match('*')
        .send()
        .await
        .map_err(|e| Error::other(format!("PutObject intent s3://{}/{}: {}", bucket, key, e)))?;
    Ok((key, intent))
}

/// Forward-apply a committed rename intent: add the entry to the destination
/// directory, then remove it from the source. Both steps are idempotent, so
/// this is safe to re-run during recovery. Destination-add happens first so
/// the child is never unreachable by name mid-rename.
async fn apply_rename_intent(client: &Client, layout: &HyperDirLayout, bucket: &str, intent: &RenameIntent) -> Result<()> {
    let inode_raw = InodeRaw::from_u8_slice(&intent.inode);
    let entry = crate::ondisk::DirFileEntryRaw::from(&inode_raw, intent.child.as_bytes(), intent.dst_name.as_bytes());

    let mut dst = HyperDir::fs_open_dir(client, layout, bucket, &intent.dst_parent, FileFlags::rdwr()).await?;
    dst.inner.insert_entry(&intent.dst_name, entry).await?;
    drop(dst);

    let mut src = HyperDir::fs_open_dir(client, layout, bucket, &intent.src_parent, FileFlags::rdwr()).await?;
    let _ = src.inner.remove_entry(&intent.src_name).await?;
    Ok(())
}

/// GET an object's raw bytes by key.
async fn get_object_raw(client: &Client, bucket: &str, key: &str) -> Result<bytes::Bytes> {
    let res = client.get_object().bucket(bucket).key(key).send().await
        .map_err(|e| Error::other(format!("GetObject s3://{}/{}: {}", bucket, key, e)))?;
    let bytes = res.body.collect().await
        .map_err(|e| Error::other(format!("collect body s3://{}/{}: {}", bucket, key, e)))?
        .into_bytes();
    Ok(bytes)
}

/// DELETE an object by key.
async fn delete_object(client: &Client, bucket: &str, key: &str) -> Result<()> {
    client.delete_object().bucket(bucket).key(key).send().await
        .map_err(|e| Error::other(format!("DeleteObject s3://{}/{}: {}", bucket, key, e)))?;
    Ok(())
}

/// True if a Unix mode word denotes a directory (`S_IFDIR`).
fn is_dir_mode(mode: u32) -> bool {
    (mode & libc::S_IFMT) == libc::S_IFDIR
}

/// S3 key of one extended-attribute sidecar object under the child's prefix.
fn xattr_key(layout: &HyperDirLayout, uuid: &Uuid, is_dir: bool, name: &str) -> String {
    format!("{}{}", xattr_prefix(layout, uuid, is_dir), B64URL.encode(name))
}

/// LIST prefix covering a child's extended-attribute sidecar objects.
fn xattr_prefix(layout: &HyperDirLayout, uuid: &Uuid, is_dir: bool) -> String {
    let base = if is_dir { layout.dir_key(uuid) } else { layout.file_key(uuid) };
    format!("{}/_xattr/", base)
}

/// Maximum optimistic-concurrency retries for an nlink read-modify-write.
const NLINK_RETRIES: usize = 5;

/// Read-modify-write a file's authoritative `i_nlink` by `delta`, returning
/// the new value. Uses a staging with no scatter interceptor (an nlink change
/// is not a directory-listing event) and retries on the inode's OCC conflict.
/// The count saturates at zero.
async fn adjust_nlink(client: &Client, child_uri: &str, delta: i64) -> Result<u64> {
    let staging = S3Staging::from(
        client,
        StagingConfig::new_s3_uri(child_uri, None),
        HyperFileRuntimeConfig::default(),
    ).await?;
    for _ in 0..NLINK_RETRIES {
        let mut raw: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let od_state = staging.load_inode(raw.as_mut_u8_slice()).await?;
        let new_nlink = (raw.i_nlink as i64 + delta).max(0) as u64;
        raw.i_nlink = new_nlink;
        match staging.flush_inode(raw.as_u8_slice(), &od_state, FlushInodeFlag::Update).await {
            Ok(_) => return Ok(new_nlink),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(Error::new(ErrorKind::ResourceBusy, "adjust_nlink: too many OCC conflicts"))
}

/// Read a file's current authoritative `i_nlink`. Returns `None` if the child
/// inode no longer exists (already reclaimed).
async fn current_nlink(client: &Client, child_uri: &str) -> Result<Option<u64>> {
    match S3Staging::from(client, StagingConfig::new_s3_uri(child_uri, None), HyperFileRuntimeConfig::default()).await {
        Ok(staging) => {
            let mut raw: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
            let _ = staging.load_inode(raw.as_mut_u8_slice()).await?;
            Ok(Some(raw.i_nlink))
        },
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
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
