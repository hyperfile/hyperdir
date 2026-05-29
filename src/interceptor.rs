//! Scatter-first commit interceptor.
//!
//! `ScatterFirstInterceptor` plugs into hyperfile's `StagingIntercept` so that
//! every `flush_inode` call on a child (file or directory) writes the parent
//! directory's scatter object **before** the child's own inode object is
//! persisted. The scatter is therefore the durable commit point of a single
//! logical mutation:
//!
//! ```text
//! 1. before_flush_inode  -> PUT  <parent dir>/!/inode_<ulid>_<name>_<uuid>_<op>
//!                                with `If-None-Match: *`     (commit point)
//! 2. hyperfile core      -> PUT  <child>/inode               (replication)
//! 3. after_flush_inode   -> no-op
//! ```
//!
//! If step 1 fails the whole `flush_inode` fails and step 2 never runs, so the
//! parent directory never observes a partial commit. If step 1 succeeds but
//! step 2 (or anything later) fails, the scatter alone is enough for a future
//! reader/compactor to reconstruct the child inode by replaying the scatter
//! body. Step 2 is therefore best-effort, idempotent replication.
//!
//! Under UUID addressing the child's prefix carries no information about its
//! parent or its directory-entry name, so the interceptor must be constructed
//! with the parent directory's staging, the child's name, and the child's
//! UUID. None of these can be derived from the `staging` argument the hook
//! receives (which is the child's own staging).

use std::io::Result;
use std::pin::Pin;
use std::future::Future;
use log::debug;
use uuid::Uuid;
use hyperfile::staging::{StagingIntercept, s3::S3Staging};
use hyperfile::inode::FlushInodeFlag;
use crate::{DirStaging, DirScatterInodeOp};

/// Interceptor that commits a scatter into the parent directory before the
/// child's inode is written.
///
/// Holds the parent directory's staging plus the child's directory-entry
/// identity (`name`, `uuid`). A future revision may also carry a per-handle
/// transaction id (ulid) so retries triggered by hyperfile's
/// `RetryLastWriterWins` policy reuse the same scatter key, giving
/// writer-side exactly-once semantics on top of the conditional PUT. Until
/// then, retries emit a fresh scatter; consolidation deduplicates.
#[derive(Clone)]
pub struct ScatterFirstInterceptor {
    /// Staging of the parent directory: scatter objects land in its `!/`
    /// namespace.
    parent_dir_staging: S3Staging,
    /// The child's name within the parent directory.
    name: String,
    /// The child's UUID (its `DIR/<uuid>` or `FILE/<uuid>` prefix).
    uuid: Uuid,
}

impl ScatterFirstInterceptor {
    pub fn new(parent_dir_staging: S3Staging, name: impl Into<String>, uuid: Uuid) -> Self {
        Self { parent_dir_staging, name: name.into(), uuid }
    }
}

impl StagingIntercept<S3Staging> for ScatterFirstInterceptor {
    fn before_flush_inode(
        &self,
        _staging: &S3Staging,
        payload: &[u8],
        flag: FlushInodeFlag,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + '_ + Send>> {
        // `_staging` is the child's own staging; it tells us nothing about the
        // parent under UUID addressing. The parent staging, name, and uuid all
        // come from `self`. Take owned copies so the returned future has no
        // lifetime tie back to the caller-owned references.
        let parent = self.parent_dir_staging.clone();
        let name = self.name.clone();
        let uuid = self.uuid;
        let payload = payload.to_vec();
        Box::pin(async move {
            let op = match flag {
                FlushInodeFlag::Create => DirScatterInodeOp::Create,
                FlushInodeFlag::Update => DirScatterInodeOp::Update,
                // Delete scatters are emitted by fs_unlink with a tombstone
                // body (TombstoneHeader || InodeRaw), not via this hook. If
                // hyperfile ever wires flush_inode(Delete) to a real call
                // path, the bytes hyperfile would pass here are just the
                // inode raw, which the GC and undelete paths cannot parse as
                // a tombstone. Skip rather than emit a malformed body.
                FlushInodeFlag::Delete => {
                    debug!("ScatterFirstInterceptor: skipping FlushInodeFlag::Delete (use fs_unlink instead)");
                    return Ok(());
                },
                // Internal hyperfile flags that aren't real commits in the
                // parent-directory sense; nothing to scatter.
                FlushInodeFlag::Ignore | FlushInodeFlag::Unkown => {
                    debug!("ScatterFirstInterceptor: skipping flag {:?}", flag);
                    return Ok(());
                },
            };
            parent.emit_scatter_event(&name, &uuid, &payload, op).await
        })
    }

    fn after_flush_inode(
        &self,
        _staging: &S3Staging,
        _payload: &[u8],
        _flag: FlushInodeFlag,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + '_ + Send>> {
        // Nothing to do: the inode write that just completed is the
        // best-effort replication of the scatter we already committed in
        // before_flush_inode. If the inode PUT failed, hyperfile's own retry
        // loop already handled it; if it succeeded, the parent's scatter is
        // still the source of truth.
        Box::pin(async { Ok(()) })
    }

    fn after_remove_inode(
        &self,
        _staging: &S3Staging,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + '_ + Send>> {
        // unlink is handled by fs_unlink (tombstone scatter) and fs_gc
        // (physical reclamation); nothing to do on this hook.
        Box::pin(async { Ok(()) })
    }
}
