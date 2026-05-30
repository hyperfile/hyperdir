//! Physical S3 layout for directories and files.
//!
//! Every directory and every file is stored under a UUID-named prefix, with
//! directories and files segregated into two top-level namespaces:
//!
//! ```text
//! s3://<bucket>/[<base>]DIR/<uuid>/      a directory's hyperfile
//! s3://<bucket>/[<base>]FILE/<uuid>/     a file's hyperfile
//! ```
//!
//! The root directory is the directory whose UUID is the nil UUID
//! ([`ROOT_DIR_UUID`]), i.e. `…/DIR/00000000-0000-0000-0000-000000000000/`.
//!
//! Because identity is a UUID rather than a path, a rename never moves any
//! object: only the name->uuid mapping held in the parent directory changes.
//! A child's prefix carries no information about its parent, so the parent
//! context must always be supplied explicitly (it cannot be derived from the
//! child's key).
//!
//! `base` is an opaque prefix placed before `DIR/` and `FILE/`. It is empty
//! by default. A higher layer (e.g. a multi-filesystem host) may set it to
//! namespace several independent trees in one bucket; this crate attaches no
//! meaning to its contents beyond "prepend it".

use uuid::Uuid;

/// UUID of the root directory. The root has no parent and therefore no
/// scatter notification path.
pub const ROOT_DIR_UUID: Uuid = Uuid::nil();

const DIR_NAMESPACE: &str = "DIR";
const FILE_NAMESPACE: &str = "FILE";
const TXN_NAMESPACE: &str = "_TXN";

/// Builds the S3 keys / URIs for directories and files in a single tree.
#[derive(Debug, Clone, Default)]
pub struct HyperDirLayout {
    /// Opaque prefix prepended to every key, already normalized to either
    /// empty or to end with a single `/`.
    base: String,
}

impl HyperDirLayout {
    /// Layout rooted directly at the bucket: keys are `DIR/<uuid>` etc.
    pub fn new() -> Self {
        Self { base: String::new() }
    }

    /// Layout under an opaque prefix. A trailing `/` is added if missing; an
    /// empty `base` is equivalent to [`HyperDirLayout::new`].
    pub fn with_base(base: &str) -> Self {
        let base = base.trim_end_matches('/');
        let base = if base.is_empty() {
            String::new()
        } else {
            format!("{base}/")
        };
        Self { base }
    }

    /// Key (no bucket, no scheme) of a directory's prefix.
    pub fn dir_key(&self, uuid: &Uuid) -> String {
        format!("{}{DIR_NAMESPACE}/{}", self.base, uuid)
    }

    /// Key (no bucket, no scheme) of a file's prefix.
    pub fn file_key(&self, uuid: &Uuid) -> String {
        format!("{}{FILE_NAMESPACE}/{}", self.base, uuid)
    }

    /// Full `s3://` URI of a directory's prefix.
    pub fn dir_uri(&self, bucket: &str, uuid: &Uuid) -> String {
        format!("s3://{}/{}", bucket, self.dir_key(uuid))
    }

    /// Full `s3://` URI of a file's prefix.
    pub fn file_uri(&self, bucket: &str, uuid: &Uuid) -> String {
        format!("s3://{}/{}", bucket, self.file_key(uuid))
    }

    /// Full `s3://` URI of the root directory's prefix.
    pub fn root_dir_uri(&self, bucket: &str) -> String {
        self.dir_uri(bucket, &ROOT_DIR_UUID)
    }

    /// Key of a cross-directory rename intent object, identified by `txn_id`.
    pub fn txn_key(&self, txn_id: &str) -> String {
        format!("{}{TXN_NAMESPACE}/{}.intent", self.base, txn_id)
    }

    /// Key of a displaced-child reclaim intent object (rename replace-over-
    /// existing), identified by `txn_id`.
    pub fn reclaim_key(&self, txn_id: &str) -> String {
        format!("{}{TXN_NAMESPACE}/{}.reclaim", self.base, txn_id)
    }

    /// LIST prefix covering all rename intent objects.
    pub fn txn_prefix(&self) -> String {
        format!("{}{TXN_NAMESPACE}/", self.base)
    }

    /// LIST prefix covering every directory's prefix (use with delimiter `/`
    /// to enumerate `DIR/<uuid>/` common prefixes).
    pub fn dir_prefix(&self) -> String {
        format!("{}{DIR_NAMESPACE}/", self.base)
    }

    /// LIST prefix covering every file's prefix (use with delimiter `/`
    /// to enumerate `FILE/<uuid>/` common prefixes).
    pub fn file_prefix(&self) -> String {
        format!("{}{FILE_NAMESPACE}/", self.base)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_without_base() {
        let l = HyperDirLayout::new();
        let u = Uuid::nil();
        assert_eq!(l.dir_key(&u), "DIR/00000000-0000-0000-0000-000000000000");
        assert_eq!(l.file_key(&u), "FILE/00000000-0000-0000-0000-000000000000");
        assert_eq!(l.dir_uri("b", &u), "s3://b/DIR/00000000-0000-0000-0000-000000000000");
        assert_eq!(l.root_dir_uri("b"), "s3://b/DIR/00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn base_is_normalized() {
        assert_eq!(HyperDirLayout::with_base("fs1").dir_key(&Uuid::nil()),
                   "fs1/DIR/00000000-0000-0000-0000-000000000000");
        assert_eq!(HyperDirLayout::with_base("fs1/").dir_key(&Uuid::nil()),
                   "fs1/DIR/00000000-0000-0000-0000-000000000000");
        assert_eq!(HyperDirLayout::with_base("").dir_key(&Uuid::nil()),
                   "DIR/00000000-0000-0000-0000-000000000000");
    }
}
