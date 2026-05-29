//! End-to-end test against a real S3 bucket.
//!
//! Requires AWS credentials in the environment plus `S3_BUCKET` and
//! `S3_REGION`. Marked `#[ignore]`; run explicitly:
//!
//! ```ignore
//! source ../hyperdir-env.sh
//! cargo test --test e2e_s3 -- --ignored --nocapture
//! ```
//!
//! Each run uses a unique base prefix for isolation and deletes everything
//! under it on success. No environment values are hard-coded; the bucket and
//! region are read from the environment at run time.

use std::time::Duration;
use aws_sdk_s3::Client;
use uuid::Uuid;
use hyperdir::HyperDirLayout;
use hyperdir::ROOT_DIR_UUID;
use hyperdir::ScatterFirstInterceptor;
use hyperdir::hyper::HyperDir;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;
use hyperfile::file::hyper::Hyper;
use hyperfile::staging::s3::S3Staging;
use hyperfile::staging::config::StagingConfig;
use hyperfile::config::HyperFileRuntimeConfig;

fn bucket() -> String {
    std::env::var("S3_BUCKET").expect("S3_BUCKET not set (source ../hyperdir-env.sh)")
}

async fn make_client() -> Client {
    let region = std::env::var("S3_REGION").expect("S3_REGION not set");
    let config = aws_config::from_env()
        .region(aws_config::Region::new(region))
        .load()
        .await;
    Client::new(&config)
}

fn dir_mode() -> FileMode {
    FileMode::from(0o755)
}

/// Delete every object under `prefix` (test cleanup).
async fn purge_prefix(client: &Client, bucket: &str, prefix: &str) {
    let mut stream = client.list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send();
    let mut keys: Vec<String> = Vec::new();
    while let Some(page) = stream.next().await {
        let page = page.expect("list for cleanup");
        if let Some(objs) = page.contents {
            for o in objs {
                if let Some(k) = o.key { keys.push(k); }
            }
        }
    }
    for chunk in keys.chunks(1000) {
        let ids: Vec<_> = chunk.iter().map(|k| {
            aws_sdk_s3::types::ObjectIdentifier::builder().key(k).build().unwrap()
        }).collect();
        let del = aws_sdk_s3::types::Delete::builder()
            .set_objects(Some(ids)).build().unwrap();
        let _ = client.delete_objects().bucket(bucket).delete(del).send().await;
    }
}

fn names(entries: &[hyperdir::file::DirFileEntry]) -> std::collections::BTreeSet<String> {
    entries.iter().map(|e| e.name.clone()).collect()
}

// Requires a large thread stack: debug-build async futures here are large
// enough to overflow the default ~2 MiB test-thread stack. `RUST_MIN_STACK`
// is exported by ../hyperdir-env.sh (sourced before running this test).
#[tokio::test]
#[ignore]
async fn e2e_directory_lifecycle() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);

    let result = run(&client, &bucket, &layout).await;
    // Always attempt cleanup before asserting the outcome.
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    result.expect("e2e flow");
}

async fn run(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    // 1. root
    let _root = HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;

    // 2. mkdir alpha, beta under root (each emits a Create scatter)
    let (_a, _a_uuid) = HyperDir::fs_create_default(client, layout, bucket, &ROOT_DIR_UUID, "alpha", FileFlags::rdwr(), dir_mode()).await?;
    let (_b, b_uuid) = HyperDir::fs_create_default(client, layout, bucket, &ROOT_DIR_UUID, "beta", FileFlags::rdwr(), dir_mode()).await?;

    // 3. compact root: both children consolidated into the bmap
    let mut root = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdwr()).await?;
    let stats = root.fs_compact().await?;
    assert_eq!(stats.entries_added, 2, "two children consolidated");
    assert_eq!(stats.tombstones_kept, 0);
    drop(root);

    // 4. read_dir sees alpha + beta
    let root = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdonly()).await?;
    let n = names(&root.fs_read_dir().await?);
    assert!(n.contains("alpha") && n.contains("beta"), "got {:?}", n);
    drop(root);

    // 5. same-dir rename alpha -> gamma
    let mut root = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdwr()).await?;
    root.fs_rename("alpha", "gamma").await?;
    let n = names(&root.fs_read_dir().await?);
    assert!(n.contains("gamma") && !n.contains("alpha") && n.contains("beta"), "got {:?}", n);
    drop(root);

    // 6. cross-dir rename: move beta into gamma as "beta2"
    //    (gamma is the renamed alpha; resolve its uuid first)
    let root = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdonly()).await?;
    let gamma_uuid = root.fs_read_entry("gamma").await?.uuid;
    drop(root);
    HyperDir::fs_rename_across(client, layout, bucket, &ROOT_DIR_UUID, "beta", &gamma_uuid, "beta2").await?;

    let root = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdonly()).await?;
    let n = names(&root.fs_read_dir().await?);
    assert!(!n.contains("beta"), "beta should have moved out of root, got {:?}", n);
    drop(root);

    let gamma = HyperDir::fs_open_dir(client, layout, bucket, &gamma_uuid, FileFlags::rdonly()).await?;
    let gn = names(&gamma.fs_read_dir().await?);
    assert!(gn.contains("beta2"), "beta2 should be under gamma, got {:?}", gn);
    drop(gamma);

    // 7. rmdir gamma's child beta2, then gamma; compact + gc
    HyperDir::fs_rmdir(client, layout, bucket, &gamma_uuid, "beta2", &b_uuid, None).await?;
    {
        let mut gamma = HyperDir::fs_open_dir(client, layout, bucket, &gamma_uuid, FileFlags::rdwr()).await?;
        gamma.fs_compact().await?;
        let gn = names(&gamma.fs_read_dir().await?);
        assert!(!gn.contains("beta2"), "beta2 removed, got {:?}", gn);
    }
    let stats = HyperDir::fs_gc(client, layout, bucket, &gamma_uuid).await?;
    assert!(stats.tombstones_reclaimed >= 1, "gc reclaimed the tombstone: {:?}", stats);

    Ok(())
}


// Create a regular file at FILE/<uuid> with a scatter interceptor toward
// `parent`, returning the file's UUID. Mirrors what hyperfs will do for
// file creation (hyperdir itself only allocates directory identities).
async fn create_file(
    client: &Client,
    layout: &HyperDirLayout,
    bucket: &str,
    parent: &Uuid,
    name: &str,
) -> std::io::Result<Uuid> {
    let uuid = Uuid::new_v4();
    let file_uri = layout.file_uri(bucket, &uuid);
    let parent_uri = layout.dir_uri(bucket, parent);
    let parent_staging = S3Staging::from(
        client,
        StagingConfig::new_s3_uri(&parent_uri, None),
        HyperFileRuntimeConfig::default(),
    ).await?;
    let interceptor = ScatterFirstInterceptor::new(parent_staging, name, uuid);
    let mode = FileMode::from((libc::S_IFREG | 0o644) as libc::mode_t);
    let mut f = Hyper::fs_create_with_interceptor(client, &file_uri, FileFlags::rdwr(), mode, interceptor).await?;
    let _ = f.fs_release().await?;
    Ok(uuid)
}

async fn file_nlink(client: &Client, layout: &HyperDirLayout, bucket: &str, uuid: &Uuid) -> std::io::Result<u64> {
    let st = HyperDir::fs_getattr_fast(client, &layout.file_uri(bucket, uuid)).await?;
    Ok(st.st_nlink)
}

async fn file_exists(client: &Client, layout: &HyperDirLayout, bucket: &str, uuid: &Uuid) -> bool {
    HyperDir::fs_getattr_fast(client, &layout.file_uri(bucket, uuid)).await.is_ok()
}

/// Hard links: two names share one file; nlink is authoritative in the file
/// inode; GC reclaims a file only once the last link is gone.
#[tokio::test]
#[ignore]
async fn e2e_hardlink_nlink() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let result = run_hardlink(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    result.expect("hardlink flow");
}

async fn run_hardlink(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    let _root = HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;

    // create a file "f1" under root, consolidate, confirm nlink == 1
    let f = create_file(client, layout, bucket, &ROOT_DIR_UUID, "f1").await?;
    {
        let mut root = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdwr()).await?;
        root.fs_compact().await?;
        assert!(names(&root.fs_read_dir().await?).contains("f1"));
    }
    assert_eq!(file_nlink(client, layout, bucket, &f).await?, 1, "fresh file nlink");

    // hard link "f2" -> same file; nlink == 2; both names resolve to f
    HyperDir::fs_link(client, layout, bucket, &ROOT_DIR_UUID, "f2", &f).await?;
    assert_eq!(file_nlink(client, layout, bucket, &f).await?, 2, "after link");
    {
        let root = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdonly()).await?;
        assert_eq!(root.fs_read_entry("f1").await?.uuid, f);
        assert_eq!(root.fs_read_entry("f2").await?.uuid, f);
    }

    // unlink f1 (no retention): nlink -> 1, GC must NOT reclaim (f2 still links)
    HyperDir::fs_unlink(client, layout, bucket, &ROOT_DIR_UUID, "f1", &f, false, None).await?;
    {
        let mut root = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdwr()).await?;
        root.fs_compact().await?;
        let n = names(&root.fs_read_dir().await?);
        assert!(!n.contains("f1") && n.contains("f2"), "got {:?}", n);
    }
    assert_eq!(file_nlink(client, layout, bucket, &f).await?, 1, "after first unlink");
    HyperDir::fs_gc(client, layout, bucket, &ROOT_DIR_UUID).await?;
    assert!(file_exists(client, layout, bucket, &f).await, "file kept while a link remains");

    // unlink f2: nlink -> 0, GC reclaims the file prefix
    HyperDir::fs_unlink(client, layout, bucket, &ROOT_DIR_UUID, "f2", &f, false, None).await?;
    {
        let mut root = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdwr()).await?;
        root.fs_compact().await?;
    }
    HyperDir::fs_gc(client, layout, bucket, &ROOT_DIR_UUID).await?;
    assert!(!file_exists(client, layout, bucket, &f).await, "file reclaimed after last unlink");
    Ok(())
}

/// Retention: GC leaves a tombstone in place until its retention expires.
#[tokio::test]
#[ignore]
async fn e2e_retention() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let result = run_retention(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    result.expect("retention flow");
}

async fn run_retention(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    let _root = HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    let f = create_file(client, layout, bucket, &ROOT_DIR_UUID, "keep").await?;
    {
        let mut root = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdwr()).await?;
        root.fs_compact().await?;
    }

    // unlink with a short retention window
    HyperDir::fs_unlink(client, layout, bucket, &ROOT_DIR_UUID, "keep", &f, false, Some(Duration::from_secs(3))).await?;
    {
        let mut root = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdwr()).await?;
        root.fs_compact().await?;
    }

    // GC before expiry: tombstone skipped, file still present
    let s1 = HyperDir::fs_gc(client, layout, bucket, &ROOT_DIR_UUID).await?;
    assert_eq!(s1.tombstones_reclaimed, 0, "nothing reclaimed before retention: {:?}", s1);
    assert!(s1.tombstones_skipped_retention >= 1, "tombstone skipped: {:?}", s1);
    assert!(file_exists(client, layout, bucket, &f).await, "file kept within retention");

    // after expiry: GC reclaims
    tokio::time::sleep(Duration::from_secs(4)).await;
    let s2 = HyperDir::fs_gc(client, layout, bucket, &ROOT_DIR_UUID).await?;
    assert!(s2.tombstones_reclaimed >= 1, "reclaimed after expiry: {:?}", s2);
    assert!(!file_exists(client, layout, bucket, &f).await, "file reclaimed after retention");
    Ok(())
}

/// Concurrent compaction: two compactors on one directory. The leader lease
/// plus hyperfile's inode OCC mean both report success or one backs off with
/// ResourceBusy; in all cases the directory ends consistent.
#[tokio::test]
#[ignore]
async fn e2e_concurrent_compact() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let result = run_concurrent_compact(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    result.expect("concurrent compact flow");
}

async fn run_concurrent_compact(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    let _root = HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    for n in 0..5 {
        let _ = HyperDir::fs_create_default(client, layout, bucket, &ROOT_DIR_UUID, &format!("d{n}"), FileFlags::rdwr(), dir_mode()).await?;
    }

    let mut a = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdwr()).await?;
    let mut b = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdwr()).await?;
    let (ra, rb) = tokio::join!(a.fs_compact(), b.fs_compact());

    // Each call either succeeds or backs off (ResourceBusy from the lease, or
    // AlreadyExists from the inode OCC under FailFast); neither corrupts.
    for r in [&ra, &rb] {
        if let Err(e) = r {
            assert!(
                matches!(e.kind(), std::io::ErrorKind::ResourceBusy | std::io::ErrorKind::AlreadyExists),
                "unexpected compact error: {e:?}"
            );
        }
    }
    assert!(ra.is_ok() || rb.is_ok(), "at least one compactor should succeed");

    // Final view is consistent: all five children present.
    let root = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdonly()).await?;
    let n = names(&root.fs_read_dir().await?);
    for i in 0..5 {
        assert!(n.contains(&format!("d{i}")), "missing d{i}, got {:?}", n);
    }
    Ok(())
}


// A pair of distinct UTF-8 names with the SAME crc64 (crc64fast), constructed
// offline. They force two directory entries into the same bmap "home" slot,
// exercising open-addressing probing + backward-shift on delete.
const COLLIDE_A: &str = "A6bZ)*$e1W{";
const COLLIDE_B: &str = "B%'#]%J G|K";

/// CRC64 collision: two names hashing to the same slot must both be stored,
/// resolve to their own UUIDs, and survive deletion of the other.
#[tokio::test]
#[ignore]
async fn e2e_crc64_collision() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let result = run_collision(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    result.expect("collision flow");
}

async fn run_collision(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    // sanity: the two names really do collide under the same hasher hyperdir uses
    let h = |s: &str| { let mut c = crc64fast::Digest::new(); c.write(s.as_bytes()); c.sum64() };
    assert_eq!(h(COLLIDE_A), h(COLLIDE_B), "test constants must collide");
    assert_ne!(COLLIDE_A, COLLIDE_B);

    let _root = HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    let (_a, ua) = HyperDir::fs_create_default(client, layout, bucket, &ROOT_DIR_UUID, COLLIDE_A, FileFlags::rdwr(), dir_mode()).await?;
    let (_b, ub) = HyperDir::fs_create_default(client, layout, bucket, &ROOT_DIR_UUID, COLLIDE_B, FileFlags::rdwr(), dir_mode()).await?;
    assert_ne!(ua, ub);

    {
        let mut root = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdwr()).await?;
        let stats = root.fs_compact().await?;
        assert_eq!(stats.entries_added, 2, "both colliding entries consolidated: {:?}", stats);
    }

    // both present and resolve to their own uuids despite the shared slot
    {
        let root = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdonly()).await?;
        let n = names(&root.fs_read_dir().await?);
        assert!(n.contains(COLLIDE_A) && n.contains(COLLIDE_B), "got {:?}", n);
        assert_eq!(root.fs_read_entry(COLLIDE_A).await?.uuid, ua);
        assert_eq!(root.fs_read_entry(COLLIDE_B).await?.uuid, ub);
    }

    // delete A; B (which probed past A's slot) must remain reachable -> tests
    // the backward-shift repair of the probe chain
    HyperDir::fs_rmdir(client, layout, bucket, &ROOT_DIR_UUID, COLLIDE_A, &ua, None).await?;
    {
        let mut root = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdwr()).await?;
        root.fs_compact().await?;
        let n = names(&root.fs_read_dir().await?);
        assert!(!n.contains(COLLIDE_A) && n.contains(COLLIDE_B), "after deleting A: {:?}", n);
        assert_eq!(root.fs_read_entry(COLLIDE_B).await?.uuid, ub, "B still resolves after A removed");
    }
    Ok(())
}

/// Cross-directory rename crash recovery: a crash right after the intent
/// commit (but before apply) leaves the move incomplete; fs_recover_renames
/// must finish it.
#[tokio::test]
#[ignore]
async fn e2e_rename_crash_recovery() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let result = run_rename_recovery(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    result.expect("rename recovery flow");
}

async fn run_rename_recovery(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    let _root = HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    // src dir A holds child "x"; dst dir B is empty
    let (_a, ua) = HyperDir::fs_create_default(client, layout, bucket, &ROOT_DIR_UUID, "A", FileFlags::rdwr(), dir_mode()).await?;
    let (_b, ub) = HyperDir::fs_create_default(client, layout, bucket, &ROOT_DIR_UUID, "B", FileFlags::rdwr(), dir_mode()).await?;
    {
        let mut root = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdwr()).await?;
        root.fs_compact().await?;
    }
    let (_x, ux) = HyperDir::fs_create_default(client, layout, bucket, &ua, "x", FileFlags::rdwr(), dir_mode()).await?;
    {
        let mut a = HyperDir::fs_open_dir(client, layout, bucket, &ua, FileFlags::rdwr()).await?;
        a.fs_compact().await?;
        assert!(names(&a.fs_read_dir().await?).contains("x"));
    }

    // crash injection: commit the intent but do NOT apply it
    HyperDir::fs_rename_emit_intent_only(client, layout, bucket, &ua, "x", &ub, "y").await?;

    // mid-crash state: x still in A, y not yet in B (the move is committed but
    // not materialized)
    {
        let a = HyperDir::fs_open_dir(client, layout, bucket, &ua, FileFlags::rdonly()).await?;
        assert!(names(&a.fs_read_dir().await?).contains("x"), "x still in A before recovery");
        let b = HyperDir::fs_open_dir(client, layout, bucket, &ub, FileFlags::rdonly()).await?;
        assert!(!names(&b.fs_read_dir().await?).contains("y"), "y not in B before recovery");
    }

    // recovery completes the move
    let n = HyperDir::fs_recover_renames(client, layout, bucket).await?;
    assert!(n >= 1, "recovered at least one intent, got {}", n);

    {
        let mut a = HyperDir::fs_open_dir(client, layout, bucket, &ua, FileFlags::rdwr()).await?;
        a.fs_compact().await?;
        assert!(!names(&a.fs_read_dir().await?).contains("x"), "x removed from A after recovery");
        let mut b = HyperDir::fs_open_dir(client, layout, bucket, &ub, FileFlags::rdwr()).await?;
        b.fs_compact().await?;
        let bn = names(&b.fs_read_dir().await?);
        assert!(bn.contains("y"), "y present in B after recovery: {:?}", bn);
        // the moved child kept its identity
        assert_eq!(b.fs_read_entry("y").await?.uuid, ux);
    }

    // recovery is idempotent: a second pass finds nothing
    let n2 = HyperDir::fs_recover_renames(client, layout, bucket).await?;
    assert_eq!(n2, 0, "no intents left on second recovery");
    Ok(())
}
