//! Multi-client / concurrency end-to-end tests.
//!
//! Each test drives several hyperdir operations *concurrently* against one
//! shared S3 namespace (the same way independent clients would) and asserts
//! **interleaving-invariant** properties — outcomes that must hold for every
//! possible interleaving, not a specific winner. Correctness rests on S3
//! conditional writes + per-inode OCC; these tests guard that the foreground
//! paths converge to a consistent namespace under contention.
//!
//! Requires AWS credentials + `S3_BUCKET` / `S3_REGION` and a large
//! `RUST_MIN_STACK`. Marked `#[ignore]`; run explicitly:
//!
//! ```ignore
//! source ../hyperdir-env.sh
//! cargo test --test e2e_concurrent -- --ignored --nocapture
//! ```

use aws_sdk_s3::Client;
use uuid::Uuid;
use std::time::Duration;
use hyperdir::{HyperDirLayout, ROOT_DIR_UUID, ScatterFirstInterceptor, LockKind, SetLkOutcome, DEFAULT_LOCK_TTL_MS};
use hyperdir::hyper::HyperDir;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;
use hyperfile::file::hyper::Hyper;
use hyperfile::staging::s3::S3Staging;
use hyperfile::staging::config::StagingConfig;
use hyperfile::config::HyperFileRuntimeConfig;

async fn make_client() -> Client {
    let region = std::env::var("S3_REGION").expect("S3_REGION not set");
    let config = aws_config::from_env().region(aws_config::Region::new(region)).load().await;
    Client::new(&config)
}
fn bucket() -> String {
    std::env::var("S3_BUCKET").expect("S3_BUCKET not set (source ../hyperdir-env.sh)")
}
fn dir_mode() -> FileMode { FileMode::from(0o755) }

/// Create a regular file at FILE/<uuid> with a scatter interceptor toward
/// `parent`, returning its UUID (mirrors hyperfs file creation).
async fn create_file(client: &Client, layout: &HyperDirLayout, bucket: &str, parent: &Uuid, name: &str)
    -> std::io::Result<Uuid>
{
    let uuid = Uuid::new_v4();
    let file_uri = layout.file_uri(bucket, &uuid);
    let parent_staging = S3Staging::from(
        client, StagingConfig::new_s3_uri(&layout.dir_uri(bucket, parent), None),
        HyperFileRuntimeConfig::default()).await?;
    let interceptor = ScatterFirstInterceptor::new(parent_staging, name, uuid);
    let mode = FileMode::from((libc::S_IFREG | 0o644) as libc::mode_t);
    let mut f = Hyper::fs_create_with_interceptor(client, &file_uri, FileFlags::rdwr(), mode, interceptor).await?;
    let _ = f.fs_release().await?;
    Ok(uuid)
}

async fn file_nlink(client: &Client, layout: &HyperDirLayout, bucket: &str, uuid: &Uuid) -> u64 {
    HyperDir::fs_getattr_fast(client, &layout.file_uri(bucket, uuid)).await.expect("getattr").st_nlink
}

async fn purge_prefix(client: &Client, bucket: &str, prefix: &str) {
    let mut stream = client.list_objects_v2().bucket(bucket).prefix(prefix).into_paginator().send();
    let mut keys: Vec<String> = Vec::new();
    while let Some(page) = stream.next().await {
        if let Ok(page) = page { if let Some(objs) = page.contents { for o in objs { if let Some(k) = o.key { keys.push(k); } } } }
    }
    for chunk in keys.chunks(1000) {
        let ids: Vec<_> = chunk.iter().map(|k| aws_sdk_s3::types::ObjectIdentifier::builder().key(k).build().unwrap()).collect();
        let del = aws_sdk_s3::types::Delete::builder().set_objects(Some(ids)).build().unwrap();
        let _ = client.delete_objects().bucket(bucket).delete(del).send().await;
    }
}

/// Names currently visible in a directory (scatter-aware merged view).
async fn dir_names(client: &Client, layout: &HyperDirLayout, bucket: &str, dir: &Uuid) -> Vec<String> {
    let d = HyperDir::fs_open_dir(client, layout, bucket, dir, FileFlags::rdonly()).await.expect("open dir");
    d.fs_read_dir().await.expect("read_dir").into_iter().map(|e| e.name).collect()
}
async fn resolve(client: &Client, layout: &HyperDirLayout, bucket: &str, dir: &Uuid, name: &str) -> Option<Uuid> {
    let d = HyperDir::fs_open_dir(client, layout, bucket, dir, FileFlags::rdonly()).await.expect("open dir");
    d.fs_read_dir().await.expect("read_dir").into_iter().find(|e| e.name == name).map(|e| e.uuid)
}

/// Concurrent hard links of the SAME name to N DISTINCT targets: exactly one
/// link wins the name, and crucially **no loser's nlink is leaked** (each
/// non-winning target stays at 1, not stuck at 2). This is the multi-client
/// version of the fs_link TOCTOU / nlink-over-count fix.
#[tokio::test]
#[ignore]
async fn e2e_concurrent_link_same_name() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let r = run_concurrent_link(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    r.expect("concurrent link flow");
}

async fn run_concurrent_link(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    // Five distinct target files, each nlink 1.
    let mut t = Vec::new();
    for i in 0..5 { t.push(create_file(client, layout, bucket, &ROOT_DIR_UUID, &format!("t{i}")).await?); }

    // All five race to claim the same new name "L".
    let res = tokio::join!(
        HyperDir::fs_link(client, layout, bucket, &ROOT_DIR_UUID, "L", &t[0]),
        HyperDir::fs_link(client, layout, bucket, &ROOT_DIR_UUID, "L", &t[1]),
        HyperDir::fs_link(client, layout, bucket, &ROOT_DIR_UUID, "L", &t[2]),
        HyperDir::fs_link(client, layout, bucket, &ROOT_DIR_UUID, "L", &t[3]),
        HyperDir::fs_link(client, layout, bucket, &ROOT_DIR_UUID, "L", &t[4]),
    );
    let oks = [res.0, res.1, res.2, res.3, res.4].iter().filter(|r| r.is_ok()).count();
    assert_eq!(oks, 1, "exactly one concurrent link wins the name");

    // The name resolves to exactly one target; that target has nlink 2, every
    // other target stayed at nlink 1 (no leaked over-count).
    let winner = resolve(client, layout, bucket, &ROOT_DIR_UUID, "L").await.expect("L resolves");
    assert!(t.contains(&winner), "winner is one of the targets");
    let mut twos = 0;
    for u in &t {
        let n = file_nlink(client, layout, bucket, u).await;
        if *u == winner { assert_eq!(n, 2, "winner nlink"); twos += 1; }
        else { assert_eq!(n, 1, "loser nlink not leaked"); }
    }
    assert_eq!(twos, 1);
    Ok(())
}

/// Concurrent creation of the SAME name (distinct UUIDs): the merged view
/// converges to exactly one entry for that name, resolvable to one of the
/// created files. (The losing files become nameless orphans, reclaimed by the
/// uuid-level orphan GC.)
#[tokio::test]
#[ignore]
async fn e2e_concurrent_create_same_name() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let r = run_concurrent_create(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    r.expect("concurrent create flow");
}

async fn run_concurrent_create(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    let res = tokio::join!(
        create_file(client, layout, bucket, &ROOT_DIR_UUID, "C"),
        create_file(client, layout, bucket, &ROOT_DIR_UUID, "C"),
        create_file(client, layout, bucket, &ROOT_DIR_UUID, "C"),
        create_file(client, layout, bucket, &ROOT_DIR_UUID, "C"),
    );
    let created = [res.0?, res.1?, res.2?, res.3?];

    let names = dir_names(client, layout, bucket, &ROOT_DIR_UUID).await;
    assert_eq!(names.iter().filter(|n| *n == "C").count(), 1, "exactly one entry named C, got {:?}", names);
    let winner = resolve(client, layout, bucket, &ROOT_DIR_UUID, "C").await.expect("C resolves");
    assert!(created.contains(&winner), "C resolves to one of the created files");
    Ok(())
}

async fn compact_root(client: &Client, layout: &HyperDirLayout, bucket: &str) -> std::io::Result<()> {
    let mut r = HyperDir::fs_open_root(client, layout, bucket, FileFlags::rdwr()).await?;
    let _ = r.fs_compact().await?;
    Ok(())
}

/// Concurrent same-source rename to two different names. A rename is a *move*:
/// the source must end up under exactly one name, never duplicated under both
/// (a double-name with nlink unchanged would dangle on the next unlink).
#[tokio::test]
#[ignore]
async fn e2e_concurrent_rename_same_source() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let r = run_concurrent_rename(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    r.expect("concurrent rename flow");
}

async fn run_concurrent_rename(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    let s = create_file(client, layout, bucket, &ROOT_DIR_UUID, "s").await?;
    compact_root(client, layout, bucket).await?; // fold "s" into the bmap (rename_within is bmap-based)

    let rename = |to: &'static str| {
        let (client, layout, bucket) = (client.clone(), layout.clone(), bucket.to_string());
        async move {
            let mut d = HyperDir::fs_open_root(&client, &layout, &bucket, FileFlags::rdwr()).await?;
            d.fs_rename("s", to).await
        }
    };
    let (_r1, _r2) = tokio::join!(rename("d1"), rename("d2"));

    let names = dir_names(client, layout, bucket, &ROOT_DIR_UUID).await;
    assert!(!names.contains(&"s".to_string()), "source gone, got {:?}", names);
    let present: Vec<&String> = names.iter().filter(|n| *n == "d1" || *n == "d2").collect();
    assert_eq!(present.len(), 1, "rename is a move: exactly one destination, got {:?}", names);
    let dst = present[0].clone();
    assert_eq!(resolve(client, layout, bucket, &ROOT_DIR_UUID, &dst).await, Some(s), "dest resolves to the file");
    assert_eq!(file_nlink(client, layout, bucket, &s).await, 1, "rename leaves nlink unchanged");
    Ok(())
}

/// Concurrent duplicate unlink of the SAME name on a 2-link file. The name is
/// removed once, so nlink must drop by exactly one (-> 1), not two (-> 0 would
/// make the file reclaimable while the other link still references it).
#[tokio::test]
#[ignore]
async fn e2e_concurrent_unlink_same_name() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let r = run_concurrent_unlink(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    r.expect("concurrent unlink flow");
}

async fn run_concurrent_unlink(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    let f = create_file(client, layout, bucket, &ROOT_DIR_UUID, "f1").await?;
    HyperDir::fs_link(client, layout, bucket, &ROOT_DIR_UUID, "f2", &f).await?; // nlink 2
    compact_root(client, layout, bucket).await?;
    assert_eq!(file_nlink(client, layout, bucket, &f).await, 2, "two links");

    let unlink = || {
        let (client, layout, bucket) = (client.clone(), layout.clone(), bucket.to_string());
        async move { HyperDir::fs_unlink(&client, &layout, &bucket, &ROOT_DIR_UUID, "f1", &f, false, None).await }
    };
    let (_a, _b) = tokio::join!(unlink(), unlink());
    compact_root(client, layout, bucket).await?;

    let names = dir_names(client, layout, bucket, &ROOT_DIR_UUID).await;
    assert!(!names.contains(&"f1".to_string()) && names.contains(&"f2".to_string()), "f1 gone, f2 kept: {:?}", names);
    assert_eq!(file_nlink(client, layout, bucket, &f).await, 1, "duplicate unlink drops nlink by one, not two");
    Ok(())
}

/// Concurrent mkdir of the same name converges to a single directory entry.
#[tokio::test]
#[ignore]
async fn e2e_concurrent_mkdir_same_name() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let r = run_concurrent_mkdir(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    r.expect("concurrent mkdir flow");
}

async fn run_concurrent_mkdir(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    let mkdir = || {
        let (client, layout, bucket) = (client.clone(), layout.clone(), bucket.to_string());
        async move { HyperDir::fs_create_default(&client, &layout, &bucket, &ROOT_DIR_UUID, "d", FileFlags::rdwr(), dir_mode()).await.map(|(_, u)| u) }
    };
    let _ = tokio::join!(mkdir(), mkdir());
    let names = dir_names(client, layout, bucket, &ROOT_DIR_UUID).await;
    assert_eq!(names.iter().filter(|n| *n == "d").count(), 1, "exactly one entry named d, got {:?}", names);
    Ok(())
}


/// How many times each test repeats its racy core. The dangerous interleavings
/// are rare windows, so a single shot is weak evidence; loop to shake them out.
const RACES: usize = 5;

/// Foreground mutation racing an in-progress `fs_compact` of the same dir — the
/// central interleaving of the scatter/compact model. An unlink's tombstone
/// and a create's scatter must each be folded exactly once regardless of
/// whether the racing compact runs before, during, or after they land: the
/// unlinked name's nlink drops by exactly one (never lost, never doubled), and
/// the created name is never lost to a compact that listed just before it.
#[tokio::test]
#[ignore]
async fn e2e_concurrent_op_vs_compact() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let r = run_op_vs_compact(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    r.expect("op vs compact flow");
}

async fn run_op_vs_compact(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    for i in 0..RACES {
        let (a, b, c) = (format!("a{i}"), format!("b{i}"), format!("c{i}"));
        let f = create_file(client, layout, bucket, &ROOT_DIR_UUID, &a).await?;
        HyperDir::fs_link(client, layout, bucket, &ROOT_DIR_UUID, &b, &f).await?; // nlink 2
        compact_root(client, layout, bucket).await?;

        // unlink "a{i}" racing a compact, and concurrently create "c{i}" racing
        // the same compact.
        let u = {
            let (client, layout, bucket, a) = (client.clone(), layout.clone(), bucket.to_string(), a.clone());
            async move { HyperDir::fs_unlink(&client, &layout, &bucket, &ROOT_DIR_UUID, &a, &f, false, None).await }
        };
        let mk = create_file(client, layout, bucket, &ROOT_DIR_UUID, &c);
        let (ru, rmk, _rc) = tokio::join!(u, mk, compact_root(client, layout, bucket));
        ru?; let cu = rmk?;
        compact_root(client, layout, bucket).await?; // settle any tombstone/scatter not yet folded

        let names = dir_names(client, layout, bucket, &ROOT_DIR_UUID).await;
        assert!(!names.contains(&a), "unlinked name gone after compact race: {names:?}");
        assert!(names.contains(&b), "surviving link kept: {names:?}");
        assert!(names.contains(&c), "created name not lost to a racing compact: {names:?}");
        assert_eq!(resolve(client, layout, bucket, &ROOT_DIR_UUID, &c).await, Some(cu), "created name resolves");
        assert_eq!(file_nlink(client, layout, bucket, &f).await, 1, "exactly-once nlink dec under compact race");
    }
    Ok(())
}

/// `fs_gc_orphans` racing an in-flight create. A create writes `FILE/<uuid>`
/// before it scatters the name, so for a moment the file is unreferenced; the
/// orphan sweep must not reclaim it. With a sane grace window a just-created
/// file always survives — this guards the grace backstop that stands between
/// orphan GC and brand-new files (a regression here is silent data loss).
#[tokio::test]
#[ignore]
async fn e2e_concurrent_orphan_gc_vs_create() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let r = run_orphan_gc_vs_create(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    r.expect("orphan gc vs create flow");
}

async fn run_orphan_gc_vs_create(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    for i in 0..RACES {
        let name = format!("g{i}");
        // Generous grace: a young in-flight create must be protected even when
        // the sweep observes it unreferenced.
        let gc = HyperDir::fs_gc_orphans(client, layout, bucket, Duration::from_secs(3600));
        let mk = create_file(client, layout, bucket, &ROOT_DIR_UUID, &name);
        let (rmk, rgc) = tokio::join!(mk, gc);
        let cu = rmk?;
        rgc?;
        compact_root(client, layout, bucket).await?;
        assert_eq!(resolve(client, layout, bucket, &ROOT_DIR_UUID, &name).await, Some(cu),
            "in-flight create survives a concurrent orphan sweep");
        assert_eq!(file_nlink(client, layout, bucket, &cu).await, 1, "created file still has its inode");
    }
    Ok(())
}

/// Mixed link + unlink on the SAME target via different names, concurrently.
/// link bumps nlink eagerly while unlink defers its decrement to compaction
/// (Scheme A), so the net must still be exact: starting at nlink 3, one
/// concurrent unlink and one concurrent link leave nlink 3 after compaction
/// (-1 +1), with the unlinked name gone and the linked name present.
#[tokio::test]
#[ignore]
async fn e2e_concurrent_link_unlink_nlink() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let r = run_link_unlink_nlink(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    r.expect("link/unlink nlink flow");
}

async fn run_link_unlink_nlink(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    for i in 0..RACES {
        let (x, y, z, w) = (format!("x{i}"), format!("y{i}"), format!("z{i}"), format!("w{i}"));
        let f = create_file(client, layout, bucket, &ROOT_DIR_UUID, &x).await?; // nlink 1
        HyperDir::fs_link(client, layout, bucket, &ROOT_DIR_UUID, &y, &f).await?; // 2
        HyperDir::fs_link(client, layout, bucket, &ROOT_DIR_UUID, &z, &f).await?; // 3
        compact_root(client, layout, bucket).await?;
        assert_eq!(file_nlink(client, layout, bucket, &f).await, 3, "three links before race");

        let u = {
            let (client, layout, bucket, x) = (client.clone(), layout.clone(), bucket.to_string(), x.clone());
            async move { HyperDir::fs_unlink(&client, &layout, &bucket, &ROOT_DIR_UUID, &x, &f, false, None).await }
        };
        let l = {
            let (client, layout, bucket, w) = (client.clone(), layout.clone(), bucket.to_string(), w.clone());
            async move { HyperDir::fs_link(&client, &layout, &bucket, &ROOT_DIR_UUID, &w, &f).await }
        };
        let (ru, rl) = tokio::join!(u, l);
        ru?; rl?;
        compact_root(client, layout, bucket).await?;

        let names = dir_names(client, layout, bucket, &ROOT_DIR_UUID).await;
        assert!(!names.contains(&x), "unlinked name gone: {names:?}");
        for n in [&y, &z, &w] { assert!(names.contains(n), "link {n} present: {names:?}"); }
        assert_eq!(file_nlink(client, layout, bucket, &f).await, 3, "net nlink after -1 unlink +1 link is exact");
    }
    Ok(())
}


/// Compact an arbitrary directory (the root helper only does root).
async fn compact_dir(client: &Client, layout: &HyperDirLayout, bucket: &str, dir: &Uuid) -> std::io::Result<()> {
    let mut d = HyperDir::fs_open_dir(client, layout, bucket, dir, FileFlags::rdwr()).await?;
    let _ = d.fs_compact().await?;
    Ok(())
}
async fn mkdir(client: &Client, layout: &HyperDirLayout, bucket: &str, parent: &Uuid, name: &str) -> std::io::Result<Uuid> {
    HyperDir::fs_create_default(client, layout, bucket, parent, name, FileFlags::rdwr(), dir_mode()).await.map(|(_, u)| u)
}
/// getattr that doesn't panic: Some(nlink) if the file exists, None if gone.
async fn maybe_nlink(client: &Client, layout: &HyperDirLayout, bucket: &str, uuid: &Uuid) -> Option<u64> {
    HyperDir::fs_getattr_fast(client, &layout.file_uri(bucket, uuid)).await.ok().map(|st| st.st_nlink)
}

/// Two clients concurrently recovering the SAME committed-but-unapplied
/// cross-directory rename intent (a crash left it behind). Recovery is forward
/// + idempotent, so racing recoverers must converge: the source is gone, the
/// destination resolves to the same file, nlink is unchanged, and no
/// double-apply corrupts the namespace.
#[tokio::test]
#[ignore]
async fn e2e_concurrent_recover_renames() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let r = run_recover_renames(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    r.expect("concurrent recover flow");
}

async fn run_recover_renames(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    let d1 = mkdir(client, layout, bucket, &ROOT_DIR_UUID, "d1").await?;
    let d2 = mkdir(client, layout, bucket, &ROOT_DIR_UUID, "d2").await?;
    compact_root(client, layout, bucket).await?;
    for i in 0..RACES {
        let (s, t) = (format!("s{i}"), format!("t{i}"));
        let f = create_file(client, layout, bucket, &d1, &s).await?;
        compact_dir(client, layout, bucket, &d1).await?;
        // Crash right after the commit point: intent written, not applied.
        HyperDir::fs_rename_emit_intent_only(client, layout, bucket, &d1, &s, &d2, &t).await?;

        let rec = || {
            let (client, layout, bucket) = (client.clone(), layout.clone(), bucket.to_string());
            async move { HyperDir::fs_recover_renames(&client, &layout, &bucket).await }
        };
        let (a, b) = tokio::join!(rec(), rec());
        a?; b?;
        compact_dir(client, layout, bucket, &d1).await?;
        compact_dir(client, layout, bucket, &d2).await?;

        assert!(!dir_names(client, layout, bucket, &d1).await.contains(&s), "source gone from d1");
        assert_eq!(resolve(client, layout, bucket, &d2, &t).await, Some(f), "dest resolves to the moved file");
        assert_eq!(file_nlink(client, layout, bucket, &f).await, 1, "recovery leaves nlink unchanged");
    }
    Ok(())
}

/// Concurrent `fs_reclaim` of the same child. Reclaim must (a) refuse a file
/// still hard-linked elsewhere (nlink > 1) and (b) be idempotent for a truly
/// orphaned one (nlink <= 1) — reclaim exactly once, the second call a no-op.
#[tokio::test]
#[ignore]
async fn e2e_concurrent_reclaim() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let r = run_reclaim(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    r.expect("concurrent reclaim flow");
}

async fn run_reclaim(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    let reclaim = |u: Uuid| {
        let (client, layout, bucket) = (client.clone(), layout.clone(), bucket.to_string());
        async move { HyperDir::fs_reclaim(&client, &layout, &bucket, &u, false).await }
    };
    for i in 0..RACES {
        // (a) guard: a hard-linked file (nlink 2) must NOT be reclaimed.
        let keep = create_file(client, layout, bucket, &ROOT_DIR_UUID, &format!("k{i}")).await?;
        HyperDir::fs_link(client, layout, bucket, &ROOT_DIR_UUID, &format!("k{i}b"), &keep).await?;
        compact_root(client, layout, bucket).await?;
        let (ka, kb) = tokio::join!(reclaim(keep), reclaim(keep));
        ka?; kb?;
        assert_eq!(maybe_nlink(client, layout, bucket, &keep).await, Some(2), "linked file not reclaimed");

        // (b) idempotent: an orphaned file (its only name unlinked -> nlink 0).
        let orphan = create_file(client, layout, bucket, &ROOT_DIR_UUID, &format!("o{i}")).await?;
        compact_root(client, layout, bucket).await?;
        HyperDir::fs_unlink(client, layout, bucket, &ROOT_DIR_UUID, &format!("o{i}"), &orphan, false, None).await?;
        compact_root(client, layout, bucket).await?; // nlink -> 0
        let (oa, ob) = tokio::join!(reclaim(orphan), reclaim(orphan));
        oa?; ob?;
        assert_eq!(maybe_nlink(client, layout, bucket, &orphan).await, None, "orphan reclaimed exactly once");
    }
    Ok(())
}

/// Two identical cross-directory renames racing. The intent commit + idempotent
/// forward apply make this safe: the namespace converges (source gone, dest
/// resolves to the file, nlink unchanged) whether both apply or the loser sees
/// the source already moved.
#[tokio::test]
#[ignore]
async fn e2e_concurrent_rename_across() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let r = run_rename_across(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    r.expect("concurrent rename_across flow");
}

async fn run_rename_across(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    let d1 = mkdir(client, layout, bucket, &ROOT_DIR_UUID, "a1").await?;
    let d2 = mkdir(client, layout, bucket, &ROOT_DIR_UUID, "a2").await?;
    compact_root(client, layout, bucket).await?;
    for i in 0..RACES {
        let (s, t) = (format!("s{i}"), format!("t{i}"));
        let f = create_file(client, layout, bucket, &d1, &s).await?;
        compact_dir(client, layout, bucket, &d1).await?;
        let across = || {
            let (client, layout, bucket, s, t) = (client.clone(), layout.clone(), bucket.to_string(), s.clone(), t.clone());
            async move { HyperDir::fs_rename_across(&client, &layout, &bucket, &d1, &s, &d2, &t).await }
        };
        let (_a, _b) = tokio::join!(across(), across()); // one may Ok, the other AlreadyExists/NotFound
        compact_dir(client, layout, bucket, &d1).await?;
        compact_dir(client, layout, bucket, &d2).await?;

        assert!(!dir_names(client, layout, bucket, &d1).await.contains(&s), "source gone from a1");
        assert_eq!(resolve(client, layout, bucket, &d2, &t).await, Some(f), "dest resolves to the file");
        assert_eq!(file_nlink(client, layout, bucket, &f).await, 1, "cross-dir rename leaves nlink unchanged");
    }
    Ok(())
}

/// Concurrent create-vs-unlink of the SAME name (distinct uuids). The merged
/// view must stay consistent: at most one entry for the name, and if present it
/// resolves to a live file (no dangling entry pointing at a reclaimed uuid).
#[tokio::test]
#[ignore]
async fn e2e_concurrent_create_vs_unlink() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let r = run_create_vs_unlink(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    r.expect("create vs unlink flow");
}

async fn run_create_vs_unlink(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    for i in 0..RACES {
        let p = format!("p{i}");
        let old = create_file(client, layout, bucket, &ROOT_DIR_UUID, &p).await?; // pre-existing entry
        compact_root(client, layout, bucket).await?;
        let u = {
            let (client, layout, bucket, p) = (client.clone(), layout.clone(), bucket.to_string(), p.clone());
            async move { HyperDir::fs_unlink(&client, &layout, &bucket, &ROOT_DIR_UUID, &p, &old, false, None).await }
        };
        let mk = create_file(client, layout, bucket, &ROOT_DIR_UUID, &p); // new uuid, same name
        let (ru, rmk) = tokio::join!(u, mk);
        ru?; rmk?;
        compact_root(client, layout, bucket).await?;

        let names = dir_names(client, layout, bucket, &ROOT_DIR_UUID).await;
        assert!(names.iter().filter(|n| **n == p).count() <= 1, "at most one entry named {p}: {names:?}");
        if let Some(u) = resolve(client, layout, bucket, &ROOT_DIR_UUID, &p).await {
            assert!(maybe_nlink(client, layout, bucket, &u).await.is_some(), "{p} resolves to a live file, not a dangling entry");
        }
    }
    Ok(())
}

/// Concurrent xattr writes: same-name writes are last-write-wins (the value
/// reads back as one of the two, never corrupt/missing), and writes to
/// different names are independent (both persist) since each is its own sidecar.
#[tokio::test]
#[ignore]
async fn e2e_concurrent_xattr() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let r = run_xattr(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    r.expect("concurrent xattr flow");
}

async fn run_xattr(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    let f = create_file(client, layout, bucket, &ROOT_DIR_UUID, "xf").await?;
    for i in 0..RACES {
        let k = format!("user.k{i}");
        // same name, two values -> last-write-wins, reads back as one of them.
        let (ra, rb) = tokio::join!(
            HyperDir::fs_setxattr(client, layout, bucket, &f, false, &k, b"A"),
            HyperDir::fs_setxattr(client, layout, bucket, &f, false, &k, b"B"),
        );
        ra?; rb?;
        let v = HyperDir::fs_getxattr(client, layout, bucket, &f, false, &k).await?.expect("xattr set");
        assert!(v == b"A" || v == b"B", "same-name xattr is one of the racers, got {v:?}");

        // distinct names -> independent sidecars, both persist.
        let (k1, k2) = (format!("user.m{i}"), format!("user.n{i}"));
        let (r1, r2) = tokio::join!(
            HyperDir::fs_setxattr(client, layout, bucket, &f, false, &k1, b"1"),
            HyperDir::fs_setxattr(client, layout, bucket, &f, false, &k2, b"2"),
        );
        r1?; r2?;
        assert_eq!(HyperDir::fs_getxattr(client, layout, bucket, &f, false, &k1).await?.as_deref(), Some(&b"1"[..]));
        assert_eq!(HyperDir::fs_getxattr(client, layout, bucket, &f, false, &k2).await?.as_deref(), Some(&b"2"[..]));
    }
    Ok(())
}

/// Two distinct owners racing for an overlapping WRITE byte-range lock on the
/// same file: the S3 OCC binding must grant exactly one (the other gets a
/// Conflict), never both. After the winner releases, the other can acquire.
#[tokio::test]
#[ignore]
async fn e2e_concurrent_locks() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let r = run_locks(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    r.expect("concurrent lock flow");
}

async fn run_locks(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    let f = create_file(client, layout, bucket, &ROOT_DIR_UUID, "lf").await?;
    let setlk = |owner: &'static str| {
        let (client, layout, bucket) = (client.clone(), layout.clone(), bucket.to_string());
        async move {
            HyperDir::fs_setlk(&client, &layout, &bucket, &f, false, owner,
                LockKind::Write, 0, 100, 1, DEFAULT_LOCK_TTL_MS).await
        }
    };
    for _ in 0..RACES {
        let (a, b) = tokio::join!(setlk("o1"), setlk("o2"));
        let granted = [a?, b?].into_iter().filter(|o| *o == SetLkOutcome::Granted).count();
        assert_eq!(granted, 1, "exactly one owner is granted an overlapping write lock");
        // release both owners so the next iteration starts clean.
        HyperDir::fs_unlock_owner(client, layout, bucket, &f, false, "o1").await?;
        HyperDir::fs_unlock_owner(client, layout, bucket, &f, false, "o2").await?;
    }
    Ok(())
}


/// C1: two concurrent cross-directory renames of the SAME source to DIFFERENT
/// destinations. A rename is a move, so the source must land under exactly one
/// name -- never both, which would leave the file reachable under two names
/// with nlink 1 (a dangling entry once one is unlinked). The source-scoped
/// rename commit point admits exactly one winner.
#[tokio::test]
#[ignore]
async fn e2e_concurrent_rename_across_distinct() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let r = run_rename_across_distinct(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    r.expect("concurrent rename_across distinct flow");
}

async fn run_rename_across_distinct(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    let d1 = mkdir(client, layout, bucket, &ROOT_DIR_UUID, "x1").await?;
    let d2 = mkdir(client, layout, bucket, &ROOT_DIR_UUID, "x2").await?;
    compact_root(client, layout, bucket).await?;
    for i in 0..RACES {
        let (s, a, b) = (format!("s{i}"), format!("a{i}"), format!("b{i}"));
        let f = create_file(client, layout, bucket, &d1, &s).await?;
        compact_dir(client, layout, bucket, &d1).await?;

        let f1 = {
            let (client, layout, bucket, s, a) = (client.clone(), layout.clone(), bucket.to_string(), s.clone(), a.clone());
            async move { HyperDir::fs_rename_across(&client, &layout, &bucket, &d1, &s, &d2, &a).await }
        };
        let f2 = {
            let (client, layout, bucket, s, b) = (client.clone(), layout.clone(), bucket.to_string(), s.clone(), b.clone());
            async move { HyperDir::fs_rename_across(&client, &layout, &bucket, &d1, &s, &d2, &b).await }
        };
        let (_r1, _r2) = tokio::join!(f1, f2); // exactly one wins; the loser sees the source moved (ENOENT)

        compact_dir(client, layout, bucket, &d1).await?;
        compact_dir(client, layout, bucket, &d2).await?;

        assert!(!dir_names(client, layout, bucket, &d1).await.contains(&s), "source gone from x1");
        let names2 = dir_names(client, layout, bucket, &d2).await;
        let present: Vec<&String> = names2.iter().filter(|n| **n == a || **n == b).collect();
        assert_eq!(present.len(), 1, "move-once: source lands under exactly one name, got {names2:?}");
        assert_eq!(resolve(client, layout, bucket, &d2, present[0]).await, Some(f), "destination resolves to the moved file");
        assert_eq!(file_nlink(client, layout, bucket, &f).await, 1, "rename leaves nlink unchanged");
    }
    Ok(())
}

/// C2: rmdir racing a create INTO the same directory. rmdir's emptiness check
/// and its tombstone aren't atomic, so a create landing in the final window
/// isn't seen. The defined semantics is best-effort: either rmdir sees the
/// child and fails (DirectoryNotEmpty, the directory and child survive
/// consistently), or rmdir wins and the racing child becomes a nameless orphan
/// that the GC chain (fs_gc reclaims the dir, then fs_gc_orphans the child)
/// reclaims -- never a permanent leak and never a dangling entry.
#[tokio::test]
#[ignore]
async fn e2e_concurrent_rmdir_vs_create() {
    let client = make_client().await;
    let bucket = bucket();
    let base = format!("hyperdir-e2e/{}", Uuid::new_v4());
    let layout = HyperDirLayout::with_base(&base);
    let r = run_rmdir_vs_create(&client, &bucket, &layout).await;
    purge_prefix(&client, &bucket, &format!("{}/", base)).await;
    r.expect("rmdir vs create flow");
}

async fn run_rmdir_vs_create(client: &Client, bucket: &str, layout: &HyperDirLayout) -> std::io::Result<()> {
    HyperDir::fs_create_root(client, layout, bucket, FileFlags::rdwr(), dir_mode()).await?;
    for i in 0..RACES {
        let name = format!("d{i}");
        let d = mkdir(client, layout, bucket, &ROOT_DIR_UUID, &name).await?;
        compact_root(client, layout, bucket).await?;

        let rm = {
            let (client, layout, bucket, name) = (client.clone(), layout.clone(), bucket.to_string(), name.clone());
            async move { HyperDir::fs_rmdir(&client, &layout, &bucket, &ROOT_DIR_UUID, &name, &d, None).await }
        };
        let mk = create_file(client, layout, bucket, &d, "c");
        let (rr, child) = tokio::join!(rm, mk);
        let child = child?; // create always commits the file + its scatter into d
        compact_root(client, layout, bucket).await?; // fold rmdir's removal of d, if it happened

        if dir_names(client, layout, bucket, &ROOT_DIR_UUID).await.contains(&name) {
            // rmdir saw the child -> failed; d and its child survive consistently.
            assert!(rr.is_err(), "d still present, so rmdir must have failed");
            assert!(dir_names(client, layout, bucket, &d).await.contains(&"c".to_string()), "child consistent under d");
            assert!(maybe_nlink(client, layout, bucket, &child).await.is_some(), "child file present");
        } else {
            // rmdir won -> the racing child is orphaned; the GC chain reclaims it.
            assert!(rr.is_ok(), "d removed, so rmdir must have succeeded");
            HyperDir::fs_gc(client, layout, bucket, &ROOT_DIR_UUID).await?;        // reclaim d's prefix
            HyperDir::fs_gc_orphans(client, layout, bucket, Duration::from_millis(0)).await?; // reclaim the child
            assert_eq!(maybe_nlink(client, layout, bucket, &child).await, None, "orphaned child reclaimed, no leak");
        }
    }
    Ok(())
}
