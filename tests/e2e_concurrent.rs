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
use hyperdir::{HyperDirLayout, ROOT_DIR_UUID, ScatterFirstInterceptor};
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
