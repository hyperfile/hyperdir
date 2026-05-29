//! End-to-end test against a real S3 bucket.
//!
//! Requires AWS credentials in the environment plus `S3_BUCKET` and
//! `S3_REGION`. Marked `#[ignore]`; run explicitly:
//!
//! ```ignore
//! source ../env.sh
//! cargo test --test e2e_s3 -- --ignored --nocapture
//! ```
//!
//! Each run uses a unique base prefix for isolation and deletes everything
//! under it on success. No environment values are hard-coded; the bucket and
//! region are read from the environment at run time.

use aws_sdk_s3::Client;
use uuid::Uuid;
use hyperdir::HyperDirLayout;
use hyperdir::ROOT_DIR_UUID;
use hyperdir::hyper::HyperDir;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;

fn bucket() -> String {
    std::env::var("S3_BUCKET").expect("S3_BUCKET not set (source ../env.sh)")
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
// is exported by ../env.sh (sourced before running this test).
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
