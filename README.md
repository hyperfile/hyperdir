# Hyperdir

A filesystem **directory namespace** on S3, implemented as a Hyperfile.

A directory is a [`hyperfile`](https://crates.io/crates/hyperfile) whose
on-disk B-tree maps a file-name hash to a directory entry `(name -> uuid +
cached inode)`. Hyperdir provides the namespace operations a filesystem needs
— create, read, rename, hard link, unlink, garbage collection — on top of
plain S3, using a scatter-then-compact commit protocol and S3 conditional
writes.

## Status

PoC. The directory protocol is implemented and validated end to end against a
real S3 bucket (see `tests/e2e_s3.rs`). See [`docs/design.md`](docs/design.md)
for the full design and the list of known limitations.

## Where it sits

```
hyperfs   FUSE filesystem (protocol, ino<->uuid)   -- separate crate
hyperdir  directory namespace (this crate)
hyperfile single file: bytes + inode on S3
```

Hyperdir owns the namespace. File byte content lives in `hyperfile`; the FUSE
protocol lives in `hyperfs`.

## Highlights

- **UUID-addressed layout** (`DIR/<uuid>`, `FILE/<uuid>`, root = nil UUID):
  identity is decoupled from path, so rename moves no data.
- **Scatter commit point**: each mutation first writes a small conditional-PUT
  object (`.inode` / `.tombstone`) into the parent directory; a later `compact`
  folds it into the B-tree. Reads (`read_dir`) are pure and concurrency-safe.
- **CRC64 collision handling** via open addressing (linear probing +
  backward-shift deletion).
- **Atomic same-directory rename**; **cross-directory rename** via a commit
  intent object with crash recovery (`fs_recover_renames`).
- **Hard links** with `nlink` authoritative in the file inode.
- **Tombstone deletion** with retention and a separate GC pass (`fs_gc`); the
  tombstone preserves the full inode to enable a future undelete.
- **Compactor leader lease** plus hyperfile's per-inode OCC for safe concurrent
  compaction.

## Layout

```
s3://<bucket>/[<base>/]DIR/<uuid>/      a directory
s3://<bucket>/[<base>/]FILE/<uuid>/     a file
s3://<bucket>/[<base>/]DIR/<nil-uuid>/  the root directory
s3://<bucket>/[<base>/]_TXN/<src_parent>_<b64(src_name)>.intent   cross-directory rename intent (source-scoped)
s3://<bucket>/[<base>/]_TXN/<ulid>.reclaim                        displaced-child reclaim intent
```

## Requirements

- `btree-ondisk >= 0.18.1` (handles a directory-entry value larger than the
  fixed 56-byte B-tree root).
- Running the integration tests needs a large stack because debug-build async
  futures are large; export `RUST_MIN_STACK` (e.g. 64 MiB):

  ```sh
  export RUST_MIN_STACK=67108864
  export S3_BUCKET=... S3_REGION=...   # plus AWS credentials
  cargo test --test e2e_s3 -- --ignored
  ```

## Documentation

- [`docs/design.md`](docs/design.md) — on-disk layout, scatter/compact
  protocol, deletion/retention/GC, rename, concurrency, constraints.

## License

This project is licensed under the Apache-2.0 License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
