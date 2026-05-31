# Hyperdir Design

Hyperdir implements a filesystem **directory namespace** on top of S3, where a
directory is itself a Hyperfile (see the [`hyperfile`](https://crates.io/crates/hyperfile)
crate) whose on-disk B-tree maps a file-name hash to a directory entry.

This document describes the on-disk layout and the protocols hyperdir uses.
Statements here track the current implementation; where something is decided
but not yet implemented it is marked *(planned)*.

## 1. Layered model

```
hyperfs   (FUSE filesystem; separate crate)   protocol + ino<->uuid
   |
hyperdir  (this crate)                         directory namespace
   |
hyperfile (single file: bytes + inode on S3)   file content + metadata
   |
btree-ondisk                                   the directory index
```

Hyperdir owns the **namespace** (name -> uuid, directory operations). It does
*not* own file byte content (that is `hyperfile`) or the FUSE protocol (that is
`hyperfs`).

S3 has no rename, no directory primitive, and no cross-object transaction.
Hyperdir therefore uses a **scatter-then-compact** model: a mutation first
writes a small "scatter" object as its durable commit point, and a later
`compact` folds outstanding scatters into the directory's B-tree. This relies
on S3's strong read-after-write consistency and conditional writes
(`If-None-Match` / `If-Match`).

## 2. Physical layout

Built by `HyperDirLayout` (`src/layout.rs`):

```
s3://<bucket>/[<base>/]DIR/<uuid>/      a directory's Hyperfile
s3://<bucket>/[<base>/]FILE/<uuid>/     a file's Hyperfile
s3://<bucket>/[<base>/]DIR/<nil-uuid>/  the root directory (ROOT_DIR_UUID)
s3://<bucket>/[<base>/]_TXN/<ulid>.intent    cross-directory rename intent
s3://<bucket>/[<base>/]_TXN/<ulid>.reclaim   displaced-child reclaim intent (rename replace-over-existing)
```

- Identity is a **UUIDv4**; ordering/transaction ids use **ULID**.
- A directory/file's S3 prefix is its UUID and is fully decoupled from its
  logical path. This is what makes rename move no data.
- The root directory is the nil UUID (`ROOT_DIR_UUID = Uuid::nil()`); it has no
  parent and no scatter notifications.
- `base` is an opaque prefix (empty by default). A higher layer (`hyperfs`)
  uses it for an `fs_name`, namespacing several trees in one bucket. This crate
  attaches no meaning to it.

Inside each prefix the standard Hyperfile structure applies (`inode`,
`segment` files, and a scatter folder `!/`), plus optional sidecar objects:
`_xattr/<name>` for extended attributes and `_lock` for the advisory-lock table
(§10). Both are reclaimed with the prefix.

## 3. Directory index

A directory is a Hyperfile whose B-tree maps `crc64(name) -> DirFileEntryRaw`:

```
DirFileEntryRaw {
    inode: InodeRaw,    // cached snapshot of the child's inode (advisory; see §6)
    uuid:  [u8; 16],    // the child's prefix UUID (stable identity; survives rename)
    name:  [u8; 256],   // file name; last byte reserved for NUL, so max 255 bytes
}                       // ~432 bytes
```

### CRC64 collision handling (open addressing)

Because the B-tree key is `crc64(name)`, two distinct names can collide. To
avoid silent overwrites, hyperdir uses linear-probing open addressing:

- `crc64(name)` is the "home" slot; on collision, probe forward.
- Entries are disambiguated by the full name stored in each entry (`name_eq`).
- `probe_lookup` stops at the first empty slot; `probe_upsert` updates a
  matching slot or fills the first empty one; `probe_delete` repairs the probe
  chain with **backward-shift** so no gap breaks a later lookup.

## 4. Scatter object naming

Scatter objects live under the parent directory's `!/`, with the **file name
as its own path segment** (name-first), typed by suffix:

```
<parent>/!/{base64(name)}/{ulid}_{uuid}_{op}.inode       Create / Update / PreDelete
<parent>/!/{base64(name)}/{ulid}_{uuid}_{op}.tombstone   Delete
```

Putting `base64(name)` in its own path segment lets a single name's scatter be
listed directly with `LIST prefix=<parent>/!/{base64(name)}/` (the cheap
single-name resolve, §6) without scanning the whole directory. The `ulid`
orders a name's events; `base64` uses an alphabet without `_`/`/` and the uuid
is hyphenated, so `_` stays an unambiguous field separator. `op` is
`DirScatterInodeOp` (`Create`, `Update`, `PreDelete`, `Delete`, `Unknown`);
only `Delete` uses the `.tombstone` suffix. `PreDelete` is phase 1 of the
two-phase delete (§8): a cheap unlink marker whose body is just `is_dir` + the
retention deadline; compaction turns it into a real `Delete` tombstone.

## 5. Write path: scatter as the commit point

`ScatterFirstInterceptor` (a `hyperfile::StagingIntercept`) makes every
`flush_inode` on a child write its scatter into the parent **before** the
child's own inode object is written:

1. `before_flush_inode` -> conditional PUT of the scatter (`If-None-Match: *`).
   On failure the whole `flush_inode` fails, so the parent never sees a partial
   commit. `412/409` is treated as already-committed (idempotent).
2. hyperfile then writes the child's `inode` object (best-effort replica).

The interceptor holds the explicit `(parent staging, child name, child uuid)`
— under UUID addressing the parent cannot be derived from the child prefix.

The child's own `inode` object is a best-effort replica: if it lags, the parent
scatter is still the authoritative commit, and hyperfile's segment-based
recovery (`refresh_bmap`) lets the next writer self-heal. No separate reader
self-heal step is needed.

## 6. Read path and authority model

`read_dir(&self)` is a pure read (safe to call concurrently): it lists scatter,
takes the latest per name (`filter_last_view`, ordered by
`last_modified -> full ULID -> S3 key`), fetches Create/Update bodies, walks the
B-tree snapshot, and merges **by name** (not by hash, so colliding names in one
batch are not collapsed). It writes nothing.

Authority model (documented on `DirFileEntry`):

| data | authority |
| --- | --- |
| namespace (name -> uuid, existence) | the **parent** directory's B-tree |
| file metadata (mode/size/times/**nlink**) | the child's **own inode** (also embedded in each segment, reachable via cno) |
| the inode cached in a directory entry | advisory readdir/stat snapshot; may lag |

Consumers that open a file through a directory entry should refresh `nlink`
(and other metadata) from the opened file's inode rather than trusting the
cached copy — particularly for hard-linked files, whose cached `nlink` is only
advisory.

### Cheap single-name resolve

`read_dir` walks the whole directory; for resolving one name (lookup, the hot
path), that is too expensive. `resolve_entry(name)` is the cheap path:

1. LIST only that name's scatter folder `!/{base64(name)}/` (0..N objects).
2. If any exist, the latest wins — Create/Update ⇒ present (with its inode), a
   Delete or PreDelete (phase-1 unlink, §8) ⇒ absent. (If the winning Create
   body was concurrently compacted away, fall back to the B-tree point lookup.)
3. Only when the name has no pending scatter, do a single B-tree point lookup.

It never concludes from a B-tree hit alone (a newer Delete tombstone would be
missed) yet avoids the full walk. `HyperDir::fs_resolve_entry_fast` is the
handle-less variant (builds staging directly), so the per-path-component lookup
pays no separate open-the-handle inode read. Point lookups read the **latest**
bmap (re-read from staging), not this handle's open-time snapshot, so a name a
concurrent compactor just folded is never missed.

## 7. Compaction and concurrency

- `read_dir` is the read path; `compact` is the write path (apply scatter to the
  B-tree, flush, delete scatter — but keep tombstones, see §8).
- `compact` first takes a per-directory leader lease at `<dir>/_compact.lease`
  (`If-None-Match: *` to acquire, `If-Match` to take over after the TTL, default
  `DEFAULT_COMPACT_LEASE_TTL_MS = 60s`, `If-Match` delete to release). The lease
  avoids duplicated work; **correctness** still comes from hyperfile's per-inode
  OCC (two compactors that both flush the parent inode produce one `Ok` and one
  `AlreadyExists`).
- `compact` reloads the handle to the latest committed inode/bmap before
  folding (so it never folds onto a stale snapshot), applies the scatter, and
  flushes. When it folds a `PreDelete` that actually removed an entry it runs
  **phase 2** of the delete (§8): read the child's inode, write the real
  `Delete` tombstone, and decrement a file's `nlink` — this is the single
  lease-serialized point that makes the count exactly-once.
- `read_dir` likewise walks the latest bmap (re-read), merged with a fresh
  scatter LIST, so it neither misses a just-folded entry nor double-counts one.
- A background **maintenance loop** (in `hyperfs`) drives `fs_recover_renames`,
  then per-directory `fs_compact` + `fs_gc`, then `fs_gc_orphans`, each pass.

## 8. Deletion, retention, and GC

Deletion is **two-phase** and tombstone-based: it does not physically delete
the child prefix.

- **Tombstone body** = `TombstoneHeader { deleted_at_unix_ms,
  retention_until_unix_ms }` (16 bytes) followed by the child's full `InodeRaw`
  (preserved to enable a future undelete).
- **Phase 1** — `fs_unlink(.., child_is_dir, retention)`: emit a cheap
  `PreDelete` scatter whose body is just `is_dir` + the retention deadline
  computed now. It does **not** read the child inode, build a tombstone, touch
  `nlink`, or delete the prefix. `read_dir`/`resolve` treat the name as gone
  immediately.
- **Phase 2** — `compact` folds the `PreDelete` out of the bmap and then, at
  that single lease-serialized point, reads the child's authoritative inode,
  writes the real `Delete` tombstone (stamped with the recorded retention), and
  decrements a file's `nlink` once. Folding the name and deciding the decrement
  happen under the lease, so duplicate/concurrent unlinks of one name
  decrement **exactly once** (never over-decrementing, which would prematurely
  reclaim a still-linked file). A later `compact` re-sees the kept tombstone but
  `probe_delete` returns `false`, so it is not double-counted. The trade-off is
  that `nlink` lags until compaction (eventual), consistent with the deferred
  deletion model.
- `fs_gc(.., parent_uuid)`: for each expired tombstone (retention passed),
  reclaim a directory unconditionally, but reclaim a file only when its
  authoritative `nlink` has reached zero (otherwise just remove the tombstone —
  other hard links remain). Then remove the tombstone.
- `fs_gc_orphans(.., grace)`: a backstop sweep. It marks every file UUID
  referenced by any directory's `read_dir` (scatter-aware), then reclaims any
  `FILE/<uuid>` not referenced whose inode is older than `grace` — catching
  nameless files no tombstone covers (e.g. a create+unlink before any compact,
  or a hard-linked child displaced by a replace-over-existing rename).
- `fs_rmdir`: verify the child directory is empty via `read_dir`
  (`DirectoryNotEmpty` otherwise), then `fs_unlink` it.
- `nlink` is authoritative in the child inode; `adjust_nlink` uses a
  no-interceptor staging (no scatter) with OCC retry.

## 9. Create, rename, hard link

- **mkdir** = `fs_create_default(.., parent_uuid, name, ..) -> (HyperDir, Uuid)`:
  hyperdir allocates the new UUID (`Uuid::new_v4()`), creates the directory
  Hyperfile, and installs a `ScatterFirstInterceptor` toward the parent; the
  initial `flush_inode(Create)` emits the Create scatter. `fs_create_root` /
  `fs_open_root` handle the parentless root; `fs_open_dir` opens by UUID.
- **Same-directory rename** (`fs_rename`): rebuild the entry with the same
  inode+uuid under the new name, `probe_delete(old)` + `probe_upsert(new)`, one
  flush — atomic via OCC, no scatter, no data move.
- **Cross-directory rename** (`fs_rename_across`): write a `_TXN/<ulid>.intent`
  object (`If-None-Match: *`) as the single commit point, then apply it
  (destination-add before source-remove, so the child is always reachable),
  then delete the intent. A crash leaves the intent for `fs_recover_renames`,
  which replays it idempotently.
- **Replace-over-existing rename**: the destination-add is an upsert, so an
  existing destination's slot is replaced in place. Before overwriting, the
  caller records a `_TXN/<ulid>.reclaim` intent (`fs_emit_reclaim_intent`)
  naming the displaced child; after the rename it reclaims that child
  (`fs_reclaim`, idempotent: a file only when its `nlink<=1`). A crash in
  between leaves the `.reclaim` intent, which `fs_recover_renames` completes —
  but only once the displaced name no longer resolves to it (it rechecks, to
  avoid racing an in-flight rename); a clearly-stale intent is just dropped.
- **Hard link** (`fs_link`): reject directory targets; bump the file's
  authoritative `nlink`, then insert the entry under the new name. The parent is
  opened **FailFast** and the insert is **exclusive** (`insert_entry_excl`
  refuses to overwrite an existing name), so a concurrent same-name link cannot
  silently clobber the winner. Any path that fails to commit the entry (the name
  was taken, a lost OCC race after retries, or a transient error) rolls the
  `nlink` bump back, so a failed/raced link never leaks an over-count. The only
  residual is a true process crash between the bump and the entry commit,
  backstopped by `fs_gc_orphans`.

## 10. Advisory locks (`lock` module)

Optional S3-backed advisory locks (used by hyperfs's `--lock-mode global`).
Two parts:

- A storage-agnostic byte-range lock engine (`LockTable`) with POSIX semantics:
  read/write compatibility, overlap, replace/split of the owner's own ranges,
  group release by owner, and TTL expiry. Pure logic, unit-tested, no I/O.
- A binding on `HyperDir` — `fs_getlk` / `fs_setlk` / `fs_unlock_owner` /
  `fs_lock_renew` — that persists one table per file/dir in a single
  `<DIR|FILE/uuid>/_lock` object. Reads are a plain GET; mutations are an OCC
  loop (`GetObject` + decode + apply + `PutObject If-Match:<etag>`, or
  `If-None-Match:*` to create), retried on a `412/409`. This is the same
  conditional-write primitive as the compactor lease.

`owner` is an opaque single-line token (hyperfs folds its per-mount client id
with the kernel `lock_owner`). Each lock carries an absolute `expire_ms`; an
expired entry is ignored and pruned on the next write, so a crashed holder's
locks free on TTL — the holder is expected to renew within the TTL. Blocking
acquisition is not implemented; the caller treats setlk as non-blocking. The
`_lock` object lives under the child's prefix, so it is reclaimed automatically
when the prefix is deleted (unlink / GC need no change).

## 11. Known limitations (PoC)

- Cross-directory rename / link still have a pre-check-vs-commit TOCTOU window,
  but it cannot lose data: the rename is intent-recovered and `fs_link`'s
  exclusive insert + nlink rollback make a raced link fail cleanly rather than
  clobber/leak.
- `nlink` is eventual: it reflects an unlink only after the parent is compacted.
- True process crashes can leave a nameless file (an `fs_link` crash between the
  nlink bump and the entry commit, or a create+unlink before any compact); this
  is a storage leak, never data loss, reclaimed by `fs_gc_orphans`.
- Cached `nlink` in a listing can lag for hard-linked files (advisory).
- `undelete` is unimplemented (the tombstone already preserves the full inode
  to enable it later).
- Advisory locks have no blocking acquisition (non-blocking only), cost an S3
  round-trip per op, and rely on TTL renewal to free a crashed holder's locks.
- `inode_mut` relies on a `&self -> &mut Inode` transmute inherited from the
  `hyperfile` trait surface; to be addressed upstream.

## 12. Engineering constraints

- The B-tree root is a fixed 56-byte inline area in the inode, but a directory
  entry is ~432 bytes, so the root must be an internal node from the first
  insert with values in 4 KiB meta-block leaves. This requires
  **`btree-ondisk >= 0.18.1`** (which fixes the large-value insert / lookup /
  delete paths).
- In debug builds the composed async future
  (`hyperdir -> hyperfile -> AWS SDK`, amplified by the ~432-byte value) can
  exceed the default ~2 MiB test-thread stack — a frame-size issue, not
  recursion. Use a large `RUST_MIN_STACK` (e.g. 64 MiB) or a release build.

## 13. Tests

`tests/e2e_s3.rs` (`#[ignore]`, requires real S3 credentials + `S3_BUCKET` /
`S3_REGION` + a large `RUST_MIN_STACK`) covers, end to end:

- directory lifecycle: create root, mkdir, compact, read_dir, same-dir rename,
  cross-dir rename, rmdir, gc;
- hard link + nlink lifecycle (GC reclaims a file only after the last link);
- retention (GC skips a tombstone until its retention expires);
- concurrent compaction (lease + OCC);
- CRC64 collision (two names in one slot, both resolvable, survive deletion of
  the other);
- cross-directory rename crash recovery (intent committed, not applied;
  `fs_recover_renames` completes it).

`tests/e2e_concurrent.rs` (same prerequisites) drives several operations
concurrently against one shared namespace and asserts interleaving-invariant
properties (every interleaving must satisfy them, not a specific winner):

- concurrent same-name hard link to distinct targets — exactly one wins, the
  winner's `nlink` is 2 and no loser's `nlink` is leaked;
- concurrent same-name create / same-name mkdir — the merged view converges to
  exactly one entry;
- concurrent same-source rename — the source ends up under exactly one name;
- concurrent duplicate unlink of one name on a 2-link file — `nlink` drops by
  exactly one (not two), so the surviving link is never orphaned;
- a foreground op racing an in-progress `compact` of the same dir — an unlink's
  delete and a create's entry are each folded exactly once;
- `fs_gc_orphans` racing an in-flight create — the grace window keeps the
  brand-new file from being reclaimed;
- mixed hard link (eager `nlink` bump) + unlink (compaction-deferred decrement)
  on one target — the net `nlink` is exact;
- concurrent `fs_recover_renames` of one committed cross-dir intent, and
  concurrent identical `fs_rename_across` — both converge idempotently;
- concurrent `fs_reclaim` — a file with `nlink>1` is refused, an orphan is
  reclaimed exactly once;
- create vs unlink of one name — at most one entry, never a dangling resolve;
- concurrent xattr (same-name last-write-wins, distinct names independent) and
  contended overlapping write locks (S3 OCC grants exactly one owner).
