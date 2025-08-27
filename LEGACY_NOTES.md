# LEGACY_NOTES

**Purpose:** Quick reference to implementation details in the old `datamgr` code (which is located in `legacy` subdirectory) that remain useful while we refactor per SPEC.md. The SPEC is authoritative for behavior; this file only points at reusable idioms and concrete patterns from the legacy code.

> **Do not copy semantics blindly.** SPEC introduces key changes (identity via specials+quantization, jagged meta, encryption hooks). Use legacy code for hashing/HDF5/FS safety patterns and pragmatic details.

---

## 0) Map: SPEC areas → legacy anchors

| SPEC area | Legacy file → anchors | Notes |
|---|---|---|
| §11 Ingest & Sealing (atomic writes, attrs, content hash) | `datamgr/atoms.py` → `publish_part`, `compute_semantic_content_hash`, `update_hasher_from_structured`, `update_hasher_from_h5_dataset` | Exact fsync/replace discipline; attrs written on HDF5; hashing order & string handling |
| §10 Jaggedness (padding + meta arrays) | *(not implemented in old code)* | Old code assumes fixed shapes only. We will add meta arrays in v2 |
| §8–9 Identity & DDL (quantization, specials) | `datamgr/manifest.py` → `ensure_key_columns`, `get_or_create_subset`, `find_subsets` | **Legacy uses float tolerance**, not specials/quantization. Do **not** reuse identity logic. |
| §4 Storage Adapters (HDF5 PartStore) | `datamgr/atoms.py` → HDF5 S⇄U converters, fsync helpers | Keep conversion and atomic patterns; wrap in adapter |
| §4 Storage Adapters (SQLite Catalog) | `datamgr/manifest.py` + `datamgr/manager.py` → schema JSON, part config, meta/data | Useful schema JSON wiring; indexes; read paths |
| §11 Crash-safety staging | `datamgr/ingest_core.py` → `Stager`, `select_and_claim_prefix`, reclaim/unclaim | Durable `synchronous=FULL` option; claim token flow |
| §11 Writer affinity & routing | `datamgr/ingest_core.py` → `Router.partition`, `stable_subset_key` | **Legacy uses DB-lookups + float_tolerance**; v2 will compute UUID purely from identity tuple |
| §7 Config & DI (PRAGMAs) | `datamgr/sqlite_loader.py`, `datamgr/atoms.py` (default_conn), `ingest_core._staging_conn_factory` | WAL2, busy_timeout, temp_store=MEMORY, cache_size hints |
| §24 Encryption hooks | *(not in old code)* | v2 introduces schema policy + per-part enc columns |
| §25 Hardening & access control | `datamgr/atoms.py` → {Subset/Dataset}Lease, fs perms helpers | Base advisory locks + fsync; v2 adds more checks |

---

## 1) Content hashing (carry forward)

**Files:** `datamgr/atoms.py`

### 1.1 Schema-coupled hashing
- `schema_signature_for_hash(dtype)` – encodes the *structured* dtype layout as JSON (field order, base dtypes, shapes) and feeds it into the hash. This avoids cross-platform differences.
- Both array and HDF5 hashers prepend this signature before data, ensuring identical schema → identical hash semantics.

### 1.2 String hashing details (important)
- `hash_utf8_lenpref_iter` normalizes strings to **NFC**, encodes to UTF‑8, and **length‑prefixes** each item before updating hash. This removes ambiguity (e.g., `"a\x00b"`).
- For HDF5 `S*` (byte strings), code decodes to UTF‑8 before iterating.

### 1.3 Chunked hashing order
- `update_hasher_from_structured(arr, max_chunk_bytes)` iterates rows in chunks; per field, updates hash with:
  - Unicode → normalized, length‑prefixed bytes
  - Numeric → contiguous memoryview
- HDF5 analog: `update_hasher_from_h5_dataset(dset, ...)` reads batches and applies same rules.

### 1.4 Digest algorithm
- `compute_semantic_content_hash(...)` → `blake2b(digest_size=16)` (hex). 16‑byte digest is sufficient for ids and dedupe keys.

**Action for v2:** Keep hashing primitives and string rules intact; extend to include **jagged meta arrays** (SPEC §10–11) in the hasher’s byte order (padded data first, then meta).

---

## 2) HDF5 storage conventions (carry forward)

**Files:** `datamgr/atoms.py`

### 2.1 Unicode storage in HDF5
- `h5_storage_dtype(np_dt)` maps Unicode `U*` → byte `S*` of equal byte length, preserving shapes.
- `to_h5_storage_array` encodes Unicode to UTF‑8 during write; `from_h5_storage_array` decodes on read.

### 2.2 Part file attributes
Written in `publish_part`:
- `part_uuid`, `subset_uuid`, `dataset_uuid`
- `created_at_epoch` (µs or µs‑like integer), `n_rows`
- `scheme_version` (from `StorageScheme`)
- `content_hash` (hex)

*(v2 will add encryption metadata when mode≠none; see SPEC §24.)*

### 2.3 Atomic write discipline
- Write to `*.h5.tmp`, flush HDF5, attempt **VFD fsync**; fallback to `os.fsync(fd)` on the tmp file.
- `os.replace(tmp, dst)` → atomic rename, then `fsync_dir(dirname(dst))`.
- Stale `*.h5.tmp` cleaned via `cleanup_stale_tmps_in_dir`.

**Action for v2:** Implement PartStore adapter around this exact sequence; add optional AEAD step per SPEC (encrypt bytes, then replace), and record enc columns in Catalog.

---

## 3) Storage scheme & sharded paths (carry forward)

**Files:** `datamgr/atoms.py`

- `StorageScheme(version=1, hash="sha256", depth, seglen)` with `validate_storage_scheme` bounds checking.
- `part_relpath(subset_uuid, part_uuid, scheme)`:
  - If `depth==0`: `subsets/<subset_uuid>/parts/v<ver>/<part_uuid>.h5`
  - Else: compute hex from `hash(subset_uuid + part_uuid)`, split into `depth` segments of `seglen` and nest directories accordingly.

**Action for v2:** Keep as‑is; ensure `.h5.enc` extension handling if encryption mode="h5"/"both".

---

## 4) Filesystem safety helpers (carry forward)

**Files:** `datamgr/atoms.py`

- `fsync_dir(path)` – directory descriptor fsync, guarded
- `makedirs_with_fsync(path)` – create nested dirs and fsync parents in order
- `cleanup_stale_tmps_in_dir(dirpath, older_than_seconds)` – remove stale `*.h5.tmp`
- `safe_unlink_inside(root, rel)` – resolves realpath and enforces containment
- `prune_empty_dirs(start, stop_at)` – upward pruning

These are used widely by publish/delete/gc flows.

---

## 5) Advisory locks (carry forward, expand)

**Files:** `datamgr/atoms.py`

- `SubsetLease(lockfile_path, hooks, ds_uuid, subset_uuid)` and `DatasetLease(lockfile_path, hooks, ds_uuid)`:
  - Try `fcntl.flock` (POSIX); fallback to `portalocker` if available.
  - Env override: `ALLOW_UNLOCKED_LEASE` permits no‑lock operation (legacy escape hatch; consider removing in strict mode).
- Lock paths: `subset_lock_path(ds_root, subset_uuid)` and `dataset_lock_path(ds_root)`.

**Action for v2:** Keep API; extend with SPEC §25 checks (owner/group/perms) during acquire; add metrics/events.

---

## 6) SQLite loader & PRAGMAs (carry forward, tweak)

**Files:** `datamgr/sqlite_loader.py`, plus connections in `atoms.py`, `manifest.py`, `ingest_core.py`

- Bundled **pysqlite3 (wal2)** wheels; extracts compatible wheel on import.
- `assert_compile_options(...)` currently checks for: `ENABLE_JSON1, ENABLE_FTS5, ENABLE_RTREE, ENABLE_STAT4, ENABLE_MATH_FUNCTIONS, ENABLE_NORMALIZE, ENABLE_DESERIALIZE, ENABLE_NAN_INF`.
  - **SPEC change:** *drop* `ENABLE_NAN_INF` from the assert.
- Connection PRAGMAs used consistently:
  - `PRAGMA journal_mode=wal2;`
  - `PRAGMA synchronous=NORMAL;` (staging durable path uses `FULL`)
  - `PRAGMA busy_timeout=5000;`
  - `PRAGMA foreign_keys=ON;`
  - `PRAGMA temp_store=MEMORY;`
  - `PRAGMA cache_size=-65536;`

**Action for v2:** Centralize in Catalog/Adapter; add `trusted_schema=OFF`, `query_only=ON` for read‑only opens (SPEC §23, §25).

---

## 7) Catalog schema & helpers (partially carry forward)

**Files:** `datamgr/manifest.py`, `datamgr/manager.py`

### 7.1 Catalog DB (global)
- `datasets(dataset_uuid PRIMARY KEY, alias UNIQUE, created_at_epoch, schema_json, storage_scheme_json)`
- `meta(key PRIMARY KEY, value)` – contains `database_uuid`, `created_at_epoch`.

### 7.2 Per‑dataset DB
- `subsets(subset_uuid PRIMARY KEY, created_at_epoch, marked_for_deletion, total_rows)` + indexes
- `parts(part_uuid PRIMARY KEY, subset_uuid, created_at_epoch, n_rows, scheme_version, file_relpath, marked_for_deletion, content_hash)` + indexes
- Helper APIs:
  - `ensure_dataset`, `ensure_dataset_db_initialized`
  - `load_schema` / `save_schema`
  - `get_storage_scheme`, `get_part_config`, `lock_part_config`
  - Query helpers: `find_subsets(...)` (with time filters), `list_marked_parts`, `gc_commit`, `fsck_dataset` (orphan scan and optional insertion)

### 7.3 Canonical dtype persistence (JSON)
- `dtype_to_canonical_json`/`dtype_from_canonical_json` in `manager.py` serve as source; SPEC keeps this shape.

**SPEC differences to apply in v2:**
- **Identity:** add REAL identity columns `<k>_s` (specials) and `<k>_q` (quantized) + UNIQUE composite index over expanded key order.
- **Parts:** add `part_stats_json`, `producer_id`, `batch_id`, and encryption columns (`enc_version`, `key_ref`, `nonce`, `tag`, `plaintext_size`).
- **Tamper chain:** add `batches`/`batch_parts` tables and `merge_log` (SPEC §9, §13, §25.5).

---

## 8) Staging DB & crash‑safe compaction (carry forward)

**Files:** `datamgr/ingest_core.py`

- Table: `staging_rows(id PK, subset_uuid, n_rows, created_at_epoch, payload BLOB, claimed_by, claimed_at)` with indexes.
- API:
  - `enqueue(subset_uuid, n_rows, payload)` – insert rows
  - `reclaim_stale(stale_after_seconds)` – clear claims older than cutoff
  - `select_and_claim_prefix(subset_uuid, part_rows, token)` – pick a prefix of rows and atomically claim them for compaction; allows a single large row > part_rows
  - `unclaim(token)`, `delete_claimed(token)`
  - `hot_subsets(limit)` – pick active subsets by oldest row id
- Durable mode: staging connection `synchronous=FULL` (configurable).

**Action for v2:** Keep the prefix‑claim strategy; ensure **part sealing** computes stats & content hash *before* Catalog insert; add optional checkpointing and metrics.

---

## 9) Ingest orchestration (carry forward with changes)

**Files:** `datamgr/affinity_ingest` (old multi‑writer path), `datamgr/ingest_core.py`, `datamgr/manager.py`

- **Parallel compute + writer affinity:** `ingest_with_subset_affinity` computes payloads in parallel (joblib) and routes by subset to one of N writer processes via `Router.partition`.
- **Writer loop:** two modes
  - Non‑crash‑safe: direct `Manager.add(... force_flush=False)` to buffer, then `flush()` at end.
  - Crash‑safe: enqueue to `Stager`, compact when queued rows ≥ part_rows, seal parts atomically.
- **Payload format:** pickled tuple `(subset_keys, field_data_dict, is_group)`; staging stores magic header `b"DMST\x01"` + pickle.
- **Dedup & compaction threshold:** `part_rows` from part config; compaction merges rows until threshold.

**SPEC changes:**
- Router must compute deterministic shard from the identity tuple without DB roundtrips (legacy calls `manifest` to resolve subset id).
- Sealing must include **PartStats** and **jagged meta**; and write `subset_keys_json` into HDF5 attrs (SPEC §11).

---

## 10) Manager behaviors (keep shape, change identity/jagged)

**Files:** `datamgr/manager.py`

### 10.1 Canonical dtype management
- `dict_to_structured` validates fields, rejects object/complex/datetime, enforces Unicode max 256 (legacy constant), casts numeric kinds to common widths; supports scalar vs group (row arrays) shaping rules.
- `ensure_canonical_dtype` locks canonical dtype on first write (if absent), persists JSON to both catalog and dataset `meta` tables; later attempts check safe casting and **may widen Unicode** by comparing widths (`maybe_widen_text_fields`).

### 10.2 Buffering & sealing
- Buffers per `(ds_uuid, subset_uuid)` with `parts`, `n`, `bytes`.
- Trigger flush at `part_rows` unless forced; `flush()` drains all.
- Sealing path holds a `SubsetLease`, calls `publish_part`, then appends rows/bytes counters; dedupe is handled in `publish_part` + Catalog insert (content_hash uniqueness).

**SPEC upgrades:**
- Add **jagged padding/meta** in `dict_to_structured` pipeline (or pre‑normalize layer) and ensure hasher includes meta arrays.
- Add AAD contract construction for AEAD (even if encryption is disabled now).

---

## 11) Legacy identity model (do **not** carry forward)

**Files:** `datamgr/manifest.py`, `datamgr/ingest_core.Router`

- Legacy equality for REAL keys uses `float_tolerance` ranges around values; `get_or_create_subset` queries by raw REAL within ±tolerance.
- `Router.resolve_subset_uuid` caches via `stable_subset_key` string (rounded by `float_tolerance`), then calls into Manifest to create/fetch subset on demand.

**SPEC v2 replacement:**
- Identity is a **dual‑key** per REAL: `(k_s, k_q)` where `k_s ∈ {0:Normal,1:NaN,2:+Inf,3:-Inf}` and `k_q = round(v * quantization[k])` when `k_s==0`.
- Deterministic `subset_uuid = UUID(blake2b16(','.join(map(str, identity_tuple))))` computed *without* DB access.
- Catalog adds the `<k>_s`, `<k>_q` columns and a UNIQUE index over expanded key order; equality queries hit these, ranges use raw REAL.

---

## 12) Hooks & events (carry forward and expand)

**Files:** `datamgr/atoms.py` → `Hooks`

- No‑op hook interface with lifecycle events: buffer enter, seal to spill, publish begin/fsynced/renamed/dir_fsynced, manifest txn begin/commit/rollback, subset lease acquire/release.

**Action for v2:** Keep hooks; route them through an EventBus for SPEC §6 (events/metrics) and TUI watch views.

---

## 13) Delete/GC & fsck utilities (carry forward)

**Files:** `datamgr/manager.py` (`delete`, `soft_delete`), `datamgr/manifest.py` (`list_marked_parts`, `gc_commit`, `fsck_dataset`)

- Soft delete marks subsets/parts then `delete()` removes marked part files and prunes empty dirs; `gc_commit` updates `total_rows` and drops subsets with no live parts.
- `fsck_dataset` scans disk, finds orphan `.h5` files, and can insert them into the DB if their subset exists; computes `content_hash` if missing in attrs.

**Action for v2:** Extend with tamper‑chain verification and optional content‑hash sampling (SPEC §25.7).

---

## 14) What’s intentionally **new** in v2 (not in legacy)

- Jagged fields + meta arrays; padding rules and overflow errors (SPEC §10).
- Part statistics (`part_stats_json`) + planner pushdown and pruning (SPEC §12).
- Change feed (`batches`, `batch_parts`) and `merge_log`; idempotent merges (SPEC §13).
- Encryption schema policy + per‑part enc metadata; AEAD seam (SPEC §24).
- Hardening posture checks, auditor CLI, tamper‑evidence chain (SPEC §25).

---

## 15) Porting checklist

1. **Hasher**: lift legacy hashing primitives; extend to include jagged meta; keep NFC + length‑prefix for strings.
2. **PartStore(HDF5)**: wrap legacy `publish_part` logic; add AAD construction & (future) AEAD step; support `.h5`/`.h5.enc` naming.
3. **FS utils & locks**: reuse helpers; add SPEC §25 validations on acquire.
4. **Catalog**: start with legacy schema JSON plumbing; add new columns/tables per SPEC §9; centralize PRAGMAs and read‑only safety.
5. **Identity**: replace legacy float‑tolerance model with specials+quantization; compute subset UUID locally.
6. **Ingest**: keep staging/compaction patterns; route by deterministic UUID; ensure part stats & jagged meta recorded at seal.
7. **Manager API**: keep function surface; adapt to jagged + new identity; ensure warnings/overflow errors match SPEC.
8. **Tests**: reuse patterns; add new unit tests for quantization/specials, jagged padding, AAD construction, tamper chain.

---

## 16) Pitfalls & notes

- **String hashing**: do not drop NFC normalization or length‑prefixing; otherwise cross‑platform hashes may diverge.
- **HDF5 fsync**: keep the VFD handle fsync attempt; some platforms require fallback.
- **Atomicity**: never write directly to final path; preserve `.tmp` + `os.replace` + dir fsync.
- **Dedup key**: dedupe by `(subset_uuid, content_hash)`; ensure content hash includes **exact** bytes (post‑padding) and meta arrays in v2.
- **PRAGMAs**: maintain WAL2; durable staging uses `synchronous=FULL`.
- **Locks**: handle non‑POSIX environments with `portalocker` fallback; consider removing `ALLOW_UNLOCKED_LEASE` in production builds.
- **Unicode widths**: legacy widens canonical Unicode fields; v2 can retain widening but must cap at policy.

---

## 17) Quick glossary (legacy symbols you’ll see)

- **Hashing**: `schema_signature_for_hash`, `hash_utf8_lenpref_iter`, `update_hasher_from_structured`, `compute_semantic_content_hash`
- **HDF5**: `h5_storage_dtype`, `to_h5_storage_array`, `from_h5_storage_array`, `publish_part`
- **FS/locks**: `fsync_dir`, `makedirs_with_fsync`, `cleanup_stale_tmps_in_dir`, `SubsetLease`, `DatasetLease`
- **Scheme**: `StorageScheme`, `part_relpath`
- **Catalog**: `Manifest.*` (schema JSON, DDL, queries), `Manager.*` (meta/data/add/flush)
- **Staging**: `Stager.enqueue/claim/delete`, `reclaim_stale`, `hot_subsets`
- **Ingest**: `ingest_with_subset_affinity`, `writer_loop`, `Router.partition`

---

**End.**

