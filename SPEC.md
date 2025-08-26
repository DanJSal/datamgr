# SPEC — datamgr Architecture, Identity, Jaggedness, Planner, Merge, TUI, and Encryption Hooks

**Scope:** This is the authoritative, implementation-ordered spec for refactoring and extending datamgr. It merges all design notes into a single, buildable plan.

**Non-goals:** Backwards compatibility and legacy DB migrations (none exist).

---

## Table of Contents

1. [Core Principles & Non-Goals](#1-core-principles--non-goals)
2. [Repository Layout & Build Order](#2-repository-layout--build-order)
3. [Core Abstractions (pure logic)](#3-core-abstractions-pure-logic)
4. [Storage Adapters (I/O)](#4-storage-adapters-io)
5. [Services (business logic)](#5-services-business-logic)
6. [Events & Metrics](#6-events--metrics)
7. [Config & Dependency Injection](#7-config--dependency-injection)
8. [Schema & Identity (Quantization, Specials & Deterministic IDs)](#8-schema--identity-quantization-specials--deterministic-ids)
9. [Authoritative DDL (Catalog & Per-Dataset DB)](#9-authoritative-ddl-catalog--per-dataset-db)
10. [Jaggedness (variable-length arrays)](#10-jaggedness-variable-length-arrays)
11. [Ingest & Sealing (buffers, hashing, stats, crash-safety)](#11-ingest--sealing-buffers-hashing-stats-crash-safety)
12. [Planner v0 (metadata-driven)](#12-planner-v0-metadata-driven)
13. [Merging Databases (change feed, idempotent merges)](#13-merging-databases-change-feed-idempotent-merges)
14. [Navigator & TUI (read-only)](#14-navigator--tui-read-only)
15. [Public Facade API](#15-public-facade-api)
16. [Breaking API Changes](#16-breaking-api-changes)
17. [Defaults, Guardrails, Conventions](#17-defaults-guardrails-conventions)
18. [Performance Notes & Safety](#18-performance-notes--safety)
19. [Operational Tools (rebuild, incomplete ingest detector)](#19-operational-tools-rebuild-incomplete-ingest-detector)
20. [Testing Plan](#20-testing-plan)
21. [Day-1 Rollout Plan](#21-day-1-rollout-plan)
22. [Open Questions / Later Work](#22-open-questions--later-work)
23. [Appendix: SQLite compile options & PRAGMAs](#23-appendix-sqlite-compile-options--pragmas)
24. [Encryption-Ready Hooks (AEAD, KMS, Policy)](#24-encryption-ready-hooks-aead-kms-policy)
25. [Hardening & Access Control (OS, Container, Tamper-Evidence)](#25-hardening--access-control-os-container-tamper-evidence)

---

## 1. Core Principles & Non-Goals

**Principles**

- Deterministic identity: Subsets are identified by a canonical identity tuple derived from key values. For REAL keys, identity uses dual-key: a specials code (NaN/+Inf/−Inf/Normal) plus a quantized integer when Normal. A deterministic UUID is derived via `blake2b16 → UUID`.
- Immutable parts: HDF5 part files are never modified post-seal; dedupe by `(subset_uuid, content_hash)`.
- Idempotence: Unique indexes + change feed make retries safe.
- Separation of concerns: Core logic is pure; storage via adapters; orchestration via services; API is thin.
- Crash-safety: Per-batch transactions; write `*.tmp → fsync → os.replace → fsync` directory.
- Raw analytics preserved: REAL keys kept for range queries; equality uses identity columns (specials/quantized).

**Non-Goals**

- Backwards/legacy DB compatibility.
- Full schema evolutions beyond widening Unicode/jagged canonical widths (migrations later).

---

## 2. Repository Layout & Build Order

```
datamgr/
  core/        schema.py, keys.py, hashing.py, jagged.py, plan.py, errors.py
  storage/     part_store_h5.py, catalog_sqlite.py, sqlite_loader.py
  services/    ingest.py, merge.py, planner.py, migrate.py     # migrate later
  api/         manager.py, navigator.py, cli.py
  util/        fs.py, events.py, config.py, warnings.py
  tests/       unit/, integration/
```

**Build order:** Interfaces → Identity/DDL → Jaggedness → Ingest/Sealing → Planner → Merge → Navigator/TUI → Ops tools.

---

## 3. Core Abstractions (pure logic)

- **KeyNormalizer**
  - **Input:** `key_schema`, `key_order`, quantization map, `subset_keys`.
  - **Output:**
    - `identity_tuple`: ordered tuple combining, for each REAL key: `(k_s, k_q)` where `k_s ∈ {0:Normal,1:NaN,2:+Inf,3:-Inf}`, and for non-REAL: raw value; for Normal REAL, `k_q = round(k * scale)`.
    - `subset_uuid = UUID(blake2b16(','.join(map(str, identity_tuple))))`.
  - **Notes:** Supports queries on specials; range queries remain on raw REAL columns.
- **StructuredDType / FieldSpec / JaggedSpec**
  - Canonical dtype; jagged `vary_dims` per field; validates padding/overflow.
- **ContentHasher**
  - Hash order: padded data bytes for each field → jagged meta arrays (`*_len` / `*_shape`).
  - Algorithm: `blake2b(digest_size=16)`.
- **PartStats**
  - Per part: `n_rows`; for each jagged field → 1-D: `min_len`, `max_len`, `avg_len`, `full_rows`; k-D analogs (per-dim min/max/avg); optional `len_encoding ∈ {constant, codebook, plain}`, `codebook_K`; optional compressed sizes.
- **PlanIR**
  - Selected parts, predicate rewrite summary, ordering/cost hints.
- **Errors**
  - `SchemaMismatch`, `DataExceedsCanon`, `IdentityConflict`, `InvalidKeyValue`, etc.
- **Human-readable key string (for logs/TUI)**
  - `stable_subset_key(…)` retained for display/search, not for identity.
- **CryptoProvider (hook; no-op by default)**
  - `generate_data_key() -> (key_bytes, key_ref)`
  - `encrypt(plaintext, *, key_ref, aad) -> (ciphertext, nonce, tag)`
  - `decrypt(ciphertext, *, key_ref, nonce, tag, aad) -> plaintext`
- **KeyManager (optional wrapper for KMS/HSM later)**
  - `get_key(key_ref)`, `rotate(key_ref)`, `revoke(key_ref)`

---

## 4. Storage Adapters (I/O)

- **PartStore (HDF5)**
  - `write_part(ds_root, ds_uuid, subset_uuid, rows, subset_keys, scheme, compression, compression_opts) -> (part_uuid, file_relpath)`
  - Writes attrs, fsyncs, atomic replace; no DB writes.
  - **EncryptedPartStore wrapper seam:** initially pass-through; later AEAD encrypts `.h5` bytes to `.h5` (inline header) or `.h5.enc` using CryptoProvider; records encryption metadata.
- **DatasetCatalog (SQLite)**
  - `ensure_dataset`, `ensure_key_columns`, `get_or_create_subset`, `find_subsets`, `mark_`, `gc_commit`, `meta()`, `data()` selection/pruning.
  - Persists schema JSON (`key_schema`, `key_order`, `dtype_descr`, `part_config`, `quantization`, `jagged`, `encryption`).
  - Stores `parts.part_stats_json`, `content_hash`, `producer_id`, `batch_id`, and encryption metadata (`enc_version`, `key_ref`, `nonce`, `tag`, `plaintext_size`).
- **ChangeFeed**
  - `batches` / `batch_parts` CRUD.
- **MergeLog (central/public only)**
  - `(producer_id, bid)` exactly-once.
- **SchemaRegistry**
  - Exposes dataset schema JSON and storage scheme version.
- **DB open hook (future SQLCipher)**
  - `open_db(path, *, sqlite_uri=None, sqlcipher_key=None) -> Connection`

---

## 5. Services (business logic)

- **IngestService**
  - Pads jagged inputs; locks canonical; computes `content_hash` + `PartStats`; calls PartStore; records parts & batches.
  - When encryption enabled (later): uses EncryptedPartStore; computes AEAD with AAD contract; writes enc metadata.
- **PlannerService (v0)**
  - Predicate rewrite (jagged → meta); part pruning; byte-cost model; operator selection; returns `PlanIR`.
- **MergeService**
  - `merge_local(…)` (see §13); enforces encryption and schema invariants; supports ciphertext copy/verify when both encrypted.
- **MigrationService (later)**
  - Retolerance/rekey/schema forks.

---

## 6. Events & Metrics

- Events: `ingest.part_sealed`, `batch.committed`, `merge.started/progress/finished`.
- Metrics snapshots: per-tier queue depth, inflight bytes, merge rate, WAL size.
- Used by TUI “watch” views and server orchestration.

---

## 7. Config & Dependency Injection

```python
from dataclasses import dataclass, field
from typing import Literal

@dataclass
class Config:
    db_root: str
    part_rows: int = 100_000
    chunk_mb: float = 8.0
    compression: str | None = None
    compression_opts: int | None = None
    quantization: dict[str, float] = field(default_factory=dict)  # REAL scales
    jagged: dict[str, dict] = field(default_factory=dict)         # {"field":{"vary_dims":[...]}}
    # Legacy float_tolerance retained only for data comparisons/ranges if needed (not identity)
    float_tolerance: float = 1e-6

    # Encryption hooks (no-ops until enabled)
    encryption_mode: Literal["none","h5","sqlite","both"] = "none"
    encryption_algorithm: str = "AES-256-GCM"
    kms_provider: str | None = None
    kms_default_key_ref: str | None = None
    require_encryption: bool = False
    key_rotation_days: int | None = 180

    # Hardening hooks (enabled gradually)
    enforce_posix_perms: bool = True           # verify 0700/0750 owner+group on roots
    data_owner_user: str | None = None         # service account name for on-disk ownership checks
    data_owner_group: str | None = "datamgr"   # shared group for read-only viewers
    advisory_locking: bool = True              # flock() lockfiles on roots and DBs
    lock_dir: str | None = None                # separate lock directory (tmpfs recommended)
    audit_log_enabled: bool = True
    audit_log_path: str | None = None          # default: <db_root>/logs/audit.log
    tamper_chain_enabled: bool = True          # hash-chained batches (see §25)
    container_required: bool = False           # fail init if not running in approved sandbox
    readonly_mounts_ok: bool = True            # enforce read-only bind-mounts for readers
```

All services accept injected PartStore, DatasetCatalog, KeyNormalizer, ContentHasher, and optional CryptoProvider/KeyManager.

---

## 8. Schema & Identity (Quantization, Specials & Deterministic IDs)

**Dual-key identity for REAL keys**

- For each REAL key `k`:
  - Specials code `k_s` (INTEGER): `0: Normal, 1: NaN, 2: +Inf, 3: -Inf`.
  - Quantized `k_q` (INTEGER): `round(k * quantization[k])` only meaningful when `k_s == 0`.
- For TEXT/INTEGER/BOOLEAN keys: identity is the raw column value.
- **Identity column list:** For each REAL key, identity contributes two columns (`k_s`, `k_q`); for non-REAL, the raw column. Composite UNIQUE index uses this ordered list.
- **Deterministic subset_uuid:** `UUID(blake2b16(','.join(map(str, identity_tuple))))`.

**Stored schema JSON**

```json
{
  "key_schema": {"temp":"REAL","lat":"REAL","site":"TEXT"},
  "key_order": ["site","lat","temp"],
  "dtype_descr": "",
  "part_config": {"part_rows":100000,"compression":null,"compression_opts":null},
  "quantization": {"temp":1000.0,"lat":1000000.0,"lon":1000000.0},
  "jagged": {"seq":{"vary_dims":[0]}, "patch":{"vary_dims":[0,1]}},
  "encryption": {
    "mode": "none",
    "algorithm": "AES-256-GCM",
    "kms_provider": null,
    "key_policy": {"default_key_ref": null, "rotation_days": 180}
  }
}
```

**Lookup rules**

- **Equality on REAL keys:**
  - Finite value `v`: compute `v_q`, filter `k_s = 0 AND k_q = ?`.
  - NaN / ±Inf: filter `k_s = 1|2|3` (no `k_q` predicate).
- **Range on REAL keys:** use raw REAL `BETWEEN` (preserves analytic semantics).

---

## 9. Authoritative DDL (Catalog & Per-Dataset DB)

Names below are illustrative; generate identity index columns from `key_schema`.

### Catalog DB (global)

```sql
CREATE TABLE IF NOT EXISTS datasets(
  dataset_uuid TEXT PRIMARY KEY,
  alias TEXT UNIQUE NOT NULL,
  created_at_epoch INTEGER NOT NULL,
  schema_json TEXT NOT NULL,
  storage_scheme_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta(
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
-- meta includes: database_uuid, created_at_epoch, (optional) lib_version
```

### Per-dataset DB

#### subsets

```sql
CREATE TABLE IF NOT EXISTS subsets(
  subset_uuid TEXT PRIMARY KEY,
  created_at_epoch INTEGER NOT NULL,
  marked_for_deletion INTEGER NOT NULL DEFAULT 0,
  total_rows INTEGER NOT NULL DEFAULT 0
);

/* Initialization path adds key columns:

- Example REAL key 'lat':
  ALTER TABLE subsets ADD COLUMN lat REAL;                  -- raw REAL (for ranges)
  ALTER TABLE subsets ADD COLUMN lat_s INTEGER NOT NULL DEFAULT 0;    -- specials code
  ALTER TABLE subsets ADD COLUMN lat_q INTEGER NOT NULL DEFAULT 0;    -- quantized identity (valid only when lat_s=0)

- Another REAL key 'lon':
  ALTER TABLE subsets ADD COLUMN lon REAL;
  ALTER TABLE subsets ADD COLUMN lon_s INTEGER NOT NULL DEFAULT 0;
  ALTER TABLE subsets ADD COLUMN lon_q INTEGER NOT NULL DEFAULT 0;

- TEXT key 'site':
  ALTER TABLE subsets ADD COLUMN site TEXT;

- Composite UNIQUE identity index (order = key_order expanded into s/q pairs for REALs)
  CREATE UNIQUE INDEX IF NOT EXISTS uniq_subsets_identity
    ON subsets(lat_s, lat_q, lon_s, lon_q, site);
*/

CREATE INDEX IF NOT EXISTS idx_subsets_epoch_subset_live
  ON subsets(created_at_epoch, subset_uuid)
  WHERE marked_for_deletion = 0;

/* Optional convenience for merges/search:
- ALTER TABLE subsets ADD COLUMN subset_qnorm TEXT;
- CREATE UNIQUE INDEX IF NOT EXISTS idx_subsets_norm ON subsets(subset_qnorm);
*/
```

#### parts

```sql
CREATE TABLE IF NOT EXISTS parts(
  part_uuid TEXT PRIMARY KEY,
  subset_uuid TEXT NOT NULL,
  created_at_epoch INTEGER NOT NULL,
  n_rows INTEGER NOT NULL,
  scheme_version INTEGER NOT NULL DEFAULT 1,
  file_relpath TEXT NOT NULL,
  marked_for_deletion INTEGER NOT NULL DEFAULT 0,
  content_hash TEXT NOT NULL,
  producer_id TEXT,
  batch_id TEXT,
  part_stats_json TEXT,
  -- Encryption metadata (hooks; enc_version=0 means plaintext)
  enc_version INTEGER NOT NULL DEFAULT 0,
  key_ref TEXT,
  nonce BLOB,
  tag   BLOB,
  plaintext_size INTEGER,
  FOREIGN KEY(subset_uuid) REFERENCES subsets(subset_uuid) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_parts_subset_contenthash
  ON parts(subset_uuid, content_hash);

CREATE INDEX IF NOT EXISTS idx_parts_subset_epoch_uuid_live
  ON parts(subset_uuid, created_at_epoch, part_uuid)
  WHERE marked_for_deletion = 0;

CREATE INDEX IF NOT EXISTS idx_parts_batch_id ON parts(batch_id);
```

#### change feed

```sql
CREATE TABLE IF NOT EXISTS batches(
  bid TEXT PRIMARY KEY,
  created_at INTEGER NOT NULL,
  schema_fingerprint TEXT NOT NULL,
  -- Tamper-evident chain (optional but recommended)
  prev_hash TEXT,
  entry_hash TEXT
);

CREATE TABLE IF NOT EXISTS batch_parts(
  bid TEXT NOT NULL,
  part_uuid TEXT NOT NULL,
  PRIMARY KEY(bid, part_uuid)
);
```

#### merge log (central/public datasets only)

```sql
CREATE TABLE IF NOT EXISTS merge_log(
  producer_id TEXT NOT NULL,
  bid TEXT NOT NULL,
  merged_at INTEGER NOT NULL,
  PRIMARY KEY(producer_id, bid)
);
```

#### tamper config (optional)

```sql
-- Applied only if tamper_chain_enabled is true
CREATE TABLE IF NOT EXISTS tamper_cfg(
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
/* keys:
  'hash_salt' : random base64 salt
  'chain_version' : '1'
*/
```

---

## 10. Jaggedness (variable-length arrays)

**Goals**

- Support variable-length subarrays with fixed on-disk shapes for compression, hashing, and dedupe.

**Canonical dtype**

- Locks max shape per jagged field (e.g., `seq:(MAX_T,)`, `patch:(MAX_H,MAX_W,C)`); post-padding only.

**Per-row metadata**

- 1-D field `F`: `F_len : uint16|uint32` (smallest fitting).
- k-D field `F`: `F_shape : int16|int32[k]` for varying dims.

**Accepted input forms for** `add(…, is_group=True)`

- Unpadded lists → library pads, emits meta.
- Padded + meta → `(F, F_len)` or `(F, F_shape)`.
- Exact canonical arrays → meta optional.

**Initialization & locking**

- Pre-init (`Manager.init_dataset(…)`) can lock canonical dtype & jagged vary-dims.
- First add (if not pre-inited): compute batch max per field, lock canonical, pad smaller rows; require meta if user supplies partially filled padded arrays.

**Padding values**

- Numeric → `0`, Bool → `False`, Unicode → `""` (avoid NaN for hashing/compression).

**Warnings & errors**

- Aggregate warning per field/batch only when library pads; overflow (observed > canonical) → error.

**Hashing**

- Content hash includes padded data bytes then meta arrays.

**Schema JSON**

```json
{
  "jagged": { "seq": {"vary_dims": [0]}, "patch": {"vary_dims": [0, 1]}}
}
```

---

## 11. Ingest & Sealing (buffers, hashing, stats, crash-safety)

**Buffers**

- Keyed by `(ds_uuid, subset_uuid)`:

```json
{"parts":["np.ndarray","..."], "n":0, "bytes":0, "keys":"subset_keys"}
```

- Spill threshold: `part_rows` (from `part_config`) or explicit `chunk_rows`.

**Sealing:**

- Must receive `subset_keys` (written into HDF5 attrs as `subset_keys_json`).

- Writes attrs: `subset_uuid`, `dataset_uuid`, `created_at_epoch`, `n_rows`, `scheme_version`, `content_hash`.

- Content hash computed on plaintext padded rows + meta arrays.

- **Encryption-ready AAD contract (decided now, implemented later):**

  AAD = concat(

  - `dataset_uuid`, `subset_uuid`, `part_uuid`,
  - `schema_fingerprint`, `storage_scheme_version`,
  - `quantization_digest`,       // hash of quantization map
  - `content_hash`               // hash of plaintext payload

  )

- **Crash safety:** write `.tmp → fsync` file → `os.replace` → fsync dir.

- **Per-part statistics:** compute at seal; serialize to `parts.part_stats_json`.

- **Hardening hooks:** create advisory lockfile per dataset during seal; verify perms (see §25).

**Writer affinity**

- Router partitions by deterministic `subset_uuid` computed from identity tuple (no DB round-trip).

**Zero-copy registration helper (optional)**

- `register_existing_part(…)` for importing files idempotently via `(subset_uuid, content_hash)`.

---

## 12. Planner v0 (metadata-driven)

**Inputs**

- `parts.part_stats_json`, `n_rows`, optional compressed sizes; query predicates (incl. jagged).

**Pipeline**

1. Predicate rewrite/pushdown (e.g., `exists(seq)` → `seq_len > 0`; `length(seq) ≥ k` → `seq_len ≥ k`; k-D analogs).
2. Part pruning (bounds elimination; combine with subset/time filters).
3. Costing & ordering (byte model; rows-per-byte; selectivity-first).
4. Operator selection (constant/codebook/plain).
5. Execution (late materialization; bounded prefix; optional bucketing; parallel by parts).
6. Result assembly (stable order only if requested).

**Surface**

- `Manager.meta(…)` exposes stats without opening HDF5.

---

## 13. Merging Databases (change feed, idempotent merges)

**Core**

- Immutable parts; dedupe by `(subset_uuid, content_hash)`. Batches are unit of change; `MergeLog` ensures exactly-once per `(producer_id, bid)`.

**Dataset invariants required to merge**

- `schema_fingerprint` equal.
- `storage_scheme_version` equal.
- `quantization` maps equal exactly.
- Encryption policy (schema JSON) equal; else require explicit fork or `--reencrypt` (later).

**Seal-time**

- Set `producer_id`, generate `batch_id`, compute `content_hash`, store `PartStats`, insert into `batch_parts` and ensure `batches` row.

**API**

```python
def merge_local(src_root, src_db, dst_root, dst_db,
                copy_mode="hardlink",         # "hardlink" | "copy" | "none"
                verify_hash=False,
                allow_schema_mismatch=False,  # if True, fork dataset in dst
                dry_run=False) -> "MergeReport":
    ...
```

**Algorithm**

1. Compare invariants (incl. encryption policy); mismatch → error or fork (if allowed).
2. `unmerged = src.batches - dst.merge_log`.
3. For each `bid` (single txn in dst):
   - Resolve subsets by identity columns (or optional normalized string).
   - Skip parts present by `(subset_uuid, content_hash)`.
   - Copy/hardlink/reflink part files; if encrypted on both sides: copy ciphertext; `verify_hash` optional; AEAD tag verify (later) using AAD.
   - Insert parts rows (+ `part_stats_json`, enc metadata), update `subsets.total_rows`.
   - Mark `merge_log(producer_id,bid)`.
   - If `tamper_chain_enabled`: compute `entry_hash = H(prev_hash || bid || part_uuid… || content_hash || created_at || salt)`.

**Safety**

- Idempotent; `.tmp → replace`; per-batch txn; `--dry-run` prints plan.

---

## 14. Navigator & TUI (read-only)

**Navigator API**

- `ListNodes(path) -> [Node]` where `/datasets/<alias>/{meta,subsets,parts,batches}`
- `GetMeta(dataset_alias) -> dict` (schema fingerprint, part\_config, counts, storage info, encryption posture)
- `ListSubsets(dataset, filters, page) -> Page[SubsetRow]`
- `ListParts(dataset, subset_uuid, page) -> Page[PartRow]`
- `TailBatches(dataset, n) -> [BatchSummary]`
- `ShowBatch(dataset, bid) -> [PartInBatch]`
- `PeekPart(dataset, part_uuid, *, fields=None, rows=64) -> table/ndarray`
- Optional search via JSON1/FTS5.

**TUI commands**

- `use; cd, ls, tree`
- `meta, schema, keys`
- `subsets [filters], parts`
- `batches, batch <id>, peek <part> [--rows N --fields a,b]`
- `watch merges [--tier 1], dbstat, sqltop` (feature-probed)

**Live views**

- Subscribe to EventBus: `ingest.part_sealed`, `batch.committed`, `merge.*`
- Metrics: queue depth, inflight bytes, merge rate, WAL size.
- Expose encryption posture: dataset `encryption.mode`, `kms_provider`, rotation age; parts `enc_version`, `key_ref` (redacted), `plaintext_size`; later: verify `tag`.
- Hardening posture: show owner/group/perm checks, container/sandbox status, advisory lock presence, tamper chain status (see §25).

**Perf/Safety**

- Pagination; rate-limit peek; read-only defaults.

---

## 15. Public Facade API

**Initialization (explicit pre-init supported)**

```python
def init_dataset(

    dataset_name: str,
    *,
    key_schema: dict[str, str] | None = None,      # optional; else inferred on first add
    key_order: list[str] | None = None,            # optional; else inferred/sorted
    dtype_descr: "np.dtype | str | None" = None,   # canonical dtype or JSON
    part_config: dict | None = None,               # {"part_rows":..., "compression":..., "compression_opts":...}
    quantization: dict[str, float] | None = None,  # REAL key scales
    jagged: dict | None = None,                    # {"field":{"vary_dims":[...]}}
    encryption: dict | None = None                 # {"mode":..., "algorithm":..., "kms_provider":..., "key_policy":...}

) -> None: ...
```

**Ingest**

- `Manager.add(…)` (pads per rules; seals when thresholds hit).
- `Manager.flush()` (force seal all buffers).

**Reading**

- `Manager.meta(…)` returns dataset info, subsets, parts (including `part_stats_json`, encryption metadata).
- `Manager.data(meta_info)` loads HDF5 parts selected by Planner (planner-aware later).

**Merge**

- `Manager.merge_local(…)` wrapper over `MergeService`.

**Navigator**

- `Navigator.*` as above for TUI.

---

## 16. Breaking API Changes

- `Manifest.get_or_create_subset(…)`: removed `float_tolerance`; identity now via specials + quantization.
- `Manifest.ensure_key_columns(…, quantization: dict[str, float] | None = None)` now persists quantization; adds REAL `_s`/`_q` columns and identity UNIQUE on first call.
- `atoms.publish_part(…, subset_keys: dict[str,Any], …)` required to embed keys in HDF5 attrs.
- `ingest_core.Router.partition(…)` computes deterministic shard via `subset_uuid` from identity tuple (no DB hit).

---

## 17. Defaults, Guardrails, Conventions

- Quantization: General numeric `1e3` (millis); Geo lat/lon `1e6` (microdegree); Time-like pick per cadence.
- Specials: REAL keys may be Normal/NaN/+Inf/−Inf; equality over REAL uses `(k_s, k_q)`; range uses raw.
- Jagged padding: numeric `0`, bool `False`, Unicode `""`.
- Warnings: aggregate per field when the library pads; silent if user provided valid meta; overflow → error.
- Hashing: `blake2b(16)`, padded data then meta arrays.
- SQLite PRAGMAs: `WAL2`, `synchronous=NORMAL`, `busy_timeout=5000`, `temp_store=MEMORY`, `cache_size=-65536`.
- Encryption: default `mode="none"`; dataset schema includes encryption block for policy fingerprinting.

---

## 18. Performance Notes & Safety

- Avoid HDF5 reads in listings: expose frequently accessed metadata via SQL (`part_stats_json`, counts).
- Buffer size queries: don’t auto-load heavyweight HDF5 stats; make opt-in.
- Planner pushdown reduces I/O (late materialization).
- Crash-safety: atomic file moves, per-batch txns, idempotent uniqueness.
- Periodic `ANALYZE`; `PRAGMA optimize`; via `Catalog.optimize()`.

---

## 19. Operational Tools (rebuild, incomplete ingest detector)

- **Rebuild tool (copy-and-swap) to change immutable aspects (quantization, canonical jagged widths):**

```python
def rebuild_dataset_or_subsets(
    dataset: str,
    *,
    subsets: list[str] | None = None,
    new_quantization: dict[str, float] | None = None,
    new_jagged: dict | None = None,
    dry_run: bool = False
) -> "Report": ...
```

- **Incomplete ingest detector:** summarizes `staging_rows` backlog, queued rows per subset, and any unflushed buffers (exposed via `Manager.meta()` / Navigator).
- **Detailed logs** everywhere feasible; **redaction** utility to prevent leaking keys/nonces/tags when encryption is enabled.
- **Hardening auditor (new):** `datamgr-audit --dataset <alias>` prints perms/ownership, lock presence, mount flags, container status, tamper-chain head (see §25).

---

## 20. Testing Plan

**Unit**

- KeyNormalizer: quantization, specials mapping, UUID determinism.
- Jagged padding: accepted forms, warnings, overflow errors.
- ContentHasher: stability across platforms; padding/meta ordering.
- PartStats: correctness for 1-D and k-D fields.
- Planner rewrite/pruning: boundaries; codebook vs plain.
- Encryption hooks: AAD construction; FakeKMS; tag verification logic (when implemented), log redaction.
- Hardening: lockfile creation, POSIX mode checks, audit output formatting, tamper chain hash.

**Integration**

- Ingest & sealing: `.tmp` discipline; idempotent `(subset_uuid, content_hash)`; stats persisted.
- Identity: composite UNIQUE index enforced; concurrent `get_or_create_subset`.
- Merge: idempotency; dedupe correctness; invariants enforcement; `--dry-run`/`--verify-hash`; encryption policy guard.
- TUI/Navigator: pagination; peek bounds; event subscriptions; posture panels.
- Crash-safety: power-fail between write/replace/insert (simulate).
- Hardening: simulate rogue writer; verify perms prevent access; verify tamper-chain mismatch is detected.

---

## 21. Day-1 Rollout Plan

1. Interfaces & DI: add core interfaces, adapters; keep existing code behind them.
2. Identity switch: implement specials + quantization; add REAL `_s`/`_q` columns & identity UNIQUE; rewrite subset creation/lookup; remove tolerance from identity.
3. Sealing updates: require `subset_keys`; compute `content_hash`; store `part_stats_json`.
4. Jagged v0: canonical lock, padding rules, warnings, hashing includes meta.
5. Planner v0: collect stats at seal; implement rewrite+prune+order; expose via `meta()`.
6. Change feed + merge: add `batches`, `batch_parts`, `merge_log`; implement `merge_local`.
7. Navigator/TUI: read-only explorer + watch views.
8. Ops tools: rebuild utility, incomplete ingest detector, logging, `Catalog.optimize()`.
9. Encryption scaffolding only: add schema JSON encryption block, parts enc columns, Config fields, CryptoProvider/KeyManager interfaces, EncryptedPartStore seam, Merge policy guard (no encryption behavior yet).
10. Hardening scaffolding: advisory lockfiles; POSIX perms check; optional `tamper_cfg`; basic auditor CLI; TUI posture surface.
11. Tests: run unit + integration gates for each milestone.

---

## 22. Open Questions / Later Work

- Migrations (retolerance/rekey/schema forks) via `MigrationService` and rebuild tooling.
- Encoding hints: codebooks for lengths/shapes; denser kernels.
- Zero-copy imports: promote `register_existing_part` as public API if desired.
- Server orchestration: tiers, quotas, checkpointing, permissions, dashboards.
- Encryption behaviors: turn on AEAD for HDF5 (`h5`/`both`), optional SQLCipher for SQLite (`sqlite`/`both`); re-encrypt during merge (`--reencrypt`); key rotation job.
- Hardening: SELinux/AppArmor profiles per distro; Windows/macOS ACL recipes; `datamgrd` service packaging.

---

## 23. Appendix: SQLite compile options & PRAGMAs

- Keep custom SQLite loader; drop `ENABLE_NAN_INF` from compile-option assert.
- Recommended PRAGMAs:

```sql
PRAGMA journal_mode=wal2;
PRAGMA synchronous=NORMAL;          -- FULL for staging if durable
PRAGMA busy_timeout=5000;
PRAGMA foreign_keys=ON;
PRAGMA temp_store=MEMORY;
PRAGMA cache_size=-65536;
```

- Periodically: `ANALYZE`; `PRAGMA optimize`;
- Optional future: connection hook to support SQLCipher (page-level encryption) without refactors.
- Read-only open for consumers (immutable pages): `file:db.sqlite?immutable=1` (when appropriate).
- Safety for readers: `PRAGMA trusted_schema=OFF`; `PRAGMA query_only=ON`.

---

## 24. Encryption-Ready Hooks (AEAD, KMS, Policy)

**Why now?** Zero runtime cost now; avoids schema churn later; keeps merge/planner ciphertext-agnostic.

**Interfaces**

- `CryptoProvider`, `KeyManager` (see §3). Inject via Config/DI.

**Config**

- Fields in §7 (`encryption_mode`, `kms_provider`, `kms_default_key_ref`, etc.). If `require_encryption=True` but `mode="none"`, fail Manager init.

**Schema JSON (dataset policy)**

```json
{
  "encryption": {
  "mode": "none",                     
  "algorithm": "AES-256-GCM",
  "kms_provider": null,
  "key_policy": { "default_key_ref": null, "rotation_days": 180 }}
}
```

Include in `schema_fingerprint` so merges reject incompatible policies.

**Per-part columns (already in §9)**

- `enc_version INTEGER DEFAULT 0`, `key_ref TEXT`, `nonce BLOB`, `tag BLOB`, `plaintext_size INTEGER`.

**AAD contract**

- See §11. Reconstruct and verify later without trusting filenames.

**PartStore seam**

- `EncryptedPartStore(PartStore)` forwards today; later:
  1. Seal plaintext `.h5.tmp`, compute `content_hash`.
  2. AEAD-encrypt bytes → `.h5` (inline header) or `.h5.enc`.
  3. Record (`enc_version`, `key_ref`, `nonce`, `tag`, `plaintext_size`); atomic `os.replace`.

**Merge policy guard**

- If dst expects encrypted parts and src has plaintext → require `--reencrypt` (later) or fail.
- If both encrypted: allow ciphertext copy; later provide AEAD tag verify using AAD (no decrypt).

**Navigator/TUI posture**

- Dataset: `encryption.mode`, `kms_provider`, rotation age.
- Parts: `enc_version`, redacted `key_ref`, `plaintext_size`.
- Future: verify `tag` performs lightweight AEAD tag check.

**Ops & Tests**

- FakeKMS for unit tests; log redaction utility now.
- Key rotation ledger (optional): `key_rotations(rotation_id, performed_at, old_key_ref, new_key_ref)`.

---

## 25. Hardening & Access Control (OS, Container, Tamper-Evidence)

**Goal:** Make out-of-band reads/writes impractical and detectable, and ensure normal users can only interact via this library.

### 25.1 Run-as user, ownership, permissions

- Create a dedicated service account (e.g., `datamgr`).
- On-disk layout owned by `datamgr:datamgr` (or a narrow group).
- Default modes:
  - dataset roots: `0750` (or `0700` for strict)
  - subdirs (`parts/`, `db/`): `0750`
  - files (`.sqlite`, `.h5/.h5.enc`): `0640` (or `0600` strict)
- Optional POSIX ACLs to grant read-only to a viewer group; no write perms except `datamgr`.
- Library startup (if `enforce_posix_perms=True`): verify modes/ownership and refuse to run if weaker than policy.

### 25.2 Advisory locks & process fencing

- Create `flock()` lockfiles:
  - `root.lock` (dataset root)
  - `db.sqlite.lock` (per DB)
- Writer connections acquire exclusive lock; readers acquire shared lock; assists operability and observability.
- Lock dir may live on tmpfs (`Config.lock_dir`) to avoid stale locks after crash.

### 25.3 Filesystem & mount options

- Recommend separate filesystem for dataset roots with:
  - `nodev,nosuid,noexec` on mount
  - `noatime` to reduce write amplification
- Optional: `chattr +i` for catalog snapshots; `chattr +a` for audit logs (Linux ext\*).
- Read-only bind mounts for reader processes where feasible (`Config.readonly_mounts_ok`).

### 25.4 Container/sandbox profile (recommended)

- Run writers under a service (`datamgrd`) in a container/jail with:
  - Read-only rootfs
  - Bind-mount dataset roots (read-write) and lock dir
  - Drop Linux capabilities; seccomp syscall allowlist; no network if not required
  - No ptrace; `/proc` limited
- systemd unit hardening (illustrative):
  - `NoNewPrivileges=yes`
  - `ProtectSystem=strict`
  - `ProtectHome=read-only`
  - `PrivateTmp=yes`
  - `ProtectKernelLogs=yes`, `ProtectControlGroups=yes`, `ProtectClock=yes`
  - `RestrictSUIDSGID=yes`
  - `LockPersonality=yes`, `MemoryDenyWriteExecute=yes`
  - `CapabilityBoundingSet=` (empty)
  - `ReadWritePaths=/srv/datamgr/datasets`
  - `BindReadOnlyPaths=/etc/ssl/certs`
- AppArmor/SELinux: add allow rules only for the bind-mounted paths and `/dev/urandom`.

### 25.5 Catalog tamper-evidence (hash chain)

- If `tamper_chain_enabled`:
  - Initialize `tamper_cfg.hash_salt` (random).
  - On each batch commit, compute `entry_hash = H(prev_hash || bid || all part_uuids || their content_hashes || created_at || salt)`.
  - Store `prev_hash/entry_hash` in `batches`.
  - TUI shows current head; auditor can recompute and alert on mismatch.
- Optional: sign head with an external KMS key for stronger anchoring.

### 25.6 Library-level guards

- Open-time safety PRAGMAs for readers:
  - `PRAGMA trusted_schema=OFF`; `PRAGMA query_only=ON`; use `immutable=1` URI flag when appropriate.
- Ensure DB schema `application_id/user_version` check; refuse unknown schemas.
- Optional “authorizer” (if using pysqlite C API hook) to restrict dangerous statements inside the library.
- Audit log: append-only log of mutating operations (ingest/merge/rebuild) with who/when/what; redact secrets.

### 25.7 Detection & response

- `auditd` (Linux) or OS-native file integrity monitoring on dataset roots.
- Nightly “verify” job:
  - Walk parts to sample/verify `content_hash`
  - Recompute tamper chain head
  - Report divergences via metrics/events.
- Optionally mirror tamper head to remote (S3/object store) for third-party anchoring.

### 25.8 TUI posture & auditor CLI

- TUI “security” panel:
  - perm/owner status (OK/FAIL)
  - container/sandbox detected (yes/no)
  - locks (held/free)
  - tamper chain head (hash prefix) and status
- `datamgr-audit`:
  - Checks modes/ACLs/owners
  - Validates active locks
  - Recomputes chain head
  - Optionally verifies random subset of part `content_hash`es

### 25.9 Limitations (explicit)

- Root can always read/modify; goal is to raise the bar and detect tampering.
- SQLite cannot be made truly “library-only”; we rely on OS perms + process fencing + audit + tamper-evidence.

---

## Checklist Summary (by area)

**(A) Interfaces & Skeleton**

- KeyNormalizer, ContentHasher, JaggedSpec, PartStats, PlanIR, errors
- PartStore(HDF5), EncryptedPartStore seam, DatasetCatalog(SQLite), ChangeFeed, MergeLog
- CryptoProvider/KeyManager hooks
- Config + DI; Manager composes services

**(B) Schema & Identity**

- Quantization in schema JSON; defaults + overrides
- `ensure_key_columns` adds raw REAL + `_s` + `_q`, creates `uniq_subsets_identity`
- `quantize_keys` + specials → deterministic `subset_uuid`
- Rewrite `get_or_create_subset` / `find_subsets` (equality via `(k_s,k_q)`, ranges via raw)

**(C) Jaggedness**

- Normalize providers; compute/lock canonical; pad; meta arrays
- Warnings (internal padding); overflow errors
- Persist `jagged` info

**(D) Ingest/Sealing**

- Buffers store `subset_keys`
- `publish_part` writes attrs; compute `content_hash`
- Persist `part_stats_json`; idempotent `(subset_uuid, content_hash)`
- Router partitions by deterministic UUID
- AAD contract defined
- Hardening: advisory lock + perms check

**(E) Planner v0**

- Expose stats in `meta`
- Rewrite/prune; cost/order; operator selection; strategies

**(F) Merge**

- DDL for `batches` / `batch_parts` / `merge_log`
- Seal path populates `producer_id` / `batch_id` / `content_hash` / stats
- `merge_local` with modes & flags; per-batch txn
- Encryption policy guard
- Hash-chain `entry_hash/prev_hash` (if enabled)

**(G) Navigator/TUI**

- Navigator methods + pagination
- TUI commands; EventBus integration; feature-probe panels
- Encryption posture + security posture panels

**(H) Ops**

- Rebuild tool
- Incomplete ingest detector
- `Catalog.optimize()`, detailed logs, redaction
- Hardening auditor CLI

**(I) Tests**

- Unit + integration including encryption & hardening scaffolding

---

*End of SPEC.md*

