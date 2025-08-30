# Goal
Create explicit, bidirectional links between:
- **SPEC.md** (authoritative design)
- **LEGACY_NOTES.md** (what to keep/replace)
- **legacy/** code (the actual reference implementations)
- **Package Atlas** (generated site)

Deliverables:
1) A crosswalk that says *which legacy artifacts are still the source of truth* for v2, which are reference-only, and which are obsolete.
2) Link conventions + build steps so the Atlas renders clickable hops SPEC ↔ LEGACY ↔ APIs.
3) Concrete text patches for SPEC/LEGACY_NOTES to install those links.

---

## Link conventions (proposed)
We’ll use lightweight, repo-relative tags that your Atlas builder rewrites into permalinks:

- **Code anchors (in source):** add a one-line marker **above** each symbol you want to link:
  ```py
  # @anchor LEGACY:atoms.publish_part
  def publish_part(...):
      ...
  ```
  The Atlas builder emits `<a id="LEGACY:atoms.publish_part">` anchors and captures file/line.

- **Doc links (in md):** reference anchors using square-bracket tags that the builder rewrites:
  - `[LEGACY:atoms.publish_part]` → links to the symbol anchor in the rendered code page
  - `[SPEC:identity.subset_uuid]` → links to a `SPEC.md` section id (see below)
  - `[NOTES:fs.atomic]` → links to anchors inside **LEGACY_NOTES.md**

- **SPEC anchors:** add explicit HTML anchors to every normative section header, e.g.:
  ```md
  <a id="SPEC:identity.subset_uuid"></a>
  ### Subset UUID derivation
  ```

- **Permalink stability:** the Atlas should resolve each tag to a permalink that includes the commit hash used for that build (e.g., `/ref/<sha>/legacy/datamgr/atoms.py#LEGACY:atoms.publish_part`). The latest build can also maintain a moving alias under `/latest/…`.

---

## Crosswalk: SPEC topics ↔ legacy code (keep / adapt / replace)
Legend:
- **KEEP**: carry forward essentially as-is (minor naming polish OK)
- **ADAPT**: keep the core idea/impl but update per SPEC changes
- **REPLACE**: do not rely on legacy behavior for v2

### Identity & Subset Keys
- SPEC: `[SPEC:identity.subset_uuid]` (specials + quantization → deterministic subset_uuid)
- Legacy reference:
  - `ingest_core.stable_subset_key` (**REPLACE**) – stringified, tolerance-rounded key. Will be superseded.
  - `ingest_core.Router.resolve_subset_uuid` (**REPLACE**) – uses tolerance + Manifest to allocate UUIDs; v2 must compute locally from specials/quantization without DB search.
  - `manifest.get_or_create_subset` (**ADAPT**) – DB path stays (insert if absent), but the *key lookup* will use the deterministic `subset_uuid` from SPEC. Real-key tolerance logic is removed.

### Content Hashing & Deduplication
- SPEC: `[SPEC:parts.content_hash]`, hashing includes padded bytes for jagged fields + meta arrays
- Legacy reference:
  - `atoms.compute_semantic_content_hash` (**ADAPT**) – NFC + length-prefixed, includes a schema signature. Extend to incorporate: (a) **padded data** and (b) **jagged meta** (e.g., `*_len`, `*_shape`) exactly as SPEC defines.
  - DB uniqueness: `manifest.ensure_dataset_db_initialized` unique index on `(subset_uuid, content_hash)` (**KEEP**)
  - `atoms.publish_part` pre-check + insert-once reconcile (**KEEP**) – race-safe dedupe.

### Storage Layout & Atomic Sealing
- SPEC: `[SPEC:storage.scheme]`, `[SPEC:sealing.atomic_write]`
- Legacy reference:
  - `atoms.StorageScheme`, `atoms.part_relpath` (**KEEP**) – optional fanout via hash slices.
  - **Atomic write** path in `atoms.publish_part` (**KEEP**) – `*.tmp → fsync(file) → rename → fsync(dir)` with HDF5 VFD fsync fallback.
  - String encoding/decoding: `atoms.h5_storage_dtype`, `to_h5_storage_array`, `from_h5_storage_array` (**KEEP**) – serves as baseline for v2’s binary format; add AAD/encryption seam later.

### Ingest: Buffers, Writers, Staging
- SPEC: `[SPEC:ingest.buffering]`, `[SPEC:ingest.crash_safe]`, `[SPEC:ingest.staging]`
- Legacy reference:
  - `manager.Manager.add` buffering + `_flush_subset_buffer` chunking by `part_rows` (**KEEP**)
  - Multi-writer flow in `affinity_ingest` (**ADAPT**) – **routing must use deterministic `subset_uuid`** (post-SPEC), not tolerance-based `Router`. The overall writer/queue/process orchestration is KEEP.
  - `ingest_core.Stager` (**KEEP/ADAPT**) – staging schema and prefix-claim mechanics are sound; update to store new AAD/encryption metadata as needed.
  - `affinity_ingest` compaction loops (**KEEP**) – reclaim stale, claim prefix, merge, publish.

### Schema, DTypes & Jaggedness
- SPEC: `[SPEC:schema.dtype_lock]`, `[SPEC:jagged.meta_arrays]`
- Legacy reference:
  - Canonical locking/widening: `manager.ensure_canonical_dtype` (**KEEP/ADAPT**) – retain lock-on-first-write, but extend to **record jagged meta fields**; ensure widening semantics remain for text.
  - `manager.dict_to_structured` (**ADAPT**) – enforce jagged meta rules (pad, populate `*_len`/`*_shape`), and disallow objects/complex/datetimes as legacy already does.
  - `manager.data` (**ADAPT**) – assemble with jagged awareness; continue surfacing `missing_parts`.

### Catalog & Tables
- SPEC: `[SPEC:catalog.ddl]` (datasets, subsets, parts, batches, batch_parts, merge_log)
- Legacy reference:
  - Existing tables/indices in `manifest.ensure_dataset_db_initialized` (**ADAPT**) – keep subsets/parts foundations; **add**: `batches`, `batch_parts`, optional `merge_log` and tamper-evidence columns per SPEC.
  - PRAGMAs / connection factories (`atoms.default_conn_factory`, `manifest.catalog_conn`) (**KEEP**)

### Soft Delete, GC, FSCK
- SPEC: `[SPEC:gc.soft_delete]`, `[SPEC:gc.fsck]`, `[SPEC:security.tamper]`
- Legacy reference:
  - `manager.soft_delete`, `manager.delete` + `manifest.gc_commit` (**KEEP/ADAPT**) – same ops flow; extend counts/consistency checks per SPEC and incorporate tamper-evident hash chain if enabled.
  - `manifest.fsck_dataset` (**KEEP/ADAPT**) – orphan detection remains; compute content hash from HDF5 when missing → align with updated hash rules.

### Security / Encryption seam
- SPEC: `[SPEC:security.aead]`, `[SPEC:security.aad_contract]`
- Legacy reference:
  - `atoms.publish_part` (**ADAPT**) – introduce hook points to build/store AAD; integrate CryptoProvider later without changing the atomicity path.
  - Hooks class (`atoms.Hooks`) (**KEEP/ADAPT**) – document new callbacks covering AEAD context and sealing states.

### Introspection / Meta surface / Planner
- SPEC: `[SPEC:planner.v0]`, `[SPEC:navigator.readonly]`
- Legacy reference:
  - `manager.meta` (**ADAPT/RENAME**) – keep the idea (single call returning typed arrays), but conform fields to SPEC (PartStats, jaggedness, costs); planner APIs will be new.

---

## Concrete text patches (ready-to-paste snippets)

### 1) SPEC.md additions (anchors + back-links)
Add at the top of SPEC.md:
```md
<!-- Link anchors used by the Atlas -->
<a id="SPEC:identity.subset_uuid"></a>
<a id="SPEC:parts.content_hash"></a>
<a id="SPEC:storage.scheme"></a>
<a id="SPEC:sealing.atomic_write"></a>
<a id="SPEC:ingest.buffering"></a>
<a id="SPEC:ingest.crash_safe"></a>
<a id="SPEC:ingest.staging"></a>
<a id="SPEC:schema.dtype_lock"></a>
<a id="SPEC:jagged.meta_arrays"></a>
<a id="SPEC:catalog.ddl"></a>
<a id="SPEC:gc.soft_delete"></a>
<a id="SPEC:gc.fsck"></a>
<a id="SPEC:security.aead"></a>
<a id="SPEC:security.aad_contract"></a>
<a id="SPEC:planner.v0"></a>
<a id="SPEC:navigator.readonly"></a>
```
At the end of each relevant section, add a short “Legacy reference” list, e.g. for **Atomic sealing**:
```md
**Legacy reference:** [LEGACY:atoms.publish_part], [LEGACY:atoms.fsync_dir], [LEGACY:atoms.makedirs_with_fsync]
```

### 2) LEGACY_NOTES.md upgrades (explicit symbol links)
Add anchors at the top:
```md
<a id="NOTES:fs.atomic"></a>
<a id="NOTES:hashing"></a>
<a id="NOTES:staging"></a>
<a id="NOTES:ddl"></a>
```
Then update bullets to use symbol tags:
```md
- **Keep** atomic HDF5 sealing: [LEGACY:atoms.publish_part], [LEGACY:atoms.fsync_dir].
- **Keep/extend** content hashing: [LEGACY:atoms.compute_semantic_content_hash]; extend per [SPEC:parts.content_hash] (padding + jagged meta).
- **Replace** tolerance-based identity: [LEGACY:ingest_core.Router.resolve_subset_uuid] → see [SPEC:identity.subset_uuid].
- **Keep** WAL2 pragmas/conn factories: [LEGACY:atoms.default_conn_factory], [LEGACY:manifest.catalog_conn].
- **Keep** staging & compaction: [LEGACY:ingest_core.Stager], [LEGACY:affinity_ingest.writer_loop].
```

### 3) Code anchor placements (add once, low-churn)
Add `# @anchor …` above these symbols:
- `atoms.py`: `fsync_dir`, `makedirs_with_fsync`, `cleanup_stale_tmps_in_dir`, `StorageScheme`, `part_relpath`, `compute_semantic_content_hash`, `publish_part`, `SubsetLease`, `DatasetLease`
- `ingest_core.py`: `stable_subset_key`, `Router`, `Stager`
- `manager.py`: `dict_to_structured`, `ensure_canonical_dtype`, `Manager.add`, `Manager.flush`, `Manager.meta`, `Manager.data`, `Manager.soft_delete`, `Manager.delete`
- `manifest.py`: `ensure_dataset`, `ensure_dataset_db_initialized`, `ensure_key_columns`, `get_or_create_subset`, `find_subsets`, `gc_commit`, `fsck_dataset`, `lock_part_config`
- `sqlite_loader.py`: `sqlite3` proxy, `assert_compile_options`

---

## Atlas build changes (tools step)
1) **Symbol scan:** add `tools/linkmap.py` to parse Python files for `# @anchor TAG` and emit `docs/linkmap.json`:
```json
{
  "LEGACY:atoms.publish_part": {
    "file": "legacy/datamgr/atoms.py",
    "line": 210,
    "sha": "<commit>"
  }
}
```
2) **Markdown rewrite:** during docs build, replace `[LEGACY:…]`, `[SPEC:…]`, `[NOTES:…]` with proper `<a href>` links using `linkmap.json` and section ids.
3) **Permalinks:** embed the build’s commit SHA in each link; also create `latest` aliases.
4) **CI:** Insert after “atlas builder” in the existing pipeline.

---

## Targeted TODOs (short list)
- [ ] Add anchors to SPEC.md sections listed above.
- [ ] Add anchors to LEGACY_NOTES.md and convert bullets to symbol-tag links.
- [ ] Insert `# @anchor` lines above the listed symbols in `legacy/datamgr/*.py`.
- [ ] Implement `tools/linkmap.py` and markdown rewriter.
- [ ] Extend hashing to handle jagged padding/meta per SPEC.
- [ ] Swap identity flow to deterministic `subset_uuid` (remove tolerance search paths). Keep DB upsert semantics.
- [ ] Extend DDL with `batches`, `batch_parts`, optional `merge_log` aligned to SPEC; wire indices.
- [ ] Add AAD stubs in sealing path and Hooks (no crypto yet).
- [ ] Update `meta()` surface to include PartStats and jagged fields.

---

## Notes on migration sequencing
1) **Non-breaking doc work first** (anchors + Atlas rewrite) so links become live right away.
2) **Hashing/jagged** (safe to add alongside legacy parts as new versions).
3) **Deterministic identity** (introduce side-by-side, add backfill tool, then flip default).
4) **DDL extensions** (add tables; feature-flag readers until populated).
5) **Planner/Meta surface** (add new entry points; keep legacy `meta()` as compatibility facade until TUI is stable).



---

## Legacy flag inventory (specific code locations to anchor)
**Legend:**
- **MUST LINK** = not yet called out in SPEC/LEGACY_NOTES; important for v2 work
- **ALREADY LINKED** = covered by the Crosswalk above
- **NICE TO LINK** = helpful operational reference

### `legacy/datamgr/affinity_ingest.py`
- **MUST LINK**
  - `DEFAULT_STALE_CLAIM_SECONDS` — staging reclaim window constant (document operational default)
  - `writer_loop()` — full crash‑safe path incl. `_attempt_compact_for_subset` and `_merge_and_publish` details
  - `ingest_with_subset_affinity()` — process topology & routing; queue back‑pressure and liveness checks
  - `ingest_serial()` — crash‑safe serial ingest; compaction-on-shutdown loop semantics
- **ALREADY LINKED**
  - `ingest()` entry point (maps to ingest UX in SPEC)
- **NICE TO LINK**
  - `assert_picklable()` — spawn safety requirement for kwargs
  - `chunk_tasks()` — batching policy
  - `compute_payload()` — Joblib glue

### `legacy/datamgr/atoms.py`
- **MUST LINK**
  - `schema_signature_for_hash()` — exact hash contract seed (v2 will extend with jagged meta)
  - `update_hasher_from_structured()` / `update_hasher_from_h5_dataset()` — structured + HDF5 hashing paths
  - `compute_semantic_content_hash()` / `..._from_h5()` — exposed hash API used by publish/dedup
  - `h5_storage_dtype()` / `to_h5_storage_array()` / `from_h5_storage_array()` — Unicode↔bytes storage codec
  - `SubsetLease` / `subset_lock_path()` — per‑subset serialization primitive
  - `DatasetLease` / `dataset_lock_path()` — dataset‑wide GC/FSCK exclusivity
  - `publish_part()` — atomic sealing, dedupe, and manifest txn; error‑handling branches
  - `safe_unlink_inside()` / `prune_empty_dirs()` — GC safety & cleanup invariants
  - `db_txn_immediate()` / `default_conn_factory()` — txn/backoff and WAL2 PRAGMAs (tie to ops guidance)
- **ALREADY LINKED**
  - `StorageScheme`, `validate_storage_scheme()`, `part_relpath()` — layout fanout
  - `fsync_dir()` / `makedirs_with_fsync()` / `cleanup_stale_tmps_in_dir()` — atomic IO support
- **NICE TO LINK**
  - `hash_utf8_lenpref_iter()` — NFC + length‑prefix rationale
  - `SUPPORTED_HASHES` — validate policy surface
  - `batched()` — generic util used in DB ops

### `legacy/datamgr/ingest_core.py`
- **MUST LINK**
  - `Stager` schema & indices (created in `_init_schema`) — authoritative staging DDL
  - `Stager.select_and_claim_prefix()` — prefix selection algorithm (oversize‑row branch, claim token use)
  - `Stager.reclaim_stale()` — reclaim policy (µs timestamps)
  - `_staging_conn_factory()` — durable vs normal synchronous mode
- **ALREADY LINKED**
  - `Router` / `stable_subset_key()` — legacy identity (to be replaced)
- **NICE TO LINK**
  - `Stager.hot_subsets()` — shutdown draining heuristic

### `legacy/datamgr/manager.py`
- **MUST LINK**
  - `dict_to_structured()` — admissible dtypes, shape logic, and error text (specify object/S/complex/datetime rejections)
  - `ensure_canonical_dtype()` — lock‑on‑first‑write; text‑field widening; catalog + dataset meta sync
  - `Manager.add()` — buffer accounting and spill threshold selection (part_rows vs chunk_rows)
  - `Manager._flush_subset_buffer()` — carry/concatenate behavior, chunking loop, and per‑slice sealing
  - `Manager.meta()` — numpy schema for subset/parts meta (fields, types) to guide v2 meta surface
  - `Manager.data()` — part assembly, canonical casting, and `missing_parts` contract
  - `Manager.soft_delete()` / `Manager.delete()` — mark/unmark semantics and GC workflow
- **ALREADY LINKED**
  - `sql_to_numpy_dtype()` / overrides — mapping rules for meta arrays
- **NICE TO LINK**
  - `dtype_to_canonical_json()` / `dtype_from_canonical_json()` / `dtype_from_json_descr()` — schema encoding
  - `widen_unicode_dtype()` / `maybe_widen_text_fields()` — widening logic details
  - `normalize_numeric_dtype()` — numeric normalization policy

### `legacy/datamgr/manifest.py`
- **MUST LINK**
  - `ensure_dataset_db_initialized()` — full DDL + indices (names, partial indexes, uniqueness)
  - `ensure_key_columns()` — first‑write column creation & filtered indices; reserved names check
  - `get_or_create_subset()` — tolerance‑based lookup/insert (legacy contract to replace) incl. NaN handling
  - `find_subsets()` — query semantics (ranges, NaN, marked filters, time windows) and chunked IN queries
  - `gc_commit()` — recompute `total_rows`, delete marked rows, subset deletion criterion
  - `fsck_dataset()` — orphan detection; content hash recomputation from HDF5 dataset
  - `lock_part_config()` — first‑lock semantics and propagation to dataset meta
- **ALREADY LINKED**
  - `ensure_dataset()` — catalog + dataset meta bootstrap (and MIRROR writes into dataset `meta`)
- **NICE TO LINK**
  - `to_epoch_us()` / `epoch_us_to_iso()` — time parsing rules (Z/offset handling)
  - `infer_sql_type()` / `convert_for_sql()` / `safe_is_nan()` — key coercion policy
  - `catalog_conn()` / `conn_factory_for_dataset()` — PRAGMA presets

### `legacy/datamgr/sqlite_loader.py`
- **MUST LINK**
  - `assert_compile_options()` — required SQLite features checklist (JSON1/FTS5/RTREE/etc.)
  - `_ensure_wheel_ready()` / `_find_local_wheel()` / `_extract_wheel()` — wheel selection & extraction contract
  - `sqlite3` proxy behavior — indirection layer all DB code depends on
- **NICE TO LINK**
  - Platform tag logic: `_py_tag()`, `_plat_tokens()`

---

## Gap notes (where SPEC/NOTES should add explicit bullets)
- State defaults: document `DEFAULT_STALE_CLAIM_SECONDS`, PRAGMAs chosen (journal_mode=wal2, busy_timeout, cache_size, synchronous levels for durable vs non‑durable).
- Exact DDL (staging + dataset): index names, filtered indices, unique constraints.
- Hashing contract subtleties: schema signature byte format; Unicode NFC + 4‑byte little‑endian length prefix; HDF5 S/U conversion.
- GC invariants: safe unlink scope checks, empty dir pruning, and when to remove subset directories.
- Time parsing: ISO8601 handling, timezone normalization, and NaN semantics for REAL queries.
- Buffering/sealing behavior: carry‑over chunks and partial spill semantics.
- Crash‑safe compaction semantics: oversize single-row claim path; reclaim window; shutdown drain order.

