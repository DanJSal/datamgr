# Tool Usage (from repo root)

> Copy/paste friendly. These are the supported entry points. Helper scripts are intentionally omitted.

---

## Lint

```bash
python tools/lint.py --dir datamgr
# Options:
#   --phase {ast,import,all}   # default: all
#   --exclude-dir PATTERN      # optional, pattern inside --dir
```

---

## Introspection & Call‑Graph Guardrails (read before coding)

- **Import‑safe modules only.** No I/O, threads, env reads, network, or heavy globals at import time.
- **Only top‑level definitions are indexed.** We record modules, classes, functions, and class methods defined at module top level. Nested functions are allowed for local helpers but are **not** nodes in the atlas and are not tracked in the call graph.
- **Stable FQIDs.** Don’t generate functions/classes dynamically or rename at runtime. Avoid `exec/execfile/eval`, dynamic `__getattr__`, or metaprogramming that obscures symbols.
- **Explicit imports.** No wildcard imports (`from x import *`). Prefer explicit relative imports within the package.
- **`__all__` only at the top‑level package `__init__.py`.** Nowhere else.
- **CALL graph is declared, not traced.** Each module may declare a module‑level `CALLS` registry to record edges using real callables (aliases resolve to their targets). Example:

  ```python
  # in some module, e.g., datamgr/services/ingest.py
  from datamgr.util.calls import Calls
  from datamgr.storage.catalog_sqlite import DatasetCatalog
  from datamgr.storage.part_store_h5 import PartStore

  CALLS = Calls()

  def ingest_part(...):
      ...

  # Declare edges using callables (introspection-only)
  CALLS.add(ingest_part, [DatasetCatalog.ensure_dataset, PartStore.write_part])
  ```

  The linter verifies that every target resolves to a top‑level function or method and that all FQIDs are importable.
- **Ignore list is centralized.** `.dm/ignore.json` is the *only* source for global exclusions (e.g., `datamgr/navspec.py`). Don’t hard‑code skips in tools.
- **Style nits that help the atlas:** keep docstrings brief; avoid massive module‑level constants; prefer one class/function per logical concern so pages stay readable.

---

## Collect nodes/edges

```bash
# Full collect
python tools/introspect_collect.py --dir datamgr --out artifacts

# Delta collect (reads .dm/deltas.json)
python tools/introspect_collect.py --dir datamgr --out artifacts --delta-only --deltas .dm/deltas.json
```

---

## Merge delta artifacts into full

```bash
python tools/nav_merge.py
# Writes artifacts/affected_fqids.txt (list of nodes to regenerate)
```

---

## Build the Package Atlas

```bash
# Full (no affected list)
python tools/build_package_atlas.py \
  --commit "$(git rev-parse --short HEAD)" \
  --nodes artifacts/nodes.json \
  --edges artifacts/edges.json \
  --out docs/api-nav \
  --repo datamgr

# Incremental (only affected + neighbors)
python tools/build_package_atlas.py \
  --commit "$(git rev-parse --short HEAD)" \
  --nodes artifacts/nodes.json \
  --edges artifacts/edges.json \
  --affected-fqids artifacts/affected_fqids.txt \
  --out docs/api-nav \
  --repo datamgr
```

> The atlas always publishes to `docs/api-nav/latest/…`. `--commit` is used only for GitHub source links.

---

## Repo Index

```bash
python tools/repo_index.py
# Writes docs/index.html with Blob/Raw links and a Package Atlas quick link.
```

### CI / GitHub Actions (auto build & deploy)
- On every push to **main**, the Pages workflow runs: lint → collect (delta/full) → merge deltas → **build Package Atlas** → **generate Repo Index** → deploy `docs/`.
- To **skip** the job for a commit, include the token **`[skip-index]`** in the commit message.
- To **force‑run** even if a previous commit had `[skip-index]`, use the manual **workflow dispatch** and set `force=true`.
- Delta builds use `.dm/deltas.json`; if it’s empty or artifacts are missing, a **full** collect runs.

---

## Deltas (change tracking)

```bash
# Add changes (paths and/or modules)
python tools/deltas.py add --path datamgr/api/navigator.py --module datamgr.api.navigator

# Add/update a note (appears on the Repo Index Deltas panel)
python tools/deltas.py note --text "Refactor api.navigator"

# Show manifest
python tools/deltas.py show

# Clear manifest
python tools/deltas.py clear
```

---

## Progress (status document)

```bash
# Update a file's status (optional --note, use --force to allow downgrade)
python tools/progress.py datamgr/api/navigator.py --status impl --note "sketched API"

# Render from progress.json only
python tools/progress.py --render-only

# Set Current/Next lists (replace lists entirely)
python tools/progress.py --set-current "Implement Package Atlas" "Write storage adapter"
python tools/progress.py --set-next "Planner v0" "Merge service"

# Clear lists (either explicit flags or empty setters)
python tools/progress.py --clear-current --clear-next
python tools/progress.py --set-current     # (clears Current)
python tools/progress.py --set-next        # (clears Next)

# Promote previous Next -> Current, then set a new Next list
python tools/progress.py --promote --set-next "Merge service"

```

---

## End‑to‑End examples

```bash
# Full rebuild (first run or big refactor)
python tools/lint.py --dir datamgr
python tools/introspect_collect.py --dir datamgr --out artifacts
python tools/build_package_atlas.py --commit "$(git rev-parse --short HEAD)" --nodes artifacts/nodes.json --edges artifacts/edges.json --out docs/api-nav --repo datamgr
python tools/repo_index.py
```

```bash
# Delta cycle
python tools/deltas.py add --path datamgr/api/navigator.py
python tools/introspect_collect.py --dir datamgr --out artifacts --delta-only --deltas .dm/deltas.json
python tools/nav_merge.py
python tools/build_package_atlas.py --commit "$(git rev-parse --short HEAD)" --nodes artifacts/nodes.json --edges artifacts/edges.json --affected-fqids artifacts/affected_fqids.txt --out docs/api-nav --repo datamgr
python tools/repo_index.py
python tools/deltas.py clear
```

---

### Notes

- All commands run from the **repo root**.
- Tools honor `.dm/ignore.json` (single source of ignore) and keep builds incremental via `.dm/deltas.json`.
- Do **not** commit generated outputs: `artifacts/`, `docs/api-nav/`, `docs/index.html`.
- Don’t combine `--set-current` and `--set-next` with `--promote` in the same call (the tool will error).
