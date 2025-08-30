# datamgr — Project Overview & Layout

**Goal:** implement the datamgr package according to the authoritative **`SPEC.md`** file, delivering the modules, adapters, and services described there. The repo publishes an introspection‑only **Package Atlas** to GitHub Pages so contributors (and ChatGPT) can navigate the code structure without running the system.

**datamgr** is a Python library for dataset management. The atlas is generated from source code (no runtime DB), and CI keeps it current using delta‑aware builds.

## Repository Layout

```
TOOL_USAGE.md            # all tool commands (single source of usage)
SPEC.md                  # single source of truth for implementation order & design
LEGACY_NOTES.md          # (optional) consolidated notes about legacy behavior/decisions
legacy/                  # (optional) legacy package/code kept for reference (read‑only)
datamgr/                 # the library package (modules only; import‑safe)
tools/                   # build & ops scripts (lint, collect, merge, atlas, index, deltas, progress)
docs/                    # published site artifacts (generated; not committed)
.dm/                     # manifests for deltas/ignores
.github/workflows/       # CI (Pages) pipeline
tests/                   # unit/integration tests
```

### Key package areas
- `datamgr/core/*` — identity, schema, hashing, jagged arrays, plan IR, errors (pure logic).
- `datamgr/storage/*` — adapters (SQLite catalog, HDF5 part store).
- `datamgr/services/*` — ingest, planner, merge, migrate.
- `datamgr/api/*` — thin facade & future TUI (`navigator.py` placeholder).
- `datamgr/util/*` — filesystem helpers, events, config, warnings.

> **Legacy context:** The `legacy/` directory (if present) and `LEGACY_NOTES.md` capture prior implementations and decisions. They are not part of the active build; treat them as read‑only references. A separate "Legacy Atlas" may be added later.

## Generated Artifacts (not committed)
- **Package Atlas**: `docs/api-nav/latest/…` — link‑only views for modules/classes/functions with per‑node pages.
- **Repo Index**: `docs/index.html` — Blob/Raw links for every tracked file plus a quick link to the Package Atlas.

## Manifests
- `.dm/ignore.json` — single source of truth for files/modules the tooling must ignore.
- `.dm/deltas.json` — tracks changed paths/modules (with timestamps) to enable delta builds and annotate the Repo Index.

## Tooling (what they do)
> Descriptions only. Usage commands live in `docs/TOOL_USAGE.md`.

- **`tools/lint.py`** — static checks (AST) + import/reflection checks (CALL graph & guardrails). Honors `.dm/ignore.json` and scopes to a directory.
- **`tools/introspect_collect.py`** — walks the package and emits `artifacts/nodes.json` and `artifacts/edges.json` (full or delta‑only).
- **`tools/nav_merge.py`** — merges delta artifacts into the full set and writes `artifacts/affected_fqids.txt` for incremental atlas builds.
- **`tools/build_package_atlas.py`** — builds the **Package Atlas** HTML under `docs/api-nav/latest/` from nodes/edges; can restrict to affected nodes.
- **`tools/repo_index.py`** — renders the top‑level HTML index at `docs/index.html` with Blob/Raw links and a **Package Atlas** quick link.
- **`tools/deltas.py`** — add/show/clear delta entries; maintains overall and per‑item timestamps.
- **`tools/progress.py`** — updates `progress.json` and regenerates `PROGRESS.md` (status document only; CLI help centralized in `docs/TOOL_USAGE.md`).

## CI / Pages Pipeline (on push to `main`)
1. **Lint** the package.
2. **Collect** nodes/edges — delta‑only if `.dm/deltas.json` lists changes and prior artifacts exist; otherwise full.
3. **Merge** deltas and compute affected node set.
4. **Build Package Atlas** to `docs/api-nav/latest/` (commit SHA used only for GitHub source anchors).
5. **Generate Repo Index** to `docs/index.html`.
6. **Deploy** `docs/` to GitHub Pages.

The site is served at `https://<user>.github.io/datamgr/` with a single **Package Atlas** link.

## Conventions & Guardrails
- All package modules must be **import‑safe** (no I/O/threads/network at import time).
- Generated outputs (`artifacts/`, `docs/api-nav/`, `docs/index.html`) are **not** committed.
- Use `.dm/ignore.json` as the **only source of ignore** across tools; avoid tool‑local skip lists.
- Use `.dm/deltas.json` to keep CI builds incremental; clear it after a successful publish when desired.

