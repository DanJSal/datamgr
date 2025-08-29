# 1) Goal & Motivation

- **Problem:** Large, intertwined Python library; cross-imports and context limits make it hard for a fresh ChatGPT session to navigate.
- **Goal:** Generate a **machine-navigable, link-only HTML universe** from **runtime introspection** (no AST). A cold start gets one URL and clicks down to exactly what’s needed.
- **Principles:** token-lean, deterministic, import-safe, no background state, minimal human prose.

# 2) High-Level Architecture

1. **Introspection collector (AST-free):** import the library under a guard, walk modules/classes/functions, serialize per-module JSON.
2. **Call-graph tracer:** run a tiny, deterministic smoke pass with a **global profiler** to capture `(caller → callee)` edges; normalize and store **outbound edges only**.
3. **HTML builder:**
   - Emits a **single-page “atlas.html”** (all nodes + cross-links) — **no code bodies** inside.
   - Emits small **meta** and **body** pages per object on demand.
4. **Publish:** snapshot per commit (`/docs/api-nav/<sha>/…`) + `/latest/` symlink.

# 3) Data Extraction (introspection-only)

- **Modules:** name, file, `__all__`, optional `DM_META`/`DM_IMPORTS`, globals (name/type/truncated repr).
- **Functions/Methods:** `inspect.signature`, raw+evaluated annotations, `__wrapped__` chain (via `functools.wraps`), decorators (names), source file + start/end lines, optional `dm_meta` (dict).
- **Classes:** bases, MRO slice, dataclass fields, properties (`fget/fset`), methods (as above), optional `dm_meta`.
- **Re-exports:** record canonical owner (`obj.__module__`) vs binding module (where exported).
- **Storage:** **per-module JSON** under `data/introspect/<sha>/pkg.mod.json`.\
  No call-graph embedded here.

# 4) Call Graph (observed first, static assist optional)

- **Best approach:** global `sys.setprofile` tracer (+ `threading.setprofile`) with an allowlist on your package prefix.
- **Registry:** map **code objects → canonical IDs** once (follow `__wrapped__`).
- **Edges:** collect `(src_id, dst_id)` where both are your package; count `samples`.
- **Normalization:**
  - Class construction → `Class.__init__` (or `__new__` if no `__init__`).
  - Bound methods → defining method on the owner class (`cls.__dict__`).
  - Re-exports → canonical owner; optional `via` alias (if you care).
- **Output:** `data/graph/<sha>/graph.json`
  ```json
  {
    "contract_version": "1.0",
    "commit": "<sha>",
    "built_at": "…",
    "nodes": [{"id":"pkg.mod.f","kind":"function","module":"pkg.mod"}, …],
    "edges": [{"src":"pkg.mod.f","dst":"pkg.util.g","evidence":"observed","samples":42}]
  }
  ```
- **Inbound (`called_by`) is derived at build time (reverse index).**
- **Optional static assist:** bytecode scan (`dis`) adds `evidence:"static"` edges for uncovered paths; later merge by priority `observed > static > hint`.

# 5) HTML Structure (machine-only)

## 5.1 Single-Page Atlas (entrypoint for ChatGPT)

- **URL:** `/docs/api-nav/latest/atlas.html`
- **Node anchors:** `#n/<readable/path>` where `<readable/path>` is hierarchical:
  - Module function: `pkg.alpha.mod/f`
  - Class: `pkg.alpha.mod/C`
  - Method: `pkg.alpha.mod/C/m`
- **Each node `<section>` includes only links + attributes:**
  - `data-fqid="pkg.alpha.mod.f"` (canonical id)
  - `data-kind="function|class|method"`
  - `data-module="pkg.alpha.mod"`
  - **Links:**
    - `rel="meta"` → `/function/<canonical.id>/meta.html`
    - `rel="body"` → `/function/<canonical.id>/body.html?lines=1-120`
    - `rel="up"` → `#top`
    - `rel="return"` → `atlas.html#n/<rt>` (populated via incoming link)
  - **Calls list:** `<li data-src="<path>" data-dst="<path>" data-evidence="observed|static" data-samples="…"> <a rel="node" href="atlas.html?rt=<thisPath>#n/<dstPath>">…</a> </li>`
  - **Called-by list:** derived the same way, linking to other node anchors.
- **Return link (stateless):** every link to another node carries `?rt=<currentPath>`; the target node renders a `rel="return"` to `#n/<rt>`.

## 5.2 “Show more” / hide (still link-only)

- Query params the atlas honors (deterministic, no JS):
  - `depth` (levels to expand), `fanout` (children per node), `offset` (pagination for first-level children)
  - `evidence` (filter: `observed,static,hint`)
  - `collapsed` (CSV of URL-encoded **readable paths** to render as leaves)
- **Link rels:** `more-depth`, `more-fanout`, `next-page`, `reset`, `hide-branch` (adds to `collapsed`), `show-branch` (removes).

## 5.3 Meta & Body pages (small, separate)

- **Meta:** signature, annotations (raw + evaluated), decorators/`__wrapped__`, `dm_meta`, source location, re-export note.
- **Body:** `<pre><code>` chunked by `?lines=a-b`; prev/next chunk links.
- **These use the canonical id in the URL path** for precision.

# 6) Coding Standards (Introspection-Only Contract v1.0 — distilled)

- **Modules**
  - `__all__` required; no conditional public symbols.
  - Import-safe; side effects gated behind CLI or `if os.getenv("DM_INTROSPECT") != "1": …`.
  - `from __future__ import annotations` at top.
  - Heavy/optional imports only under `if TYPE_CHECKING:`.
  - Optional `DM_META`/`DM_IMPORTS`.
- **Functions & Methods**
  - Public callables are **top-level defs** (no nested defs/lambdas); no exported `functools.partial/partialmethod`.
  - All decorators use `functools.wraps` and preserve `__wrapped__`.
  - Full type hints; docstrings are one-liners.
  - Optional `dm_meta` dict right after the def (notes/tags/raises/see\_also).
  - `singledispatch` allowed; we read `registry`.
- **Classes**
  - Methods defined in class body; prefer `@dataclass`.
  - Properties OK; keep `fget/fset` as normal functions (with `wraps` if decorated).
  - No module-level `__getattr__` API tricks; no dynamic public injection.
- **Source & Identity**
  - All public objects defined in `.py` files; `inspect.getsource/getsourcelines` must work.
  - Stable canonical ID: `{module}.{qualname}`.
  - Re-exports allowed; annotate canonical owner vs binding module.
- **Globals**
  - Small, serializable, non-secret; `UPPER_SNAKE` for public constants.

# 7) Linting (enforced in CI)

- Verify: `__all__`, future annotations, import-safety under `DM_INTROSPECT=1`, no nested exported defs/lambdas/partials, `wraps` present, annotations present, source retrievable, no secrets in exported constants, no star-imports in public modules, etc.
- Waivers allowed (with reason + expiry) via `DM_META["waivers"]`.

# 8) Build & CI Flow (per commit)

1. **Linter** — must pass errors (or explicit waivers).
2. **Collector** — per-module introspection JSON (no AST).
3. **Tracer (profile pass)** — run the **tiny smoke suite** under `DM_INTROSPECT=profile`; emit `graph.json` (outbound edges only).
   - **Smoke suite:** a handful of safe, deterministic calls that touch representative paths; no real I/O; fixed seeds.
4. **(Optional) Static assist** — bytecode scan; merge edges with `observed > static > hint`.
5. **HTML build** — create `atlas.html` (readable paths + links) and per-object `meta.html` / `body.html`.
6. **Publish** — to `docs/api-nav/<sha>/…` and update `/latest/`.

# 9) Identity & Paths

- **Canonical ID** (used in JSON and meta/body URLs): `{module}.{qualname}`.
- **Readable path** (used inside the atlas): hierarchical segments from root, e.g., `pkg.alpha.mod/C/m`.
- **No manual IDs** needed; paths are inferred; anchors remain unique via name/owner rules (linter can flag collisions; optional `fn:`/`cl:` prefixes only if truly needed).

# 10) Safety, Size & Determinism

- Atlas excludes code bodies; keep overall size to a few MB.
- All links are deterministic, no client state/JS required.
- Strict HTML escaping (never render raw `repr` as HTML).
- Cycles marked and not expanded beyond first encounter.
- Everything keyed by commit; pages show `commit`, `built_at`, `contract_version`.

# 11) Future-Ready (optional later)

- Add cursors instead of `offset` if pagination grows.
- Pre-slice adjacency tiers for very large graphs.
- Static SVG call-graph snapshots per node (links still anchor-based).
