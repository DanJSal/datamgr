# tools/scaffold.py
from __future__ import annotations
import argparse, os, sys, textwrap, pathlib, datetime

ROOT = pathlib.Path(__file__).resolve().parents[1]  # repo root

PKG = "datamgr"

# Directory layout from SPEC §2
DIRS = [
    f"{PKG}",
    f"{PKG}/core",
    f"{PKG}/storage",
    f"{PKG}/services",
    f"{PKG}/api",
    f"{PKG}/util",
    "tests/unit",
    "tests/integration",
    "tools",
]

FILES: dict[str, str] = {
    # package inits
    f"{PKG}/__init__.py": '''"""
datamgr — refactor skeleton per SPEC.md.
Public facade lives in datamgr.api.manager.Manager once populated.
"""''',

    f"{PKG}/core/__init__.py": '''"""
Core (pure logic): schema, keys/identity, hashing, jagged, plan IR, errors.
"""''',

    f"{PKG}/storage/__init__.py": '''"""
Storage adapters (I/O): HDF5 part store, SQLite catalog, custom sqlite loader.
"""''',

    f"{PKG}/services/__init__.py": '''"""
Services (business logic): ingest, planner, merge, migrations (later).
"""''',

    f"{PKG}/api/__init__.py": '''"""
Public API & Navigator/TUI entrypoints.
"""''',

    f"{PKG}/util/__init__.py": '''"""
Utilities: filesystem helpers, events/metrics bus, config, warnings.
"""''',

    # core
    f"{PKG}/core/schema.py": '''"""Schema & canonical dtype / jagged specs (SPEC §3, §8, §10)."""''',
    f"{PKG}/core/keys.py": '''"""Key normalization, quantization, specials, deterministic UUIDs (SPEC §3, §8, §16)."""''',
    f"{PKG}/core/hashing.py": '''"""Content hashing (blake2b16), AAD scaffolding, stats hooks (SPEC §3, §11)."""''',
    f"{PKG}/core/jagged.py": '''"""Jagged handling: padding rules, meta arrays, validators (SPEC §10)."""''',
    f"{PKG}/core/plan.py": '''"""Planner IR & predicates rewrite/pruning interfaces (SPEC §3, §12)."""''',
    f"{PKG}/core/errors.py": '''"""Typed errors: SchemaMismatch, IdentityConflict, etc. (SPEC §3)."""''',

    # storage
    f"{PKG}/storage/part_store_h5.py": '''"""HDF5 PartStore adapter (atomic seal; encryption seam) (SPEC §4, §11, §24)."""''',
    f"{PKG}/storage/catalog_sqlite.py": '''"""DatasetCatalog adapter (DDL, CRUD, meta/data) (SPEC §4, §9)."""''',
    f"{PKG}/storage/sqlite_loader.py": '''"""Custom pysqlite loader & compile-option checks (SPEC §23)."""''',

    # services
    f"{PKG}/services/ingest.py": '''"""IngestService: buffers, sealing, stats, idempotence (SPEC §5, §11)."""''',
    f"{PKG}/services/planner.py": '''"""PlannerService v0: rewrite, prune, cost, order (SPEC §5, §12)."""''',
    f"{PKG}/services/merge.py": '''"""MergeService: change feed, idempotent merges (SPEC §5, §13)."""''',
    f"{PKG}/services/migrate.py": '''"""MigrationService (later): retolerance/rekey/schema forks (SPEC §5, §22)."""''',

    # api
    f"{PKG}/api/manager.py": '''"""Public facade API (init_dataset/add/flush/meta/data/merge) (SPEC §15)."""''',
    f"{PKG}/api/navigator.py": '''"""Navigator read-only API surface for listings/peek (SPEC §14)."""''',
    f"{PKG}/api/cli.py": '''"""CLI/TUI entrypoint (read-only navigator + ops views) (SPEC §14, §19)."""''',

    # util
    f"{PKG}/util/fs.py": '''"""FS helpers: atomic replace, fsync, perms checks, lockfiles (SPEC §11, §25)."""''',
    f"{PKG}/util/events.py": '''"""Event/Metrics bus scaffolding (SPEC §6)."""''',
    f"{PKG}/util/config.py": '''"""Config dataclass & DI helpers (SPEC §7, §24, §25)."""''',
    f"{PKG}/util/warnings.py": '''"""Aggregated user warnings (padding, overflow) (SPEC §10, §17)."""''',

    # project files
    "README.md": "# datamgr (refactor skeleton)\n\nSee SPEC.md for architecture.\n",
    "SPEC.md": "# SPEC.md placeholder\n\nPaste the SPEC content from your source here.\n",
    "pyproject.toml": textwrap.dedent('''\
        [build-system]
        requires = ["setuptools>=67", "wheel"]
        build-backend = "setuptools.build_meta"

        [project]
        name = "datamgr"
        version = "0.0.0"
        description = "Data manager (skeleton per SPEC.md)"
        readme = "README.md"
        requires-python = ">=3.9"
        license = {text = "Proprietary"}
        authors = [{name = "Your Name"}]
        classifiers = [
            "Programming Language :: Python :: 3",
            "Development Status :: 2 - Pre-Alpha",
        ]

        [tool.setuptools.packages.find]
        include = ["datamgr*"]
    '''),
    ".gitignore": textwrap.dedent('''\
        __pycache__/
        *.pyc
        *.pyo
        .DS_Store
        .idea/
        .vscode/
        .pytest_cache/
        .mypy_cache/
        .coverage
        dist/
        build/
        *.egg-info/
    '''),

    # tests placeholders
    "tests/unit/test_placeholder.py": '''def test_placeholder(): assert True''',
    "tests/integration/test_placeholder.py": '''def test_placeholder(): assert True''',
}

HEADER = """# This file was created by tools/scaffold.py on {ts}.
# Minimal placeholder only. We will populate real skeletons next.
"""

def write(path: pathlib.Path, content: str, force: bool):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force:
        return
    if path.suffix == ".py" and content and not content.lstrip().startswith('"""'):
        content = f'"""{path.name} — placeholder module created by scaffold."""\\n\\n' + content
    stamp = HEADER.format(ts=datetime.datetime.utcnow().isoformat() + "Z") if path.suffix in {".py", ".md", ".toml"} else ""
    path.write_text(stamp + content, encoding="utf-8")

def main():
    ap = argparse.ArgumentParser(description="Scaffold datamgr repo per SPEC.md")
    ap.add_argument("--force", action="store_true", help="overwrite existing files")
    args = ap.parse_args()

    for d in DIRS:
        (ROOT / d).mkdir(parents=True, exist_ok=True)

    for rel, content in FILES.items():
        write(ROOT / rel, content, args.force)

    print("Scaffold complete.")
    print(f"Root: {ROOT}")
    print("Next:")
    print("  1) Paste your full spec into SPEC.md (overwriting the placeholder).")
    print("  2) Commit the scaffold.")
    print("  3) We’ll populate module/class/function skeletons file-by-file.")

if __name__ == "__main__":
    sys.exit(main())
