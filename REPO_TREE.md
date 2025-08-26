# Repository Tree

_Updated: 2025-08-27T03:50:24Z_

Root: `datamgr`

## CLI Reference

```text
usage: python tools/dump_tree.py [-h] [--root ROOT] [--out OUT] [--max-depth MAX_DEPTH]

Write REPO_TREE.md with a file tree snapshot of the repository.

options:
  -h, --help            show this help message and exit
  --root ROOT           Repository root to scan. (default: C:\Users\Daniel\PycharmProjects\datamgr)
  --out OUT             Output Markdown path. (default: C:\Users\Daniel\PycharmProjects\datamgr\REPO_TREE.md)
  --max-depth MAX_DEPTH
                        Maximum directory depth (-1 = unlimited). (default: -1)
```
## Tree

```text
datamgr
├── datamgr
│   ├── api
│   │   ├── __init__.py
│   │   ├── cli.py
│   │   ├── manager.py
│   │   └── navigator.py
│   ├── core
│   │   ├── __init__.py
│   │   ├── errors.py
│   │   ├── hashing.py
│   │   ├── jagged.py
│   │   ├── keys.py
│   │   ├── plan.py
│   │   └── schema.py
│   ├── services
│   │   ├── __init__.py
│   │   ├── ingest.py
│   │   ├── merge.py
│   │   ├── migrate.py
│   │   └── planner.py
│   ├── storage
│   │   ├── __init__.py
│   │   ├── catalog_sqlite.py
│   │   ├── part_store_h5.py
│   │   └── sqlite_loader.py
│   ├── util
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── events.py
│   │   ├── fs.py
│   │   └── warnings.py
│   └── __init__.py
├── legacy
│   └── datamgr
│       ├── pysqlite3-wal2-wheels
│       │   └── .extracted
│       │       └── pysqlite3-0.5.4-cp39-cp39-win_amd64-887819-1756091981
│       │           ├── pysqlite3
│       │           │   ├── __init__.py
│       │           │   └── dbapi2.py
│       │           └── pysqlite3-0.5.4.dist-info
│       │               ├── licenses
│       │               │   └── LICENSE
│       │               ├── METADATA
│       │               ├── RECORD
│       │               ├── top_level.txt
│       │               └── WHEEL
│       ├── __init__.py
│       ├── affinity_ingest.py
│       ├── atoms.py
│       ├── ingest_core.py
│       ├── manager.py
│       ├── manifest.py
│       └── sqlite_loader.py
├── tests
│   ├── integration
│   │   └── test_placeholder.py
│   └── unit
│       └── test_placeholder.py
├── tools
│   ├── dump_tree.py
│   └── tick_progress.py
├── LEGACY_NOTES.md
├── progress.json
├── PROGRESS.md
├── README.md
├── REPO_TREE.md
└── SPEC.md
```
