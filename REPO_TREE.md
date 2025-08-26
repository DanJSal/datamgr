# Repository Tree

_Updated: 2025-08-27T01:55:07Z_

Root: `/home/djs421/PycharmProjects/datamgr`

## CLI Reference

```text
usage: dump_tree.py [-h] [--root ROOT] [--output OUTPUT] [--max-depth MAX_DEPTH] [--include-hidden] [--ignore IGNORE]

Write REPO_TREE.md with a file tree snapshot of the repository.

options:
  -h, --help            show this help message and exit
  --root ROOT           Repository root to scan. (default: /home/djs421/PycharmProjects/datamgr)
  --output OUTPUT       Output Markdown path. (default: /home/djs421/PycharmProjects/datamgr/REPO_TREE.md)
  --max-depth MAX_DEPTH
                        Limit depth (-1 = unlimited). (default: -1)
  --include-hidden      Include dotfiles/directories. (default: False)
  --ignore IGNORE       Extra names to ignore (repeatable). (default: [])
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
│       │   ├── sqlite-wal2-amalgamation
│       │   ├── wheels-linux-manylinux-aarch64
│       │   ├── wheels-linux-manylinux-x86_64
│       │   ├── wheels-linux-musllinux-aarch64
│       │   ├── wheels-linux-musllinux-x86_64
│       │   ├── wheels-macos-arm64
│       │   ├── wheels-macos-x86_64
│       │   └── wheels-windows-amd64
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
