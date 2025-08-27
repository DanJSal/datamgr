# Progress

## CLI Reference

```text
usage: python tools/progress.py [-h] [--status STATUS] [--note NOTE] [--force] [--render-only] [--set-current ITEM [ITEM ...]] [--set-next ITEM [ITEM ...]] [--promote]
                                [path]

Update progress and regenerate PROGRESS.md

positional arguments:
  path                  Path to a single file to update. (default: None)

options:
  -h, --help            show this help message and exit
  --status STATUS       New status. (default: None)
  --note NOTE           Optional note to append. (default: )
  --force               Allow status downgrade. (default: False)
  --render-only         Only regenerate PROGRESS.md from progress.json. (default: False)
  --set-current ITEM [ITEM ...]
                        Replace the Current list with these item(s). (default: None)
  --set-next ITEM [ITEM ...]
                        Replace the Next list with these item(s). (default: None)
  --promote             Before setting Next, move existing Next -> Current. (default: False)
```

## Current

- (none)

## Next

- (none)

## Status Summary

Flow: `pending → skeleton → impl → tested → docs`

**Overall:** **pending**: 29 (100.0%) | **skeleton**: 0 (0.0%) | **impl**: 0 (0.0%) | **tested**: 0 (0.0%) | **docs**: 0 (0.0%)

## By Area

### datamgr

| Module | Status | Updated |
|---|---|---|
| `datamgr/__init__.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/api/__init__.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/api/cli.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/api/manager.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/api/navigator.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/core/__init__.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/core/errors.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/core/hashing.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/core/jagged.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/core/keys.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/core/plan.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/core/schema.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/services/__init__.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/services/ingest.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/services/merge.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/services/migrate.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/services/planner.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/storage/__init__.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/storage/catalog_sqlite.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/storage/part_store_h5.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/storage/sqlite_loader.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/util/__init__.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/util/config.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/util/events.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/util/fs.py` | `pending` | 2025-08-27T15:18:25Z |
| `datamgr/util/warnings.py` | `pending` | 2025-08-27T15:18:25Z |

### tests

| Module | Status | Updated |
|---|---|---|
| `tests/integration/test_placeholder.py` | `pending` | 2025-08-27T15:18:25Z |
| `tests/test_placeholder.py` | `pending` | 2025-08-27T15:18:25Z |
| `tests/unit/test_placeholder.py` | `pending` | 2025-08-27T15:18:25Z |

## Latest Notes

| Updated | Module | Status | Note |
|---|---|---|---|
_No notes yet. Add one with `--note`._

