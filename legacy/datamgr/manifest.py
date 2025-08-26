# datamgr/manifest.py
from __future__ import annotations
import os, json, time, uuid, re
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Optional, Callable, List
from contextlib import closing
import h5py
import numpy as np
from .sqlite_loader import sqlite3
from .atoms import StorageScheme, scheme_to_json, scheme_from_json, db_txn_immediate, default_conn_factory, batched, compute_semantic_content_hash, compute_semantic_content_hash_from_h5

_SAFE_STR_RE = re.compile(r"^[A-Za-z0-9_]+$")
RESERVED_SUBSET_COLS = {
    "subset_uuid",
    "created_at_epoch",
    "created_at_utc",
    "marked_for_deletion",
    "total_rows",
    "buffer_rows",
}

def assert_safe_dataset(name: str):
    if not name or not isinstance(name, str) or not _SAFE_STR_RE.match(name):
        raise ValueError(f"Invalid dataset name: {name!r} (only A–Z, a–z, 0–9, and _ allowed)")

def assert_safe_field_name(name: str):
    if not name or not isinstance(name, str) or not _SAFE_STR_RE.match(name):
        raise ValueError(f"Invalid field name: {name!r} (only A–Z, a–z, 0–9, and _ allowed)")

def safe_is_nan(x) -> bool:
    try:
        if isinstance(x, (float, np.floating)):
            return x != x
        xf = float(x)
        return xf != xf
    except Exception:
        return False

def infer_sql_type(v) -> str:
    if isinstance(v, (bool, np.bool_)):
        return "BOOLEAN"
    if isinstance(v, (int,  np.integer)):
        return "INTEGER"
    if isinstance(v, (float, np.floating)):
        return "REAL"
    if isinstance(v, str):
        return "TEXT"
    raise TypeError(f"Unsupported subset key type: {type(v)} (value={v})")

def convert_for_sql(v, sql_type: str):
    if v is None: raise ValueError("subset key values cannot be NULL")
    t = sql_type.upper()
    if t == "BOOLEAN":
        if isinstance(v, (bool, np.bool_, int, np.integer)):
            return 1 if bool(v) else 0
        raise TypeError(f"Cannot store {type(v)} in BOOLEAN column.")
    if t == "INTEGER":
        if isinstance(v, (bool, np.bool_, int, np.integer)):
            return int(v)
        raise TypeError(f"Cannot store {type(v)} in INTEGER column.")
    if t == "REAL":
        if isinstance(v, (int, np.integer, float, np.floating)):
            return float(v)
        raise TypeError(f"Cannot store {type(v)} in REAL column.")
    if t == "TEXT":
        if isinstance(v, str):
            return v
        raise TypeError(f"Cannot store {type(v)} in TEXT column.")
    raise ValueError(f"Unrecognized column type {sql_type}")

def to_epoch_us(x) -> int:
    if isinstance(x, (int, float, np.integer, np.floating)):
        return int(float(x) * 1_000_000)
    if isinstance(x, str):
        s = x.strip()
        if len(s) == 10 and s[4] == '-' and s[7] == '-':
            dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        else:
            if s.endswith('Z'):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
        return int(dt.timestamp() * 1_000_000)
    raise TypeError(f"Unsupported time value: {type(x)}")

def epoch_us_to_iso(us: int) -> str:
    return datetime.fromtimestamp(us / 1_000_000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

class Manifest:
    def __init__(self, db_root: str):
        self.db_root = os.path.abspath(db_root)
        os.makedirs(self.db_root, exist_ok=True)
        self.catalog_path = os.path.join(self.db_root, "catalog.db")
        self.init_catalog()

    def catalog_conn(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.catalog_path, timeout=30)
        con.execute("PRAGMA journal_mode=wal2;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA busy_timeout=5000;")
        con.execute("PRAGMA foreign_keys=ON;")
        con.execute("PRAGMA temp_store=MEMORY;")
        con.execute("PRAGMA cache_size=-65536;")
        con.row_factory = sqlite3.Row
        return con

    def dataset_root(self, ds_uuid: str) -> str:
        return os.path.join(self.db_root, "datasets", ds_uuid)

    def dataset_db_path(self, ds_uuid: str) -> str:
        return os.path.join(self.dataset_root(ds_uuid), "dataset.db")

    def conn_factory_for_dataset(self, ds_uuid: str) -> Callable[[], sqlite3.Connection]:
        return default_conn_factory(self.dataset_db_path(ds_uuid))

    def init_catalog(self) -> None:
        cf = default_conn_factory(self.catalog_path)
        with db_txn_immediate(cf, retries=8, backoff=0.03) as c:
            c.execute("CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            c.execute("""
                CREATE TABLE IF NOT EXISTS datasets(
                    dataset_uuid         TEXT PRIMARY KEY,
                    alias                TEXT UNIQUE NOT NULL,
                    created_at_epoch     INTEGER NOT NULL,       -- µs
                    schema_json          TEXT NOT NULL,          -- {'key_schema': {...}, 'dtype_descr': '...'}
                    storage_scheme_json  TEXT NOT NULL
                )
            """)
            if not c.execute("SELECT 1 FROM meta WHERE key='database_uuid'").fetchone():
                c.execute("INSERT INTO meta(key,value) VALUES('database_uuid',?)", (str(uuid.uuid4()),))
                c.execute("INSERT INTO meta(key,value) VALUES('created_at_epoch',?)", (int(time.time_ns() // 1_000),))

    def get_dataset_row(self, alias: str) -> Optional[sqlite3.Row]:
        con = self.catalog_conn()
        try:
            return con.execute("SELECT * FROM datasets WHERE alias=?", (alias,)).fetchone()
        finally:
            con.close()

    def get_dataset_row_by_uuid(self, ds_uuid: str) -> Optional[sqlite3.Row]:
        con = self.catalog_conn()
        try:
            return con.execute("SELECT * FROM datasets WHERE dataset_uuid=?", (ds_uuid,)).fetchone()
        finally:
            con.close()

    def ensure_dataset(self, alias: str, default_scheme: Optional[StorageScheme] = None) -> Tuple[str, StorageScheme]:
        assert_safe_dataset(alias)
        row = self.get_dataset_row(alias)
        if row:
            ds_uuid = row["dataset_uuid"]
            scheme = scheme_from_json(row["storage_scheme_json"])
            self.ensure_dataset_db_initialized(ds_uuid)
            return ds_uuid, scheme
        ds_uuid = str(uuid.uuid4())
        scheme = default_scheme or StorageScheme()
        created_us = int(time.time_ns() // 1_000)
        schema = {"key_schema": {}, "key_order": [], "dtype_descr": "", "part_config": {}}
        sj = json.dumps(schema)
        sj_scheme = scheme_to_json(scheme)
        cf = default_conn_factory(self.catalog_path)
        try:
            with db_txn_immediate(cf, retries=8, backoff=0.03) as c:
                c.execute("""
                        INSERT INTO datasets(dataset_uuid, alias, created_at_epoch, schema_json, storage_scheme_json)
                        VALUES(?,?,?,?,?)
                    """, (ds_uuid, alias, created_us, sj, sj_scheme))
        except sqlite3.IntegrityError:
            row = self.get_dataset_row(alias)
            if not row:
                raise
            ds_uuid = row["dataset_uuid"]
            scheme = scheme_from_json(row["storage_scheme_json"])
            self.ensure_dataset_db_initialized(ds_uuid)
            return ds_uuid, scheme
        self.ensure_dataset_db_initialized(ds_uuid)
        ds_cf = self.conn_factory_for_dataset(ds_uuid)
        with db_txn_immediate(ds_cf) as d:
            d.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('schema_json',?)", (sj,))
            d.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('storage_scheme_json',?)", (sj_scheme,))
            d.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('dataset_uuid',?)", (ds_uuid,))
            d.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('created_at_epoch',?)", (str(created_us),))
        return ds_uuid, scheme

    def load_schema(self, ds_uuid: str) -> Dict[str, Any]:
        row = self.get_dataset_row_by_uuid(ds_uuid)
        if not row:
            raise ValueError(f"Unknown dataset {ds_uuid}")
        return json.loads(row["schema_json"])

    def save_schema(self, ds_uuid: str, schema: Dict[str, Any]) -> None:
        sj = json.dumps(schema)
        cat_cf = default_conn_factory(self.catalog_path)
        with db_txn_immediate(cat_cf, retries=8, backoff=0.03) as c:
            c.execute("UPDATE datasets SET schema_json=? WHERE dataset_uuid=?", (sj, ds_uuid))
        ds_cf = self.conn_factory_for_dataset(ds_uuid)
        try:
            with db_txn_immediate(ds_cf, retries=8, backoff=0.03) as d:
                d.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('schema_json',?)", (sj,))
        except Exception:
            pass

    def get_storage_scheme(self, ds_uuid: str) -> StorageScheme:
        row = self.get_dataset_row_by_uuid(ds_uuid)
        if not row:
            raise ValueError(f"Unknown dataset {ds_uuid}")
        return scheme_from_json(row["storage_scheme_json"])

    def ensure_key_columns(self, ds_uuid: str, subset_keys: Dict[str, Any]) -> Dict[str, str]:
        for k in subset_keys.keys():
            assert_safe_field_name(k)
        bad = RESERVED_SUBSET_COLS.intersection(subset_keys.keys())
        if bad:
            raise ValueError(f"subset_keys contain reserved column name(s): {sorted(bad)}")
        schema = self.load_schema(ds_uuid)
        key_schema: Dict[str, str] = schema.get("key_schema") or {}
        key_order: list[str] = schema.get("key_order") or []
        if not key_schema:
            order = sorted(subset_keys.keys())
            inferred = {k: infer_sql_type(subset_keys[k]) for k in order}
            cf = self.conn_factory_for_dataset(ds_uuid)
            with db_txn_immediate(cf) as conn:
                for k in order:
                    conn.execute(f"ALTER TABLE subsets ADD COLUMN {k} {inferred[k]}")
                for k in order:
                    conn.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_subsets_key_{k} "
                        f"ON subsets({k}) WHERE marked_for_deletion=0"
                    )
            schema["key_schema"] = inferred
            schema["key_order"] = order
            self.save_schema(ds_uuid, schema)
            return inferred
        if not key_order:
            raise RuntimeError("Dataset schema missing 'key_order' (legacy not supported).")
        expected_names = set(key_schema.keys())
        incoming_names = set(subset_keys.keys())
        if incoming_names != expected_names:
            raise ValueError(f"subset_keys must have keys {sorted(expected_names)}")
        for k in key_schema.keys():
            incoming_t = infer_sql_type(subset_keys[k])
            if incoming_t != key_schema[k]:
                raise TypeError(f"Key '{k}' expected type {key_schema[k]}, got {incoming_t}")
        return key_schema

    def get_or_create_subset(self, ds_uuid: str, subset_keys: Dict[str, Any], float_tolerance: float = 1e-6) -> str:
        schema = self.load_schema(ds_uuid)
        key_schema = schema["key_schema"]
        key_order = schema["key_order"]
        conds, vals = [], []
        for k in key_order:
            sqlt = key_schema[k]
            raw_v = subset_keys[k]
            if sqlt == "REAL":
                v = convert_for_sql(raw_v, sqlt)
                if safe_is_nan(v):
                    conds.append(f"{k} != {k}")
                else:
                    conds.append(f"{k} BETWEEN ? AND ?")
                    vals.extend([v - float_tolerance, v + float_tolerance])
            else:
                v = convert_for_sql(raw_v, sqlt)
                conds.append(f"{k}=?")
                vals.append(v)
        where = " AND ".join(conds) if conds else "1=1"
        factory = self.conn_factory_for_dataset(ds_uuid)
        with closing(factory()) as con:
            con.row_factory = sqlite3.Row
            row = con.execute(
                f"SELECT subset_uuid, marked_for_deletion FROM subsets WHERE {where} LIMIT 1",
                vals,
            ).fetchone()
            if row and int(row["marked_for_deletion"]) == 0:
                return row["subset_uuid"]
        with db_txn_immediate(factory) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                f"SELECT subset_uuid, marked_for_deletion FROM subsets WHERE {where} LIMIT 1",
                vals,
            ).fetchone()
            if row:
                su = row["subset_uuid"]
                if int(row["marked_for_deletion"]) != 0:
                    conn.execute("UPDATE subsets SET marked_for_deletion=0 WHERE subset_uuid=?", (su,))
                return su
            su = str(uuid.uuid4())
            cols = ["subset_uuid", "created_at_epoch"] + key_order
            ph = ",".join(["?"] * len(cols))
            exact_vals = [convert_for_sql(subset_keys[k], key_schema[k]) for k in key_order]
            conn.execute(
                f"INSERT INTO subsets({','.join(cols)}) VALUES({ph})",
                (su, int(time.time_ns() // 1_000), *exact_vals),
            )
            return su

    def find_subsets(
            self,
            ds_uuid: str,
            queries: Dict[str, Any],
            *,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            exclude_marked: bool = True,
            float_tolerance: float = 1e-6,
            return_parts: bool = False,
            parts_start_time: Optional[str] = None,
            parts_end_time: Optional[str] = None,
            exclude_marked_parts: bool = True,
    ):
        schema = self.load_schema(ds_uuid)
        key_schema = schema.get("key_schema") or {}
        reserved_types = {
            "subset_uuid": "TEXT",
            "created_at_epoch": "INTEGER",
            "marked_for_deletion": "BOOLEAN",
            "total_rows": "INTEGER",
        }
        def is_range(v_) -> bool:
            try:
                if isinstance(v_, (str, bytes)):
                    return False
                it = list(v_)
                return len(it) == 2
            except TypeError:
                return False
        conds: list[str] = []
        vals: list[Any] = []
        for k, raw in (queries or {}).items():
            if k in key_schema:
                sqlt = key_schema[k]
            elif k in reserved_types:
                sqlt = reserved_types[k]
            else:
                raise ValueError(f"Key '{k}' not in schema for dataset {ds_uuid}.")
            if sqlt == "INTEGER":
                if is_range(raw):
                    lo, hi = list(raw)
                    lo, hi = int(lo), int(hi)
                    if lo > hi: lo, hi = hi, lo
                    conds.append(f"{k} BETWEEN ? AND ?")
                    vals.extend([lo, hi])
                else:
                    conds.append(f"{k} = ?")
                    vals.append(int(raw))
            elif sqlt == "REAL":
                if is_range(raw):
                    lo, hi = list(raw)
                    if safe_is_nan(lo) or safe_is_nan(hi):
                        raise ValueError(f"NaN cannot be used as a range bound for REAL key '{k}'.")
                    lo, hi = float(lo), float(hi)
                    if lo > hi:
                        lo, hi = hi, lo
                    conds.append(f"{k} BETWEEN ? AND ?")
                    vals.extend([lo, hi])
                else:
                    v = float(raw)
                    if safe_is_nan(v):
                        conds.append(f"{k} != {k}")
                    else:
                        conds.append(f"{k} BETWEEN ? AND ?")
                        vals.extend([v - float_tolerance, v + float_tolerance])
            elif sqlt == "BOOLEAN":
                conds.append(f"{k} = ?")
                vals.append(1 if bool(raw) else 0)
            elif sqlt == "TEXT":
                if not isinstance(raw, str):
                    raise TypeError(f"Key '{k}' must be TEXT")
                conds.append(f"{k} = ?")
                vals.append(raw)
            else:
                raise ValueError(f"Unsupported SQL type for key '{k}': {sqlt}")
        if start_time is not None:
            conds.append("created_at_epoch >= ?")
            vals.append(to_epoch_us(start_time))
        if end_time is not None:
            conds.append("created_at_epoch <= ?")
            vals.append(to_epoch_us(end_time))
        if exclude_marked:
            conds.append("marked_for_deletion = 0")
        where = " AND ".join(conds) if conds else "1=1"
        cf = self.conn_factory_for_dataset(ds_uuid)
        con = cf()
        con.row_factory = sqlite3.Row
        try:
            subset_sql = f"SELECT * FROM subsets WHERE {where} ORDER BY subset_uuid ASC, created_at_epoch ASC"
            subset_rows = con.execute(subset_sql, vals).fetchall()
            if not return_parts:
                return subset_rows
            if not subset_rows:
                return [], {}
            subset_ids = [row["subset_uuid"] for row in subset_rows]
            parts_rows_all: list[sqlite3.Row] = []
            for chunk in batched(subset_ids, 500):
                pconds = [f"subset_uuid IN ({','.join(['?'] * len(chunk))})"]
                pvals: list[Any] = list(chunk)
                if parts_start_time is not None:
                    pconds.append("created_at_epoch >= ?")
                    pvals.append(to_epoch_us(parts_start_time))
                if parts_end_time is not None:
                    pconds.append("created_at_epoch <= ?")
                    pvals.append(to_epoch_us(parts_end_time))
                if exclude_marked_parts:
                    pconds.append("marked_for_deletion = 0")
                pwhere = " AND ".join(pconds)
                parts_sql = (f"SELECT * FROM parts WHERE {pwhere} "
                             f"ORDER BY subset_uuid, created_at_epoch, part_uuid")
                parts_rows_all.extend(con.execute(parts_sql, pvals).fetchall())
        finally:
            con.close()
        parts_by_subset: Dict[str, List[sqlite3.Row]] = {s: [] for s in subset_ids}
        for r in parts_rows_all:
            parts_by_subset.setdefault(r["subset_uuid"], []).append(r)
        return subset_rows, parts_by_subset

    def resolve_dataset_uuid(self, alias: str) -> str:
        row = self.get_dataset_row(alias)
        if not row:
            raise ValueError(f"Unknown dataset alias: {alias!r}")
        return row["dataset_uuid"]

    def mark_subsets(self, ds_uuid: str, subset_ids, marked: bool) -> int:
        cf = self.conn_factory_for_dataset(ds_uuid)
        status = 1 if marked else 0
        changed = 0
        with db_txn_immediate(cf) as conn:
            for chunk in batched(subset_ids, 900):
                q = f"UPDATE subsets SET marked_for_deletion=? WHERE subset_uuid IN ({','.join('?' * len(chunk))})"
                cur = conn.execute(q, (status, *chunk))
                changed += cur.rowcount or 0
        return changed

    def mark_parts(self, ds_uuid: str, part_ids, marked: bool) -> int:
        cf = self.conn_factory_for_dataset(ds_uuid)
        status = 1 if marked else 0
        changed = 0
        with db_txn_immediate(cf) as conn:
            for chunk in batched(part_ids, 900):
                q = f"UPDATE parts SET marked_for_deletion=? WHERE part_uuid IN ({','.join('?' * len(chunk))})"
                cur = conn.execute(q, (status, *chunk))
                changed += cur.rowcount or 0
        return changed

    def list_marked_parts(self, ds_uuid: str):
        cf = self.conn_factory_for_dataset(ds_uuid)
        con = cf()
        con.row_factory = sqlite3.Row
        try:
            return con.execute(
                "SELECT part_uuid, subset_uuid, file_relpath, n_rows FROM parts WHERE marked_for_deletion=1"
            ).fetchall()
        finally:
            con.close()

    def gc_commit(self, ds_uuid: str, part_ids, touched_subset_ids):
        cf = self.conn_factory_for_dataset(ds_uuid)
        parts_deleted = subsets_deleted = 0
        with db_txn_immediate(cf) as conn:
            if part_ids:
                for chunk in batched(part_ids, 900):
                    q = f"DELETE FROM parts WHERE part_uuid IN ({','.join('?' * len(chunk))})"
                    cur = conn.execute(q, chunk)
                    parts_deleted += cur.rowcount or 0
            to_check = set(touched_subset_ids)
            rows = conn.execute("SELECT subset_uuid FROM subsets WHERE marked_for_deletion=1").fetchall()
            to_check.update([r["subset_uuid"] for r in rows])
            for su in to_check:
                total = conn.execute(
                    "SELECT COALESCE(SUM(n_rows),0) FROM parts WHERE subset_uuid=? AND marked_for_deletion=0",
                    (su,)
                ).fetchone()[0]
                conn.execute("UPDATE subsets SET total_rows=? WHERE subset_uuid=?", (int(total), su))
            doomed = conn.execute(
                "SELECT subset_uuid FROM subsets WHERE marked_for_deletion=1 AND total_rows=0"
            ).fetchall()
            doomed_ids = [r["subset_uuid"] for r in doomed]
            for chunk in batched(doomed_ids, 900):
                q = f"DELETE FROM subsets WHERE subset_uuid IN ({','.join('?' * len(chunk))})"
                cur = conn.execute(q, chunk)
                subsets_deleted += cur.rowcount or 0
        return parts_deleted, subsets_deleted, doomed_ids

    def fsck_dataset(self, ds_uuid: str, *, insert_orphans: bool = True) -> dict:
        ds_root = self.dataset_root(ds_uuid)
        cf = self.conn_factory_for_dataset(ds_uuid)
        con = cf()
        con.row_factory = sqlite3.Row
        try:
            known = set(r["file_relpath"] for r in con.execute("SELECT file_relpath FROM parts"))
            existing_subsets = set(r["subset_uuid"] for r in con.execute("SELECT subset_uuid FROM subsets"))
        finally:
            con.close()
        rels = []
        parts_dir = os.path.join(ds_root, "subsets")
        for root, _, files in os.walk(parts_dir):
            for fn in files:
                if not fn.endswith(".h5"): continue
                abs_p = os.path.join(root, fn)
                rel_p = os.path.relpath(abs_p, ds_root)
                rels.append(rel_p)
        orphans = [r for r in rels if r not in known]
        inserts = 0
        skipped = 0
        failures = 0
        if insert_orphans and orphans:
            cf = self.conn_factory_for_dataset(ds_uuid)
            with db_txn_immediate(cf) as conn:
                added_by_subset = {}
                for rel in orphans:
                    abs_p = os.path.join(ds_root, rel)
                    try:
                        with h5py.File(abs_p, "r") as f:
                            su = f.attrs.get("subset_uuid")
                            pu = f.attrs.get("part_uuid")
                            if isinstance(su, (bytes, bytearray)): su = su.decode("utf-8")
                            if isinstance(pu, (bytes, bytearray)): pu = pu.decode("utf-8")
                            if not su or not pu:
                                failures += 1
                                continue
                            created_epoch = int(f.attrs.get("created_at_epoch", 0))
                            n_rows = int(f.attrs.get("n_rows", 0))
                            scheme_version = int(f.attrs.get("scheme_version", 1))
                            dset = f.get("data")
                            if dset is None:
                                raise ValueError("orphan part missing dataset 'data'")
                            content_hash = f.attrs.get("content_hash")
                            if isinstance(content_hash, (bytes, bytearray)):
                                content_hash = content_hash.decode("utf-8")
                            if not content_hash:
                                content_hash = compute_semantic_content_hash_from_h5(dset)
                        if su not in existing_subsets:
                            skipped += 1
                            continue
                        cur = conn.execute(
                            "INSERT OR IGNORE INTO parts("
                            " part_uuid, subset_uuid, created_at_epoch, n_rows, scheme_version,"
                            " file_relpath, marked_for_deletion, content_hash"
                            ") VALUES(?,?,?,?,?,?,0,?)",
                            (pu, su, created_epoch, n_rows, scheme_version, rel, content_hash)
                        )
                        if cur.rowcount:
                            added_by_subset[su] = added_by_subset.get(su, 0) + n_rows
                            inserts += 1
                    except Exception:
                        failures += 1
                for su, delta in added_by_subset.items():
                    conn.execute(
                        "UPDATE subsets SET total_rows = total_rows + ? WHERE subset_uuid=?",
                        (int(delta), su)
                    )
        return {
            "fs_files": len(rels),
            "db_files": len(known),
            "orphans_found": len(orphans),
            "inserted": inserts,
            "skipped": skipped,
            "failures": failures,
        }

    def get_part_config(self, ds_uuid: str) -> Optional[dict]:
        schema = self.load_schema(ds_uuid)
        return schema.get("part_config")

    def lock_part_config(self, ds_uuid: str, *, part_rows: int, compression: Optional[str],
                         compression_opts: Optional[int]) -> dict:
        if part_rows < 1:
            raise ValueError("part_rows must be >= 1")
        desired = {
            "part_rows": int(part_rows),
            "compression": compression,
            "compression_opts": (int(compression_opts)
                                 if (compression is not None and compression_opts is not None)
                                 else None),
        }
        existing = None
        schema_after = None
        cf = default_conn_factory(self.catalog_path)
        with db_txn_immediate(cf) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT schema_json FROM datasets WHERE dataset_uuid=?",
                (ds_uuid,)
            ).fetchone()
            if not row:
                raise ValueError(f"Unknown dataset {ds_uuid}")
            schema = json.loads(row["schema_json"]) if row["schema_json"] else {}
            pc = schema.get("part_config") or {}
            if pc:
                existing = pc
            else:
                schema["part_config"] = desired
                schema_after = json.dumps(schema)
                conn.execute(
                    "UPDATE datasets SET schema_json=? WHERE dataset_uuid=?",
                    (schema_after, ds_uuid)
                )
        if existing:
            return existing
        ds_cf = self.conn_factory_for_dataset(ds_uuid)
        try:
            with db_txn_immediate(ds_cf) as d:
                d.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('schema_json',?)", (schema_after,))
        except Exception:
            pass
        return desired

    def ensure_dataset_db_initialized(self, ds_uuid: str) -> None:
        ds_root = self.dataset_root(ds_uuid)
        os.makedirs(os.path.join(ds_root, "subsets"), exist_ok=True)
        with sqlite3.connect(self.dataset_db_path(ds_uuid)) as d:
            d.execute("PRAGMA journal_mode=wal2;")
            d.execute("PRAGMA synchronous=NORMAL;")
            d.execute("PRAGMA busy_timeout=5000;")
            d.execute("PRAGMA foreign_keys=ON;")
            d.execute("PRAGMA temp_store=MEMORY;")
            d.execute("PRAGMA cache_size=-65536;")
            d.execute("CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            d.execute("""
                CREATE TABLE IF NOT EXISTS subsets(
                    subset_uuid         TEXT PRIMARY KEY,
                    created_at_epoch    INTEGER NOT NULL,
                    marked_for_deletion INTEGER NOT NULL DEFAULT 0,
                    total_rows          INTEGER NOT NULL DEFAULT 0
                )
            """)
            d.execute("""
                CREATE TABLE IF NOT EXISTS parts(
                    part_uuid           TEXT PRIMARY KEY,
                    subset_uuid         TEXT NOT NULL,
                    created_at_epoch    INTEGER NOT NULL,
                    n_rows              INTEGER NOT NULL,
                    scheme_version      INTEGER NOT NULL DEFAULT 1,
                    file_relpath        TEXT NOT NULL,
                    marked_for_deletion INTEGER NOT NULL DEFAULT 0,
                    content_hash        TEXT NOT NULL,
                    FOREIGN KEY(subset_uuid) REFERENCES subsets(subset_uuid) ON DELETE CASCADE
                )
            """)
            d.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_parts_subset_contenthash
                         ON parts(subset_uuid, content_hash)""")
            d.execute("""CREATE INDEX IF NOT EXISTS idx_subsets_subset_epoch
                                 ON subsets(subset_uuid, created_at_epoch)""")
            d.execute("""CREATE INDEX IF NOT EXISTS idx_subsets_epoch_subset_live
                                 ON subsets(created_at_epoch, subset_uuid)
                                 WHERE marked_for_deletion = 0""")
            d.execute("""CREATE INDEX IF NOT EXISTS idx_parts_subset_epoch_uuid
                                 ON parts(subset_uuid, created_at_epoch, part_uuid)""")
            d.execute("""CREATE INDEX IF NOT EXISTS idx_parts_subset_epoch_uuid_live
                                 ON parts(subset_uuid, created_at_epoch, part_uuid)
                                 WHERE marked_for_deletion = 0""")
