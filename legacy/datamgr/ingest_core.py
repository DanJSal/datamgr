# datamgr/ingest_core
from __future__ import annotations
import hashlib
import json
import time
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
from .sqlite_loader import sqlite3
from .manifest import Manifest
from .atoms import db_txn_immediate as immediate_txn

Payload = Tuple[Dict[str, Any], Dict[str, Any], bool]

def stable_subset_key(subset_keys: Dict[str, Any], *, decimals: int) -> str:
    def norm(v):
        if isinstance(v, (np.bool_, bool)):
            return bool(v)
        if isinstance(v, (np.integer, int)):
            return int(v)
        if isinstance(v, (np.floating, float)):
            return round(float(v), decimals)
        if isinstance(v, str):
            return v
        try:
            return v.item()
        except Exception:
            raise TypeError(f"Unsupported subset key type for hashing: {type(v)}")
    cleaned = {k: norm(subset_keys[k]) for k in sorted(subset_keys)}
    return json.dumps(cleaned, separators=(",", ":"), sort_keys=True)

class Router:
    def __init__(self, db_root: str, dataset_name: str, manager_kwargs: Optional[Dict[str, Any]]):
        self.manifest = Manifest(db_root)
        self.ds_uuid, _ = self.manifest.ensure_dataset(dataset_name)
        self.float_tol = float((manager_kwargs or {}).get("float_tolerance", 1e-6))
        self._round_decimals = round(np.ceil(-np.log10(self.float_tol)))
        self._cache: Dict[str, str] = {}

    def resolve_subset_uuid(self, subset_keys: Dict[str, Any]) -> str:
        ck = stable_subset_key(subset_keys, decimals=self._round_decimals)
        su = self._cache.get(ck)
        if su:
            return su
        self.manifest.ensure_key_columns(self.ds_uuid, subset_keys)
        su = self.manifest.get_or_create_subset(self.ds_uuid, subset_keys, float_tolerance=self.float_tol)
        self._cache[ck] = su
        return su

    def partition(self, subset_keys: Dict[str, Any], n_partitions: int) -> int:
        su = self.resolve_subset_uuid(subset_keys)
        if n_partitions <= 0:
            return 0
        h = hashlib.blake2b(su.encode("utf-8"), digest_size=8).digest()
        return int.from_bytes(h, "little") % n_partitions

def _staging_conn_factory(db_path: str, *, durable: bool):
    def _conn() -> sqlite3.Connection:
        con = sqlite3.connect(db_path, timeout=30)
        con.execute("PRAGMA journal_mode=wal2;")
        con.execute(f"PRAGMA synchronous={'FULL' if durable else 'NORMAL'};")
        con.execute("PRAGMA busy_timeout=5000;")
        con.execute("PRAGMA foreign_keys=ON;")
        con.execute("PRAGMA temp_store=MEMORY;")
        con.execute("PRAGMA cache_size=-65536;")
        return con
    return _conn

class Stager:
    def __init__(self, db_path: str, *, durable: bool):
        self.db_path = db_path
        self.connf = _staging_conn_factory(db_path, durable=durable)
        self._init_schema()

    def _init_schema(self):
        with self.connf() as d:
            d.execute("PRAGMA journal_mode=wal2;")
            d.execute("PRAGMA busy_timeout=5000;")
            d.execute("""
                CREATE TABLE IF NOT EXISTS staging_rows(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    subset_uuid TEXT NOT NULL,
                    n_rows INTEGER NOT NULL,
                    created_at_epoch INTEGER NOT NULL,
                    payload BLOB NOT NULL,
                    claimed_by TEXT,
                    claimed_at INTEGER
                )
            """)
            d.execute("CREATE INDEX IF NOT EXISTS idx_staging_claimed_by ON staging_rows(claimed_by)")
            d.execute("CREATE INDEX IF NOT EXISTS idx_staging_claimed_at ON staging_rows(claimed_at)")
            d.execute("CREATE INDEX IF NOT EXISTS idx_staging_subset_id ON staging_rows(subset_uuid, id)")

    def enqueue(self, subset_uuid: str, n_rows: int, payload_blob: bytes):
        with immediate_txn(self.connf) as con:
            con.execute(
                "INSERT INTO staging_rows(subset_uuid, n_rows, created_at_epoch, payload) VALUES(?,?,?,?)",
                (subset_uuid, int(n_rows), int(time.time_ns() // 1_000), sqlite3.Binary(payload_blob)),
            )

    def reclaim_stale(self, *, stale_after_seconds: int):
        now_us = int(time.time_ns() // 1_000)
        cutoff_us = now_us - int(stale_after_seconds * 1_000_000)
        with immediate_txn(self.connf) as con:
            con.execute(
                "UPDATE staging_rows SET claimed_by=NULL, claimed_at=NULL "
                "WHERE claimed_by IS NOT NULL AND claimed_at <= ?",
                (cutoff_us,),
            )

    def select_and_claim_prefix(self, subset_uuid: str, part_rows: int, token: str) -> List[sqlite3.Row]:
        now_us = int(time.time_ns() // 1_000)
        with immediate_txn(self.connf) as con:
            con.row_factory = sqlite3.Row
            rows = con.execute(
                "SELECT id, n_rows, payload FROM staging_rows "
                "WHERE subset_uuid=? AND claimed_by IS NULL "
                "ORDER BY id LIMIT ?",
                (subset_uuid, part_rows * 8),
            ).fetchall()
            if not rows:
                return []
            picked, total = [], 0
            for r in rows:
                nr = int(r["n_rows"])
                if nr <= 0:
                    continue
                if picked and total + nr > part_rows:
                    break
                if not picked and nr > part_rows:
                    picked = [r]
                    total = nr
                    break
                picked.append(r)
                total += nr
            if not picked:
                zero_ids = [r["id"] for r in rows if int(r["n_rows"]) <= 0]
                if zero_ids:
                    con.execute(
                        "DELETE FROM staging_rows WHERE subset_uuid=? AND id IN (" +
                        ",".join("?" * len(zero_ids)) + ")",
                        (subset_uuid, *zero_ids),
                    )
                return []
            ids_to_claim = [r["id"] for r in picked]
            con.execute(
                "UPDATE staging_rows SET claimed_by=?, claimed_at=? "
                "WHERE id IN (" + ",".join("?" * len(ids_to_claim)) + ") AND claimed_by IS NULL",
                (token, now_us, *ids_to_claim),
            )
            claimed = con.execute(
                "SELECT id, n_rows, payload FROM staging_rows WHERE claimed_by=? ORDER BY id",
                (token,),
            ).fetchall()
            return claimed

    def unclaim(self, token: str):
        with immediate_txn(self.connf) as con:
            con.execute("UPDATE staging_rows SET claimed_by=NULL, claimed_at=NULL WHERE claimed_by=?", (token,))

    def delete_claimed(self, token: str):
        with immediate_txn(self.connf) as con:
            con.execute("DELETE FROM staging_rows WHERE claimed_by=?", (token,))

    def checkpoint(self):
        try:
            with sqlite3.connect(self.db_path) as d:
                d.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        except Exception:
            pass

    def hot_subsets(self, limit: int = 256) -> List[str]:
        with self.connf() as con:
            con.row_factory = sqlite3.Row
            rows = con.execute(
                "SELECT subset_uuid, MIN(id) AS first_id "
                "FROM staging_rows WHERE claimed_by IS NULL "
                "GROUP BY subset_uuid ORDER BY first_id LIMIT ?",
                (limit,),
            ).fetchall()
            return [r["subset_uuid"] for r in rows]
