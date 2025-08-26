# datamgr/atoms.py
from __future__ import annotations
from contextlib import contextmanager
import time, os, hashlib, uuid
from dataclasses import dataclass
import json
import h5py
import numpy as np
import unicodedata
from typing import Callable, Optional
from .sqlite_loader import sqlite3

SUPPORTED_HASHES = {"sha256", "sha1", "md5"}

def schema_signature_for_hash(dt: np.dtype) -> bytes:
    items = []
    for name in dt.names:
        fdt = dt.fields[name][0]
        base, shape = (fdt.subdtype if fdt.subdtype else (fdt, ()))
        base_tag = "U" if base.kind == "U" else base.str
        items.append((name, base_tag, tuple(shape)))
    return json.dumps(items, separators=(",", ":"), sort_keys=True).encode("utf-8")

def hash_utf8_lenpref_iter(hasher, scalar_iterable):
    for s in scalar_iterable:
        if not isinstance(s, str):
            s = str(s)
        b = unicodedata.normalize("NFC", s).encode("utf-8")
        hasher.update(len(b).to_bytes(4, "little"))
        hasher.update(b)

def update_hasher_from_structured(hasher, arr: np.ndarray, *, max_chunk_bytes=16*1024*1024):
    hasher.update(schema_signature_for_hash(arr.dtype))
    n = int(arr.shape[0])
    r = max(1, max_chunk_bytes // max(1, arr.dtype.itemsize))
    for start in range(0, n, r):
        end = min(start + r, n)
        sl = slice(start, end)
        for name in arr.dtype.names:
            fdt = arr.dtype.fields[name][0]
            base = fdt.subdtype[0] if fdt.subdtype else fdt
            v = arr[name][sl]
            if base.kind == "U":
                hash_utf8_lenpref_iter(hasher, v.reshape(-1))
            else:
                hasher.update(memoryview(np.ascontiguousarray(v)))

def update_hasher_from_h5_dataset(hasher, dset, *, max_chunk_bytes=16*1024*1024):
    dt = dset.dtype
    hasher.update(schema_signature_for_hash(dt))
    n = int(dset.shape[0])
    itemsize = max(1, dt.itemsize)
    r = max(1, max_chunk_bytes // itemsize)
    for start in range(0, n, r):
        end = min(start + r, n)
        batch = dset[start:end]
        for name in dt.names:
            fdt = dt.fields[name][0]
            base = fdt.subdtype[0] if fdt.subdtype else fdt
            v = batch[name]
            if base.kind == "U" or v.dtype.kind in ("S", "U"):
                vals = np.char.decode(v, "utf-8") if v.dtype.kind == "S" else v
                hash_utf8_lenpref_iter(hasher, vals.reshape(-1))
            else:
                hasher.update(memoryview(np.ascontiguousarray(v)))

def compute_semantic_content_hash(arr: np.ndarray, *, max_chunk_bytes=16*1024*1024) -> str:
    h = hashlib.blake2b(digest_size=16)
    update_hasher_from_structured(h, arr, max_chunk_bytes=max_chunk_bytes)
    return h.hexdigest()

def compute_semantic_content_hash_from_h5(dset, *, max_chunk_bytes=16*1024*1024) -> str:
    h = hashlib.blake2b(digest_size=16)
    update_hasher_from_h5_dataset(h, dset, max_chunk_bytes=max_chunk_bytes)
    return h.hexdigest()

class Hooks:
    def on_subset_lease_acquire(self, ds, subset): pass
    def on_subset_lease_release(self, ds, subset): pass
    def on_buffer_enter(self, ds, subset, nrows): pass
    def on_seal_to_spill(self, ds, subset, nrows): pass
    def on_publish_begin(self, ds, subset, part_uuid): pass
    def on_publish_fsynced(self, ds, subset, part_uuid): pass
    def on_publish_renamed(self, ds, subset, part_uuid): pass
    def on_publish_dir_fsynced(self, ds, subset, part_uuid): pass
    def on_manifest_txn_begin(self, ds): pass
    def on_manifest_txn_commit(self, ds): pass
    def on_manifest_txn_rollback(self, ds, err): pass

@contextmanager
def db_txn_immediate(conn_factory: Callable[[], sqlite3.Connection], retries: int = 5, backoff: float = 0.02):
    for attempt in range(retries):
        conn = conn_factory()
        try:
            conn.isolation_level = None
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            try:
                conn.rollback()
            except Exception:
                pass
            conn.close()
            msg = str(e).lower()
            retryable = any(s in msg for s in (
                "database is locked",
                "database schema is locked",
                "database table is locked",
                "database is busy",
            ))
            if retryable and attempt + 1 < retries:
                time.sleep(backoff * (2 ** attempt))
                continue
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass

def default_conn_factory(db_path: str) -> Callable[[], sqlite3.Connection]:
    def conn_factory() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=wal2;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-65536;")
        return conn
    return conn_factory

class SubsetLease:
    def __init__(self, lockfile_path: str, hooks: Hooks, ds_uuid: str = None, subset_uuid: str = None):
        self.path = lockfile_path
        self.fp = None
        self.hooks = hooks
        self.ds_uuid = ds_uuid
        self.subset_uuid = subset_uuid

    def __enter__(self):
        dirpath = os.path.dirname(self.path)
        makedirs_with_fsync(dirpath)
        pre_exists = os.path.exists(self.path)
        self.fp = open(self.path, "a+")
        if not pre_exists:
            try:
                fsync_dir(dirpath)
            except Exception:
                pass
        locked = False
        try:
            import fcntl
            fcntl.flock(self.fp, fcntl.LOCK_EX)
            locked = True
        except Exception:
            pass
        if not locked:
            try:
                import portalocker
                portalocker.lock(self.fp, portalocker.LOCK_EX)
                locked = True
            except Exception:
                pass
        if not locked and not os.environ.get("ALLOW_UNLOCKED_LEASE"):
            raise RuntimeError("No locking backend available for SubsetLease")
        try:
            self.hooks.on_subset_lease_acquire(self.ds_uuid, self.subset_uuid)
        except Exception:
            pass
        return self

    def __exit__(self, exc_type, exc, tb):
        unlocked = False
        try:
            import fcntl
            fcntl.flock(self.fp, fcntl.LOCK_UN)
            unlocked = True
        except Exception:
            pass
        if not unlocked:
            try:
                import portalocker
                portalocker.unlock(self.fp)
            except Exception:
                pass
        try:
            self.hooks.on_subset_lease_release(self.ds_uuid, self.subset_uuid)
        except Exception:
            pass
        try:
            self.fp.close()
        finally:
            self.fp = None

def subset_lock_path(ds_root: str, subset_uuid: str) -> str:
    return os.path.join(ds_root, "locks", "subsets", f"{subset_uuid}.lock")

@dataclass
class StorageScheme:
    version: int = 1
    hash: str = "sha256"
    depth: int = 0
    seglen: int = 2

def scheme_to_json(s: StorageScheme) -> str:
    return json.dumps({"version": s.version, "hash": s.hash, "depth": s.depth, "seglen": s.seglen})

def scheme_from_json(js: str) -> StorageScheme:
    d = json.loads(js)
    return StorageScheme(version=int(d["version"]), hash=d["hash"], depth=int(d["depth"]), seglen=int(d["seglen"]))

def validate_storage_scheme(s: StorageScheme):
    if s.hash not in SUPPORTED_HASHES:
        raise ValueError(f"Unsupported hash '{s.hash}'")
    if s.depth < 0:
        raise ValueError("depth must be >= 0")
    if s.depth > 0 and s.seglen <= 0:
        raise ValueError("seglen must be > 0 when depth > 0")
    max_hex = 64 if s.hash == "sha256" else hashlib.new(s.hash).digest_size * 2
    if s.depth * s.seglen > max_hex:
        raise ValueError("depth*seglen exceeds available hash hex length")

def part_relpath(subset_uuid: str, part_uuid: str, scheme: StorageScheme) -> str:
    validate_storage_scheme(scheme)
    base = f"subsets/{subset_uuid}/parts/v{scheme.version}"
    if scheme.depth <= 0:
        return f"{base}/{part_uuid}.h5"
    h = hashlib.new(scheme.hash)
    h.update((subset_uuid + part_uuid).encode("utf-8"))
    hexs = h.hexdigest()
    slices = [hexs[i * scheme.seglen:(i + 1) * scheme.seglen] for i in range(scheme.depth)]
    return f"{base}/{'/'.join(slices)}/{part_uuid}.h5"

def fsync_dir(path: str) -> None:
    try:
        flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            flags |= os.O_DIRECTORY
        dfd = os.open(path, flags)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except Exception:
        pass

def makedirs_with_fsync(path: str) -> None:
    to_make = []
    cur = os.path.abspath(path)
    while not os.path.isdir(cur):
        to_make.append(cur)
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent
    for d in reversed(to_make):
        parent = os.path.dirname(d)
        os.makedirs(d, exist_ok=True)
        fsync_dir(parent)

def cleanup_stale_tmps_in_dir(dirpath: str, older_than_seconds: int = 24 * 3600) -> None:
    now = time.time()
    try:
        for fn in os.listdir(dirpath):
            if not fn.endswith(".h5.tmp"):
                continue
            p = os.path.join(dirpath, fn)
            try:
                st = os.stat(p)
                if (now - st.st_mtime) >= older_than_seconds:
                    os.remove(p)
            except Exception:
                pass
        try:
            fsync_dir(dirpath)
        except Exception:
            pass
    except Exception:
        pass

def h5_storage_dtype(np_dt: np.dtype) -> np.dtype:
    fields = []
    for name in np_dt.names:
        fdt = np_dt.fields[name][0]
        if fdt.kind == "U":
            if fdt.subdtype:
                base_dt, shape = fdt.subdtype
                fields.append((name, np.dtype((np.dtype(f"S{base_dt.itemsize}"), shape))))
            else:
                fields.append((name, np.dtype(f"S{fdt.itemsize}")))
        else:
            fields.append((name, fdt))
    return np.dtype(fields)

def to_h5_storage_array(arr: np.ndarray, storage_dt: np.dtype) -> np.ndarray:
    out = np.empty(arr.shape, dtype=storage_dt)
    for name in arr.dtype.names:
        src = arr[name]
        if src.dtype.kind == "U":
            out[name] = np.char.encode(src, "utf-8")
        else:
            out[name] = src
    return out

def from_h5_storage_array(h5_arr: np.ndarray, target_dt: np.dtype) -> np.ndarray:
    out = np.empty(h5_arr.shape, dtype=target_dt)
    for name in target_dt.names:
        src = h5_arr[name]
        tgt = target_dt.fields[name][0]
        base = tgt.subdtype[0] if tgt.subdtype else tgt
        if base.kind == "U":
            out[name] = np.char.decode(src, "utf-8")
        else:
            out[name] = src
    return out

def publish_part(
    ds_root: str,
    ds_uuid: str,
    subset_uuid: str,
    arr,
    scheme: StorageScheme,
    conn_factory: Callable[[], sqlite3.Connection],
    hooks: Hooks,
    *,
    compression: Optional[str] = None,
    compression_opts: Optional[int] = None,
):
    def safe_call(fn, *a, **kw):
        try:
            fn(*a, **kw)
        except Exception:
            pass
    if getattr(arr, "dtype", None) is None or arr.dtype.fields is None:
        raise TypeError("publish_part expects a structured numpy array")
    part_uuid = str(uuid.uuid4())
    rel = part_relpath(subset_uuid, part_uuid, scheme)
    abs_tmp = os.path.join(ds_root, rel + ".tmp")
    abs_dst = os.path.join(ds_root, rel)
    dirpath = os.path.dirname(abs_dst)
    makedirs_with_fsync(dirpath)
    cleanup_stale_tmps_in_dir(dirpath)
    safe_call(hooks.on_publish_begin, ds_uuid, subset_uuid, part_uuid)
    created_epoch = int(time.time_ns() // 1_000)
    n_rows = int(getattr(arr, "shape", (0,))[0] or 0)
    if n_rows <= 0:
        raise ValueError("publish_part received an empty array")
    h = compute_semantic_content_hash(arr)
    conn_chk = conn_factory()
    try:
        conn_chk.row_factory = sqlite3.Row
        row = conn_chk.execute(
            "SELECT part_uuid, file_relpath FROM parts "
            "WHERE subset_uuid=? AND content_hash=? AND marked_for_deletion=0 "
            "LIMIT 1",
            (subset_uuid, h),
        ).fetchone()
    finally:
        conn_chk.close()
    if row:
        return row["part_uuid"], row["file_relpath"]
    try:
        with h5py.File(abs_tmp, "w") as f:
            storage_dt = h5_storage_dtype(arr.dtype)
            h5_data = to_h5_storage_array(arr, storage_dt)
            dset = f.create_dataset(
                "data",
                data=h5_data,
                chunks=(n_rows,),
                compression=compression,
                compression_opts=(compression_opts if compression is not None else None),
                maxshape=(None,),
            )
            f.attrs["part_uuid"] = part_uuid
            f.attrs["subset_uuid"] = subset_uuid
            f.attrs["dataset_uuid"] = ds_uuid
            f.attrs["created_at_epoch"] = created_epoch
            f.attrs["n_rows"] = n_rows
            f.attrs["scheme_version"] = scheme.version
            f.attrs["content_hash"] = h
            f.flush()
            did_fsync = False
            try:
                vfd = f.id.get_vfd_handle()
                if isinstance(vfd, int):
                    os.fsync(vfd)
                    did_fsync = True
            except Exception:
                pass
            if not did_fsync:
                try:
                    fd2 = os.open(abs_tmp, os.O_RDONLY)
                    try:
                        os.fsync(fd2)
                    finally:
                        os.close(fd2)
                except Exception:
                    pass
        safe_call(hooks.on_publish_fsynced, ds_uuid, subset_uuid, part_uuid)
        os.replace(abs_tmp, abs_dst)
        safe_call(hooks.on_publish_renamed, ds_uuid, subset_uuid, part_uuid)
        fsync_dir(dirpath)
        safe_call(hooks.on_publish_dir_fsynced, ds_uuid, subset_uuid, part_uuid)
    except Exception:
        try:
            if os.path.exists(abs_tmp):
                os.remove(abs_tmp)
                try:
                    fsync_dir(os.path.dirname(abs_tmp))
                except Exception:
                    pass
        except Exception:
            pass
        raise
    def insert_conn(conn: sqlite3.Connection):
        safe_call(hooks.on_manifest_txn_begin, ds_uuid)
        cols = ["part_uuid", "subset_uuid", "created_at_epoch",
                "scheme_version", "n_rows", "file_relpath",
                "marked_for_deletion", "content_hash"]
        vals = [part_uuid, subset_uuid, created_epoch,
                scheme.version, n_rows, rel, 0, h]
        q = f"INSERT INTO parts({','.join(cols)}) VALUES({','.join(['?'] * len(cols))})"
        try:
            conn.execute(q, vals)
            conn.execute("UPDATE subsets SET total_rows = total_rows + ? WHERE subset_uuid=?",
                         (n_rows, subset_uuid))
            return part_uuid, rel
        except sqlite3.IntegrityError:
            try:
                if os.path.exists(abs_dst):
                    os.remove(abs_dst)
                    try:
                        fsync_dir(dirpath)
                    except Exception:
                        pass
            except Exception:
                pass
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT part_uuid, file_relpath FROM parts "
                "WHERE subset_uuid=? AND content_hash=? AND marked_for_deletion=0 "
                "LIMIT 1",
                (subset_uuid, h),
            ).fetchone()
            if row:
                return row["part_uuid"], row["file_relpath"]
            raise
    try:
        with db_txn_immediate(conn_factory) as conn_:
            res_part_uuid, res_rel = insert_conn(conn_)
            safe_call(hooks.on_manifest_txn_commit, ds_uuid)
    except Exception as e:
        safe_call(hooks.on_manifest_txn_rollback, ds_uuid, e)
        raise
    return res_part_uuid, res_rel

class DatasetLease:
    def __init__(self, lockfile_path: str, hooks: Hooks, ds_uuid: str = None):
        self.path = lockfile_path
        self.fp = None
        self.hooks = hooks
        self.ds_uuid = ds_uuid

    def __enter__(self):
        dirpath = os.path.dirname(self.path)
        makedirs_with_fsync(dirpath)
        pre_exists = os.path.exists(self.path)
        self.fp = open(self.path, "a+")
        if not pre_exists:
            try:
                fsync_dir(dirpath)
            except Exception:
                pass
        locked = False
        try:
            import fcntl
            fcntl.flock(self.fp, fcntl.LOCK_EX)
            locked = True
        except Exception:
            pass
        if not locked:
            try:
                import portalocker
                portalocker.lock(self.fp, portalocker.LOCK_EX)
                locked = True
            except Exception:
                pass
        if not locked:
            if not os.environ.get("ALLOW_UNLOCKED_LEASE"):
                raise RuntimeError("No locking backend available for DatasetLease")
        return self

    def __exit__(self, *args):
        unlocked = False
        try:
            import fcntl
            fcntl.flock(self.fp, fcntl.LOCK_UN)
            unlocked = True
        except Exception:
            pass
        if not unlocked:
            try:
                import portalocker
                portalocker.unlock(self.fp)
            except Exception:
                pass
        try:
            self.fp.close()
        finally:
            self.fp = None

def dataset_lock_path(ds_root: str) -> str:
    return os.path.join(ds_root, "locks", "dataset.lock")

def safe_unlink_inside(root: str, rel: str) -> bool:
    p = os.path.realpath(os.path.join(root, rel))
    r = os.path.realpath(root)
    if not (p == r or p.startswith(r + os.sep)):
        raise ValueError(f"Unsafe path outside dataset root: {rel!r}")
    if p == r:
        raise ValueError("Refusing to unlink the dataset root")
    if os.path.exists(p):
        os.remove(p)
        try:
            fsync_dir(os.path.dirname(p))
        except Exception:
            pass
        return True
    return False

def prune_empty_dirs(start: str, stop_at: str) -> None:
    cur = os.path.realpath(start)
    stop = os.path.realpath(stop_at)
    while cur.startswith(stop) and cur != stop:
        try: os.rmdir(cur)
        except OSError:
            break
        cur = os.path.dirname(cur)

def batched(it, n: int):
    it = iter(it)
    while True:
        chunk = []
        try:
            for _ in range(n):
                chunk.append(next(it))
        except StopIteration:
            pass
        if not chunk:
            return
        yield chunk
