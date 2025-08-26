# datamgr/manager.py
from typing import Dict, Any, Tuple, Optional, Union
import h5py, os, json
import numpy as np
from .atoms import Hooks, StorageScheme, SubsetLease, subset_lock_path, publish_part, DatasetLease, dataset_lock_path, safe_unlink_inside, prune_empty_dirs, default_conn_factory, db_txn_immediate, from_h5_storage_array
from .manifest import assert_safe_field_name, epoch_us_to_iso, Manifest

SQL_TO_NUMPY = {
    "INTEGER": np.int64,
    "REAL":    np.float64,
    "BOOLEAN": np.bool_,
    "TEXT":    np.dtype("U256"),
}
SUBSET_META_OVERRIDES = {
    "subset_uuid":          np.dtype("U36"),
    "created_at_utc":       np.dtype("U40"),
    "marked_for_deletion":  np.bool_,
    "total_rows":           np.int64,
    "buffer_rows":          np.int64,
}
PARTS_META_OVERRIDES = {
    "part_uuid":            np.dtype("U36"),
    "subset_uuid":          np.dtype("U36"),
    "created_at_utc":       np.dtype("U40"),
    "n_rows":               np.int64,
    "scheme_version":       np.int64,
    "file_relpath":         np.dtype("U256"),
    "marked_for_deletion":  np.bool_,
}

def dtype_to_canonical_json(dt: np.dtype) -> str:
    items = []
    for name in dt.names:
        fdt = dt.fields[name][0]
        if fdt.subdtype:
            base, shape = fdt.subdtype
        else:
            base, shape = fdt, ()
        items.append({"name": name, "base": base.str, "shape": list(shape)})
    return json.dumps(items, separators=(",", ":"), sort_keys=False)

def dtype_from_canonical_json(js: str) -> np.dtype:
    items = json.loads(js)
    out = []
    for it in items:
        base = np.dtype(it["base"])
        shape = tuple(it["shape"])
        if shape:
            out.append((it["name"], base, shape))
        else:
            out.append((it["name"], base))
    return np.dtype(out)

def dtype_from_json_descr(js: str) -> np.dtype:
    def tuplify(x):
        if isinstance(x, list):
            return tuple(tuplify(y) for y in x)
        return x
    descr = json.loads(js)
    descr = [tuplify(item) for item in descr]
    return np.dtype(descr)

def widen_unicode_dtype(dt: np.dtype, max_len: int = 256) -> np.dtype:
    out = []
    for name in dt.names:
        fdt, offset = dt.fields[name][:2]
        base, shape = fdt.base, fdt.shape
        if base.kind == "U":
            widened = np.dtype(f"<U{int(max_len)}")
            new = np.dtype((widened, shape)) if shape else widened
        else:
            new = fdt
        out.append((name, new, shape) if shape else (name, new))
    return np.dtype(out)

def maybe_widen_text_fields(canon: np.dtype, incoming: np.dtype) -> Union[np.dtype, None]:
    assert canon.names == incoming.names, "field mismatch"
    widened_items = []
    changed = False
    for name in canon.names:
        c_fdt = canon.fields[name][0]
        i_fdt = incoming.fields[name][0]
        if c_fdt.subdtype:
            c_base, shape = c_fdt.subdtype
        else:
            c_base, shape = c_fdt, ()
        if i_fdt.subdtype:
            i_base, i_shape = i_fdt.subdtype
        else:
            i_base, i_shape = i_fdt, ()
        if i_shape != shape:
            widened_items.append((name, c_base, shape) if shape else (name, c_base))
            continue
        if c_base.kind in ("U", "S") and i_base.kind in ("U", "S"):
            if i_base.itemsize > c_base.itemsize:
                changed = True
                new_base = np.dtype(f"{i_base.str}")
            else:
                new_base = c_base
        else:
            new_base = c_base
        widened_items.append((name, new_base, shape) if shape else (name, new_base))
    if not changed:
        return None
    return np.dtype(widened_items)

def sql_to_numpy_dtype(sql_type: str, col_name: Optional[str] = None, *, is_parts=False) -> np.dtype:
    if is_parts and col_name in PARTS_META_OVERRIDES:
        return PARTS_META_OVERRIDES[col_name]
    if not is_parts and col_name in SUBSET_META_OVERRIDES:
        return SUBSET_META_OVERRIDES[col_name]
    try:
        return SQL_TO_NUMPY[sql_type.upper()]
    except KeyError:
        raise ValueError(f"Unsupported SQL type: {sql_type!r}")

def normalize_numeric_dtype(dt: np.dtype) -> np.dtype:
    if np.issubdtype(dt, np.bool_):
        return np.dtype(np.bool_)
    if np.issubdtype(dt, np.integer):
        return np.dtype(np.int64)
    if np.issubdtype(dt, np.floating):
        return np.dtype(np.float64)
    return dt

def dict_to_structured(field_data: Dict[str, Any], *, is_group: bool, max_str_len: int = 256) -> np.ndarray:
    if not isinstance(field_data, dict):
        raise TypeError("field_data_dict must be a dict of field -> array-like")
    names = sorted(field_data.keys())
    for nm in names:
        assert_safe_field_name(nm)
    if is_group:
        lengths = []
        norm = {}
        for k in names:
            a = np.asarray(field_data[k])
            if a.ndim == 0:
                raise ValueError(f"is_group=True requires 1D+ arrays; field {k!r} is scalar")
            lengths.append(a.shape[0])
            norm[k] = a
        if len(set(lengths)) != 1:
            raise ValueError("All fields must have the same number of rows for is_group=True.")
        nrows = int(lengths[0])
        if nrows == 0:
            raise ValueError("is_group=True requires at least one row (got 0).")
    else:
        nrows = 1
        norm = {k: np.asarray(v) for k, v in field_data.items()}
    dtype_list = []
    for k in names:
        a = norm[k]
        kind = a.dtype.kind
        if kind == "O":
            raise TypeError(f"Field {k!r} has object dtype; only numeric or Unicode strings are allowed")
        if kind == "S":
            raise TypeError(f"Field {k!r} is bytes/ASCII (S*); please provide Unicode strings (U*)")
        if kind == "c":
            raise TypeError(f"Field {k!r} is complex; complex types are not supported")
        if kind in ("m", "M"):
            raise TypeError(f"Field {k!r} is datetime/timedelta; not supported in this dataset")
        if kind == "U":
            ulen = a.dtype.itemsize // 4
            if ulen > max_str_len:
                raise ValueError(f"String field '{k}' exceeds {max_str_len} characters (saw U{ulen}).")
            base = a.dtype
        else:
            base = normalize_numeric_dtype(a.dtype)
        if is_group:
            if a.ndim == 1:
                dtype_list.append((k, base))
            else:
                dtype_list.append((k, base, tuple(a.shape[1:])))
        else:
            if a.ndim == 0:
                dtype_list.append((k, base))
            else:
                dtype_list.append((k, base, tuple(a.shape)))
    for name, typ, *maybe_shape in dtype_list:
        if isinstance(typ, tuple):
            raise AssertionError(
                f"Internal error: nested subarray spec for field {name!r}: {typ} with outer shape {maybe_shape}"
            )
    out = np.zeros(nrows, dtype=dtype_list)
    if is_group:
        for k in names:
            src = norm[k]
            tgt = out.dtype.fields[k][0]
            if tgt.subdtype:
                base_dt, subshape = tgt.subdtype
                if src.ndim == 0 or tuple(src.shape[1:]) != tuple(subshape):
                    raise ValueError(f"Field {k!r} shape mismatch: got {src.shape}, expected (rows,{subshape})")
            else:
                base_dt = tgt
                if src.ndim > 1:
                    raise ValueError(f"Field {k!r} expected scalar/1D per row; got {src.shape}")
            out[k] = src.astype(base_dt, copy=False)
    else:
        for k in names:
            src = norm[k]
            tgt = out.dtype.fields[k][0]
            if tgt.subdtype:
                base_dt, subshape = tgt.subdtype
                if tuple(src.shape) != tuple(subshape):
                    raise ValueError(f"Field {k!r} scalar-row shape mismatch: got {src.shape}, expected {subshape}")
            else:
                base_dt = tgt
                if src.ndim > 0:
                    raise ValueError(f"Field {k!r} expected scalar value; got array with shape {src.shape}")
            out[k][0] = src.astype(base_dt, copy=False)
    return out

def ensure_canonical_dtype(manifest: Manifest, ds_uuid: str, rows: np.ndarray) -> np.dtype:
    if rows.dtype.fields is None:
        raise TypeError("rows must be a structured array")
    schema = manifest.load_schema(ds_uuid)
    djson = schema.get("dtype_descr") or ""
    if not djson:
        cf = default_conn_factory(manifest.catalog_path)
        with db_txn_immediate(cf) as c:
            row = c.execute("SELECT schema_json FROM datasets WHERE dataset_uuid=?", (ds_uuid,)).fetchone()
            cur = json.loads(row[0]) if row and row[0] else {}
            if not cur.get("dtype_descr"):
                cur["dtype_descr"] = dtype_to_canonical_json(rows.dtype)
                c.execute("UPDATE datasets SET schema_json=? WHERE dataset_uuid=?", (json.dumps(cur), ds_uuid))
        manifest.save_schema(ds_uuid, cur)
        return rows.dtype
    canonical = dtype_from_canonical_json(djson)
    widened = maybe_widen_text_fields(canonical, rows.dtype)
    if widened is not None:
        schema["dtype_descr"] = dtype_to_canonical_json(widened)
        manifest.save_schema(ds_uuid, schema)
        canonical = widened
    if not np.can_cast(rows.dtype, canonical, casting="safe"):
        raise TypeError(
            f"Incompatible dtype for dataset: incoming={rows.dtype} cannot be safely cast to canonical={canonical}"
        )
    return canonical

class Manager:
    def __init__(
        self,
        db_root: str,
        *,
        hooks: Optional[Hooks] = None,
        default_scheme: Optional[StorageScheme] = None,
        float_tolerance: float = 1e-6,
        chunk_rows: int = 100_000,
        chunk_mb: float = 8.0,
    ):
        self.manifest = Manifest(db_root)
        self.hooks = hooks or Hooks()
        self.default_scheme = default_scheme or StorageScheme()
        self.float_tolerance = float_tolerance
        self.chunk_rows = int(chunk_rows)
        self.chunk_bytes = int(chunk_mb * 1024 * 1024)
        self._buffers: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def meta(
            self,
            dataset_name: str,
            *,
            subset_start_time: Optional[str] = None,
            subset_end_time: Optional[str] = None,
            exclude_marked_subsets: bool = True,
            parts_start_time: Optional[str] = None,
            parts_end_time: Optional[str] = None,
            exclude_marked_parts: bool = True,
            float_tolerance: Optional[float] = None,
            **queries,
    ):
        ds_uuid = self.manifest.resolve_dataset_uuid(dataset_name)
        row = self.manifest.get_dataset_row(dataset_name)
        schema = json.loads(row["schema_json"])
        scheme = json.loads(row["storage_scheme_json"])
        dataset_info = {
            "dataset_uuid": row["dataset_uuid"],
            "dataset_name": row["alias"],
            "created_at_utc": epoch_us_to_iso(int(row["created_at_epoch"])),
            "schema": schema,
            "storage_scheme": scheme,
            "dtype": (dtype_from_canonical_json(schema["dtype_descr"]) if schema.get("dtype_descr") else None),
            "part_config": self.manifest.get_part_config(ds_uuid),
        }
        subset_rows, parts_by_subset = self.manifest.find_subsets(
            ds_uuid,
            queries,
            start_time=subset_start_time,
            end_time=subset_end_time,
            exclude_marked=exclude_marked_subsets,
            float_tolerance=(self.float_tolerance if float_tolerance is None else float_tolerance),
            return_parts=True,
            parts_start_time=parts_start_time,
            parts_end_time=parts_end_time,
            exclude_marked_parts=exclude_marked_parts,
        )
        key_schema: Dict[str, str] = schema.get("key_schema") or {}
        key_order = schema.get("key_order")
        if not key_order:
            raise RuntimeError("Dataset schema missing 'key_order'.")
        meta_cols = ["subset_uuid", "created_at_utc", "marked_for_deletion", "total_rows", "buffer_rows"]
        subset_dtype = []
        for k in key_order:
            subset_dtype.append((k, sql_to_numpy_dtype(key_schema[k], k)))
        for col in meta_cols:
            subset_dtype.append((col, sql_to_numpy_dtype("TEXT", col)))
        subset_np = np.zeros(len(subset_rows), dtype=subset_dtype)
        for i, r in enumerate(subset_rows):
            for k in key_order:
                t = key_schema[k]
                v = r[k]
                if t == "BOOLEAN": v = bool(v)
                subset_np[k][i] = v
            subset_np["subset_uuid"][i] = r["subset_uuid"]
            subset_np["created_at_utc"][i] = epoch_us_to_iso(int(r["created_at_epoch"]))
            subset_np["marked_for_deletion"][i] = bool(r["marked_for_deletion"])
            subset_np["total_rows"][i] = int(r["total_rows"])
            buf = self._buffers.get((ds_uuid, r["subset_uuid"]))
            subset_np["buffer_rows"][i] = int(buf["n"]) if buf else 0
        parts_rows_flat = []
        for su in subset_np["subset_uuid"].tolist():
            parts_rows_flat.extend(parts_by_subset.get(su, []))
        parts_cols = ["part_uuid", "subset_uuid", "created_at_utc", "n_rows",
                      "scheme_version", "file_relpath", "marked_for_deletion"]
        parts_dtype = [(c, sql_to_numpy_dtype("TEXT", c, is_parts=True)) for c in parts_cols]
        parts_np = np.zeros(len(parts_rows_flat), dtype=parts_dtype)
        for i, r in enumerate(parts_rows_flat):
            parts_np["part_uuid"][i] = r["part_uuid"]
            parts_np["subset_uuid"][i] = r["subset_uuid"]
            parts_np["created_at_utc"][i] = epoch_us_to_iso(int(r["created_at_epoch"]))
            parts_np["n_rows"][i] = int(r["n_rows"])
            parts_np["scheme_version"][i] = int(r["scheme_version"])
            parts_np["file_relpath"][i] = r["file_relpath"]
            parts_np["marked_for_deletion"][i] = bool(r["marked_for_deletion"])
        return {
            "dataset_info": dataset_info,
            "subset_info": subset_np,
            "parts_info": parts_np,
        }

    def data(self, meta_info: Dict[str, Any]):
        ds = meta_info["dataset_info"]
        subset_info = meta_info["subset_info"]
        parts_info = meta_info["parts_info"]
        canonical = ds["dtype"]
        if parts_info.size == 0:
            return {
                "data": np.empty((0,), dtype=(canonical or np.dtype([]))),
                "subset_bounds": np.zeros((subset_info.shape[0] + 1,), dtype=np.int64),
                "part_bounds": np.zeros((1,), dtype=np.int64),
                "part_row_bounds": np.zeros((subset_info.shape[0] + 1,), dtype=np.int64),
                "missing_parts": [],
            }
        if canonical is None:
            raise RuntimeError("Dataset has parts but no canonical dtype locked.")
        total_rows = int(parts_info["n_rows"].sum())
        out = np.empty(total_rows, dtype=canonical)
        pos = 0
        subset_bounds = [0]
        part_bounds = [0]
        part_row_bounds = [0]
        missing = []
        ds_root = self.manifest.dataset_root(ds["dataset_uuid"])
        for su in subset_info["subset_uuid"].tolist():
            ps = parts_info[parts_info["subset_uuid"] == su]
            parts_seen = 0
            for pr in ps:
                rel = pr["file_relpath"]
                p = os.path.join(ds_root, rel)
                try:
                    with h5py.File(p, "r") as f:
                        storage = f["data"][...]
                    arr = from_h5_storage_array(storage, canonical)
                except (FileNotFoundError, OSError):
                    missing.append(rel)
                    continue
                if arr.dtype != canonical:
                    decoded = np.empty(arr.shape, dtype=canonical)
                    for name in canonical.names:
                        src = arr[name]
                        tgt_dt = canonical.fields[name][0]
                        if src.dtype.kind == "S" and tgt_dt.kind == "U":
                            decoded[name] = np.char.decode(src, "utf-8").astype(tgt_dt, copy=False)
                        elif src.dtype == tgt_dt or np.can_cast(src.dtype, tgt_dt, casting="safe"):
                            decoded[name] = src.astype(tgt_dt, copy=False)
                        else:
                            try:
                                decoded[name] = src.astype(tgt_dt, copy=False)
                            except Exception:
                                raise TypeError(f"Field {name!r}: cannot cast {src.dtype} -> {tgt_dt}")
                    arr = decoded
                n = int(arr.shape[0])
                out[pos:pos + n] = arr
                pos += n
                part_bounds.append(pos)
                parts_seen += 1
            subset_bounds.append(pos)
            part_row_bounds.append(part_row_bounds[-1] + parts_seen)
        if pos < out.shape[0]:
            out = out[:pos]
        return {
            "data": out,
            "subset_bounds": np.asarray(subset_bounds, dtype=np.int64),
            "part_bounds": np.asarray(part_bounds, dtype=np.int64),
            "part_row_bounds": np.asarray(part_row_bounds, dtype=np.int64),
            "missing_parts": missing,
        }

    def soft_delete(
            self,
            dataset_name: str,
            *,
            queries: Optional[Dict[str, Any]] = None,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            mark_parts: bool = True,
            parts_start_time: Optional[str] = None,
            parts_end_time: Optional[str] = None,
            unmark: bool = False,
    ) -> Dict[str, int]:
        ds_uuid = self.manifest.resolve_dataset_uuid(dataset_name)
        res = self.manifest.find_subsets(ds_uuid,
                                         queries or {},
                                         start_time=start_time,
                                         end_time=end_time,
                                         exclude_marked=False,
                                         return_parts=mark_parts,
                                         parts_start_time=parts_start_time,
                                         parts_end_time=parts_end_time,
                                         exclude_marked_parts=False,
                                         )
        if mark_parts:
            subset_rows, parts_by_subset = res
        else:
            subset_rows, parts_by_subset = res, {}
        subset_ids = [r["subset_uuid"] for r in subset_rows]
        part_ids = [p["part_uuid"] for su in subset_ids for p in parts_by_subset.get(su, [])]
        subsets_changed = self.manifest.mark_subsets(ds_uuid, subset_ids, marked=not unmark) if subset_ids else 0
        parts_changed = self.manifest.mark_parts(ds_uuid, part_ids, marked=not unmark) if part_ids else 0
        return {"subsets": subsets_changed, "parts": parts_changed}

    def delete(self, dataset_name: str) -> Dict[str, int]:
        ds_uuid = self.manifest.resolve_dataset_uuid(dataset_name)
        ds_root = self.manifest.dataset_root(ds_uuid)
        with DatasetLease(dataset_lock_path(ds_root), self.hooks, ds_uuid=ds_uuid):
            try:
                self.manifest.fsck_dataset(ds_uuid, insert_orphans=True)
            except Exception:
                pass
            rows = self.manifest.list_marked_parts(ds_uuid)
            files_removed = 0
            touched = set()
            for r in rows:
                try:
                    subset_dir = os.path.join(ds_root, "subsets", r["subset_uuid"])
                    file_abs = os.path.join(ds_root, r["file_relpath"])
                    if safe_unlink_inside(ds_root, r["file_relpath"]):
                        files_removed += 1
                        prune_empty_dirs(os.path.dirname(file_abs), subset_dir)
                except Exception:
                    pass
            part_ids = [r["part_uuid"] for r in rows]
            touched.update([r["subset_uuid"] for r in rows])
            parts_deleted, subsets_deleted, doomed_ids = self.manifest.gc_commit(ds_uuid, part_ids, touched)
            for su in doomed_ids:
                sub_dir = os.path.join(ds_root, "subsets", su)
                try:
                    if os.path.isdir(sub_dir):
                        import shutil
                        shutil.rmtree(sub_dir, ignore_errors=True)
                except Exception:
                    pass
        return {"files_removed": files_removed, "parts_deleted": parts_deleted, "subsets_deleted": subsets_deleted}

    def add(
        self,
        dataset_name: str,
        subset_keys: Dict[str, Any],
        field_data_dict: Dict[str, Any],
        is_group: bool,
        *,
        force_flush: bool = False,
        chunk_rows: Optional[int] = None,
        chunk_mb: Optional[float] = None,
        compression: Optional[str] = None,
        compression_opts: Optional[int] = None,
    ) -> Tuple[str, str]:
        rows = dict_to_structured(field_data_dict, is_group=is_group)
        for n in rows.dtype.names:
            fdt = rows.dtype.fields[n][0]
            if getattr(fdt, "subdtype", None) and getattr(fdt.subdtype[0], "subdtype", None):
                raise TypeError(f"Unsupported nested subarray dtype for field {n!r}: {fdt}")
        ds_uuid, scheme = self.manifest.ensure_dataset(dataset_name, self.default_scheme)
        self.manifest.ensure_key_columns(ds_uuid, subset_keys)
        canonical = ensure_canonical_dtype(self.manifest, ds_uuid, rows)
        for n in canonical.names:
            fdt = canonical.fields[n][0]
            if getattr(fdt, "subdtype", None) and getattr(fdt.subdtype[0], "subdtype", None):
                raise TypeError(f"Unsupported nested subarray dtype for field {n!r}: {fdt}")
        rows = rows.astype(canonical, copy=False)
        part_cfg = self.manifest.get_part_config(ds_uuid)
        if not part_cfg:
            nrows = int(rows.shape[0]) or 1
            bpr = max(1, int(rows.nbytes // nrows))
            candidates = []
            if chunk_rows is not None:
                candidates.append(int(chunk_rows))
            if chunk_mb is not None:
                rows_by_mb = int((float(chunk_mb) * 1024 * 1024) // max(1, bpr))
                candidates.append(max(1, rows_by_mb))
            if not candidates:
                candidates = [100_000]
            part_rows = max(1, min(candidates)) if len(candidates) > 1 else max(1, candidates[0])
            part_cfg = self.manifest.lock_part_config(
                ds_uuid,
                part_rows=part_rows,
                compression=compression,
                compression_opts=compression_opts,
            )
        subset_uuid = self.manifest.get_or_create_subset(
            ds_uuid, subset_keys, float_tolerance=self.float_tolerance
        )
        key = (ds_uuid, subset_uuid)
        buf = self._buffers.get(key)
        if buf is None:
            buf = {"parts": [], "n": 0, "bytes": 0}
            self._buffers[key] = buf
        nrows = int(rows.shape[0])
        try:
            self.hooks.on_buffer_enter(ds_uuid, subset_uuid, nrows)
        except Exception:
            pass
        buf["parts"].append(rows)
        buf["n"] += nrows
        buf["bytes"] += int(rows.nbytes)
        cfg = self.manifest.get_part_config(ds_uuid) or {}
        base_rows = int(cfg.get("part_rows", 100_000))
        trigger_rows = int(chunk_rows) if (chunk_rows is not None and int(chunk_rows) > 0) else base_rows
        if force_flush or buf["n"] >= trigger_rows:
            try:
                self.hooks.on_seal_to_spill(ds_uuid, subset_uuid, buf["n"])
            except Exception:
                pass
            self._flush_subset_buffer(
                ds_uuid, subset_uuid, scheme,
                flush_all=bool(force_flush),
                chunk_rows=(int(chunk_rows) if chunk_rows is not None else None),
            )
        return ds_uuid, subset_uuid

    def flush(self) -> None:
        keys = list(self._buffers.keys())
        for ds_uuid, subset_uuid in keys:
            scheme = self.manifest.get_storage_scheme(ds_uuid)
            self._flush_subset_buffer(ds_uuid, subset_uuid, scheme, flush_all=True)

    def _flush_subset_buffer(
            self,
            ds_uuid: str,
            subset_uuid: str,
            scheme: StorageScheme,
            *,
            flush_all: bool = True,
            chunk_rows: Optional[int] = None,
    ) -> None:
        key = (ds_uuid, subset_uuid)
        buf = self._buffers.get(key)
        if not buf or buf["n"] == 0:
            return
        ds_root = self.manifest.dataset_root(ds_uuid)
        conn_factory = self.manifest.conn_factory_for_dataset(ds_uuid)
        parts = buf["parts"]
        cfg = self.manifest.get_part_config(ds_uuid) or {}
        base_rows = int(cfg.get("part_rows", 100_000))
        target_rows = int(chunk_rows) if (chunk_rows is not None and int(chunk_rows) > 0) else base_rows
        target_rows = max(1, target_rows)
        comp = cfg.get("compression", None)
        comp_opts = cfg.get("compression_opts", None)
        lock_path = subset_lock_path(ds_root, subset_uuid)
        carry = None
        for chunk in parts:
            view = chunk if carry is None else np.concatenate([carry, chunk], axis=0)
            n = int(view.shape[0])
            start = 0
            while start < n:
                end = min(start + target_rows, n)
                if end - start < target_rows:
                    carry = view[start:end]
                    break
                slice_arr = view[start:end]
                with SubsetLease(lock_path, self.hooks, ds_uuid=ds_uuid, subset_uuid=subset_uuid):
                    publish_part(
                        ds_root=ds_root,
                        ds_uuid=ds_uuid,
                        subset_uuid=subset_uuid,
                        arr=slice_arr,
                        scheme=scheme,
                        conn_factory=conn_factory,
                        hooks=self.hooks,
                        compression=comp,
                        compression_opts=comp_opts,
                    )
                start = end
            else:
                carry = None
        if carry is not None and carry.shape[0] > 0:
            if flush_all:
                with SubsetLease(lock_path, self.hooks, ds_uuid=ds_uuid, subset_uuid=subset_uuid):
                    publish_part(
                        ds_root=ds_root, ds_uuid=ds_uuid, subset_uuid=subset_uuid,
                        arr=carry, scheme=scheme, conn_factory=conn_factory, hooks=self.hooks,
                        compression=comp, compression_opts=comp_opts,
                    )
                self._buffers.pop(key, None)
            else:
                self._buffers[key] = {"parts": [carry], "n": int(carry.shape[0]), "bytes": int(carry.nbytes)}
        else:
            self._buffers.pop(key, None)
