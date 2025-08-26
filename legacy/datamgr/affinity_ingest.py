# datamgr/affinity_ingest
from __future__ import annotations
import pickle
import uuid
import queue
import time
from multiprocessing import Process, Queue, get_context
from typing import Any, Callable, Dict, List, Optional, Sequence
from joblib import Parallel, delayed
from tqdm import tqdm
try:
    from tqdm_joblib import tqdm_joblib
except Exception:
    from contextlib import contextmanager
    @contextmanager
    def tqdm_joblib(*_a, **_k):
        yield
import numpy as np
from .manager import Manager
from .ingest_core import Router, Stager, Payload

DEFAULT_STALE_CLAIM_SECONDS = 30 * 60

def assert_picklable(obj, name: str):
    try:
        pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as e:
        raise TypeError(f"{name} must be picklable for multiprocessing 'spawn': {e}") from e

def compact_subset(
    manager,
    ds_uuid: str,
    stager,
    dataset_name: str,
    subset_uuid: str,
    add_kwargs: Optional[Dict[str, Any]],
    stale_seconds: int,
    staged_rows_dict: Dict[str, int],
) -> None:
    if stager is None:
        return
    cfg = manager.manifest.get_part_config(ds_uuid) or {}
    part_rows_val = int(cfg.get("part_rows", 100_000))
    while staged_rows_dict.get(subset_uuid, 0) >= part_rows_val:
        stager.reclaim_stale(stale_after_seconds=stale_seconds)
        claim_token = str(uuid.uuid4())
        claimed_rows = stager.select_and_claim_prefix(subset_uuid, part_rows_val, claim_token)
        if not claimed_rows:
            stager.unclaim(claim_token)
            break
        total_rows = sum(int(r["n_rows"]) for r in claimed_rows)
        if total_rows < part_rows_val:
            stager.unclaim(claim_token)
            break
        try:
            bufs: Dict[str, List[np.ndarray]] = {}
            merged_subset_keys = None
            for rec in claimed_rows:
                payload_bytes = rec["payload"]
                if not (isinstance(payload_bytes, (bytes, bytearray)) and payload_bytes.startswith(b"DMST\x01")):
                    raise ValueError("bad staging payload magic/version")
                sk2, fd2, _g2 = pickle.loads(payload_bytes[len(b"DMST\x01"):])
                merged_subset_keys = sk2
                for fname, fvals in fd2.items():
                    bufs.setdefault(fname, []).append(np.atleast_1d(np.asarray(fvals)))
            merged_fields = {
                fname: (arrs[0] if len(arrs) == 1 else np.concatenate(arrs, axis=0))
                for fname, arrs in bufs.items()
            }
            kw = dict(add_kwargs or {})
            n_rows = next(iter(merged_fields.values())).shape[0]
            kw["chunk_rows"] = int(n_rows)
            manager.add(
                dataset_name=dataset_name,
                subset_keys=merged_subset_keys,
                field_data_dict=merged_fields,
                is_group=True,
                force_flush=True,
                **kw,
            )
        except Exception:
            stager.unclaim(claim_token)
            raise
        else:
            stager.delete_claimed(claim_token)
            staged_rows_dict[subset_uuid] = max(
                0, staged_rows_dict.get(subset_uuid, 0) - total_rows
            )

def writer_loop(
    db_root: str,
    dataset_name: str,
    q: Queue,
    manager_kwargs: Optional[Dict[str, Any]],
    add_kwargs: Optional[Dict[str, Any]],
    *,
    crash_safe: bool = False,
    durable_staging: bool = False,
    stale_claim_seconds: int = DEFAULT_STALE_CLAIM_SECONDS,
    checkpoint_after_compact: bool = False,
):
    mgr = Manager(db_root, **(manager_kwargs or {}))
    ds_uuid, _ = mgr.manifest.ensure_dataset(dataset_name)
    def _direct_add(subset_keys_, field_data_dict_, is_group_):
        kw = dict(add_kwargs or {})
        if "chunk_rows" not in kw:
            cfg = mgr.manifest.get_part_config(ds_uuid) or {}
            pr = cfg.get("part_rows")
            if pr:
                kw["chunk_rows"] = int(pr)
        mgr.add(
            dataset_name=dataset_name,
            subset_keys=subset_keys_,
            field_data_dict=field_data_dict_,
            is_group=is_group_,
            force_flush=False,
            **kw,
        )
    if not crash_safe:
        while True:
            try:
                item = q.get()
            except (EOFError, OSError):
                break
            if item is None:
                break
            subset_keys, field_data_dict, is_group = item
            _direct_add(subset_keys, field_data_dict, is_group)
        try:
            mgr.flush()
        except Exception:
            pass
        return
    stager = Stager(mgr.manifest.dataset_db_path(ds_uuid), durable=durable_staging)
    queued_rows: Dict[str, int] = {}
    def _merge_and_publish(claimed_rows):
        """Merge claimed staging rows and publish one part."""
        bufs: Dict[str, List[np.ndarray]] = {}
        merged_subset_keys = None
        for rec in claimed_rows:
            payload = rec["payload"]
            if not (isinstance(payload, (bytes, bytearray)) and payload.startswith(b"DMST\x01")):
                raise ValueError("bad staging payload magic/version")
            sk, fields, _grp = pickle.loads(payload[len(b"DMST\x01"):])
            merged_subset_keys = sk
            for fname, fval in fields.items():
                bufs.setdefault(fname, []).append(np.atleast_1d(np.asarray(fval)))
        merged_fields = {
            fname: (chunks[0] if len(chunks) == 1 else np.concatenate(chunks, axis=0))
            for fname, chunks in bufs.items()
        }
        kw = dict(add_kwargs or {})
        n_rows_ = next(iter(merged_fields.values())).shape[0]
        kw["chunk_rows"] = int(n_rows_)
        mgr.add(
            dataset_name=dataset_name,
            subset_keys=merged_subset_keys,
            field_data_dict=merged_fields,
            is_group=True,
            force_flush=True,
            **kw,
        )
    def _attempt_compact_for_subset(su: str, allow_remainder: bool) -> int:
        """Claim a prefix for su and publish; returns rows published (0 = nothing)."""
        part_cfg = mgr.manifest.get_part_config(ds_uuid) or {}
        part_rows = int(part_cfg.get("part_rows", 100_000))
        stager.reclaim_stale(stale_after_seconds=stale_claim_seconds)
        token = str(uuid.uuid4())
        claimed = stager.select_and_claim_prefix(su, part_rows, token)
        if not claimed:
            return 0
        rows_claimed = sum(int(r["n_rows"]) for r in claimed)
        if rows_claimed < part_rows and not allow_remainder:
            stager.unclaim(token)
            return 0
        try:
            _merge_and_publish(claimed)
        except Exception:
            stager.unclaim(token)
            raise
        else:
            stager.delete_claimed(token)
            queued_rows[su] = max(0, queued_rows.get(su, 0) - rows_claimed)
            return rows_claimed
    while True:
        try:
            item = q.get()
        except (EOFError, OSError):
            break
        if item is None:
            break
        subset_keys, field_data_dict, is_group = item
        mgr.manifest.ensure_key_columns(ds_uuid, subset_keys)
        subset_uuid = mgr.manifest.get_or_create_subset(ds_uuid, subset_keys)
        if is_group:
            lengths = []
            for _fname, _val in field_data_dict.items():
                arr = np.asarray(_val)
                if arr.ndim == 0:
                    raise ValueError(f"is_group=True requires arrays; a field is scalar")
                lengths.append(arr.shape[0])
            if len(set(lengths)) != 1:
                raise ValueError(f"is_group=True requires equal-length arrays; got lengths={lengths}")
            n_rows = int(lengths[0])
        else:
            n_rows = 1
        blob = b"DMST\x01" + pickle.dumps((subset_keys, field_data_dict, is_group), protocol=pickle.HIGHEST_PROTOCOL)
        stager.enqueue(subset_uuid, n_rows, blob)
        queued_rows[subset_uuid] = queued_rows.get(subset_uuid, 0) + n_rows
        part_cfg_now = mgr.manifest.get_part_config(ds_uuid) or {}
        part_rows_now = int(part_cfg_now.get("part_rows", 100_000))
        while queued_rows.get(subset_uuid, 0) >= part_rows_now:
            if _attempt_compact_for_subset(subset_uuid, allow_remainder=False) == 0:
                break
    try:
        part_cfg = mgr.manifest.get_part_config(ds_uuid) or {}
        part_rows = int(part_cfg.get("part_rows", 100_000))
        stager.reclaim_stale(stale_after_seconds=stale_claim_seconds)
        while True:
            hot = stager.hot_subsets(limit=1024)
            if not hot:
                break
            for su in hot:
                while _attempt_compact_for_subset(su, allow_remainder=True) > 0:
                    pass
        if checkpoint_after_compact:
            stager.checkpoint()
        mgr.flush()
    except Exception:
        pass

def compute_payload(worker: Callable[..., Payload], args, kwargs) -> Payload:
    return worker(*args, **kwargs)

def chunk_tasks(args_list: List[tuple], kwargs_list: List[dict], size: int):
    """Yield slices of (args, kwargs) pairs of at most `size`."""
    if size is None or size <= 0:
        yield list(zip(args_list, kwargs_list))
        return
    n = len(args_list)
    for i in range(0, n, size):
        yield list(zip(args_list[i: i + size], kwargs_list[i: i + size]))

def ingest_with_subset_affinity(
    db_root: str,
    dataset_name: str,
    worker: Callable[..., Payload],
    worker_args: Optional[Sequence[tuple]] = None,
    worker_kwargs: Optional[Sequence[dict]] = None,
    *,
    n_jobs_compute: int = -1,
    compute_backend: str = "loky",
    compute_chunk_size: int = 256,
    n_writers: int = 4,
    writer_ctx: str = "spawn",
    writer_queue_maxsize: int = 1024,
    manager_kwargs: Optional[Dict[str, Any]] = None,
    add_kwargs: Optional[Dict[str, Any]] = None,
    desired_part_rows: Optional[int] = None,
    desired_compression: Optional[str] = None,
    desired_compression_opts: Optional[int] = None,
    crash_safe: bool = False,
    durable_staging: bool = False,
    stale_claim_seconds: int = DEFAULT_STALE_CLAIM_SECONDS,
    checkpoint_after_compact: bool = False,
    desc: str = "Ingest",
    raise_on_error: bool = True,
) -> Dict[str, int]:
    args_list = list(worker_args or [])
    kwargs_list = list(worker_kwargs or [])
    if not args_list and not kwargs_list:
        raise ValueError("Either worker_args or worker_kwargs must be provided.")
    if args_list and not kwargs_list:
        kwargs_list = [{} for _ in range(len(args_list))]
    if kwargs_list and not args_list:
        args_list = [tuple() for _ in range(len(kwargs_list))]
    if len(args_list) != len(kwargs_list):
        raise ValueError(
            f"Length mismatch: len(worker_args)={len(args_list)} vs len(worker_kwargs)={len(kwargs_list)}"
        )
    total_tasks = len(args_list)
    if manager_kwargs is not None:
        assert_picklable(manager_kwargs, "manager_kwargs")
    if add_kwargs is not None:
        assert_picklable(add_kwargs, "add_kwargs")
    if desired_part_rows is not None:
        m0 = Manager(db_root, **(manager_kwargs or {}))
        ds_uuid, _ = m0.manifest.ensure_dataset(dataset_name)
        if not m0.manifest.get_part_config(ds_uuid):
            m0.manifest.lock_part_config(
                ds_uuid,
                part_rows=int(desired_part_rows),
                compression=desired_compression,
                compression_opts=desired_compression_opts,
            )
        del m0
    router = Router(db_root, dataset_name, manager_kwargs)
    _ = router.ds_uuid
    mp = get_context(writer_ctx)
    n_writers = max(1, int(n_writers))
    queues: List[Queue] = [mp.Queue(maxsize=max(1, int(writer_queue_maxsize))) for _ in range(n_writers)]
    writers: List[Process] = []
    for i in range(n_writers):
        p = mp.Process(
            target=writer_loop,
            args=(db_root, dataset_name, queues[i], manager_kwargs, add_kwargs),
            kwargs={
                "crash_safe": crash_safe,
                "durable_staging": durable_staging,
                "stale_claim_seconds": int(stale_claim_seconds),
                "checkpoint_after_compact": checkpoint_after_compact,
            },
        )
        p.start()
        writers.append(p)
    def route(payload):
        su_keys, _, _ = payload
        idx = router.partition(su_keys, n_writers)
        while True:
            if not writers[idx].is_alive():
                raise RuntimeError(f"Writer process {idx} is not alive")
            try:
                queues[idx].put(payload, timeout=0.25)
                break
            except queue.Full:
                time.sleep(0.005)
    errors = 0
    for batch in chunk_tasks(args_list, kwargs_list, compute_chunk_size):
        try:
            with tqdm_joblib(tqdm(total=len(batch), desc=f"{desc} batch")):
                results = Parallel(n_jobs=n_jobs_compute, backend=compute_backend, batch_size=1)(
                    delayed(compute_payload)(worker, a, k) for (a, k) in batch
                )
        except Exception:
            errors += len(batch)
            if raise_on_error:
                for q in queues:
                    q.put(None)
                for p in writers:
                    p.join()
                raise
            continue
        for r in results:
            if isinstance(r, Exception):
                errors += 1
                if raise_on_error:
                    for q in queues:
                        q.put(None)
                    for p in writers:
                        p.join()
                    raise r
                continue
            route(r)
    for q in queues:
        q.put(None)
    for p in writers:
        p.join()
    return {"tasks": total_tasks, "errors": errors, "writers": n_writers}


def ingest_serial(
    db_root: str,
    dataset_name: str,
    worker: Callable[..., Payload],
    worker_args: Optional[Sequence[tuple]] = None,
    worker_kwargs: Optional[Sequence[dict]] = None,
    *,
    manager_kwargs: Optional[Dict[str, Any]] = None,
    add_kwargs: Optional[Dict[str, Any]] = None,
    desired_part_rows: Optional[int] = None,
    desired_compression: Optional[str] = None,
    desired_compression_opts: Optional[int] = None,
    crash_safe: bool = False,
    durable_staging: bool = False,
    stale_claim_seconds: int = DEFAULT_STALE_CLAIM_SECONDS,
    checkpoint_after_compact: bool = False,
) -> Dict[str, int]:
    args_list = list(worker_args or [])
    kwargs_list = list(worker_kwargs or [])
    if not args_list and not kwargs_list:
        raise ValueError("Either worker_args or worker_kwargs must be provided.")
    if args_list and not kwargs_list:
        kwargs_list = [{} for _ in range(len(args_list))]
    if kwargs_list and not args_list:
        args_list = [tuple() for _ in range(len(kwargs_list))]
    if desired_part_rows is not None:
        m0 = Manager(db_root, **(manager_kwargs or {}))
        ds_uuid, _ = m0.manifest.ensure_dataset(dataset_name)
        if not m0.manifest.get_part_config(ds_uuid):
            m0.manifest.lock_part_config(
                ds_uuid,
                part_rows=int(desired_part_rows),
                compression=desired_compression,
                compression_opts=desired_compression_opts,
            )
        del m0
    mgr = Manager(db_root, **(manager_kwargs or {}))
    ds_uuid, _ = mgr.manifest.ensure_dataset(dataset_name)
    stager = None
    staged_rows_per_subset: Dict[str, int] = {}
    if crash_safe:
        stager = Stager(mgr.manifest.dataset_db_path(ds_uuid), durable=durable_staging)
    for a, k in tqdm(zip(args_list, kwargs_list), total=len(args_list), desc="Ingest"):
        subset_keys, field_data_dict, is_group = worker(*a, **k)
        if crash_safe:
            mgr.manifest.ensure_key_columns(ds_uuid, subset_keys)
            subset_uuid = mgr.manifest.get_or_create_subset(ds_uuid, subset_keys)
            if is_group:
                lengths = []
                for k2, v2 in field_data_dict.items():
                    a2 = np.asarray(v2)
                    if a2.ndim == 0:
                        raise ValueError(f"is_group=True requires arrays; field {k2!r} is scalar")
                    lengths.append(a2.shape[0])
                if len(set(lengths)) != 1:
                    raise ValueError(f"is_group=True requires equal-length arrays; got lengths={lengths}")
                n_rows = int(lengths[0])
            else:
                n_rows = 1
            payload = b"DMST\x01" + pickle.dumps((subset_keys, field_data_dict, is_group), protocol=pickle.HIGHEST_PROTOCOL)
            stager.enqueue(subset_uuid, n_rows, payload)
            staged_rows_per_subset[subset_uuid] = staged_rows_per_subset.get(subset_uuid, 0) + n_rows
            compact_subset(
                manager=mgr,
                ds_uuid=ds_uuid,
                stager=stager,
                dataset_name=dataset_name,
                subset_uuid=subset_uuid,
                add_kwargs=add_kwargs,
                stale_seconds=stale_claim_seconds,
                staged_rows_dict=staged_rows_per_subset,
            )
        else:
            mgr.manifest.ensure_key_columns(ds_uuid, subset_keys)
            kw = dict(add_kwargs or {})
            if "chunk_rows" not in kw:
                cfg = mgr.manifest.get_part_config(ds_uuid) or {}
                base_rows = cfg.get("part_rows")
                if base_rows:
                    kw["chunk_rows"] = int(base_rows)
            mgr.add(
                dataset_name=dataset_name,
                subset_keys=subset_keys,
                field_data_dict=field_data_dict,
                is_group=is_group,
                force_flush=False,
                **kw,
            )
    if crash_safe and stager is not None:
        cfg = mgr.manifest.get_part_config(ds_uuid) or {}
        part_rows = int(cfg.get("part_rows", 100_000))
        stager.reclaim_stale(stale_after_seconds=stale_claim_seconds)
        while True:
            hot = stager.hot_subsets(limit=1024)
            if not hot:
                break
            for su in hot:
                while True:
                    token = str(uuid.uuid4())
                    claimed = stager.select_and_claim_prefix(su, part_rows, token)
                    if not claimed:
                        break
                    try:
                        bufs: Dict[str, List[np.ndarray]] = {}
                        merged_subset_keys = None
                        for r2 in claimed:
                            blob = r2["payload"]
                            if not (isinstance(blob, (bytes, bytearray)) and blob.startswith(b"DMST\x01")):
                                raise ValueError("bad staging payload magic/version")
                            sk2, fd2, _g2 = pickle.loads(blob[len(b"DMST\x01"):])
                            merged_subset_keys = sk2
                            for k3, v3 in fd2.items():
                                bufs.setdefault(k3, []).append(np.atleast_1d(np.asarray(v3)))
                        merged_fields = {
                            k3: (a3[0] if len(a3) == 1 else np.concatenate(a3, axis=0))
                            for k3, a3 in bufs.items()
                        }
                        kw = dict(add_kwargs or {})
                        n_rows = next(iter(merged_fields.values())).shape[0]
                        kw["chunk_rows"] = int(n_rows)
                        mgr.add(
                            dataset_name=dataset_name,
                            subset_keys=merged_subset_keys,
                            field_data_dict=merged_fields,
                            is_group=True,
                            force_flush=True,
                            **kw,
                        )
                    except Exception:
                        stager.unclaim(token)
                        raise
                    else:
                        stager.delete_claimed(token)
        if checkpoint_after_compact:
            stager.checkpoint()
    mgr.flush()
    return {"tasks": len(args_list), "errors": 0, "writers": 1}

def ingest(
    db_root: str,
    dataset_name: str,
    worker: Callable[..., Payload],
    worker_args: Optional[Sequence[tuple]] = None,
    worker_kwargs: Optional[Sequence[dict]] = None,
    *,
    manager_kwargs: Optional[Dict[str, Any]] = None,
    add_kwargs: Optional[Dict[str, Any]] = None,
    desired_part_rows: Optional[int] = None,
    desired_compression: Optional[str] = None,
    desired_compression_opts: Optional[int] = None,
    crash_safe: bool = False,
    durable_staging: bool = False,
    stale_claim_seconds: int = DEFAULT_STALE_CLAIM_SECONDS,
    checkpoint_after_compact: bool = False,
    n_jobs_compute: int = -1,
    compute_backend: str = "loky",
    compute_chunk_size: int = 256,
    n_writers: int = 4,
    writer_ctx: str = "spawn",
    writer_queue_maxsize: int = 1024,
    desc: str = "Ingest",
    raise_on_error: bool = True,
) -> Dict[str, int]:
    if int(n_jobs_compute) == 1:
        return ingest_serial(
            db_root=db_root,
            dataset_name=dataset_name,
            worker=worker,
            worker_args=worker_args,
            worker_kwargs=worker_kwargs,
            manager_kwargs=manager_kwargs,
            add_kwargs=add_kwargs,
            desired_part_rows=desired_part_rows,
            desired_compression=desired_compression,
            desired_compression_opts=desired_compression_opts,
            crash_safe=crash_safe,
            durable_staging=durable_staging,
            stale_claim_seconds=stale_claim_seconds,
            checkpoint_after_compact=checkpoint_after_compact,
        )
    else:
        return ingest_with_subset_affinity(
            db_root=db_root,
            dataset_name=dataset_name,
            worker=worker,
            worker_args=worker_args,
            worker_kwargs=worker_kwargs,
            n_jobs_compute=n_jobs_compute,
            compute_backend=compute_backend,
            compute_chunk_size=compute_chunk_size,
            n_writers=n_writers,
            writer_ctx=writer_ctx,
            writer_queue_maxsize=writer_queue_maxsize,
            manager_kwargs=manager_kwargs,
            add_kwargs=add_kwargs,
            desired_part_rows=desired_part_rows,
            desired_compression=desired_compression,
            desired_compression_opts=desired_compression_opts,
            crash_safe=crash_safe,
            durable_staging=durable_staging,
            stale_claim_seconds=stale_claim_seconds,
            checkpoint_after_compact=checkpoint_after_compact,
            desc=desc,
            raise_on_error=raise_on_error,
        )

