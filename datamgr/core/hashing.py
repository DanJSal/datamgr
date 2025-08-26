# datamgr/core/hashing.py
from __future__ import annotations
"""
Content hashing (padded data then jagged meta) and per-part statistics.

SPEC refs:
- §3  ContentHasher, PartStats
- §10 Jagged meta arrays
- §11 Sealing (hash order), AAD contract for encryption hooks
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union
import hashlib
import json
import unicodedata
import numpy as np

from .errors import (
    DTypeMismatchError,
)

__all__ = [
    "schema_signature_for_hash",
    "hash_utf8_lenpref_iter",
    "update_hasher_from_structured",
    "update_hasher_from_meta",
    "quantization_digest",
    "ContentHasher",
    "Jagged1DStats",
    "JaggedKDStats",
    "PartStats",
    "compute_part_stats",
    "build_aad",
]

# ---------------------------------------------------------------------------
# Stateless helpers (signatures OK; v0 behavior may be refined later)
# ---------------------------------------------------------------------------

def schema_signature_for_hash(dt: np.dtype) -> bytes:
    """
    Stable, minimal schema signature for hashing structured arrays.
    Encodes (name, base dtype tag, outer shape) for each field in order.
    """
    if dt.fields is None:
        raise DTypeMismatchError("Structured numpy dtype required.")
    items = []
    for name in dt.names:
        fdt = dt.fields[name][0]
        base, shape = (fdt.subdtype if fdt.subdtype else (fdt, ()))
        base_tag = "U" if base.kind == "U" else base.str
        items.append((name, base_tag, tuple(shape)))
    return json.dumps(items, separators=(",", ":"), sort_keys=True).encode("utf-8")


def hash_utf8_lenpref_iter(hasher: "hashlib._Hash", scalars: Iterable[str]) -> None:
    """
    Update *hasher* with a length-prefixed NFC-UTF8 encoding of each string in *scalars*.
    """
    for s in scalars:
        if not isinstance(s, str):
            s = str(s)
        b = unicodedata.normalize("NFC", s).encode("utf-8")
        hasher.update(len(b).to_bytes(4, "little"))
        hasher.update(b)


def update_hasher_from_structured(
    hasher: "hashlib._Hash",
    arr: np.ndarray,
    *,
    max_chunk_bytes: int = 16 * 1024 * 1024,
) -> None:
    """
    Update *hasher* with the bytes of a structured ndarray in a stable order:

    1) schema_signature_for_hash(arr.dtype)
    2) For each chunk of rows, for each field:
         - Unicode: NFC-UTF8 length-prefixed elements
         - Other: raw contiguous bytes
    """
    if arr.dtype.fields is None:
        raise DTypeMismatchError("Structured numpy dtype required.")
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


def update_hasher_from_meta(
    hasher: "hashlib._Hash",
    meta: Mapping[str, np.ndarray],
) -> None:
    """
    Update *hasher* with jagged meta arrays (e.g., *_len, *_shape) in sorted key order.
    Uses raw contiguous bytes for numeric meta.
    """
    for k in sorted(meta.keys()):
        v = np.ascontiguousarray(meta[k])
        hasher.update(k.encode("utf-8"))
        hasher.update(b"\x00")
        hasher.update(memoryview(v))


def quantization_digest(qmap: Mapping[str, float]) -> str:
    """
    Stable blake2b-16 hex digest of the quantization map (sorted by key).
    """
    payload = json.dumps({k: float(qmap[k]) for k in sorted(qmap)}, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.blake2b(payload, digest_size=16).hexdigest()


# ---------------------------------------------------------------------------
# Content hasher
# ---------------------------------------------------------------------------

class ContentHasher:
    """
    Compute content_hash for a part: blake2b(digest_size=16) over
    padded structured data bytes (field order) THEN jagged meta arrays.

    This class is stateless aside from parameters; safe to reuse.
    """

    def __init__(self, *, digest_size: int = 16):
        self.digest_size = int(digest_size)

    def new(self) -> "hashlib._Hash":
        """Return a new blake2b hasher with configured digest size."""
        return hashlib.blake2b(digest_size=self.digest_size)

    def hash_rows(
        self,
        rows: np.ndarray,
        *,
        meta: Optional[Mapping[str, np.ndarray]] = None,
        max_chunk_bytes: int = 16 * 1024 * 1024,
    ) -> str:
        """
        Compute hex content hash for *rows* (and optional *meta*).
        Hash order: data first, then meta (sorted keys).
        """
        h = self.new()
        update_hasher_from_structured(h, rows, max_chunk_bytes=max_chunk_bytes)
        if meta:
            update_hasher_from_meta(h, meta)
        return h.hexdigest()


# ---------------------------------------------------------------------------
# Per-part statistics (skeleton per SPEC §3 / §11)
# ---------------------------------------------------------------------------

@dataclass
class Jagged1DStats:
    """
    Stats for a 1-D jagged field (meta kind = len).
    """
    min_len: int
    max_len: int
    avg_len: float
    full_rows: int                      # rows where len == max_len
    encoding: Optional[str] = None      # {"constant","codebook","plain"} (optional)
    codebook_k: Optional[int] = None


@dataclass
class JaggedKDStats:
    """
    Stats for a k-D jagged field (meta kind = shape).
    Per-dimension aggregates over varying dims.
    """
    per_dim_min: Tuple[int, ...]
    per_dim_max: Tuple[int, ...]
    per_dim_avg: Tuple[float, ...]
    full_rows: int
    encoding: Optional[str] = None
    codebook_k: Optional[int] = None


@dataclass
class PartStats:
    """
    Per-part statistics, serialized later into parts.part_stats_json.
    """
    n_rows: int
    jagged: Dict[str, Union[Jagged1DStats, JaggedKDStats]] = field(default_factory=dict)
    bytes_plain: Optional[int] = None
    bytes_compressed: Optional[int] = None

    def to_json(self) -> str:
        """Serialize to JSON (simple dataclass → dict encoding)."""
        def encode(v: Any) -> Any:
            if isinstance(v, (Jagged1DStats, JaggedKDStats)):
                return v.__dict__
            return v
        obj = {
            "n_rows": self.n_rows,
            "jagged": {k: encode(v) for k, v in self.jagged.items()},
            "bytes_plain": self.bytes_plain,
            "bytes_compressed": self.bytes_compressed,
        }
        return json.dumps(obj, separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_json(cls, js: str) -> "PartStats":
        """Inverse of to_json; accepts the simple encoded form."""
        d = json.loads(js) if js else {}
        # Skeleton: do not reconstruct typed sub-objects yet
        return cls(
            n_rows=int(d.get("n_rows", 0)),
            jagged=dict(d.get("jagged") or {}),
            bytes_plain=d.get("bytes_plain"),
            bytes_compressed=d.get("bytes_compressed"),
        )


def compute_part_stats(
    rows: np.ndarray,
    *,
    meta: Optional[Mapping[str, np.ndarray]] = None,
    jagged_fields: Optional[Mapping[str, str]] = None,  # field -> "len"|"shape"
) -> PartStats:
    """
    Compute PartStats for a sealed part. Minimal skeleton for now.
    Implementations should:
      - n_rows from rows.shape[0]
      - For each jagged field:
          * if "len": derive 1-D stats from <field>_len
          * if "shape": derive per-dim stats from <field>_shape
      - Optionally set bytes_plain/bytes_compressed if available
    """
    raise NotImplementedError


# ---------------------------------------------------------------------------
# Encryption-ready AAD builder (skeleton) — SPEC §11 / §24
# ---------------------------------------------------------------------------

def build_aad(
    *,
    dataset_uuid: str,
    subset_uuid: str,
    part_uuid: str,
    schema_fingerprint: str,
    storage_scheme_version: int,
    quantization_digest_hex: str,
    content_hash_hex: str,
) -> bytes:
    """
    Construct Additional Authenticated Data (AAD) for future AEAD encryption.

    AAD layout (stringified JSON, stable ordering):
      {
        "ds": dataset_uuid,
        "su": subset_uuid,
        "pu": part_uuid,
        "sf": schema_fingerprint,
        "sv": storage_scheme_version,
        "qd": quantization_digest_hex,
        "ch": content_hash_hex
      }
    """
    obj = {
        "ds": dataset_uuid,
        "su": subset_uuid,
        "pu": part_uuid,
        "sf": schema_fingerprint,
        "sv": int(storage_scheme_version),
        "qd": quantization_digest_hex,
        "ch": content_hash_hex,
    }
    return json.dumps(obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
