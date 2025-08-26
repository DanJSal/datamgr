# datamgr/core/schema.py
from __future__ import annotations

"""
Schema & canonical dtype / jagged specs (SPEC §3, §8, §9, §10, §16).
Focus: types, signatures, and minimal helpers; behavior filled in later.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple, Union, Literal

import json
import hashlib
import re
import numpy as np

from .errors import (
    DTypeMismatchError,
    CanonicalNotLockedError,
    JaggedSpecError,
    DataExceedsCanonicalError,
    FieldNameError,
    KeySchemaError,
    UnicodeWideningRequired,
)

__all__ = [
    # constants / aliases
    "SQLType",
    "SPECIALS_NORMAL",
    "SPECIALS_NAN",
    "SPECIALS_PINF",
    "SPECIALS_NINF",
    "DEFAULT_MAX_UNICODE",
    # field & jagged specs
    "FieldSpec",
    "JaggedSpec",
    "CanonicalSpec",
    # schema JSON model
    "SchemaModel",
    # helpers
    "assert_safe_field_name",
    "dtype_to_canonical_json",
    "dtype_from_canonical_json",
    "schema_fingerprint",
    "identity_column_names",
    "normalize_key_schema",
]

# ---------------------------------------------------------------------------
# Constants & simple aliases
# ---------------------------------------------------------------------------

SQLType = Literal["INTEGER", "REAL", "BOOLEAN", "TEXT"]

SPECIALS_NORMAL = 0  # finite
SPECIALS_NAN    = 1
SPECIALS_PINF   = 2
SPECIALS_NINF   = 3

DEFAULT_MAX_UNICODE = 256

_SAFE_STR_RE = re.compile(r"^[A-Za-z0-9_]+$")


def assert_safe_field_name(name: str) -> None:
    """Raise FieldNameError if *name* is not A–Z/a–z/0–9/_."""
    if not name or not isinstance(name, str) or not _SAFE_STR_RE.match(name):
        raise FieldNameError(f"Invalid field name: {name!r} (only A–Z, a–z, 0–9, and _ allowed)")


# ---------------------------------------------------------------------------
# Field & Jagged specs
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FieldSpec:
    """
    Canonical field spec.

    Attributes:
        name: Field name.
        base: Base numpy dtype (no nested subarrays beyond one level).
        shape: Fixed outer shape for this field (empty tuple for scalars).
        max_unicode: Optional cap for Unicode width (U{N}); None for non-text.
    """
    name: str
    base: np.dtype
    shape: Tuple[int, ...] = field(default_factory=tuple)
    max_unicode: Optional[int] = None

    def as_numpy_dtype(self) -> np.dtype:
        """Return numpy dtype for this field (applies shape if present)."""
        dt = np.dtype(self.base)
        return np.dtype((dt, self.shape)) if self.shape else dt

    @property
    def is_text(self) -> bool:
        """True if the base dtype is Unicode."""
        return np.dtype(self.base).kind == "U"

    @property
    def is_numeric(self) -> bool:
        """True if numeric/bool."""
        k = np.dtype(self.base).kind
        return k in ("i", "u", "f", "b")


@dataclass(frozen=True)
class JaggedSpec:
    """
    Jagged (variable-length) metadata (SPEC §10).

    Attributes:
        vary_dims: Map field -> tuple of dimension indices that vary per row.
                   Example: {"seq": (0,), "patch": (0, 1)}
    """
    vary_dims: Mapping[str, Tuple[int, ...]] = field(default_factory=dict)

    def meta_names_for(self, field: str) -> List[str]:
        """
        Return meta column names for *field*:
        - 1D varying -> [f"{field}_len"]
        - kD varying -> [f"{field}_shape"]
        """
        dims = self.vary_dims.get(field, ())
        if not dims:
            return []
        return [f"{field}_len"] if len(dims) == 1 else [f"{field}_shape"]

    def validate_against_dtype(self, dtype: np.dtype) -> None:
        """
        Validate jagged spec against provided dtype; raise JaggedSpecError on mismatch.
        (Lightweight in v0; deep checks added later.)
        """
        # Placeholder: we’ll cross-check field existence and dims later.
        for fname in self.vary_dims.keys():
            if fname not in dtype.names:
                raise JaggedSpecError(f"Jagged field {fname!r} not found in dtype fields.")


@dataclass
class CanonicalSpec:
    """
    Canonical dtype + jagged configuration (locked once established).

    Attributes:
        dtype: Structured numpy dtype (None until locked).
        jagged: JaggedSpec (fields with varying dims).
        max_unicode_default: Default Unicode width if none provided.
    """
    dtype: Optional[np.dtype] = None
    jagged: JaggedSpec = field(default_factory=JaggedSpec)
    max_unicode_default: int = DEFAULT_MAX_UNICODE

    # --- Locking & compatibility ---

    def lock_from_first_batch(self, incoming: np.dtype) -> np.dtype:
        """
        Lock canonical dtype based on the first observed batch.
        May widen Unicode fields up to max_unicode_default.
        """
        if self.dtype is not None:
            return self.dtype
        canon = normalize_structured_dtype(incoming, default_u=self.max_unicode_default)
        self.dtype = canon
        return canon

    def ensure_compatible(self, incoming: np.dtype) -> np.dtype:
        """
        Ensure *incoming* can be safely cast to canonical dtype; may raise DTypeMismatchError
        or UnicodeWideningRequired.
        """
        if self.dtype is None:
            raise CanonicalNotLockedError("Canonical dtype not locked.")
        canon = self.dtype
        if not can_cast_structured(incoming, canon):
            # Future: detect text widening specifically and raise UnicodeWideningRequired
            raise DTypeMismatchError(f"incoming={incoming} cannot be safely cast to canonical={canon}")
        return canon


# ---------------------------------------------------------------------------
# Schema JSON model (persisted in catalog DB)
# ---------------------------------------------------------------------------

@dataclass
class SchemaModel:
    """
    In-memory representation of the dataset schema JSON blob (SPEC §8, §9, §24).

    Fields mirror the on-disk JSON layout. Methods are helpers—no I/O here.
    """
    key_schema: Dict[str, SQLType] = field(default_factory=dict)
    key_order: List[str] = field(default_factory=list)
    dtype_descr: str = ""  # canonical dtype (json form)
    part_config: Dict[str, Any] = field(default_factory=dict)
    quantization: Dict[str, float] = field(default_factory=dict)
    jagged: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    encryption: Dict[str, Any] = field(default_factory=lambda: {
        "mode": "none",
        "algorithm": "AES-256-GCM",
        "kms_provider": None,
        "key_policy": {"default_key_ref": None, "rotation_days": 180},
    })

    # --- Construction / (de)serialization ---

    def to_json_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable dict."""
        return {
            "key_schema": dict(self.key_schema),
            "key_order": list(self.key_order),
            "dtype_descr": self.dtype_descr,
            "part_config": dict(self.part_config),
            "quantization": dict(self.quantization),
            "jagged": dict(self.jagged),
            "encryption": dict(self.encryption),
        }

    @classmethod
    def from_json_dict(cls, d: Mapping[str, Any]) -> "SchemaModel":
        """Build from a parsed JSON mapping."""
        return cls(
            key_schema=dict(d.get("key_schema") or {}),
            key_order=list(d.get("key_order") or []),
            dtype_descr=str(d.get("dtype_descr") or ""),
            part_config=dict(d.get("part_config") or {}),
            quantization=dict(d.get("quantization") or {}),
            jagged=dict(d.get("jagged") or {}),
            encryption=dict(d.get("encryption") or {}),
        )

    # --- Canonical dtype helpers ---

    def has_canonical_dtype(self) -> bool:
        """True if dtype_descr is non-empty."""
        return bool(self.dtype_descr)

    def canonical_dtype(self) -> Optional[np.dtype]:
        """Return canonical dtype (or None if not locked)."""
        return dtype_from_canonical_json(self.dtype_descr) if self.dtype_descr else None

    def set_canonical_dtype(self, dt: np.dtype) -> None:
        """Serialize and store canonical dtype."""
        self.dtype_descr = dtype_to_canonical_json(dt)

    # --- Validation & identity helpers ---

    def validate_keys(self) -> None:
        """Validate key_schema and key_order (names, types, order agreement)."""
        if not self.key_schema or not self.key_order:
            raise KeySchemaError("key_schema and key_order must be set.")
        if set(self.key_schema.keys()) != set(self.key_order):
            raise KeySchemaError("key_order must list exactly the keys in key_schema.")
        for k in self.key_order:
            assert_safe_field_name(k)
            t = (self.key_schema.get(k) or "").upper()
            if t not in ("INTEGER", "REAL", "BOOLEAN", "TEXT"):
                raise KeySchemaError(f"Unsupported SQL type for key {k!r}: {t!r}")

    def identity_columns(self) -> List[str]:
        """
        Expanded identity column names for UNIQUE index (SPEC §8, §9):
        - REAL keys contribute two columns: <k>_s, <k>_q
        - non-REAL keys contribute raw column name k
        """
        self.validate_keys()
        return identity_column_names(self.key_schema, self.key_order)

    # --- Quantization helpers ---

    def require_quantization(self, key: str) -> float:
        """
        Return quantization scale for REAL key or raise KeySchemaError.
        (Defaults applied elsewhere; here we enforce presence.)
        """
        t = (self.key_schema.get(key) or "").upper()
        if t != "REAL":
            raise KeySchemaError(f"Quantization requested for non-REAL key {key!r}.")
        try:
            return float(self.quantization[key])
        except Exception as e:
            raise KeySchemaError(f"Missing quantization for REAL key {key!r}.") from e

    # --- Fingerprint ---

    def fingerprint(self) -> str:
        """
        Return a short, deterministic schema fingerprint (blake2b-16 hex) over a
        normalized JSON representation (sorted keys, no whitespace).
        """
        payload = json.dumps(self.to_json_dict(), sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.blake2b(payload, digest_size=16).hexdigest()


# ---------------------------------------------------------------------------
# dtype helpers (canonical JSON form)
# ---------------------------------------------------------------------------

def dtype_to_canonical_json(dt: np.dtype) -> str:
    """
    Serialize a structured dtype into a canonical JSON string with fields of the form:
    [{"name": "...", "base": "<dtype.str>", "shape": [..]}, ...]
    """
    if dt.fields is None:
        raise DTypeMismatchError("Structured dtype required.")
    items: List[Dict[str, Any]] = []
    for name in dt.names or ():
        fdt = dt.fields[name][0]
        if fdt.subdtype:
            base, shape = fdt.subdtype
        else:
            base, shape = fdt, ()
        items.append({"name": name, "base": base.str, "shape": list(shape)})
    return json.dumps(items, separators=(",", ":"), sort_keys=False)


def dtype_from_canonical_json(js: str) -> np.dtype:
    """Inverse of dtype_to_canonical_json."""
    if not js:
        return None  # type: ignore[return-value]
    items = json.loads(js)
    out = []
    for it in items:
        base = np.dtype(it["base"])
        shape = tuple(int(x) for x in it.get("shape", []))
        out.append((it["name"], base) if not shape else (it["name"], base, shape))
    return np.dtype(out)


# ---------------------------------------------------------------------------
# Key schema helpers
# ---------------------------------------------------------------------------

def normalize_key_schema(key_schema: Mapping[str, str]) -> Dict[str, SQLType]:
    """Uppercase SQL types and validate names."""
    out: Dict[str, SQLType] = {}
    for k, v in key_schema.items():
        assert_safe_field_name(k)
        t = (v or "").upper()
        if t not in ("INTEGER", "REAL", "BOOLEAN", "TEXT"):
            raise KeySchemaError(f"Unsupported SQL type for key {k!r}: {v!r}")
        out[k] = t  # type: ignore[assignment]
    return out


def identity_column_names(key_schema: Mapping[str, str], key_order: Sequence[str]) -> List[str]:
    """
    Return ordered list of identity columns for UNIQUE index:
    REAL -> k_s, k_q ; others -> k
    """
    cols: List[str] = []
    for k in key_order:
        t = (key_schema.get(k) or "").upper()
        if t == "REAL":
            cols.extend([f"{k}_s", f"{k}_q"])
        else:
            cols.append(k)
    return cols


# ---------------------------------------------------------------------------
# Internal dtype normalization utilities (minimal v0 behavior)
# ---------------------------------------------------------------------------

def normalize_structured_dtype(dt: np.dtype, *, default_u: int = DEFAULT_MAX_UNICODE) -> np.dtype:
    """
    Return a copy of *dt* normalized for canonical storage:
    - ints -> int64, floats -> float64, bool -> bool
    - Unicode -> ensure width <= default_u
    """
    if dt.fields is None:
        raise DTypeMismatchError("Structured dtype required.")
    new_fields = []
    for name in dt.names or ():
        fdt = dt.fields[name][0]
        if fdt.subdtype:
            base, shape = fdt.subdtype
        else:
            base, shape = fdt, ()
        if base.kind == "U":
            ulen = base.itemsize // 4
            width = min(max(ulen, 1), default_u)
            base = np.dtype(f"<U{width}")
        elif np.issubdtype(base, np.bool_):
            base = np.dtype(np.bool_)
        elif np.issubdtype(base, np.integer):
            base = np.dtype(np.int64)
        elif np.issubdtype(base, np.floating):
            base = np.dtype(np.float64)
        new_fields.append((name, base) if not shape else (name, base, shape))
    return np.dtype(new_fields)


def can_cast_structured(src: np.dtype, dst: np.dtype) -> bool:
    """
    Return True if *src* can be safely cast to *dst* field-by-field.
    (v0: simple kind/shape check; detailed safety rules later.)
    """
    if src.fields is None or dst.fields is None:
        return False
    if src.names != dst.names:
        return False
    for name in src.names or ():
        sdt = src.fields[name][0]
        ddt = dst.fields[name][0]
        if bool(sdt.subdtype) != bool(ddt.subdtype):
            return False
        if sdt.subdtype:
            sb, sshape = sdt.subdtype
            db, dshape = ddt.subdtype
            if sshape != dshape:
                return False
            if not _can_cast_base(sb, db):
                return False
        else:
            if not _can_cast_base(sdt, ddt):
                return False
    return True


def _can_cast_base(sb: np.dtype, db: np.dtype) -> bool:
    """Helper for can_cast_structured (v0 rules)."""
    if sb.kind == "U" and db.kind == "U":
        return sb.itemsize <= db.itemsize
    if np.issubdtype(sb, np.bool_) and np.issubdtype(db, np.bool_):
        return True
    if np.issubdtype(sb, np.integer) and np.issubdtype(db, np.integer):
        return np.dtype(np.int64) == db
    if np.issubdtype(sb, np.floating) and np.issubdtype(db, np.floating):
        return np.dtype(np.float64) == db
    # disallow other casts for now
    return sb == db


# ---------------------------------------------------------------------------
# Fingerprint helper
# ---------------------------------------------------------------------------

def schema_fingerprint(schema_json: Mapping[str, Any]) -> str:
    """
    Return blake2b-16 hex fingerprint for a schema JSON mapping with stable sorting.
    """
    payload = json.dumps(schema_json, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.blake2b(payload, digest_size=16).hexdigest()
