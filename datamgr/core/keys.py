# datamgr/core/keys.py
from __future__ import annotations
"""
Key identity, quantization, specials codes, and deterministic subset UUIDs.
SPEC refs: §3 (KeyNormalizer), §8 (Schema & Identity), §9 (DDL identity columns).
"""

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple, Union, overload
import hashlib
import json
import uuid
import math
import numpy as np

from .errors import (
    IdentityConflictError,
    InvalidKeyValueError,
    QuantizationMissingError,
    SpecialsCodeError,
    DeterministicUUIDError,
    KeySchemaError,
)
from .schema import (
    SQLType,
    SPECIALS_NORMAL,
    SPECIALS_NAN,
    SPECIALS_PINF,
    SPECIALS_NINF,
    normalize_key_schema,
    identity_column_names,
)

__all__ = [
    "KeyIdentity",
    "classify_specials",
    "quantize_value",
    "stable_subset_key",
    "deterministic_uuid_from_identity",
    "KeyNormalizer",
]


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class KeyIdentity:
    """Container for computed identity tuple and UUID."""
    identity_tuple: Tuple[Any, ...]
    subset_uuid: str


# ---------------------------------------------------------------------------
# Stateless helpers
# ---------------------------------------------------------------------------

def classify_specials(x: Any) -> int:
    """
    Map value to specials code:
      0: Normal finite, 1: NaN, 2: +Inf, 3: -Inf
    Non-floats are treated as NORMAL.
    """
    try:
        xf = float(x)
    except Exception:
        return SPECIALS_NORMAL
    if math.isnan(xf):  # NaN
        return SPECIALS_NAN
    if math.isinf(xf):
        return SPECIALS_PINF if xf > 0 else SPECIALS_NINF
    return SPECIALS_NORMAL


def quantize_value(x: float, scale: float) -> int:
    """
    Quantize finite REAL value using round(x * scale).
    Caller must ensure value is SPECIALS_NORMAL.
    """
    # Minimal v0 implementation; rounding policy may evolve.
    return int(round(float(x) * float(scale)))


def stable_subset_key(
    subset_keys: Mapping[str, Any],
    *,
    decimals: int = 6,
) -> str:
    """
    Human-readable, stable key string (for logs/TUI); not used for identity.
    Floats are rounded to *decimals*. Preserves key ordering by name.
    """
    def norm(v: Any) -> Any:
        if isinstance(v, (np.bool_, bool)):
            return bool(v)
        if isinstance(v, (np.integer, int)):
            return int(v)
        if isinstance(v, (np.floating, float)):
            return round(float(v), decimals)
        return str(v) if isinstance(v, (bytes, bytearray)) else v
    cleaned = {k: norm(subset_keys[k]) for k in sorted(subset_keys)}
    return json.dumps(cleaned, sort_keys=True, separators=(",", ":"))


def deterministic_uuid_from_identity(identity: Iterable[Any]) -> str:
    """
    Derive UUID from identity tuple via blake2b(16) hex → UUID.
    """
    try:
        s = ",".join(str(x) for x in identity)
        h = hashlib.blake2b(s.encode("utf-8"), digest_size=16).hexdigest()
        return str(uuid.UUID(h))
    except Exception as e:
        raise DeterministicUUIDError("Failed to derive subset UUID from identity tuple.") from e


# ---------------------------------------------------------------------------
# Key normalizer
# ---------------------------------------------------------------------------

class KeyNormalizer:
    """
    Compute deterministic identity tuples and subset UUIDs from (key_schema, key_order, quantization).

    Identity tuple rules (SPEC §8):
      - For REAL keys: contribute (k_s, k_q) where k_s ∈ {0,1,2,3} and
        k_q = round(v * quantization[k]) only when k_s == 0 (finite).
      - For INTEGER/BOOLEAN/TEXT: contribute raw value.

    Notes:
      - This module does not perform I/O. It only validates and transforms values.
      - Equality over REAL keys should later use (k_s, k_q). Range predicates use raw REAL columns.
    """

    def __init__(
        self,
        key_schema: Mapping[str, SQLType],
        key_order: Sequence[str],
        quantization: Mapping[str, float],
        *,
        default_quantization: float = 1e3,  # SPEC §17 default
    ) -> None:
        self.key_schema: Dict[str, SQLType] = normalize_key_schema(key_schema)
        self.key_order: List[str] = list(key_order)
        # Validate key_order covers schema exactly
        if set(self.key_schema.keys()) != set(self.key_order):
            raise KeySchemaError("key_order must list exactly the keys in key_schema.")
        # Build effective quantization map for REAL keys
        self.quantization: Dict[str, float] = {}
        for k in self.key_order:
            t = self.key_schema[k]
            if t == "REAL":
                scale = quantization.get(k, default_quantization)
                try:
                    self.quantization[k] = float(scale)
                except Exception as e:
                    raise QuantizationMissingError(f"Quantization for REAL key {k!r} is missing/invalid.") from e

    # ---- public API ---------------------------------------------------------

    def identity_columns(self) -> List[str]:
        """Return expanded identity column names for UNIQUE index."""
        return identity_column_names(self.key_schema, self.key_order)

    def normalize_keys(self, subset_keys: Mapping[str, Any]) -> Dict[str, Any]:
        """
        Return a normalized copy of subset_keys with Python-native types suitable for hashing/SQL binding.
        Minimal v0 normalization; detailed coercions land later.
        """
        out: Dict[str, Any] = {}
        for k in self.key_order:
            if k not in subset_keys:
                raise InvalidKeyValueError(f"Missing key: {k!r}")
            v = subset_keys[k]
            t = self.key_schema[k]
            try:
                if t == "BOOLEAN":
                    out[k] = bool(v)
                elif t == "INTEGER":
                    out[k] = int(v)
                elif t == "REAL":
                    out[k] = float(v)
                elif t == "TEXT":
                    if isinstance(v, (bytes, bytearray)):
                        v = v.decode("utf-8", errors="strict")
                    out[k] = str(v)
                else:
                    raise KeySchemaError(f"Unsupported SQL type {t!r} for key {k!r}")
            except Exception as e:
                raise InvalidKeyValueError(f"Invalid value for key {k!r}: {v!r}") from e
        return out

    def identity_tuple(self, subset_keys: Mapping[str, Any]) -> Tuple[Any, ...]:
        """
        Compute ordered identity tuple from subset_keys per SPEC §8.
        """
        nk = self.normalize_keys(subset_keys)
        parts: List[Any] = []
        for k in self.key_order:
            t = self.key_schema[k]
            if t == "REAL":
                s = classify_specials(nk[k])
                if s == SPECIALS_NORMAL:
                    q = quantize_value(nk[k], self.quantization[k])
                    parts.extend([s, q])
                else:
                    parts.extend([s, 0])
            else:
                parts.append(nk[k])
        return tuple(parts)

    def subset_uuid(self, arg: Union[Mapping[str, Any], Tuple[Any, ...]]) -> str:
        """
        Return deterministic subset UUID from either subset_keys or a precomputed identity tuple.
        """
        if isinstance(arg, tuple):
            return deterministic_uuid_from_identity(arg)
        return deterministic_uuid_from_identity(self.identity_tuple(arg))

    def equality_predicates(self, subset_keys: Mapping[str, Any]) -> Dict[str, Any]:
        """
        Build an equality predicate dict over identity columns for SQL WHERE clauses.

        Example output (for keys: site TEXT, lat REAL):
            {
              "site": "A",
              "lat_s": 0,
              "lat_q": 1234567
            }

        Range predicates over REAL should be handled elsewhere (storage/planner).
        """
        nk = self.normalize_keys(subset_keys)
        preds: Dict[str, Any] = {}
        for k in self.key_order:
            t = self.key_schema[k]
            if t == "REAL":
                s = classify_specials(nk[k])
                preds[f"{k}_s"] = s
                preds[f"{k}_q"] = (quantize_value(nk[k], self.quantization[k]) if s == SPECIALS_NORMAL else 0)
            else:
                preds[k] = nk[k]
        return preds

    # ---- convenience --------------------------------------------------------

    def stable_key_string(self, subset_keys: Mapping[str, Any], *, decimals: int = 6) -> str:
        """Human-readable, stable key string (for logs/TUI)."""
        return stable_subset_key(subset_keys, decimals=decimals)
