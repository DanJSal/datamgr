# datamgr/core/jagged.py
from __future__ import annotations
"""
Jagged (variable-length) arrays: specs, padding plans, meta arrays, and helpers.
SPEC refs: §10 (Jaggedness), §11 (Ingest & Sealing: hashing includes meta).
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple, Union, Literal

import numpy as np

from .errors import (
    JaggedSpecError,
    DataExceedsCanonicalError,
)
from .schema import (
    JaggedSpec,                    # lightweight spec & meta-name helper
    CanonicalSpec,                 # holds canonical dtype + jagged config
    DEFAULT_MAX_UNICODE,
)

__all__ = [
    # policies / info
    "PaddingPolicy",
    "FieldJaggedInfo",
    "PaddingPlan",
    "PaddingResult",
    # planner / normalizer
    "JaggedNormalizer",
    # helpers
    "infer_max_shape",
    "pick_meta_dtype",
    "meta_field_names",
]

ArrayLike = Any
MetaKind = Literal["len", "shape"]


# ---------------------------------------------------------------------------
# Policies & data containers
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PaddingPolicy:
    """
    Values used when padding shorter rows to canonical shapes.
    """
    pad_numeric: float = 0.0
    pad_bool: bool = False
    pad_unicode: str = ""


@dataclass(frozen=True)
class FieldJaggedInfo:
    """
    Per-field jagged info resolved for a batch or canonical:
      - field: name of the data field
      - vary_dims: tuple of dimension indices that vary per row
      - max_shape: resolved maximum shape for the field (outer shape, incl. fixed dims)
      - meta_kind: "len" for 1-D varying, "shape" for k-D varying
    """
    field: str
    vary_dims: Tuple[int, ...]
    max_shape: Tuple[int, ...]
    meta_kind: MetaKind


@dataclass
class PaddingPlan:
    """
    Padding plan for a batch: how each jagged field should be padded and what meta to emit.
    Non-jagged fields are omitted from `fields`.
    """
    fields: Dict[str, FieldJaggedInfo] = field(default_factory=dict)

    def meta_names(self, field: str) -> List[str]:
        """Return the meta column names for a jagged field."""
        info = self.fields.get(field)
        if not info:
            return []
        return [f"{field}_len"] if info.meta_kind == "len" else [f"{field}_shape"]


@dataclass
class PaddingResult:
    """
    Output of padding a batch:
      - arrays: dict[field] -> padded ndarray (canonical outer shape)
      - meta:   dict[meta_field_name] -> ndarray of lengths/shapes per row
      - warnings: list of human-readable notes (e.g., padding applied)
    """
    arrays: Dict[str, np.ndarray]
    meta: Dict[str, np.ndarray]
    warnings: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers (signatures only for now)
# ---------------------------------------------------------------------------

def infer_max_shape(samples: Sequence[ArrayLike]) -> Tuple[int, ...]:
    """
    Infer the maximum outer shape across a sequence of row values for a field.
    Intended for first-batch locking when canonical not set yet.
    """
    raise NotImplementedError


def pick_meta_dtype(max_value: int, *, signed: bool = False) -> np.dtype:
    """
    Choose a compact integer dtype for meta arrays (lengths/shapes).
    E.g., max <= 65535 -> uint16; else uint32 (signed variants if requested).
    """
    raise NotImplementedError


def meta_field_names(field: str, vary_dims: Sequence[int]) -> List[str]:
    """
    Convenience wrapper to compute meta names from vary_dims.
    1-D varying -> [f"{field}_len"]; k-D -> [f"{field}_shape"].
    """
    if not vary_dims:
        return []
    return [f"{field}_len"] if len(tuple(vary_dims)) == 1 else [f"{field}_shape"]


# ---------------------------------------------------------------------------
# Planner / Normalizer
# ---------------------------------------------------------------------------

class JaggedNormalizer:
    """
    Pads jagged inputs to canonical shapes and emits per-row meta arrays.
    This class is pure logic (no I/O); sealing and hashing use its outputs.

    Usage pattern (later in ingest):
      1) If canonical not locked, analyze first batch to compute max shapes,
         then lock canonical dtype + jagged (via CanonicalSpec).
      2) For each batch: plan = analyze_batch(...); result = pad(...).

    Notes:
      - Padding values follow PaddingPolicy (SPEC §10).
      - Overflow (observed shape > canonical) must raise DataExceedsCanonicalError.
      - Result.meta dtypes should be compact (pick_meta_dtype).
    """

    def __init__(
        self,
        canonical: CanonicalSpec,
        *,
        policy: Optional[PaddingPolicy] = None,
    ) -> None:
        self.canonical = canonical
        self.policy = policy or PaddingPolicy()

    # --- analysis ---

    def analyze_batch(
        self,
        batch_fields: Mapping[str, ArrayLike],
        *,
        jagged_override: Optional[JaggedSpec] = None,
    ) -> PaddingPlan:
        """
        Inspect provided batch fields and return a PaddingPlan describing the
        required padding for jagged fields (use canonical.jagged unless overridden).
        """
        raise NotImplementedError

    # --- padding ---

    def pad(
        self,
        batch_fields: Mapping[str, ArrayLike],
        plan: PaddingPlan,
    ) -> PaddingResult:
        """
        Apply padding for fields described in *plan* and produce meta arrays.
        Non-jagged fields should be forwarded unchanged (cast to canonical by caller).
        """
        raise NotImplementedError

    # --- validation ---

    def validate_no_overflow(
        self,
        plan: PaddingPlan,
    ) -> None:
        """
        Ensure planned max_shape does not exceed canonical shapes (if locked).
        Raise DataExceedsCanonicalError on overflow.
        """
        raise NotImplementedError
