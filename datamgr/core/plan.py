# datamgr/core/plan.py
from __future__ import annotations
"""
Planner IR and interfaces: predicate rewrite → part pruning → costing → ordering.
SPEC refs: §3 PlanIR; §12 Planner v0; §11 stats exposure.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union
from enum import Enum

from .errors import (
    PlannerError,
)

__all__ = [
    # enums / small types
    "OrderPolicy",
    "PruneReason",
    # meta rows
    "PartMetaRow",
    # summaries
    "RewriteSummary",
    "PruneSummary",
    "CostSummary",
    # hints / selections / IR
    "PartHint",
    "PartSelection",
    "PlanIR",
    # abstract interfaces (skeletons)
    "rewrite_predicates",
    "prune_parts",
    "estimate_costs",
    "order_parts",
    "build_plan",
]


# ---------------------------------------------------------------------------
# Enums & small types
# ---------------------------------------------------------------------------

class OrderPolicy(str, Enum):
    """Ordering strategies for part execution."""
    AUTO = "auto"                  # heuristic (default)
    ROWS_ASC = "rows_asc"          # smallest parts first
    BYTES_ASC = "bytes_asc"        # when byte estimates exist
    SELECTIVITY_FIRST = "selectivity_first"  # most promising first
    TIME_ASC = "time_asc"          # older parts first


class PruneReason(str, Enum):
    """Why a part was pruned."""
    OUT_OF_RANGE = "out_of_range"
    EMPTY_JAGGED = "empty_jagged"
    STATS_ELIMINATION = "stats_elimination"
    DUPLICATE_CONTENT = "duplicate_content"
    OTHER = "other"


# ---------------------------------------------------------------------------
# Part metadata row (planner input)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PartMetaRow:
    """
    Minimal metadata needed by the planner. All fields are optional in v0
    except identifiers and n_rows; callers can leave bytes_* unknown.
    """
    dataset_uuid: str
    subset_uuid: str
    part_uuid: str
    created_at_epoch: int
    n_rows: int
    part_stats_json: Optional[str] = None
    bytes_plain: Optional[int] = None
    bytes_compressed: Optional[int] = None


# ---------------------------------------------------------------------------
# Summaries produced by planner stages
# ---------------------------------------------------------------------------

@dataclass
class RewriteSummary:
    """
    Result of predicate rewrite/pushdown.
      - pushed_meta: derived jagged meta predicates (e.g., seq_len >= 1)
      - notes: free-form human-readable notes for debugging/TUI
    """
    pushed_meta: Dict[str, Any] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)


@dataclass
class PruneSummary:
    """
    Part pruning decision set.
      - kept: list of part_uuids to keep
      - pruned: mapping part_uuid -> reason
    """
    kept: List[str] = field(default_factory=list)
    pruned: Dict[str, PruneReason] = field(default_factory=dict)


@dataclass
class CostSummary:
    """
    Per-part cost estimates and optional selectivity scores.
      - cost: mapping part_uuid -> float cost (lower is cheaper)
      - selectivity: mapping part_uuid -> estimated fraction in (0,1]
      - notes: human-readable cost model hints
    """
    cost: Dict[str, float] = field(default_factory=dict)
    selectivity: Dict[str, float] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Hints, selections, and final IR
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PartHint:
    """Optional ordering/cost hint attached to a part selection."""
    cost: Optional[float] = None
    selectivity: Optional[float] = None


@dataclass(frozen=True)
class PartSelection:
    """Chosen part with optional hints."""
    subset_uuid: str
    part_uuid: str
    n_rows: int
    hint: PartHint = PartHint()


@dataclass
class PlanIR:
    """
    Planner output. Read-only execution layer consumes this to load parts.
      - selections: ordered list of parts to read
      - rewrite: predicate pushdown summary
      - pruning: pruning decisions for audit/TUI
      - costs: cost model results
      - policy: ordering policy used
    """
    selections: List[PartSelection] = field(default_factory=list)
    rewrite: RewriteSummary = field(default_factory=RewriteSummary)
    pruning: PruneSummary = field(default_factory=PruneSummary)
    costs: CostSummary = field(default_factory=CostSummary)
    policy: OrderPolicy = OrderPolicy.AUTO


# ---------------------------------------------------------------------------
# Abstract interfaces (skeleton functions; implementations live in services)
# ---------------------------------------------------------------------------

def rewrite_predicates(
    predicates: Mapping[str, Any],
    *,
    jagged_schema: Optional[Mapping[str, Dict[str, Any]]] = None,
) -> RewriteSummary:
    """
    Rewrite high-level predicates to meta pushdowns.
    Examples:
      - exists(seq)          -> seq_len > 0
      - length(seq) >= k     -> seq_len >= k
      - shape(patch)[0] >= k -> patch_shape[:,0] >= k
    """
    raise NotImplementedError


def prune_parts(
    parts: Sequence[PartMetaRow],
    rewrite: RewriteSummary,
    *,
    subset_filters: Optional[Mapping[str, Any]] = None,
    time_range: Optional[Tuple[int, int]] = None,
) -> PruneSummary:
    """
    Decide which parts to keep using stats + rewritten predicates.
    time_range uses created_at_epoch (inclusive bounds expected).
    """
    raise NotImplementedError


def estimate_costs(
    parts: Sequence[PartMetaRow],
    rewrite: RewriteSummary,
) -> CostSummary:
    """
    Produce simple byte/row-based costs and (optional) selectivity hints.
    Minimal v0 may default cost ~ bytes_compressed or n_rows.
    """
    raise NotImplementedError


def order_parts(
    kept_parts: Sequence[PartMetaRow],
    costs: CostSummary,
    *,
    policy: OrderPolicy = OrderPolicy.AUTO,
) -> List[PartSelection]:
    """
    Turn kept parts + costs into an ordered selection list.
    """
    raise NotImplementedError


def build_plan(
    parts: Sequence[PartMetaRow],
    *,
    predicates: Mapping[str, Any],
    subset_filters: Optional[Mapping[str, Any]] = None,
    time_range: Optional[Tuple[int, int]] = None,
    policy: OrderPolicy = OrderPolicy.AUTO,
    jagged_schema: Optional[Mapping[str, Dict[str, Any]]] = None,
) -> PlanIR:
    """
    Convenience one-shot: rewrite → prune → cost → order → PlanIR.
    Service layer can replace this with a stateful PlannerService.
    """
    raise NotImplementedError
