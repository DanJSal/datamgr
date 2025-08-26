# datamgr/core/__init__.py
"""
Core (pure logic) subpackage: schema, keys, jagged, hashing, planning, and errors.
Minimal re-exports so adapters/services can import from `datamgr.core` directly.
"""

# Errors
from .errors import (
    DatamgrError,
    KeySchemaError,
    IdentityConflictError,
    InvalidKeyValueError,
    QuantizationMissingError,
    SpecialsCodeError,
    DeterministicUUIDError,
    DTypeMismatchError,
    JaggedSpecError,
    DataExceedsCanonicalError,
    PlannerError,
)

# Schema
from .schema import (
    SQLType,
    SchemaModel,
    CanonicalSpec,
    JaggedSpec,
    SPECIALS_NORMAL,
    SPECIALS_NAN,
    SPECIALS_PINF,
    SPECIALS_NINF,
)

# Keys / identity
from .keys import (
    KeyNormalizer,
    KeyIdentity,
    classify_specials,
    quantize_value,
    stable_subset_key,
    deterministic_uuid_from_identity,
)

# Jagged
from .jagged import (
    JaggedNormalizer,
    PaddingPolicy,
    PaddingPlan,
    PaddingResult,
    FieldJaggedInfo,
    infer_max_shape,
    pick_meta_dtype,
    meta_field_names,
)

# Hashing & stats
from .hashing import (
    ContentHasher,
    PartStats,
    schema_signature_for_hash,
    update_hasher_from_structured,
    update_hasher_from_meta,
    quantization_digest,
    build_aad,
)

# Planner IR
from .plan import (
    PlanIR,
    PartSelection,
    PartMetaRow,
    OrderPolicy,
    PruneReason,
    RewriteSummary,
    PruneSummary,
    CostSummary,
)

__all__ = [
    # errors
    "DatamgrError","KeySchemaError","IdentityConflictError","InvalidKeyValueError",
    "QuantizationMissingError","SpecialsCodeError","DeterministicUUIDError",
    "DTypeMismatchError","JaggedSpecError","DataExceedsCanonicalError","PlannerError",
    # schema
    "SQLType","SchemaModel","CanonicalSpec","JaggedSpec",
    "SPECIALS_NORMAL","SPECIALS_NAN","SPECIALS_PINF","SPECIALS_NINF",
    # keys
    "KeyNormalizer","KeyIdentity","classify_specials","quantize_value",
    "stable_subset_key","deterministic_uuid_from_identity",
    # jagged
    "JaggedNormalizer","PaddingPolicy","PaddingPlan","PaddingResult",
    "FieldJaggedInfo","infer_max_shape","pick_meta_dtype","meta_field_names",
    # hashing
    "ContentHasher","PartStats","schema_signature_for_hash",
    "update_hasher_from_structured","update_hasher_from_meta",
    "quantization_digest","build_aad",
    # planner
    "PlanIR","PartSelection","PartMetaRow","OrderPolicy","PruneReason",
    "RewriteSummary","PruneSummary","CostSummary",
]
