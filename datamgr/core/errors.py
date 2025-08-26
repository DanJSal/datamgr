# datamgr/core/errors.py
from __future__ import annotations

from typing import Any, Mapping, Optional


__all__ = [
    # Base
    "DatamgrError",
    "DatamgrWarning",
    "WithContext",
    # Config / DI
    "ConfigError",
    "InvalidConfigValueError",
    "DependencyInjectionError",
    # Schema / dtype / jagged
    "SchemaMismatchError",
    "DTypeMismatchError",
    "CanonicalNotLockedError",
    "JaggedSpecError",
    "DataExceedsCanonicalError",
    "PaddingOverflowError",
    "FieldNameError",
    "KeySchemaError",
    "UnicodeWideningRequired",
    # Identity / keys / quantization
    "IdentityConflictError",
    "InvalidKeyValueError",
    "QuantizationMissingError",
    "SpecialsCodeError",
    "DeterministicUUIDError",
    # Storage adapters (SQLite/HDF5)
    "CatalogError",
    "CatalogOpenError",
    "CatalogDDLApplyError",
    "CatalogQueryError",
    "CatalogIntegrityError",
    "PartStoreError",
    "H5WriteError",
    "AtomicReplaceError",
    "FsyncError",
    "ContentHashMismatchError",
    "PartAlreadyExistsError",
    "SQLiteLoaderError",
    # Services: ingest / planner / merge / migrate
    "IngestError",
    "BufferOverflowError",
    "FlushInProgressError",
    "PlannerError",
    "PredicateRewriteError",
    "MergeError",
    "MergeInvariantError",
    "MergePolicyError",
    "MergeDryRunOnlyError",
    "MigrationError",
    # Change feed / batches
    "ChangeFeedError",
    "BatchConflictError",
    "BatchMissingError",
    # Encryption hooks / policy
    "EncryptionNotSupportedError",
    "EncryptionPolicyMismatchError",
    "CryptoProviderError",
    "KeyManagerError",
    # Hardening / OS posture / tamper-evidence
    "HardeningError",
    "PosixPermissionError",
    "OwnershipError",
    "LockAcquisitionError",
    "ContainerRequirementError",
    "AuditLogError",
    "TamperEvidenceError",
    # Navigator / TUI
    "NavigatorError",
    "PeekLimitExceededError",
    "PaginationError",
    # Lookup / existence
    "NotFoundError",
    "DatasetNotFoundError",
    "SubsetNotFoundError",
    "PartNotFoundError",
    # Warnings
    "PaddingAppliedWarning",
    "UnicodeWideningWarning",
    "DeprecatedAPIWarning",
    "PerformanceWarning",
    "SecurityPostureWarning",
]


class WithContext:
    """
    Mixin to attach structured context to exceptions for diagnostics.
    """

    def __init__(self, *args: object, context: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> None:
        super().__init__(*args)  # type: ignore[misc]
        self.context: Mapping[str, Any] = dict(context or {})


# Base -------------------------------------------------------------------------

class DatamgrError(Exception, WithContext):
    """Base class for all datamgr errors."""


class DatamgrWarning(Warning):
    """Base class for datamgr warnings."""


# Config / DI ------------------------------------------------------------------

class ConfigError(DatamgrError):
    """Invalid or inconsistent configuration."""


class InvalidConfigValueError(ConfigError):
    """A configuration field has an invalid value."""


class DependencyInjectionError(ConfigError):
    """A required dependency was not provided or could not be constructed."""


# Schema / dtype / jagged ------------------------------------------------------

class SchemaMismatchError(DatamgrError):
    """Observed schema does not match the expected dataset schema."""


class DTypeMismatchError(SchemaMismatchError):
    """Incoming dtype cannot be safely cast to the canonical dtype."""


class CanonicalNotLockedError(SchemaMismatchError):
    """Canonical dtype/jagged spec is not locked when required."""


class JaggedSpecError(DatamgrError):
    """Invalid jagged specification or incompatible input meta."""


class DataExceedsCanonicalError(JaggedSpecError):
    """Observed row/shape exceeds locked canonical limits."""


class PaddingOverflowError(JaggedSpecError):
    """Padding would overflow the canonical bounds."""


class FieldNameError(DatamgrError):
    """Illegal or reserved field name encountered."""


class KeySchemaError(DatamgrError):
    """Key schema missing/invalid or key order mismatch."""


class UnicodeWideningRequired(SchemaMismatchError):
    """Incoming Unicode width exceeds canonical width; widening needed."""


# Identity / keys / quantization -----------------------------------------------

class IdentityConflictError(DatamgrError):
    """Conflicting subset identity detected (specials/quantized mismatch)."""


class InvalidKeyValueError(DatamgrError):
    """Subset key value is invalid for its declared SQL type."""


class QuantizationMissingError(DatamgrError):
    """Quantization scale required for REAL key is missing."""


class SpecialsCodeError(DatamgrError):
    """Invalid specials code (expected {0:Normal,1:NaN,2:+Inf,3:-Inf})."""


class DeterministicUUIDError(DatamgrError):
    """Failed to derive deterministic subset UUID from identity tuple."""


# Storage adapters (SQLite/HDF5) -----------------------------------------------

class CatalogError(DatamgrError):
    """Base for SQLite catalog adapter errors."""


class CatalogOpenError(CatalogError):
    """Failed to open or initialize the catalog/dataset database."""


class CatalogDDLApplyError(CatalogError):
    """Failed to apply authoritative DDL to a database."""


class CatalogQueryError(CatalogError):
    """Query or update failed (SQL error)."""


class CatalogIntegrityError(CatalogError):
    """Constraint violation (e.g., UNIQUE identity index)."""


class PartStoreError(DatamgrError):
    """Base for HDF5 part store errors."""


class H5WriteError(PartStoreError):
    """HDF5 write/seal operation failed."""


class AtomicReplaceError(PartStoreError):
    """Atomic rename/replace step failed during seal."""


class FsyncError(PartStoreError):
    """fsync of file or directory failed (crash-safety hook)."""


class ContentHashMismatchError(PartStoreError):
    """Computed content hash does not match expected value."""


class PartAlreadyExistsError(PartStoreError):
    """Part with same (subset_uuid, content_hash) is already registered."""


class SQLiteLoaderError(CatalogError):
    """Custom SQLite loader could not locate/prepare a suitable wheel."""


# Services: ingest / planner / merge / migrate ---------------------------------

class IngestError(DatamgrError):
    """Generic ingest pipeline failure."""


class BufferOverflowError(IngestError):
    """Ingest buffer exceeded configured thresholds."""


class FlushInProgressError(IngestError):
    """Operation not allowed while a flush is in progress."""


class PlannerError(DatamgrError):
    """Planner rewrite/pruning/costing failure."""


class PredicateRewriteError(PlannerError):
    """Failed to rewrite predicates (e.g., jagged meta pushdown)."""


class MergeError(DatamgrError):
    """Generic merge failure."""


class MergeInvariantError(MergeError):
    """Merge blocked due to dataset invariants mismatch (schema/policy)."""


class MergePolicyError(MergeError):
    """Merge rejected by policy (e.g., encryption posture)."""


class MergeDryRunOnlyError(MergeError):
    """Operation allowed only in --dry-run mode."""


class MigrationError(DatamgrError):
    """Migration/retolerance/rekey operation failed."""


# Change feed / batches ---------------------------------------------------------

class ChangeFeedError(DatamgrError):
    """Change feed (batches/batch_parts) error."""


class BatchConflictError(ChangeFeedError):
    """Batch already exists or conflicts with merge log."""


class BatchMissingError(ChangeFeedError):
    """Referenced batch does not exist."""


# Encryption hooks / policy -----------------------------------------------------

class EncryptionNotSupportedError(DatamgrError):
    """Encryption requested but not supported by current build or config."""


class EncryptionPolicyMismatchError(DatamgrError):
    """Encryption policy mismatch between datasets (merge guard)."""


class CryptoProviderError(DatamgrError):
    """Crypto provider failed (generate/encrypt/decrypt)."""


class KeyManagerError(DatamgrError):
    """Key manager failed (lookup/rotate/revoke)."""


# Hardening / OS posture / tamper-evidence -------------------------------------

class HardeningError(DatamgrError):
    """Hardening/OS posture check failed."""


class PosixPermissionError(HardeningError):
    """Filesystem permissions are weaker than policy requires."""


class OwnershipError(HardeningError):
    """Filesystem ownership does not match configured user/group."""


class LockAcquisitionError(HardeningError):
    """Failed to acquire advisory lock (dataset/db)."""


class ContainerRequirementError(HardeningError):
    """Process not running in required container/sandbox environment."""


class AuditLogError(HardeningError):
    """Audit log operation failed (append/rotate/verify)."""


class TamperEvidenceError(HardeningError):
    """Tamper-evidence chain mismatch or verification failure."""


# Navigator / TUI ---------------------------------------------------------------

class NavigatorError(DatamgrError):
    """Navigator/TUI operation failed."""


class PeekLimitExceededError(NavigatorError):
    """Requested peek exceeds configured row/byte limits."""


class PaginationError(NavigatorError):
    """Invalid pagination parameters or bounds."""


# Lookup / existence ------------------------------------------------------------

class NotFoundError(DatamgrError):
    """Generic not-found condition."""


class DatasetNotFoundError(NotFoundError):
    """Dataset alias/UUID not found."""


class SubsetNotFoundError(NotFoundError):
    """Subset UUID not found."""


class PartNotFoundError(NotFoundError):
    """Part UUID / file not found."""


# Warnings ---------------------------------------------------------------------

class PaddingAppliedWarning(DatamgrWarning):
    """Library padded jagged inputs (see SPEC §10); may affect size/hashing."""


class UnicodeWideningWarning(DatamgrWarning):
    """Canonical Unicode width widened to accommodate inputs."""


class DeprecatedAPIWarning(DatamgrWarning):
    """Use of a deprecated API surface."""


class PerformanceWarning(DatamgrWarning):
    """Potentially expensive operation detected."""


class SecurityPostureWarning(DatamgrWarning):
    """Security posture degraded (e.g., missing locks or weak perms)."""
