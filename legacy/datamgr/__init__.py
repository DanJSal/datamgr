#datamgr/__init__.py
from .manager import Manager
from .affinity_ingest import ingest

__all__ = ["Manager", "ingest"]
