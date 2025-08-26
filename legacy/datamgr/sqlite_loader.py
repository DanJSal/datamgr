# datamgr/sqlite_loader.py
from __future__ import annotations
import importlib
import sys
import zipfile
import platform
from pathlib import Path
from typing import Optional

_PKG_DIR = Path(__file__).resolve().parent
_WHEELS_ROOT = _PKG_DIR / "pysqlite3-wal2-wheels"
_CACHE_DIR = _WHEELS_ROOT / ".extracted"

_SQLITE_MOD: Optional[object] = None
_SQLITE_SRC: Optional[str] = None

def _py_tag() -> str:
    v = sys.version_info
    return f"cp{v.major}{v.minor}"

def _plat_tokens() -> list[str]:
    sysplat = sys.platform
    mach = platform.machine().lower()
    if sysplat == "win32":
        return ["win_amd64" if "64" in platform.architecture()[0] else "win32"]
    if sysplat == "darwin":
        return ["macosx", ("arm64" if mach in ("arm64", "aarch64") else "x86_64")]
    return ["linux", ("aarch64" if mach in ("aarch64", "arm64") else "x86_64")]

def _find_local_wheel() -> Optional[Path]:
    if not _WHEELS_ROOT.is_dir():
        return None
    py = _py_tag()
    plats = _plat_tokens()
    cands = []
    for whl in _WHEELS_ROOT.rglob("*.whl"):
        n = whl.name
        if not n.startswith("pysqlite3-"):
            continue
        if py not in n:
            continue
        if all(tok in n for tok in plats):
            cands.append(whl)
    if not cands:
        for whl in _WHEELS_ROOT.rglob("*.whl"):
            if whl.name.startswith("pysqlite3-") and _py_tag() in whl.name:
                cands.append(whl)
    if not cands:
        return None
    def score(p: Path) -> int:
        n = p.name
        s = 0
        for k in ("manylinux", "musllinux", "macosx", "win_"):
            if k in n: s += 10
        return s + len(n)
    cands.sort(key=score, reverse=True)
    return cands[0]

def _extract_wheel(whl: Path) -> Path:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    st = whl.stat()
    stamp = f"{whl.stem}-{st.st_size}-{int(st.st_mtime)}"
    dest = _CACHE_DIR / stamp
    if dest.is_dir():
        return dest
    tmp = _CACHE_DIR / (stamp + ".tmp")
    if tmp.exists():
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(str(whl), "r") as zf:
        zf.extractall(str(tmp))
    try:
        tmp.rename(dest)
    except OSError:
        if not dest.is_dir():
            raise
    return dest

def _ensure_wheel_ready() -> bool:
    try:
        importlib.import_module("pysqlite3")
        return True
    except Exception:
        pass
    whl = _find_local_wheel()
    if not whl:
        return False
    extracted = _extract_wheel(whl)
    if str(extracted) not in sys.path:
        sys.path.insert(0, str(extracted))
    return True

def _load_once():
    global _SQLITE_MOD, _SQLITE_SRC
    if _SQLITE_MOD is not None:
        return _SQLITE_MOD
    if not _ensure_wheel_ready():
        raise ImportError(
            "datamgr: could not locate or prepare a bundled pysqlite3 wheel for this "
            f"interpreter/platform under {_WHEELS_ROOT}"
        )
    mod = importlib.import_module("pysqlite3.dbapi2")
    _SQLITE_SRC = getattr(mod, "__file__", "pysqlite3")
    _SQLITE_MOD = mod
    return mod

def assert_compile_options(required: tuple[str, ...] = (
    "ENABLE_JSON1","ENABLE_FTS5","ENABLE_RTREE","ENABLE_STAT4",
    "ENABLE_MATH_FUNCTIONS","ENABLE_NORMALIZE","ENABLE_DESERIALIZE","ENABLE_NAN_INF"
)):
    m = _load_once()
    try:
        con = m.connect(":memory:")
        try:
            opts = {row[0] for row in con.execute("PRAGMA compile_options")}
        finally:
            con.close()
    except Exception:
        opts = set()
    missing = [o for o in required if not any(o in x for x in opts)]
    if missing:
        raise AssertionError(f"SQLite missing required compile options: {missing}")

class _SqliteProxy:
    __slots__ = ()
    def __getattr__(self, name):
        return getattr(_load_once(), name)
    def __dir__(self):
        return dir(_load_once())
    def __repr__(self):
        return f"<sqlite3 proxy -> {_SQLITE_SRC or 'unloaded'}>"

sqlite3 = _SqliteProxy()
