#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, fnmatch
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

# ---------- data classes ----------
@dataclass
class Opts:
    # --pkg is optional; inferred from --dir if it looks like a package
    pkg: Optional[str]
    # repo root (fixed to ".")
    src: str
    # subdirectory to lint (repo-relative), e.g. "datamgr" or "tests"
    dir: str
    # optional excludes (modules within pkg; dirs within --dir)
    exclude_mod: List[str]
    exclude_dir: List[str]
    # phases/format
    phase: str
    format: str
    fail_on_warn: bool

@dataclass
class Finding:
    path: str
    line: int
    code: str
    severity: str  # "FAIL" or "WARN"
    msg: str

# ---------- ignore helpers ----------
def load_ignore(src_root: str) -> dict:
    p = Path(src_root) / ".dm" / "ignore.json"
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"files": [], "modules": []}

def match_any(val: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatch(val, pat) for pat in (patterns or []))

# ---------- argv / opts ----------
def parse_argv(argv: List[str]) -> Opts:
    p = argparse.ArgumentParser(description="datamgr code-level lints")
    p.add_argument("--dir", required=True, help="repo-relative subdirectory to lint (e.g., 'datamgr' or 'tests')")
    p.add_argument("--pkg", default=None, help="top-level package name (optional; auto-detected from --dir)")
    # src fixed to repo root; hide the flag
    p.add_argument("--src", default=".", help=argparse.SUPPRESS)
    p.add_argument("--exclude-mod", action="append", default=[])
    p.add_argument("--exclude-dir", action="append", default=[])
    p.add_argument("--phase", choices=["ast", "import", "all"], default="all")
    p.add_argument("--format", choices=["text", "json"], default="text")
    p.add_argument("--fail-on-warn", action="store_true")
    a = p.parse_args(argv)

    # auto-detect pkg if not provided and --dir looks like a package root
    pkg = a.pkg
    pkg_root = Path(a.dir)
    if pkg is None and (pkg_root / "__init__.py").exists():
        pkg = pkg_root.name

    return Opts(pkg, a.src, a.dir, a.exclude_mod, a.exclude_dir, a.phase, a.format, a.fail_on_warn)

# ---------- discovery ----------
def _want_path(path: Path, opts: Opts, base: Path, repo_root: Path) -> bool:
    # only inside --dir; apply --exclude-dir against repo-relative path
    try:
        rel_in_dir = path.relative_to(base)
    except ValueError:
        return False
    rel_repo = path.resolve().relative_to(repo_root.resolve()).as_posix()
    if opts.exclude_dir and match_any(rel_repo, opts.exclude_dir):
        return False
    return True

def discover_py_files(src_root: str, opts: Opts) -> List[Path]:
    root = Path(src_root).resolve()
    base = (root / opts.dir).resolve()
    ign_files = (load_ignore(src_root).get("files") or [])

    files: List[Path] = []
    for p in base.rglob("*.py"):
        rel_repo = p.resolve().relative_to(root).as_posix()
        if match_any(rel_repo, ign_files):
            continue
        if _want_path(p, opts, base, root):
            files.append(p)
    return files

# ---------- module mapping ----------
def module_name_from_path(opts: Opts, path: Path) -> Optional[str]:
    """
    Build a module name only if the path is under a detected/declared package.
    We treat opts.dir as the candidate package root.
    """
    if not opts.pkg:
        return None
    repo_root = Path(opts.src).resolve()
    try:
        rel = path.resolve().relative_to(repo_root)
    except ValueError:
        return None
    parts = list(rel.parts)
    if not parts or parts[0] != opts.pkg:
        return None
    if parts[-1] == "__init__.py":
        parts = parts[:-1]
    else:
        parts[-1] = parts[-1][:-3]
    if not parts:
        return None
    return ".".join(parts)

def filter_modules(mods: Iterable[str], opts: Opts) -> List[str]:
    ms = []
    for m in mods:
        if not m:
            continue
        if opts.exclude_mod and match_any(m, opts.exclude_mod):
            continue
        ms.append(m)
    return sorted(set(ms))

# ---------- reporting ----------
def print_findings(findings: List[Finding], fmt: str) -> int:
    if fmt == "json":
        import json as _json
        print(_json.dumps([f.__dict__ for f in findings], indent=2))
    else:
        for f in findings:
            loc = f"{f.path}:{f.line}" if f.line else f.path
            print(f"[{f.severity}] {loc} {f.code}: {f.msg}")
        print(f"— {len(findings)} finding(s)")
    # return number of FAILs (caller uses to decide exit code)
    return sum(1 for f in findings if f.severity == "FAIL")

def severity_ok(findings: List[Finding], fail_on_warn: bool) -> bool:
    if any(f.severity == "FAIL" for f in findings):
        return False
    if fail_on_warn and any(f.severity == "WARN" for f in findings):
        return False
    return True
