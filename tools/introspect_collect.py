#!/usr/bin/env python3
from __future__ import annotations
import argparse, importlib, inspect, json, fnmatch, os, pkgutil, sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# ---------- helpers: ignores, matching ----------
def load_ignore(repo_root: str) -> dict:
    p = Path(repo_root) / ".dm" / "ignore.json"
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"files": [], "modules": []}

def match_any(val: str, pats) -> bool:
    return any(fnmatch.fnmatch(val, pat) for pat in (pats or []))

# ---------- args ----------
def parse_args():
    p = argparse.ArgumentParser(description="Collect nodes + edges (CALLS) into artifacts JSONs")
    p.add_argument("--dir", required=True, help="repo-relative directory containing the package (e.g., 'datamgr' or 'src/datamgr')")
    p.add_argument("--pkg", default=None, help="top-level package name (optional; auto-detected from --dir)")
    p.add_argument("--out", default="artifacts", help="output directory (default: artifacts)")
    p.add_argument("--delta-only", action="store_true", help="collect only modules named in the deltas manifest")
    p.add_argument("--deltas", default=".dm/deltas.json", help="deltas manifest path (default: .dm/deltas.json)")
    return p.parse_args()

# ---------- module discovery ----------
def infer_pkg_name(dir_path: Path, pkg_hint: Optional[str]) -> str:
    if pkg_hint:
        return pkg_hint
    if (dir_path / "__init__.py").exists():
        return dir_path.name
    raise SystemExit(f"[collect] --pkg not provided and '{dir_path}' is not a package root (missing __init__.py)")

def discover_modules(pkg: str, pkg_path: str, ignore_mods: List[str]) -> List[str]:
    mods = sorted(m.name for m in pkgutil.walk_packages([pkg_path], prefix=f"{pkg}."))
    mods.append(pkg)  # include the package root
    if ignore_mods:
        mods = [m for m in mods if not match_any(m, ignore_mods)]
    return mods

def manifest_to_modules(pkg: str, repo_root: str, manifest_path: str, ignore_mods: List[str], dir_root: Path) -> List[str]:
    """
    Convert .dm/deltas.json {modules, paths} into a list of module names under dir_root.
    - modules: treated as module globs
    - paths: repo-relative globs; mapped to modules only if they live under dir_root/pkg
    """
    try:
        d = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    except Exception:
        return []

    out: set[str] = set()

    # module globs
    for m in d.get("modules", []) or []:
        if m.startswith(pkg + ".") or m == pkg:
            out.add(m)

    # path globs -> modules
    for pat in (d.get("paths") or []):
        for p in Path(repo_root).rglob(pat):
            try:
                # require inside the selected dir_root
                p.relative_to(dir_root)
            except ValueError:
                continue
            # map file to module if inside pkg subtree
            try:
                rel = p.relative_to(dir_root)
            except ValueError:
                continue
            parts = rel.parts
            if not parts or parts[0] != pkg or not str(p).endswith(".py"):
                continue
            if parts[-1] == "__init__.py":
                mod = ".".join(parts[:-1])  # pkg.sub
            else:
                mod = ".".join(parts)[:-3]  # strip .py
            out.add(mod)

    mods = sorted(out)
    if ignore_mods:
        mods = [m for m in mods if not match_any(m, ignore_mods)]
    return mods

# ---------- reflection ----------
def get_source_span(obj) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    try:
        lines, start = inspect.getsourcelines(obj)
        file = inspect.getsourcefile(obj) or inspect.getfile(obj)
        return (Path(file).as_posix(), start, start + len(lines) - 1)
    except Exception:
        return (None, None, None)

def is_top_level_func(obj, modname: str) -> bool:
    return inspect.isfunction(obj) and obj.__module__ == modname and "<locals>" not in (obj.__qualname__ or "")

def is_top_level_class(obj, modname: str) -> bool:
    return inspect.isclass(obj) and obj.__module__ == modname

def class_body_members(cls) -> List[Tuple[str, object]]:
    out = []
    for name, val in cls.__dict__.items():
        func = None
        if isinstance(val, property) and val.fget: func = val.fget
        elif inspect.isfunction(val): func = val
        if func: out.append((name, func))
    return out

def collect_module_nodes(mod) -> Dict[str, Dict]:
    modname = mod.__name__
    nodes: Dict[str, Dict] = {}
    mfile, ms0, ms1 = get_source_span(mod)
    nodes[modname] = {"fqid": modname, "kind":"module", "has_body": False,
                      "source_path": mfile, "source_start": ms0, "source_end": ms1}
    for name, obj in sorted(mod.__dict__.items()):
        if is_top_level_func(obj, modname):
            f, s0, s1 = get_source_span(obj)
            nodes[f"{modname}.{obj.__qualname__}"] = {"fqid": f"{modname}.{obj.__qualname__}", "kind":"function",
                                                      "has_body": True, "source_path": f, "source_start": s0, "source_end": s1}
        elif is_top_level_class(obj, modname):
            f, s0, s1 = get_source_span(obj)
            nodes[f"{modname}.{obj.__qualname__}"] = {"fqid": f"{modname}.{obj.__qualname__}", "kind":"class",
                                                      "has_body": True, "source_path": f, "source_start": s0, "source_end": s1}
            for mname, func in class_body_members(obj):
                f2, s20, s21 = get_source_span(func)
                nodes[f"{modname}.{obj.__qualname__}.{mname}"] = {
                    "fqid": f"{modname}.{obj.__qualname__}.{mname}", "kind":"method",
                    "has_body": True, "source_path": f2, "source_start": s20, "source_end": s21
                }
    return nodes

# ---------- main ----------
def main():
    a = parse_args()
    repo_root = Path(".").resolve()
    dir_root = (repo_root / a.dir).resolve()
    if not dir_root.exists():
        raise SystemExit(f"[collect] --dir '{a.dir}' does not exist")

    pkg = infer_pkg_name(dir_root, a.pkg)
    pkg_path = str(dir_root)  # filesystem path to package root

    # sys.path for imports
    sys.path.insert(0, str(repo_root))
    os.environ.pop("DM_INTROSPECT", None)  # modules must be import-safe regardless

    # central ignore
    ign = load_ignore(str(repo_root))
    ignore_mods = ign.get("modules") or []

    # module list (full or delta)
    if a.delta_only:
        modules = manifest_to_modules(pkg, str(repo_root), a.deltas, ignore_mods, dir_root)
        if not modules:
            print("[collect] delta-only requested but no modules matched; nothing to do")
            modules = []
    else:
        modules = discover_modules(pkg, pkg_path, ignore_mods)

    nodes_all: Dict[str, Dict] = {}
    edges_out: Dict[str, List[str]] = {}

    for m in modules:
        try:
            mod = importlib.import_module(m)
        except Exception as e:
            raise SystemExit(f"[collect] import failed for {m}: {e}")
        nodes = collect_module_nodes(mod)
        nodes_all.update(nodes)
        calls = getattr(mod, "CALLS", None)
        if getattr(calls, "to_mapping", None):
            for src, tgts in calls.to_mapping().items():
                edges_out[src] = tgts

    outdir = Path(a.out); outdir.mkdir(parents=True, exist_ok=True)

    if a.delta_only:
        (outdir / "nodes_delta.json").write_text(json.dumps(list(nodes_all.values()), indent=2), encoding="utf-8")
        (outdir / "edges_delta.json").write_text(json.dumps({"out": edges_out}, indent=2), encoding="utf-8")
        print("[collect] wrote nodes_delta.json and edges_delta.json")
    else:
        (outdir / "nodes.json").write_text(json.dumps(list(nodes_all.values()), indent=2), encoding="utf-8")
        (outdir / "edges.json").write_text(json.dumps({"out": edges_out}, indent=2), encoding="utf-8")
        print("[collect] wrote nodes.json and edges.json")

if __name__ == "__main__":
    main()
