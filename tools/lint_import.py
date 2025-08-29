#!/usr/bin/env python3
from __future__ import annotations
import builtins, importlib, inspect, os, pkgutil, socket, subprocess, sys, threading
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from lint_common import Finding, Opts, module_name_from_path, filter_modules, discover_py_files, load_ignore, match_any

FAIL, WARN = "FAIL", "WARN"

# ---- import-safety watchdog ----
class _ImportWatch:
    def __init__(self):
        self._old_open = builtins.open
        self._old_conn = socket.create_connection
        self._old_popen = subprocess.Popen
        self._threads_before = set(threading.enumerate())
    def __enter__(self):
        def safe_open(path, mode="r", *a, **k):
            if any(m in mode for m in ("w","a","x","+")):
                raise RuntimeError(f"write at import: {path} mode={mode}")
            return self._old_open(path, mode, *a, **k)
        builtins.open = safe_open  # type: ignore
        socket.create_connection = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("network at import"))  # type: ignore
        subprocess.Popen = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("subprocess at import"))       # type: ignore
        return self
    def __exit__(self, *exc):
        builtins.open = self._old_open   # type: ignore
        socket.create_connection = self._old_conn  # type: ignore
        subprocess.Popen = self._old_popen  # type: ignore
        after = set(threading.enumerate())
        leaked = [t for t in after - self._threads_before if t.is_alive()]
        if leaked:
            raise AssertionError(f"threads spawned at import: {leaked}")

# ---- reflection helpers ----
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

def get_source_span(obj) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    try:
        lines, start = inspect.getsourcelines(obj)
        file = inspect.getsourcefile(obj) or inspect.getfile(obj)
        return (Path(file).as_posix(), start, start + len(lines) - 1)
    except Exception:
        return (None, None, None)

def fqid(mod: str, qualname: str) -> str:
    return f"{mod}.{qualname}"

def nodes_for_module(mod) -> Dict[str, Dict]:
    modname = mod.__name__
    nodes: Dict[str, Dict] = {}
    # module node
    mfile, ms0, ms1 = get_source_span(mod)
    nodes[modname] = {"fqid": modname, "kind": "module", "has_body": False,
                      "source_path": mfile, "source_start": ms0, "source_end": ms1}
    # funcs/classes
    for name, obj in sorted(mod.__dict__.items()):
        if is_top_level_func(obj, modname):
            f, s0, s1 = get_source_span(obj)
            nodes[fqid(modname, obj.__qualname__)] = {"fqid": fqid(modname, obj.__qualname__),
                                                      "kind": "function", "has_body": True,
                                                      "source_path": f, "source_start": s0, "source_end": s1}
        elif is_top_level_class(obj, modname):
            f, s0, s1 = get_source_span(obj)
            nodes[fqid(modname, obj.__qualname__)] = {"fqid": fqid(modname, obj.__qualname__),
                                                      "kind": "class", "has_body": True,
                                                      "source_path": f, "source_start": s0, "source_end": s1}
            for mname, func in class_body_members(obj):
                f2, s20, s21 = get_source_span(func)
                nodes[fqid(modname, f"{obj.__qualname__}.{mname}")] = {
                    "fqid": fqid(modname, f"{obj.__qualname__}.{mname}"),
                    "kind": "method", "has_body": True,
                    "source_path": f2, "source_start": s20, "source_end": s21,
                }
    return nodes

def uniq_fqids(nodes_by_module: Dict[str, Dict[str, Dict]]) -> List[str]:
    seen, dups = {}, []
    for _m, nodes in nodes_by_module.items():
        for fq, meta in nodes.items():
            if fq in seen and meta != seen[fq]:
                dups.append(fq)
            seen[fq] = meta
    return dups

def validate_calls(mod, nodes_all: Dict[str, Dict]) -> List[Finding]:
    findings: List[Finding] = []
    calls = getattr(mod, "CALLS", None)
    if calls is None: return findings
    if not hasattr(calls, "to_mapping"):
        findings.append(Finding(mod.__file__ or mod.__name__, 1, "DM040_calls_shape", FAIL, "CALLS lacks to_mapping()"))
        return findings
    mapping = calls.to_mapping()  # {src_fqid: [tgt_fqid,...]}
    for src, tgts in mapping.items():
        if src not in nodes_all:
            findings.append(Finding(mod.__file__ or mod.__name__, 1, "DM041_calls_source", FAIL, f"unknown source {src}"))
        # targets: allow Class -> Class.__init__ normalization
        for t in tgts:
            if t in nodes_all: continue
            if t + ".__init__" in nodes_all: continue
            findings.append(Finding(mod.__file__ or mod.__name__, 1, "DM042_calls_target", FAIL, f"unknown target {t}"))
        if src in set(tgts):
            findings.append(Finding(mod.__file__ or mod.__name__, 1, "DM043_calls_self", FAIL, f"self-edge {src}"))
    return findings

def run_import(opts: Opts):
    findings = []
    files = discover_py_files(opts.src, opts)

    # Modules only from --dir, and only if we can resolve to the package
    mods_all = [module_name_from_path(opts, p) for p in files]
    mods = [m for m in mods_all if m]

    # Central ignore (modules)
    ign = load_ignore(opts.src)
    mod_ign = ign.get("modules", []) or []
    if mod_ign:
        mods = [m for m in mods if not match_any(m, mod_ign)]

    if not mods:
        return []  # Nothing to import/reflect; AST phase will still report

    mods = filter_modules(mods, opts)

    sys.path.insert(0, str(Path(opts.src).resolve()))
    os.environ.pop("DM_INTROSPECT", None)

    nodes_by_module: Dict[str, Dict[str, Dict]] = {}
    # Import under watchdog
    with _ImportWatch():
        for m in mods:
            try:
                mod = importlib.import_module(m)
            except Exception as e:
                findings.append(Finding(m, 1, "DM002_import", FAIL, f"import failed: {e}"))
                continue
            nodes = nodes_for_module(mod)
            # source retrievability
            for fq, meta in nodes.items():
                if meta.get("has_body"):
                    f, s0, s1 = meta.get("source_path"), meta.get("source_start"), meta.get("source_end")
                    if not f or not s0 or not s1 or s1 < s0:
                        findings.append(Finding(mod.__file__ or m, 1, "DM020_source", FAIL, f"unretrievable source for {fq}"))
            nodes_by_module[m] = nodes
            # CALLS validation against a flattened view (all known nodes so far + later merged)
    # Flatten once after imports
    flat_nodes: Dict[str, Dict] = {}
    for _m, ns in nodes_by_module.items():
        flat_nodes.update(ns)
    # Validate CALLS now
    for m in mods:
        mod = sys.modules.get(m)
        if mod:
            findings.extend(validate_calls(mod, flat_nodes))

    # FQID global uniqueness
    dups = uniq_fqids(nodes_by_module)
    for fq in dups:
        findings.append(Finding(fq, 1, "DM019_fqid_dupe", FAIL, f"duplicate FQID {fq}"))
    # Coverage (warn): functions/methods with no CALLS source
    for m, nodes in nodes_by_module.items():
        mod = sys.modules.get(m); calls = getattr(mod, "CALLS", None) if mod else None
        declared = set(calls.to_mapping().keys()) if getattr(calls, "to_mapping", None) else set()
        for fq, meta in nodes.items():
            if meta["kind"] in ("function", "method") and fq not in declared:
                findings.append(Finding(mod.__file__ or m if mod else m, 1, "DM044_calls_coverage", WARN, f"{fq} has no CALLS entry"))
    return findings
