#!/usr/bin/env python3
from __future__ import annotations
import ast
from pathlib import Path
from typing import List
from lint_common import Finding, Opts

FAIL, WARN = "FAIL", "WARN"

def _attach_parents(tree: ast.AST) -> None:
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            child.parent = parent  # type: ignore[attr-defined]

def _has_star_import(tree: ast.AST) -> bool:
    return any(isinstance(n, ast.ImportFrom) and any(a.name == "*" for a in n.names) for n in ast.walk(tree))

def _decorators_missing_wraps(func: ast.FunctionDef) -> bool:
    if not func.decorator_list: return False
    for d in func.decorator_list:
        name = d.id if isinstance(d, ast.Name) else (d.attr if isinstance(d, ast.Attribute) else None)
        if name and name.endswith("wraps"): return False
    return True

def _nested_def_violations(tree: ast.AST) -> List[tuple[int, str]]:
    out: List[tuple[int, str]] = []
    class V(ast.NodeVisitor):
        def visit_FunctionDef(self, node: ast.FunctionDef):
            inners = [n for n in node.body if isinstance(n, ast.FunctionDef)]
            # forbid nested-nesting
            for inner in inners:
                if any(isinstance(ch, ast.FunctionDef) for ch in ast.walk(inner) if ch is not inner):
                    out.append((inner.lineno, "DM021 nested-nesting not allowed"))
            inner_names = {n.name for n in inners}
            for sub in ast.walk(node):
                if isinstance(sub, ast.Name) and sub.id in inner_names:
                    parent = getattr(sub, "parent", None)
                    ok = isinstance(parent, ast.Call) and parent.func is sub
                    if not ok:
                        out.append((sub.lineno, f"DM022 inner '{sub.id}' escapes parent"))
            self.generic_visit(node)
    _attach_parents(tree); V().visit(tree); return out

def run_ast(files, opts: Opts):
    findings = []
    top_init = (Path(opts.src) / opts.dir / "__init__.py") if opts.pkg else None
    for path in files:
        try:
            text = path.read_text(encoding="utf-8")
            tree = ast.parse(text, filename=str(path))
        except Exception as e:
            findings.append(Finding(str(path), 1, "DM000_parse", FAIL, f"parse error: {e}"))
            continue

        if _has_star_import(tree):
            findings.append(Finding(str(path), 1, "DM010_star_import", FAIL, "star-import is forbidden"))

        has_all = any(isinstance(n, (ast.Assign, ast.AnnAssign)) and any(
            isinstance(t, ast.Name) and t.id == "__all__" for t in (n.targets if isinstance(n, ast.Assign) else [n.target])
        ) for n in tree.body)
        if has_all and top_init and path.resolve() != top_init.resolve():
            findings.append(Finding(str(path), 1, "DM011_all_internal", "FAIL",
                                    "__all__ only allowed in top-level __init__.py"))

        for n in ast.walk(tree):
            if isinstance(n, ast.FunctionDef) and _decorators_missing_wraps(n):
                findings.append(Finding(str(path), n.lineno, "DM030_wraps", WARN, "decorated function w/o @wraps (heuristic)"))

        for ln, msg in _nested_def_violations(tree):
            findings.append(Finding(str(path), ln, "DM021_nested", FAIL, msg))
    return findings
