#!/usr/bin/env python3
from __future__ import annotations
import sys
from lint_common import parse_argv, print_findings, severity_ok
import lint_ast, lint_import
from lint_common import discover_py_files

def main(argv):
    opts = parse_argv(argv)
    findings = []
    if opts.phase in ("ast", "all"):
        files = discover_py_files(opts.src, opts)
        findings += lint_ast.run_ast(files, opts)
    if opts.phase in ("import", "all"):
        findings += lint_import.run_import(opts)
    exit_code = 0 if severity_ok(findings, opts.fail_on_warn) else 1
    print_findings(findings, opts.format)
    raise SystemExit(exit_code)

if __name__ == "__main__":
    main(sys.argv[1:])
