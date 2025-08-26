#!/usr/bin/env python3
from __future__ import annotations
import argparse, os, sys
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_OUTPUT = ROOT / "REPO_TREE.md"
DEFAULT_IGNORES = {
    ".git", ".hg", ".svn", ".idea", ".vscode", ".DS_Store",
    "__pycache__", ".mypy_cache", ".pytest_cache", ".ruff_cache",
    ".cache", ".tox", ".nox", ".eggs", "dist", "build", "node_modules",
    ".venv", "venv",
}

def build_argparser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Write REPO_TREE.md with a file tree snapshot of the repository.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--root", default=str(ROOT), help="Repository root to scan.")
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output Markdown path.")
    ap.add_argument("--max-depth", type=int, default=-1, help="Limit depth (-1 = unlimited).")
    ap.add_argument("--include-hidden", action="store_true", help="Include dotfiles/directories.")
    ap.add_argument("--ignore", action="append", default=[],
                    help="Extra names to ignore (repeatable).")
    return ap

def cli_help_markdown() -> str:
    return "\n".join([
        "## CLI Reference",
        "",
        "```text",
        build_argparser().format_help().rstrip(),
        "```",
        "",
    ])

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def list_entries(dirpath: Path, include_hidden: bool, ignores: set[str]) -> list[Path]:
    try:
        items = list(dirpath.iterdir())
    except Exception:
        return []
    def visible(p: Path) -> bool:
        name = p.name
        if name in ignores:
            return False
        if not include_hidden and name.startswith("."):
            return False
        return True
    items = [p for p in items if visible(p)]
    items.sort(key=lambda p: (p.is_file(), p.name.lower()))
    return items

def tree_lines(path: Path, prefix: str, depth: int, max_depth: int,
               include_hidden: bool, ignores: set[str]) -> list[str]:
    if max_depth >= 0 and depth > max_depth:
        return []
    entries = list_entries(path, include_hidden, ignores)
    lines: list[str] = []
    for i, p in enumerate(entries):
        connector = "└── " if i == len(entries) - 1 else "├── "
        line = f"{prefix}{connector}{p.name}"
        if p.is_symlink():
            try:
                target = os.readlink(p)
                line += f" -> {target}"
            except OSError:
                line += " -> ?"
        lines.append(line)
        if p.is_dir() and (max_depth < 0 or depth < max_depth) and not p.is_symlink():
            extension = "    " if i == len(entries) - 1 else "│   "
            lines.extend(
                tree_lines(p, prefix + extension, depth + 1, max_depth, include_hidden, ignores)
            )
    return lines

def write_atomic(path: Path, content: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)

def main(argv=None) -> int:
    ap = build_argparser()
    args = ap.parse_args(argv)

    root = Path(args.root).resolve()
    out = Path(args.output).resolve()

    ignores = set(DEFAULT_IGNORES)
    ignores.update(args.ignore or [])

    header = (
        "# Repository Tree\n\n"
        f"_Updated: {now_iso()}_\n\n"
        f"Root: `{root}`\n\n"
    )

    cli = cli_help_markdown()

    tree_block_open = "## Tree\n\n```text\n" + f"{root.name}\n"
    body = "\n".join(
        tree_lines(root, prefix="", depth=1, max_depth=args.max_depth,
                   include_hidden=args.include_hidden, ignores=ignores)
    )
    tree_block_close = "\n```\n"

    write_atomic(out, header + cli + tree_block_open + body + tree_block_close)
    print(f"Wrote {out}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
