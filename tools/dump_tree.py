#!/usr/bin/env python3
from __future__ import annotations
import argparse, os, sys, subprocess
from pathlib import Path
from datetime import datetime, timezone

PROG = "python tools/dump_tree.py"
ROOT = Path(__file__).resolve().parents[1]

DEFAULT_OUTPUT = ROOT / "REPO_TREE.md"

ALWAYS_HIDE = {".git", ".idea", ".gitignore", ".gitattributes"}

FALLBACK_IGNORES = {
    ".hg", ".svn", ".DS_Store",
    "__pycache__", ".mypy_cache", ".pytest_cache", ".ruff_cache",
    ".cache", ".tox", ".nox", ".eggs", "dist", "build", "node_modules",
    ".venv", "venv",
}

def build_argparser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog=PROG,
        description="Write REPO_TREE.md with a file tree snapshot of the repository.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--root", default=str(ROOT), help="Repository root to scan.")
    ap.add_argument("--out", default=str(DEFAULT_OUTPUT), help="Output Markdown path.")
    ap.add_argument("--max-depth", type=int, default=-1, help="Maximum directory depth (-1 = unlimited).")
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

def _load_git_ignored(root: Path) -> tuple[set[str], set[str], bool]:
    try:
        chk = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--is-inside-work-tree"],
            check=True, capture_output=True, text=True,
        )
        if chk.stdout.strip().lower() != "true":
            return set(), set(), False

        cp = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-o", "-i", "--exclude-standard", "--directory", "--full-name"],
            check=True, capture_output=True, text=True,
        )
    except Exception:
        return set(), set(), False

    files: set[str] = set()
    dir_prefixes: set[str] = set()
    for raw in cp.stdout.splitlines():
        s = raw.strip()
        if not s:
            continue
        if s.endswith("/"):
            dir_prefixes.add(s)  # keep trailing slash for startswith checks
        else:
            files.add(s)
    return files, dir_prefixes, True

def list_entries(dirpath: Path, root: Path, git_ignored_files: set[str], git_ignored_dir_prefixes: set[str],
                 is_git_repo: bool, fallback_ignores: set[str]) -> list[Path]:
    try:
        items = list(dirpath.iterdir())
    except Exception:
        return []

    def visible(p: Path) -> bool:
        name = p.name

        if name in ALWAYS_HIDE:
            return False

        if is_git_repo:
            try:
                rel = p.relative_to(root).as_posix()
            except ValueError:
                rel = None
            if rel is not None:
                if rel in git_ignored_files:
                    return False
                for pref in git_ignored_dir_prefixes:
                    if rel == pref.rstrip("/") or rel.startswith(pref):
                        return False
            return True
        else:
            if name in fallback_ignores:
                return False
            return True

    items = [p for p in items if visible(p)]
    items.sort(key=lambda p: (p.is_file(), p.name.lower()))
    return items

def tree_lines(path: Path, prefix: str, depth: int, max_depth: int, root: Path,
               git_ignored_files: set[str], git_ignored_dir_prefixes: set[str], is_git_repo: bool,
               fallback_ignores: set[str]) -> list[str]:
    if max_depth >= 0 and depth > max_depth:
        return []
    entries = list_entries(path, root, git_ignored_files, git_ignored_dir_prefixes, is_git_repo, fallback_ignores)
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
                tree_lines(p, prefix + extension, depth + 1, max_depth, root,
                           git_ignored_files, git_ignored_dir_prefixes, is_git_repo, fallback_ignores)
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
    out = Path(args.out).resolve()

    git_files, git_dir_prefixes, is_git_repo = _load_git_ignored(root)

    header = (
        "# Repository Tree\n\n"
        f"_Updated: {now_iso()}_\n\n"
        f"Root: `{root.name}`\n\n"
    )

    cli = cli_help_markdown()

    tree_block_open = "## Tree\n\n```text\n" + f"{root.name}\n"
    body = "\n".join(
        tree_lines(
            root, prefix="", depth=1, max_depth=args.max_depth, root=root,
            git_ignored_files=git_files, git_ignored_dir_prefixes=git_dir_prefixes,
            is_git_repo=is_git_repo, fallback_ignores=FALLBACK_IGNORES
        )
    )
    tree_block_close = "\n```\n"

    write_atomic(out, header + cli + tree_block_open + body + tree_block_close)
    print(f"Wrote {out}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
