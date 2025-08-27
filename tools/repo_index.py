#!/usr/bin/env python3
from __future__ import annotations
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import quote as urlquote
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "REPO_INDEX.md"

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _git(root: Path, *args: str) -> str | None:
    try:
        out = subprocess.check_output(["git", "-C", str(root), *args], text=True)
        return out.strip() or None
    except Exception:
        return None

def _detect_repo_info(root: Path) -> tuple[str, str, str]:
    owner = os.environ.get("GITHUB_OWNER")
    repo = os.environ.get("GITHUB_REPO")
    gh_repo = os.environ.get("GITHUB_REPOSITORY")
    if (not owner or not repo) and gh_repo and "/" in gh_repo:
        owner, repo = gh_repo.split("/", 1)
    if not (owner and repo):
        remote = _git(root, "config", "--get", "remote.origin.url") or ""
        if "github.com" in remote:
            s = remote.split("github.com", 1)[1].lstrip(":").lstrip("/").rstrip("/")
            if s.endswith(".git"):
                s = s[:-4]
            parts = s.split("/")
            if len(parts) >= 2:
                owner = owner or parts[-2]
                repo = repo or parts[-1]
    if not (owner and repo):
        raise SystemExit(
            "repo_index: cannot detect owner/repo. Set GITHUB_REPOSITORY=owner/repo "
            "or ensure a GitHub 'origin' remote is configured."
        )
    ref = os.environ.get("GITHUB_REF_NAME") or _git(root, "rev-parse", "--abbrev-ref", "HEAD") or "main"
    return owner, repo, ref

def _group_by_top(paths: Iterable[str]) -> dict[str, list[str]]:
    buckets: dict[str, list[str]] = {}
    for p in paths:
        top = p.split("/", 1)[0]
        buckets.setdefault(top, []).append(p)
    return buckets

def _gh_tree(owner: str, repo: str, ref: str) -> dict:
    ref_for_api = urlquote(ref, safe="")
    url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{ref_for_api}?recursive=1"
    headers = {"User-Agent": "repo_index/1.0"}
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with urlopen(Request(url, headers=headers)) as resp:
        return json.load(resp)

def _blob_url(owner: str, repo: str, ref: str, path: str) -> str:
    ref_segment = urlquote(ref, safe="/")
    path_segment = urlquote(path, safe="/")
    return f"https://github.com/{owner}/{repo}/blob/{ref_segment}/{path_segment}"

def _raw_url(owner: str, repo: str, ref: str, path: str) -> str:
    ref_segment = urlquote(ref, safe="/")
    path_segment = urlquote(path, safe="/")
    return f"https://raw.githubusercontent.com/{owner}/{repo}/{ref_segment}/{path_segment}"

def _tree_url(owner: str, repo: str, ref: str) -> str:
    ref_segment = urlquote(ref, safe="/")
    return f"https://github.com/{owner}/{repo}/tree/{ref_segment}"

def _render_markdown(owner: str, repo: str, ref: str, files: list[str], *, truncated: bool) -> str:
    header_lines = [
        "# Repository Index (GitHub-sourced)",
        "",
        f"_Updated: {_now_iso()}_",
        "",
        f"Repo: `{owner}/{repo}`  —  Branch: `{ref}`  —  Root: `{ROOT.name}`  ",
        f"GitHub tree: {_tree_url(owner, repo, ref)}",
        "",
    ]
    if truncated:
        header_lines += [
            "> **Note:** GitHub API reported this tree as **truncated**. "
            "Some very large repositories may omit deep entries in this listing.",
            "",
        ]
    top_docs = sorted(
        [p for p in files if "/" not in p and p.lower().endswith((".md", ".rst", ".txt"))],
        key=str.lower,
    )
    lines = header_lines[:]
    if top_docs:
        lines += ["## Top-level docs", ""]
        for p in top_docs:
            lines.append(f"- {p} — [Blob]({_blob_url(owner, repo, ref, p)}) · [Raw]({_raw_url(owner, repo, ref, p)})")
        lines.append("")
    groups = _group_by_top(files)
    for top in sorted(groups, key=str.lower):
        lines += [f"## {top}", ""]
        for p in sorted(groups[top], key=str.lower):
            lines.append(f"- {p} — [Blob]({_blob_url(owner, repo, ref, p)}) · [Raw]({_raw_url(owner, repo, ref, p)})")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"

def main() -> int:
    owner, repo, ref = _detect_repo_info(ROOT)
    payload = _gh_tree(owner, repo, ref)
    tree = payload.get("tree") or []
    truncated = bool(payload.get("truncated"))
    files = [e["path"] for e in tree if e.get("type") == "blob" and isinstance(e.get("path"), str)]
    md = _render_markdown(owner, repo, ref, files, truncated=truncated)
    OUT.write_text(md, encoding="utf-8")
    print(f"Wrote {OUT}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
