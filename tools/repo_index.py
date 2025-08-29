#!/usr/bin/env python3
"""Generate a clickable HTML index for the repo (for GitHub Pages).

- Uses the Git Data API (recursive tree) as the source of truth.
- Emits docs/index.html with Blob + Raw links for every tracked file.
- Permalink mode: links (and the API fetch) are pinned to the current commit SHA.
- The header shows the branch label and links to the live GitHub tree for that branch.
"""
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
OUT_HTML = ROOT / "docs" / "index.html"

def _now_iso() -> str:
    """Return current UTC time in ISO-8601 (Z) format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _git(root: Path, *args: str) -> str | None:
    """Run a git command at `root` and return stripped stdout, or None on error."""
    try:
        out = subprocess.check_output(["git", "-C", str(root), *args], text=True)
        return out.strip() or None
    except Exception:
        return None

def _detect_repo_info(root: Path) -> tuple[str, str, str]:
    """Detect (owner, repo, branch_label). Fail fast if owner/repo unknown."""
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

def _gh_tree(owner: str, repo: str, ref: str) -> dict:
    """Fetch the recursive tree for `ref` (branch or SHA). Return the JSON payload."""
    ref_for_api = urlquote(ref, safe="")  # encode '/' in names if present
    url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{ref_for_api}?recursive=1"
    headers = {"User-Agent": "repo_index/1.0"}
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with urlopen(Request(url, headers=headers)) as resp:
        return json.load(resp)

def _blob_url(owner: str, repo: str, ref: str, path: str) -> str:
    """Return a GitHub Blob URL for `path` at `ref` (commit SHA or branch)."""
    ref_segment = urlquote(ref, safe="/")
    path_segment = urlquote(path, safe="/")
    return f"https://github.com/{owner}/{repo}/blob/{ref_segment}/{path_segment}"

def _raw_url(owner: str, repo: str, ref: str, path: str) -> str:
    """Return a raw.githubusercontent.com URL for `path` at `ref`."""
    ref_segment = urlquote(ref, safe="/")
    path_segment = urlquote(path, safe="/")
    return f"https://raw.githubusercontent.com/{owner}/{repo}/{ref_segment}/{path_segment}"

def _tree_url(owner: str, repo: str, ref: str) -> str:
    """Return the GitHub UI tree URL for the given branch/ref."""
    ref_segment = urlquote(ref, safe="/")
    return f"https://github.com/{owner}/{repo}/tree/{ref_segment}"

# --- NEW: read deltas manifest (optional) ---
def _load_deltas(root: Path) -> dict:
    p = root / ".dm" / "deltas.json"
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"paths": [], "modules": [], "note": ""}

def _group_by_top(paths: Iterable[str]) -> dict[str, list[str]]:
    """Group file paths by their first path segment."""
    buckets: dict[str, list[str]] = {}
    for p in paths:
        top = p.split("/", 1)[0]
        buckets.setdefault(top, []).append(p)
    return buckets

def _render_html(owner: str, repo: str, branch_label: str, files: list[str], *, link_ref: str, truncated: bool) -> str:
    """Render the HTML link hub (grouped by top-level directory)."""
    parts: list[str] = []
    parts.append("<!doctype html>")
    parts.append("<html><head><meta charset='utf-8'>")
    parts.append(f"<title>{owner}/{repo} — {branch_label} @ {link_ref[:7]}</title>")
    parts.append(
        "<style>body{font:14px/1.45 system-ui,Segoe UI,Roboto,Helvetica,Arial}"
        "code{background:#f6f8fa;padding:2px 4px;border-radius:4px}"
        "h1{font-size:20px;margin:16px 0} h2{margin:20px 0 8px} ul{margin:6px 0 14px}"
        "li{margin:2px 0}</style>"
    )
    parts.append("</head><body>")
    parts.append(f"<h1>{owner}/{repo} — branch <code>{branch_label}</code>, commit <code>{link_ref[:7]}</code></h1>")
    parts.append(f"<p><a href='{_tree_url(owner, repo, branch_label)}'>View GitHub tree for branch</a> · "
                 f"Generated: {_now_iso()}</p>")

    # --- NEW: Quick links to the navigator ---
    # GitHub Pages base path is '/<repo>/...'; the navigator uses a short commit dir.
    short = link_ref[:7]
    parts.append(
        "<p><strong>Quick links:</strong> "
        f"<a href='/{repo}/api-nav/latest/atlas.html'>Navigator (latest)</a> · "
        f"<a href='/{repo}/api-nav/{short}/atlas.html'>Navigator (this commit)</a>"
        "</p>"
    )

    if truncated:
        parts.append("<p><strong>Note:</strong> GitHub API reported this tree as <em>truncated</em>; "
                     "some deep entries may be omitted.</p>")

    # --- NEW: Deltas panel (optional, informational) ---
    deltas = _load_deltas(ROOT)
    if (deltas.get("paths") or deltas.get("modules") or deltas.get("note")):
        parts.append("<details open><summary><strong>Deltas</strong></summary>")
        if deltas.get("updated"):
            parts.append(f"<p><em>Updated:</em> {deltas['updated']}</p>")
        if deltas.get("note"):
            parts.append(f"<p>{deltas['note']}</p>")
        stamps = (deltas.get("stamps") or {})
        sp = (stamps.get("paths") or {})
        sm = (stamps.get("modules") or {})

        if deltas.get("paths"):
            parts.append("<p><em>Paths</em></p><ul>")
            for p in deltas["paths"]:
                t = sp.get(p, "")
                when = f" <small>(touched {t})</small>" if t else ""
                parts.append(f"<li><code>{p}</code>{when}</li>")
            parts.append("</ul>")
        if deltas.get("modules"):
            parts.append("<p><em>Modules</em></p><ul>")
            for m in deltas["modules"]:
                t = sm.get(m, "")
                when = f" <small>(touched {t})</small>" if t else ""
                parts.append(f"<li><code>{m}</code>{when}</li>")
            parts.append("</ul>")
        parts.append("</details>")

    groups = _group_by_top(files)
    for top in sorted(groups, key=str.lower):
        parts.append(f"<h2>{top}</h2>")
        parts.append("<ul>")
        for p in sorted(groups[top], key=str.lower):
            blob = _blob_url(owner, repo, link_ref, p)
            raw = _raw_url(owner, repo, link_ref, p)
            parts.append(f"<li><code>{p}</code> — <a href='{blob}'>Blob</a> · <a href='{raw}'>Raw</a></li>")
        parts.append("</ul>")

    parts.append("</body></html>")
    return "".join(parts)

def main() -> int:
    """Entry point: detect repo info, fetch tree at SHA, render HTML, and write docs/index.html."""
    owner, repo, branch_label = _detect_repo_info(ROOT)
    link_ref = os.environ.get("GITHUB_SHA") or _git(ROOT, "rev-parse", "HEAD") or branch_label
    payload = _gh_tree(owner, repo, link_ref)
    tree = payload.get("tree") or []
    truncated = bool(payload.get("truncated"))
    files = [e["path"] for e in tree if e.get("type") == "blob" and isinstance(e.get("path"), str)]
    (ROOT / "docs").mkdir(exist_ok=True)
    html = _render_html(owner, repo, branch_label, files, link_ref=link_ref, truncated=truncated)
    OUT_HTML.write_text(html, encoding="utf-8")
    print(f"Wrote {OUT_HTML}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
