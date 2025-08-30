#!/usr/bin/env python3
"""
Builds the package atlas HTML set.

docs/api-nav/<commit>/
  atlas.html
  node/<fqid>.html
  meta/<fqid>.html
  body/<fqid>.html
  edges/<fqid>/out.html
  edges/<fqid>/in.html
and mirrors to docs/api-nav/latest/

Usage (repo/package both 'datamgr', package at repo root):
  python tools/build_package_atlas.py \
    --commit "$(git rev-parse --short HEAD)" \
    --nodes artifacts/nodes.json \
    --edges artifacts/edges.json \
    --out docs/api-nav --repo datamgr
"""
from __future__ import annotations
import argparse, json, shutil
from pathlib import Path
from html import escape

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--commit", required=True)
    p.add_argument("--nodes", required=True)
    p.add_argument("--edges", required=True)
    p.add_argument("--out",   default="docs/api-nav")
    p.add_argument("--repo",  default="datamgr")  # site base path: /datamgr
    p.add_argument("--page-size", type=int, default=200)     # edges pagination
    p.add_argument("--max-code-lines", type=int, default=400)
    p.add_argument("--affected-fqids", default="", help="newline-delimited file; if set, only rebuild affected nodes + neighbors (atlas always full)")
    return p.parse_args()

def site_root(repo):
    return f"/{repo}"

def nav_header(repo: str) -> str:
    return (
        f'<p><a href="/{repo}/">← Back to Repo Index</a> · '
        f'<a href="/{repo}/api-nav/latest/atlas.html">Package Atlas</a></p>'
    )

def write_html(path: Path, body: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")

def build_in_edges(edges_out: dict) -> dict:
    inbound = {}
    for src, outs in edges_out.items():
        for dst in outs:
            inbound.setdefault(dst, []).append(src)
    for k in inbound:
        inbound[k] = sorted(set(inbound[k]))
    return inbound

def trim_code(text: str, max_lines: int) -> str:
    lines = text.splitlines(keepends=True)
    if len(lines) <= max_lines:
        return "".join(lines)
    half = max_lines // 2
    return "".join(lines[:half]) + "\n# … trimmed …\n" + "".join(lines[-half:])

def main():
    a = parse_args()
    nodes = json.loads(Path(a.nodes).read_text(encoding="utf-8"))
    edges = json.loads(Path(a.edges).read_text(encoding="utf-8"))
    edges_out = edges.get("out", {})
    edges_in = edges.get("in") or build_in_edges(edges_out)

    # index nodes
    node_index = {n["fqid"]: n for n in nodes}
    all_ids = sorted(node_index.keys())

    out_base = Path(a.out)
    snap = out_base / "latest"  # <— use latest as the only output
    snap.mkdir(parents=True, exist_ok=True)

    # 1) atlas (link to latest)
    atlas_lines = [nav_header(a.repo), "<ul>\n"]
    for fq in all_ids:
        atlas_lines.append(f'  <li><a href="/{a.repo}/api-nav/latest/node/{escape(fq)}.html">{escape(fq)}</a></li>\n')
    atlas_lines.append("</ul>\n")
    write_html(snap / "atlas.html", "".join(atlas_lines))

    # ---- regen set (build-all if no/empty affected list)
    all_set = set(all_ids)
    regen = all_set  # default: full rebuild

    base = None
    if a.affected_fqids:
        try:
            base = {
                ln.strip()
                for ln in Path(a.affected_fqids).read_text(encoding="utf-8").splitlines()
                if ln.strip()
            }
        except FileNotFoundError:
            base = set()

    if base:  # only narrow when we actually have affected ids
        neighbors = set()
        # outbound neighbors
        for s in base:
            neighbors.update(edges_out.get(s, []))
        # inbound neighbors
        for s, outs in edges_out.items():
            if any(t in base for t in outs):
                neighbors.add(s)
        regen = (base | neighbors) & all_set

# ---- 2) per-node pages (only regen set)
    for fq in sorted(regen):
        meta = node_index[fq]
        # hub
        hub = [nav_header(a.repo), "<ul>\n"]
        hub.append(f'  <li><a rel="meta" href="/{a.repo}/api-nav/latest/meta/{escape(fq)}.html">meta</a></li>\n')
        if meta.get("has_body"):
            hub.append(f'  <li><a rel="body" href="/{a.repo}/api-nav/latest/body/{escape(fq)}.html">body</a></li>\n')
        hub.append(f'  <li><a rel="out" href="/{a.repo}/api-nav/latest/edges/{escape(fq)}/out.html">calls →</a></li>\n')
        hub.append(
            f'  <li><a rel="in"  href="/{a.repo}/api-nav/latest/edges/{escape(fq)}/in.html">← called-by</a></li>\n')

        sp, s0, s1 = meta.get("source_path"), meta.get("source_start"), meta.get("source_end")
        if sp and s0 and s1:
            hub.append(f'  <li><a rel="src" href="https://github.com/danjsal/datamgr/blob/{a.commit}/{escape(sp)}#L{s0}-L{s1}">source</a></li>\n')
        hub.append("</ul>\n")
        write_html(snap / "node" / f"{fq}.html", "".join(hub))

        # meta page
        meta_page = [nav_header(a.repo), f"<h1>{escape(fq)}</h1>\n<ul>"]
        meta_page.append(f"<li>kind: {escape(meta.get('kind','unknown'))}</li>")
        if sp and s0 and s1:
            meta_page.append(f"<li>source: <code>{escape(sp)}:{s0}-{s1}</code></li>")
        meta_page.append("</ul>")
        write_html(snap / "meta" / f"{fq}.html", "".join(meta_page))

        # body page
        if meta.get("has_body") and sp and s0 and s1:
            try:
                text = Path(sp).read_text(encoding="utf-8")
                snippet = trim_code("".join(text.splitlines(keepends=True)[s0-1:s1]), a.max_code_lines)
            except Exception:
                snippet = "# source unavailable"
            write_html(snap / "body" / f"{fq}.html", nav_header(a.repo) + f"<pre><code>{escape(snippet)}</code></pre>")

        # edges pages (pure link lists)
        def write_edges(direction: str, seq: list[str]):
            base = snap / "edges" / fq
            base.mkdir(parents=True, exist_ok=True)
            if not seq:
                write_html(base / f"{direction}.html", nav_header(a.repo) + "<ul></ul>")
                return
            page, i, size = 1, 0, a.page_size
            while i < len(seq):
                chunk = seq[i:i+size]; i += size
                body = [nav_header(a.repo), "<ul>\n"]
                for t in chunk:
                    body.append(f'  <li><a href="/{a.repo}/api-nav/latest/node/{escape(t)}.html">{escape(t)}</a></li>\n')
                body.append("</ul>\n")
                name = f"{direction}.html" if page == 1 else f"{direction}.{page}.html"
                write_html(base / name, "".join(body)); page += 1

        write_edges("out", sorted(set(edges_out.get(fq, []))))
        write_edges("in",  sorted(set(edges_in.get(fq, []))))

    print(f"[build] wrote {snap} (Package Atlas)")

if __name__ == "__main__":
    main()
