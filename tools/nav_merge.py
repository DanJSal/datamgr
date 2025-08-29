#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path

def loadj(p: Path):
    return json.loads(p.read_text(encoding="utf-8"))

def main(nodes_full="artifacts/nodes.json", edges_full="artifacts/edges.json",
         nodes_delta="artifacts/nodes_delta.json", edges_delta="artifacts/edges_delta.json"):
    nf, ef = Path(nodes_full), Path(edges_full)
    nd, ed = Path(nodes_delta), Path(edges_delta)
    if not nd.exists() or not ed.exists():
        print("[merge] no delta artifacts; nothing to do")
        return

    nodes_all = loadj(nf) if nf.exists() else []
    edges_all = loadj(ef)["out"] if ef.exists() else {}

    idx = {n["fqid"]: n for n in nodes_all}
    delta_nodes = loadj(nd)
    delta_fqids = {n["fqid"] for n in delta_nodes}
    delta_sources = {n.get("source_path") for n in delta_nodes if n.get("source_path")}

    # prune nodes that match delta fqids or share source_path
    for fq in list(idx.keys()):
        sp = idx[fq].get("source_path")
        if fq in delta_fqids or (sp and sp in delta_sources) or (sp and not Path(sp).exists()):
            idx.pop(fq, None)

    # insert/overwrite
    for n in delta_nodes:
        idx[n["fqid"]] = n

    nodes_merged = sorted(idx.values(), key=lambda x: x["fqid"])

    # edges: replace any sources present in delta; keep rest
    edges_delta = loadj(ed)["out"]
    for src in list(edges_all.keys()):
        if src in edges_delta:
            edges_all.pop(src)
    edges_all.update(edges_delta)

    Path(nodes_full).write_text(json.dumps(nodes_merged, indent=2), encoding="utf-8")
    Path(edges_full).write_text(json.dumps({"out": edges_all}, indent=2), encoding="utf-8")
    Path("artifacts/affected_fqids.txt").write_text("\n".join(sorted(delta_fqids)), encoding="utf-8")
    print("[merge] merged nodes/edges; wrote artifacts/affected_fqids.txt")

if __name__ == "__main__":
    import sys
    main(*sys.argv[1:])
