#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, os, sys, time, pathlib
from collections import defaultdict

ROOT = pathlib.Path(__file__).resolve().parents[1]
TRACK = ROOT / "progress.json"
OUT = ROOT / "PROGRESS.md"

DEFAULT_STATUSES = ["pending", "skeleton", "impl", "tested", "docs"]

def load_state():
    if TRACK.exists():
        with TRACK.open("r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {"statuses": DEFAULT_STATUSES, "modules": {}}
    if "statuses" not in data:
        data["statuses"] = DEFAULT_STATUSES
    if "modules" not in data:
        data["modules"] = {}
    return data

def save_state(data):
    TRACK.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")

def rel(p: str) -> str:
    rp = os.path.relpath(os.path.abspath(p), str(ROOT))
    return rp.replace("\\", "/")

def validate_paths(paths):
    out = []
    for p in paths:
        rp = rel(p)
        # allow creating entries before file exists; just normalize path
        out.append(rp)
    return out

def update_modules(data, paths, status, note, force=False):
    statuses = data["statuses"]
    if status not in statuses:
        raise SystemExit(f"Unknown status '{status}'. Known: {statuses}")
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for rp in paths:
        cur = data["modules"].get(rp, {"status": "pending", "notes": [], "updated_at": ""})
        if not force:
            # prevent accidental regression
            if statuses.index(status) < statuses.index(cur["status"]):
                print(f"Skip {rp}: '{status}' < current '{cur['status']}'. Use --force to downgrade.")
                continue
        cur["status"] = status
        if note:
            cur["notes"] = list(cur.get("notes", [])) + [note]
        cur["updated_at"] = now
        data["modules"][rp] = cur

def _latest_note(meta: dict) -> str:
    ns = meta.get("notes")
    return ns[-1] if isinstance(ns, list) and ns else ""

def _ellipsize(s: str, maxlen: int = 160) -> str:
    s = (s or "").strip()
    return (s[: maxlen - 1] + "…") if len(s) > maxlen else s

def _md_escape_cell(s: str) -> str:
    # Basic escaping for Markdown table cell content
    return (s or "").replace("|", "\\|").replace("\n", " ")

def render_latest_notes(modules: dict, limit: int = 20) -> str:
    """Render a table of the most recent notes across modules."""
    rows = []
    for rp, meta in modules.items():
        note = _latest_note(meta)
        if not note:
            continue
        rows.append((meta.get("updated_at", ""), rp, meta.get("status", ""), note))
    rows.sort(key=lambda r: r[0], reverse=True)
    head = [
        "## Latest Notes",
        "",
        "| Updated | Module | Status | Note |",
        "|---|---|---|---|",
    ]
    body = [
        f"| `{u}` | `{rp}` | `{s}` | {_md_escape_cell(_ellipsize(n))} |"
        for (u, rp, s, n) in rows[:limit]
    ]
    if not body:
        body = ["_No notes yet. Add one with `--note`._"]
    return "\n".join(head + body) + "\n"

def render_progress(data):
    # group modules by top-level dir
    buckets = defaultdict(list)
    for rp, meta in sorted(data["modules"].items()):
        head = rp.split("/", 2)[0]  # e.g., datamgr, tests, tools
        buckets[head].append((rp, meta))
    # add known directories even if empty
    for d in ["datamgr", "tests"]:
        buckets.setdefault(d, [])
    # compute overall stats
    sts = data["statuses"]
    totals = {s: 0 for s in sts}
    for _, meta in data["modules"].items():
        totals[meta["status"]] = totals.get(meta["status"], 0) + 1
    total_count = sum(totals.values()) or 0

    # markdown
    lines = []
    lines.append("# Progress\n")
    lines.append("Status flow: `pending → skeleton → impl → tested → docs`\n")
    if total_count:
        pct = lambda s: f"{(100.0 * totals.get(s, 0) / total_count):.1f}%"
        summary = " | ".join(f"**{s}**: {totals.get(s, 0)} ({pct(s)})" for s in sts)
        lines.append(f"**Overall:** {summary}\n")
    lines.append("## By area\n")
    def badge(s): return f"`{s}`"
    for area in sorted(buckets):
        items = buckets[area]
        # small table per area
        lines.append(f"### {area}\n")
        lines.append("| Module | Status | Updated |\n|---|---|---|")
        if not items:
            lines.append("| _none_ |  |  |")
        else:
            for rp, meta in items:
                lines.append(f"| `{rp}` | {badge(meta['status'])} | {meta.get('updated_at','')} |")
        lines.append("")
    # dynamic latest notes
    lines.append(render_latest_notes(data["modules"]))
    # usage/help
    lines.append("## Notes")
    lines.append("- Use `tools/tick_progress.py path [path ...] --status skeleton|impl|tested|docs --note \"msg\"`")
    lines.append("- Add `--force` to allow downgrades (rare).")
    return "\n".join(lines) + "\n"

def main(argv=None):
    ap = argparse.ArgumentParser(description="Tick progress and regenerate PROGRESS.md")
    ap.add_argument("paths", nargs="*", help="Paths to tick (files or dirs).")
    ap.add_argument("--status", required=False, default=None, help="New status.")
    ap.add_argument("--note", default="", help="Optional note to append.")
    ap.add_argument("--force", action="store_true", help="Allow status downgrade.")
    ap.add_argument("--render-only", action="store_true", help="Only regenerate PROGRESS.md from progress.json.")
    args = ap.parse_args(argv)

    data = load_state()

    if args.render_only:
        OUT.write_text(render_progress(data), encoding="utf-8")
        print(f"Rendered {OUT}")
        return 0

    if not args.paths or not args.status:
        ap.error("Provide at least one path and --status (or use --render-only).")

    paths = validate_paths(args.paths)
    update_modules(data, paths, args.status, args.note, force=args.force)
    save_state(data)
    OUT.write_text(render_progress(data), encoding="utf-8")
    print(f"Updated {', '.join(paths)} -> {args.status}")
    print(f"Wrote {OUT}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
