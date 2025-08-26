#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, os, sys, time, pathlib
from collections import defaultdict

PROG = "python tools/tick_progress.py"
ROOT = pathlib.Path(__file__).resolve().parents[1]
TRACK = ROOT / "progress.json"
OUT = ROOT / "PROGRESS.md"

DEFAULT_STATUSES = ["pending", "skeleton", "impl", "tested", "docs"]

def build_argparser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog=PROG,
        description="Tick progress and regenerate PROGRESS.md",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("paths", nargs="*", help="Paths to tick (files or dirs).")
    ap.add_argument("--status", required=False, default=None, help="New status.")
    ap.add_argument("--note", default="", help="Optional note to append.")
    ap.add_argument("--force", action="store_true", help="Allow status downgrade.")
    ap.add_argument("--render-only", action="store_true",
                    help="Only regenerate PROGRESS.md from progress.json.")
    ap.add_argument("--set-current", nargs="+", metavar="ITEM",
                    help="Replace the Current list with these item(s).")
    ap.add_argument("--set-next", nargs="+", metavar="ITEM",
                    help="Replace the Next list with these item(s).")
    ap.add_argument("--promote", action="store_true",
                    help="Before setting Next, move existing Next -> Current.")
    return ap

def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

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
    data.setdefault("current", [])
    data.setdefault("next", [])
    data.setdefault("updated_at", "")
    data.setdefault("current_updated_at", "")
    data.setdefault("next_updated_at", "")
    return data

def save_state(data):
    data["updated_at"] = _now_iso()
    TRACK.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")

def rel(p: str) -> str:
    rp = os.path.relpath(os.path.abspath(p), str(ROOT))
    return rp.replace("\\", "/")

def validate_paths(paths):
    out = []
    for p in paths:
        rp = rel(p)
        out.append(rp)
    return out

def update_modules(data, paths, status, note, force=False):
    statuses = data["statuses"]
    if status not in statuses:
        raise SystemExit(f"Unknown status '{status}'. Known: {statuses}")
    now = _now_iso()
    for rp in paths:
        cur = data["modules"].get(rp, {"status": "pending", "notes": [], "updated_at": ""})
        if not force:
            if statuses.index(status) < statuses.index(cur["status"]):
                print(f"Skip {rp}: '{status}' < current '{cur['status']}'. Use --force to downgrade.")
                continue
        cur["status"] = status
        if note:
            cur["notes"] = list(cur.get("notes", [])) + [note]
        cur["updated_at"] = now
        data["modules"][rp] = cur

def set_current(data, items):
    data["current"] = list(items)
    data["current_updated_at"] = _now_iso()

def set_next(data, items, *, promote=False):
    now = _now_iso()
    if promote:
        data["current"] = list(data.get("next", []))
        data["current_updated_at"] = now
    data["next"] = list(items)
    data["next_updated_at"] = now

def _latest_note(meta: dict) -> str:
    ns = meta.get("notes")
    return ns[-1] if isinstance(ns, list) and ns else ""

def _ellipsize(s: str, maxlen: int = 160) -> str:
    s = (s or "").strip()
    return (s[: maxlen - 1] + "…") if len(s) > maxlen else s

def _md_escape_cell(s: str) -> str:
    return (s or "").replace("|", "\\|").replace("\n", " ")

def render_latest_notes(modules: dict, limit: int = 20) -> str:
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
    buckets = defaultdict(list)
    for rp, meta in sorted(data["modules"].items()):
        head = rp.split("/", 2)[0]
        buckets[head].append((rp, meta))
    for d in ["datamgr", "tests"]:
        buckets.setdefault(d, [])

    sts = data["statuses"]
    totals = {s: 0 for s in sts}
    for _, meta in data["modules"].items():
        totals[meta["status"]] = totals.get(meta["status"], 0) + 1
    total_count = sum(totals.values()) or 0

    lines = []
    lines.append("# Progress\n")

    if data.get("updated_at"):
        lines.append(f"_Updated: {data['updated_at']}_\n")

    lines.append("## CLI Reference\n")
    lines.append("```text")
    lines.append(build_argparser().format_help().rstrip())
    lines.append("```\n")

    cur = data.get("current", []) or []
    nxt = data.get("next", []) or []

    lines.append("## Current\n")
    if data.get("current_updated_at"):
        lines.append(f"_Updated: {data['current_updated_at']}_\n")
    lines.extend([f"- {item}" for item in cur] or ["- (none)"])
    lines.append("")

    lines.append("## Next\n")
    if data.get("next_updated_at"):
        lines.append(f"_Updated: {data['next_updated_at']}_\n")
    lines.extend([f"- {item}" for item in nxt] or ["- (none)"])
    lines.append("")

    lines.append("## Status Summary\n")
    lines.append("Flow: `pending → skeleton → impl → tested → docs`\n")
    if total_count:
        pct = lambda s: f"{(100.0 * totals.get(s, 0) / total_count):.1f}%"
        summary = " | ".join(f"**{s}**: {totals.get(s, 0)} ({pct(s)})" for s in sts)
        lines.append(f"**Overall:** {summary}\n")

    lines.append("## By Area\n")
    def badge(s): return f"`{s}`"
    for area in sorted(buckets):
        items = buckets[area]
        lines.append(f"### {area}\n")
        lines.append("| Module | Status | Updated |\n|---|---|---|")
        if not items:
            lines.append("| _none_ |  |  |")
        else:
            for rp, meta in items:
                lines.append(f"| `{rp}` | {badge(meta['status'])} | {meta.get('updated_at','')} |")
        lines.append("")

    lines.append(render_latest_notes(data["modules"]))

    return "\n".join(lines) + "\n"

def main(argv=None):
    ap = build_argparser()
    args = ap.parse_args(argv)

    if args.set_current and args.set_next and args.promote:
        build_argparser().error(
            "Ambiguous: --set-current with --set-next --promote. "
            "Promote moves the old Next → Current; don’t also set Current in the same call."
        )

    data = load_state()

    if args.set_current or args.set_next:
        if args.set_current:
            set_current(data, args.set_current)
        if args.set_next:
            promote_flag = bool(args.promote)
            set_next(data, args.set_next, promote=promote_flag)
        save_state(data)
        OUT.write_text(render_progress(data), encoding="utf-8")
        print(f"Updated Current/Next; wrote {OUT}")
        return 0

    if args.render_only:
        OUT.write_text(render_progress(data), encoding="utf-8")
        print(f"Rendered {OUT}")
        return 0

    if not args.paths or not args.status:
        ap.error("Provide at least one path and --status (or use --set-current/--set-next or --render-only).")

    paths = validate_paths(args.paths)
    update_modules(data, paths, args.status, args.note, force=args.force)
    save_state(data)
    OUT.write_text(render_progress(data), encoding="utf-8")
    print(f"Updated {', '.join(paths)} -> {args.status}")
    print(f"Wrote {OUT}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
