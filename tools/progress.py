#!/usr/bin/env python3
"""Render and maintain project progress.

This script updates a JSON state file (progress.json) tracking module statuses,
notes, and **Current / Previous / Next** queues, and renders a human-readable
PROGRESS.md. It can also set statuses/notes for a single file path and manage
the worklists.

Changes vs. prior version:
- Adds a **Previous** list (with set/clear).
- `--promote` now shifts **Current → Previous** and **Next → Current**
  before (optionally) setting a new Next list.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import pathlib
from collections import defaultdict
from typing import Any, Dict, List

PROG = "python tools/progress.py"
ROOT = pathlib.Path(__file__).resolve().parents[1]
TRACK = ROOT / "progress.json"
OUT = ROOT / "PROGRESS.md"
DEFAULT_STATUSES = ["pending", "skeleton", "impl", "tested", "docs"]


# ----------------------------- CLI ---------------------------------
def build_argparser() -> argparse.ArgumentParser:
    """Construct the CLI argument parser for updating progress and rendering output."""
    ap = argparse.ArgumentParser(
        prog=PROG,
        description="Update progress and regenerate PROGRESS.md",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("path", nargs="?", help="Path to a single file to update.")
    ap.add_argument("--status", required=False, default=None, help="New status.")
    ap.add_argument("--note", default="", help="Optional note to append.")
    ap.add_argument("--force", action="store_true", help="Allow status downgrade.")
    ap.add_argument(
        "--render-only",
        action="store_true",
        help="Only regenerate PROGRESS.md from progress.json.",
    )

    # Allow clearing by accepting zero items AND explicit clear flags.
    ap.add_argument(
        "--set-current",
        nargs="*",
        metavar="ITEM",
        help="Replace the Current list with these item(s). Empty = clear.",
    )
    ap.add_argument(
        "--set-previous",
        nargs="*",
        metavar="ITEM",
        help="Replace the Previous list with these item(s). Empty = clear.",
    )
    ap.add_argument(
        "--set-next",
        nargs="*",
        metavar="ITEM",
        help="Replace the Next list with these item(s). Empty = clear.",
    )
    ap.add_argument("--clear-current", action="store_true", help="Clear Current list.")
    ap.add_argument("--clear-previous", action="store_true", help="Clear Previous list.")
    ap.add_argument("--clear-next", action="store_true", help="Clear Next list.")
    ap.add_argument(
        "--promote",
        action="store_true",
        help="Before setting Next, move Current → Previous and Next → Current.",
    )
    return ap


# --------------------------- utilities ------------------------------
def _now_iso() -> str:
    """Return current UTC time in ISO8601 (Z) format."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_state() -> Dict[str, Any]:
    """Load progress state from progress.json, providing defaults if missing."""
    if TRACK.exists():
        with TRACK.open("r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {"statuses": DEFAULT_STATUSES, "modules": {}}

    if "statuses" not in data:
        data["statuses"] = DEFAULT_STATUSES
    if "modules" not in data:
        data["modules"] = {}

    # Worklists + timestamps
    data.setdefault("current", [])
    data.setdefault("previous", [])
    data.setdefault("next", [])
    data.setdefault("updated_at", "")
    data.setdefault("current_updated_at", "")
    data.setdefault("previous_updated_at", "")
    data.setdefault("next_updated_at", "")
    return data


def save_state(data: Dict[str, Any]) -> None:
    """Persist progress state to progress.json, stamping an updated_at timestamp."""
    data["updated_at"] = _now_iso()
    TRACK.write_text(
        json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def rel(p: str) -> str:
    """Normalize a path to a repo-relative POSIX-style string."""
    rp = os.path.relpath(os.path.abspath(p), str(ROOT))
    return rp.replace("\\", "/")


def validate_path(path: str) -> str:
    """Validate a single path: must be an existing file within the repo."""
    if not path:
        raise SystemExit("Missing file path.")
    abs_p = pathlib.Path(path).resolve()
    try:
        _ = abs_p.relative_to(ROOT)
    except ValueError:
        raise SystemExit(f"Path must be inside the repository: {path}")
    if not abs_p.exists():
        raise SystemExit(f"Path does not exist: {path}")
    if not abs_p.is_file():
        raise SystemExit(f"Path is not a file: {path}")
    return rel(str(abs_p))


# --------------------------- mutations ------------------------------
def update_modules(
    data: Dict[str, Any], paths: List[str], status: str, note: str, *, force: bool = False
) -> None:
    """Update status (and optionally append a note) for each path entry."""
    statuses = data["statuses"]
    if status not in statuses:
        raise SystemExit(f"Unknown status '{status}'. Known: {statuses}")
    now = _now_iso()
    for rp in paths:
        cur = data["modules"].get(
            rp, {"status": "pending", "notes": [], "updated_at": ""}
        )
        if not force:
            if statuses.index(status) < statuses.index(cur["status"]):
                print(
                    f"Skip {rp}: '{status}' < current '{cur['status']}'. "
                    f"Use --force to downgrade."
                )
                continue
        cur["status"] = status
        if note:
            cur["notes"] = list(cur.get("notes", [])) + [note]
        cur["updated_at"] = now
        data["modules"][rp] = cur


def set_current(data: Dict[str, Any], items: List[str]) -> None:
    """Replace the Current list and timestamp it."""
    data["current"] = list(items)
    data["current_updated_at"] = _now_iso()


def set_previous(data: Dict[str, Any], items: List[str]) -> None:
    """Replace the Previous list and timestamp it."""
    data["previous"] = list(items)
    data["previous_updated_at"] = _now_iso()


def set_next(data: Dict[str, Any], items: List[str], *, promote: bool = False) -> None:
    """Replace the Next list; if promoting, shift Current→Previous and Next→Current."""
    now = _now_iso()
    if promote:
        # Current → Previous
        data["previous"] = list(data.get("current", []))
        data["previous_updated_at"] = now
        # Next → Current
        data["current"] = list(data.get("next", []))
        data["current_updated_at"] = now
    # Now set (or keep) Next
    data["next"] = list(items)
    data["next_updated_at"] = now


# ----------------------------- render -------------------------------
def _latest_note(meta: dict) -> str:
    """Return the most recent note from a module's metadata, if any."""
    ns = meta.get("notes")
    return ns[-1] if isinstance(ns, list) and ns else ""


def _ellipsize(s: str, maxlen: int = 160) -> str:
    """Trim a string to maxlen characters and add an ellipsis if needed."""
    s = (s or "").strip()
    return (s[: maxlen - 1] + "…") if len(s) > maxlen else s


def _md_escape_cell(s: str) -> str:
    """Escape Markdown table cell delimiters and newlines."""
    return (s or "").replace("|", "\\|").replace("\n", " ")


def render_latest_notes(modules: dict, limit: int = 20) -> str:
    """Render a Markdown table of the latest notes across modules."""
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


def render_progress(data: Dict[str, Any]) -> str:
    """Render the full PROGRESS.md from the given state dict."""
    buckets: Dict[str, List[Any]] = defaultdict(list)
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

    lines: List[str] = []
    lines.append("# Progress\n")

    cur = data.get("current", []) or []
    prv = data.get("previous", []) or []
    nxt = data.get("next", []) or []

    lines.append("## Current\n")
    if data.get("current_updated_at"):
        lines.append(f"_Updated: {data['current_updated_at']}_\n")
    lines.extend([f"- {item}" for item in cur] or ["- (none)"])
    lines.append("")

    lines.append("## Previous\n")
    if data.get("previous_updated_at"):
        lines.append(f"_Updated: {data['previous_updated_at']}_\n")
    lines.extend([f"- {item}" for item in prv] or ["- (none)"])
    lines.append("")

    lines.append("## Next\n")
    if data.get("next_updated_at"):
        lines.append(f"_Updated: {data['next_updated_at']}_\n")
    lines.extend([f"- {item}" for item in nxt] or ["- (none)"])
    lines.append("")

    lines.append("## Status Summary\n")
    lines.append("Flow: `pending → skeleton → impl → tested → docs`\n")
    if total_count:
        def pct(s: str) -> str:
            return f"{(100.0 * totals.get(s, 0) / total_count):.1f}%"
        summary = " | ".join(f"**{s}**: {totals.get(s, 0)} ({pct(s)})" for s in sts)
        lines.append(f"**Overall:** {summary}\n")

    lines.append("## By Area\n")
    def badge(s: str) -> str: return f"`{s}`"
    for area in sorted(buckets):
        items = buckets[area]
        lines.append(f"### {area}\n")
        lines.append("| Module | Status | Updated |\n|---|---|---|")
        if not items:
            lines.append("| _none_ |  |  |")
        else:
            for rp, meta in items:
                lines.append(
                    f"| `{rp}` | {badge(meta['status'])} | {meta.get('updated_at','')} |"
                )
        lines.append("")

    lines.append(render_latest_notes(data["modules"]))
    return "\n".join(lines) + "\n"


# ----------------------------- main --------------------------------
def main(argv: List[str] | None = None) -> int:
    """CLI entrypoint: parse args, mutate state as requested, and write outputs."""
    ap = build_argparser()
    args = ap.parse_args(argv)

    # Presence detection (empty list means "clear")
    present_current = (args.set_current is not None)
    present_previous = (args.set_previous is not None)
    present_next = (args.set_next is not None)

    # Disallow mixing both setters with --promote (presence-based)
    if present_current and present_next and args.promote:
        ap.error(
            "Ambiguous: --set-current with --set-next --promote. "
            "Promote moves old Next → Current; don’t also set Current in the same call."
        )
    # Also disallow setting Previous while promoting (Previous is derived from Current)
    if present_previous and args.promote:
        ap.error(
            "Ambiguous: --set-previous with --promote. "
            "Promote sets Previous from Current; don’t also set Previous in the same call."
        )

    data = load_state()

    # Handle explicit clears or setters (empty list means clear) and/or promotion.
    if (
        args.clear_current or args.clear_previous or args.clear_next
        or present_current or present_previous or present_next
        or args.promote
    ):
        if args.clear_current or present_current:
            set_current(data, [] if args.clear_current else (args.set_current or []))
        if args.clear_previous or present_previous:
            set_previous(data, [] if args.clear_previous else (args.set_previous or []))
        if args.clear_next or present_next or args.promote:
            # If only promoting, keep existing Next items while shifting.
            next_items = [] if args.clear_next else (args.set_next or data.get("next", []))
            set_next(data, next_items, promote=bool(args.promote))

        save_state(data)
        OUT.write_text(render_progress(data), encoding="utf-8")

        which = []
        if args.clear_current or (present_current and not args.set_current):
            which.append("Current")
        if args.clear_previous or (present_previous and not args.set_previous):
            which.append("Previous")
        if args.clear_next or (present_next and not args.set_next):
            which.append("Next")
        label = " and ".join(which) if which else ("lists" if not args.promote else "lists (promoted)")
        print(f"Updated {label}; wrote {OUT}")
        return 0

    # Render-only path
    if args.render_only:
        OUT.write_text(render_progress(data), encoding="utf-8")
        print(f"Rendered {OUT}")
        return 0

    # Path/status update path
    if not args.path or not args.status:
        ap.error(
            "Provide a file path and --status "
            "(or use --set-current/--set-previous/--set-next/--clear-*/--promote/--render-only)."
        )

    rp = validate_path(args.path)
    update_modules(data, [rp], args.status, args.note, force=args.force)
    save_state(data)
    OUT.write_text(render_progress(data), encoding="utf-8")
    print(f"Updated {rp} -> {args.status}")
    print(f"Wrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
