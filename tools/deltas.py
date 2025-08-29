#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from datetime import datetime, timezone
from pathlib import Path

MANIFEST = Path(".dm") / "deltas.json"
ISO = "%Y-%m-%dT%H:%M:%SZ"
DEFAULT = {
    "paths": [],
    "modules": [],
    "note": "",
    "updated": None,
    "stamps": {"paths": {}, "modules": {}}
}

def _now() -> str:
    return datetime.now(timezone.utc).strftime(ISO)

def load() -> dict:
    try:
        text = MANIFEST.read_text(encoding="utf-8")
        if not text.strip():
            return DEFAULT.copy()
        d = json.loads(text)
    except FileNotFoundError:
        return DEFAULT.copy()
    except json.JSONDecodeError:
        return DEFAULT.copy()
    # ensure keys exist
    d.setdefault("paths", []); d.setdefault("modules", [])
    d.setdefault("note", ""); d.setdefault("updated", None)
    d.setdefault("stamps", {"paths": {}, "modules": {}})
    d["stamps"].setdefault("paths", {}); d["stamps"].setdefault("modules", {})
    return d

def save(d: dict) -> None:
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(d, indent=2), encoding="utf-8")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["add", "clear", "show", "note"])
    ap.add_argument("--path", action="append", default=[])
    ap.add_argument("--module", action="append", default=[])
    ap.add_argument("--text", default="", help="note text for 'note' cmd")
    a = ap.parse_args()

    if a.cmd == "clear":
        save(DEFAULT.copy()); print("[deltas] cleared"); return

    d = load()

    if a.cmd == "show":
        print(json.dumps(d, indent=2)); return

    if a.cmd == "note":
        d["note"] = a.text
        d["updated"] = _now()
        save(d)
        print("[deltas] note updated"); return

    if a.cmd == "add":
        ts = _now()
        paths = set(d.get("paths") or [])
        mods  = set(d.get("modules") or [])
        for p in a.path:
            p = Path(p).as_posix()
            paths.add(p)
            d["stamps"]["paths"][p] = ts
        for m in a.module:
            mods.add(m)
            d["stamps"]["modules"][m] = ts
        d["paths"] = sorted(paths)
        d["modules"] = sorted(mods)
        d["updated"] = ts
        save(d)
        print(f"[deltas] paths={len(d['paths'])} modules={len(d['modules'])} @ {ts}")

if __name__ == "__main__":
    main()
