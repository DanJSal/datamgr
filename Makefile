# Makefile
SHELL := bash
.ONESHELL:
.SILENT:

PKG_DIR   ?= datamgr
ARTIFACTS ?= artifacts
DMANIFEST ?= .dm/deltas.json
SHA       ?= $(shell git rev-parse --short HEAD)
REPO      ?= $(shell basename `git rev-parse --show-toplevel`)

.PHONY: lint collect collect-delta merge nav index site clean-deltas

lint:
	python tools/lint.py --dir $(PKG_DIR)

collect:
	mkdir -p $(ARTIFACTS)
	python tools/introspect_collect.py --dir $(PKG_DIR) --out $(ARTIFACTS)

collect-delta:
	mkdir -p $(ARTIFACTS)
	if [[ -f $(ARTIFACTS)/nodes.json ]] && python - <<'PY'
import json, pathlib, sys
p = pathlib.Path("$(DMANIFEST)")
try:
    d = json.loads(p.read_text(encoding="utf-8"))
    sys.exit(0 if (d.get("paths") or d.get("modules")) else 1)
except Exception:
    sys.exit(1)
PY
	then
		echo "[delta] collecting only flagged modules/files"
		python tools/introspect_collect.py --dir $(PKG_DIR) --out $(ARTIFACTS) --delta-only --deltas $(DMANIFEST)
		python tools/nav_merge.py
	else
		echo "[delta] no prior full or empty manifest; doing full collect"
		$(MAKE) collect
	fi

merge:
	python tools/nav_merge.py

nav:
	AF=$$( [[ -s $(ARTIFACTS)/affected_fqids.txt ]] && echo "$(ARTIFACTS)/affected_fqids.txt" || echo "/dev/null" )
	python tools/build_navigator.py \
	  --commit "$(SHA)" \
	  --nodes $(ARTIFACTS)/nodes.json \
	  --edges $(ARTIFACTS)/edges.json \
	  --affected-fqids $$AF \
	  --out docs/api-nav \
	  --repo "$(REPO)"

index:
	python tools/repo_index.py

site: collect-delta nav index

clean-deltas:
	python tools/deltas.py clear || true
