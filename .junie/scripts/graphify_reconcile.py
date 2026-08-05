#!/usr/bin/env python3
# Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.
"""Reconcile a graphify extraction before it is built into a graph.

Run between the AST/semantic merge and `build_from_json`:

    python3 .junie/scripts/graphify_reconcile.py graphify-out/.graphify_extract.json

Two things leave edges pointing at nodes that do not exist, and the graph builder
silently drops both:

1. Import edges. The AST extractor emits `imports` edges targeting `go_pkg_<path>`
   but never creates a node for the imported package, so every import of something
   outside the scanned tree dangles. These edges are correct and worth keeping, so
   the missing package nodes are synthesized rather than the edges discarded. That
   turns the largest slice of "dangling" into real structure: with the nodes
   present, the graph can answer which packages depend on a given dependency.

2. Semantic IDs a doc extraction referenced but never defined, usually a node-ID
   format mismatch. Nothing can be recovered from these, so they are dropped and
   counted. Silently keeping them would leave the graph reporting edges it does
   not have.
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

# Prefixes the AST extractor uses for things it references but does not create a
# node for: `go_pkg_` for imported packages, `pkg_` for module dependencies.
PACKAGE_PREFIXES = ("go_pkg_", "pkg_")


def is_package(node_id: str) -> bool:
    return node_id.startswith(PACKAGE_PREFIXES)


def package_label(node_id: str) -> str:
    """Recover a readable import path from a synthesized package id.

    The id is lossy — `/` and `.` and `-` all became `_` — so this cannot perfectly
    invert it. It reads well for the common cases (`go_pkg_sync_atomic` ->
    `sync/atomic`) and never claims more precision than it has.
    """
    stem = node_id
    for prefix in PACKAGE_PREFIXES:
        if stem.startswith(prefix):
            stem = stem.removeprefix(prefix)
            break

    if stem.startswith("github_com_"):
        return stem.replace("github_com_", "github.com/", 1).replace("_", "/")
    return stem.replace("_", "/")


def reconcile(extraction: dict) -> dict:
    """Return the extraction with package nodes added and unresolvable edges dropped."""
    known = {node["id"] for node in extraction.get("nodes", [])}
    edges = extraction.get("edges", [])

    missing = Counter()
    for edge in edges:
        for end in ("source", "target"):
            if edge[end] not in known:
                missing[edge[end]] += 1

    # Synthesize a node for every referenced package.
    synthesized = []
    for node_id in sorted(missing):
        if not is_package(node_id):
            continue
        synthesized.append(
            {
                "id": node_id,
                "label": package_label(node_id),
                "file_type": "code",
                "source_file": None,
                "source_location": None,
                "source_url": None,
                "captured_at": None,
                "author": None,
                "contributor": None,
            }
        )
        known.add(node_id)

    # Whatever still dangles cannot be resolved.
    kept, dropped = [], []
    for edge in edges:
        if edge["source"] in known and edge["target"] in known:
            kept.append(edge)
        else:
            dropped.append(edge)

    hyperedges = [
        h for h in extraction.get("hyperedges", []) if all(n in known for n in h.get("nodes", []))
    ]
    dropped_hyperedges = len(extraction.get("hyperedges", [])) - len(hyperedges)

    print(f"packages synthesized: {len(synthesized)}")
    print(f"edges kept:           {len(kept)}")
    print(f"edges dropped:        {len(dropped)} (unresolvable node ids)")
    if dropped_hyperedges:
        print(f"hyperedges dropped:   {dropped_hyperedges}")

    if dropped:
        unresolved = Counter()
        for edge in dropped:
            for end in ("source", "target"):
                if edge[end] not in known:
                    unresolved[edge[end]] += 1
        print("unresolvable ids:")
        for node_id, count in unresolved.most_common(15):
            print(f"  {count:4d}  {node_id}")

    extraction["nodes"] = extraction.get("nodes", []) + synthesized
    extraction["edges"] = kept
    extraction["hyperedges"] = hyperedges
    return extraction


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__.strip(), file=sys.stderr)
        return 2

    path = Path(sys.argv[1])
    extraction = json.loads(path.read_text(encoding="utf-8"))
    path.write_text(
        json.dumps(reconcile(extraction), indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
