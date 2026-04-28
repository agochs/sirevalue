"""Copy worker/stakes-results.json → public/data/stakes-results.json with
indexing by sire and a recency sort.

Source-of-truth is the hand-curated worker file (cited primary sources per
ZERO FABRICATION rule). This script just shapes it for the front end:
  - Sorts races by date desc (so home-page panel shows most recent first)
  - Builds `by_sire` index (sire name → list of races) so stallion-card can
    surface "Progeny stakes wins" per stallion in O(1)
  - Strips the _doc / _schema / _workflow fields so they don't ship to clients
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).parent
SRC = HERE / "stakes-results.json"
OUT = HERE.parent / "public" / "data" / "stakes-results.json"


def main():
    if not SRC.exists():
        raise SystemExit(f"Missing {SRC.name}")
    src = json.loads(SRC.read_text())
    races = src.get("races") or []

    # Sort: most recent first
    races_sorted = sorted(races, key=lambda r: (r.get("date") or "0000-00-00"), reverse=True)

    # Index by sire (case-preserving, but we lowercase the key for matching)
    by_sire = defaultdict(list)
    for r in races_sorted:
        sire = (r.get("winner_sire") or "").strip()
        if not sire:
            continue
        by_sire[sire].append(r)

    out_doc = {
        "updated_at":  src.get("updated_at"),
        "summary": {
            "total_races":   len(races),
            "sires_covered": len(by_sire),
        },
        "races":   races_sorted,
        "by_sire": dict(by_sire),
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out_doc, indent=2))
    print(f"Wrote {OUT} with {len(races_sorted)} race(s) across {len(by_sire)} sire(s)")
    for sire, rs in sorted(by_sire.items(), key=lambda kv: -len(kv[1])):
        print(f"  {sire:24s} {len(rs)} progeny stakes win(s)")


if __name__ == "__main__":
    main()
