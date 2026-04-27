"""Copy worker/stallion-movements.json → public/data/stallion-movements.json
with sorting + a thin summary block.

Source-of-truth is the hand-curated worker file (cited primary sources per
ZERO FABRICATION rule). This script just shapes it for the front end:
  - Sorts movements by date desc (so the home-page panel shows recent first)
  - Adds a summary {total, by_type} block for quick stats
  - Strips the _doc / _schema fields so they don't ship to clients
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

HERE = Path(__file__).parent
SRC = HERE / "stallion-movements.json"
OUT = HERE.parent / "public" / "data" / "stallion-movements.json"


def parse_date_key(date_str: str) -> str:
    """Normalize 'YYYY-MM' or 'YYYY' to a sortable key. Missing → '0000-00'."""
    if not date_str:
        return "0000-00"
    parts = date_str.split("-")
    if len(parts) == 1:
        return f"{parts[0]}-00"   # year-only sorts after year-month within same year
    return date_str


def main():
    if not SRC.exists():
        raise SystemExit(f"Missing {SRC.name}")
    src = json.loads(SRC.read_text())
    movements = src.get("movements") or []

    # Sort: newest first by date, then alphabetic by stallion as tiebreaker
    movements_sorted = sorted(
        movements,
        key=lambda m: (parse_date_key(m.get("date", "")), m.get("stallion", "")),
        reverse=True,
    )

    by_type = Counter(m.get("type") for m in movements)
    summary = {
        "total":   len(movements),
        "by_type": dict(by_type),
    }

    out_doc = {
        "updated_at": src.get("updated_at"),
        "summary":    summary,
        "movements":  movements_sorted,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out_doc, indent=2))
    print(f"Wrote {OUT} with {len(movements_sorted)} movement(s)")
    print(f"  by_type: {dict(by_type)}")


if __name__ == "__main__":
    main()
