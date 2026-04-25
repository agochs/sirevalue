"""Aggregate every prior scores.json snapshot into a per-stallion time series.

The worker writes a snapshot into scores_snapshots/ at the start of every
nightly cycle (see snapshot_scores in nightly_refresh.py). That archive
becomes our truth source for "how has this stallion's score moved over
time" — no separate database needed.

Output:
  score-history.json — { "by_stallion": { name: [ {date, score, tier, grade}, ... ] } }
                       Sorted oldest → newest. One entry per snapshot day.
                       (If multiple snapshots land on the same day — e.g. a
                       manually-triggered cycle — we keep only the latest.)
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).parent
SNAPSHOT_DIR = HERE / "scores_snapshots"
OUTPUT_JSON = HERE / "score-history.json"

# Snapshot filenames are scores-YYYYMMDD-HHMMSS.json
SNAP_RE = re.compile(r"^scores-(\d{8})-(\d{6})\.json$")


def parse_snap_filename(name: str):
    m = SNAP_RE.match(name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S")
    except ValueError:
        return None


def main():
    if not SNAPSHOT_DIR.exists():
        print(f"No snapshot directory at {SNAPSHOT_DIR}; nothing to aggregate.")
        OUTPUT_JSON.write_text(json.dumps({
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "by_stallion": {},
            "summary": {"snapshots_processed": 0, "stallions_with_history": 0},
        }, indent=2))
        return

    snaps = []
    for p in SNAPSHOT_DIR.glob("scores-*.json"):
        ts = parse_snap_filename(p.name)
        if ts:
            snaps.append((ts, p))
    snaps.sort(key=lambda x: x[0])
    print(f"Found {len(snaps)} snapshot(s) in {SNAPSHOT_DIR.name}")

    # by_stallion[name] = list of {date, score, tier, grade}, in chronological order
    by_stallion: dict[str, list[dict]] = {}
    seen_dates: dict[str, set[str]] = {}   # name -> set of date strings already added

    for ts, path in snaps:
        try:
            data = json.loads(path.read_text())
        except Exception as e:
            print(f"  skip {path.name}: {e}")
            continue
        date_str = ts.strftime("%Y-%m-%d")
        for s in data.get("stallions", []):
            name = s.get("name")
            sc = s.get("score") or {}
            value = sc.get("value")
            if not name or value is None:
                continue
            entry = {
                "date": date_str,
                "score": value,
                "tier": sc.get("tier"),
                "grade": sc.get("grade"),
            }
            seen = seen_dates.setdefault(name, set())
            if date_str in seen:
                # Same-day duplicate — replace with the LATER snapshot's value
                # (snaps is sorted ascending, so the latest in this iteration
                # is the most recent for the day).
                history = by_stallion[name]
                if history and history[-1]["date"] == date_str:
                    history[-1] = entry
                continue
            seen.add(date_str)
            by_stallion.setdefault(name, []).append(entry)

    # Cap each stallion's history to most recent ~120 days for payload size
    HISTORY_CAP = 120
    for name in by_stallion:
        if len(by_stallion[name]) > HISTORY_CAP:
            by_stallion[name] = by_stallion[name][-HISTORY_CAP:]

    output = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "by_stallion": by_stallion,
        "summary": {
            "snapshots_processed": len(snaps),
            "stallions_with_history": len(by_stallion),
            "max_points_per_stallion": HISTORY_CAP,
        },
    }
    OUTPUT_JSON.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    print(
        f"Wrote {OUTPUT_JSON.name}: {len(by_stallion)} stallions, "
        f"{sum(len(v) for v in by_stallion.values())} total data points"
    )


if __name__ == "__main__":
    main()
