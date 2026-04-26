"""Audit each farm's CSV against the expected 2026 roster published by the farm.

Workflow:
  1. Open each farm's announcement_urls in expected-rosters-2026.json
  2. Copy the published 2026 stallion list into the matching expected_names[] array
  3. Run this script — it diffs CSV vs expected and reports:
       - missing-from-CSV  (stallion is on the farm's published roster but not in our CSV — likely a real gap)
       - extra-in-CSV      (stallion is in our CSV but not in the published roster — possibly pensioned, sold, or moved)

The script does name normalization (lowercase, strip apostrophes, strip "the"/"of" articles)
so spelling variants don't create false positives.

ZERO FABRICATION applies: we only flag stallions that the user has explicitly
listed in expected_names[]. We never invent names. Empty expected_names[] just
means "audit not yet performed for this farm" — the script silently skips it.

Usage:
  python3 audit_rosters.py
  python3 audit_rosters.py --farm spendthrift   # single farm
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path

HERE = Path(__file__).parent
CONFIG_JSON = HERE / "expected-rosters-2026.json"


def normalize_name(name: str) -> str:
    """Stable normalization: lowercase, strip country suffix (IRE/USA/...),
    strip ALL non-alphanumeric chars (including spaces and periods). So:
      'Honor A.P.'  → 'honorap'
      'Honor A. P.' → 'honorap'   (matches)
      'McKinzie'    → 'mckinzie'
      'Mckinzie'    → 'mckinzie'  (matches)
      'Bolt d\u2019Oro' / 'Bolt d\u2019oro' / 'Bolt D\u2019oro' all → 'boltdoro'
    """
    if not name:
        return ""
    s = name.strip().lower()
    s = re.sub(r"\s*\([^)]+\)\s*$", "", s)        # strip "(IRE)" / "(GB)" suffixes
    s = re.sub(r"[^a-z0-9]+", "", s)              # collapse everything else
    return s


def load_csv_names(csv_path: Path) -> list[str]:
    if not csv_path.exists():
        return []
    out = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            n = (row.get("name") or "").strip()
            if n:
                out.append(n)
    return out


def audit_one(farm_id: str, farm_cfg: dict) -> dict:
    csv_path = HERE / farm_cfg["csv"]
    csv_names = load_csv_names(csv_path)
    expected = farm_cfg.get("expected_names") or []

    csv_norm = {normalize_name(n): n for n in csv_names}
    exp_norm = {normalize_name(n): n for n in expected}

    missing = sorted(exp_norm[k] for k in exp_norm.keys() - csv_norm.keys())  # in expected, not in CSV
    extras  = sorted(csv_norm[k] for k in csv_norm.keys() - exp_norm.keys())  # in CSV, not in expected

    return {
        "farm_id":    farm_id,
        "name":       farm_cfg.get("name"),
        "csv_count":  len(csv_names),
        "expected_n": len(expected),
        "missing":    missing,   # likely roster gaps to add
        "extras":     extras,    # likely pensioned / moved / deceased
    }


def print_report(report: dict, *, show_extras: bool = True) -> None:
    fid = report["farm_id"]
    name = report["name"]
    n_csv = report["csv_count"]
    n_exp = report["expected_n"]

    if n_exp == 0:
        # No expected list populated yet — silently note it
        print(f"  [{fid:14s}] {name:32s}  CSV={n_csv:>3d}  expected=(empty — populate first)")
        return

    print(f"\n=== [{fid}] {name} ===")
    print(f"  CSV has {n_csv} stallion(s); expected list has {n_exp}")
    if report["missing"]:
        print(f"  MISSING from CSV (potential gaps to add — verify each on BH register first):")
        for n in report["missing"]:
            print(f"    + {n}")
    else:
        print(f"  No missing — CSV covers everything in expected list.")
    if show_extras and report["extras"]:
        print(f"  EXTRA in CSV (in our data but not on the 2026 announcement — verify status):")
        for n in report["extras"]:
            print(f"    - {n}")


def main():
    ap = argparse.ArgumentParser(description="Audit per-farm CSV vs published 2026 rosters")
    ap.add_argument("--farm", help="Limit to a single farm id (e.g. spendthrift)")
    ap.add_argument("--no-extras", action="store_true", help="Hide the 'extra in CSV' section")
    args = ap.parse_args()

    if not CONFIG_JSON.exists():
        raise SystemExit(f"Missing {CONFIG_JSON.name}. See expected-rosters-2026.json template.")

    config = json.loads(CONFIG_JSON.read_text())
    farms_cfg = config.get("farms") or {}
    if args.farm:
        if args.farm not in farms_cfg:
            raise SystemExit(f"Farm '{args.farm}' not in config. Known: {sorted(farms_cfg)}")
        farms_cfg = {args.farm: farms_cfg[args.farm]}

    print(f"Roster audit — {len(farms_cfg)} farm(s)\n")
    summary_rows = []
    populated = 0
    for fid, fcfg in farms_cfg.items():
        rep = audit_one(fid, fcfg)
        if rep["expected_n"] > 0:
            populated += 1
        summary_rows.append(rep)

    if populated == 0:
        # Compact summary mode — no expected lists populated yet
        print("Status of each farm's expected_names list:")
        for r in summary_rows:
            print_report(r, show_extras=not args.no_extras)
        print("\nTo run a real audit:")
        print("  1. Open the announcement_urls in expected-rosters-2026.json")
        print("  2. Copy the 2026 stallion names into the matching expected_names[]")
        print("  3. Re-run this script")
        return

    # Real audit output for any farms with populated lists
    for r in summary_rows:
        print_report(r, show_extras=not args.no_extras)

    print(f"\nTotals: {populated}/{len(summary_rows)} farms audited")


if __name__ == "__main__":
    main()
