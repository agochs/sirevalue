"""Aggregate hip-pinhook pairs by consignor.

Reads hip-pinhooks.json (produced by compute_hip_pinhooks.py) and bins each
matched pair under both:
  - the yearling consignor (who sold the horse as a yearling)
  - the 2YO consignor (who sold the horse as a 2YO)

For each consignor we compute summary stats over their pinhook pairs:
  matched_pairs, median_return_pct, mean_return_pct, positive_return_pct,
  median_yearling_price, median_twoyo_price, top_partners (top counterparties).

The two perspectives serve different audiences:
  - yearling-consignor leaderboard: which farms sell yearlings that turn out
    to pinhook well? Useful to a pinhooker at the BUY side of a yearling sale.
  - 2YO-consignor leaderboard: which 2YO consignors are good at adding value
    through training and resale? Useful to anyone evaluating consignment
    options or watching who reliably profits.

Output:
  consignor-pinhooks.json
    {
      generated_at,
      summary: { yearling_consignor_count, twoyo_consignor_count,
                 total_matched_pairs },
      yearling_consignors: { name: { matches: [...], summary: {...} } },
      twoyo_consignors:    { name: { matches: [...], summary: {...} } }
    }

Run AFTER compute_hip_pinhooks.py.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from statistics import median, mean

HERE = Path(__file__).parent
INPUT_JSON  = HERE / "hip-pinhooks.json"
OUTPUT_JSON = HERE / "consignor-pinhooks.json"


def normalize_consignor(name: str) -> str:
    """Lightly normalize consignor names so trivial variants collapse.

    Strips trailing ', agent' / ', agent for X' / 'LLC' / 'Inc' / period
    fragments and collapses whitespace. Preserves the canonical readable
    name for display."""
    if not name:
        return ""
    s = name.strip()
    # Strip ", Agent" / ", Agent for ..." / " , for ..." (keep the principal)
    s = re.sub(r"\s*,\s*agent(\s+for\s+.+)?$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*,\s*for\s+.+$", "", s, flags=re.IGNORECASE)
    # Common corp suffixes
    s = re.sub(r",?\s*\b(LLC|Inc\.?|Ltd\.?|Co\.?)\b\.?$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.rstrip(",")
    return s


def aggregate(pairs: list[dict], side: str) -> dict:
    """side = 'yearling' or 'twoyo'. Returns the per-consignor blob."""
    by_consignor: dict[str, list[dict]] = {}
    consignor_field = f"{side}_consignor"
    partner_field   = "twoyo_consignor" if side == "yearling" else "yearling_consignor"

    for p in pairs:
        c = normalize_consignor(p.get(consignor_field) or "")
        if not c:
            continue
        by_consignor.setdefault(c, []).append(p)

    out = {}
    for consignor, matches in by_consignor.items():
        if len(matches) < 3:
            # Drop consignors with sub-3 sample — too noisy to profile
            continue
        returns    = [m["return_pct"] for m in matches]
        yrl_prices = [m["yearling_price"] for m in matches]
        two_prices = [m["twoyo_price"] for m in matches]
        positive   = [r for r in returns if r > 0]

        # Top 5 counterparties (other side of the trade)
        partners = Counter()
        for m in matches:
            partner = normalize_consignor(m.get(partner_field) or "")
            if partner:
                partners[partner] += 1
        top_partners = [{"name": p, "count": c} for p, c in partners.most_common(5)]

        # Trim per-pair display payload (UI shows top 10 sorted by return)
        matches_sorted = sorted(matches, key=lambda m: -m["return_pct"])

        out[consignor] = {
            "matches": matches_sorted,
            "summary": {
                "matched_pairs":         len(matches),
                "median_return_pct":     round(median(returns), 1),
                "mean_return_pct":       round(mean(returns), 1),
                "positive_return_pct":   round(100 * len(positive) / len(matches), 1),
                "median_yearling_price": int(median(yrl_prices)),
                "median_twoyo_price":    int(median(two_prices)),
                "top_partners":          top_partners,
            },
        }
    return out


def main():
    if not INPUT_JSON.exists():
        print(f"Input {INPUT_JSON} missing — run compute_hip_pinhooks.py first.")
        return

    src = json.loads(INPUT_JSON.read_text())
    by_stallion = src.get("by_stallion") or {}

    # Flatten every match across stallions, keeping stallion attribution
    # so the UI can render which stallion each pair came from.
    all_pairs: list[dict] = []
    for stallion, blob in by_stallion.items():
        for m in blob.get("matches", []):
            mm = dict(m)
            mm["stallion"] = stallion
            all_pairs.append(mm)

    print(f"Loaded {len(all_pairs):,} matched pairs from {len(by_stallion):,} stallions")

    yrl_consignors  = aggregate(all_pairs, "yearling")
    two_consignors  = aggregate(all_pairs, "twoyo")

    out = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "method": "by-consignor-aggregation-from-hip-pinhooks",
        "summary": {
            "yearling_consignor_count": len(yrl_consignors),
            "twoyo_consignor_count":    len(two_consignors),
            "total_matched_pairs":      len(all_pairs),
        },
        "yearling_consignors": yrl_consignors,
        "twoyo_consignors":    two_consignors,
    }
    OUTPUT_JSON.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print(f"Wrote {OUTPUT_JSON.name}: "
          f"{len(yrl_consignors)} yearling consignors, "
          f"{len(two_consignors)} 2YO consignors profiled")

    # Quick top-10 snapshot for human verification
    print()
    print("Top 10 yearling consignors by median pinhook return (n>=10):")
    yrl_top = sorted(
        [(c, b['summary']) for c, b in yrl_consignors.items()
         if b['summary']['matched_pairs'] >= 10],
        key=lambda x: -x[1]['median_return_pct']
    )[:10]
    for c, s in yrl_top:
        print(f"  {s['median_return_pct']:+6.1f}%  {c[:35]:35s}  "
              f"n={s['matched_pairs']:3d}  profitable {s['positive_return_pct']:5.1f}%")

    print()
    print("Top 10 2YO consignors by median pinhook return (n>=10):")
    two_top = sorted(
        [(c, b['summary']) for c, b in two_consignors.items()
         if b['summary']['matched_pairs'] >= 10],
        key=lambda x: -x[1]['median_return_pct']
    )[:10]
    for c, s in two_top:
        print(f"  {s['median_return_pct']:+6.1f}%  {c[:35]:35s}  "
              f"n={s['matched_pairs']:3d}  profitable {s['positive_return_pct']:5.1f}%")


if __name__ == "__main__":
    main()
