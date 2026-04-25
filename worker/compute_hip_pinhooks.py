"""Hip-level pinhook tracking — match the same horse across yearling and 2YO sales.

For every horse sold as a yearling in year Y and as a 2YO in year Y+1, we
identify it as the SAME individual via (sire, dam) fingerprint within the
cohort, compute the realized pinhook return, and aggregate per stallion.

Cohort logic:
  A foal born in year F:
    - sold as yearling in calendar year F+1 (Kee Sept, FT Saratoga, FT Oct,
      FT Midlantic, OBS Oct, etc.)
    - sold as 2YO in calendar year F+2 (OBS March/Spring/June, FT March, etc.)

So we pair yearling-sale year Y with 2YO-sale year Y+1. The (sire, dam)
fingerprint within that cohort is overwhelmingly unique — collisions would
require a stallion to have two foals out of the same dam in the same crop,
which is biologically impossible (mares only produce one foal per year).

Output:
  hip-pinhooks.json — { by_stallion: { name: { matches: [...], summary: {...} } } }
    matches[i] = {
        horse_name, dam, cohort, yearling_sale, yearling_price, yearling_hip,
        twoyo_sale, twoyo_price, twoyo_hip, return_pct, gross_return_usd,
    }
    summary = {
        matched_pairs, median_return_pct, mean_return_pct,
        positive_return_pct (% of pairs that pinhooked profitably),
        median_yearling_price, median_twoyo_price,
    }

Run after fetch_sales.py. Reads worker/recent-sale-results-{year}.json.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from statistics import median, mean

HERE = Path(__file__).parent
OUTPUT_JSON = HERE / "hip-pinhooks.json"


def normalize_dam(dam: str) -> str:
    """Normalize a dam name for cross-sale matching. Lowercase, strip country
    suffix, collapse whitespace and most punctuation."""
    if not dam:
        return ""
    s = dam.lower().strip()
    s = re.sub(r"\s*\([^)]+\)\s*$", "", s)   # strip trailing "(IRE)" etc.
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def normalize_sire(sire: str) -> str:
    if not sire:
        return ""
    s = sire.lower().strip()
    s = re.sub(r"\s*\([^)]+\)\s*$", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def classify_sale(sale_name: str) -> str:
    """Returns 'yearling', 'twoyo', or 'other' for a BH sale name. The
    yearling/2YO classification matters because pinhook matching only pairs
    yearling sales (sale year Y) with 2YO sales (sale year Y+1) of the same
    cohort."""
    s = sale_name.lower()
    if "2yo" in s or "two-year-old" in s or "in training" in s:
        return "twoyo"
    if "yearling" in s:
        return "yearling"
    return "other"


def sale_year(sale_name: str) -> int | None:
    """Extract the 4-digit year that appears at the end of a BH sale name."""
    m = re.search(r"\b(20\d{2})\b\s*$", sale_name.strip())
    return int(m.group(1)) if m else None


def load_year_file(year: int) -> dict:
    p = HERE / f"recent-sale-results-{year}.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text()).get("by_sire", {})
    except Exception:
        return {}


def main():
    # Load every year file we have
    years_present = []
    by_year_by_sire: dict[int, dict[str, list[dict]]] = {}
    for y in range(2020, 2030):
        bs = load_year_file(y)
        if bs:
            years_present.append(y)
            by_year_by_sire[y] = bs
    print(f"Loaded year files: {years_present}")
    if len(years_present) < 2:
        print("Need at least 2 years of data to match pinhook pairs.")
        return

    # Index every hip by (sire_norm, dam_norm, sale_year, kind, sale_name).
    # We only need yearling and 2YO hips that sold for a real price.
    # yearlings_by_cohort[(sire_norm, dam_norm, foal_year)] = [hip_record, ...]
    # twoyo_by_cohort[same key] = [hip_record, ...]
    yearlings_by_key: dict[tuple, list[dict]] = {}
    twoyos_by_key:    dict[tuple, list[dict]] = {}

    total_yearling_hips = 0
    total_twoyo_hips = 0

    for y, bs in by_year_by_sire.items():
        for stallion_name, hips in bs.items():
            for h in hips:
                if h.get("sold_price_usd") is None:
                    continue   # RNA / not sold
                sale = h.get("sale_name") or ""
                kind = classify_sale(sale)
                if kind == "other":
                    continue
                sy = sale_year(sale)
                if sy is None:
                    continue
                # Foal year = sale year - age. Yearling = sold year-1 born,
                # 2YO = sold year-2 born. So:
                #   yearling sold in Y → foal of Y-1
                #   2YO     sold in Y → foal of Y-2
                if kind == "yearling":
                    foal_year = sy - 1
                else:
                    foal_year = sy - 2

                sire_n = normalize_sire(stallion_name)
                dam_n = normalize_dam(h.get("dam") or "")
                if not sire_n or not dam_n:
                    continue
                key = (sire_n, dam_n, foal_year)

                rec = {
                    "stallion_name": stallion_name,
                    "horse_name":    h.get("horse_name"),
                    "dam":           h.get("dam"),
                    "sale_name":     sale,
                    "sale_year":     sy,
                    "hip":           h.get("hip"),
                    "price":         h["sold_price_usd"],
                    "consignor":     h.get("consignor"),
                }
                if kind == "yearling":
                    yearlings_by_key.setdefault(key, []).append(rec)
                    total_yearling_hips += 1
                else:
                    twoyos_by_key.setdefault(key, []).append(rec)
                    total_twoyo_hips += 1

    print(f"  {total_yearling_hips:,} yearling hips, {total_twoyo_hips:,} 2YO hips indexed")

    # Match: for every (sire, dam, foal_year) key that exists in BOTH the
    # yearling and 2YO indexes, we have a pinhook pair. If a key has multiple
    # entries on either side, keep the priciest pairing (likely the canonical
    # public sale; smaller online auctions for the same horse exist but a
    # pinhook return is best characterized by the principal transactions).
    matches_by_stallion: dict[str, list[dict]] = {}
    for key, yrls in yearlings_by_key.items():
        twos = twoyos_by_key.get(key)
        if not twos:
            continue
        yrls_sorted = sorted(yrls, key=lambda x: -x["price"])
        twos_sorted = sorted(twos, key=lambda x: -x["price"])
        yrl = yrls_sorted[0]
        two = twos_sorted[0]
        return_pct = (two["price"] - yrl["price"]) / yrl["price"] * 100
        gross = two["price"] - yrl["price"]
        match = {
            "horse_name":     two["horse_name"] or yrl["horse_name"],
            "dam":            two["dam"] or yrl["dam"],
            "cohort":         f"{key[2]} foals",
            "yearling_sale":  yrl["sale_name"],
            "yearling_hip":   yrl["hip"],
            "yearling_price": yrl["price"],
            "yearling_consignor": yrl.get("consignor"),
            "twoyo_sale":     two["sale_name"],
            "twoyo_hip":      two["hip"],
            "twoyo_price":    two["price"],
            "twoyo_consignor": two.get("consignor"),
            "return_pct":     round(return_pct, 1),
            "gross_return_usd": gross,
        }
        # Stallion is shared between yrl and two (same sire); use the
        # canonical roster name (whichever has more punctuation preserved).
        stallion = yrl["stallion_name"]
        matches_by_stallion.setdefault(stallion, []).append(match)

    # Compute per-stallion summary
    out_by_stallion: dict[str, dict] = {}
    for stallion, matches in matches_by_stallion.items():
        if not matches:
            continue
        returns = [m["return_pct"] for m in matches]
        yrl_prices = [m["yearling_price"] for m in matches]
        two_prices = [m["twoyo_price"] for m in matches]
        positive = [r for r in returns if r > 0]
        # Sort matches by return_pct desc for display
        matches.sort(key=lambda m: -m["return_pct"])
        out_by_stallion[stallion] = {
            "matches": matches,
            "summary": {
                "matched_pairs":         len(matches),
                "median_return_pct":     round(median(returns), 1),
                "mean_return_pct":       round(mean(returns), 1),
                "positive_return_pct":   round(100 * len(positive) / len(matches), 1),
                "median_yearling_price": int(median(yrl_prices)),
                "median_twoyo_price":    int(median(two_prices)),
            },
        }

    # Top matches across the whole dataset (by return_pct, with min yearling price)
    all_matches = []
    for stallion, blob in out_by_stallion.items():
        for m in blob["matches"]:
            if m["yearling_price"] >= 25_000:   # filter noise — matches under $25K skew %s
                all_matches.append({**m, "stallion": stallion})
    all_matches.sort(key=lambda m: -m["return_pct"])
    top_overall = all_matches[:50]

    output = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "method": "cohort-fingerprint-by-sire-dam-foalyear",
        "summary": {
            "stallions_with_pairs": len(out_by_stallion),
            "total_matched_pairs":  sum(len(b["matches"]) for b in out_by_stallion.values()),
            "yearling_hips_indexed": total_yearling_hips,
            "twoyo_hips_indexed":    total_twoyo_hips,
        },
        "by_stallion": out_by_stallion,
        "top_returns_overall": top_overall,
    }
    OUTPUT_JSON.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    print(
        f"Wrote {OUTPUT_JSON.name}: "
        f"{output['summary']['stallions_with_pairs']} stallions, "
        f"{output['summary']['total_matched_pairs']} matched pinhook pairs"
    )
    print()
    print("Top 10 realized pinhook returns:")
    for m in top_overall[:10]:
        print(f"  {m['return_pct']:+7.1f}%  {m['stallion']:25s}  "
              f"${m['yearling_price']:>9,} → ${m['twoyo_price']:>10,}  "
              f"({m['horse_name']}, out of {m['dam']})")


if __name__ == "__main__":
    main()
