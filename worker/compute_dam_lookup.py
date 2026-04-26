"""Aggregate every sale appearance + pinhook pair by dam name.

For breeders evaluating a particular female family, this is the surface that
answers "show me everything we know about this mare." For a given dam:
  - every progeny we've seen at a public sale (yearling, 2YO, broodmare)
  - any matched pinhook pairs (same horse sold twice)
  - summary: # foals seen, # sold, total gross, top hammer

Output:
  dam-lookup.json
    {
      generated_at,
      summary: { total_dams, dams_with_pairs, total_appearances },
      by_dam: {
        "Heavenly Love": {
          canonical_name, damsire, total_appearances, sold_count, top_price, total_gross,
          progeny: [
            { foal_year, sire, horse_name, sale_name, sale_url, hip, price, status }, ...
          ],
          matched_pairs: [...],   # subset from hip-pinhooks where this dam appears
        }
      }
    }

Run AFTER fetch_sales.py + compute_hip_pinhooks.py.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).parent
PINHOOKS_JSON = HERE / "hip-pinhooks.json"
INDEX_JSON    = HERE / "dam-lookup-index.json"

# Skip dams whose normalized form is shorter than this. Real thoroughbred dam
# names are reliably ≥4 characters; 2-3 char "names" are virtually always
# truncation artifacts that lump unrelated horses together (e.g. raw='Ba').
MIN_NORM_LEN = 4


def normalize_dam(name: str) -> str:
    """Same normalization compute_hip_pinhooks uses — strip country suffix,
    lowercase, drop punctuation. So 'Heavenly Love' and 'Heavenly Love (USA)'
    fold together."""
    if not name:
        return ""
    s = name.lower().strip()
    s = re.sub(r"\s*\([^)]+\)\s*$", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def sale_year(sale_name: str) -> int | None:
    m = re.search(r"\b(20\d{2})\b\s*$", (sale_name or "").strip())
    return int(m.group(1)) if m else None


def classify_sale(sale_name: str) -> str:
    s = (sale_name or "").lower()
    if "2yo" in s or "two-year-old" in s or "in training" in s: return "twoyo"
    if "yearling" in s: return "yearling"
    if "breeding stock" in s: return "broodmare"
    return "other"


def main():
    # Walk every recent-sale-results-{year}.json and aggregate by dam.
    by_dam: dict[str, dict] = {}
    canonical_count: dict[str, Counter] = {}    # norm -> Counter of raw spellings
    damsire_count: dict[str, Counter] = {}      # norm -> Counter of damsire names

    total_appearances = 0
    for p in sorted(HERE.glob("recent-sale-results-*.json")):
        if p.name == "recent-sale-results-index.json":
            continue
        try:
            data = json.loads(p.read_text())
        except Exception:
            continue
        for sire, hips in (data.get("by_sire") or {}).items():
            for h in hips:
                dam_raw = h.get("dam") or ""
                norm = normalize_dam(dam_raw)
                if len(norm) < MIN_NORM_LEN:
                    continue
                # Track the canonical (most common) spelling and damsire
                canonical_count.setdefault(norm, Counter())[dam_raw.strip()] += 1
                if h.get("damsire"):
                    damsire_count.setdefault(norm, Counter())[h["damsire"].strip()] += 1

                kind = classify_sale(h.get("sale_name") or "")
                sy = sale_year(h.get("sale_name") or "")
                # Foal year inference: yearling sold in Y → foal of Y-1; 2YO of Y-2.
                # For broodmare/other sales we don't know the foal year cleanly.
                if kind == "yearling":
                    foal_year = sy - 1 if sy else None
                elif kind == "twoyo":
                    foal_year = sy - 2 if sy else None
                else:
                    foal_year = None

                rec = {
                    "foal_year":  foal_year,
                    "sire":       sire,
                    "horse_name": h.get("horse_name"),
                    "sale_name":  h.get("sale_name"),
                    "sale_url":   h.get("sale_url"),
                    "hip":        h.get("hip"),
                    "price":      h.get("sold_price_usd"),
                    "status":     h.get("status"),
                    "consignor":  h.get("consignor"),
                    "kind":       kind,
                }
                blob = by_dam.setdefault(norm, {"progeny": [], "matched_pairs": []})
                blob["progeny"].append(rec)
                total_appearances += 1

    # Pull matched pinhook pairs from hip-pinhooks.json and bucket by dam
    if PINHOOKS_JSON.exists():
        pin = json.loads(PINHOOKS_JSON.read_text())
        for stallion, b in (pin.get("by_stallion") or {}).items():
            for m in b.get("matches") or []:
                norm = normalize_dam(m.get("dam") or "")
                if not norm or norm not in by_dam:
                    continue
                # Slim payload — keep just what's needed for display
                by_dam[norm]["matched_pairs"].append({
                    "stallion":        stallion,
                    "horse_name":      m.get("horse_name"),
                    "cohort":          m.get("cohort"),
                    "yearling_price":  m.get("yearling_price"),
                    "twoyo_price":     m.get("twoyo_price"),
                    "return_pct":      m.get("return_pct"),
                    "yearling_sale":   m.get("yearling_sale"),
                    "twoyo_sale":      m.get("twoyo_sale"),
                    "yearling_hip":    m.get("yearling_hip"),
                    "twoyo_hip":       m.get("twoyo_hip"),
                    "quality_flags":   m.get("quality_flags") or [],
                })

    # Compute per-dam summary + finalize canonical name + damsire
    out_by_dam: dict[str, dict] = {}
    dams_with_pairs = 0
    for norm, blob in by_dam.items():
        canonical = canonical_count[norm].most_common(1)[0][0] if canonical_count.get(norm) else norm
        damsire   = damsire_count[norm].most_common(1)[0][0] if damsire_count.get(norm) else None
        progeny = blob["progeny"]
        sold = [p for p in progeny if p.get("price") is not None]
        prices = [p["price"] for p in sold]
        # Sort progeny: most recent first, then by price desc
        progeny.sort(key=lambda p: (-(p.get("foal_year") or 0), -(p.get("price") or 0)))
        # Sort matched pairs by return desc
        blob["matched_pairs"].sort(key=lambda m: -(m.get("return_pct") or 0))

        if blob["matched_pairs"]:
            dams_with_pairs += 1

        # Distinct foal-year-sire pairs gives a rough "# foals seen" count
        # (a horse can appear in multiple sales — yearling AND 2YO — so we
        # dedupe by (foal_year, sire) which fingerprints the horse).
        seen = set()
        for p in progeny:
            seen.add((p.get("foal_year"), p.get("sire")))
        distinct_foals = len([s for s in seen if s[0] is not None])

        out_by_dam[canonical] = {
            "canonical_name":     canonical,
            "damsire":            damsire,
            "total_appearances":  len(progeny),
            "distinct_foals":     distinct_foals,
            "sold_count":         len(sold),
            "top_price":          max(prices) if prices else 0,
            "total_gross":        sum(prices),
            "progeny":            progeny,
            "matched_pairs":      blob["matched_pairs"],
        }

    # ---- Output split: compact index + per-shard full data --------------
    # Index has just enough info for client-side autocomplete + ranking
    # without forcing a 20MB download. Each dam record in the index is ~100B.
    # Per-shard full data files are loaded lazily when the user picks a dam.
    def shard_key(name: str) -> str:
        first = (name or "?").lstrip()[:1].lower()
        return first if first.isalpha() else "0"   # group all non-alpha into '0'

    # Wipe stale per-shard files (best-effort; ignore permission errors as score_catalog does)
    for old in HERE.glob("dam-lookup-shard-*.json"):
        try:
            old.unlink()
        except (PermissionError, OSError):
            pass

    by_shard: dict[str, dict[str, dict]] = {}
    index_entries = []
    for canonical, blob in out_by_dam.items():
        shard = shard_key(canonical)
        by_shard.setdefault(shard, {})[canonical] = blob
        index_entries.append({
            "name":              canonical,
            "shard":             shard,
            "damsire":           blob["damsire"],
            "distinct_foals":    blob["distinct_foals"],
            "sold_count":        blob["sold_count"],
            "top_price":         blob["top_price"],
            "total_gross":       blob["total_gross"],
            "matched_pairs_n":   len(blob["matched_pairs"]),
        })

    # Sort index by total_gross desc — most prolific/valuable dams surface first
    index_entries.sort(key=lambda d: -d["total_gross"])

    INDEX_JSON.write_text(json.dumps({
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "method":       "aggregate-by-normalized-dam-name",
        "summary": {
            "total_dams":        len(out_by_dam),
            "dams_with_pairs":   dams_with_pairs,
            "total_appearances": total_appearances,
            "shards":            sorted(by_shard.keys()),
        },
        "dams": index_entries,
    }, indent=2, ensure_ascii=False))

    for shard, dams in by_shard.items():
        path = HERE / f"dam-lookup-shard-{shard}.json"
        path.write_text(json.dumps({"shard": shard, "by_dam": dams}, indent=2, ensure_ascii=False))

    idx_kb = INDEX_JSON.stat().st_size / 1024
    total_shard_mb = sum((HERE / f"dam-lookup-shard-{s}.json").stat().st_size for s in by_shard) / (1024*1024)
    print(f"Wrote {INDEX_JSON.name} ({idx_kb:.0f} KB) + {len(by_shard)} per-letter shards "
          f"(total {total_shard_mb:.1f} MB across all shards)")
    print(f"Dams: {len(out_by_dam):,}  · Appearances: {total_appearances:,}  "
          f"· With pinhook pairs: {dams_with_pairs:,}")
    print()
    # Spot-check: most prolific dams (most distinct foals seen)
    top = sorted(out_by_dam.values(), key=lambda d: -d["distinct_foals"])[:8]
    print("Most-represented dams (distinct foals across all sales):")
    for d in top:
        print(f"  {d['canonical_name']:30s}  foals={d['distinct_foals']:>2d}  "
              f"sold={d['sold_count']:>2d}  top=${d['top_price']:>9,}  "
              f"pairs={len(d['matched_pairs'])}")


if __name__ == "__main__":
    main()
