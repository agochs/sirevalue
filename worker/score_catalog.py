"""Score sale catalogs / results with sire pinhook signal.

For every hip in upcoming catalogs (`upcoming-sales.json`) AND in the most
recent results files (`recent-sale-results-{year}.json`), attach the sire's
realized pinhook track record from `hip-pinhooks.json`:

  - sire_pinhook_pairs        : sample size (N matched pairs across history)
  - sire_pinhook_median_pct   : median % return on prior pinhook pairs
  - sire_pinhook_positive_pct : % of prior pairs that pinhooked profitably
  - sire_pinhook_median_yrl   : median yearling price the sire pinhooks at
  - sire_pinhook_signal       : 'strong' | 'positive' | 'neutral' | 'weak' | 'none'

The signal classification (used to color-code the catalog UI):
  strong    : >= 50% median return AND >= 70% profitable AND N >= 10
  positive  : >= 25% median return AND >= 60% profitable AND N >= 5
  neutral   : in between
  weak      : negative median return AND profitable < 50%
  none      : no matched pairs for this sire (no track record)

We also attach the sire's grade and value score from scores.json for context.

Output: catalog-scoring.json
{
  generated_at,
  sales: [
    { sale_name, kind ('upcoming'|'results'), hip_count, hips: [...annotated...] },
    ...
  ],
  summary: { sales_scored, hips_scored, pinhook_match_rate }
}

Run AFTER fetch_sales.py and compute_hip_pinhooks.py.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).parent
HIP_PINHOOKS_JSON  = HERE / "hip-pinhooks.json"
COST_BASIS_JSON    = HERE / "yearling-cost-basis.json"
UPCOMING_JSON      = HERE / "upcoming-sales.json"
SCORES_JSON        = HERE / "scores.json"
INDEX_JSON         = HERE / "catalog-scoring-index.json"

# Limit catalog hip output payload — we keep at most this many hips per sale
# in the JSON pushed to the static site (sorted by sire's pinhook signal).
# This keeps the page snappy even for 4,000-hip catalogs like Keeneland Sept.
HIPS_PER_SALE_CAP = 4000


def normalize_sire(name: str) -> str:
    if not name:
        return ""
    s = name.lower().strip()
    s = re.sub(r"\s*\([^)]+\)\s*$", "", s)   # strip trailing "(IRE)" etc.
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def normalize_dam(name: str) -> str:
    """Same scheme as compute_hip_pinhooks.normalize_dam — must stay aligned."""
    if not name:
        return ""
    s = name.lower().strip()
    s = re.sub(r"\s*\([^)]+\)\s*$", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def classify_kind(sale_name: str) -> str:
    """Returns 'yearling' / 'twoyo' / 'other' from a BH sale name. Mirrors
    compute_hip_pinhooks.classify_sale so the cost-basis lookup uses the
    exact same cohort keys."""
    s = (sale_name or "").lower()
    if "2yo" in s or "two-year-old" in s or "in training" in s:
        return "twoyo"
    if "yearling" in s:
        return "yearling"
    return "other"


def sale_year(sale_name: str) -> int | None:
    m = re.search(r"\b(20\d{2})\b\s*$", (sale_name or "").strip())
    return int(m.group(1)) if m else None


def classify_signal(s: dict | None) -> str:
    """Bucket the sire's pinhook track record into a signal tier."""
    if not s or not s.get("matched_pairs"):
        return "none"
    n     = s["matched_pairs"]
    med   = s.get("median_return_pct") or 0
    pos   = s.get("positive_return_pct") or 0
    if n >= 10 and med >= 50 and pos >= 70:
        return "strong"
    if n >= 5 and med >= 25 and pos >= 60:
        return "positive"
    if med < 0 and pos < 50:
        return "weak"
    return "neutral"


def load_sire_signals() -> dict:
    """Map normalized sire name → pinhook signal blob (or empty if missing)."""
    if not HIP_PINHOOKS_JSON.exists():
        return {}
    data = json.loads(HIP_PINHOOKS_JSON.read_text())
    by_stallion = data.get("by_stallion", {})
    out = {}
    for name, blob in by_stallion.items():
        s = blob.get("summary", {})
        norm = normalize_sire(name)
        if not norm:
            continue
        out[norm] = {
            "stallion_canonical":  name,
            "matched_pairs":       s.get("matched_pairs"),
            "median_return_pct":   s.get("median_return_pct"),
            "positive_return_pct": s.get("positive_return_pct"),
            "median_yearling_price": s.get("median_yearling_price"),
            "median_twoyo_price":  s.get("median_twoyo_price"),
        }
    return out


def load_cost_basis() -> dict:
    """Map (sire_norm, dam_norm, foal_year) -> yearling cost record."""
    if not COST_BASIS_JSON.exists():
        return {}
    data = json.loads(COST_BASIS_JSON.read_text())
    return data.get("by_key") or {}


def load_score_index() -> dict:
    """Map normalized sire name → { value, grade, fee_usd } from scores.json."""
    if not SCORES_JSON.exists():
        return {}
    data = json.loads(SCORES_JSON.read_text())
    out = {}
    for s in data.get("stallions", []):
        norm = normalize_sire(s.get("name") or "")
        if not norm:
            continue
        sc = s.get("score") or {}
        out[norm] = {
            "value":    sc.get("value"),
            "grade":    sc.get("grade"),
            "fee_usd":  s.get("fee_usd"),
        }
    return out


def annotate_hip(hip: dict, sale_name: str, sire_signals: dict,
                 scores_idx: dict, cost_basis: dict) -> dict:
    """Return a copy of `hip` with sire pinhook signal + score + (for 2YOs)
    yearling cost-basis attached."""
    sire = hip.get("sire") or hip.get("stallion_name") or ""
    norm = normalize_sire(sire)
    sig  = sire_signals.get(norm)
    sc   = scores_idx.get(norm)

    # Build a slimmed-down hip record — drop verbose fields we don't need
    # client-side, keep only what the catalog UI actually displays + sorts.
    out = {
        "hip":         hip.get("hip"),
        "horse_name":  hip.get("horse_name"),
        "sire":        sire,
        "dam":         hip.get("dam"),
        "consignor":   hip.get("consignor"),
    }
    # Sale-result-only fields (skipped if absent for catalog hips)
    if hip.get("sold_price_usd") is not None:
        out["sold_price_usd"] = hip["sold_price_usd"]
    if sig:
        out["sire_pinhook_pairs"]        = sig["matched_pairs"]
        out["sire_pinhook_median_pct"]   = sig["median_return_pct"]
        out["sire_pinhook_positive_pct"] = sig["positive_return_pct"]
        out["sire_pinhook_median_yrl"]   = sig["median_yearling_price"]
    out["sire_pinhook_signal"] = classify_signal(sig)
    if sc:
        out["sire_value_score"] = sc.get("value")
        out["sire_grade"]       = sc.get("grade")
        out["sire_fee_usd"]     = sc.get("fee_usd")

    # Cost-basis lookup: only for 2YO sales. The cohort key is
    # (sire_norm, dam_norm, foal_year). For a 2YO sold/cataloged in year Y,
    # the foal year is Y-2. The yearling sale would have happened in Y-1.
    kind = classify_kind(sale_name)
    if kind == "twoyo":
        sy = sale_year(sale_name)
        dam_norm = normalize_dam(hip.get("dam") or "")
        if sy and dam_norm and norm:
            foal_year = sy - 2
            key = f"{norm}|{dam_norm}|{foal_year}"
            cb = cost_basis.get(key)
            if cb and cb.get("yearling_price"):
                out["yearling_cost_usd"]      = cb["yearling_price"]
                out["yearling_cost_sale"]     = cb.get("yearling_sale")
                out["yearling_cost_hip"]      = cb.get("yearling_hip")
                out["yearling_cost_consignor"] = cb.get("yearling_consignor")
                # If we ALSO have this 2YO's sold price, compute realized return.
                if hip.get("sold_price_usd") is not None:
                    p = cb["yearling_price"]
                    out["realized_return_pct"] = round(
                        (hip["sold_price_usd"] - p) / p * 100, 1
                    )
    return out


def collect_hips_by_sale(sources: list[tuple[str, str]]) -> dict[str, dict]:
    """sources is a list of (kind, json_path) where kind is 'upcoming' or
    'results'. Returns: { sale_name: { kind, hips: [...] } } merged across
    all sources. Hips are stored with their stallion attribution from the
    by_sire structure flattened up onto the hip record."""
    sales: dict[str, dict] = {}
    for kind, path in sources:
        p = Path(path)
        if not p.exists():
            continue
        try:
            data = json.loads(p.read_text())
        except Exception:
            continue
        by_sire = data.get("by_sire", {})
        for sire_name, hips in by_sire.items():
            for h in hips:
                sale = h.get("sale_name") or "(unknown)"
                sales.setdefault(sale, {"kind": kind, "hips": []})
                # 'kind' from the FIRST source wins (upcoming-sales.json is
                # listed first below so a sale that's both in upcoming AND
                # results is treated as upcoming — i.e. catalog mode).
                rec = dict(h)
                rec["sire"] = rec.get("sire") or sire_name
                sales[sale]["hips"].append(rec)
    return sales


def main():
    if not HIP_PINHOOKS_JSON.exists():
        print(f"Missing {HIP_PINHOOKS_JSON.name} — run compute_hip_pinhooks.py first.")
        return

    sire_signals = load_sire_signals()
    scores_idx   = load_score_index()
    cost_basis   = load_cost_basis()
    print(f"Loaded {len(sire_signals)} sires with pinhook track record, "
          f"{len(scores_idx)} sires with value score, "
          f"{len(cost_basis):,} cohort keys for cost-basis lookup")

    # Sources: upcoming-sales.json (catalog-only hips) + every recent-sale-
    # results-{year}.json. Upcoming first so it 'wins' if a sale appears in
    # both (catalogs sometimes get partial results posted mid-sale).
    sources = [("upcoming", str(UPCOMING_JSON))]
    for p in sorted(HERE.glob("recent-sale-results-*.json")):
        if p.name == "recent-sale-results-index.json":
            continue
        sources.append(("results", str(p)))

    sales_map = collect_hips_by_sale(sources)
    if not sales_map:
        print("No sale data found in any source — nothing to score.")
        OUTPUT_JSON.write_text(json.dumps({
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "sales": [],
            "summary": {"sales_scored": 0, "hips_scored": 0, "pinhook_match_rate": 0.0},
        }, indent=2))
        return

    # Score every hip; also count match rate (how often we have a pinhook
    # signal for the sire). Drop sales with no priced + named hips for the
    # 'results' view (mostly digital duplicates).
    scored_sales = []
    total_hips = 0
    matched_with_signal = 0
    for sale_name in sorted(sales_map.keys()):
        blob = sales_map[sale_name]
        annotated = [annotate_hip(h, sale_name, sire_signals, scores_idx, cost_basis) for h in blob["hips"]]
        # Sort: hips with stronger signal first; within same tier, larger
        # sample size first; then by hip number ascending for stable order.
        signal_rank = {"strong": 0, "positive": 1, "neutral": 2, "weak": 3, "none": 4}
        annotated.sort(key=lambda h: (
            signal_rank.get(h.get("sire_pinhook_signal", "none"), 4),
            -(h.get("sire_pinhook_pairs") or 0),
            int(h.get("hip") or 0) if str(h.get("hip") or "").isdigit() else 99999,
        ))
        annotated = annotated[:HIPS_PER_SALE_CAP]
        for h in annotated:
            total_hips += 1
            if h.get("sire_pinhook_signal") not in ("none", None):
                matched_with_signal += 1
        scored_sales.append({
            "sale_name": sale_name,
            "kind":      blob["kind"],
            "hip_count": len(annotated),
            "hips":      annotated,
        })

    # Sort sales: upcoming first, then results in reverse-chronological order
    def sale_year(name: str) -> int:
        m = re.search(r"\b(20\d{2})\b\s*$", name.strip())
        return int(m.group(1)) if m else 0
    scored_sales.sort(key=lambda s: (
        0 if s["kind"] == "upcoming" else 1,
        -sale_year(s["sale_name"]),
        s["sale_name"],
    ))

    summary = {
        "sales_scored":       len(scored_sales),
        "hips_scored":        total_hips,
        "pinhook_match_rate": round(100 * matched_with_signal / max(1, total_hips), 1),
        "sires_with_signal":  len(sire_signals),
    }

    # Slugify sale name for stable per-sale filenames
    def slugify(name: str) -> str:
        s = re.sub(r"[^a-zA-Z0-9]+", "-", name.lower()).strip("-")
        return s[:80]

    # Wipe stale per-sale files from previous runs so we don't accumulate.
    # Tolerate permission errors — if we can't delete a stale file, the new
    # write below will simply overwrite it (slugs are deterministic).
    for old in HERE.glob("catalog-scoring-sale-*.json"):
        try:
            old.unlink()
        except (PermissionError, OSError):
            pass

    # Write one file per sale with the full hip list
    index_sales = []
    for s in scored_sales:
        slug = slugify(s["sale_name"])
        per_sale_file = HERE / f"catalog-scoring-sale-{slug}.json"
        per_sale_file.write_text(json.dumps({
            "sale_name": s["sale_name"],
            "kind":      s["kind"],
            "hips":      s["hips"],
        }, indent=2, ensure_ascii=False))
        # Per-sale signal counts for the index
        sig_counts = {"strong": 0, "positive": 0, "neutral": 0, "weak": 0, "none": 0}
        for h in s["hips"]:
            sig_counts[h.get("sire_pinhook_signal", "none")] += 1
        index_sales.append({
            "sale_name":     s["sale_name"],
            "slug":          slug,
            "kind":          s["kind"],
            "hip_count":     s["hip_count"],
            "signal_counts": sig_counts,
        })

    INDEX_JSON.write_text(json.dumps({
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "method":  "join-sale-hips-with-sire-pinhook-track-record",
        "summary": summary,
        "sales":   index_sales,
    }, indent=2, ensure_ascii=False))
    print(f"Wrote {INDEX_JSON.name} + {len(scored_sales)} per-sale files: "
          f"{summary['hips_scored']:,} hips, "
          f"{summary['pinhook_match_rate']}% with pinhook signal")
    print()
    # Per-sale signal breakdown for spot-checking
    print("Sale breakdown (showing first 10):")
    for s in scored_sales[:10]:
        sig_counts = {"strong": 0, "positive": 0, "neutral": 0, "weak": 0, "none": 0}
        for h in s["hips"]:
            sig_counts[h.get("sire_pinhook_signal", "none")] += 1
        print(f"  [{s['kind']:8s}] {s['sale_name'][:50]:50s}  "
              f"hips={s['hip_count']:4d}  "
              f"strong={sig_counts['strong']:3d}  pos={sig_counts['positive']:3d}  "
              f"weak={sig_counts['weak']:3d}  none={sig_counts['none']:3d}")


if __name__ == "__main__":
    main()
