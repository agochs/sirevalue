"""Post-sale validation: did the predicted pinhook signal actually predict?

For every sale that has priced hips (i.e. completed sale, not pre-sale
catalog), bucket each hip by its predicted signal tier (strong / positive /
neutral / weak / none) and compute the actual outcome stats:
  - sold count, RNA count
  - median + mean sold price
  - total gross
  - (for 2YO sales with cost-basis) realized return % distribution

Then aggregate across all 2YO sales and across all yearling sales separately.
A signal that predicts is one where strong hips really do outsell weak hips
on the same sale, and where strong-signal 2YOs really do generate higher
realized returns than weak-signal ones.

Reads:
  catalog-scoring-index.json          (sale list)
  public/data/catalogs/catalog-scoring-sale-{slug}.json (per-sale hips)
    (or worker/catalog-scoring-sale-{slug}.json — checked first)

Writes:
  validation.json
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from statistics import median, mean

HERE = Path(__file__).parent
INDEX_JSON      = HERE / "catalog-scoring-index.json"
PUBLIC_CATALOGS = HERE.parent / "public" / "data" / "catalogs"
OUTPUT_JSON     = HERE / "validation.json"

SIGNAL_ORDER = ["strong", "positive", "neutral", "weak", "none"]


def is_twoyo(sale_name: str) -> bool:
    s = (sale_name or "").lower()
    return ("2yo" in s) or ("two-year-old" in s) or ("in training" in s)


def is_yearling(sale_name: str) -> bool:
    return "yearling" in (sale_name or "").lower() and not is_twoyo(sale_name)


def stats(prices: list[float]) -> dict:
    if not prices:
        return {"n": 0, "median": None, "mean": None, "total": 0}
    return {
        "n":      len(prices),
        "median": int(median(prices)),
        "mean":   int(mean(prices)),
        "total":  int(sum(prices)),
    }


def returns_stats(returns: list[float]) -> dict:
    if not returns:
        return {"n": 0, "median_pct": None, "mean_pct": None, "positive_pct": None}
    pos = [r for r in returns if r > 0]
    return {
        "n":            len(returns),
        "median_pct":   round(median(returns), 1),
        "mean_pct":     round(mean(returns), 1),
        "positive_pct": round(100 * len(pos) / len(returns), 1),
    }


def load_sale(slug: str) -> dict | None:
    """Try worker/, fall back to public/data/catalogs/."""
    for d in (HERE, PUBLIC_CATALOGS):
        p = d / f"catalog-scoring-sale-{slug}.json"
        if p.exists():
            try:
                return json.loads(p.read_text())
            except Exception:
                pass
    return None


def slice_by_signal(hips: list[dict]) -> dict:
    """Bucket hips by predicted signal, then compute outcome stats."""
    buckets = {sig: [] for sig in SIGNAL_ORDER}
    for h in hips:
        buckets[h.get("sire_pinhook_signal") or "none"].append(h)

    by_signal: dict[str, dict] = {}
    for sig in SIGNAL_ORDER:
        bucket = buckets[sig]
        sold     = [h for h in bucket if h.get("sold_price_usd") is not None]
        rna      = [h for h in bucket if (h.get("status") or "").lower() == "rna"]
        prices   = [h["sold_price_usd"] for h in sold]
        rets_all = [h["realized_return_pct"] for h in sold if h.get("realized_return_pct") is not None]
        by_signal[sig] = {
            "total_hips":  len(bucket),
            "sold_n":      len(sold),
            "rna_n":       len(rna),
            "price":       stats(prices),
            "realized":    returns_stats(rets_all),
        }
    return by_signal


def main():
    if not INDEX_JSON.exists():
        print(f"Missing {INDEX_JSON.name} — run score_catalog.py first.")
        return
    index = json.loads(INDEX_JSON.read_text())
    sales = index.get("sales", [])

    by_sale: dict[str, dict] = {}
    twoyo_pool:    list[dict] = []   # every hip across all 2YO results
    yearling_pool: list[dict] = []   # every hip across all yearling results

    for s in sales:
        # Validation only makes sense for completed sales (we need actual prices)
        if s.get("kind") != "results":
            continue
        slug = s.get("slug")
        sale_name = s.get("sale_name") or ""
        sale_data = load_sale(slug)
        if not sale_data:
            continue
        hips = sale_data.get("hips", [])
        # Need at least some priced hips for validation to be meaningful
        priced = [h for h in hips if h.get("sold_price_usd") is not None]
        if len(priced) < 5:
            continue

        kind_label = "2yo" if is_twoyo(sale_name) else ("yearling" if is_yearling(sale_name) else "other")
        by_sale[sale_name] = {
            "slug":            slug,
            "kind":            kind_label,
            "total_hips":      len(hips),
            "priced_hips":     len(priced),
            "by_signal":       slice_by_signal(hips),
        }
        if kind_label == "2yo":
            twoyo_pool.extend(hips)
        elif kind_label == "yearling":
            yearling_pool.extend(hips)

    aggregate = {
        "all_2yo_sales":     {"hip_count": len(twoyo_pool),    "by_signal": slice_by_signal(twoyo_pool)},
        "all_yearling_sales":{"hip_count": len(yearling_pool), "by_signal": slice_by_signal(yearling_pool)},
    }

    out = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "method":       "slice-priced-hips-by-predicted-signal-tier",
        "summary": {
            "sales_scored":      len(by_sale),
            "twoyo_hips_pooled": len(twoyo_pool),
            "yrl_hips_pooled":   len(yearling_pool),
        },
        "aggregate": aggregate,
        "by_sale":   by_sale,
    }
    OUTPUT_JSON.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print(f"Wrote {OUTPUT_JSON.name}: "
          f"{len(by_sale)} sales scored, "
          f"{len(twoyo_pool):,} 2YO hips + {len(yearling_pool):,} yearling hips pooled")
    print()
    print("Aggregate validation across all 2YO sales:")
    bs = aggregate["all_2yo_sales"]["by_signal"]
    hdr = f"  {'tier':10s}  {'hips':>5s}  {'sold':>5s}  {'rna':>5s}  {'med $':>10s}  {'avg $':>10s}  "
    hdr += f"{'realized n':>11s}  {'med %':>7s}  {'mean %':>7s}  {'+%':>5s}"
    print(hdr)
    for sig in SIGNAL_ORDER:
        b = bs[sig]
        p = b["price"]; r = b["realized"]
        med = f"${p['median']:,}" if p['median'] else "—"
        avg = f"${p['mean']:,}"   if p['mean']   else "—"
        rn  = r['n']
        rmed = f"{r['median_pct']:+.1f}" if r['median_pct'] is not None else "—"
        rmean = f"{r['mean_pct']:+.1f}" if r['mean_pct'] is not None else "—"
        rpos = f"{r['positive_pct']:.0f}" if r['positive_pct'] is not None else "—"
        print(f"  {sig:10s}  {b['total_hips']:>5d}  {b['sold_n']:>5d}  {b['rna_n']:>5d}  "
              f"{med:>10s}  {avg:>10s}  {rn:>11d}  {rmed:>7s}  {rmean:>7s}  {rpos:>5s}")


if __name__ == "__main__":
    main()
