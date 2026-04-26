"""Predict pinhook signal for first-crop / pre-data stallions.

For every stallion that has zero realized pinhook pairs in hip-pinhooks.json
(typically because their first foals are too young to have completed a
yearling-to-2YO sale cycle yet), we estimate what their pinhook signal is
likely to be by leaning on the most similar stallions that DO have data.

The "similars" come from compute_similar_stallions.py, which scores other
stallions by shared sire/damsire/sire-line/etc. We pick comparators that:
  - have >=5 matched pinhook pairs (avoid noisy small-sample comparators)
  - rank in the top N by similarity score for this stallion

We aggregate (sample-size-weighted) the median return %, mean return %, and
positive % of those comparators. Confidence = high / medium / low based on
how many comparators we found and total pairs they represent.

Output: first-crop-predictions.json
{
  generated_at,
  summary: { stallions_total, predicted, no_comparators },
  stallions: [
    {
      name, farm, sire, damsire, pedigree, fee_usd, entered_stud_year,
      year_of_birth, value_score, value_grade,
      predicted: {
        median_return_pct, mean_return_pct, positive_pct,
        comparators_n, comparator_pairs_total, confidence,
      },
      comparators: [
        { name, similarity_score, similarity_reasons,
          matched_pairs, median_return_pct, positive_return_pct }, ...
      ],
    }, ...
  ]
}

Run AFTER compute_hip_pinhooks.py and compute_similar_stallions.py.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from statistics import mean, median

HERE = Path(__file__).parent
SCORES_JSON  = HERE / "scores.json"
PINHOOKS_JSON = HERE / "hip-pinhooks.json"
SIMILAR_JSON  = HERE / "similar-stallions.json"
OUTPUT_JSON   = HERE / "first-crop-predictions.json"

MIN_PAIRS_PER_COMPARATOR = 5     # comparator needs this many pairs to count
TOP_K_COMPARATORS        = 8     # cap how many similars we use per stallion
MIN_PAIRS_FOR_HIGH_CONF  = 60    # total pairs across comparators for "high"
MIN_PAIRS_FOR_MED_CONF   = 20    # total for "medium"


def confidence_label(comparators_n: int, total_pairs: int) -> str:
    if comparators_n >= 4 and total_pairs >= MIN_PAIRS_FOR_HIGH_CONF:
        return "high"
    if comparators_n >= 2 and total_pairs >= MIN_PAIRS_FOR_MED_CONF:
        return "medium"
    return "low"


def main():
    if not (SCORES_JSON.exists() and PINHOOKS_JSON.exists() and SIMILAR_JSON.exists()):
        print("Missing required input(s) — need scores.json, hip-pinhooks.json, similar-stallions.json")
        return

    scores  = json.loads(SCORES_JSON.read_text())
    pinhks  = json.loads(PINHOOKS_JSON.read_text())
    sim     = json.loads(SIMILAR_JSON.read_text())

    by_name_score   = {s["name"]: s for s in scores.get("stallions", [])}
    by_name_pinhook = pinhks.get("by_stallion") or {}
    by_name_sim     = sim.get("by_stallion") or {}

    # First-crop = no pinhook data at all
    first_crop = [s for s in scores["stallions"] if s["name"] not in by_name_pinhook]
    print(f"First-crop / pre-data stallions: {len(first_crop)}")

    out_stallions: list[dict] = []
    no_comparators_n = 0
    predicted_n = 0
    for s in first_crop:
        name = s["name"]
        # Pull the similar list — keep only those with enough pinhook data
        similars = by_name_sim.get(name) or []
        usable = []
        for sim_rec in similars:
            sim_name = sim_rec.get("name")
            sim_blob = by_name_pinhook.get(sim_name)
            if not sim_blob:
                continue
            sim_summary = sim_blob.get("summary") or {}
            n_pairs = sim_summary.get("matched_pairs") or 0
            if n_pairs < MIN_PAIRS_PER_COMPARATOR:
                continue
            usable.append({
                "name":                sim_name,
                "similarity_score":    sim_rec.get("score") or 0,
                "similarity_reasons":  sim_rec.get("reasons") or [],
                "matched_pairs":       n_pairs,
                "median_return_pct":   sim_summary.get("median_return_pct"),
                "mean_return_pct":     sim_summary.get("mean_return_pct"),
                "positive_return_pct": sim_summary.get("positive_return_pct"),
                "median_yearling_price": sim_summary.get("median_yearling_price"),
                "median_twoyo_price":  sim_summary.get("median_twoyo_price"),
            })
            if len(usable) >= TOP_K_COMPARATORS:
                break

        rec = {
            "name":               name,
            "farm":               s.get("farm"),
            "sire":               s.get("sire"),
            "damsire":            s.get("damsire"),
            "pedigree":           s.get("pedigree"),
            "fee_usd":            s.get("fee_usd"),
            "entered_stud_year":  s.get("entered_stud_year"),
            "year_of_birth":      s.get("year_of_birth"),
            "value_score":        (s.get("score") or {}).get("value"),
            "value_grade":        (s.get("score") or {}).get("grade"),
            "comparators":        usable,
        }

        if not usable:
            no_comparators_n += 1
            rec["predicted"] = None
        else:
            # Sample-size-weighted means across comparators
            total_pairs = sum(c["matched_pairs"] for c in usable)
            def w_avg(field):
                vals = [(c[field], c["matched_pairs"]) for c in usable if c.get(field) is not None]
                if not vals:
                    return None
                return sum(v * w for v, w in vals) / sum(w for _, w in vals)
            rec["predicted"] = {
                "median_return_pct":      round(w_avg("median_return_pct"), 1) if w_avg("median_return_pct") is not None else None,
                "mean_return_pct":        round(w_avg("mean_return_pct"), 1)   if w_avg("mean_return_pct")   is not None else None,
                "positive_pct":           round(w_avg("positive_return_pct"), 1) if w_avg("positive_return_pct") is not None else None,
                "median_yearling_price":  int(w_avg("median_yearling_price")) if w_avg("median_yearling_price") is not None else None,
                "median_twoyo_price":     int(w_avg("median_twoyo_price"))    if w_avg("median_twoyo_price")    is not None else None,
                "comparators_n":          len(usable),
                "comparator_pairs_total": total_pairs,
                "confidence":             confidence_label(len(usable), total_pairs),
            }
            predicted_n += 1

        out_stallions.append(rec)

    # Sort: high-confidence first, then by predicted median return desc
    def sort_key(r):
        p = r.get("predicted") or {}
        conf_rank = {"high": 0, "medium": 1, "low": 2}.get(p.get("confidence"), 3)
        med = p.get("median_return_pct")
        return (conf_rank, -(med if med is not None else -1e9))
    out_stallions.sort(key=sort_key)

    out = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "method":       "weighted-comparator-from-similar-stallions-with-pinhook-data",
        "thresholds": {
            "min_pairs_per_comparator": MIN_PAIRS_PER_COMPARATOR,
            "top_k_comparators":        TOP_K_COMPARATORS,
            "high_conf_total_pairs":    MIN_PAIRS_FOR_HIGH_CONF,
            "medium_conf_total_pairs":  MIN_PAIRS_FOR_MED_CONF,
        },
        "summary": {
            "stallions_total":  len(first_crop),
            "predicted":        predicted_n,
            "no_comparators":   no_comparators_n,
        },
        "stallions": out_stallions,
    }
    OUTPUT_JSON.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print(f"Wrote {OUTPUT_JSON.name}: {predicted_n}/{len(first_crop)} first-crop stallions predicted "
          f"({no_comparators_n} with no usable comparators)")
    print()
    # Quick spot-check: show top 10 high-confidence predictions
    high_conf = [r for r in out_stallions if (r.get("predicted") or {}).get("confidence") == "high"]
    print(f"Top {min(10, len(high_conf))} high-confidence predictions:")
    for r in high_conf[:10]:
        p = r["predicted"]
        comp_names = ", ".join(c["name"] for c in r["comparators"][:3])
        print(f"  {p['median_return_pct']:+6.1f}%  {r['name']:25s}  "
              f"conf=high  pos={p['positive_pct']:.0f}%  "
              f"comps=[{comp_names}…]")


if __name__ == "__main__":
    main()
