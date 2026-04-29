"""Grade-band calibration: do higher-graded stallions actually outperform?

For each letter grade (A+, A, B, C, D, F), aggregate the realized performance
signals across the stallions in that grade. If the model is well-calibrated,
A-graded stallions should show meaningfully higher realized pinhook lifts,
yearling ratios, and 2YO ratios than B-graded ones, and so on.

If A and B converge, the grade cutoffs are wrong (or the score isn't
predictive). The output JSON drives /calibration.html — visually shows
either "model is calibrated" or "model needs retuning."

Source: public/data/scores.json (current model output)
Output: public/data/calibration.json
"""
from __future__ import annotations

import json
import statistics
from pathlib import Path

HERE = Path(__file__).parent
SCORES = HERE.parent / "public" / "data" / "scores.json"
OUT    = HERE.parent / "public" / "data" / "calibration.json"

GRADE_ORDER = ["A+", "A", "B", "C", "D", "F"]


def safe_mean(vals):
    return round(statistics.mean(vals), 3) if vals else None


def safe_median(vals):
    return round(statistics.median(vals), 3) if vals else None


def main():
    data = json.loads(SCORES.read_text())
    stallions = data["stallions"]

    # Bucket by grade
    by_grade: dict[str, list[dict]] = {g: [] for g in GRADE_ORDER}
    for s in stallions:
        g = s["score"]["grade"]
        if g in by_grade:
            by_grade[g].append(s)

    grades_out = []
    for g in GRADE_ORDER:
        bucket = by_grade[g]
        if not bucket:
            grades_out.append({"grade": g, "n": 0})
            continue

        # Collect realized signals per stallion in this bucket
        lifts        = []   # pinhook lift_ratio (realized yearling→2YO)
        yrl_ratios   = []   # yearling_avg / stud_fee
        twoyo_ratios = []   # twoyo_avg / stud_fee
        scores       = []   # raw model score (sanity check that bucket is bounded)

        for s in bucket:
            sc = s["score"]
            scores.append(sc["value"])
            p = s.get("pinhook")
            if p and p.get("lift_ratio") is not None and (p.get("twoyo_n") or 0) >= 3:
                lifts.append(p["lift_ratio"])
            me = (sc.get("components", {}).get("market_efficiency") or {}).get("inputs") or {}
            te = (sc.get("components", {}).get("twoyo_market_efficiency") or {}).get("inputs") or {}
            if me.get("ratio"):
                yrl_ratios.append(me["ratio"])
            if te.get("ratio"):
                twoyo_ratios.append(te["ratio"])

        grades_out.append({
            "grade":               g,
            "n":                   len(bucket),
            "n_with_pinhook":      len(lifts),
            "n_with_yrl_ratio":    len(yrl_ratios),
            "n_with_twoyo_ratio":  len(twoyo_ratios),
            "score_min":           round(min(scores), 1)    if scores else None,
            "score_max":           round(max(scores), 1)    if scores else None,
            "score_mean":          safe_mean(scores),
            "lift_mean":           safe_mean(lifts),
            "lift_median":         safe_median(lifts),
            "yrl_ratio_mean":      safe_mean(yrl_ratios),
            "yrl_ratio_median":    safe_median(yrl_ratios),
            "twoyo_ratio_mean":    safe_mean(twoyo_ratios),
            "twoyo_ratio_median":  safe_median(twoyo_ratios),
        })

    # Calibration check: is each lower grade band's mean LIFT actually lower
    # than the previous one? If a grade has no pinhook data, skip in the chain.
    monotonic_violations = []
    last_lift = None
    last_grade = None
    for row in grades_out:
        if row.get("lift_mean") is None:
            continue
        if last_lift is not None and row["lift_mean"] > last_lift + 0.05:
            # Lower grade has HIGHER lift than higher grade — calibration miss
            monotonic_violations.append({
                "violation": f"{last_grade} mean lift {last_lift} < {row['grade']} mean lift {row['lift_mean']}",
                "lower_grade": row["grade"],
                "higher_grade": last_grade,
            })
        last_lift = row["lift_mean"]
        last_grade = row["grade"]

    out = {
        "generated_at": data.get("generated_at"),
        "model_version": data.get("model_version"),
        "summary": {
            "total_stallions":      len(stallions),
            "graded_stallions":     sum(g["n"] for g in grades_out),
            "n_with_pinhook":       sum(g.get("n_with_pinhook", 0) for g in grades_out),
            "monotonic_violations": monotonic_violations,
        },
        "grades": grades_out,
    }
    OUT.write_text(json.dumps(out, indent=2))
    print(f"Wrote {OUT}: {len(grades_out)} grade rows, "
          f"{sum(g.get('n_with_pinhook', 0) for g in grades_out)} stallions w/ realized pinhook data")
    print()
    print(f"  {'grade':4s}  {'n':>3s}  {'lift mean':>9s}  {'yrl ratio':>9s}  {'2yo ratio':>9s}")
    for row in grades_out:
        print(f"  {row['grade']:>4s}  {row['n']:>3d}  "
              f"{row.get('lift_mean') or '—':>9}  "
              f"{row.get('yrl_ratio_mean') or '—':>9}  "
              f"{row.get('twoyo_ratio_mean') or '—':>9}")
    if monotonic_violations:
        print()
        print(f"  ⚠  Calibration violations:")
        for v in monotonic_violations:
            print(f"     {v['violation']}")
    else:
        print()
        print(f"  ✓  Lift means are monotonically ordered by grade")


if __name__ == "__main__":
    main()
