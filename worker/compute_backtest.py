"""Out-of-sample predictive accuracy for the Value Score.

Question: how much does the score actually predict realized pinhook lift,
above what a naive baseline would? If the model adds nothing over "predict
by fee" or "predict the global mean," the score is decorative.

Method (leave-one-out cross-validation):
  For each stallion S with realized pinhook lift:
    1. Hold S out
    2. Fit baseline + model on the rest:
        - baseline_mean: predict the global mean lift
        - baseline_fee:  fit lift ~ log(fee), predict S's lift from S's fee
        - model:         fit lift ~ score,    predict S's lift from S's score
    3. Record squared error for each predictor
  Compute RMSE for each predictor, plus R² vs the global-mean baseline
  (R² > 0 means the predictor beats predict-the-mean).

The score is "doing the work it claims" iff its RMSE is meaningfully lower
than both baselines.

Source: public/data/scores.json
Output: public/data/backtest.json
"""
from __future__ import annotations

import json
import math
import statistics
from pathlib import Path

HERE = Path(__file__).parent
SCORES = HERE.parent / "public" / "data" / "scores.json"
OUT    = HERE.parent / "public" / "data" / "backtest.json"


def linfit(xs, ys):
    """Simple OLS for y = a + b*x. Returns (a, b)."""
    n = len(xs)
    if n == 0:
        return 0.0, 0.0
    if n == 1:
        return ys[0], 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs) or 1e-9
    b = num / den
    a = my - b * mx
    return a, b


def main():
    data = json.loads(SCORES.read_text())
    rows = []
    for s in data["stallions"]:
        p = s.get("pinhook")
        if not p or p.get("lift_ratio") is None or (p.get("twoyo_n") or 0) < 3:
            continue
        if not s.get("fee_usd"):
            continue
        rows.append({
            "name":  s["name"],
            "lift":  float(p["lift_ratio"]),
            "score": float(s["score"]["value"]),
            "fee":   float(s["fee_usd"]),
            "log_fee": math.log(s["fee_usd"]),
            "grade": s["score"]["grade"],
        })

    n = len(rows)
    if n < 5:
        OUT.write_text(json.dumps({"error": "insufficient_sample", "n": n}, indent=2))
        print(f"insufficient sample (n={n}); need at least 5")
        return

    print(f"Backtest sample: {n} stallions with realized pinhook lift")

    se_mean = []   # squared errors for predict-the-mean baseline
    se_fee  = []   # squared errors for predict-by-log-fee baseline
    se_model = []  # squared errors for predict-by-score (the model)
    abs_errors_model = []

    for i, target in enumerate(rows):
        train = rows[:i] + rows[i+1:]
        train_lifts = [r["lift"] for r in train]

        # Baseline 1: global mean
        pred_mean = statistics.mean(train_lifts)
        # Baseline 2: linear in log(fee)
        a_fee, b_fee = linfit([r["log_fee"] for r in train], train_lifts)
        pred_fee = a_fee + b_fee * target["log_fee"]
        # Model: linear in score
        a_mod, b_mod = linfit([r["score"] for r in train], train_lifts)
        pred_model = a_mod + b_mod * target["score"]

        se_mean.append((pred_mean - target["lift"]) ** 2)
        se_fee.append((pred_fee - target["lift"]) ** 2)
        se_model.append((pred_model - target["lift"]) ** 2)
        abs_errors_model.append(abs(pred_model - target["lift"]))

    rmse_mean = math.sqrt(statistics.mean(se_mean))
    rmse_fee  = math.sqrt(statistics.mean(se_fee))
    rmse_model = math.sqrt(statistics.mean(se_model))
    mae_model = statistics.mean(abs_errors_model)

    # R² relative to predict-the-mean baseline
    r2_fee   = 1.0 - (sum(se_fee) / sum(se_mean) if sum(se_mean) > 0 else 1.0)
    r2_model = 1.0 - (sum(se_model) / sum(se_mean) if sum(se_mean) > 0 else 1.0)

    out = {
        "generated_at":  data.get("generated_at"),
        "model_version": data.get("model_version"),
        "method": "leave-one-out cross-validation",
        "target": "realized pinhook lift_ratio (twoyo_avg / yearling_avg)",
        "min_twoyo_n": 3,
        "summary": {
            "n":                  n,
            "rmse_predict_mean":  round(rmse_mean, 3),
            "rmse_predict_fee":   round(rmse_fee, 3),
            "rmse_predict_score": round(rmse_model, 3),
            "mae_predict_score":  round(mae_model, 3),
            "r2_vs_mean_fee":     round(r2_fee, 3),
            "r2_vs_mean_score":   round(r2_model, 3),
            "score_beats_fee":    rmse_model < rmse_fee,
            "score_beats_mean":   rmse_model < rmse_mean,
        },
    }
    OUT.write_text(json.dumps(out, indent=2))
    print()
    print(f"  RMSE predict-the-mean baseline:    {rmse_mean:.3f}")
    print(f"  RMSE predict-by-log(fee) baseline: {rmse_fee:.3f}    (R² vs mean: {r2_fee:+.3f})")
    print(f"  RMSE predict-by-score (the model): {rmse_model:.3f}    (R² vs mean: {r2_model:+.3f})")
    print(f"  MAE  predict-by-score:             {mae_model:.3f}")
    print()
    if rmse_model < rmse_fee and rmse_model < rmse_mean:
        print(f"  ✓  Score outperforms BOTH baselines on out-of-sample prediction")
    elif rmse_model < rmse_mean:
        print(f"  ⚠  Score beats predict-the-mean but not predict-by-fee")
    else:
        print(f"  ✗  Score does NOT beat baselines — model is no better than guessing")


if __name__ == "__main__":
    main()
