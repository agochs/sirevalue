"""Backtest-tune component weights via multivariate regression.

The current heuristic weights (e.g. FULL_TIER_1_WEIGHTS = {market 20, twoyo
15, pinhook 15, pedigree 22, fee 18, maturity 10}) were chosen by intuition.
This script asks: given realized pinhook lift as the target, what weights
would maximize predictive power?

Method:
  Fit linear regression:
    lift_pct = β0 + β_market·market_pct + β_twoyo·twoyo_pct + β_pinhook·pinhook_pct
              + β_pedigree·pedigree_pct + β_fee·fee_pct + β_maturity·maturity_pct
  Subject to: all β_i ≥ 0 (negative weights are uninterpretable)
  Normalize {β_i} so they sum to 100 (the weight scale we use today).

We compare the implied "tuned" weights to the current weights, and report
the in-sample R² uplift. The user can then decide whether to adopt the
tuned weights or stick with the heuristic.

This is in-sample; pair with compute_backtest.py for out-of-sample R².

Source: public/data/scores.json
Output: public/data/weight-tuning.json
"""
from __future__ import annotations

import json
import math
import statistics
from pathlib import Path

HERE = Path(__file__).parent
SCORES = HERE.parent / "public" / "data" / "scores.json"
OUT    = HERE.parent / "public" / "data" / "weight-tuning.json"

CURRENT_WEIGHTS = {
    "market_efficiency":       20,
    "twoyo_market_efficiency": 15,
    "pinhook_efficiency":      15,
    "pedigree_prestige":       22,
    "fee_band_position":       18,
    "maturity_context":        10,
}
COMPONENT_ORDER = list(CURRENT_WEIGHTS.keys())


def linreg_normal_eqns(X, y):
    """Solve OLS β = (X'X)^-1 X'y by 2-step Gauss-Jordan elimination on the
    normal equations. Hand-rolled to avoid a numpy dep."""
    n_rows = len(X)
    n_cols = len(X[0]) if X else 0
    # Build X'X (n_cols x n_cols) and X'y (n_cols)
    XtX = [[0.0] * n_cols for _ in range(n_cols)]
    Xty = [0.0] * n_cols
    for i in range(n_rows):
        for c in range(n_cols):
            Xty[c] += X[i][c] * y[i]
            for d in range(n_cols):
                XtX[c][d] += X[i][c] * X[i][d]
    # Augment XtX with Xty as the last column, then gauss-jordan
    M = [XtX[i] + [Xty[i]] for i in range(n_cols)]
    for col in range(n_cols):
        # Find pivot row (max abs value in this column at or below `col`)
        pivot = col
        max_val = abs(M[col][col])
        for r in range(col + 1, n_cols):
            if abs(M[r][col]) > max_val:
                pivot = r
                max_val = abs(M[r][col])
        if max_val < 1e-12:
            return None   # singular
        if pivot != col:
            M[col], M[pivot] = M[pivot], M[col]
        # Normalize pivot row
        pv = M[col][col]
        for k in range(col, n_cols + 1):
            M[col][k] /= pv
        # Eliminate other rows
        for r in range(n_cols):
            if r == col:
                continue
            factor = M[r][col]
            if factor == 0:
                continue
            for k in range(col, n_cols + 1):
                M[r][k] -= factor * M[col][k]
    return [M[i][n_cols] for i in range(n_cols)]


def project_to_nonneg(b):
    """Negative coefficients aren't meaningful as weights. Clip and re-fit
    isn't trivial without iteration; cheapest robust trick is just to clip
    to ≥ 0 and renormalize. Loses a bit of in-sample R² but stays
    interpretable."""
    return [max(0.0, x) for x in b]


def main():
    data = json.loads(SCORES.read_text())
    rows = []
    for s in data["stallions"]:
        p = s.get("pinhook")
        if not p or p.get("lift_ratio") is None or (p.get("twoyo_n") or 0) < 3:
            continue
        comps = s.get("score", {}).get("components") or {}
        feature = []
        valid = True
        for k in COMPONENT_ORDER:
            c = comps.get(k)
            if c is None or c.get("percentile") is None:
                valid = False
                break
            feature.append(float(c["percentile"]))
        if not valid:
            continue
        rows.append({"name": s["name"], "lift": float(p["lift_ratio"]),
                     "features": feature, "score": float(s["score"]["value"])})

    n = len(rows)
    if n < 10:
        OUT.write_text(json.dumps({"error": "insufficient_sample", "n": n}, indent=2))
        print(f"insufficient sample (n={n}); need at least 10 stallions with full Tier 1 + realized pinhook")
        return
    print(f"Tuning sample: {n} stallions with all 6 components + realized pinhook lift")

    # Build design matrix with intercept column
    X = [[1.0] + r["features"] for r in rows]
    y = [r["lift"] for r in rows]
    coefs = linreg_normal_eqns(X, y)
    if coefs is None:
        print("regression singular — components likely collinear")
        return
    intercept = coefs[0]
    raw_betas = coefs[1:]   # one per component, in COMPONENT_ORDER

    # Renormalize betas to weights summing to 100, ≥0
    nonneg_betas = project_to_nonneg(raw_betas)
    total = sum(nonneg_betas)
    tuned_weights = (
        [round(b / total * 100, 1) for b in nonneg_betas]
        if total > 0 else
        [round(100.0 / len(COMPONENT_ORDER), 1)] * len(COMPONENT_ORDER)
    )

    # Compute in-sample R² for current vs tuned vs intercept-only
    mean_y = statistics.mean(y)
    ss_tot = sum((yi - mean_y) ** 2 for yi in y)

    def r2_for(weights):
        # Score = sum(weight_i * pct_i / 100)  (matches the live model)
        preds = []
        for r in rows:
            pred_score = sum(weights[i] * r["features"][i] / 100 for i in range(len(weights)))
            preds.append(pred_score)
        # Predictions are on a 0-100 scale; calibrate to lift via simple linear fit
        # so we're comparing variance explained, not absolute level
        a, b = _linfit_simple(preds, y)
        residuals = [y[i] - (a + b * preds[i]) for i in range(len(y))]
        ss_res = sum(rsq * rsq for rsq in residuals)
        return 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    current_w_list = [CURRENT_WEIGHTS[k] for k in COMPONENT_ORDER]
    r2_current = r2_for(current_w_list)
    r2_tuned   = r2_for(tuned_weights)

    out = {
        "generated_at":  data.get("generated_at"),
        "model_version": data.get("model_version"),
        "method": "OLS regression of realized pinhook lift on component percentiles, weights ≥0 + renormalized to 100",
        "sample": {"n": n},
        "components": COMPONENT_ORDER,
        "current_weights": current_w_list,
        "tuned_weights":   tuned_weights,
        "comparison": {
            "in_sample_r2_current": round(r2_current, 3),
            "in_sample_r2_tuned":   round(r2_tuned, 3),
            "uplift":               round(r2_tuned - r2_current, 3),
        },
        "raw_regression": {
            "intercept": round(intercept, 3),
            "raw_betas": [round(b, 3) for b in raw_betas],
        },
    }
    OUT.write_text(json.dumps(out, indent=2))

    print()
    print(f"  {'Component':<27}  {'Current':>8}  {'Tuned':>8}  {'Δ':>6}")
    for i, k in enumerate(COMPONENT_ORDER):
        delta = tuned_weights[i] - current_w_list[i]
        print(f"  {k:<27}  {current_w_list[i]:>7}%  {tuned_weights[i]:>7}%  {delta:>+5.1f}")
    print(f"  {'-' * 27}  {'-' * 8}  {'-' * 8}  {'-' * 6}")
    print(f"  {'In-sample R²':<27}  {r2_current:>7.3f}  {r2_tuned:>7.3f}  {r2_tuned - r2_current:>+5.3f}")


def _linfit_simple(xs, ys):
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


if __name__ == "__main__":
    main()
