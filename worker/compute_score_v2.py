"""Score v2 — regression-fit predicted pinhook lift.

Where the additive score (v1) sums weighted component percentiles and asks
"how does this stallion compare to peers across multiple dimensions?", the
regression-fit score (v2) asks "given this stallion's features, what's our
best forecast of realized pinhook lift?"

The two are complementary:
  * v1 is interpretable per-component, multi-objective, peer-relative
  * v2 is single-objective, fit to maximize predictive accuracy on one
    target (pinhook lift), more honest about which features actually predict

Method
------
Features (per-stallion):
    - log_fee                  : log(stud_fee_usd)            (the strong baseline)
    - market_eff_pct           : market_efficiency percentile (yearling market)
    - twoyo_eff_pct            : twoyo_market_efficiency      (2YO market)
    - pinhook_eff_pct          : pinhook_efficiency           (lift percentile)
    - pedigree_pct             : pedigree_prestige
    - fee_band_pct             : fee_band_position
    - maturity_pct             : maturity_context
    - book_demand_pct          : book_demand                  (if present)

Target: realized pinhook lift_ratio (twoyo_avg / yearling_avg) for stallions
        with twoyo_n >= 3.

Fit: ordinary least squares (closed form normal equations). No regularization
yet; sample is small enough (~94 stallions) that we want to use every signal.

Output: public/data/score-v2.json with:
    - fitted coefficients
    - per-stallion predicted_lift (for any stallion with feature coverage —
      predictions for stallions WITHOUT realized lift are forecasts)
    - in-sample R² + leave-one-out R²
"""
from __future__ import annotations

import json
import math
import statistics
from pathlib import Path

HERE = Path(__file__).parent
SCORES = HERE.parent / "public" / "data" / "scores.json"
OUT    = HERE.parent / "public" / "data" / "score-v2.json"

# Feature ordering (also defines display order in the output coefficients block)
FEATURES = [
    "log_fee",
    "market_efficiency",
    "twoyo_market_efficiency",
    "pinhook_efficiency",
    "pedigree_prestige",
    "fee_band_position",
    "maturity_context",
    "book_demand",
]


def linfit_normal_eqns(X, y):
    """OLS via Gauss-Jordan on the normal equations. Same code shape as in
    compute_weight_tuning.py — duplicated to keep this script self-contained."""
    n_rows = len(X)
    n_cols = len(X[0]) if X else 0
    XtX = [[0.0] * n_cols for _ in range(n_cols)]
    Xty = [0.0] * n_cols
    for i in range(n_rows):
        for c in range(n_cols):
            Xty[c] += X[i][c] * y[i]
            for d in range(n_cols):
                XtX[c][d] += X[i][c] * X[i][d]
    M = [XtX[i] + [Xty[i]] for i in range(n_cols)]
    for col in range(n_cols):
        pivot = col
        max_val = abs(M[col][col])
        for r in range(col + 1, n_cols):
            if abs(M[r][col]) > max_val:
                pivot = r
                max_val = abs(M[r][col])
        if max_val < 1e-12:
            return None
        if pivot != col:
            M[col], M[pivot] = M[pivot], M[col]
        pv = M[col][col]
        for k in range(col, n_cols + 1):
            M[col][k] /= pv
        for r in range(n_cols):
            if r == col:
                continue
            factor = M[r][col]
            if factor == 0:
                continue
            for k in range(col, n_cols + 1):
                M[r][k] -= factor * M[col][k]
    return [M[i][n_cols] for i in range(n_cols)]


def extract_features(s, fee_usd, strict=True):
    """Pull the feature vector for a stallion.

    strict=True: require all 7 components present (used for training).
    strict=False: missing components imputed at 50th percentile (used for
        predictions on stallions without full coverage — typically freshmen
        or early-career sires). Imputation dilutes accuracy; we surface a
        flag on the prediction so consumers can see whether it's based on
        complete data."""
    if not fee_usd or fee_usd <= 0:
        return None, None
    comps = s.get("score", {}).get("components") or {}
    feats = {"log_fee": math.log(fee_usd)}
    missing = []
    for k in FEATURES:
        if k == "log_fee":
            continue
        c = comps.get(k)
        if c is None or c.get("percentile") is None:
            if strict:
                return None, None
            feats[k] = 50.0
            missing.append(k)
        else:
            feats[k] = float(c["percentile"])
    return feats, missing


def predict(coefs, features):
    """coefs is a list of (feature_name, beta); intercept is coefs[0] = ('intercept', a)."""
    pred = 0.0
    for name, beta in coefs:
        if name == "intercept":
            pred += beta
        else:
            pred += beta * features.get(name, 0.0)
    return pred


def main():
    data = json.loads(SCORES.read_text())
    stallions = data["stallions"]

    # Build training set: stallions with realized pinhook lift + full feature coverage
    training = []
    for s in stallions:
        p = s.get("pinhook")
        if not p or p.get("lift_ratio") is None or (p.get("twoyo_n") or 0) < 3:
            continue
        feats, _ = extract_features(s, s.get("fee_usd"), strict=True)
        if feats is None:
            continue
        training.append({
            "name":  s["name"],
            "y":     float(p["lift_ratio"]),
            "feats": feats,
        })
    n_train = len(training)
    if n_train < 10:
        OUT.write_text(json.dumps({"error": "insufficient_training_sample", "n": n_train}, indent=2))
        print(f"insufficient training sample (n={n_train}); need ≥10")
        return
    print(f"Training sample: {n_train} stallions with realized pinhook lift + full features")

    # Prepare design matrix (intercept column + features in FEATURES order)
    X = [[1.0] + [r["feats"][k] for k in FEATURES] for r in training]
    y = [r["y"] for r in training]
    raw_coefs = linfit_normal_eqns(X, y)
    if raw_coefs is None:
        print("regression singular")
        return
    coefs = [("intercept", raw_coefs[0])] + list(zip(FEATURES, raw_coefs[1:]))

    # In-sample R²
    mean_y = statistics.mean(y)
    ss_tot = sum((yi - mean_y) ** 2 for yi in y)
    preds_in = [predict(coefs, r["feats"]) for r in training]
    ss_res = sum((y[i] - preds_in[i]) ** 2 for i in range(n_train))
    in_sample_r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    # Leave-one-out R²
    ss_loo = 0.0
    for i in range(n_train):
        X_minus = [X[j] for j in range(n_train) if j != i]
        y_minus = [y[j] for j in range(n_train) if j != i]
        c_loo = linfit_normal_eqns(X_minus, y_minus)
        if c_loo is None:
            continue
        coef_loo = [("intercept", c_loo[0])] + list(zip(FEATURES, c_loo[1:]))
        pred_i = predict(coef_loo, training[i]["feats"])
        ss_loo += (y[i] - pred_i) ** 2
    loo_r2 = 1.0 - ss_loo / ss_tot if ss_tot > 0 else 0.0
    rmse_loo = math.sqrt(ss_loo / n_train)

    # Now predict for every stallion with at least a fee. Missing components
    # are imputed at 50th percentile (flagged on output for transparency —
    # imputed predictions are less reliable, especially for freshmen).
    predictions = []
    n_with_features = 0
    n_imputed = 0
    for s in stallions:
        feats, missing = extract_features(s, s.get("fee_usd"), strict=False)
        if feats is None:
            predictions.append({"name": s["name"], "predicted_lift": None, "has_realized": False})
            continue
        n_with_features += 1
        if missing:
            n_imputed += 1
        raw_pred = predict(coefs, feats)
        # Linear regression can extrapolate to physically impossible values
        # (lift can't be < 0 — that'd mean negative 2YO sale price). Clamp
        # to a small positive floor so the UI shows interpretable -%; flag
        # when clamping happens so users know the model went out-of-range.
        clamped = max(0.05, raw_pred)
        was_clamped = (clamped != raw_pred)
        p = s.get("pinhook")
        realized = (p["lift_ratio"] if (p and p.get("lift_ratio") is not None and (p.get("twoyo_n") or 0) >= 3) else None)
        predictions.append({
            "name":           s["name"],
            "predicted_lift": round(clamped, 3),
            "predicted_lift_pct": round((clamped - 1) * 100, 1),
            "raw_unclamped_lift": round(raw_pred, 3) if was_clamped else None,
            "extrapolation_flag": was_clamped,    # True = model went out-of-distribution
            "realized_lift":  realized,
            "has_realized":   realized is not None,
            "imputed_components": missing,   # which components were filled with 50th pct
            "is_forecast":    realized is None,
            "delta_realized_minus_predicted": (round(realized - clamped, 3) if realized is not None else None),
        })

    out = {
        "generated_at":  data.get("generated_at"),
        "model_version": data.get("model_version"),
        "method":        "OLS regression of realized pinhook lift on log(fee) + 7 component percentiles",
        "n_training":    n_train,
        "n_predicted":   n_with_features,
        "features":      FEATURES,
        "coefficients":  [{"feature": k, "beta": round(v, 4)} for k, v in coefs],
        "fit_quality": {
            "in_sample_r2":    round(in_sample_r2, 3),
            "leave_one_out_r2": round(loo_r2, 3),
            "rmse_loo":        round(rmse_loo, 3),
        },
        "predictions":   predictions,
    }
    OUT.write_text(json.dumps(out, indent=2))

    print()
    print(f"  Coefficients (regression betas):")
    for k, v in coefs:
        print(f"    {k:<27}  β = {v:>+8.4f}")
    print()
    print(f"  Fit quality:")
    print(f"    in-sample R²:        {in_sample_r2:>+.3f}")
    print(f"    leave-one-out R²:    {loo_r2:>+.3f}")
    print(f"    leave-one-out RMSE:  {rmse_loo:.3f}")
    print()
    print(f"  Wrote {OUT.name} with {n_with_features} per-stallion predictions")
    print(f"  ({n_with_features - n_train} forecasts for stallions without realized lift)")
    print(f"  ({n_imputed} of those used component imputation — flagged in output)")


if __name__ == "__main__":
    main()
