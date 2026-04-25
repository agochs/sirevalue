"""Compute pedigree-similar stallions for every horse in the roster.

This is the deterministic, accuracy-first alternative to LLM-based RAG: each
stallion's "vector" is a sparse set of weighted ancestors (sire, damsire),
and similarity is computed from concrete shared ancestors. Every output has
a labeled reason ("share sire Tapit") so we never put a number on the page
that we can't explain.

Scoring components, point-weighted by genetic relatedness:
    same sire (half-brothers):                 +50
    same damsire (half through dam line):      +30
    cross: stallion's sire = other's damsire:  +20
    cross: stallion's damsire = other's sire:  +20

For each stallion we emit the top 5 most-similar others with score > 0 plus
a list of human-readable match reasons.

Output:
    similar-stallions.json — { "by_stallion": { name: [ { name, score, reasons[] }, ... ] } }
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).parent
SCORES_JSON = HERE / "scores.json"
OUTPUT_JSON = HERE / "similar-stallions.json"

TOP_K = 5

# Weights for each kind of pedigree match. Tuned to reflect how breeders
# actually rank similarity: half-brothers (same sire) > shared damsire >
# cross relationships.
W_SAME_SIRE     = 50
W_SAME_DAMSIRE  = 30
W_SIRE_X_DAMSIRE = 20   # stallion.sire == other.damsire
W_DAMSIRE_X_SIRE = 20   # stallion.damsire == other.sire


def normalize(name: str | None) -> str:
    """Lowercase + strip country suffix for ancestor matching. Returns empty
    string if name is missing — empty strings never match anything else
    (we filter on truthy below)."""
    if not name:
        return ""
    s = name.lower().strip()
    # Strip trailing " (XXX)" country code so "Lope de Vega (IRE)" matches
    # "Lope de Vega" if a roster entry happens to drop the suffix.
    import re
    s = re.sub(r"\s*\([^)]+\)\s*$", "", s)
    return s


def similarity(a: dict, b: dict) -> tuple[int, list[str]]:
    """Returns (score, reasons[]). Score is 0 when no shared ancestors."""
    if a is b or a.get("name") == b.get("name"):
        return 0, []

    a_sire    = normalize(a.get("sire"))
    a_damsire = normalize(a.get("damsire"))
    b_sire    = normalize(b.get("sire"))
    b_damsire = normalize(b.get("damsire"))

    score = 0
    reasons: list[str] = []

    if a_sire and a_sire == b_sire:
        score += W_SAME_SIRE
        reasons.append(f"Half-brothers — share sire {a.get('sire')}")
    if a_damsire and a_damsire == b_damsire:
        score += W_SAME_DAMSIRE
        reasons.append(f"Share damsire {a.get('damsire')}")
    if a_sire and a_sire == b_damsire:
        score += W_SIRE_X_DAMSIRE
        reasons.append(f"His sire {a.get('sire')} is their damsire")
    if a_damsire and a_damsire == b_sire:
        score += W_DAMSIRE_X_SIRE
        reasons.append(f"His damsire {a.get('damsire')} is their sire")

    return score, reasons


def main():
    data = json.loads(SCORES_JSON.read_text())
    stallions = data["stallions"]
    n = len(stallions)
    print(f"Computing similarity across {n} stallions ({n*(n-1)//2:,} pairs)…")

    by_stallion: dict[str, list[dict]] = {}
    for i, a in enumerate(stallions):
        scored: list[dict] = []
        for j, b in enumerate(stallions):
            if i == j:
                continue
            score, reasons = similarity(a, b)
            if score <= 0:
                continue
            scored.append({
                "name": b["name"],
                "farm": b["farm"],
                "score": score,
                "fee_usd": b.get("fee_usd"),
                "value_score": b.get("score", {}).get("value"),
                "value_grade": b.get("score", {}).get("grade"),
                "reasons": reasons,
            })
        scored.sort(key=lambda x: -x["score"])
        if scored:
            by_stallion[a["name"]] = scored[:TOP_K]

    out = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "method": "deterministic-shared-ancestors",
        "weights": {
            "same_sire": W_SAME_SIRE,
            "same_damsire": W_SAME_DAMSIRE,
            "sire_x_damsire": W_SIRE_X_DAMSIRE,
            "damsire_x_sire": W_DAMSIRE_X_SIRE,
        },
        "top_k_per_stallion": TOP_K,
        "summary": {
            "stallions_with_matches": len(by_stallion),
            "stallions_total": n,
        },
        "by_stallion": by_stallion,
    }
    OUTPUT_JSON.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print(
        f"Wrote {OUTPUT_JSON.name}: "
        f"{len(by_stallion)} stallions have ≥1 match (out of {n}). "
        f"Sample (first 3):"
    )
    for name in list(by_stallion.keys())[:3]:
        matches = by_stallion[name]
        print(f"  {name}:")
        for m in matches[:3]:
            print(f"    [{m['score']:3d}] {m['name']:25s} — {m['reasons'][0]}")


if __name__ == "__main__":
    main()
