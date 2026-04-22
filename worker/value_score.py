"""
Value Score — reference implementation.

Two entry points:

  score_commercial_appeal(s, peers)  -> TIER 0 score computable from
                                        farm-website data alone. Labeled
                                        "Commercial Appeal" in the UI.

  score_value(s, peers)              -> TIER 1 score. Requires progeny and
                                        sale aggregates. Stubbed with
                                        NotImplementedError until those
                                        facts exist in the DB.

See value-score-model.md for methodology, weights, and rationale.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Sequence

from leading_sires import sire_points, bms_points

MODEL_VERSION = "vs-1.3-nybred-nightly"
CURRENT_YEAR = datetime.now().year


# ---------------------------------------------------------------------------
# Keeneland yearling-average lookup — loaded at import if the file is present.
# Cross-source identity is by exact sire name (with light country-tag
# normalization). See leading_sires.py for the name-canonicalization pattern.
# ---------------------------------------------------------------------------

import json as _json
from pathlib import Path as _Path

_KEENE_FILE = _Path(__file__).parent / "keeneland-yearling-avgs.json"


def _normalize_lookup_key(name: str) -> str:
    """Canonicalize a sire name for cross-source matching.

    Same rules as normalize_name() in spendthrift_scraper — straighten quotes,
    per-word first-letter-upper (so "Bolt d'Oro" and "Bolt D'oro" match).
    Defined inline here to avoid circular imports with the scraper module.
    """
    if not name:
        return name
    # Straighten curly quotes
    name = name.replace("\u2019", "'").replace("\u2018", "'")
    words = []
    for word in name.split():
        if word.startswith("(") and word.endswith(")"):
            inner = word[1:-1]
            words.append("(" + (inner[0].upper() + inner[1:].lower() if inner else "") + ")")
        else:
            words.append(word[0].upper() + word[1:].lower() if word else word)
    return " ".join(words)


_YEARLING_FILES = [
    _KEENE_FILE,                                                        # Keeneland September 2025 (full, 3,078 sold)
    _Path(__file__).parent / "ft-saratoga-2025.json",                   # FT Saratoga Select 2025 (premium)
    _Path(__file__).parent / "ft-ny-bred-yearlings-2025.json",          # FT NY-Bred Yearlings 2025 (regional NY)
    _Path(__file__).parent / "ft-kentucky-october-2025.json",           # FT Kentucky October 2025 (volume)
    _Path(__file__).parent / "ft-midlantic-fall-2025.json",             # FT Midlantic Fall 2025 (regional mid-Atlantic)
    _Path(__file__).parent / "obs-october-2025-yearlings.json",         # OBS October 2025 (mid-market)
]


def _load_yearling_lookup() -> dict:
    """Merge Keeneland September + OBS October yearling sales into one
    lookup. Each sire's yearling record is a weighted average across all
    sales, with a sale_history preserved. Mirrors the multi-sale OBS 2YO
    approach in _load_obs_lookup()."""
    combined: dict[str, dict] = {}
    for path in _YEARLING_FILES:
        if not path.exists():
            continue
        data = _json.loads(path.read_text())
        sale_label = data.get("sale", path.stem)
        for row in data["per_sire"]:
            key = _normalize_lookup_key(row["sire"])
            if key not in combined:
                combined[key] = {
                    "sire": row["sire"],
                    "n": 0,
                    "yearling_avg_usd": 0,
                    "sale_history": [],
                }
            prev = combined[key]
            new_n = prev["n"] + row["n"]
            new_avg = round(
                (prev["n"] * prev["yearling_avg_usd"] + row["n"] * row["yearling_avg_usd"]) / new_n
            ) if new_n else 0
            prev["n"] = new_n
            prev["yearling_avg_usd"] = new_avg
            prev["sale_history"].append({
                "sale": sale_label,
                "n": row["n"],
                "yearling_avg_usd": row["yearling_avg_usd"],
            })
    out = {}
    for key, record in combined.items():
        out[key] = record
        out[record["sire"]] = record
    return out


KEENELAND_BY_SIRE: dict[str, dict] = _load_yearling_lookup()


# ---------------------------------------------------------------------------
# OBS 2YO-in-training lookup — combines multiple sales. Each sire's OBS record
# reflects the weighted-average price across every OBS sale we've ingested,
# with a per-sale breakdown preserved for display.
# ---------------------------------------------------------------------------

_OBS_FILES = [
    # Load chronologically — "latest" in sale_history is the most recent sale,
    # which is what the temporal-shift detector compares against prior sales.
    _Path(__file__).parent / "obs-2yo-avgs.json",      # OBS 2025 Spring 2YO (Apr 2025)
    _Path(__file__).parent / "obs-june-2025.json",     # OBS 2025 June 2YO + HRA (Jun 2025)
    _Path(__file__).parent / "obs-march-2026.json",    # OBS 2026 March 2YO (Mar 2026)
]


def _load_obs_lookup() -> dict:
    """Merge every OBS file into one sire lookup.

    For each sire, the merged record has:
      - n             : total yearlings sold across all sales
      - price_avg_usd : weighted average (by n) across sales
      - sale_history  : [{sale: label, n, price_avg_usd}, ...] for display
    """
    combined: dict[str, dict] = {}
    for path in _OBS_FILES:
        if not path.exists():
            continue
        data = _json.loads(path.read_text())
        sale_label = data.get("sale", path.stem)
        for row in data["per_sire"]:
            # Canonicalize the key so "Bolt d'Oro" and "Bolt D'oro" merge cleanly.
            key = _normalize_lookup_key(row["sire"])
            if key not in combined:
                combined[key] = {
                    "sire": row["sire"],
                    "n": 0,
                    "price_avg_usd": 0,
                    "sale_history": [],
                }
            prev = combined[key]
            new_n = prev["n"] + row["n"]
            new_avg = round(
                (prev["n"] * prev["price_avg_usd"] + row["n"] * row["price_avg_usd"]) / new_n
            ) if new_n else 0
            prev["n"] = new_n
            prev["price_avg_usd"] = new_avg
            prev["sale_history"].append({
                "sale": sale_label,
                "n": row["n"],
                "price_avg_usd": row["price_avg_usd"],
            })

    # Expose under both the canonical key and the raw sire spelling(s) so
    # all callers (whose stallion.name may use various casings) resolve.
    out = {}
    for key, record in combined.items():
        out[key] = record
        out[record["sire"]] = record
    return out


OBS_BY_SIRE: dict[str, dict] = _load_obs_lookup()


# ---------------------------------------------------------------------------
# Stallion snapshot — the minimum inputs needed to score
# ---------------------------------------------------------------------------

@dataclass
class StallionSnapshot:
    """Data needed to score one stallion.

    Tier 0 requires: name, stud_fee_usd, sire, damsire, entered_stud_year.
    Tier 1 additionally requires: yearling_avg_usd, progeny_earnings_usd,
        foals_of_racing_age, starters, winners, stakes_winners, fee_history.
    """
    name: str
    stud_fee_usd: Optional[int] = None
    sire_name: Optional[str] = None
    damsire_name: Optional[str] = None
    entered_stud_year: Optional[int] = None

    # Tier 1 fields (None if not yet ingested)
    yearling_avg_usd: Optional[float] = None
    yearlings_sold_n: Optional[int] = None
    progeny_earnings_usd: Optional[float] = None
    foals_of_racing_age: Optional[int] = None
    starters: Optional[int] = None
    winners: Optional[int] = None
    stakes_winners: Optional[int] = None
    fee_history_usd: list[Optional[int]] = field(default_factory=list)  # [t-2, t-1, t]


# ---------------------------------------------------------------------------
# Peer-group bucketing
# ---------------------------------------------------------------------------

FEE_BANDS = [
    ("u10k",   0,         10_000),
    ("10_25k", 10_000,    25_000),
    ("25_50k", 25_000,    50_000),
    ("50_100k",50_000,    100_000),
    ("100kplus",100_000,  float("inf")),
]

def fee_band(fee_usd: Optional[int]) -> Optional[str]:
    if fee_usd is None:
        return None
    for label, lo, hi in FEE_BANDS:
        if lo <= fee_usd < hi:
            return label
    return None


def maturity_stage(entered_stud_year: Optional[int]) -> str:
    if entered_stud_year is None:
        return "unknown"
    # A foal is typically 3 before it races. Crops are counted from first
    # foal crop year, which is entered_stud_year + 1.
    years_at_stud = CURRENT_YEAR - entered_stud_year
    if years_at_stud <= 1:
        return "first_crop"
    if years_at_stud <= 2:
        return "second_crop"
    if years_at_stud <= 4:
        return "early_proven"
    if years_at_stud <= 10:
        return "established"
    return "senior"


def in_peer_set(target: StallionSnapshot, candidate: StallionSnapshot) -> bool:
    return (
        fee_band(candidate.stud_fee_usd) == fee_band(target.stud_fee_usd)
        and maturity_stage(candidate.entered_stud_year) == maturity_stage(target.entered_stud_year)
        and candidate.name != target.name
    )


# ---------------------------------------------------------------------------
# Small-sample shrinkage (James-Stein / empirical Bayes)
# ---------------------------------------------------------------------------

def shrink(observed: float, peer_mean: float, peer_var: float,
           n: int, within_var: float) -> tuple[float, float]:
    """Return (shrunk_value, shrinkage_factor B in [0,1]).

    B=0 means no shrinkage (trust observed); B=1 means fully shrunk to peer_mean.
    """
    if n <= 0 or peer_var <= 0:
        return peer_mean, 1.0
    B = within_var / (within_var + n * peer_var)
    B = max(0.0, min(1.0, B))
    return B * peer_mean + (1.0 - B) * observed, B


# ---------------------------------------------------------------------------
# Percentile helper (target's rank within a peer list, 0..100)
# ---------------------------------------------------------------------------

def percentile(target: float, peers: Sequence[float]) -> float:
    if not peers:
        return 50.0
    below = sum(1 for p in peers if p < target)
    eq = sum(1 for p in peers if p == target)
    # Average-rank convention
    return 100.0 * (below + 0.5 * eq) / len(peers)


# ---------------------------------------------------------------------------
# TIER 0 — Commercial Appeal components
# ---------------------------------------------------------------------------

def component_pedigree_prestige(s: StallionSnapshot) -> tuple[float, dict]:
    """0..100 percentile on simple sire + BMS tier points.

    Max raw points: 5 (sire leading) + 3 (BMS leading) = 8.
    Returns (percentile_0_100, explain).
    """
    sp = sire_points(s.sire_name or "")
    bp = bms_points(s.damsire_name or "")
    raw = sp + bp
    # Percentile maps: raw 0 → ~15, 1-2 → ~40, 3-4 → ~60, 5-6 → ~75, 7-8 → ~92.
    # Simple piecewise linear.
    if raw == 0: pct = 15
    elif raw <= 2: pct = 40
    elif raw <= 4: pct = 60
    elif raw <= 6: pct = 75
    else: pct = 92
    return pct, {
        "sire_name": s.sire_name, "sire_points": sp,
        "damsire_name": s.damsire_name, "bms_points": bp,
        "raw_total": raw,
    }


def component_fee_band_position(s: StallionSnapshot, peers: list[StallionSnapshot]) -> tuple[float, dict]:
    """Where the stallion's fee sits within its own fee band peer list.

    Higher fee within a band generally indicates stronger commercial demand.
    (In Tier 1 the market-efficiency component will replace this.)
    """
    if s.stud_fee_usd is None:
        return 50.0, {"reason": "no_fee_available"}
    peer_fees = [p.stud_fee_usd for p in peers if p.stud_fee_usd is not None]
    if not peer_fees:
        return 50.0, {"reason": "no_peer_fees"}
    pct = percentile(s.stud_fee_usd, peer_fees)
    return pct, {
        "stud_fee_usd": s.stud_fee_usd,
        "peer_fee_median": statistics.median(peer_fees),
        "peer_n": len(peer_fees),
    }


def component_market_efficiency(
    s: StallionSnapshot,
    roster: Sequence[StallionSnapshot],
) -> tuple[Optional[float], dict]:
    """Tier 1 signal. Ratio of yearling average ÷ stud fee, peer-percentiled.

    Returns (percentile_0_100, explain) if Keeneland data exists for this
    stallion AND a fee is present; otherwise (None, explain).
    """
    # Need both a fee and Keeneland data keyed on stallion name.
    if s.stud_fee_usd is None:
        return None, {"reason": "no_fee_for_ratio"}
    keene = KEENELAND_BY_SIRE.get(s.name) or KEENELAND_BY_SIRE.get(_normalize_lookup_key(s.name))
    if not keene:
        return None, {"reason": "no_keeneland_data_for_stallion"}

    yearling_avg = keene["yearling_avg_usd"]
    n = keene["n"]
    ratio = yearling_avg / s.stud_fee_usd

    # Peer cohort: every stallion in the roster that also has Keeneland data
    # AND a fee. This is the ONLY peer-ranking that matters for this signal —
    # fee-band and maturity-stage don't apply here.
    peer_ratios = []
    for p in roster:
        if p.stud_fee_usd is None or p.name == s.name:
            continue
        peer_keene = KEENELAND_BY_SIRE.get(p.name) or KEENELAND_BY_SIRE.get(_normalize_lookup_key(p.name))
        if peer_keene:
            peer_ratios.append(peer_keene["yearling_avg_usd"] / p.stud_fee_usd)

    pct = percentile(ratio, peer_ratios) if peer_ratios else 50.0

    sale_history = keene.get("sale_history", [])
    source_summary = (
        ", ".join(f"{h['sale']}: n={h['n']}, avg=${h['yearling_avg_usd']:,}"
                  for h in sale_history)
        if sale_history else "Keeneland September 2025"
    )

    return pct, {
        "yearling_avg_usd": yearling_avg,
        "yearlings_sold_n": n,
        "stud_fee_usd": s.stud_fee_usd,
        "ratio": round(ratio, 2),
        "peer_n": len(peer_ratios),
        "peer_median_ratio": round(
            sorted(peer_ratios)[len(peer_ratios) // 2] if peer_ratios else 0, 2
        ),
        "sales_covered": len(sale_history),
        "source": source_summary,
    }


def component_2yo_market_efficiency(
    s: StallionSnapshot,
    roster: Sequence[StallionSnapshot],
) -> tuple[Optional[float], dict]:
    """Tier 1 signal, OBS side. Ratio of 2YO-in-training avg ÷ stud fee,
    peer-percentiled. Reflects how the sire's offspring hold up after
    training — a more conviction-rich signal than yearling price alone.
    """
    if s.stud_fee_usd is None:
        return None, {"reason": "no_fee_for_ratio"}
    obs = OBS_BY_SIRE.get(s.name) or OBS_BY_SIRE.get(_normalize_lookup_key(s.name))
    if not obs:
        return None, {"reason": "no_obs_data_for_stallion"}

    twoyo_avg = obs["price_avg_usd"]
    n = obs["n"]
    ratio = twoyo_avg / s.stud_fee_usd

    peer_ratios = []
    for p in roster:
        if p.stud_fee_usd is None or p.name == s.name:
            continue
        peer_obs = OBS_BY_SIRE.get(p.name) or OBS_BY_SIRE.get(_normalize_lookup_key(p.name))
        if peer_obs:
            peer_ratios.append(peer_obs["price_avg_usd"] / p.stud_fee_usd)

    pct = percentile(ratio, peer_ratios) if peer_ratios else 50.0

    sale_history = obs.get("sale_history", [])
    source_summary = (
        ", ".join(f"{h['sale']}: n={h['n']}, avg=${h['price_avg_usd']:,}"
                  for h in sale_history)
        if sale_history else "OBS (single sale)"
    )

    # Temporal shift: compare the most recent sale's per-sire ratio to the
    # earlier-sale ratios. Gate the flag on:
    #   - ≥30% relative change
    #   - latest sale has n >= 5 (suppresses noisy small-sample swings)
    #   - prior combined has n >= 5 (a single prior 2-count sample isn't a trend)
    temporal_shift = None
    SHIFT_MIN_N = 5
    SHIFT_MIN_PCT = 0.30
    if len(sale_history) >= 2 and s.stud_fee_usd:
        per_sale_ratios = [
            {"sale": h["sale"], "n": h["n"],
             "ratio": round(h["price_avg_usd"] / s.stud_fee_usd, 2)}
            for h in sale_history
        ]
        latest = per_sale_ratios[-1]
        prior = per_sale_ratios[:-1]
        prior_n = sum(p["n"] for p in prior)
        if (latest["n"] >= SHIFT_MIN_N and prior_n >= SHIFT_MIN_N):
            prior_mean_ratio = sum(p["ratio"] * p["n"] for p in prior) / prior_n
            pct_change = (latest["ratio"] - prior_mean_ratio) / prior_mean_ratio \
                if prior_mean_ratio > 0 else 0
            if abs(pct_change) >= SHIFT_MIN_PCT:
                direction = "up" if pct_change > 0 else "down"
                temporal_shift = {
                    "direction": direction,
                    "prior_ratio": round(prior_mean_ratio, 2),
                    "prior_n": prior_n,
                    "latest_ratio": latest["ratio"],
                    "latest_n": latest["n"],
                    "pct_change": round(pct_change * 100, 1),
                    "latest_sale": latest["sale"],
                    "message": (
                        f"2YO-market ratio moved {direction} {abs(round(pct_change*100,1))}% "
                        f"at {latest['sale']} "
                        f"({round(prior_mean_ratio,1)}x over n={prior_n} → "
                        f"{latest['ratio']}x over n={latest['n']})"
                    ),
                }

    return pct, {
        "twoyo_avg_usd": twoyo_avg,
        "twoyos_sold_n": n,
        "stud_fee_usd": s.stud_fee_usd,
        "ratio": round(ratio, 2),
        "peer_n": len(peer_ratios),
        "peer_median_ratio": round(
            sorted(peer_ratios)[len(peer_ratios) // 2] if peer_ratios else 0, 2
        ),
        "sales_covered": len(sale_history),
        "temporal_shift": temporal_shift,
        "source": source_summary,
    }


def component_maturity_context(s: StallionSnapshot) -> tuple[float, dict]:
    """Simple maturity-stage mapping. Proven > early_proven > senior > first.

    Rationale: very-young sires carry high uncertainty (penalized); senior
    sires are past their commercial peak (slight penalty); established and
    early-proven sires with visible results are the commercial sweet spot.
    """
    stage = maturity_stage(s.entered_stud_year)
    mapping = {
        "first_crop":   35,   # high uncertainty
        "second_crop":  50,   # first results incoming
        "early_proven": 70,   # results visible
        "established":  75,   # sweet spot
        "senior":       55,   # past peak
        "unknown":      50,
    }
    return mapping[stage], {"stage": stage,
                            "years_at_stud": CURRENT_YEAR - s.entered_stud_year
                                             if s.entered_stud_year else None}


# ---------------------------------------------------------------------------
# TIER 0 aggregation
# ---------------------------------------------------------------------------

TIER_0_WEIGHTS = {
    "pedigree_prestige":  50,
    "fee_band_position":  30,
    "maturity_context":   20,
}

# Partial Tier 1 — only Keeneland data (yearling market only).
PARTIAL_TIER_1_WEIGHTS = {
    "market_efficiency":  40,
    "pedigree_prestige":  30,
    "fee_band_position":  20,
    "maturity_context":   10,
}

# Full Tier 1 — both Keeneland (yearling) AND OBS (2YO) data available.
# Market signals combine to 45%; pedigree and fee context hold 45%; maturity 10%.
FULL_TIER_1_WEIGHTS = {
    "market_efficiency":       25,   # yearling-market, Keeneland
    "twoyo_market_efficiency": 20,   # 2YO-in-training market, OBS
    "pedigree_prestige":       25,
    "fee_band_position":       20,
    "maturity_context":        10,
}

# OBS-only Tier 1 — OBS data but no Keeneland data (common for mid-tier sires
# whose yearlings don't make Book 1 at Keeneland but DO sell at OBS).
OBS_ONLY_TIER_1_WEIGHTS = {
    "twoyo_market_efficiency": 40,
    "pedigree_prestige":       30,
    "fee_band_position":       20,
    "maturity_context":        10,
}


def letter_grade(score: float) -> str:
    if score >= 85: return "A+"
    if score >= 75: return "A"
    if score >= 60: return "B"
    if score >= 40: return "C"
    if score >= 25: return "D"
    return "F"


@dataclass
class ScoreResult:
    name: str
    score: float
    grade: str
    model_version: str
    tier: str
    peer_group: dict
    components: dict
    confidence: str
    notes: list[str] = field(default_factory=list)


def score_commercial_appeal(s: StallionSnapshot,
                            roster: Sequence[StallionSnapshot]) -> ScoreResult:
    """Commercial Appeal / partial Tier 1 score.

    When Keeneland yearling-avg data is present for the stallion AND a fee
    is available, the Market Efficiency component is used and the scorer
    reports tier='partial_tier1'. Otherwise the pure Tier 0 formula runs.
    """
    peers = [p for p in roster if in_peer_set(s, p)]
    notes: list[str] = []
    if len(peers) < 4:
        notes.append(
            f"insufficient_peer_data (only {len(peers)} peers; "
            f"score shown with wider tolerance)"
        )

    prestige_pct, prestige_ex = component_pedigree_prestige(s)
    feeband_pct, feeband_ex = component_fee_band_position(s, peers)
    maturity_pct, maturity_ex = component_maturity_context(s)
    market_pct, market_ex = component_market_efficiency(s, roster)       # Keeneland
    twoyo_pct, twoyo_ex = component_2yo_market_efficiency(s, roster)     # OBS

    have_kee = market_pct is not None
    have_obs = twoyo_pct is not None

    # Pick weights based on which market signals are available.
    if have_kee and have_obs:
        W, tier_label = FULL_TIER_1_WEIGHTS, "tier1_full"
    elif have_kee:
        W, tier_label = PARTIAL_TIER_1_WEIGHTS, "tier1_keeneland_only"
    elif have_obs:
        W, tier_label = OBS_ONLY_TIER_1_WEIGHTS, "tier1_obs_only"
    else:
        W, tier_label = TIER_0_WEIGHTS, "commercial_appeal"

    components: dict = {}
    if "market_efficiency" in W and have_kee:
        components["market_efficiency"] = {
            "percentile": market_pct,
            "weight": W["market_efficiency"],
            "points": round(W["market_efficiency"] * market_pct / 100, 2),
            "inputs": market_ex,
        }
    if "twoyo_market_efficiency" in W and have_obs:
        components["twoyo_market_efficiency"] = {
            "percentile": twoyo_pct,
            "weight": W["twoyo_market_efficiency"],
            "points": round(W["twoyo_market_efficiency"] * twoyo_pct / 100, 2),
            "inputs": twoyo_ex,
        }
    components["pedigree_prestige"] = {
        "percentile": prestige_pct,
        "weight": W["pedigree_prestige"],
        "points": round(W["pedigree_prestige"] * prestige_pct / 100, 2),
        "inputs": prestige_ex,
    }
    components["fee_band_position"] = {
        "percentile": feeband_pct,
        "weight": W["fee_band_position"],
        "points": round(W["fee_band_position"] * feeband_pct / 100, 2),
        "inputs": feeband_ex,
    }
    components["maturity_context"] = {
        "percentile": maturity_pct,
        "weight": W["maturity_context"],
        "points": round(W["maturity_context"] * maturity_pct / 100, 2),
        "inputs": maturity_ex,
    }

    score = sum(c["points"] for c in components.values())

    # Confidence: best when we have both market signals with non-trivial samples.
    kee_n = market_ex.get("yearlings_sold_n", 0) if have_kee else 0
    obs_n = twoyo_ex.get("twoyos_sold_n", 0) if have_obs else 0
    if have_kee and have_obs and (kee_n >= 5 or obs_n >= 5):
        confidence = "high"
    elif (have_kee and kee_n >= 5) or (have_obs and obs_n >= 5):
        confidence = "medium"
    elif have_kee or have_obs:
        confidence = "low"
    elif len(peers) < 4 or s.stud_fee_usd is None:
        confidence = "low"
    else:
        confidence = "medium"

    if have_kee and kee_n < 5:
        notes.append(f"keeneland_small_sample (n={kee_n} yearlings)")
    if have_obs and obs_n < 5:
        notes.append(f"obs_small_sample (n={obs_n} 2YOs)")

    return ScoreResult(
        name=s.name,
        score=round(score, 1),
        grade=letter_grade(score),
        model_version=MODEL_VERSION,
        tier=tier_label,
        peer_group={
            "fee_band": fee_band(s.stud_fee_usd),
            "maturity_stage": maturity_stage(s.entered_stud_year),
            "peer_n": len(peers),
        },
        components=components,
        confidence=confidence,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# TIER 1 — Full Value Score (stubbed)
# ---------------------------------------------------------------------------

TIER_1_WEIGHTS = {
    "market_efficiency":        40,
    "progeny_output":           25,
    "strike_rate":              15,
    "black_type_efficiency":    10,
    "trajectory":               10,
}


def score_value(s: StallionSnapshot, roster: Sequence[StallionSnapshot]) -> ScoreResult:
    """Tier 1 Value Score. Not computable until progeny facts are ingested."""
    missing = [f for f in (
        "yearling_avg_usd", "progeny_earnings_usd", "foals_of_racing_age",
        "starters", "winners", "stakes_winners",
    ) if getattr(s, f) is None]
    if missing:
        raise NotImplementedError(
            f"Tier 1 Value Score needs facts not yet ingested: {missing}. "
            f"Use score_commercial_appeal() for Tier 0 today."
        )
    raise NotImplementedError(
        "Tier 1 scoring logic to be implemented once progeny-data scraper lands. "
        "See value-score-model.md §4 and §5 for the formula."
    )
