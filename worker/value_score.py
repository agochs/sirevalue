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

# Book Quality (#5) intentionally not implemented as a scoring component.
# A defensible book-quality measure requires actual mare-by-mare records
# (career earnings, black-type produced, n stakes-winning offspring) for
# each mare in each stallion's annual book. We don't have that data
# ingested today. A proxy — "damsires of his progeny that made it to sale"
# — has obvious survival bias and is not real book quality.
#
# Real data sources (each requires its own scraper + ingest pipeline):
#   - The Jockey Club's annual "Report of Mares Bred" — published per stallion
#   - BloodHorse Stallion Register's per-stallion mares-bred tab
#   - Equineline mare reports (paywall)
# Until one of those is wired in, book_quality stays out of the formula.


# ---------------------------------------------------------------------------
# Time-weighted + cohort-adjusted aggregates  (model improvements #7 + #8)
# ---------------------------------------------------------------------------
# The flat `yearling_avg_usd` / `price_avg_usd` per sire weights every sale
# equally. That hides two things:
#   1. RECENCY — a 2022 yearling avg is less informative for 2026 scoring than
#      a 2025 average. We apply exponential decay weight = DECAY^(year_lag).
#   2. COHORT YEAR EFFECT — a strong 2025 sale season inflates everyone's
#      average; that's a market-wide move, not a stallion-quality signal. We
#      subtract the per-year cross-sire MEDIAN before aggregating, so each
#      sale's contribution is "premium over the year's median."
#
# Output attached per-sire: `adjusted_avg_usd` — a recency-weighted, cohort-
# adjusted estimate, expressed back in dollar units (year-median + premium).
# Components below use this in place of the flat average for percentile
# comparison; the flat average is preserved on each record for display.

import re as _re
RECENCY_DECAY = 0.85  # weight = DECAY ** years_since_sale; tunable

def _extract_year(sale_label: str) -> Optional[int]:
    if not sale_label:
        return None
    m = _re.search(r"\b(20\d{2})\b", sale_label)
    return int(m.group(1)) if m else None


def _add_adjusted_avg(by_sire: dict, price_field: str) -> None:
    """Compute year_medians, then attach adjusted_avg_usd to every sire
    record in by_sire (mutates in place). `price_field` is either
    'yearling_avg_usd' (Keeneland lookup) or 'price_avg_usd' (OBS)."""
    # Year medians: across all sires, all sales tagged with that year
    year_to_prices: dict[int, list[float]] = {}
    for record in {id(r): r for r in by_sire.values()}.values():   # dedup the dual-key alias
        for h in record.get("sale_history") or []:
            y = _extract_year(h.get("sale", ""))
            if y is None:
                continue
            price = h.get(price_field) or 0
            n = h.get("n") or 0
            # Repeat per-hip so the median reflects per-hip distribution
            year_to_prices.setdefault(y, []).extend([price] * n)
    year_median = {}
    for y, prices in year_to_prices.items():
        if prices:
            prices_sorted = sorted(prices)
            year_median[y] = prices_sorted[len(prices_sorted) // 2]

    # Per-sire: compute year-adjusted, recency-weighted avg
    seen_records = set()
    for record in by_sire.values():
        rid = id(record)
        if rid in seen_records:
            continue
        seen_records.add(rid)
        history = record.get("sale_history") or []
        if not history:
            record["adjusted_avg_usd"] = record.get(price_field)
            record["year_medians_used"] = {}
            continue
        weighted_premium = 0.0   # year-adjusted price premium, weighted
        weighted_baseline = 0.0  # the year-median baseline, weighted
        weight_sum = 0.0
        years_used = {}
        for h in history:
            sale = h.get("sale", "")
            y = _extract_year(sale)
            n = h.get("n") or 0
            avg = h.get(price_field) or 0
            if y is None or n <= 0 or avg <= 0:
                continue
            baseline = year_median.get(y, avg)
            decay_w = RECENCY_DECAY ** max(0, CURRENT_YEAR - y)
            w = n * decay_w
            weighted_premium  += (avg - baseline) * w
            weighted_baseline += baseline * w
            weight_sum        += w
            years_used[y] = round(baseline, 2)
        if weight_sum > 0:
            record["adjusted_avg_usd"] = round((weighted_premium + weighted_baseline) / weight_sum, 2)
        else:
            record["adjusted_avg_usd"] = record.get(price_field)
        record["year_medians_used"] = years_used


# Run the adjustment on both lookups at import time
_add_adjusted_avg(KEENELAND_BY_SIRE, "yearling_avg_usd")
_add_adjusted_avg(OBS_BY_SIRE,       "price_avg_usd")


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
    farm: Optional[str] = None   # used for regional peer-set splitting (#6)

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


# Region inference from farm name. Used by the peer-set splitter so a $7.5K
# NY-bred-market stallion is compared to other NY/regional stallions, not
# to KY commercial $7.5K stallions whose foals enter different premium
# programs and have a different buyer base.
def farm_to_region(farm: Optional[str]) -> str:
    if not farm:
        return "unknown"
    f = farm.lower()
    # NY: Sequel, McMahon, anything with "new york" or "saratoga"
    if "sequel" in f or "mcmahon" in f or "new york" in f or "saratoga" in f:
        return "NY"
    # CA: Rancho San Miguel, Harris Farms, anything with "california" or "san miguel"
    if "rancho san miguel" in f or "harris farms" in f or "california" in f:
        return "CA"
    # FL: Ocala, Bridlewood, Journeyman, Adena Springs South
    if "ocala" in f or "bridlewood" in f or "journeyman" in f or "adena springs south" in f:
        return "FL"
    # MD/PA mid-Atlantic regional: Northview
    if "northview" in f or "country life" in f:
        return "MD"
    # Default: treat as KY commercial market (the bulk of our roster)
    return "KY"


def in_peer_set(target: StallionSnapshot, candidate: StallionSnapshot,
                same_region: bool = False) -> bool:
    """Default peer set: same fee_band × same maturity_stage. With same_region
    True, additionally restrict to same region — used for a tighter "regional
    peer" signal when the region's bucket is dense enough."""
    if (fee_band(candidate.stud_fee_usd) != fee_band(target.stud_fee_usd)
        or maturity_stage(candidate.entered_stud_year) != maturity_stage(target.entered_stud_year)
        or candidate.name == target.name):
        return False
    if same_region:
        target_region    = farm_to_region(getattr(target, "farm", None))
        candidate_region = farm_to_region(getattr(candidate, "farm", None))
        if target_region != candidate_region:
            return False
    return True


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

    yearling_avg     = keene["yearling_avg_usd"]                      # flat-weighted (display)
    adjusted_avg     = keene.get("adjusted_avg_usd", yearling_avg)    # recency + cohort-adjusted (scoring)
    n = keene["n"]
    ratio = adjusted_avg / s.stud_fee_usd

    # Peer cohort: every stallion in the roster that also has Keeneland data
    # AND a fee. We percentile on the SAME adjusted ratio so all comparisons
    # are apples-to-apples post-recency-decay and post-year-effect.
    peer_ratios = []
    for p in roster:
        if p.stud_fee_usd is None or p.name == s.name:
            continue
        peer_keene = KEENELAND_BY_SIRE.get(p.name) or KEENELAND_BY_SIRE.get(_normalize_lookup_key(p.name))
        if peer_keene:
            p_adj = peer_keene.get("adjusted_avg_usd") or peer_keene["yearling_avg_usd"]
            peer_ratios.append(p_adj / p.stud_fee_usd)

    # ---- Empirical-Bayes shrinkage (#9, partial) ----
    # Without shrinkage a stallion with n=2 sales at a 30× ratio dominates the
    # percentile ranking — that's not a robust signal. We pull the observed
    # ratio toward the peer mean by a factor that depends on n: small n →
    # heavy shrinkage; large n → trust observed. Same James-Stein form used
    # everywhere; B = within_var / (within_var + n × between_var).
    # NOTE: this is empirical Bayes at one level. A FULL hierarchical Bayesian
    # model would also pool across years and across peer-groups (regions, fee
    # bands) with formal credible intervals — that's a deeper refactor.
    shrink_factor = 0.0
    shrunk_ratio = ratio
    if len(peer_ratios) >= 4:
        peer_mean = statistics.mean(peer_ratios)
        peer_var  = statistics.pvariance(peer_ratios)
        # Within-stallion variance proxy: scale of the peer distribution. We
        # don't have per-sire intra-cohort variance with current data, so use
        # peer variance as a conservative within-var estimate.
        within_var_proxy = peer_var
        shrunk_ratio, shrink_factor = shrink(ratio, peer_mean, peer_var, n, within_var_proxy)

    # Percentile on the SHRUNK ratio (so n=2 outliers don't dominate)
    pct = percentile(shrunk_ratio, peer_ratios) if peer_ratios else 50.0

    sale_history = keene.get("sale_history", [])
    source_summary = (
        ", ".join(f"{h['sale']}: n={h['n']}, avg=${h['yearling_avg_usd']:,}"
                  for h in sale_history)
        if sale_history else "Keeneland September 2025"
    )

    return pct, {
        "yearling_avg_usd": yearling_avg,
        "yearling_adjusted_avg_usd": round(adjusted_avg, 2),
        "yearlings_sold_n": n,
        "stud_fee_usd": s.stud_fee_usd,
        "ratio": round(ratio, 2),
        "shrunk_ratio": round(shrunk_ratio, 2),
        "shrinkage_factor": round(shrink_factor, 3),   # 0=trust observed, 1=fully shrunk to peer mean
        "peer_n": len(peer_ratios),
        "peer_median_ratio": round(
            sorted(peer_ratios)[len(peer_ratios) // 2] if peer_ratios else 0, 2
        ),
        "sales_covered": len(sale_history),
        "year_medians": keene.get("year_medians_used", {}),
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

    twoyo_avg     = obs["price_avg_usd"]                        # flat-weighted (display)
    adjusted_avg  = obs.get("adjusted_avg_usd", twoyo_avg)      # recency + cohort-adjusted (scoring)
    n = obs["n"]
    ratio = adjusted_avg / s.stud_fee_usd

    peer_ratios = []
    for p in roster:
        if p.stud_fee_usd is None or p.name == s.name:
            continue
        peer_obs = OBS_BY_SIRE.get(p.name) or OBS_BY_SIRE.get(_normalize_lookup_key(p.name))
        if peer_obs:
            p_adj = peer_obs.get("adjusted_avg_usd") or peer_obs["price_avg_usd"]
            peer_ratios.append(p_adj / p.stud_fee_usd)

    # Empirical-Bayes shrinkage — same logic as Keeneland component above.
    # Small-sample 2YO ratios get pulled toward peer mean.
    shrink_factor = 0.0
    shrunk_ratio = ratio
    if len(peer_ratios) >= 4:
        peer_mean = statistics.mean(peer_ratios)
        peer_var  = statistics.pvariance(peer_ratios)
        within_var_proxy = peer_var
        shrunk_ratio, shrink_factor = shrink(ratio, peer_mean, peer_var, n, within_var_proxy)

    pct = percentile(shrunk_ratio, peer_ratios) if peer_ratios else 50.0

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
        "twoyo_adjusted_avg_usd": round(adjusted_avg, 2),
        "twoyos_sold_n": n,
        "stud_fee_usd": s.stud_fee_usd,
        "ratio": round(ratio, 2),
        "shrunk_ratio": round(shrunk_ratio, 2),
        "shrinkage_factor": round(shrink_factor, 3),
        "peer_n": len(peer_ratios),
        "peer_median_ratio": round(
            sorted(peer_ratios)[len(peer_ratios) // 2] if peer_ratios else 0, 2
        ),
        "sales_covered": len(sale_history),
        "year_medians": obs.get("year_medians_used", {}),
        "temporal_shift": temporal_shift,
        "source": source_summary,
    }


def component_pinhook_efficiency(
    s: StallionSnapshot,
    roster: Sequence[StallionSnapshot],
) -> tuple[Optional[float], dict]:
    """Pinhook lift signal: yearling-to-2YO appreciation, percentile vs peers
    who also have both sale-side data points.

    Lift = twoyo_avg / yearling_avg. Both averages are pulled from the same
    in-process Keeneland (yearling) and OBS (2YO) lookups that the market_*
    components use, so this component shares their data dependency.

    Returns (None, dict) when either side is missing or sample is below the
    minimum (smaller of yearling_n, twoyo_n must be ≥3) — the scorer then
    skips this component for the stallion.

    This is the platform's value-creation thesis as a scoring signal: a
    stallion whose foals appreciate between sales is materially different
    from one whose don't, even at the same fee level.
    """
    # Pull yearling and 2YO averages from the per-sire lookups
    kee = KEENELAND_BY_SIRE.get(s.name) or KEENELAND_BY_SIRE.get(_normalize_lookup_key(s.name))
    obs = OBS_BY_SIRE.get(s.name) or OBS_BY_SIRE.get(_normalize_lookup_key(s.name))
    if not kee or not obs:
        return None, {"reason": "missing_yearling_or_2yo_data"}
    # Use ADJUSTED averages (recency-weighted, year-effect-cancelled) so
    # the lift ratio reflects stallion quality across cycles, not just the
    # mix of sale years their progeny happened to be in.
    yearling_avg = kee.get("adjusted_avg_usd") or kee.get("yearling_avg_usd") or 0
    yearling_n   = kee.get("n") or 0
    twoyo_avg    = obs.get("adjusted_avg_usd") or obs.get("price_avg_usd") or 0
    twoyo_n      = obs.get("n") or 0
    if yearling_avg <= 0 or twoyo_avg <= 0:
        return None, {"reason": "zero_average_price"}
    if min(yearling_n, twoyo_n) < 3:
        return None, {"reason": f"sample_too_small (yrl_n={yearling_n}, 2yo_n={twoyo_n})"}

    lift_ratio = twoyo_avg / yearling_avg
    lift_pct = (lift_ratio - 1) * 100

    # Peer lift_ratios — same adjusted-vs-adjusted comparison
    peer_lifts = []
    for p in roster:
        if p.name == s.name:
            continue
        pk = KEENELAND_BY_SIRE.get(p.name) or KEENELAND_BY_SIRE.get(_normalize_lookup_key(p.name))
        po = OBS_BY_SIRE.get(p.name) or OBS_BY_SIRE.get(_normalize_lookup_key(p.name))
        if not pk or not po:
            continue
        py = pk.get("adjusted_avg_usd") or pk.get("yearling_avg_usd") or 0
        pt = po.get("adjusted_avg_usd") or po.get("price_avg_usd") or 0
        py_n = pk.get("n") or 0
        pt_n = po.get("n") or 0
        if py <= 0 or pt <= 0 or min(py_n, pt_n) < 3:
            continue
        peer_lifts.append(pt / py)

    pct = percentile(lift_ratio, peer_lifts) if peer_lifts else 50.0

    return pct, {
        "yearling_avg_usd": yearling_avg,
        "twoyo_avg_usd":    twoyo_avg,
        "yearling_n":       yearling_n,
        "twoyo_n":          twoyo_n,
        "lift_ratio":       round(lift_ratio, 2),
        "lift_pct":         round(lift_pct, 1),
        "peer_n_with_pinhook": len(peer_lifts),
        "peer_median_lift": round(
            sorted(peer_lifts)[len(peer_lifts) // 2] if peer_lifts else 0, 2
        ),
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
# Market signals + pinhook lift combine to 50%; pedigree+fee 40%; maturity 10%.
# Pinhook efficiency is the platform's value-creation thesis encoded directly
# into scoring: a stallion whose foals appreciate yearling→2YO is rewarded
# beyond what the absolute-price market signals alone would credit.
FULL_TIER_1_WEIGHTS = {
    "market_efficiency":       20,   # yearling-market $-level (Keeneland)
    "twoyo_market_efficiency": 15,   # 2YO-market $-level (OBS)
    "pinhook_efficiency":      15,   # yearling→2YO appreciation
    "pedigree_prestige":       22,
    "fee_band_position":       18,
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


def score_uncertainty(peers_n: int, kee_n: int, obs_n: int, tier_label: str) -> float:
    """Estimated 1-sigma uncertainty around the score in points (0..100 scale).

    Heuristic:
      base 1.5 pts for any score
      + 4 pts if peer_n < 4 (peer percentile is volatile)
      + 3 pts if neither market signal is present (Tier 0 — no fact-grounding)
      + 3 pts if either market signal has n < 5 (small market sample)
    Capped at 12 pts so the band stays interpretable.

    Surfaces as ±N alongside the score on the UI; lets users see "this is an
    A but with a 6-point CI vs another A with a 2-point CI." Different grade
    of conviction in the same letter grade.
    """
    u = 1.5
    if peers_n < 4: u += 4.0
    if tier_label == "commercial_appeal": u += 3.0
    if (kee_n and kee_n < 5) or (obs_n and obs_n < 5): u += 3.0
    if not kee_n and not obs_n: u += 1.5  # no market data at all
    return round(min(u, 12.0), 1)


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
    score_uncertainty: float = 0.0   # ±N point band around the score
    notes: list[str] = field(default_factory=list)


def score_commercial_appeal(s: StallionSnapshot,
                            roster: Sequence[StallionSnapshot]) -> ScoreResult:
    """Commercial Appeal / partial Tier 1 score.

    When Keeneland yearling-avg data is present for the stallion AND a fee
    is available, the Market Efficiency component is used and the scorer
    reports tier='partial_tier1'. Otherwise the pure Tier 0 formula runs.
    """
    notes: list[str] = []
    # Try same-region peers first (tighter, more honest comparison). Fall back
    # to cross-region peers when the regional bucket is too thin to be
    # statistically meaningful (< 4 peers).
    region_label = farm_to_region(s.farm)
    region_peers = [p for p in roster if in_peer_set(s, p, same_region=True)]
    if len(region_peers) >= 4:
        peers = region_peers
        peer_scope = "region"
    else:
        peers = [p for p in roster if in_peer_set(s, p, same_region=False)]
        peer_scope = "cross-region"
        if region_peers:
            notes.append(
                f"region_peers_too_thin ({len(region_peers)} {region_label} peers; "
                f"falling back to {len(peers)} cross-region peers in same fee/stage)"
            )
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
    pinhook_pct, pinhook_ex = component_pinhook_efficiency(s, roster)    # lift = 2yo/yrl

    have_kee = market_pct is not None
    have_obs = twoyo_pct is not None
    have_pinhook = pinhook_pct is not None

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
    if "pinhook_efficiency" in W and have_pinhook:
        components["pinhook_efficiency"] = {
            "percentile": pinhook_pct,
            "weight": W["pinhook_efficiency"],
            "points": round(W["pinhook_efficiency"] * pinhook_pct / 100, 2),
            "inputs": pinhook_ex,
        }
    # Renormalization: if pinhook_efficiency was in W but the data wasn't
    # present (sample too small), redistribute its weight proportionally
    # across the remaining components so the score still spans 0..100.
    if "pinhook_efficiency" in W and not have_pinhook:
        dropped_weight = W["pinhook_efficiency"]
        remaining_total = sum(c["weight"] for c in components.values())
        if remaining_total > 0:
            scale = (remaining_total + dropped_weight) / remaining_total
            for k, c in components.items():
                c["weight"] = round(c["weight"] * scale, 1)
                c["points"] = round(c["weight"] * c["percentile"] / 100, 2)
        notes.append(
            f"pinhook_efficiency unavailable (small sample); "
            f"its {dropped_weight}% weight redistributed across remaining components"
        )
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

    uncertainty = score_uncertainty(len(peers), kee_n, obs_n, tier_label)

    return ScoreResult(
        name=s.name,
        score=round(score, 1),
        grade=letter_grade(score),
        model_version=MODEL_VERSION,
        tier=tier_label,
        score_uncertainty=uncertainty,
        peer_group={
            "fee_band": fee_band(s.stud_fee_usd),
            "maturity_stage": maturity_stage(s.entered_stud_year),
            "region": region_label,
            "peer_scope": peer_scope,                       # 'region' or 'cross-region'
            "region_peers_n": len(region_peers),
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
