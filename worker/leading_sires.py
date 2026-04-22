"""
Sire-prestige lookup — v2, computed from real Keeneland 2025 September sale data.

This replaces the provisional hand-curated tier list. The ranking here is
computed from an auditable quantitative signal: total gross yearling sales
revenue per sire (n * yearling_avg) from the first 500 hips of the 2025
Keeneland September sale. This captures market conviction — sires whose
foals the market actually paid serious money for, aggregated over 2–30+
transactions.

Caveats:
  * Sample bias: only the first 500 hips (Book 1 / premium tier) captured;
    a full-sale ingest will shift the rankings somewhat at the mid-tier.
  * Single-sale bias: one year at one venue. A production version should
    aggregate Keeneland + Fasig-Tipton + OBS over 2–3 seasons.
  * Sparse sample suppression: sires with fewer than 2 yearlings sold are
    intentionally excluded from 'leading' and 'proven' tiers — not enough
    market signal.

The Broodmare-sire (BMS) lookup is kept as a tiny hand-curated list of
all-time elite BMS lines. Replace with computed BMS rankings from Jockey
Club / BloodHorse publications when that ingestion lands.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

HERE = Path(__file__).parent
BLOODHORSE_FILE = HERE / "bloodhorse-leading-sires-2025.json"
KEENE_FILE = HERE / "keeneland-yearling-avgs.json"


def _load_bloodhorse_tiers() -> dict[str, str]:
    """Return {sire_name: tier} from BloodHorse's 2025 progeny-earnings ranking.

    This is the AUTHORITATIVE prestige source — sires ranked by actual race
    earnings of their progeny, not yearling sale prices. Leagues more defensible
    than our Keeneland-derived fallback because it reflects on-track performance
    directly.
    """
    if not BLOODHORSE_FILE.exists():
        return {}
    data = json.loads(BLOODHORSE_FILE.read_text())
    return {s["sire"]: s["tier"] for s in data["sires"]}


def _load_keeneland_tiers() -> dict[str, str]:
    """Fallback: tiers computed from Keeneland yearling sale gross. Used only
    if the BloodHorse file is missing."""
    if not KEENE_FILE.exists():
        return {}
    sires = json.loads(KEENE_FILE.read_text())["per_sire"]
    scored = [(s["sire"], s["n"] * s["yearling_avg_usd"], s["n"]) for s in sires]
    scored.sort(key=lambda x: x[1], reverse=True)
    total = len(scored)
    leading_cut = max(1, total // 10)
    proven_cut = max(leading_cut + 1, total // 3)
    tiers: dict[str, str] = {}
    for i, (name, gross, n) in enumerate(scored):
        if i < leading_cut:
            tiers[name] = "leading"
        elif i < proven_cut and n >= 2:
            tiers[name] = "proven"
    return tiers


# Prefer BloodHorse; fall through to Keeneland-derived if BH file is missing.
SIRE_TIERS: dict[str, str] = _load_bloodhorse_tiers() or _load_keeneland_tiers()


BMS_EARNINGS_FILE = HERE / "bloodhorse-bms-earnings-2026.json"
BMS_SALES_FILE = HERE / "bloodhorse-bms-2025.json"   # fallback


def _load_bms_tiers() -> dict[str, str]:
    """Load BMS tiers from BloodHorse. Prefer the earnings-based list
    (progeny-of-daughters earnings, 2026 YTD); fall back to the sales-based
    list if the earnings file is missing.

    Rank 1-15 = leading, 16-50 = proven.
    """
    source = BMS_EARNINGS_FILE if BMS_EARNINGS_FILE.exists() else BMS_SALES_FILE
    if not source.exists():
        return {}
    data = json.loads(source.read_text())
    out: dict[str, str] = {}
    for row in data["bms"]:
        if row.get("tier"):
            out[row["sire"]] = row["tier"]
            canon = row["sire"]
            for tag in ("(IRE)","(ARG)","(JPN)","(GB)","(AUS)","(NZ)","(BRZ)","(GER)","(FR)"):
                pretty = "(" + tag[1].upper() + tag[2:-1].lower() + ")"
                canon = canon.replace(tag, pretty)
            if canon != row["sire"]:
                out[canon] = row["tier"]
    return out


BROODMARE_SIRE_TIERS: dict[str, str] = _load_bms_tiers()

SIRE_TIER_POINTS = {"leading": 5, "proven": 2}
BMS_TIER_POINTS = {"leading": 3, "proven": 1}


def _normalize_sire_name(name: str) -> str:
    """Match Keeneland's '(ARG)' uppercase to our '(Arg)' canonical form."""
    if not name:
        return name
    # Canonical form for country tags.
    out = name
    for tag in ("(ARG)", "(IRE)", "(JPN)", "(GB)", "(AUS)", "(NZ)", "(BRZ)", "(GER)", "(FR)"):
        pretty = "(" + tag[1].upper() + tag[2:-1].lower() + ")"
        out = out.replace(tag, pretty)
    return out


def sire_points(name: str) -> int:
    if not name:
        return 0
    tier = SIRE_TIERS.get(name) or SIRE_TIERS.get(_normalize_sire_name(name))
    # Also try the reverse — our canonical "(Arg)" matched against Keeneland "(ARG)".
    if not tier:
        for variant in (name.upper(), name.replace("(Arg)", "(ARG)").replace("(Ire)", "(IRE)")):
            tier = SIRE_TIERS.get(variant)
            if tier:
                break
    return SIRE_TIER_POINTS.get(tier, 0)


def bms_points(name: str) -> int:
    if not name:
        return 0
    return BMS_TIER_POINTS.get(BROODMARE_SIRE_TIERS.get(name), 0)


def tier_of(name: str) -> Optional[str]:
    """Exposed for tests / debugging."""
    return SIRE_TIERS.get(name) or SIRE_TIERS.get(_normalize_sire_name(name))


if __name__ == "__main__":
    print(f"Loaded {len(SIRE_TIERS)} sires with tiers from Keeneland data")
    leading = sorted([n for n, t in SIRE_TIERS.items() if t == "leading"])
    proven = sorted([n for n, t in SIRE_TIERS.items() if t == "proven"])
    print(f"Leading ({len(leading)}): {', '.join(leading)}")
    print(f"Proven ({len(proven)}): {', '.join(proven)}")
