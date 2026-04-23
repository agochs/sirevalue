"""
Combine the Spendthrift and WinStar dry-run CSVs into one file with a
unified schema and a `farm` column, then write a scores.json suitable
for the HTML stallion-card UI.
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from value_score import StallionSnapshot, score_commercial_appeal

HERE = Path(__file__).parent
SPENDTHRIFT_CSV = HERE / "spendthrift-dryrun.csv"
WINSTAR_CSV = HERE / "winstar-dryrun.csv"
LANESEND_CSV = HERE / "lanesend-dryrun.csv"
ASHFORD_CSV = HERE / "ashford-dryrun.csv"
THREECHIMNEYS_CSV = HERE / "threechimneys-dryrun.csv"
GAINESWAY_CSV = HERE / "gainesway-dryrun.csv"
HILLNDALE_CSV = HERE / "hillndale-dryrun.csv"
CLAIBORNE_CSV = HERE / "claiborne-dryrun.csv"
DARLEY_CSV = HERE / "darley-dryrun.csv"
TAYLORMADE_CSV = HERE / "taylormade-dryrun.csv"
AIRDRIE_CSV = HERE / "airdrie-dryrun.csv"
CALUMET_CSV = HERE / "calumet-dryrun.csv"
COMBINED_CSV = HERE / "rosters-combined.csv"
SCORES_JSON = HERE / "scores.json"
# BloodHorse Stallion Register enrichment — canonical bio data per stallion.
# Produced by enrich_from_bloodhorse.py. When present, BH values take
# precedence over CSV values for year_of_birth / color / height / entered-stud.
BH_BIOS_JSON = HERE / "bloodhorse-register-bios.json"

COMMON_COLS = [
    "farm", "name", "year_of_birth", "color", "height_hands",
    "sire", "dam", "damsire",
    "fee_usd", "fee_terms", "fee_qualifier",
    "entered_stud_year", "nominations",
    "source_url",
    "bloodhorse_url",
]

# BloodHorse / Jockey Club registered color codes -> reader-friendly names.
# These are the authoritative registered color categories; we don't invent new
# ones or guess. If BH returns a code not in this map, we preserve it verbatim.
BH_COLOR_CODES = {
    "b":      "Bay",
    "ch":     "Chestnut",
    "gr":     "Gray",
    "ro":     "Roan",
    "gr/ro":  "Gray/Roan",
    "blk":    "Black",
    "br":     "Brown",
    "dkb/br": "Dark Bay/Brown",
    "wh":     "White",
    "pal":    "Palomino",
}


def expand_bh_color(code):
    """Translate a BH color code ('b', 'dkb/br') to its registered name
    ('Bay', 'Dark Bay/Brown'). Returns the input unchanged for anything
    not in the known-codes map — better to show the raw value than to
    invent one."""
    if not code:
        return code
    return BH_COLOR_CODES.get(code.lower(), code)


def load_spendthrift() -> list[dict]:
    rows = []
    with SPENDTHRIFT_CSV.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append({
                "farm": "Spendthrift Farm",
                "name": r["name"],
                "year_of_birth": r.get("year_of_birth") or "",
                "color": r.get("color") or "",
                "height_hands": r.get("height_hands") or "",
                "sire": r.get("sire") or "",
                "dam": r.get("dam") or "",
                "damsire": r.get("damsire") or "",
                "fee_usd": r.get("fee_usd") or "",
                "fee_terms": r.get("fee_terms") or "",
                "fee_qualifier": r.get("fee_qualifier") or "",
                "entered_stud_year": r.get("entered_stud_year") or "",
                "nominations": r.get("nominations") or "",
                "source_url": (
                    f"https://www.spendthriftfarm.com/stallions/"
                    f"{r['name'].lower().replace(' ', '-').replace(chr(0x2019), '').replace(chr(0x2018), '')}/"
                ),
            })
    return rows


def load_lanesend() -> list[dict]:
    rows = []
    with LANESEND_CSV.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            slug = (
                r["name"].lower()
                .replace(" ", "")
                .replace(chr(0x2019), "")
                .replace("'", "")
                .replace(".", "")
                .replace("(arg)", "")
                .replace("(ire)", "")
            )
            rows.append({
                "farm": "Lane's End",
                "name": r["name"],
                "year_of_birth": "",
                "color": "",
                "height_hands": "",
                "sire": r.get("sire") or "",
                "dam": r.get("dam") or "",
                "damsire": r.get("damsire") or "",
                "fee_usd": r.get("fee_usd") or "",
                "fee_terms": r.get("fee_terms") or "",
                "fee_qualifier": r.get("fee_qualifier") or "",
                "entered_stud_year": "",
                "nominations": "",
                "source_url": f"https://lanesend.com/{slug}",
            })
    return rows


def load_ashford() -> list[dict]:
    """Coolmore America / Ashford Stud. Roster captured via the fee list on
    the main stallion-page summary; individual stallion pages are heavy
    marketing-first pages and pedigree wasn't extracted here (deferred)."""
    rows = []
    with ASHFORD_CSV.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            slug = r["name"].lower().replace(" ", "-").replace(chr(0x2019), "").replace("'", "")
            rows.append({
                "farm": "Ashford Stud (Coolmore America)",
                "name": r["name"],
                "year_of_birth": r.get("year_of_birth") or "",
                "color": r.get("color") or "",
                "height_hands": "",
                "sire": r.get("sire") or "",
                "dam": r.get("dam") or "",
                "damsire": r.get("damsire") or "",
                "fee_usd": r.get("fee_usd") or "",
                "fee_terms": r.get("fee_terms") or "",
                "fee_qualifier": r.get("fee_qualifier") or "",
                "entered_stud_year": "",
                "nominations": "",
                "source_url": f"https://coolmore.com/en/america/stallion/{slug}/",
            })
    return rows


def _load_generic_farm(csv_path: Path, farm_name: str, url_template: str) -> list[dict]:
    """Generic loader for the name+pedigree+fee CSVs. Used for simple farms
    where we don't have year/color/height data on the roster page."""
    rows = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            slug = r["name"].lower().replace(" ", "-").replace("'", "").replace("(", "").replace(")", "")
            rows.append({
                "farm": farm_name,
                "name": r["name"],
                "year_of_birth": r.get("year_of_birth") or "",
                "color": r.get("color") or "",
                "height_hands": "",
                "sire": r.get("sire") or "",
                "dam": r.get("dam") or "",
                "damsire": r.get("damsire") or "",
                "fee_usd": r.get("fee_usd") or "",
                "fee_terms": r.get("fee_terms") or "",
                "fee_qualifier": r.get("fee_qualifier") or "",
                "entered_stud_year": "",
                "nominations": "",
                "source_url": url_template.format(slug=slug),
            })
    return rows


def load_threechimneys() -> list[dict]:
    return _load_generic_farm(
        THREECHIMNEYS_CSV,
        "Three Chimneys Farm",
        "https://www.threechimneys.com/stallions/",
    )


def load_gainesway() -> list[dict]:
    return _load_generic_farm(
        GAINESWAY_CSV,
        "Gainesway Farm",
        "https://gainesway.com/stallions/",
    )


def load_hillndale() -> list[dict]:
    return _load_generic_farm(
        HILLNDALE_CSV,
        "Hill 'n' Dale Farms at Xalapa",
        "https://www.hillndalefarms.com/{slug}",
    )


def load_claiborne() -> list[dict]:
    return _load_generic_farm(
        CLAIBORNE_CSV,
        "Claiborne Farm",
        "https://claibornefarm.com/stallions/{slug}/",
    )


def load_darley() -> list[dict]:
    return _load_generic_farm(
        DARLEY_CSV,
        "Darley America (Godolphin)",
        "https://www.darleyamerica.com/stallions/our-stallions/{slug}",
    )


def load_taylormade() -> list[dict]:
    return _load_generic_farm(
        TAYLORMADE_CSV,
        "Taylor Made Stallions",
        "https://taylormadestallions.com/horse/{slug}/",
    )


def load_airdrie() -> list[dict]:
    return _load_generic_farm(
        AIRDRIE_CSV,
        "Airdrie Stud",
        "https://www.airdriestud.com/horse/{slug}/",
    )


def load_calumet() -> list[dict]:
    return _load_generic_farm(
        CALUMET_CSV,
        "Calumet Farm",
        "https://calumetfarm.com/stallions/",
    )


def load_winstar() -> list[dict]:
    rows = []
    with WINSTAR_CSV.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            slug = (
                r["name"].lower()
                .replace(" ", "-")
                .replace(chr(0x2019), "")
                .replace(chr(0x2018), "")
                .replace("'", "")
            )
            rows.append({
                "farm": "WinStar Farm",
                "name": r["name"],
                "year_of_birth": "",
                "color": "",
                "height_hands": "",
                "sire": r.get("sire") or "",
                "dam": r.get("dam") or "",
                "damsire": r.get("damsire") or "",
                "fee_usd": r.get("fee_usd") or "",
                "fee_terms": r.get("fee_terms") or "",
                "fee_qualifier": r.get("fee_qualifier") or "",
                "entered_stud_year": "",
                "nominations": "",
                "source_url": f"https://www.winstarfarm.com/horse/{slug}/",
            })
    return rows


def to_snapshot(row: dict) -> StallionSnapshot:
    return StallionSnapshot(
        name=row["name"],
        stud_fee_usd=int(row["fee_usd"]) if row["fee_usd"] else None,
        sire_name=row["sire"] or None,
        damsire_name=row["damsire"] or None,
        entered_stud_year=int(row["entered_stud_year"]) if row["entered_stud_year"] else None,
    )


def load_bh_bios() -> dict:
    """Returns the `resolved` dict from bloodhorse-register-bios.json, or {} if
    the enrichment file isn't present. Keys are stallion names."""
    if not BH_BIOS_JSON.exists():
        return {}
    try:
        data = json.loads(BH_BIOS_JSON.read_text())
    except Exception as e:
        print(f"WARNING: failed to read {BH_BIOS_JSON.name}: {e}")
        return {}
    return data.get("resolved", {}) or {}


def merge_bh_bios(rows: list[dict], bios: dict) -> dict:
    """Overlay BH Stallion Register facts onto roster rows IN-PLACE. BH is
    authoritative for year_of_birth / color / height_hands / entered_stud_year
    and we carry the bh_url through for citation on the stallion card.

    Returns a summary dict: {merged, conflicts_resolved, missing}.
    Disagreements are logged (BH wins, but audit trail matters).
    """
    stats = {"merged": 0, "conflicts_resolved": [], "missing": []}
    for row in rows:
        bio = bios.get(row["name"])
        if not bio:
            stats["missing"].append(row["name"])
            continue

        def _overlay(field: str, bh_val):
            """Set row[field] to BH's value, recording any disagreement."""
            if bh_val is None or bh_val == "":
                return
            # Preserve as string for consistency with CSV storage
            new_val = str(bh_val) if not isinstance(bh_val, str) else bh_val
            existing = (row.get(field) or "").strip()
            if existing and existing != new_val:
                stats["conflicts_resolved"].append({
                    "name": row["name"],
                    "field": field,
                    "csv": existing,
                    "bh": new_val,
                })
            row[field] = new_val

        _overlay("year_of_birth",     bio.get("year_of_birth"))
        _overlay("color",             expand_bh_color(bio.get("color")))
        _overlay("height_hands",      bio.get("height_hands"))
        _overlay("entered_stud_year", bio.get("entered_stud_year"))
        # Carry BH's canonical URL for citation on the stallion card
        row["bloodhorse_url"] = bio.get("bh_url")
        stats["merged"] += 1
    return stats


def main():
    all_rows = (
        load_spendthrift() + load_winstar() + load_lanesend()
        + load_ashford() + load_threechimneys() + load_gainesway()
        + load_hillndale() + load_claiborne() + load_darley()
        + load_taylormade() + load_airdrie() + load_calumet()
    )
    all_rows.sort(key=lambda r: r["name"])

    # Overlay BH Stallion Register facts (year_of_birth, color, height,
    # entered_stud_year). Authoritative source; conflicts logged for audit.
    bh_bios = load_bh_bios()
    if bh_bios:
        bh_stats = merge_bh_bios(all_rows, bh_bios)
        print(f"BH overlay: {bh_stats['merged']} merged, "
              f"{len(bh_stats['missing'])} missing, "
              f"{len(bh_stats['conflicts_resolved'])} conflict(s) (BH won)")
        for conflict in bh_stats["conflicts_resolved"]:
            print(f"  CONFLICT: {conflict['name']}.{conflict['field']}: "
                  f"CSV={conflict['csv']!r} -> BH={conflict['bh']!r}")
        if bh_stats["missing"]:
            preview = ", ".join(bh_stats["missing"][:5])
            suffix = "" if len(bh_stats["missing"]) <= 5 else f" (+{len(bh_stats['missing'])-5} more)"
            print(f"  No BH record for: {preview}{suffix}")
    else:
        print(f"BH overlay skipped: {BH_BIOS_JSON.name} not found")

    # Write unified CSV (reflects BH overlay)
    with COMBINED_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COMMON_COLS)
        w.writeheader()
        w.writerows(all_rows)
    print(f"Wrote {COMBINED_CSV.name}: {len(all_rows)} rows")

    # Score every stallion against the combined roster
    snapshots = [to_snapshot(r) for r in all_rows]
    by_name = {s.name: (s, r) for s, r in zip(snapshots, all_rows)}

    results = []
    for snap, row in zip(snapshots, all_rows):
        result = score_commercial_appeal(snap, snapshots)
        results.append({
            "name": snap.name,
            "farm": row["farm"],
            "pedigree": (
                f"{row['sire']} \u2013 {row['dam']}, by {row['damsire']}"
                if row["sire"] and row["dam"] and row["damsire"]
                else None
            ),
            "sire": row["sire"] or None,
            "dam": row["dam"] or None,
            "damsire": row["damsire"] or None,
            "fee_usd": snap.stud_fee_usd,
            "fee_terms": row["fee_terms"] or None,
            "fee_qualifier": row["fee_qualifier"] or None,
            "year_of_birth": snap.entered_stud_year and (
                int(row["year_of_birth"]) if row["year_of_birth"] else None
            ),
            "color": row["color"] or None,
            "height_hands": row["height_hands"] or None,
            "entered_stud_year": snap.entered_stud_year,
            "source_url": row["source_url"],
            "bloodhorse_url": row.get("bloodhorse_url"),
            "score": {
                "value": result.score,
                "grade": result.grade,
                "tier": result.tier,
                "confidence": result.confidence,
                "model_version": result.model_version,
                "peer_group": result.peer_group,
                "components": result.components,
                "notes": result.notes,
            },
        })

    # Sort by score descending for convenient default ranking
    results.sort(key=lambda x: x["score"]["value"], reverse=True)

    with SCORES_JSON.open("w", encoding="utf-8") as f:
        json.dump({
            "model_version": results[0]["score"]["model_version"],
            "stallion_count": len(results),
            "farms": sorted({r["farm"] for r in results}),
            "stallions": results,
        }, f, indent=2)
    print(f"Wrote {SCORES_JSON.name}: {len(results)} scored stallions")

    # Summary
    print()
    print(f"Combined roster: {len(results)} stallions from {len(set(r['farm'] for r in results))} farms")
    print(f"Top 5:")
    for r in results[:5]:
        print(f"  {r['score']['grade']:3}  {r['score']['value']:5.1f}  "
              f"{r['name']:<25}  ({r['farm']}, ${r['fee_usd'] or '?':,})")


if __name__ == "__main__":
    main()
