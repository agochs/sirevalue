"""Build a chronological sales calendar: completed + upcoming.

We don't have actual sale dates from the BH scrape (only year + sale name),
so we infer month from name patterns. Annual sales recur on the same week,
so this is reliable enough for a planning view. The output marks each sale
with status (completed / upcoming / current) based on whether it appears
in our completed-sale results.

Output: sales-calendar.json
{
  generated_at,
  current_year, current_month,
  by_year: {
    "2026": [
      {
        sale_name, slug, kind ('yearling'|'twoyo'|'mixed'|'broodmare'|'hora'|'other'),
        month_num, month_label, status ('completed'|'upcoming'|'unknown'),
        hip_count, sold_count, gross_usd,
        signal_counts: {strong, positive, neutral, weak, none},
        approx_window: e.g. "Jun 9-11"  # optional
      }, ...
    ]
  }
}

Run AFTER score_catalog.py (needs catalog-scoring-index.json + per-sale files).
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).parent
INDEX_JSON       = HERE / "catalog-scoring-index.json"
PUBLIC_CATALOGS  = HERE.parent / "public" / "data" / "catalogs"
OUTPUT_JSON      = HERE / "sales-calendar.json"

# Month inference rules. Each rule has a regex matched against sale_name (case-
# insensitive). First match wins. The approx_window is what shows up in the UI
# alongside the sale name as a planning hint — these are typical date ranges
# that the major US auctions recur on, year over year.
MONTH_RULES = [
    # (pattern, month, month_label, kind, approx_window)
    (r"\bjanuary\b",                 1,  "January",   "horsesofallages",  "Early-mid Jan"),
    (r"\bfebruary\b",                2,  "February",  "mixed",            "Mid Feb"),
    (r"\bwinter mixed\b",            2,  "February",  "mixed",            "Late Jan-Feb"),
    (r"\bmarch\b.*\b2yos?\b",        3,  "March",     "twoyo",            "Mid March"),
    (r"\bmarch\b",                   3,  "March",     "mixed",            "March"),
    (r"\bspring\b.*\b2yos?\b",       4,  "April",     "twoyo",            "Mid-late April"),
    (r"\bapril\b",                   4,  "April",     "horsesofallages",  "Mid April"),
    (r"\bmay\b",                     5,  "May",       "twoyo",            "May"),
    (r"\bjune\b.*\b2yos?\b",         6,  "June",      "twoyo",            "Mid June"),
    (r"\bjune\b",                    6,  "June",      "mixed",            "June"),
    (r"\bjuly\b.*\bracing age\b",    7,  "July",      "hora",             "Mid July"),
    (r"\bjuly\b.*\byearling",        7,  "July",      "yearling",         "Mid July"),
    (r"\bjuly\b",                    7,  "July",      "mixed",            "July"),
    (r"\bsaratoga\b.*\bnew york\b.*\byearling", 8, "August", "yearling", "Early August"),
    (r"\bsaratoga\b.*\byearling",    8,  "August",    "yearling",         "Aug 11-12 (Selected)"),
    (r"\bsaratoga\b",                8,  "August",    "yearling",         "August"),
    (r"\baugust\b",                  8,  "August",    "yearling",         "August"),
    (r"\bseptember\b.*\byearling",   9,  "September", "yearling",         "Sep 8-19 (Keeneland)"),
    (r"\bseptember\b",               9,  "September", "yearling",         "September"),
    (r"\boctober\b.*\byearling",    10,  "October",   "yearling",         "Mid October"),
    (r"\boctober\b",                10,  "October",   "yearling",         "October"),
    (r"\bnovember\b.*\bbreeding stock\b", 11, "November", "broodmare",   "Nov 4-15 (Keeneland)"),
    (r"\bnovember\b",               11,  "November",  "mixed",            "Early-mid November"),
    (r"\bdecember\b",               12,  "December",  "mixed",            "December"),
    # Region-specific fall yearling sales (no month in name, but they all run Oct-Nov)
    (r"\bcalifornia\b.*\bfall\b.*\byearling", 10, "October", "yearling",  "October (Fasig California)"),
    (r"\bmidlantic\b.*\bfall\b.*\byearling",  10, "October", "yearling",  "October (Fasig Midlantic)"),
    (r"\bnew york\b.*\bfall\b.*\bmixed",      11, "November","mixed",     "November (Fasig NY Fall)"),
    (r"\bmidlantic\b.*\b2yos?\b",              5, "May",     "twoyo",     "May (Fasig Midlantic 2YO)"),
    (r"\bfall\b.*\byearling",                 10, "October", "yearling",  "October-November"),
    (r"\bchampionship\b",                     11, "November","mixed",     "November"),
    # Fallback for digital / online sales that recur monthly
    (r"\bdigital\b.*\bjanuary\b",    1,  "January",   "online",           "January (online)"),
    (r"\bdigital\b.*\bfebruary\b",   2,  "February",  "online",           "February (online)"),
    (r"\bdigital\b.*\bmarch\b",      3,  "March",     "online",           "March (online)"),
    (r"\bdigital\b.*\bapril\b",      4,  "April",     "online",           "April (online)"),
]


def infer_month(sale_name: str) -> tuple[int, str, str, str] | None:
    """Returns (month, month_label, kind_hint, approx_window) or None."""
    s = (sale_name or "").lower()
    for pat, month, label, kind, window in MONTH_RULES:
        if re.search(pat, s):
            return month, label, kind, window
    return None


def sale_year(sale_name: str) -> int | None:
    m = re.search(r"\b(20\d{2})\b\s*$", (sale_name or "").strip())
    return int(m.group(1)) if m else None


def kind_from_name(sale_name: str) -> str:
    s = (sale_name or "").lower()
    if "2yo" in s or "two-year-old" in s or "in training" in s: return "twoyo"
    if "yearling" in s: return "yearling"
    if "breeding stock" in s: return "broodmare"
    if "racing age" in s or "horses of racing age" in s: return "hora"
    if "mixed" in s: return "mixed"
    return "other"


def load_per_sale(slug: str) -> dict | None:
    for d in (HERE, PUBLIC_CATALOGS):
        p = d / f"catalog-scoring-sale-{slug}.json"
        if p.exists():
            try:
                return json.loads(p.read_text())
            except Exception:
                pass
    return None


def main():
    if not INDEX_JSON.exists():
        print(f"Missing {INDEX_JSON.name} — run score_catalog.py first.")
        return
    index = json.loads(INDEX_JSON.read_text())
    sales = index.get("sales", [])

    today = datetime.utcnow()
    current_year = today.year

    # We'll group by year, only keeping years current_year - 1 onward to keep
    # the calendar focused on actionable timeframes.
    keep_years = {current_year - 1, current_year, current_year + 1}

    by_year: dict[int, list[dict]] = {}
    for s in sales:
        sale_name = s.get("sale_name") or ""
        year = sale_year(sale_name)
        if year is None or year not in keep_years:
            continue
        info = infer_month(sale_name)
        if info is None:
            month, month_label, _kind_hint, window = 0, "Unknown", None, None
        else:
            month, month_label, _kind_hint, window = info
        kind = kind_from_name(sale_name)

        # Compute outcome fields from per-sale file if it's a results sale
        sold_count = 0
        gross = 0
        per = load_per_sale(s.get("slug") or "")
        if per:
            for h in per.get("hips", []):
                if h.get("sold_price_usd"):
                    sold_count += 1
                    gross += h["sold_price_usd"]

        # Status: results-kind = completed; upcoming-kind = upcoming.
        # If month already passed in current year, also infer 'completed' for
        # safety even when worker hasn't backfilled results.
        if s.get("kind") == "results":
            status = "completed"
        elif s.get("kind") == "upcoming":
            status = "upcoming"
        else:
            status = "unknown"

        by_year.setdefault(year, []).append({
            "sale_name":     sale_name,
            "slug":          s.get("slug"),
            "kind":          kind,
            "status":        status,
            "month":         month,
            "month_label":   month_label,
            "approx_window": window,
            "hip_count":     s.get("hip_count") or 0,
            "sold_count":    sold_count,
            "gross_usd":     gross,
            "signal_counts": s.get("signal_counts") or {},
        })

    # Within each year, sort by month, then sale name
    for y in by_year:
        by_year[y].sort(key=lambda r: (r["month"] or 99, r["sale_name"]))

    # Build a list of "anticipated" upcoming sales: any annual sale that ran
    # in (current_year - 1) but not in current_year yet. We project a forecast
    # entry for the current year if no actual entry exists. This fills the
    # forward-looking gap before BloodHorse posts the next catalog.
    if current_year in by_year and (current_year - 1) in by_year:
        # Normalize the sale name into a stable cross-year key so abbrevs don't
        # create false negatives. "OBS Spring Sale of 2YOs in Training 2026"
        # and "Ocala Breeders Sales Co Spring Sale Of 2yos In Training 2025"
        # should map to the same key.
        def name_key(name: str) -> str:
            n = re.sub(r"\s*\b20\d{2}\b\s*$", "", name).strip().lower()
            n = re.sub(r"\bocala breeders sales co\b", "obs", n)
            n = re.sub(r"\bocala breeders sale co\b",  "obs", n)
            n = re.sub(r"\bfasig tipton\b",            "ft",  n)
            n = re.sub(r"\b(2yos?|two-year-olds?|two year olds?)\b", "2yo", n)
            n = re.sub(r"\s+", " ", n)
            n = re.sub(r"[^a-z0-9 ]", "", n)
            return n.strip()
        seen_in_current = {name_key(r["sale_name"]) for r in by_year[current_year]}
        anticipated = []
        for r in by_year[current_year - 1]:
            key = name_key(r["sale_name"])
            base = re.sub(r"\s*\b20\d{2}\b\s*$", "", r["sale_name"]).strip()
            if key in seen_in_current:
                continue
            # Only forecast major recurring sales — skip the noisy weekly online
            # auctions which add too many low-value rows.
            if "digital" in base.lower() or "online" in base.lower():
                continue
            # Skip if month already passed — it would have already happened
            if r["month"] and r["month"] < today.month - 1:
                continue
            anticipated.append({
                "sale_name":     base + " " + str(current_year),
                "slug":          None,
                "kind":          r["kind"],
                "status":        "anticipated",
                "month":         r["month"],
                "month_label":   r["month_label"],
                "approx_window": r["approx_window"],
                "hip_count":     0,
                "sold_count":    0,
                "gross_usd":     0,
                "signal_counts": {},
            })
        # Merge anticipated into current year
        by_year[current_year] = sorted(
            by_year[current_year] + anticipated,
            key=lambda r: (r["month"] or 99, r["sale_name"]),
        )

    # Stats
    total_completed   = sum(1 for y in by_year.values() for r in y if r["status"] == "completed")
    total_upcoming    = sum(1 for y in by_year.values() for r in y if r["status"] in ("upcoming", "anticipated"))
    total_anticipated = sum(1 for y in by_year.values() for r in y if r["status"] == "anticipated")

    out = {
        "generated_at":  today.isoformat(timespec="seconds") + "Z",
        "current_year":  current_year,
        "current_month": today.month,
        "summary": {
            "completed":   total_completed,
            "upcoming":    total_upcoming,
            "anticipated": total_anticipated,
        },
        # by_year keys must be strings for JSON
        "by_year": {str(y): r for y, r in by_year.items()},
    }
    OUTPUT_JSON.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print(f"Wrote {OUTPUT_JSON.name}: "
          f"{total_completed} completed, "
          f"{total_upcoming} upcoming/anticipated "
          f"({total_anticipated} forecast from prior-year recurrence)")
    print()
    # Quick spot-check
    if str(current_year) in out["by_year"]:
        print(f"{current_year} sales:")
        for r in out["by_year"][str(current_year)]:
            tag = {"completed": "✓", "upcoming": "→", "anticipated": "?", "unknown": "·"}[r["status"]]
            print(f"  {tag}  {r['month_label'][:10]:10s}  {r['sale_name'][:60]:60s}  "
                  f"hips={r['hip_count']:>4d}  sold={r['sold_count']:>4d}  "
                  f"gross=${r['gross_usd']:>11,}")


if __name__ == "__main__":
    main()
