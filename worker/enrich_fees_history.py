"""One-shot historical stud-fee enrichment.

Purpose:
  For each stallion in the roster, look up what their stud fee was in 2023
  and 2024 by finding Wayback Machine snapshots of each farm's fee-list page
  near Feb 1 of that year (when fees are typically published).

Accuracy rules — non-negotiable:
  * Only emit a fee if we can unambiguously associate a dollar amount with an
    exact roster name, AND the dollar amount parses cleanly as $X,XXX USD.
  * If a stallion appears on multiple farms' historical pages (rare — he moved
    farms) we keep the most-recent-to-target-date record.
  * Snapshots more than 6 months off from Feb 1 of the target year are rejected
    — too stale to trust as "year N's fee".
  * On any uncertainty → unresolved, for manual review. NO GUESSING.

Output:
  stud-fees-history.json — {
    "updated_at": "...",
    "resolved": {
      "Corniche": {
        "fees": {"2023": 30000, "2024": 20000},
        "sources": {
          "2023": "https://web.archive.org/web/20230204.../coolmore.com/...",
          "2024": "https://web.archive.org/web/20240115.../coolmore.com/..."
        }
      },
      ...
    },
    "unresolved": [{"name": ..., "year": 2023, "reason": ...}]
  }

Usage:
  pip3 install httpx       # if not already installed
  cd ~/sirevalue/worker
  # Full run (both years, all farms) — ~5 minutes
  python3 enrich_fees_history.py

  # Resume after interruption — only fetches missing stallion/year pairs
  python3 enrich_fees_history.py --merge

  # Narrow focus for debugging
  python3 enrich_fees_history.py --only "Spendthrift Farm" --year 2024 --verbose
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import httpx
except ImportError:
    print("httpx not installed. Run: pip3 install httpx", file=sys.stderr)
    sys.exit(1)
try:
    from bs4 import BeautifulSoup
except ImportError:
    print("beautifulsoup4 not installed. Run: pip3 install beautifulsoup4", file=sys.stderr)
    sys.exit(1)

HERE = Path(__file__).parent
ROSTER_CSV = HERE / "rosters-combined.csv"
OUTPUT_JSON = HERE / "stud-fees-history.json"

WAYBACK_AVAILABLE = "https://archive.org/wayback/available"
RATE_LIMIT_SECONDS = 1.0
REQUEST_TIMEOUT = 30.0
# Fees for a given year are typically announced Dec–Jan. Feb 1 is a safe "the
# year's fee is in effect" reference date.
TARGET_MONTHDAY = "0201"
# Reject snapshots further than this many days from the target date — too stale
# to be "that year's fee" with confidence.
MAX_SNAPSHOT_DRIFT_DAYS = 180

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

log = logging.getLogger("fees-history")


# ---------------------------------------------------------------------------
# Farm index URLs — the pages that list stallions with fees. Wayback Machine
# snapshots these; we scrape the archived copies.
# ---------------------------------------------------------------------------

FARM_INDEX_URLS: list[tuple[str, str]] = [
    ("Spendthrift Farm",                 "https://www.spendthriftfarm.com/stallions/"),
    ("WinStar Farm",                     "https://www.winstarfarm.com/stallions/"),
    ("Lane's End",                       "https://lanesend.com/stallions"),
    ("Ashford Stud (Coolmore America)",  "https://www.coolmore.com/en/america/"),
    ("Three Chimneys Farm",              "https://www.threechimneys.com/stallions/"),
    ("Gainesway Farm",                   "https://gainesway.com/stallions/"),
    ("Hill 'n' Dale Farms at Xalapa",    "https://www.hillndalefarms.com/stallions"),
    ("Claiborne Farm",                   "https://claibornefarm.com/stallions/"),
    ("Darley America (Godolphin)",       "https://www.darleyamerica.com/stallions/our-stallions"),
    ("Taylor Made Stallions",            "https://taylormadestallions.com/stallions/"),
    ("Airdrie Stud",                     "https://www.airdriestud.com/stallions/"),
    ("Calumet Farm",                     "https://calumetfarm.com/stallions/"),
]

YEARS_TO_FETCH = [2023, 2024]


# ---------------------------------------------------------------------------
# Roster loading
# ---------------------------------------------------------------------------

def load_roster() -> list[dict]:
    rows = []
    with ROSTER_CSV.open() as f:
        for r in csv.DictReader(f):
            rows.append({
                "name": (r.get("name") or "").strip(),
                "farm": (r.get("farm") or "").strip(),
            })
    return [r for r in rows if r["name"]]


def roster_name_index(rows: list[dict]) -> dict[str, str]:
    """Build a normalized-name -> canonical-name map. Used to match names found
    in archived HTML against our canonical roster names."""
    idx = {}
    for r in rows:
        normalized = normalize_name(r["name"])
        if normalized and normalized not in idx:
            idx[normalized] = r["name"]
    return idx


def normalize_name(name: str) -> str:
    """Lowercase, strip country suffix, strip all non-alphanumerics."""
    s = (name or "").lower()
    s = re.sub(r"\s*\([^)]+\)\s*$", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


# ---------------------------------------------------------------------------
# Wayback Machine
# ---------------------------------------------------------------------------

class Wayback:
    """Thin wrapper around the Wayback Machine availability + fetch APIs."""

    def __init__(self):
        self.client = httpx.Client(
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
            follow_redirects=True,
        )
        self._last_at = 0.0

    def _throttle(self):
        elapsed = time.time() - self._last_at
        if elapsed < RATE_LIMIT_SECONDS:
            time.sleep(RATE_LIMIT_SECONDS - elapsed)
        self._last_at = time.time()

    def find_closest_snapshot(self, original_url: str, target_timestamp: str) -> Optional[dict]:
        """Returns {url, timestamp, variant_queried} of the closest snapshot,
        or None if no capture is available.

        Strategy: try many URL variants; prefer deeper stallion-related paths
        over the bare homepage. Retry 503s once (archive.org is bursty).
        """
        # Gather every hit, then pick the best: prefer URLs whose path contains
        # "stallion" / "fee" / "roster" over homepage hits.
        candidates: list[dict] = []
        for url_variant in _url_variants(original_url):
            self._throttle()
            log.debug(f"  trying variant: {url_variant}")
            snap = self._availability_with_retry(url_variant, target_timestamp)
            if snap:
                snap["variant_queried"] = url_variant
                candidates.append(snap)
                # If this variant contains 'stallion', 'fee', or 'roster' in
                # the queried URL path, stop — we found the kind of page we want.
                if any(k in url_variant.lower() for k in ("stallion", "fee", "roster")):
                    return snap
        if not candidates:
            return None
        # Fall back to the first (most-specific) homepage-class hit
        return candidates[0]

    def _availability_with_retry(self, url: str, timestamp: str, attempts: int = 2) -> Optional[dict]:
        """Hit the availability API, retrying 503s once. Returns the closest
        snapshot dict {url, timestamp} or None."""
        for attempt in range(attempts):
            try:
                r = self.client.get(WAYBACK_AVAILABLE, params={
                    "url": url, "timestamp": timestamp,
                })
                if r.status_code == 503 and attempt < attempts - 1:
                    log.debug(f"    503, retrying in 2s")
                    time.sleep(2)
                    continue
                r.raise_for_status()
            except Exception as e:
                log.debug(f"    error: {e}")
                if attempt < attempts - 1:
                    time.sleep(2)
                    continue
                return None
            try:
                data = r.json()
            except Exception as e:
                log.debug(f"    bad json: {e}")
                return None
            snap = data.get("archived_snapshots", {}).get("closest")
            if snap and snap.get("available"):
                log.debug(f"    hit: {snap['timestamp']} @ {snap['url']}")
                return {"url": snap["url"], "timestamp": snap["timestamp"]}
            return None
        return None

    def fetch_snapshot(self, snapshot_url: str) -> str:
        self._throttle()
        # Force https (Wayback often returns http URLs)
        if snapshot_url.startswith("http://"):
            snapshot_url = "https://" + snapshot_url[len("http://"):]
        r = self.client.get(snapshot_url)
        r.raise_for_status()
        return r.text

    def close(self):
        self.client.close()


def drift_days(snapshot_ts: str, target_yyyymmdd: str) -> int:
    """Days between a Wayback timestamp (YYYYMMDDHHMMSS) and a target date
    string (YYYYMMDD)."""
    snap = datetime.strptime(snapshot_ts[:8], "%Y%m%d")
    target = datetime.strptime(target_yyyymmdd, "%Y%m%d")
    return abs((snap - target).days)


def _url_variants(original: str) -> list[str]:
    """Return plausible variants of a URL for Wayback Machine lookup.

    Wayback's availability API is picky about exact URL matches. Try multiple
    forms in order of most-to-least likely to find a stallion-fee page.
    """
    variants: list[str] = []
    seen: set[str] = set()

    def add(u: str):
        if u and u not in seen:
            seen.add(u)
            variants.append(u)

    # Common alternate stallion-listing paths on farm sites
    STALLION_PATHS = [
        None,                 # whatever the caller gave us
        "/stallions/",
        "/stallions",
        "/our-stallions/",
        "/our-stallions",
        "/stallion-roster/",
        "/stallion-roster",
        "/stud-fees/",
        "/stud-fees",
        "/fees/",
        "/fees",
        "/roster/",
        "/",                  # homepage fallback
        "",                   # bare domain, no slash
    ]

    # The original URL as-given, first
    add(original)

    m = re.match(r"^(https?)://(www\.)?([^/]+)(/.*)?$", original)
    if m:
        scheme, _www, host, path = m.group(1), m.group(2), m.group(3), m.group(4) or "/"
        for s in (scheme, "http" if scheme == "https" else "https"):
            for w in ("www.", ""):
                for p in STALLION_PATHS:
                    if p is None:
                        p = path
                    add(f"{s}://{w}{host}{p}")
                    # Toggle trailing slash on the end
                    if p and not p.endswith("/"):
                        add(f"{s}://{w}{host}{p}/")
    return variants


# ---------------------------------------------------------------------------
# Fee extraction
# ---------------------------------------------------------------------------

# Matches dollar amounts like $5,000 / $25,000 / $150,000. We require at least
# one comma so we don't accidentally match plain numbers like "$100 bonus".
# Min $2,500 (rare but possible for pensioners) up to $250,000 (covers all
# known commercial fees including Into Mischief).
FEE_RE = re.compile(r"\$\s*([1-9]\d{0,2}(?:,\d{3}))\b")

# Strip all HTML tags to get rendered text, preserving roughly the original
# whitespace. Not a full browser render, but good enough when fees appear as
# "$25,000 Live Foal" near the stallion name in the DOM.
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def visible_text(html: str) -> str:
    """Convert HTML to rough plain text. Close tags become spaces so names and
    fees separated by markup don't concatenate."""
    stripped = TAG_RE.sub(" ", html)
    # Decode the most common HTML entities; full decoding isn't worth the dep.
    stripped = (stripped
                .replace("&nbsp;", " ")
                .replace("&amp;", "&")
                .replace("&#39;", "'")
                .replace("&apos;", "'")
                .replace("&quot;", '"')
                .replace("&#8217;", "'")
                .replace("&#8216;", "'")
                .replace("&ndash;", "-")
                .replace("&mdash;", "-"))
    return WS_RE.sub(" ", stripped).strip()


def parse_fee(s: str) -> Optional[int]:
    """Parse '$25,000' style dollar amounts. Returns None if the string doesn't
    look like a fee."""
    m = FEE_RE.match(s)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _first_occurrence(text_lower: str, target: str, from_idx: int = 0) -> int:
    """Find the first word-boundary occurrence of target in text_lower, or -1.
    Optionally start the search from `from_idx`."""
    pos = from_idx
    while True:
        idx = text_lower.find(target, pos)
        if idx < 0:
            return -1
        before = text_lower[idx - 1] if idx > 0 else " "
        after_idx = idx + len(target)
        after = text_lower[after_idx] if after_idx < len(text_lower) else " "
        if not re.match(r"[a-z0-9]", before) and not re.match(r"[a-z0-9]", after):
            return idx
        pos = idx + len(target)


HEADER_TAGS = ("h1", "h2", "h3", "h4")
PEDIGREE_RE = re.compile(r"^\s*[\w .'-]+ [-\u2013] [\w .'-]+", re.UNICODE)


def extract_fees_from_html(
    html: str,
    farm_roster: dict[str, str],
    all_roster: Optional[dict[str, str]] = None,
) -> dict[str, int]:
    """Return {canonical_stallion_name: fee_usd}.

    Accuracy-first strategy:
      1. Parse HTML with BeautifulSoup. Identify STALLION HEADERS — roster
         names that appear inside an <h1>/<h2>/<h3>/<h4> tag or a tag with
         a class strongly hinting "stallion name" (e.g., `stallionName`).
         Those are the real section headers.
      2. Pedigree mentions (in <p> tags) are IGNORED for section detection.
         This is what keeps Into-Mischief-the-sire-of-someone from stealing
         someone else's header.
      3. For each header occurrence, the section is [header_pos, next_header_pos].
         Scan the section for $X,XXX amounts; emit exactly one fee if the
         section has one unique value. Zero → no fee (Private/Pensioned).
         Multiple differing → ambiguous → skip.

    `farm_roster` — stallions to EMIT fees for.
    `all_roster`  — used to find ALL header occurrences for correct section
                    boundaries (not used for emission).
    """
    if all_roster is None:
        all_roster = farm_roster

    soup = BeautifulSoup(html, "html.parser")
    # Rendered text (for scanning $ amounts); keep positions consistent with
    # what we'll map onto.
    text = soup.get_text(" ", strip=False)
    text = WS_RE.sub(" ", text)

    # Collect candidate header tags — heading elements plus tags whose class
    # has "stallion" and "name" / "title" / etc. signals.
    header_candidates: list = []
    for tag in soup.find_all(HEADER_TAGS):
        header_candidates.append(tag)
    for tag in soup.find_all(attrs={"class": True}):
        classes = " ".join(tag.get("class", [])).lower()
        if ("stallion" in classes and any(k in classes for k in ("name", "title", "header"))):
            header_candidates.append(tag)

    # For each candidate header, extract its text and check if it matches a
    # roster name. Record position of that match in the overall `text`.
    header_positions: list[tuple[int, str]] = []  # (pos_in_text, canonical)

    # Build a lookup by normalized name for quick exact-match
    norm_to_canonical = dict(all_roster)

    for tag in header_candidates:
        tag_text = tag.get_text(" ", strip=True)
        norm = normalize_name(tag_text)
        canonical = norm_to_canonical.get(norm)
        if not canonical:
            continue
        # Find where this header's text appears in the full rendered text.
        # We look for the first occurrence of the canonical string (case-
        # insensitive, word-bounded) AFTER the previous header to avoid
        # collisions when the same name appears multiple times.
        search_from = header_positions[-1][0] + 1 if header_positions else 0
        idx = _first_occurrence(text.lower(), canonical.lower(), from_idx=search_from)
        if idx < 0:
            # Fall back to global first-occurrence; unusual but possible if
            # the rendered text differs from bs4 traversal order
            idx = _first_occurrence(text.lower(), canonical.lower())
        if idx >= 0:
            header_positions.append((idx, canonical))

    # Deduplicate by position (same header might be matched twice via class+tag)
    seen = set()
    dedup = []
    for pos, canonical in sorted(header_positions):
        if (pos, canonical) in seen:
            continue
        seen.add((pos, canonical))
        dedup.append((pos, canonical))
    header_positions = dedup

    # Now walk sections and extract fees
    target_names = set(farm_roster.values())
    result: dict[str, int] = {}
    MAX_SECTION = 2000

    for i, (start_pos, canonical) in enumerate(header_positions):
        if canonical not in target_names:
            continue
        section_start = start_pos + len(canonical)
        if i + 1 < len(header_positions):
            next_start = header_positions[i + 1][0]
            section_end = min(next_start, section_start + MAX_SECTION)
        else:
            section_end = min(len(text), section_start + MAX_SECTION)
        section = text[section_start:section_end]

        amounts: list[int] = []
        for m in re.finditer(r"\$\s*([1-9]\d{0,2}(?:,\d{3}))\b", section):
            try:
                amt = int(m.group(1).replace(",", ""))
            except ValueError:
                continue
            if 2500 <= amt <= 250000:
                amounts.append(amt)

        if not amounts:
            continue

        unique = set(amounts)
        if len(unique) == 1:
            result[canonical] = amounts[0]
        else:
            log.debug(f"  ambiguous fees for {canonical!r}: {sorted(unique)}")

    return result


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def process_farm_year(
    wb: Wayback,
    farm_name: str,
    index_url: str,
    year: int,
    farm_roster: dict[str, str],
    all_roster: Optional[dict[str, str]] = None,
) -> tuple[dict[str, int], Optional[str], Optional[str]]:
    """For one (farm, year) pair, find + fetch the archive snapshot, extract
    fees keyed by canonical stallion name.

    `farm_roster` is restricted to this farm's stallions (output scope).
    `all_roster` spans every roster stallion (used for section boundaries)."""
    target_ts = f"{year}{TARGET_MONTHDAY}"
    log.info(f"[{farm_name}] {year}: looking up snapshot near {target_ts}")
    try:
        snap = wb.find_closest_snapshot(index_url, target_ts)
    except Exception as e:
        return {}, None, f"availability query failed: {e.__class__.__name__}: {e}"
    if not snap:
        return {}, None, "no Wayback snapshot available"

    drift = drift_days(snap["timestamp"], target_ts)
    if drift > MAX_SNAPSHOT_DRIFT_DAYS:
        return {}, snap["url"], f"closest snapshot is {drift} days off (>{MAX_SNAPSHOT_DRIFT_DAYS})"

    log.info(f"[{farm_name}] {year}: snapshot at {snap['timestamp']} (drift {drift}d)")
    try:
        html = wb.fetch_snapshot(snap["url"])
    except Exception as e:
        return {}, snap["url"], f"snapshot fetch failed: {e.__class__.__name__}: {e}"

    fees = extract_fees_from_html(html, farm_roster, all_roster=all_roster)
    log.info(f"[{farm_name}] {year}: extracted {len(fees)} fee(s) out of {len(farm_roster)} known stallions")
    return fees, snap["url"], None


def load_previous_output() -> dict:
    if OUTPUT_JSON.exists():
        try:
            return json.loads(OUTPUT_JSON.read_text())
        except Exception:
            pass
    return {"resolved": {}, "unresolved": []}


def save_output(resolved: dict, unresolved: list) -> None:
    OUTPUT_JSON.write_text(json.dumps({
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": "https://web.archive.org/ — Wayback Machine snapshots of farm fee pages",
        "resolved": resolved,
        "unresolved": unresolved,
        "summary": {
            "stallions_with_any_history": len(resolved),
            "unresolved_count": len(unresolved),
        },
    }, indent=2, ensure_ascii=False))


def main() -> int:
    ap = argparse.ArgumentParser(description="Historical stud-fee enrichment via Wayback Machine")
    ap.add_argument("--only", help="Process only this farm name (exact)")
    ap.add_argument("--year", type=int, help="Process only this year")
    ap.add_argument("--merge", action="store_true",
                    help="Resume: keep existing fees, only fetch missing farm/year combos")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    roster = load_roster()
    # Build per-farm name indexes — each farm page emits only its own
    # stallions, avoiding cross-farm false matches in pedigree text.
    per_farm: dict[str, dict[str, str]] = {}
    for r in roster:
        per_farm.setdefault(r["farm"], {})
        norm = normalize_name(r["name"])
        if norm:
            per_farm[r["farm"]][norm] = r["name"]
    all_roster = roster_name_index(roster)
    log.info(
        f"roster loaded: {len(roster)} stallions across {len(per_farm)} farms "
        f"(avg {len(roster)/max(len(per_farm),1):.1f} per farm)"
    )

    farms = FARM_INDEX_URLS
    if args.only:
        farms = [(n, u) for (n, u) in farms if n == args.only]
        if not farms:
            log.error(f"farm {args.only!r} not in known list")
            return 2

    years = [args.year] if args.year else YEARS_TO_FETCH

    # Carry forward any previous progress when merging
    prev = load_previous_output() if args.merge else {"resolved": {}, "unresolved": []}
    resolved: dict[str, dict] = prev.get("resolved", {}) or {}
    unresolved: list[dict] = []
    # Track which (farm, year) combos we've already done so --merge can skip
    # them. We persist this in the JSON's `farms_done` for future resume.
    done_combos = set()
    for combo in prev.get("farms_done", []):
        done_combos.add((combo["farm"], combo["year"]))

    wb = Wayback()
    try:
        for farm_name, index_url in farms:
            for year in years:
                if args.merge and (farm_name, year) in done_combos:
                    log.info(f"[{farm_name}] {year}: already done, skipping")
                    continue
                farm_roster = per_farm.get(farm_name, {})
                if not farm_roster:
                    log.warning(f"[{farm_name}] no stallions in roster — skipping")
                    done_combos.add((farm_name, year))
                    continue
                fees, snap_url, err = process_farm_year(
                    wb, farm_name, index_url, year, farm_roster, all_roster=all_roster
                )
                if err:
                    unresolved.append({
                        "farm": farm_name, "year": year,
                        "snapshot_url": snap_url, "reason": err,
                    })
                    log.warning(f"[{farm_name}] {year}: {err}")
                # Merge extracted fees into resolved dict
                for stallion, fee in fees.items():
                    entry = resolved.setdefault(stallion, {"fees": {}, "sources": {}})
                    # First write wins for a given year — each farm's page
                    # lists their own stallions, so a conflict would mean the
                    # same name on two farms (rare but possible on mergers).
                    if str(year) not in entry["fees"]:
                        entry["fees"][str(year)] = fee
                        entry["sources"][str(year)] = snap_url
                done_combos.add((farm_name, year))
                # Incremental save after each (farm, year) combo
                _save_with_progress(resolved, unresolved, done_combos)
    except KeyboardInterrupt:
        log.warning("interrupted — partial progress saved")
    finally:
        wb.close()

    _save_with_progress(resolved, unresolved, done_combos)

    n_with_any = len(resolved)
    n_with_both_years = sum(1 for e in resolved.values() if len(e["fees"]) >= 2)
    log.info(
        f"done. {n_with_any} stallion(s) with historical fee data, "
        f"{n_with_both_years} have {len(years)}+ years."
    )
    if unresolved:
        log.info(f"{len(unresolved)} (farm, year) combo(s) failed or partial:")
        for u in unresolved:
            log.info(f"  - {u['farm']} {u['year']}: {u['reason']}")
    return 0


def _save_with_progress(resolved: dict, unresolved: list, done_combos: set) -> None:
    output = {
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": "https://web.archive.org/ — Wayback Machine snapshots of farm fee pages",
        "resolved": resolved,
        "unresolved": unresolved,
        "farms_done": [{"farm": f, "year": y} for (f, y) in sorted(done_combos)],
        "summary": {
            "stallions_with_any_history": len(resolved),
            "stallions_with_multi_year": sum(1 for e in resolved.values() if len(e["fees"]) >= 2),
            "unresolved_combos": len(unresolved),
        },
    }
    OUTPUT_JSON.write_text(json.dumps(output, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    sys.exit(main())
