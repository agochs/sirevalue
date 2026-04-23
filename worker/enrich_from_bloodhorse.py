"""One-shot BloodHorse Stallion Register enrichment (Playwright version).

Purpose:
  Populate year_of_birth, color, height_hands, and entered_stud_year for every
  stallion in rosters-combined.csv by looking them up on BloodHorse's
  Stallion Register (https://www.bloodhorse.com/stallion-register/).

Accuracy rules — non-negotiable:
  * Only emit a record if navigation lands on the EXACT stallion (slug-match)
    AND the bio page parses cleanly into all four fields.
  * If either check fails, the stallion goes into `unresolved` with the reason.
    NO GUESSING, NO PARTIAL RECORDS, NO HEURISTICS.
  * The user reviews `unresolved` manually and fills those in by hand.

Why Playwright instead of httpx:
  BloodHorse sits behind Imperva, which serves a JS challenge to anything that
  looks like a bot on sustained requests. Playwright drives real Chrome, which
  executes the challenge JS automatically and keeps the session warm.

Output:
  bloodhorse-register-bios.json — {"updated_at", "resolved": {name: {...}},
  "unresolved": [{name, reason}], "summary": {...}}.
  Written after EVERY stallion — partial progress survives crashes, and --merge
  lets you resume where you left off.

Usage:
  # Install Playwright + chromium binary once:
  pip3 install playwright
  python3 -m playwright install chromium

  # One stallion, verbose — use this to sanity-check
  python3 enrich_from_bloodhorse.py --only "Newgate" --verbose

  # Full run — ~8-12 minutes
  python3 enrich_from_bloodhorse.py

  # Resume after a crash or partial run
  python3 enrich_from_bloodhorse.py --merge

  # Troubleshooting: watch the browser drive itself
  python3 enrich_from_bloodhorse.py --only "Newgate" --headful
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

# Playwright is only needed for the actual scrape — import lazily so the pure
# parser can be imported / unit-tested without it.
sync_playwright = None  # type: ignore
def _require_playwright():
    global sync_playwright
    if sync_playwright is None:
        try:
            from playwright.sync_api import sync_playwright as _sp
            sync_playwright = _sp
        except ImportError:
            print(
                "Playwright not installed. Run:\n"
                "  pip3 install playwright\n"
                "  python3 -m playwright install chromium",
                file=sys.stderr,
            )
            sys.exit(1)

HERE = Path(__file__).parent
ROSTER_CSV = HERE / "rosters-combined.csv"
OUTPUT_JSON = HERE / "bloodhorse-register-bios.json"

BH_BASE = "https://www.bloodhorse.com"
SEARCH_URL = f"{BH_BASE}/stallion-register/SearchResults"
HOME_URL = f"{BH_BASE}/stallion-register/"
# Browser-like UA — BH's CDN (Imperva) returns a challenge page when it sees
# a bot UA, which truncates responses to ~1KB. Mimicking Chrome's headers
# gets us past that. We're still polite about request rate.
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
ACCEPT = (
    "text/html,application/xhtml+xml,application/xml;q=0.9,"
    "image/avif,image/webp,*/*;q=0.8"
)
RATE_LIMIT_SECONDS = 1.5   # be polite
TIMEOUT = 20.0

log = logging.getLogger("bh-enrich")


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

# Matches the second <p> inside <div class="stallionInfo">, which has form:
#   "2020, b, 16.3 hands, entered stud 2025"
STALLION_INFO_RE = re.compile(
    r'<div[^>]*class="stallionInfo"[^>]*>'
    r'.*?<p[^>]*>\s*([^<]+?)\s*</p>'      # 1. pedigree line
    r'.*?<p[^>]*>\s*([^<]+?)\s*</p>',     # 2. data line (the gold)
    flags=re.DOTALL,
)

# Parses the data line. Color is 1-6 alpha chars (b, ch, gr, ro, blk, br, dkb,
# etc.) optionally with a slash (dkb/br). Height is like "16.3", "15.2",
# "16", or "16.1 1/2" (horse-world fractional notation — 16 hands 1.5 inches).
# Height is OPTIONAL — BH omits it entirely when unavailable.
#
# Examples this matches:
#   "2020, b, 16.3 hands, entered stud 2025"         (Newgate — full)
#   "2020, b, entered stud 2025"                     (Angel of Empire — no height)
#   "2018, b, 16.1 1/2 hands, entered stud 2022"     (Beau Liam — fractional)
#   "2011, gr/ro, 16.1 1/2 hands, entered stud 2015" (Cairo Prince)
DATA_LINE_RE = re.compile(
    r'(\d{4}),\s*'                                        # year_of_birth
    r'([a-z]{1,6}(?:/[a-z]{1,6})?),\s*'                  # color
    r'(?:(\d{1,2}(?:\.\d+)?(?:\s+\d+/\d+)?)\s+hands,\s*)?'  # height — OPTIONAL, allows fractional
    r'entered stud\s+(\d{4})',                            # entered_stud_year
    flags=re.IGNORECASE,
)

# Matches /stallion-register/stallions/{id}/{slug} in search results HTML
STALLION_URL_RE = re.compile(
    r'/stallion-register/stallions/(\d+)/([a-z0-9-]+)',
    flags=re.IGNORECASE,
)


def parse_bio_page(html: str) -> tuple[Optional[dict], Optional[str]]:
    """Extract {year_of_birth, color, height_hands, entered_stud_year} from a
    BH bio page HTML. Returns (record, raw_line) — record is None on parse
    failure, raw_line is the unparsed data-line text when we can see it (so
    the caller can log it for diagnosis)."""
    m = STALLION_INFO_RE.search(html)
    if not m:
        return None, None
    data_line = m.group(2).strip()
    dm = DATA_LINE_RE.match(data_line)
    if not dm:
        return None, data_line   # give the caller the raw line
    yob, color, height, esy = dm.groups()
    # Store height_hands as the verbatim string — preserves BH's fractional
    # notation (e.g., "16.1 1/2") without the risk of a wrong float conversion
    # from horse-world non-decimal semantics.
    return {
        "year_of_birth": int(yob),
        "color": color.lower(),
        "height_hands": height.strip() if height else None,
        "entered_stud_year": int(esy),
        "_raw_data_line": data_line,   # keep for audit
    }, data_line


# ---------------------------------------------------------------------------
# Name matching
# ---------------------------------------------------------------------------

# BH's set of country codes that appear as parenthetical suffixes on foreign-bred
# sires. When BH renders these into URL slugs, the parens become hyphens.
COUNTRY_CODES = {
    "arg", "aus", "brz", "chi", "fr", "gb", "ger", "ire", "ity", "jpn",
    "nz", "per", "pol", "saf", "swi", "tur", "uru", "ven",
}

def normalize_name(name: str) -> str:
    """Lowercase, strip country-code suffix, collapse whitespace and punctuation."""
    s = name.lower()
    s = re.sub(r"\s*\([^)]+\)\s*$", "", s)   # strip " (IRE)", " (JPN)" etc.
    s = re.sub(r"[^a-z0-9]+", "", s)         # strip all punctuation / spaces
    return s


def _normalize_slug(slug: str) -> str:
    """Convert a BH URL slug ('candy-ride-arg', 'newgate') to the same canonical
    form as normalize_name. Handles the trailing country-code suffix that BH
    encodes as a final '-xxx' segment for foreign-bred sires.
    """
    parts = slug.lower().split("-")
    if parts and parts[-1] in COUNTRY_CODES:
        parts = parts[:-1]
    return "".join(parts)


def find_matching_bio_url(html: str, target_name: str) -> Optional[tuple[int, str]]:
    """Scan search-result HTML for a link whose displayed name matches the
    target. Returns (url_id, slug) or None.

    Strategy: find all /stallions/ID/SLUG links, slug-compare to the target.
    Slug is lowercase with hyphens, so "Newgate" → "newgate",
    "Candy Ride (ARG)" → "candy-ride-arg" (BH convention).
    """
    target_normalized = normalize_name(target_name)
    seen = set()
    for m in STALLION_URL_RE.finditer(html):
        url_id = int(m.group(1))
        slug = m.group(2).lower()
        if (url_id, slug) in seen:
            continue
        seen.add((url_id, slug))
        if _normalize_slug(slug) == target_normalized:
            return (url_id, slug)
    return None


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

class SearchResult:
    """Browser-independent search result container."""
    def __init__(self, final_url: str, html: str):
        self.url = final_url
        self.text = html


class BHBrowser:
    """Playwright-driven wrapper. Presents the same `search(name) -> result`
    interface the resolver needs, but under the hood it's a real Chrome doing
    page.fill + form submit + navigation wait."""

    def __init__(self, headful: bool = False, debug_dump_dir: Optional[Path] = None):
        _require_playwright()
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=not headful,
            args=["--disable-blink-features=AutomationControlled"],
        )
        self._context = self._browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        self._page = self._context.new_page()
        self._last_request_at = 0.0
        self._warmed_up = False
        self._debug_dump_dir = debug_dump_dir

    def _throttle(self):
        elapsed = time.time() - self._last_request_at
        if elapsed < RATE_LIMIT_SECONDS:
            time.sleep(RATE_LIMIT_SECONDS - elapsed)
        self._last_request_at = time.time()

    def warm_up(self):
        """Navigate to the stallion-register home once so Imperva sees a normal
        browser session establishing itself — JS runs, cookies get set, future
        navigations are treated as same-session."""
        if self._warmed_up:
            return
        log.debug(f"warm-up: goto {HOME_URL}")
        self._page.goto(HOME_URL, wait_until="domcontentloaded", timeout=30_000)
        # Give any challenge JS a moment to resolve
        try:
            self._page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass
        log.debug(f"  warm-up final url: {self._page.url}")
        self._warmed_up = True

    def search(self, name: str) -> SearchResult:
        """Submit the name through the real search form. BH's server returns
        a 302 to the bio page on exact match, and Playwright follows it
        naturally. Returns the final URL and HTML."""
        self.warm_up()
        self._throttle()
        log.debug(f"search: {name!r}")
        # Make sure we're on a page that has the search form. If we just came
        # from a bio page, the search form is still present in the masthead.
        try:
            self._page.fill("input.searchQuery", name, timeout=10_000)
        except Exception:
            # Fallback: navigate home first
            log.debug("search form not found — re-navigating to home")
            self._page.goto(HOME_URL, wait_until="domcontentloaded", timeout=30_000)
            self._page.fill("input.searchQuery", name, timeout=10_000)

        # Submit by pressing Enter (equivalent to clicking the Search button)
        with self._page.expect_navigation(wait_until="domcontentloaded", timeout=30_000):
            self._page.keyboard.press("Enter")
        try:
            self._page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass

        final_url = self._page.url
        html = self._page.content()
        log.debug(f"  -> final url={final_url}, {len(html)} bytes")
        if self._debug_dump_dir:
            self._debug_dump_dir.mkdir(parents=True, exist_ok=True)
            (self._debug_dump_dir / f"search-{_safe_slug(name)}.html").write_text(html)
        return SearchResult(final_url=final_url, html=html)

    def fetch_bio_by_id(self, url_id: int, slug: str) -> SearchResult:
        """Direct navigation to /stallions/{id}/{slug}. Used when search lands
        on an ambiguous results page rather than redirecting to a single bio."""
        self.warm_up()
        self._throttle()
        url = f"{BH_BASE}/stallion-register/stallions/{url_id}/{slug}"
        log.debug(f"fetch_bio: {url}")
        self._page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        try:
            self._page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass
        final_url = self._page.url
        html = self._page.content()
        log.debug(f"  -> final url={final_url}, {len(html)} bytes")
        if self._debug_dump_dir:
            self._debug_dump_dir.mkdir(parents=True, exist_ok=True)
            (self._debug_dump_dir / f"bio-{slug}.html").write_text(html)
        return SearchResult(final_url=final_url, html=html)

    def close(self):
        try:
            self._context.close()
            self._browser.close()
        finally:
            self._pw.stop()


def _safe_slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def resolve_one(bh, name: str) -> tuple[Optional[dict], Optional[str]]:
    """Returns (record, error). Exactly one is non-None.

    Works with either BHBrowser (Playwright) or any client that exposes
    search(name) -> object with .url and .text attributes.
    """
    try:
        r = bh.search(name)
    except Exception as e:
        return None, f"search failed: {e.__class__.__name__}: {e}"

    # Primary path: form submission navigated us directly to /stallions/ID/slug
    final_url = str(r.url)
    m = re.search(r"/stallion-register/stallions/(\d+)/([a-z0-9-]+)", final_url, flags=re.I)
    if m:
        url_id, slug = int(m.group(1)), m.group(2).lower()
        # Accuracy check: reject if BH redirected us to a near-miss.
        if _normalize_slug(slug) != normalize_name(name):
            return None, f"navigated to different stallion: slug={slug!r}"
        bio_html = r.text
    else:
        # Fallback: search landed on a multi-candidate results page. Find the
        # exact-name match by slug and do a second-hop navigation to that bio.
        match = find_matching_bio_url(r.text, name)
        if not match:
            return None, "no exact-name match (ambiguous or no results)"
        url_id, slug = match
        try:
            r2 = bh.fetch_bio_by_id(url_id, slug)
        except Exception as e:
            return None, f"bio fetch failed: {e.__class__.__name__}: {e}"
        # Confirm the second-hop navigation landed where we asked (guard against
        # any redirect surprises)
        m2 = re.search(r"/stallion-register/stallions/(\d+)/([a-z0-9-]+)", str(r2.url), flags=re.I)
        if not m2 or int(m2.group(1)) != url_id:
            return None, f"second-hop navigated elsewhere: url={r2.url!r}"
        bio_html = r2.text

    # Sanity check on body — a valid bio page is ~50-100KB
    if len(bio_html) < 5000:
        return None, (
            f"bio body too small ({len(bio_html)} bytes) — likely CDN challenge"
        )

    parsed, raw_line = parse_bio_page(bio_html)
    if not parsed:
        if raw_line is not None:
            return None, f"bio parse failed — raw line: {raw_line!r}"
        return None, "bio parse failed (stallionInfo div not found)"

    parsed["bh_url_id"] = url_id
    parsed["bh_slug"] = slug
    parsed["bh_url"] = f"{BH_BASE}/stallion-register/stallions/{url_id}/{slug}"
    return parsed, None


def save_output(resolved: dict, unresolved: list, total: int) -> None:
    """Write bloodhorse-register-bios.json with the current resolved/unresolved
    sets. Called after every stallion so partial progress survives crashes."""
    output = {
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": "https://www.bloodhorse.com/stallion-register/",
        "resolved": resolved,
        "unresolved": unresolved,
        "summary": {
            "total": total,
            "resolved": len(resolved),
            "unresolved": len(unresolved),
        },
    }
    OUTPUT_JSON.write_text(json.dumps(output, indent=2, ensure_ascii=False))


def load_roster_names() -> list[str]:
    names = []
    with ROSTER_CSV.open() as f:
        for row in csv.DictReader(f):
            n = (row.get("name") or "").strip()
            if n:
                names.append(n)
    # Unique-preserving-order (duplicates would waste fetches)
    seen = set()
    out = []
    for n in names:
        if n.lower() in seen:
            continue
        seen.add(n.lower())
        out.append(n)
    return out


def main():
    ap = argparse.ArgumentParser(description="Enrich roster with BloodHorse bios (Playwright)")
    ap.add_argument("--only", help="Process only this stallion (exact name)")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--merge", action="store_true",
                    help="Resume: keep already-resolved records, only fetch missing ones")
    ap.add_argument("--headful", action="store_true",
                    help="Show the browser window (helpful for debugging Imperva challenges)")
    ap.add_argument("--debug-dump", metavar="DIR",
                    help="Save every page's HTML to DIR for inspection")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    names = load_roster_names()
    if args.only:
        names = [n for n in names if n.lower() == args.only.lower()]
        if not names:
            log.error(f"stallion {args.only!r} not found in roster")
            return 2

    # Load previous run when resuming
    resolved = {}
    unresolved_prev = []
    if args.merge and OUTPUT_JSON.exists():
        prev = json.loads(OUTPUT_JSON.read_text())
        resolved = prev.get("resolved", {})
        unresolved_prev = prev.get("unresolved", [])
        log.info(
            f"resuming: {len(resolved)} already resolved, "
            f"{len(unresolved_prev)} previously unresolved (will retry)"
        )

    total = len(names)
    log.info(f"processing {total} stallion(s)")

    unresolved = []
    dump_dir = Path(args.debug_dump) if args.debug_dump else None
    bh = BHBrowser(headful=args.headful, debug_dump_dir=dump_dir)
    try:
        for i, name in enumerate(names, 1):
            if name in resolved:
                log.info(f"[{i}/{total}] {name}: already have, skipping")
                continue
            record, err = resolve_one(bh, name)
            if record:
                resolved[name] = record
                h = record['height_hands']
                height_str = f"{h}h" if h is not None else "no-height"
                log.info(
                    f"[{i}/{total}] {name}: OK "
                    f"({record['year_of_birth']}, {record['color']}, "
                    f"{height_str}, stud {record['entered_stud_year']})"
                )
            else:
                unresolved.append({"name": name, "reason": err})
                log.warning(f"[{i}/{total}] {name}: UNRESOLVED ({err})")
            # Incremental save — so a crash / kill / Imperva block mid-run
            # doesn't lose what we've already fetched.
            save_output(resolved, unresolved, total)
    except KeyboardInterrupt:
        log.warning("interrupted — partial progress saved to output file")
    finally:
        bh.close()

    save_output(resolved, unresolved, total)
    log.info(
        f"wrote {OUTPUT_JSON.name}: {len(resolved)} resolved, "
        f"{len(unresolved)} unresolved"
    )
    if unresolved:
        log.info("Unresolved stallions (for manual review):")
        for u in unresolved:
            log.info(f"  - {u['name']}: {u['reason']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
