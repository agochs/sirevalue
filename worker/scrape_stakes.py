"""Scrape stakes race results from BloodHorse race pages and append them
to stakes-results.json (with --merge to skip already-known entries).

Modeled on enrich_from_bloodhorse.py: same Playwright + Imperva handling,
same merge semantics, same ZERO FABRICATION discipline (a record is only
emitted if every required field parses cleanly; anything ambiguous goes to
'unresolved' with a reason the user can act on).

Usage
-----
  # 1. Scrape one race by its BH URL — fastest way to verify selectors
  python3 scrape_stakes.py --url "https://www.bloodhorse.com/horse-racing/race/usa/sa/2026/4/4/10/santa-anita-derby-g1"

  # 2. Scrape every URL listed in a file (one per line)
  python3 scrape_stakes.py --urls-file recent-stakes.txt --merge

  # 3. Watch the browser drive itself (for selector-debugging)
  python3 scrape_stakes.py --url "..." --headful --verbose

  # 4. Dump every page's HTML for offline inspection when parsing fails
  python3 scrape_stakes.py --url "..." --debug-dump ./html-dumps

After scraping, run build_stakes.py to copy + index into public/data/.

Race URL format on BloodHorse
-----------------------------
  /horse-racing/race/{country}/{track}/{YYYY}/{M}/{D}/{race_no}/{slug}

  e.g. /horse-racing/race/usa/sa/2026/4/4/10/santa-anita-derby-g1

Selectors (best guesses; tune as needed)
----------------------------------------
The BH race page lays out result data in semi-structured blocks. We try
several CSS / regex patterns per field. If everything fails for a record,
the URL goes into 'unresolved' with a reason like "winner_sire not found"
so the user can either fix selectors here or hand-add the entry to
worker/stakes-results.json.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Optional

# Lazy Playwright import — same pattern as enrich_from_bloodhorse.py
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
OUTPUT_JSON = HERE / "stakes-results.json"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
RATE_LIMIT_SECONDS = 1.5
TIMEOUT = 30_000

log = logging.getLogger("stakes-scrape")


# ---------------------------------------------------------------------------
# URL parsing — extract date / track / race no from a BH race URL
# ---------------------------------------------------------------------------

URL_RE = re.compile(
    r"/horse-racing/race/(?P<country>[a-z]+)/(?P<track>[a-z]+)/"
    r"(?P<year>\d{4})/(?P<month>\d{1,2})/(?P<day>\d{1,2})/"
    r"(?P<race_no>\d+)/(?P<slug>[a-z0-9-]+)"
)


def parse_race_url(url: str) -> Optional[dict]:
    m = URL_RE.search(url)
    if not m:
        return None
    g = m.groupdict()
    return {
        "country": g["country"],
        "track":   g["track"],
        "date":    f"{g['year']}-{int(g['month']):02d}-{int(g['day']):02d}",
        "race_no": int(g["race_no"]),
        "slug":    g["slug"],
    }


# ---------------------------------------------------------------------------
# HTML parsing — selector heuristics
# ---------------------------------------------------------------------------

# Race title typically in <h1> at top of page
H1_RE = re.compile(r"<h1[^>]*>\s*(.*?)\s*</h1>", flags=re.DOTALL | re.IGNORECASE)

# Grade (G1/G2/G3/Listed) often appears in title or a sub-line
GRADE_RE = re.compile(r"\b(G[123]|Listed|Black\s*Type)\b", flags=re.IGNORECASE)

# Purse ("$1,000,000" near "purse"). Optional — many BH race pages don't
# expose purse in the structured area; we set null when missing.
PURSE_RE = re.compile(r"purse[^$]{0,40}\$\s*([\d,]+)", flags=re.IGNORECASE)

# Winner: BH's race page shows each finisher in an <h4><span class="horseName">
# block, in finish order. The FIRST one is the winner.
#   <h4><span class="horseName"><a href="...">So Happy</a></span></h4>
WINNER_NAME_RE = re.compile(
    r'<span\s+class="horseName"\s*>\s*<a[^>]*href="(?P<href>[^"]+)"[^>]*>(?P<name>[^<]+?)</a>',
    flags=re.DOTALL | re.IGNORECASE,
)

# Per-horse pedigree block, immediately following the horseName span:
#   <p class="race-data-pedigree">
#     <a href=".../stallions/.../runhappy" title="Runhappy | ...">Runhappy</a> ...
#     <span> – So Cunning</span>
#   </p>
# We capture sire from the first <a> inside this block, and dam from the
# trailing <span> after the en-dash separator. Damsire is NOT in this block —
# BH puts it elsewhere ("Broodmare Sire:" structured field), so we leave it
# null and let downstream enrichment fill it in.
PEDIGREE_BLOCK_RE = re.compile(
    r'<p\s+class="race-data-pedigree"\s*>\s*'
    r'<a[^>]*href="[^"]*stallion-register[^"]*"[^>]*>(?P<sire>[^<]+?)</a>'
    r'.*?<span>\s*[–—\-]\s*(?P<dam>[^<]+?)</span>',
    flags=re.DOTALL | re.IGNORECASE,
)

# Age block right after pedigree: <p class="race-data-age">3YO\nColt</p>
AGE_RE = re.compile(
    r'<p\s+class="race-data-age"\s*>\s*(?P<age>\d+)\s*YO',
    flags=re.IGNORECASE,
)


def parse_race_page(html: str, url_meta: dict) -> tuple[Optional[dict], Optional[str]]:
    """Try to extract a stakes-result record from a BH race page.

    Returns (record, None) on success or (None, reason) on parse failure.
    The caller writes successful records to stakes-results.json and routes
    failures to 'unresolved' for manual review.
    """
    # Race title
    h1 = H1_RE.search(html)
    if not h1:
        return None, "no <h1> found on page"
    raw_title = re.sub(r"<[^>]+>", "", h1.group(1)).strip()
    raw_title = re.sub(r"\s+", " ", raw_title)
    if not raw_title:
        return None, "<h1> empty"

    # Grade
    grade_m = GRADE_RE.search(raw_title) or GRADE_RE.search(html[:5000])
    grade = grade_m.group(1).upper().replace(" ", "") if grade_m else None
    # Race name = title minus the trailing grade label
    race_name = raw_title
    if grade:
        race_name = re.sub(GRADE_RE, "", race_name).strip(" ()-,")

    # Purse (optional)
    purse_m = PURSE_RE.search(html)
    purse_usd = int(purse_m.group(1).replace(",", "")) if purse_m else None

    # Winner — the FIRST <span class="horseName"> on the page is the winner
    win_m = WINNER_NAME_RE.search(html)
    if not win_m:
        return None, "winner row not found (WINNER_NAME_RE may need tuning)"
    winner_name = re.sub(r"\s+", " ", win_m.group("name")).strip()

    # Pedigree — within a window AFTER the winner's name span. The pedigree
    # block appears immediately after the horse-name <h4>.
    after = html[win_m.end() : win_m.end() + 4000]
    ped = PEDIGREE_BLOCK_RE.search(after)
    sire = dam = None
    if ped:
        sire = re.sub(r"\s+", " ", ped.group("sire")).strip()
        dam  = re.sub(r"\s+", " ", ped.group("dam")).strip()
    if not sire:
        return None, "winner_sire not parsed (PEDIGREE_BLOCK_RE may need tuning)"

    # Damsire is NOT in the per-finisher pedigree block on BH race pages.
    # Leave null; can be backfilled by re-running BH bio scrape on the winner.
    damsire = None

    # Age — "3YO" inside <p class="race-data-age">
    age_m = AGE_RE.search(after)
    winner_age = int(age_m.group("age")) if age_m else None

    # Build the record (matches the schema in stakes-results.json)
    rec = {
        "race":            race_name,
        "race_grade":      grade,
        "track":           url_meta.get("track", "").upper(),
        "date":            url_meta.get("date"),
        "purse_usd":       purse_usd,
        "winner":          winner_name,
        "winner_age":      winner_age,
        "winner_sire":     sire,
        "winner_dam":      dam,
        "winner_damsire":  damsire,
        "source_url":      None,   # filled in by caller
        "note":            None,   # optional manual context
    }
    return rec, None


# ---------------------------------------------------------------------------
# HTTP — Playwright wrapper (mirror of enrich_from_bloodhorse.py)
# ---------------------------------------------------------------------------

class BHBrowser:
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
        self._debug_dump_dir = debug_dump_dir

    def _throttle(self):
        elapsed = time.time() - self._last_request_at
        if elapsed < RATE_LIMIT_SECONDS:
            time.sleep(RATE_LIMIT_SECONDS - elapsed)
        self._last_request_at = time.time()

    def fetch(self, url: str) -> tuple[str, str]:
        """Navigate to URL. Returns (final_url, html). Imperva challenges are
        handled by Playwright executing the JS naturally; we just wait."""
        self._throttle()
        log.debug(f"goto {url}")
        self._page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT)
        try:
            self._page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass
        final_url = self._page.url
        html = self._page.content()
        log.debug(f"  -> final url={final_url}, {len(html)} bytes")
        if self._debug_dump_dir:
            self._debug_dump_dir.mkdir(parents=True, exist_ok=True)
            slug = re.sub(r"[^a-z0-9]+", "-", url.lower())[-80:]
            (self._debug_dump_dir / f"race-{slug}.html").write_text(html)
        return final_url, html

    def close(self):
        try: self._browser.close()
        except Exception: pass
        try: self._pw.stop()
        except Exception: pass


# ---------------------------------------------------------------------------
# Merge with existing stakes-results.json
# ---------------------------------------------------------------------------

def load_existing() -> dict:
    if not OUTPUT_JSON.exists():
        return {"races": [], "_doc": "Created by scrape_stakes.py"}
    return json.loads(OUTPUT_JSON.read_text())


def write_existing(data: dict):
    OUTPUT_JSON.write_text(json.dumps(data, indent=2))


def fingerprint(rec: dict) -> str:
    """Stable fingerprint for dedup. We normalize aggressively so harmless
    differences don't create duplicates:
      - Race name: lowercase, drop trailing 'stakes'/'s.'/'(g1)'/etc, strip
        non-alphanumeric. So "Toyota Blue Grass Stakes" and "Toyota Blue
        Grass S." collapse to the same key.
      - Date: only YYYY-MM (publish-day vs race-day off-by-one is common
        across sources; same race + same month + same winner = same race).
      - Winner: lowercase + alphanumeric only."""
    race_n = (rec.get("race") or "").lower()
    race_n = re.sub(r"\b(stakes|s\.|presented by[^,]+|grade[s]?)\b", "", race_n)
    race_n = re.sub(r"\([^)]+\)", "", race_n)
    race_n = re.sub(r"[^a-z0-9]+", "", race_n)
    date_ym = (rec.get("date") or "")[:7]   # YYYY-MM
    winner_n = re.sub(r"[^a-z0-9]+", "", (rec.get("winner") or "").lower())
    return "|".join([race_n, date_ym, winner_n])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Scrape stakes results from BH and append to stakes-results.json")
    ap.add_argument("--url", help="Single BH race URL")
    ap.add_argument("--urls-file", help="Path to a text file with one race URL per line")
    ap.add_argument("--merge", action="store_true", help="Skip records whose fingerprint already exists")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--headful", action="store_true", help="Show the browser window for debugging")
    ap.add_argument("--debug-dump", metavar="DIR", help="Save every page's HTML to DIR for inspection")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Collect URLs from --url or --urls-file
    urls: list[str] = []
    if args.url:
        urls.append(args.url.strip())
    if args.urls_file:
        for line in Path(args.urls_file).read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    if not urls:
        log.error("No URLs supplied. Pass --url or --urls-file.")
        return 2

    existing = load_existing()
    existing_races = existing.get("races") or []
    seen = {fingerprint(r) for r in existing_races} if args.merge else set()

    dump_dir = Path(args.debug_dump) if args.debug_dump else None
    bh = BHBrowser(headful=args.headful, debug_dump_dir=dump_dir)

    added: list[dict] = []
    unresolved: list[dict] = []
    try:
        for i, url in enumerate(urls, 1):
            log.info(f"[{i}/{len(urls)}] {url}")
            url_meta = parse_race_url(url)
            if not url_meta:
                unresolved.append({"url": url, "reason": "URL did not match BH race format"})
                log.warning(f"   bad URL format")
                continue
            try:
                final_url, html = bh.fetch(url)
            except Exception as e:
                unresolved.append({"url": url, "reason": f"fetch failed: {e}"})
                log.warning(f"   fetch failed: {e}")
                continue
            rec, err = parse_race_page(html, url_meta)
            if not rec:
                unresolved.append({"url": url, "reason": err})
                log.warning(f"   parse failed: {err}")
                continue
            rec["source_url"] = final_url
            fp = fingerprint(rec)
            if args.merge and fp in seen:
                log.info(f"   already have {rec['race']} ({rec['date']}) — skipping")
                continue
            existing_races.append(rec)
            seen.add(fp)
            added.append(rec)
            log.info(f"   OK: {rec['race']} ({rec['race_grade']}) — {rec['winner']} by {rec['winner_sire']}")
    finally:
        bh.close()

    # Sort by date desc and persist
    existing_races.sort(key=lambda r: r.get("date") or "0000-00-00", reverse=True)
    existing["races"] = existing_races
    existing["updated_at"] = time.strftime("%Y-%m-%d")
    # Always overwrite the unresolved field so a clean run wipes stale entries
    if unresolved:
        existing["unresolved"] = unresolved
    elif "unresolved" in existing:
        del existing["unresolved"]
    write_existing(existing)
    log.info(f"\nDone. Added {len(added)} new race(s); {len(unresolved)} unresolved; total {len(existing_races)}.")
    if unresolved:
        for u in unresolved:
            log.info(f"  - {u['url']}: {u['reason']}")
    log.info(f"\nNext: python3 build_stakes.py  (regenerates public/data/stakes-results.json)")


if __name__ == "__main__":
    sys.exit(main() or 0)
