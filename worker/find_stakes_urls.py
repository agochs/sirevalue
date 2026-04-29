"""Discover recent stakes-race URLs from BloodHorse's stakes-results listing.

Walks https://www.bloodhorse.com/horse-racing/stakes-results, extracts every
URL that matches the BH race-page format, optionally filters by grade
(G1/G2/G3/Listed) and date window, then writes them to a text file ready
for scrape_stakes.py to consume.

Usage
-----
  # Find every recent stakes URL on the page
  python3 find_stakes_urls.py --out recent-stakes.txt

  # Only G1/G2 from the past 30 days
  python3 find_stakes_urls.py --grade G1 G2 --days 30 --out recent-stakes.txt

  # Watch the browser drive itself (selector-debugging)
  python3 find_stakes_urls.py --headful --verbose --debug-dump ./html-dumps

  # Pipe directly into the scraper (one-shot end-to-end)
  python3 find_stakes_urls.py --out recent-stakes.txt && \
    python3 scrape_stakes.py --urls-file recent-stakes.txt --merge --verbose && \
    python3 build_stakes.py

Selector strategy
-----------------
The listing page renders results as cards or rows containing one anchor per
race pointing to the canonical race URL. We extract every href matching the
URL_RE pattern (re-used from scrape_stakes.py) and dedup. Surrounding text
is captured for sanity-printing.

ZERO FABRICATION rule: this script doesn't infer anything — it just lists
what's actually on the page. Tuning happens here only if BH changes their
URL format or page structure; the per-race scrape is then the next step.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# Lazy Playwright import — same pattern as scrape_stakes.py
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
# Try these listing URLs in order until one returns race URLs. The first is
# BloodHorse+ subscription-gated (often returns only nav menus). The second
# is the open Graded Stakes Races index by year. The third is the open
# stakes-calendar page.
DEFAULT_LISTING_URLS = [
    "https://www.bloodhorse.com/horse-racing/thoroughbred-racing/graded-stakes-races/2026",
    "https://www.bloodhorse.com/horse-racing/thoroughbred-racing/stakes-calendar",
    "https://www.bloodhorse.com/horse-racing/stakes-results",
]

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
TIMEOUT = 30_000

log = logging.getLogger("find-stakes-urls")


# Match BH race URL anywhere in HTML. Same shape as scrape_stakes.parse_race_url.
URL_RE = re.compile(
    r"/horse-racing/race/(?P<country>[a-z]+)/(?P<track>[a-z]+)/"
    r"(?P<year>\d{4})/(?P<month>\d{1,2})/(?P<day>\d{1,2})/"
    r"(?P<race_no>\d+)/(?P<slug>[a-z0-9-]+)"
)


def url_grade(slug: str) -> Optional[str]:
    """Pick a race grade out of the slug, e.g. 'santa-anita-derby-g1' → 'G1'.
    Returns None if no grade marker found (slug for a non-graded stakes)."""
    m = re.search(r"-(g[123]|listed|black-type)$", slug, flags=re.IGNORECASE)
    if not m:
        return None
    g = m.group(1).lower()
    if g.startswith("g"):
        return g.upper()
    if g == "listed":
        return "Listed"
    return "Black Type"


def url_date(year: str, month: str, day: str) -> str:
    return f"{year}-{int(month):02d}-{int(day):02d}"


def main():
    ap = argparse.ArgumentParser(description="Harvest BH stakes-results URLs")
    ap.add_argument("--out", default="recent-stakes.txt", help="Output text file (one URL per line)")
    ap.add_argument("--grade", nargs="+", choices=["G1", "G2", "G3", "Listed", "Black Type"],
                    default=None,
                    help="Optional grade filter (uses slug suffix). Triple Crown / Breeders' Cup races often lack a grade suffix in their slug — leave unset to include them.")
    ap.add_argument("--days", type=int, default=None,
                    help="Only include races from the past N days (default: any date)")
    ap.add_argument("--include-future", action="store_true",
                    help="Include races whose date is in the future (default: past + today only)")
    ap.add_argument("--listing-url", action="append",
                    help="Override the listing URL(s). Pass multiple times to try several.")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--debug-dump", metavar="DIR", help="Save the listing HTML for inspection")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    grades_wanted = set(args.grade) if args.grade else None
    listing_urls = args.listing_url or DEFAULT_LISTING_URLS

    _require_playwright()
    pw = sync_playwright().start()
    html = ""
    final_url = ""
    try:
        browser = pw.chromium.launch(
            headless=not args.headful,
            args=["--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 1400},
            locale="en-US",
        )
        page = ctx.new_page()

        # Try each listing URL in order; stop when we get >0 race URLs
        all_html_pieces = []
        for url in listing_urls:
            log.info(f"Loading {url}")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT)
            except Exception as e:
                log.warning(f"  goto failed: {e}")
                continue
            try:
                page.wait_for_load_state("networkidle", timeout=12_000)
            except Exception:
                pass
            log.info(f"  final URL: {page.url}")
            for i in range(3):
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(700)
                except Exception as e:
                    log.debug(f"  scroll {i+1} skipped: {e}")
                    break
            try:
                page.wait_for_load_state("networkidle", timeout=5_000)
            except Exception:
                pass
            page_html = page.content()
            all_html_pieces.append(page_html)
            n_matches = len(URL_RE.findall(page_html))
            log.info(f"  found {n_matches} URL match(es) in {len(page_html)} bytes")
            # Save each listing's HTML separately so we can debug link
            # patterns per source page (not just the union).
            if args.debug_dump:
                d = Path(args.debug_dump)
                d.mkdir(parents=True, exist_ok=True)
                slug = re.sub(r"[^a-z0-9]+", "-", url.lower())[-80:]
                (d / f"listing-{slug}.html").write_text(page_html)
            if n_matches >= 5:
                final_url = page.url
                html = page_html
                break

        # If no individual page hit our threshold, concatenate everything we
        # did pull and parse from the union — better than nothing.
        if not html:
            html = "\n".join(all_html_pieces)
            log.info(f"  using union of all pages tried ({len(html)} bytes)")
        log.debug(f"got {len(html)} bytes of HTML")
        if args.debug_dump:
            d = Path(args.debug_dump)
            d.mkdir(parents=True, exist_ok=True)
            (d / "stakes-results-listing.html").write_text(html)
            log.debug(f"saved listing HTML to {d}/stakes-results-listing.html")

        # Extract every race URL match, dedupe by full path
        seen: set[str] = set()
        rows: list[dict] = []
        for m in URL_RE.finditer(html):
            country = m.group("country")
            if country != "usa":
                continue   # listing sometimes mixes in foreign races
            full_path = m.group(0)
            full_url = "https://www.bloodhorse.com" + full_path
            if full_url in seen:
                continue
            seen.add(full_url)
            slug = m.group("slug")
            grade = url_grade(slug)
            d = url_date(m.group("year"), m.group("month"), m.group("day"))
            rows.append({
                "url": full_url, "slug": slug, "grade": grade, "date": d,
                "track": m.group("track"),
            })

        log.info(f"Found {len(rows)} unique race URL(s) on the listing")

        # Filter by grade — only when explicitly requested (default is no filter
        # so Triple Crown / Breeders' Cup races without grade suffixes pass)
        if grades_wanted:
            rows = [r for r in rows if r["grade"] in grades_wanted]
        # Default: only include races whose date has passed (we want results,
        # not upcoming entries). Override with --include-future.
        if not args.include_future:
            today = datetime.utcnow().strftime("%Y-%m-%d")
            rows = [r for r in rows if r["date"] <= today]
        # Filter by date window
        if args.days:
            cutoff = (datetime.utcnow() - timedelta(days=args.days)).strftime("%Y-%m-%d")
            rows = [r for r in rows if r["date"] >= cutoff]

        # Sort by date desc
        rows.sort(key=lambda r: r["date"], reverse=True)

        out_path = Path(args.out)
        out_path.write_text("\n".join(r["url"] for r in rows) + ("\n" if rows else ""))
        log.info(f"\nWrote {len(rows)} URL(s) to {out_path}:")
        for r in rows[:25]:
            print(f"  {r['date']} {r['track']:>4s} {r['grade'] or '   ':>5s}  {r['url']}")
        if len(rows) > 25:
            print(f"  ... ({len(rows) - 25} more)")
        log.info(f"\nNext: python3 scrape_stakes.py --urls-file {args.out} --merge --verbose")
    finally:
        try: browser.close()
        except Exception: pass
        try: pw.stop()
        except Exception: pass


if __name__ == "__main__":
    sys.exit(main() or 0)
