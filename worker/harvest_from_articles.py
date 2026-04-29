"""Harvest BH race-page URLs from BloodHorse article pages.

BH publishes race-recap articles after every major stakes win. Each article
typically embeds a link to the canonical /horse-racing/race/... URL of the
race it recaps. This script walks an article-listing page (or a custom list
of article URLs), opens each article, and extracts any embedded race URLs
matching the BH race-page format.

Output is a recent-stakes.txt file ready for scrape_stakes.py to consume.

Why this works when find_stakes_urls.py doesn't
-----------------------------------------------
BH's listing pages (/stakes-results, /graded-stakes-races/{year}) are
either subscription-gated or render summary-tables-only. Article pages, by
contrast, are public and almost always link to the canonical race URL in
their body. This makes article scraping the most reliable public discovery
path on BH for stakes-result coverage.

Usage
-----
  # Walk an article-tag/section index, open each article, harvest race URLs
  python3 harvest_from_articles.py --listing-url "https://www.bloodhorse.com/horse-racing/articles/section/thoroughbred-racing" --max-articles 30 --out recent-stakes.txt

  # Or pass an explicit list of article URLs (e.g. from search results)
  python3 harvest_from_articles.py --articles-file my-articles.txt --out recent-stakes.txt

  # Debug — save every page's HTML for inspection
  python3 harvest_from_articles.py --listing-url "..." --debug-dump ./html-dumps --verbose

Default listing
---------------
We default to the broad "thoroughbred-racing" article section since it's
public and high-volume. Likely tag-style alternatives the user can pass in:
  /horse-racing/articles/section/triple-crown
  /horse-racing/articles/section/breeders-cup
  /horse-racing/articles/section/pedigree-analysis
  /horse-racing/articles/tag/derby-dozen
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from pathlib import Path
from typing import Optional

# Lazy Playwright import — same pattern as the other scrapers in this dir
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
DEFAULT_LISTING_URL = "https://www.bloodhorse.com/horse-racing/articles/section/thoroughbred-racing"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
TIMEOUT = 30_000
RATE_LIMIT_SECONDS = 1.2

log = logging.getLogger("harvest-articles")


# Match BH article URL anywhere in href="..." — handles both absolute
# (https://www.bloodhorse.com/...) and relative (/horse-racing/...) forms.
# The slug often hints at race-recap content (e.g. "commandment-wins-florida-derby-...")
ARTICLE_URL_RE = re.compile(
    r'href="(?:https?://(?:www\.)?bloodhorse\.com)?'
    r'(?P<path>/horse-racing/articles/(?P<id>\d+)/(?P<slug>[a-z0-9-]+))"',
    flags=re.IGNORECASE,
)

# Match BH race-page URL inside an article body
RACE_URL_RE = re.compile(
    r'/horse-racing/race/(?P<country>[a-z]+)/(?P<track>[a-z]+)/'
    r'(?P<year>\d{4})/(?P<month>\d{1,2})/(?P<day>\d{1,2})/'
    r'(?P<race_no>\d+)/(?P<slug>[a-z0-9-]+)'
)

# Heuristic: keep articles whose slug suggests a race-result recap. Articles
# that don't match are probably feature stories / news / pedigree analysis
# that won't link to a race page. Keeps the per-article fetch budget tight.
SLUG_LOOKS_LIKE_RECAP = re.compile(
    r'-(wins?|takes|denies|stuns|captures|claims|romp[s]?-(to)?|'
    r'returns|holds-off|stays-perfect|edges)-',
    flags=re.IGNORECASE,
)


class Browser:
    def __init__(self, headful: bool = False, debug_dump_dir: Optional[Path] = None):
        _require_playwright()
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=not headful,
            args=["--disable-blink-features=AutomationControlled"],
        )
        self._context = self._browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 1400},
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

    def fetch(self, url: str, label: str = "page") -> tuple[str, str]:
        self._throttle()
        log.debug(f"goto {url}")
        try:
            self._page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT)
        except Exception as e:
            log.warning(f"  goto failed: {e}")
            return self._page.url, ""
        try:
            self._page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass
        # Trigger lazy-load
        for i in range(2):
            try:
                self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                self._page.wait_for_timeout(600)
            except Exception:
                break
        final_url = self._page.url
        html = self._page.content()
        log.debug(f"  -> final url={final_url}, {len(html)} bytes")
        if self._debug_dump_dir:
            self._debug_dump_dir.mkdir(parents=True, exist_ok=True)
            slug = re.sub(r"[^a-z0-9]+", "-", url.lower())[-80:]
            (self._debug_dump_dir / f"{label}-{slug}.html").write_text(html)
        return final_url, html

    def close(self):
        try: self._browser.close()
        except Exception: pass
        try: self._pw.stop()
        except Exception: pass


def harvest_article_urls(html: str, base_only_recap_slugs: bool = True) -> list[str]:
    seen = set()
    urls: list[str] = []
    for m in ARTICLE_URL_RE.finditer(html):
        path = m.group("path")
        slug = m.group("slug")
        if path in seen:
            continue
        seen.add(path)
        if base_only_recap_slugs and not SLUG_LOOKS_LIKE_RECAP.search(slug):
            continue
        urls.append("https://www.bloodhorse.com" + path)
    return urls


def harvest_race_urls(html: str, include_future: bool = False) -> list[str]:
    """Extract /horse-racing/race/... URLs from html. By default we filter out
    races whose date is in the future (we want past results, not upcoming
    entries — articles tend to link to BOTH, and we don't care about the
    upcoming ones for stakes-results purposes)."""
    from datetime import datetime
    today = datetime.utcnow().strftime("%Y-%m-%d")
    seen = set()
    urls: list[str] = []
    for m in RACE_URL_RE.finditer(html):
        if m.group("country") != "usa":
            continue
        full = "https://www.bloodhorse.com" + m.group(0)
        if full in seen:
            continue
        seen.add(full)
        if not include_future:
            race_date = f"{m.group('year')}-{int(m.group('month')):02d}-{int(m.group('day')):02d}"
            if race_date > today:
                continue
        urls.append(full)
    return urls


def main():
    ap = argparse.ArgumentParser(description="Harvest race URLs from BH articles")
    ap.add_argument("--listing-url", default=DEFAULT_LISTING_URL,
                    help="BH article-listing URL (section/tag index page)")
    ap.add_argument("--articles-file",
                    help="Optional path to a text file of article URLs (one per line)")
    ap.add_argument("--max-articles", type=int, default=20,
                    help="Cap on number of articles to open from the listing (default: 20)")
    ap.add_argument("--no-recap-filter", action="store_true",
                    help="Open EVERY article on the listing, not just ones whose slug looks like a race-recap")
    ap.add_argument("--include-future", action="store_true",
                    help="Include races whose date is in the future (default: past+today only — we want results, not upcoming entries)")
    ap.add_argument("--out", default="recent-stakes.txt", help="Output file (one race URL per line)")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--debug-dump", metavar="DIR")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    dump_dir = Path(args.debug_dump) if args.debug_dump else None
    bh = Browser(headful=args.headful, debug_dump_dir=dump_dir)

    article_urls: list[str] = []
    try:
        # Mode 1: listing-derived
        if not args.articles_file:
            log.info(f"Loading listing {args.listing_url}")
            _, listing_html = bh.fetch(args.listing_url, label="listing")
            article_urls = harvest_article_urls(
                listing_html,
                base_only_recap_slugs=not args.no_recap_filter,
            )
            log.info(f"  found {len(article_urls)} article URL(s) from listing"
                     + (" (filtered by recap slug)" if not args.no_recap_filter else ""))
            article_urls = article_urls[: args.max_articles]
        # Mode 2: explicit list
        else:
            for line in Path(args.articles_file).read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    article_urls.append(line)

        if not article_urls:
            log.warning("No article URLs to process. Run with --debug-dump and inspect the listing HTML.")
            return 1

        # Open each article, extract embedded race URLs
        race_urls: set[str] = set()
        article_to_races: dict[str, list[str]] = {}
        for i, art_url in enumerate(article_urls, 1):
            log.info(f"[{i}/{len(article_urls)}] {art_url}")
            _, art_html = bh.fetch(art_url, label="article")
            races = harvest_race_urls(art_html, include_future=args.include_future)
            if races:
                log.info(f"  -> {len(races)} race URL(s): {races[0]}")
            else:
                log.info(f"  -> no race URLs found")
            for r in races:
                race_urls.add(r)
            article_to_races[art_url] = races

        if not race_urls:
            log.warning(
                "No race URLs found in any article. Possible reasons:\n"
                "  - The articles in this section don't link to canonical race pages\n"
                "  - Recap-slug filter excluded everything (try --no-recap-filter)\n"
                "  - Try a different --listing-url (e.g. articles/section/triple-crown)"
            )
            return 1

        out_path = Path(args.out)
        sorted_urls = sorted(race_urls)
        out_path.write_text("\n".join(sorted_urls) + "\n")
        log.info(f"\nWrote {len(sorted_urls)} race URL(s) to {out_path}:")
        for u in sorted_urls[:25]:
            print(f"  {u}")
        if len(sorted_urls) > 25:
            print(f"  ... ({len(sorted_urls) - 25} more)")
        log.info(f"\nNext: python3 scrape_stakes.py --urls-file {args.out} --merge --verbose")
    finally:
        bh.close()


if __name__ == "__main__":
    sys.exit(main() or 0)
