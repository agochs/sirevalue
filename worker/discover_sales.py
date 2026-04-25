"""Discover BH sale URLs for 2022-2026 by walking BloodHorse's sales archive.

Strategy:
  1. Fetch a set of candidate index/archive pages on BH (Playwright to defeat
     Imperva).
  2. Extract every link matching /horse-racing/thoroughbred-sales/results/
     {year}/{sale_id}/{slug}/ — these are the per-sale result pages.
  3. Filter to 2022-2026 and to major US sales (Keeneland / Fasig-Tipton / OBS).
  4. Write the resulting list into sales-config.json (preserving any manual
     entries already there).
  5. Report what was found and what's missing so the user can fill any gaps.

Major sales we care about (slugs may vary slightly per year, regex-tolerant):
  Keeneland: September Yearling, November Breeding Stock, January, April HRA
  Fasig-Tipton: Saratoga Select, Saratoga NY-Bred, Kentucky July, Kentucky October,
                Midlantic Fall, Midlantic May, March 2YO, November Breeding Stock
  OBS: March, Spring, April, June, October Yearling, Winter Mixed

Usage:
  pip3 install playwright beautifulsoup4   # already installed
  cd ~/sirevalue/worker
  python3 discover_sales.py --years 2022-2026 --verbose
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("beautifulsoup4 not installed. Run: pip3 install beautifulsoup4", file=sys.stderr)
    sys.exit(1)

sync_playwright = None
def _require_playwright():
    global sync_playwright
    if sync_playwright is None:
        try:
            from playwright.sync_api import sync_playwright as _sp
            sync_playwright = _sp
        except ImportError:
            print("Playwright not installed. Run: pip3 install playwright && python3 -m playwright install chromium", file=sys.stderr)
            sys.exit(1)

HERE = Path(__file__).parent
SALES_CONFIG_JSON = HERE / "sales-config.json"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
BH_HOME = "https://www.bloodhorse.com/horse-racing/thoroughbred-sales/"

# Candidate index / archive pages to scrape. We try several because BH's
# archive structure isn't documented. Each may yield some URLs; we union the
# results and dedupe.
CANDIDATE_INDEXES = [
    "https://www.bloodhorse.com/horse-racing/thoroughbred-sales/results",
    "https://www.bloodhorse.com/horse-racing/thoroughbred-sales/",
    "https://www.bloodhorse.com/horse-racing/thoroughbred-sales/results/2026",
    "https://www.bloodhorse.com/horse-racing/thoroughbred-sales/results/2025",
    "https://www.bloodhorse.com/horse-racing/thoroughbred-sales/results/2024",
    "https://www.bloodhorse.com/horse-racing/thoroughbred-sales/results/2023",
    "https://www.bloodhorse.com/horse-racing/thoroughbred-sales/results/2022",
]

SALE_LINK_RE = re.compile(
    r"/horse-racing/thoroughbred-sales/results/(\d{4})/(\d+)/([a-z0-9\-]+?)/?(?:\d+)?$",
    flags=re.IGNORECASE,
)

# Keywords identifying major US sales we care about (case-insensitive substring)
MAJOR_SALE_KEYWORDS = [
    "keeneland",
    "fasig-tipton",
    "ocala-breeders",   # OBS slug starts with this
    "obs-",
]

# Common sale-name patterns to give each discovered URL a clean display name
def humanize_slug(slug: str) -> str:
    s = slug.replace("-", " ").strip()
    return " ".join(w.capitalize() for w in s.split())


log = logging.getLogger("discover")


class BHBrowser:
    def __init__(self, headful: bool = False):
        _require_playwright()
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=not headful,
            args=["--disable-blink-features=AutomationControlled"],
        )
        self._context = self._browser.new_context(
            user_agent=USER_AGENT, viewport={"width": 1280, "height": 900}, locale="en-US"
        )
        self._page = self._context.new_page()
        self._warmed_up = False

    def warm_up(self):
        if self._warmed_up:
            return
        log.debug(f"warm-up: {BH_HOME}")
        self._page.goto(BH_HOME, wait_until="domcontentloaded", timeout=30_000)
        try:
            self._page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass
        self._warmed_up = True

    def get_html(self, url: str) -> str | None:
        self.warm_up()
        log.info(f"fetching {url}")
        try:
            self._page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            try:
                self._page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:
                pass
            html = self._page.content()
            log.debug(f"  -> {len(html)} bytes")
            return html
        except Exception as e:
            log.warning(f"  fetch failed: {e}")
            return None

    def close(self):
        try:
            self._context.close()
            self._browser.close()
        finally:
            self._pw.stop()


def extract_sale_links(html: str) -> list[tuple[int, int, str]]:
    """Find every (year, sale_id, slug) triple linked from this HTML."""
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.find_all("a", href=True):
        m = SALE_LINK_RE.search(a["href"])
        if not m:
            continue
        try:
            year = int(m.group(1))
            sale_id = int(m.group(2))
            slug = m.group(3).lower()
        except (ValueError, AttributeError):
            continue
        out.append((year, sale_id, slug))
    return out


def is_major_us_sale(slug: str) -> bool:
    sl = slug.lower()
    return any(k in sl for k in MAJOR_SALE_KEYWORDS)


def merge_into_config(discovered: list[dict]) -> dict:
    """Merge discovered sales into the existing sales-config.json. Existing
    'sales' entries are kept verbatim; discovered entries are added if their
    URL isn't already present. Returns the merged config."""
    if SALES_CONFIG_JSON.exists():
        try:
            cfg = json.loads(SALES_CONFIG_JSON.read_text())
        except Exception:
            cfg = {}
    else:
        cfg = {}

    existing = cfg.get("sales", []) or []
    existing_urls = {s.get("url", "").rstrip("/") for s in existing}

    added = 0
    for d in discovered:
        if d["url"].rstrip("/") in existing_urls:
            continue
        existing.append(d)
        existing_urls.add(d["url"].rstrip("/"))
        added += 1

    cfg["sales"] = existing
    cfg.setdefault("_doc", []).append(
        f"Auto-discovered {added} sale(s) at {datetime.utcnow().isoformat(timespec='seconds')}Z"
    )
    SALES_CONFIG_JSON.write_text(json.dumps(cfg, indent=2, ensure_ascii=False))
    return {"merged_added": added, "total": len(existing)}


def main() -> int:
    ap = argparse.ArgumentParser(description="Discover BH sale URLs for a year range")
    ap.add_argument("--years", default="2022-2026",
                    help="Year range, e.g. '2022-2026' or '2024,2025'")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--all", action="store_true",
                    help="Don't filter to major US sales — keep every discovered URL")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show what would be added without writing sales-config.json")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Parse --years
    years = set()
    for chunk in args.years.split(","):
        chunk = chunk.strip()
        if "-" in chunk:
            a, b = chunk.split("-", 1)
            years.update(range(int(a), int(b) + 1))
        elif chunk:
            years.add(int(chunk))
    log.info(f"target years: {sorted(years)}")

    bh = BHBrowser()
    found: dict[tuple[int, int, str], dict] = {}
    try:
        for url in CANDIDATE_INDEXES:
            html = bh.get_html(url)
            if not html:
                continue
            links = extract_sale_links(html)
            log.info(f"  parsed {len(links)} sale link(s)")
            for year, sale_id, slug in links:
                if year not in years:
                    continue
                if not args.all and not is_major_us_sale(slug):
                    continue
                key = (year, sale_id, slug)
                if key in found:
                    continue
                full_url = (
                    f"https://www.bloodhorse.com/horse-racing/thoroughbred-sales/"
                    f"results/{year}/{sale_id}/{slug}/"
                )
                found[key] = {
                    "name": humanize_slug(slug),
                    "url":  full_url,
                    "kind": "auto",
                }
            time.sleep(1)
    finally:
        bh.close()

    log.info(f"discovered {len(found)} unique sale(s) across {len(years)} year(s)")
    if not found:
        log.warning("no sales found — try --all to disable major-sale filter, "
                    "or check whether BH's archive page rendered correctly")

    by_year: dict[int, list] = {}
    for entry in found.values():
        m = SALE_LINK_RE.search(entry["url"])
        if m:
            by_year.setdefault(int(m.group(1)), []).append(entry["name"])
    print()
    print("Found by year:")
    for y in sorted(by_year):
        print(f"  {y}: {len(by_year[y])} sales")
        for name in sorted(by_year[y])[:8]:
            print(f"    - {name}")
        if len(by_year[y]) > 8:
            print(f"    … +{len(by_year[y]) - 8} more")

    if args.dry_run:
        print("\n--dry-run: not writing sales-config.json")
        return 0

    discovered_list = list(found.values())
    discovered_list.sort(key=lambda x: x["url"])
    stats = merge_into_config(discovered_list)
    print(f"\nWrote sales-config.json: {stats['merged_added']} new entries, "
          f"{stats['total']} total in 'sales' list")
    return 0


if __name__ == "__main__":
    sys.exit(main())
