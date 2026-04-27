"""Scrape a farm's stallion roster page into a {farm}-roster.csv file.

Each farm's HTML is different so we drive the scraper from a per-farm config
in farm-scrape-config.json. The config lists the URL + a set of CSS selectors
that locate the stallion name, sire, dam, damsire, and stud fee in the DOM.

This script is intentionally conservative: any field whose selector returns
empty or unparseable text is left blank in the CSV (with the row name still
captured) so the user can fill it in manually. We never fabricate values.

Usage:
  python3 scrape_farm_roster.py --farm pinoak
  python3 scrape_farm_roster.py --farm millridge --dry-run
  python3 scrape_farm_roster.py --all

Output:
  worker/{farm-id}-roster.csv with columns:
    name, sire, dam, damsire, fee_usd, fee_terms, fee_qualifier,
    year_of_birth, color, entered_stud_year

Notes:
  - Uses Playwright Chromium with browser-like headers (defeats most CDN
    bot-detection). Same approach as scrapers.py for sale-results pages.
  - Each farm runs in a fresh browser context so cookies/auth don't leak.
  - Rate-limited at 2 seconds between farm scrapes to be polite.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent
CONFIG_JSON = HERE / "farm-scrape-config.json"


def load_config() -> dict:
    if not CONFIG_JSON.exists():
        raise SystemExit(f"Missing {CONFIG_JSON.name}. See worker/farm-scrape-config.json template.")
    return json.loads(CONFIG_JSON.read_text())


def parse_money(text: str) -> tuple[int | None, str | None]:
    """Extract integer USD fee from a string like '$15,000' or 'Private'.
    Returns (fee_usd, fee_qualifier). fee_qualifier captures 'Private',
    'Pensioned', etc. when no number is present."""
    if not text:
        return None, None
    s = text.strip()
    # Look for $X,XXX or X,XXX patterns
    m = re.search(r"\$?\s*([\d,]+)", s)
    if m:
        digits = m.group(1).replace(",", "")
        try:
            return int(digits), None
        except ValueError:
            pass
    # No number → return the qualifier text (Private, On Application, etc.)
    cleaned = re.sub(r"\s+", " ", s).strip()
    return None, cleaned[:40] if cleaned else None


CHALLENGE_TITLES = (
    "checking your browser",
    "just a moment",
    "attention required",
    "please wait",
)


def wait_through_challenge(page, max_wait_ms: int = 12_000):
    """If the page is on a Cloudflare/Imperva-style 'Checking your browser…'
    interstitial, wait for it to clear. Returns True if cleared, False if
    we ran out of patience and the title still looks like a challenge page."""
    waited = 0
    step = 1_500
    while waited < max_wait_ms:
        title = (page.title() or "").lower()
        if not any(c in title for c in CHALLENGE_TITLES):
            return True   # cleared
        page.wait_for_timeout(step)
        waited += step
    return False


def scrape_one(page, farm_id: str, farm_cfg: dict) -> list[dict]:
    """Scrape a single farm's roster page into a list of stallion dict rows."""
    url = farm_cfg["url"]
    selectors = farm_cfg.get("selectors") or {}
    print(f"  fetching {url}")
    page.goto(url, wait_until="networkidle", timeout=30_000)
    page.wait_for_timeout(1_500)   # give JS a moment to populate
    # Some sites (Cloudflare-fronted) show a challenge page first. Wait it out.
    if any(c in (page.title() or "").lower() for c in CHALLENGE_TITLES):
        print(f"  challenge interstitial detected — waiting for it to clear…")
        cleared = wait_through_challenge(page)
        if not cleared:
            print(f"  challenge did not clear; this site needs manual entry "
                  f"or a stealth plugin (playwright-stealth)")
            return []

    # The "stallion card" selector identifies each stallion's containing element
    card_sel = selectors.get("stallion_card")
    if not card_sel:
        raise ValueError(f"farm '{farm_id}' has no stallion_card selector")
    cards = page.query_selector_all(card_sel)
    print(f"  found {len(cards)} stallion card(s)")

    rows = []
    for card in cards:
        def grab(key: str) -> str:
            sel = selectors.get(key)
            if not sel:
                return ""
            try:
                el = card.query_selector(sel)
                return (el.inner_text() if el else "").strip()
            except Exception:
                return ""

        name = grab("name")
        if not name:
            continue   # skip if we can't even get the name

        # Pedigree often comes as a single line "Sire – Dam, by Damsire"
        # so allow either a single 'pedigree' selector or three separate ones.
        sire = grab("sire")
        dam = grab("dam")
        damsire = grab("damsire")
        if not sire or not dam:
            ped = grab("pedigree")
            if ped:
                # Common pattern: "Sire – Dam, by Damsire" or "Sire x Dam by Damsire"
                m = re.match(
                    r"\s*(?P<sire>[^–\-x×]+?)\s*[–\-x×]\s*"
                    r"(?P<dam>[^,]+?)\s*,\s*by\s*(?P<damsire>.+?)\s*$",
                    ped,
                )
                if m:
                    sire = sire or m.group("sire").strip()
                    dam = dam or m.group("dam").strip()
                    damsire = damsire or m.group("damsire").strip()

        fee_text = grab("fee")
        fee_usd, fee_qualifier = parse_money(fee_text)

        rows.append({
            "name": name,
            "sire": sire,
            "dam": dam,
            "damsire": damsire,
            "fee_usd": fee_usd or "",
            "fee_terms": "",   # most farm pages don't expose; leave blank
            "fee_qualifier": fee_qualifier or "",
            "year_of_birth": grab("year_of_birth"),
            "color": grab("color"),
            "entered_stud_year": grab("entered_stud_year"),
        })

    return rows


def write_csv(farm_id: str, rows: list[dict]) -> Path:
    out = HERE / f"{farm_id}-roster.csv"
    cols = [
        "name", "sire", "dam", "damsire",
        "fee_usd", "fee_terms", "fee_qualifier",
        "year_of_birth", "color", "entered_stud_year",
    ]
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})
    return out


def main():
    ap = argparse.ArgumentParser(description="Scrape a farm's stallion roster")
    ap.add_argument("--farm", help="Farm id to scrape (e.g. pinoak, millridge)")
    ap.add_argument("--all", action="store_true", help="Scrape every enabled farm in the config")
    ap.add_argument("--dry-run", action="store_true", help="Print rows but don't write CSV")
    ap.add_argument("--inspect", action="store_true",
                    help="DOM-diagnostic mode: print page title + most-common repeating tags + sample text. "
                         "Use this to figure out which selector matches the stallion-card pattern.")
    args = ap.parse_args()

    config = load_config()
    farms_cfg = config.get("farms") or {}

    if args.all:
        targets = [(fid, fcfg) for fid, fcfg in farms_cfg.items() if fcfg.get("enabled", True)]
    elif args.farm:
        if args.farm not in farms_cfg:
            raise SystemExit(f"Farm '{args.farm}' not in config. Known: {sorted(farms_cfg)}")
        targets = [(args.farm, farms_cfg[args.farm])]
    else:
        raise SystemExit("Specify --farm <id> or --all")

    # Lazy import — Playwright isn't always installed locally
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise SystemExit(
            "Playwright not installed. In the worker container it's already there.\n"
            "Locally: pip install playwright && python -m playwright install chromium"
        )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()

        for i, (farm_id, farm_cfg) in enumerate(targets):
            if i > 0:
                time.sleep(2)   # rate-limit between farms
            print(f"\n[{farm_id}] {farm_cfg.get('name', '')}")
            if args.inspect:
                # Diagnostic: load the page and dump structural hints so the
                # user can identify the right stallion_card selector.
                url = farm_cfg["url"]
                print(f"  fetching {url}")
                page.goto(url, wait_until="networkidle", timeout=30_000)
                page.wait_for_timeout(1_500)
                title = page.title()
                print(f"  page title: {title}")
                # Count common repeating tags that often hold stallion cards
                for sel in ["article", ".stallion", ".stallion-card", ".stallions__item",
                            ".horse", ".post", ".card", "[class*='stallion']",
                            "main h2", "main h3", ".elementor-widget-wrap article"]:
                    try:
                        n = len(page.query_selector_all(sel))
                    except Exception:
                        n = 0
                    if n > 0:
                        print(f"    {n:>3d} × {sel}")
                # Print first H2 / H3 inner text — useful when the site lists
                # stallions with simple heading-only structure.
                for sel in ["main h2", "main h3", ".content h2"]:
                    els = page.query_selector_all(sel)
                    if els:
                        print(f"  sample {sel} headings:")
                        for e in els[:6]:
                            try:
                                print(f"    - {e.inner_text().strip()[:80]}")
                            except Exception:
                                pass
                        break
                continue
            try:
                rows = scrape_one(page, farm_id, farm_cfg)
            except Exception as e:
                print(f"  ERROR scraping {farm_id}: {e}")
                continue
            if not rows:
                print(f"  no rows extracted — check selectors in config")
                continue
            if args.dry_run:
                for r in rows:
                    print(f"  {r['name']:30s}  by {r['sire'] or '?'}  fee=${r['fee_usd'] or r['fee_qualifier'] or '?'}")
            else:
                out = write_csv(farm_id, rows)
                print(f"  wrote {out.name} with {len(rows)} stallions")

        browser.close()

    print("\nDone. Next: verify CSVs, then flip enabled=true in farms-extra.json.")


if __name__ == "__main__":
    main()
