"""Scrape The Jockey Club's annual Report of Mares Bred (RMB) from BH.

The RMB is the canonical public source for "how many mares were bred to
each stallion in year Y" — a real demand signal that complements the
sale-side data we already ingest.

URL pattern: bloodhorse.com/horse-racing/thoroughbred-breeding/mares-bred/{year}

Output: worker/mares-bred-{year}.json keyed by stallion name → {state, n_mares, year}.
Also a normalized-name index so casing/quotes don't trip lookups in the
scoring code.

Same Playwright + Imperva-handling pattern as enrich_from_bloodhorse.py
and scrape_stakes.py.

Usage:
  python3 scrape_mares_bred.py --year 2024
  python3 scrape_mares_bred.py --year 2024 --debug-dump ./html-dumps
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
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
TIMEOUT = 30_000

log = logging.getLogger("mares-bred")


# Row pattern: <tr> <td><strong>{stallion}</strong></td> <td>{n}</td> <td>{state}</td> </tr>
ROW_RE = re.compile(
    r'<tr[^>]*>\s*'
    r'<td[^>]*>\s*<strong[^>]*>(?P<name>[^<]+?)</strong>\s*</td>\s*'
    r'<td[^>]*>\s*(?P<n>\d+)\s*</td>\s*'
    r'<td[^>]*>\s*(?P<state>[^<]*?)\s*</td>\s*'
    r'</tr>',
    flags=re.IGNORECASE | re.DOTALL,
)


def normalize_name_for_lookup(name: str) -> str:
    """Same canonicalization as the rest of the scoring code: straighten
    quotes, per-word title-case so 'Bolt d'Oro'/'Bolt D’Oro' match."""
    if not name:
        return name
    name = name.replace("’", "'").replace("‘", "'")
    words = []
    for word in name.split():
        if word.startswith("(") and word.endswith(")"):
            inner = word[1:-1]
            words.append("(" + (inner[0].upper() + inner[1:].lower() if inner else "") + ")")
        else:
            words.append(word[0].upper() + word[1:].lower() if word else word)
    return " ".join(words)


def main():
    ap = argparse.ArgumentParser(description="Scrape annual Report of Mares Bred from BH")
    ap.add_argument("--year", type=int, required=True, help="Breeding year (e.g. 2024)")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--debug-dump", metavar="DIR")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    url = f"https://www.bloodhorse.com/horse-racing/thoroughbred-breeding/mares-bred/{args.year}"
    out_path = HERE / f"mares-bred-{args.year}.json"

    _require_playwright()
    pw = sync_playwright().start()
    try:
        browser = pw.chromium.launch(
            headless=not args.headful,
            args=["--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(user_agent=USER_AGENT, viewport={"width": 1280, "height": 1400})
        page = ctx.new_page()
        log.info(f"Loading {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT)
        try:
            page.wait_for_load_state("networkidle", timeout=12_000)
        except Exception:
            pass
        for _ in range(3):
            try:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(700)
            except Exception:
                break
        try:
            page.wait_for_load_state("networkidle", timeout=5_000)
        except Exception:
            pass

        html = page.content()
        log.info(f"got {len(html)} bytes")
        if args.debug_dump:
            d = Path(args.debug_dump)
            d.mkdir(parents=True, exist_ok=True)
            (d / f"mares-bred-{args.year}.html").write_text(html)

        rows = list(ROW_RE.finditer(html))
        log.info(f"parsed {len(rows)} stallion rows")
        if len(rows) < 50:
            log.warning("Suspiciously few rows — selectors may need tuning. Aborting write.")
            return 2

        by_name: dict[str, dict] = {}
        for m in rows:
            name = m.group("name").strip()
            n = int(m.group("n"))
            state = m.group("state").strip()
            rec = {"sire": name, "year": args.year, "n_mares": n, "state": state}
            # Index under both raw and normalized names so name-variant lookups hit
            by_name[name] = rec
            by_name[normalize_name_for_lookup(name)] = rec

        out = {
            "generated_at":  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source_url":    url,
            "year":          args.year,
            "n_stallions":   len(rows),
            "by_sire":       by_name,
        }
        out_path.write_text(json.dumps(out, indent=2))
        log.info(f"wrote {out_path} with {len(by_name)} entries (incl. normalized aliases)")

        # Sanity: print top 5 by mare count
        sorted_rows = sorted(
            ((m.group("name"), int(m.group("n")), m.group("state").strip())
             for m in rows),
            key=lambda x: -x[1]
        )
        log.info("Top 5 by mare count:")
        for name, n, state in sorted_rows[:5]:
            log.info(f"  {n:>4d}  {state:>3s}  {name}")
    finally:
        try: browser.close()
        except Exception: pass
        try: pw.stop()
        except Exception: pass


if __name__ == "__main__":
    sys.exit(main() or 0)
