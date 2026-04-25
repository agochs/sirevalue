"""Unified BH sale scraper — handles both CATALOGS (pre-sale) and RESULTS
(post-sale) from the same per-session HTML pages.

Produces two outputs:
  upcoming-sales.json — hips from catalog pages for upcoming/active sales,
    keyed by sire, with sold_price_usd=null (or set once the hip sells live)
  recent-sale-results.json — hips from completed sales, same shape, with price

Accuracy rules:
  * Emit hips only when the sire name matches a roster stallion exactly
    (case-insensitive, country-suffix-aware). No fuzzy matching.
  * Price is recorded as an integer when the row is "Sold", None otherwise.
  * Rows missing required fields (no sire, no hip, no horse) are skipped.

Usage:
  pip3 install httpx beautifulsoup4 lxml
  cd ~/sirevalue/worker

  # Fetch all sales configured in SALES below
  python3 fetch_sales.py

  # Narrow to a single sale for debugging
  python3 fetch_sales.py --only "Keeneland September 2026" --verbose

Configuration: edit SALES at the top of the file. Each entry:
  {
    "name": "Keeneland September Yearling Sale 2026",
    "url":  "https://www.bloodhorse.com/.../results/2026/{sale_id}/.../",
    "kind": "catalog" | "results" | "auto"   # auto picks by sale date vs today
  }
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
UPCOMING_JSON = HERE / "upcoming-sales.json"
# Per-year results files: recent-sale-results-2024.json, etc. Keeps each
# payload small enough to fetch on demand and lets the UI pull only the
# years it cares about.
RESULTS_INDEX_JSON = HERE / "recent-sale-results-index.json"

# Regex to extract year from a sale URL (used when splitting output)
_SALE_YEAR_RE = re.compile(r"/results/(\d{4})/", re.I)


def _year_from_url(url: str) -> int | None:
    m = _SALE_YEAR_RE.search(url or "")
    return int(m.group(1)) if m else None

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
RATE_LIMIT_SECONDS = 1.0
TIMEOUT = 30
SESSION_MAX = 15          # sensible upper bound; scraper stops on empty sessions

log = logging.getLogger("sales")


# ---------------------------------------------------------------------------
# Sale configuration — EDIT THIS. Add each URL you want scraped. Kind can be:
#   "catalog"  — pre-sale, prices not yet set for unsold rows
#   "results"  — post-sale, every sold row has a price
#   "auto"     — infer from whether price column has content
# ---------------------------------------------------------------------------

SALES_CONFIG_JSON = HERE / "sales-config.json"

# Patterns identifying low-signal sales we skip by default. These are the
# tiny digital monthly auctions, single-farm dispersals, and online flash
# sales that contribute almost nothing to ranking or pinhook analysis. Keep
# them in sales-config.json for completeness, but skip at scrape time.
SKIP_PATTERNS = [
    re.compile(r"\bdigital[ -]?(?:flash|express)", re.I),
    re.compile(r"\bdispersal\b", re.I),
    # Generic monthly online "Digital Sale" (FT runs ~one a month). Distinct
    # from named premium sales like "March Digital Selected" or "Saratoga
    # Digital" which we keep when explicitly opted-in.
    re.compile(r"^.*\bdigital sale\b", re.I),
    re.compile(r"\bdigital selected sale\b", re.I),
    re.compile(r"\bonline sale\b", re.I),
    re.compile(r"\bflash sale\b", re.I),
]


def _is_minor_sale(name: str) -> bool:
    return any(p.search(name) for p in SKIP_PATTERNS)


def _load_sales_from_config(skip_minor: bool = True) -> list[dict]:
    """Read sales-config.json next to this file. Falls back to an empty list
    if absent. When skip_minor=True (default), filter out low-signal sales."""
    if not SALES_CONFIG_JSON.exists():
        return []
    try:
        cfg = json.loads(SALES_CONFIG_JSON.read_text())
        all_sales = [s for s in cfg.get("sales", []) if s.get("url")]
    except Exception as e:
        log.warning(f"failed to read {SALES_CONFIG_JSON.name}: {e}")
        return []
    if not skip_minor:
        return all_sales
    kept, skipped = [], []
    for s in all_sales:
        if _is_minor_sale(s.get("name", "")):
            skipped.append(s["name"])
        else:
            kept.append(s)
    if skipped:
        log.info(f"skipping {len(skipped)} low-signal sale(s) (digital flash / dispersal / online); "
                 f"use --include-minor to keep them")
    return kept

SALES: list[dict] = _load_sales_from_config()


# ---------------------------------------------------------------------------
# Roster / name matching (reuse patterns from enrich_from_bloodhorse.py)
# ---------------------------------------------------------------------------

def normalize_name(name: str) -> str:
    s = (name or "").lower()
    s = re.sub(r"\s*\([^)]+\)\s*$", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def load_roster_sires() -> dict[str, str]:
    """Returns {normalized_name: canonical_name} for every stallion in the roster."""
    idx: dict[str, str] = {}
    with ROSTER_CSV.open() as f:
        for r in csv.DictReader(f):
            n = (r.get("name") or "").strip()
            if not n:
                continue
            norm = normalize_name(n)
            if norm and norm not in idx:
                idx[norm] = n
    return idx


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

BH_HOME = "https://www.bloodhorse.com/horse-racing/thoroughbred-sales/"

# Playwright is only needed when BH's Imperva gets aggressive — which it
# regularly does after a few httpx requests in quick succession. Import
# lazily so the pure parser can be unit-tested without it.
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


class BHClient:
    """Playwright-driven. Proven to get past Imperva's bot detection (same
    pattern we use in enrich_from_bloodhorse.py). Slower than httpx (~3-4s
    per page vs ~1s) but reliable."""

    def __init__(self, headful: bool = False):
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
        self._last_at = 0.0
        self._warmed_up = False

    def _throttle(self):
        elapsed = time.time() - self._last_at
        if elapsed < RATE_LIMIT_SECONDS:
            time.sleep(RATE_LIMIT_SECONDS - elapsed)
        self._last_at = time.time()

    def warm_up(self):
        if self._warmed_up:
            return
        log.debug(f"  warm-up: goto {BH_HOME}")
        try:
            self._page.goto(BH_HOME, wait_until="domcontentloaded", timeout=30_000)
            try:
                self._page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:
                pass
        except Exception as e:
            log.warning(f"warm-up failed: {e}")
        self._warmed_up = True

    def get(self, url: str) -> Optional[str]:
        self.warm_up()
        self._throttle()
        try:
            resp = self._page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            if resp and resp.status == 404:
                return None
            try:
                self._page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:
                pass
            html = self._page.content()
            log.debug(f"  fetched {url}: {len(html)} bytes")
            return html
        except Exception as e:
            log.debug(f"  fetch failed: {e}")
            return None

    def close(self):
        try:
            self._context.close()
            self._browser.close()
        finally:
            self._pw.stop()


def parse_price(cell: str) -> Optional[int]:
    """Parse a BH price cell. Handles '$150,000', '$1,200,000', 'RNA', '', etc."""
    s = (cell or "").strip()
    if not s or s.upper() in ("RNA", "OUT", "SCRATCHED", "NOT SOLD"):
        return None
    digits = re.sub(r"[^\d]", "", s)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def parse_hip_row(cells: list[str]) -> Optional[dict]:
    """Extract a hip record from a BH sale-results/catalog row.

    Actual BH cell layout (verified from OBS Spring 2026 results):
      [0] 'OBSAPR2026 Hip: 3 Two-year-old'    — sale tag, hip number, age
      [1] 'UNNAMED C 2024' or 'Valentine Eve F 2024' — name / sex / YOB
      [2] 'Charlatan - Pammy Whammy, by War Front'   — sire - dam, by damsire
      [3] 'C: Eaton Sales B: Smith Farm'              — consignor / breeder
      [4] '$100,000' or '($60,000)'                   — $ = sold; ($) = RNA
      [5] 'Sold' | 'RNA' | 'Out'                      — status

    Cells with parentheses around the price indicate RNA (bid-off, not sold);
    we record status='RNA' and sold_price_usd=None.
    """
    if len(cells) < 4:
        return None

    # Hip: look for "Hip: N" inside any early cell
    hip = None
    for c in cells[:2]:
        m = re.search(r"Hip\s*:?\s*(\d{1,4})([A-Z]?)", c)
        if m:
            hip = int(m.group(1))
            break
    if hip is None:
        return None

    # Horse name: cell [1]. UNNAMED gets a clean label.
    horse = cells[1].strip() if len(cells) > 1 else ""

    # Pedigree cell — find the cell with " - " and ", by "
    sire = dam = damsire = None
    for c in cells[1:5]:
        if " - " in c or " \u2013 " in c:
            parts = re.split(r"\s[-\u2013]\s", c, maxsplit=1)
            if len(parts) == 2:
                sire = parts[0].strip()
                rest = parts[1]
                dm = re.match(r"^([^(,]+?)\s*(?:\([^)]*\))?,?\s*(?:by\s+(.+))?$", rest)
                if dm:
                    dam = (dm.group(1) or "").strip() or None
                    damsire = (dm.group(2) or "").strip() or None
                break

    # Consignor / breeder — cell [3] format "C: ConsignorName B: BreederName"
    consignor = None
    for c in cells[2:5]:
        cm = re.search(r"C\s*:\s*([^|]+?)(?:\s+B\s*:|$)", c)
        if cm:
            consignor = cm.group(1).strip() or None
            break

    # Price — look for dollar cell. Parens => RNA / bid-off, not a real sale.
    price = None
    status = None
    for c in cells:
        if "$" not in c:
            continue
        raw = c.strip()
        if raw.startswith("(") and raw.endswith(")"):
            # "($60,000)" — RNA bid amount; don't record as price
            status = "RNA"
        else:
            price = parse_price(raw)
        break

    # Status cell (last) overrides if present
    for c in cells:
        cu = c.strip().upper()
        if cu in ("SOLD", "RNA", "OUT", "SCRATCHED", "NOT SOLD"):
            status = cu.title()
            break

    if not sire or not horse:
        return None

    return {
        "hip": hip,
        "horse_name": horse,
        "sire": sire,
        "dam": dam,
        "damsire": damsire,
        "consignor": consignor,
        "sold_price_usd": price,
        "status": status,
    }


def scrape_sale(bh: BHClient, sale: dict, sire_idx: dict[str, str],
                dump_sample: bool = False) -> list[dict]:
    """Walk session pages for a sale and extract all hips whose sire matches
    a roster stallion. Returns a list of {sale_name, session, hip, sire, ...}.

    If dump_sample=True, prints the cell contents of the first 5 rows of the
    first session to help diagnose a parser mismatch with BH's actual HTML."""
    name = sale["name"]
    base_url = sale["url"].rstrip("/")
    kind = sale.get("kind", "auto")

    log.info(f"[{name}] scraping ({kind})")
    entries: list[dict] = []
    dumped = False

    for session in range(1, SESSION_MAX + 1):
        session_url = f"{base_url}/{session}"
        html = bh.get(session_url)
        if html is None:
            log.debug(f"  session {session}: not found, stopping")
            break
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table tbody tr")
        if not rows:
            log.debug(f"  session {session}: no rows, stopping")
            break
        log.debug(f"  session {session}: {len(rows)} rows")

        for i, row in enumerate(rows):
            cells = [td.get_text(" ", strip=True) for td in row.select("td")]
            if dump_sample and not dumped and i < 5:
                log.info(f"  SAMPLE row {i} ({len(cells)} cells):")
                for ci, c in enumerate(cells):
                    log.info(f"    [{ci}] {c!r}")
            rec = parse_hip_row(cells)
            if not rec:
                continue
            norm = normalize_name(rec["sire"])
            canonical = sire_idx.get(norm)
            if not canonical:
                continue   # sire isn't in our roster
            rec["sire_canonical"] = canonical
            rec["sale_name"] = name
            rec["sale_url"] = base_url + "/"
            rec["session"] = session
            entries.append(rec)
        if dump_sample:
            dumped = True

    log.info(f"[{name}] {len(entries)} roster-sire hip(s) extracted")
    return entries


# ---------------------------------------------------------------------------
# Aggregation & output
# ---------------------------------------------------------------------------

def group_by_sire(entries: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for e in entries:
        out.setdefault(e["sire_canonical"], []).append({
            "sale_name": e["sale_name"],
            "sale_url":  e["sale_url"],
            "session":   e["session"],
            "hip":       e["hip"],
            "horse_name": e["horse_name"],
            "dam":        e["dam"],
            "damsire":    e["damsire"],
            "consignor":  e.get("consignor"),
            "sold_price_usd": e["sold_price_usd"],
            "status":     e["status"],
        })
    for k in out:
        out[k].sort(key=lambda h: (h["sale_name"], h["hip"]))
    return out


def classify_entries(entries: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split entries into (upcoming, results) based on presence of a price
    on any hip in the sale. Sales with no prices anywhere → upcoming."""
    upcoming_sales = set()
    results_sales = set()
    for e in entries:
        if e["sold_price_usd"] is not None:
            results_sales.add(e["sale_name"])
    upcoming_sales = {e["sale_name"] for e in entries} - results_sales

    upcoming = [e for e in entries if e["sale_name"] in upcoming_sales]
    results = [e for e in entries if e["sale_name"] in results_sales]
    return upcoming, results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch BH sale catalogs/results")
    ap.add_argument("--only", help="Process only this sale name (substring match)")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--dump-sample", action="store_true",
                    help="Print first 5 rows' cell contents per sale — for diagnosing parser mismatches")
    ap.add_argument("--include-minor", action="store_true",
                    help="Include low-signal sales (digital flash, dispersals, monthly online) — skipped by default")
    args = ap.parse_args()

    # Reload SALES with the chosen filter
    global SALES
    SALES = _load_sales_from_config(skip_minor=not args.include_minor)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not SALES:
        log.error(
            "No sales configured. Edit the SALES list at the top of "
            "fetch_sales.py and add catalog/results URLs to scrape."
        )
        return 2

    sire_idx = load_roster_sires()
    log.info(f"roster: {len(sire_idx)} stallions")

    sales = SALES
    if args.only:
        needle = args.only.lower()
        sales = [s for s in SALES if needle in s["name"].lower()]
        if not sales:
            log.error(f"no configured sale matches {args.only!r}")
            return 2

    bh = BHClient()
    all_entries: list[dict] = []
    try:
        for sale in sales:
            entries = scrape_sale(bh, sale, sire_idx, dump_sample=args.dump_sample)
            all_entries.extend(entries)
    finally:
        bh.close()

    upcoming_entries, results_entries = classify_entries(all_entries)

    upcoming_by_sire = group_by_sire(upcoming_entries)
    results_by_sire  = group_by_sire(results_entries)

    def summary(by_sire: dict) -> dict:
        return {
            "stallions_with_entries": len(by_sire),
            "total_hips": sum(len(v) for v in by_sire.values()),
            "sales_covered": sorted(
                {h["sale_name"] for hips in by_sire.values() for h in hips}
            ),
        }

    UPCOMING_JSON.write_text(json.dumps({
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": "BloodHorse sale catalog pages",
        "by_sire": upcoming_by_sire,
        "summary": summary(upcoming_by_sire),
    }, indent=2, ensure_ascii=False))

    log.info(
        f"wrote {UPCOMING_JSON.name}: "
        f"{summary(upcoming_by_sire)['stallions_with_entries']} stallions, "
        f"{summary(upcoming_by_sire)['total_hips']} upcoming hips"
    )

    # Split results by year. Each entry's sale URL contains the year — we
    # bucket per stallion per year so each output file stays bite-sized.
    results_by_year: dict[int, list[dict]] = {}
    for e in results_entries:
        y = _year_from_url(e["sale_url"])
        if y is None:
            continue
        results_by_year.setdefault(y, []).append(e)

    years_written = []
    for year, year_entries in sorted(results_by_year.items()):
        ybs = group_by_sire(year_entries)
        out_path = HERE / f"recent-sale-results-{year}.json"
        out_path.write_text(json.dumps({
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "year": year,
            "source": "BloodHorse sale results pages",
            "by_sire": ybs,
            "summary": summary(ybs),
        }, indent=2, ensure_ascii=False))
        s = summary(ybs)
        log.info(f"wrote {out_path.name}: "
                 f"{s['stallions_with_entries']} stallions, "
                 f"{s['total_hips']} sold hips")
        years_written.append({
            "year": year,
            "file": out_path.name,
            "stallions_with_entries": s["stallions_with_entries"],
            "total_hips": s["total_hips"],
            "sales_covered": s["sales_covered"],
        })

    # Write an index file the UI loads first to know which year-files exist.
    RESULTS_INDEX_JSON.write_text(json.dumps({
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "years": years_written,
    }, indent=2, ensure_ascii=False))
    log.info(f"wrote {RESULTS_INDEX_JSON.name}: {len(years_written)} year(s) indexed")

    return 0


if __name__ == "__main__":
    sys.exit(main())
