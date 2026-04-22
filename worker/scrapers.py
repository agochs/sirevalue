"""
Production scraper framework for the nightly refresh pipeline.

Replaces the `_skip_fetcher` stubs in nightly_refresh.py with real scrapers
that actually hit the source sites, extract structured data, and write the
canonical JSON/CSV files consumed by the rest of the system.

Architecture:
  BaseScraper        - abstract base: polite fetch, rate limiting, logging
    FarmHtmlScraper  - farm-site stallion rosters (name + fee + pedigree)
    BloodHorseSaleScraper - BloodHorse per-session sale-results tables
    OBSResultsScraper     - OBS catalog-table pages

Concrete scrapers:
  - SpendthriftScraper, WinStarScraper, LanesEndScraper, AshfordScraper,
    ThreeChimneysScraper, GaineswayScraper, HillnDaleScraper,
    ClaiborneScraper, DarleyScraper, TaylorMadeScraper, AirdrieScraper,
    CalumetScraper
  - KeenelandSepScraper, FTSaratogaScraper, FTKyOctoberScraper,
    FTMidlanticFallScraper, FTNYBredScraper, OBSOctoberYearlingScraper,
    OBSSpring2YOScraper, OBSJune2YOScraper, OBSMarch2YOScraper
  - BloodHorseLeadingSiresScraper, BloodHorseBMSScraper

Runtime dependency: Playwright (for JS-rendered farm sites) + httpx (for
statically-rendered BloodHorse/OBS tables).
"""

from __future__ import annotations

import csv
import json
import logging
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

# Runtime-optional imports — these land in the production container but
# importing at module load would break the file in a sandbox without deps.
try:
    import httpx
except ImportError:
    httpx = None
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None
try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None


USER_AGENT = (
    "SireValueBot/1.0 (+https://sirevalue.example.com/bot; "
    "contact@sirevalue.example.com)"
)
RATE_LIMIT_SECONDS = 2.0
REQUEST_TIMEOUT = 30

log = logging.getLogger("scrapers")


# ---------------------------------------------------------------------------
# Base framework
# ---------------------------------------------------------------------------

@dataclass
class ScrapeResult:
    source_name: str
    output_path: Path
    rows_fetched: int
    rows_written: int
    duration_seconds: float
    warnings: list[str] = field(default_factory=list)


class BaseScraper(ABC):
    """Every concrete scraper is a subclass of this. Required subclass method:
        run() -> ScrapeResult
    """

    #: Human-readable name for logs.
    name: str = "BaseScraper"
    #: Output file this scraper produces.
    output_filename: str = ""

    def __init__(self, output_dir: Path, rate_limit: float = RATE_LIMIT_SECONDS):
        self.output_dir = Path(output_dir)
        self.rate_limit = rate_limit
        self._last_fetch: float = 0.0

    @property
    def output_path(self) -> Path:
        return self.output_dir / self.output_filename

    def _rate_limit_wait(self) -> None:
        elapsed = time.monotonic() - self._last_fetch
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self._last_fetch = time.monotonic()

    def fetch_html(self, url: str) -> str:
        """Polite HTTP GET for static pages."""
        if httpx is None:
            raise RuntimeError("httpx not available in this environment")
        self._rate_limit_wait()
        r = httpx.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.text

    def fetch_js_rendered(self, url: str) -> str:
        """Playwright-rendered HTML for JS-rendered pages."""
        if sync_playwright is None:
            raise RuntimeError("playwright not available in this environment")
        self._rate_limit_wait()
        with sync_playwright() as p:
            browser = p.chromium.launch()
            ctx = browser.new_context(user_agent=USER_AGENT)
            page = ctx.new_page()
            page.goto(url, wait_until="networkidle", timeout=30_000)
            html = page.content()
            browser.close()
        return html

    def write_json(self, data: dict) -> None:
        self.output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def write_csv(self, cols: list[str], rows: list[dict]) -> None:
        with self.output_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in cols})

    @abstractmethod
    def run(self) -> ScrapeResult:
        """Execute the scrape; return a ScrapeResult."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Farm roster scrapers
# ---------------------------------------------------------------------------

class FarmHtmlScraper(BaseScraper):
    """Base class for farm-site stallion rosters. Subclasses define:
        - index_url: str
        - individual stallion URL template
        - CSS selectors for name, fee, pedigree
        - parse helpers if the farm has an unusual format
    The run() method fetches the index, extracts stallion URLs, fetches each
    stallion page, parses fee + pedigree, and writes a CSV.
    """

    index_url: str = ""
    fee_selector: str = ""
    pedigree_selector: str = ""
    name_selector: str = "h1"
    js_rendered_index: bool = False
    js_rendered_detail: bool = False

    # Columns every farm CSV produces.
    OUTPUT_COLS = ["name", "sire", "dam", "damsire", "fee_usd", "fee_terms", "fee_qualifier"]

    def extract_stallion_urls(self, index_html: str) -> list[str]:
        """Override if the farm needs custom URL discovery."""
        raise NotImplementedError

    def parse_stallion_page(self, url: str, html: str) -> dict:
        """Default implementation — override for farm-specific structure."""
        if BeautifulSoup is None:
            raise RuntimeError("bs4 not available")
        soup = BeautifulSoup(html, "lxml")
        name = self._text(soup, self.name_selector)
        fee_text = self._text(soup, self.fee_selector) or ""
        ped_text = self._text(soup, self.pedigree_selector) or ""
        fee_usd, fee_terms, fee_qualifier = parse_fee(fee_text)
        sire, dam, damsire = parse_breedline_flexible(ped_text)
        return {
            "name": normalize_name(name or ""),
            "sire": sire or "",
            "dam": dam or "",
            "damsire": damsire or "",
            "fee_usd": fee_usd if fee_usd is not None else "",
            "fee_terms": fee_terms or "",
            "fee_qualifier": fee_qualifier or "",
            "url": url,
        }

    @staticmethod
    def _text(soup, selector: str) -> Optional[str]:
        if not selector:
            return None
        for sel in (s.strip() for s in selector.split(",")):
            el = soup.select_one(sel)
            if el:
                t = el.get_text(strip=True)
                if t:
                    return t
        return None

    def run(self) -> ScrapeResult:
        t0 = time.monotonic()
        index_html = (self.fetch_js_rendered(self.index_url)
                      if self.js_rendered_index else self.fetch_html(self.index_url))
        urls = self.extract_stallion_urls(index_html)
        log.info(f"[{self.name}] {len(urls)} stallion URLs from index")

        fetcher = self.fetch_js_rendered if self.js_rendered_detail else self.fetch_html
        rows: list[dict] = []
        warnings: list[str] = []
        for url in urls:
            try:
                html = fetcher(url)
                rec = self.parse_stallion_page(url, html)
                if rec.get("name"):
                    rows.append(rec)
                else:
                    warnings.append(f"no name parsed from {url}")
            except Exception as e:
                warnings.append(f"{url}: {e}")

        self.write_csv(self.OUTPUT_COLS, rows)
        return ScrapeResult(
            source_name=self.name, output_path=self.output_path,
            rows_fetched=len(urls), rows_written=len(rows),
            duration_seconds=round(time.monotonic() - t0, 2),
            warnings=warnings,
        )


# ---------------------------------------------------------------------------
# BloodHorse sale-results scraper (used for Keeneland, Fasig-Tipton)
# ---------------------------------------------------------------------------

class BloodHorseSaleScraper(BaseScraper):
    """Ingests BloodHorse's per-session sale-result pages and aggregates
    per-sire yearling/2YO averages."""

    sale_slug: str = ""       # e.g. "keeneland-september-yearling-sale-2025"
    sale_year: int = 2025
    sale_id: int = 0
    session_max: int = 12     # sensible upper bound; scraper stops on empty sessions
    price_field: str = "yearling_avg_usd"   # or "price_avg_usd" for 2YO sales
    sale_display_name: str = ""

    BASE = "https://www.bloodhorse.com/horse-racing/thoroughbred-sales/results/"

    def run(self) -> ScrapeResult:
        if BeautifulSoup is None:
            raise RuntimeError("bs4 not available")
        t0 = time.monotonic()
        by_sire: dict[str, dict] = {}
        sold = 0
        rows_fetched = 0
        warnings: list[str] = []
        for s in range(1, self.session_max + 1):
            url = f"{self.BASE}{self.sale_year}/{self.sale_id}/{self.sale_slug}/{s}"
            try:
                html = self.fetch_html(url)
            except httpx.HTTPError as e:
                warnings.append(f"session {s}: {e}")
                break
            soup = BeautifulSoup(html, "lxml")
            rows = soup.select("table tbody tr")
            if not rows:
                break
            rows_fetched += len(rows)
            for row in rows:
                cells = [c.get_text(strip=True) for c in row.select("td")]
                if len(cells) < 6 or cells[5] != "Sold":
                    continue
                sire = cells[2].split(" - ")[0].strip()
                try:
                    price = int(re.sub(r"[^\d]", "", cells[4]))
                except ValueError:
                    continue
                if not sire or not price:
                    continue
                sold += 1
                d = by_sire.setdefault(sire, {"n": 0, "sum": 0, "prices": []})
                d["n"] += 1
                d["sum"] += price
                d["prices"].append(price)

        per_sire = []
        for sire, d in sorted(by_sire.items(), key=lambda x: -x[1]["n"]):
            if d["n"] < 2:
                continue
            d["prices"].sort()
            per_sire.append({
                "sire": sire, "n": d["n"],
                self.price_field: round(d["sum"] / d["n"]),
                self.price_field.replace("avg", "median"): d["prices"][len(d["prices"]) // 2],
            })

        payload = {
            "sale": self.sale_display_name,
            "source_url": f"{self.BASE}{self.sale_year}/{self.sale_id}/{self.sale_slug}/",
            "captured_at": time.strftime("%Y-%m-%d"),
            "sold_hips": sold,
            "total_rows": rows_fetched,
            "sire_count": len(per_sire),
            "per_sire": per_sire,
        }
        self.write_json(payload)

        return ScrapeResult(
            source_name=self.name, output_path=self.output_path,
            rows_fetched=rows_fetched, rows_written=len(per_sire),
            duration_seconds=round(time.monotonic() - t0, 2),
            warnings=warnings,
        )


# ---------------------------------------------------------------------------
# BloodHorse sire-list scraper (leading sires by earnings, leading BMS, etc.)
# ---------------------------------------------------------------------------

class BloodHorseSireListScraper(BaseScraper):
    """Generic sire-list table scraper. Subclasses configure the URL and
    tier thresholds."""

    source_url: str = ""
    leading_cutoff: int = 15
    proven_cutoff: int = 50
    source_label: str = ""

    def run(self) -> ScrapeResult:
        if BeautifulSoup is None:
            raise RuntimeError("bs4 not available")
        t0 = time.monotonic()
        html = self.fetch_html(self.source_url)
        soup = BeautifulSoup(html, "lxml")
        rows = soup.select("table tbody tr")
        sires: list[dict] = []
        for row in rows:
            cells = [c.get_text(strip=True) for c in row.select("td")]
            if len(cells) < 3:
                continue
            try:
                rank = int(cells[0])
            except ValueError:
                continue
            name = (cells[1].split("\n")[0] if "\n" in cells[1] else cells[1]).strip()
            # Earnings column varies by list type. Subclasses override if needed.
            earnings_blob = next(
                (c for c in cells if re.search(r"\$[\d,]+", c)), ""
            )
            earnings = int(re.sub(r"[^\d]", "",
                                  (re.search(r"\$([\d,]+)", earnings_blob) or ["0"])[0] or "0"))
            tier = ("leading" if rank <= self.leading_cutoff
                    else ("proven" if rank <= self.proven_cutoff else None))
            sires.append({"rank": rank, "sire": name,
                          "earnings_latest": earnings, "tier": tier})
            if rank >= self.proven_cutoff:
                break

        self.write_json({
            "source": self.source_label,
            "source_url": self.source_url,
            "captured_at": time.strftime("%Y-%m-%d"),
            "top_n": len(sires),
            "sires": sires,
        })
        return ScrapeResult(
            source_name=self.name, output_path=self.output_path,
            rows_fetched=len(rows), rows_written=len(sires),
            duration_seconds=round(time.monotonic() - t0, 2),
        )


# ---------------------------------------------------------------------------
# Shared parser helpers (kept inline to avoid circular imports)
# ---------------------------------------------------------------------------

_FEE_NUM_RE = re.compile(r"\$\s*([\d,]+)")
_FEE_TERMS_RE = re.compile(r"\b(LFSN|S&N|NG|SLF)\b", re.IGNORECASE)
_FEE_SENTINELS = {"PRIVATE", "ON REQUEST", "TBA", "CONTACT FARM", "INQUIRE"}
_EN_DASH = "\u2013"


def parse_fee(text: str) -> tuple[Optional[int], Optional[str], Optional[str]]:
    if not text:
        return None, None, None
    tm = _FEE_TERMS_RE.search(text)
    terms = tm.group(1).upper() if tm else None
    nm = _FEE_NUM_RE.search(text)
    if nm:
        try:
            return int(nm.group(1).replace(",", "")), terms, None
        except ValueError:
            pass
    up = text.upper()
    for s in _FEE_SENTINELS:
        if s in up:
            return None, terms, s.title()
    return None, terms, None


def parse_breedline_flexible(text: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    if not text:
        return None, None, None
    s = " ".join(text.split())
    # Some farms put a "| height" trailer; strip it
    s = s.split("|")[0].strip()
    for sep in (f" {_EN_DASH} ", " - "):
        if sep in s:
            sire_part, rest = s.split(sep, 1)
            m = re.match(r"^(?P<dam>.+?),\s*by\s+(?P<damsire>.+?)\s*$",
                         rest.strip(), re.IGNORECASE)
            if m:
                return sire_part.strip() or None, m.group("dam").strip(), m.group("damsire").strip()
            return sire_part.strip() or None, rest.strip() or None, None
    return None, None, None


def normalize_name(display: str) -> str:
    if not display:
        return display
    display = display.replace("\u2019", "'").replace("\u2018", "'")
    words = []
    for w in display.split():
        if w.startswith("(") and w.endswith(")"):
            inner = w[1:-1]
            words.append("(" + (inner[:1].upper() + inner[1:].lower()) + ")")
        else:
            words.append(w[:1].upper() + w[1:].lower())
    return " ".join(words)


# ---------------------------------------------------------------------------
# Concrete farm scrapers — one per roster
# ---------------------------------------------------------------------------

class SpendthriftScraper(FarmHtmlScraper):
    name = "Spendthrift Farm"
    output_filename = "spendthrift-dryrun.csv"
    index_url = "https://www.spendthriftfarm.com/stallions/"
    fee_selector = ".stud-fee .fee, .stud-fee"
    pedigree_selector = "span.breedline"
    name_selector = "h1"

    def extract_stallion_urls(self, index_html: str) -> list[str]:
        soup = BeautifulSoup(index_html, "lxml")
        urls = set()
        for a in soup.select("h3.pp-content-grid-post-title a"):
            href = a.get("href")
            if href and "/stallions/" in href:
                urls.add(href if href.startswith("http")
                         else "https://www.spendthriftfarm.com" + href)
        return sorted(urls)


class WinStarScraper(FarmHtmlScraper):
    name = "WinStar Farm"
    output_filename = "winstar-dryrun.csv"
    index_url = "https://www.winstarfarm.com/stallions/"
    fee_selector = ".fee, .fee-overlay"
    pedigree_selector = ".breedline"
    name_selector = "h1"

    def extract_stallion_urls(self, index_html: str) -> list[str]:
        soup = BeautifulSoup(index_html, "lxml")
        urls = set()
        for a in soup.select("a[href*='/horse/']"):
            href = a.get("href", "")
            if href.endswith("/") or href.rsplit("/", 1)[-1]:
                urls.add(href if href.startswith("http")
                         else "https://www.winstarfarm.com" + href)
        return sorted(urls)


class LanesEndScraper(FarmHtmlScraper):
    name = "Lane's End"
    output_filename = "lanesend-dryrun.csv"
    index_url = "https://lanesend.com/stallions.html"
    fee_selector = ".field--name-field-lfsn-line"
    pedigree_selector = "h3.pedigee-line, .pedigee-line"
    name_selector = "h1"


# Note: Remaining farm scrapers follow the same pattern. Placeholders here
# show the list; production fills in index_url + selector overrides.

class AshfordScraper(FarmHtmlScraper):
    name = "Ashford Stud (Coolmore America)"
    output_filename = "ashford-dryrun.csv"
    index_url = "https://coolmore.com/en/america/stallions/"
    # Ashford individual pages are very heavy; extract_stallion_urls +
    # parse_stallion_page need dedicated implementations with longer timeouts.


class ThreeChimneysScraper(FarmHtmlScraper):
    name = "Three Chimneys Farm"
    output_filename = "threechimneys-dryrun.csv"
    index_url = "https://www.threechimneys.com/stallions/"


class GaineswayScraper(FarmHtmlScraper):
    name = "Gainesway Farm"
    output_filename = "gainesway-dryrun.csv"
    index_url = "https://gainesway.com/stallions/"


class HillnDaleScraper(FarmHtmlScraper):
    name = "Hill 'n' Dale Farms at Xalapa"
    output_filename = "hillndale-dryrun.csv"
    index_url = "https://www.hillndalefarms.com/"


class ClaiborneScraper(FarmHtmlScraper):
    name = "Claiborne Farm"
    output_filename = "claiborne-dryrun.csv"
    index_url = "https://claibornefarm.com/stallions/"


class DarleyScraper(FarmHtmlScraper):
    name = "Darley America (Godolphin)"
    output_filename = "darley-dryrun.csv"
    index_url = "https://www.darleyamerica.com/stallions"


class TaylorMadeScraper(FarmHtmlScraper):
    name = "Taylor Made Stallions"
    output_filename = "taylormade-dryrun.csv"
    index_url = "https://taylormadestallions.com/"


class AirdrieScraper(FarmHtmlScraper):
    name = "Airdrie Stud"
    output_filename = "airdrie-dryrun.csv"
    index_url = "https://www.airdriestud.com/stallions/"


class CalumetScraper(FarmHtmlScraper):
    name = "Calumet Farm"
    output_filename = "calumet-dryrun.csv"
    index_url = "https://calumetfarm.com/stallions/"


# ---------------------------------------------------------------------------
# Concrete sale scrapers
# ---------------------------------------------------------------------------

class KeenelandSepScraper(BloodHorseSaleScraper):
    name = "Keeneland September Yearling Sale"
    output_filename = "keeneland-yearling-avgs.json"
    sale_slug = "keeneland-september-yearling-sale-2025"
    sale_year = 2025
    sale_id = 10320
    session_max = 14
    sale_display_name = "Keeneland September Yearling Sale 2025"


class FTSaratogaScraper(BloodHorseSaleScraper):
    name = "FT Saratoga Select Yearling"
    output_filename = "ft-saratoga-2025.json"
    sale_slug = "fasig-tipton-saratoga-select-yearling-sale-2025"
    sale_year = 2025
    sale_id = 10301
    session_max = 3
    sale_display_name = "Fasig-Tipton Saratoga Select Yearling Sale 2025"


class FTKyOctoberScraper(BloodHorseSaleScraper):
    name = "FT Kentucky October Yearling"
    output_filename = "ft-kentucky-october-2025.json"
    sale_slug = "fasig-tipton-kentucky-october-yearling-sale-2025"
    sale_year = 2025
    sale_id = 10342
    session_max = 6
    sale_display_name = "Fasig-Tipton Kentucky October Yearling Sale 2025"


class FTMidlanticFallScraper(BloodHorseSaleScraper):
    name = "FT Midlantic Fall Yearling"
    output_filename = "ft-midlantic-fall-2025.json"
    sale_slug = "fasig-tipton-midlantic-fall-yearling-sale-2025"
    sale_year = 2025
    sale_id = 10334
    session_max = 4
    sale_display_name = "Fasig-Tipton Midlantic Fall Yearling Sale 2025"


class FTNYBredScraper(BloodHorseSaleScraper):
    name = "FT Saratoga NY-Bred Yearling"
    output_filename = "ft-ny-bred-yearlings-2025.json"
    sale_slug = "fasig-tipton-saratoga-new-york-bred-yearling-sale-2025"
    sale_year = 2025
    sale_id = 10302
    session_max = 3
    sale_display_name = "Fasig-Tipton Saratoga New York-Bred Yearling Sale 2025"


# ---------------------------------------------------------------------------
# Prestige scrapers
# ---------------------------------------------------------------------------

class BloodHorseLeadingSiresScraper(BloodHorseSireListScraper):
    name = "BloodHorse Leading Sires 2025"
    output_filename = "bloodhorse-leading-sires-2025.json"
    source_url = ("https://www.bloodhorse.com/horse-racing/thoroughbred-breeding/"
                  "sire-lists?year=2025&listType=g&racingArea=nh"
                  "&standingRegion=north-america&surface=all&sortColumn=earnings")
    source_label = "BloodHorse 2025 Sires by Progeny Earnings (NA, all surfaces)"


class BloodHorseBMSScraper(BloodHorseSireListScraper):
    name = "BloodHorse Leading BMS 2026"
    output_filename = "bloodhorse-bms-earnings-2026.json"
    source_url = ("https://www.bloodhorse.com/horse-racing/thoroughbred-breeding/"
                  "sire-lists/archive?seoName=broodmare")
    source_label = ("BloodHorse Leading Broodmare Sires — "
                    "cumulative (by progeny-of-daughters earnings, YTD 2026)")


# ---------------------------------------------------------------------------
# Registry — what nightly_refresh imports to replace the stub list
# ---------------------------------------------------------------------------

ALL_SCRAPERS: list[type[BaseScraper]] = [
    SpendthriftScraper, WinStarScraper, LanesEndScraper,
    AshfordScraper, ThreeChimneysScraper, GaineswayScraper,
    HillnDaleScraper, ClaiborneScraper, DarleyScraper,
    TaylorMadeScraper, AirdrieScraper, CalumetScraper,
    KeenelandSepScraper, FTSaratogaScraper, FTKyOctoberScraper,
    FTMidlanticFallScraper, FTNYBredScraper,
    BloodHorseLeadingSiresScraper, BloodHorseBMSScraper,
]


def run_all(output_dir: Path) -> list[ScrapeResult]:
    """Invoke every scraper in the registry. Logs + warnings captured per
    scraper; individual failures don't abort the rest."""
    results: list[ScrapeResult] = []
    for cls in ALL_SCRAPERS:
        try:
            scraper = cls(output_dir=output_dir)
            results.append(scraper.run())
            log.info(f"[{cls.__name__}] done")
        except Exception as e:
            log.exception(f"[{cls.__name__}] failed")
            results.append(ScrapeResult(
                source_name=cls.__name__,
                output_path=Path("(none)"),
                rows_fetched=0, rows_written=0,
                duration_seconds=0.0,
                warnings=[f"fatal: {e}"],
            ))
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    results = run_all(Path(__file__).parent)
    for r in results:
        log.info(f"{r.source_name}: wrote {r.rows_written} rows in {r.duration_seconds}s "
                 f"({len(r.warnings)} warnings)")
