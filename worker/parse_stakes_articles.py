"""Parse stakes-result info directly from BH article body text.

Pivot from harvest_from_articles.py (which followed embedded race-page links —
those usually point at upcoming races, not past results). Instead, parse the
article body for the structured info that recap articles consistently expose:
race name, winner, sire, dam, damsire.

How recap articles consistently format the lede
-----------------------------------------------
"Commandment, a son of Into Mischief out of the Orb mare Sippican Harbor,
captured the $1 million Florida Derby (G1)..."

We search for two patterns in the first ~1500 chars of article body:
  1. Pedigree clause: "{winner}, a {color} {sex/age} by {sire} (out of)
     the {damsire} mare {dam}, ..."
  2. Race-and-date clause: "{Race Name} ({grade}) {…} on {date}"

Output: appends to worker/stakes-results.json with the same schema as
scrape_stakes.py output (race, race_grade, track, date, purse_usd, winner,
winner_age, winner_sire, winner_dam, winner_damsire, source_url, note).

Usage:
  python3 parse_stakes_articles.py --listing-url "https://www.bloodhorse.com/horse-racing/articles/section/thoroughbred-racing" --max-articles 30
  python3 parse_stakes_articles.py --articles-file my-articles.txt
  python3 parse_stakes_articles.py --debug-dump ./html-dumps --verbose

When parsing fails for an article, it goes to "unresolved" with the reason
so the user can either fix selectors here or hand-add the entry.
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
STAKES_JSON = HERE / "stakes-results.json"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
TIMEOUT = 30_000
RATE_LIMIT_SECONDS = 1.2

log = logging.getLogger("parse-articles")


# Article URL pattern (absolute or relative href)
ARTICLE_URL_RE = re.compile(
    r'href="(?:https?://(?:www\.)?bloodhorse\.com)?'
    r'(?P<path>/horse-racing/articles/(?P<id>\d+)/(?P<slug>[a-z0-9-]+))"',
    flags=re.IGNORECASE,
)

# Recap-slug filter (same as harvest_from_articles.py)
SLUG_LOOKS_LIKE_RECAP = re.compile(
    r'-(wins?|takes|denies|stuns|captures|claims|romps?-(to)?|'
    r'returns|holds-off|stays-perfect|edges)-',
    flags=re.IGNORECASE,
)

# Article body extraction — strips HTML markup to plain text. We focus on the
# first ~2KB which is where the lede (with structured race info) lives.
TAG_RE        = re.compile(r"<[^>]+>", flags=re.DOTALL)
ENTITY_RE     = re.compile(r"&(amp|nbsp|quot|apos|lt|gt|#\d+);")
ARTICLE_BODY_RE = re.compile(
    r'<(?:div|article|section)[^>]*class="[^"]*(?:article-body|entry-content|content-body|article-content|post-content)[^"]*"[^>]*>'
    r'(.*?)</(?:div|article|section)>',
    flags=re.DOTALL | re.IGNORECASE,
)
H1_RE = re.compile(r"<h1[^>]*>\s*(.*?)\s*</h1>", flags=re.DOTALL | re.IGNORECASE)


def html_to_text(html: str) -> str:
    """Cheap HTML → plain text. Drops tags, decodes a few common entities."""
    if not html:
        return ""
    text = TAG_RE.sub(" ", html)
    text = (text
            .replace("&amp;", "&")
            .replace("&nbsp;", " ")
            .replace("&quot;", '"')
            .replace("&apos;", "'")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("’", "'")
            .replace("—", "-")
            .replace("–", "-"))
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_lede(html: str, max_chars: int = 2500) -> str:
    """Grab the first ~2500 chars of article body, plain text. We try several
    common BH wrapper class names; if none match, fall back to whole-page text."""
    m = ARTICLE_BODY_RE.search(html)
    body = m.group(1) if m else html
    return html_to_text(body)[:max_chars]


# --- Parsers ---------------------------------------------------------------

# Race name + grade in the lede ("Florida Derby (G1)", "Toyota Blue Grass S. (G1)")
RACE_RE = re.compile(
    r"(?P<race>[A-Z][A-Za-z .'\-&]+(?:Derby|Stakes|S\.|Cup|Mile|Sprint|Classic|Oaks|Handicap|Invitational))"
    r"\s*\((?P<grade>G[123]|Listed)\)"
)

# Pedigree clause: "{winner} ... by {sire} (out of)... {dam}"
# The winner is captured from the article H1 typically (e.g. "Commandment Takes
# Florida Derby Thriller Over The Puma" — winner is "Commandment").
SLUG_TO_WINNER_RE = re.compile(r"^([a-z0-9-]+?)-(?:wins?|takes|denies|stuns|captures|claims|romps?|edges)-")

# Sire clause: "by {Sire Name}" — captures the sire after "by"
BY_SIRE_RE = re.compile(
    r"\bby\s+(?P<sire>[A-Z][A-Za-z .'\-]+(?:\s\([A-Z][a-z]+\))?)\s+(?:out\s+of|,)"
)

# Dam clause: "out of (the {Damsire} mare )?{Dam Name}"
OUT_OF_RE = re.compile(
    r"\bout\s+of\s+(?:the\s+(?P<damsire>[A-Z][A-Za-z .'\-]+(?:\s\([A-Z][a-z]+\))?)\s+mare\s+)?"
    r"(?P<dam>[A-Z][A-Za-z .'\-]+(?:\s\([A-Z][a-z]+\))?)"
)

# Date in lede ("on Saturday", "March 28", "April 4, 2026") — best-effort
DATE_LONG_RE = re.compile(
    r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{1,2})(?:,\s*(20\d{2}))?",
    flags=re.IGNORECASE,
)

# Track name (best-effort — looks for "at {Track}" pattern)
AT_TRACK_RE = re.compile(
    r"\bat\s+([A-Z][A-Za-z .'\-]+(?:Park|Downs|Racetrack|Race\s+Course))"
)


def slug_to_winner_name(slug: str) -> Optional[str]:
    m = SLUG_TO_WINNER_RE.match(slug)
    if not m:
        return None
    raw = m.group(1)   # e.g. "commandment" or "improving-take-a-breath"
    parts = raw.split("-")
    return " ".join(p.capitalize() for p in parts)


def parse_article(html: str, article_url: str, slug: str) -> tuple[Optional[dict], Optional[str]]:
    lede = extract_lede(html)
    if not lede:
        return None, "no article body found"

    # Race + grade
    race_m = RACE_RE.search(lede)
    if not race_m:
        return None, "no '{Race} ({grade})' clause in lede"
    race = race_m.group("race").strip()
    grade = race_m.group("grade")

    # Winner name from slug (most reliable — slug always starts with winner)
    winner = slug_to_winner_name(slug)
    if not winner:
        return None, "could not parse winner name from slug"

    # Sire (look in lede for "by {Sire}")
    sire_m = BY_SIRE_RE.search(lede)
    sire = sire_m.group("sire").strip() if sire_m else None

    # Dam + damsire from "out of {damsire mare} {dam}"
    out_m = OUT_OF_RE.search(lede)
    dam = out_m.group("dam").strip() if out_m else None
    damsire = out_m.group("damsire").strip() if (out_m and out_m.group("damsire")) else None

    # Date — look for "{month} {day}, {year}" or month+day with article year context
    date_str = None
    dm = DATE_LONG_RE.search(lede)
    if dm:
        month_to_num = {m.lower(): i+1 for i, m in enumerate(
            ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'])}
        m_short = dm.group(1).lower()[:3]
        m_num = month_to_num.get(m_short)
        day = int(dm.group(2))
        year = int(dm.group(3)) if dm.group(3) else datetime.now().year
        if m_num:
            date_str = f"{year:04d}-{m_num:02d}-{day:02d}"

    # Track
    track_m = AT_TRACK_RE.search(lede)
    track = track_m.group(1).strip() if track_m else None

    if not sire:
        return None, f"could not parse 'by {{sire}}' in lede (winner={winner})"

    return {
        "race":            race,
        "race_grade":      grade,
        "track":           track,
        "date":            date_str,
        "purse_usd":       None,    # often present in lede but inconsistent format; leave for review
        "winner":          winner,
        "winner_age":      None,
        "winner_sire":     sire,
        "winner_dam":      dam,
        "winner_damsire":  damsire,
        "source_url":      article_url,
        "note":            None,
    }, None


# --- Browser wrapper -------------------------------------------------------

class Browser:
    def __init__(self, headful=False, debug_dump_dir=None):
        _require_playwright()
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=not headful,
            args=["--disable-blink-features=AutomationControlled"],
        )
        self._ctx = self._browser.new_context(
            user_agent=USER_AGENT, viewport={"width": 1280, "height": 1400})
        self._page = self._ctx.new_page()
        self._last = 0.0
        self._dump = debug_dump_dir

    def _throttle(self):
        elapsed = time.time() - self._last
        if elapsed < RATE_LIMIT_SECONDS:
            time.sleep(RATE_LIMIT_SECONDS - elapsed)
        self._last = time.time()

    def fetch(self, url, label):
        self._throttle()
        try:
            self._page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT)
        except Exception as e:
            log.warning(f"  goto failed: {e}")
            return self._page.url, ""
        try:
            self._page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass
        html = self._page.content()
        if self._dump:
            self._dump.mkdir(parents=True, exist_ok=True)
            slug = re.sub(r"[^a-z0-9]+", "-", url.lower())[-80:]
            (self._dump / f"{label}-{slug}.html").write_text(html)
        return self._page.url, html

    def close(self):
        try: self._browser.close()
        except Exception: pass
        try: self._pw.stop()
        except Exception: pass


def fingerprint(rec):
    """Same dedup fingerprint shape as scrape_stakes.py."""
    race_n = (rec.get("race") or "").lower()
    race_n = re.sub(r"\b(stakes|s\.|presented by[^,]+|grade[s]?)\b", "", race_n)
    race_n = re.sub(r"\([^)]+\)", "", race_n)
    race_n = re.sub(r"[^a-z0-9]+", "", race_n)
    date_ym = (rec.get("date") or "")[:7]
    winner_n = re.sub(r"[^a-z0-9]+", "", (rec.get("winner") or "").lower())
    return "|".join([race_n, date_ym, winner_n])


def main():
    ap = argparse.ArgumentParser(description="Parse stakes results from BH article bodies")
    ap.add_argument("--listing-url",
        default="https://www.bloodhorse.com/horse-racing/articles/section/thoroughbred-racing",
        help="BH article-listing URL")
    ap.add_argument("--articles-file", help="Path to text file of article URLs")
    ap.add_argument("--max-articles", type=int, default=30)
    ap.add_argument("--no-recap-filter", action="store_true",
        help="Open every article on the listing, not just recap-slugged ones")
    ap.add_argument("--merge", action="store_true", default=True,
        help="Skip records whose fingerprint already exists in stakes-results.json")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--debug-dump", metavar="DIR")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s")

    dump = Path(args.debug_dump) if args.debug_dump else None
    bh = Browser(headful=args.headful, debug_dump_dir=dump)

    article_urls = []
    try:
        if args.articles_file:
            for line in Path(args.articles_file).read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    article_urls.append(line)
        else:
            log.info(f"Loading listing {args.listing_url}")
            _, listing = bh.fetch(args.listing_url, "listing")
            seen = set()
            for m in ARTICLE_URL_RE.finditer(listing):
                path = m.group("path")
                slug = m.group("slug")
                if path in seen:
                    continue
                seen.add(path)
                if not args.no_recap_filter and not SLUG_LOOKS_LIKE_RECAP.search(slug):
                    continue
                article_urls.append("https://www.bloodhorse.com" + path)
            article_urls = article_urls[: args.max_articles]
            log.info(f"  found {len(article_urls)} recap-slug articles")

        if not article_urls:
            log.warning("No article URLs to process.")
            return 1

        # Load existing stakes-results.json for dedup
        existing = json.loads(STAKES_JSON.read_text()) if STAKES_JSON.exists() else {"races": []}
        races_existing = existing.get("races") or []
        seen_fp = {fingerprint(r) for r in races_existing} if args.merge else set()

        added, unresolved = [], []
        for i, art_url in enumerate(article_urls, 1):
            log.info(f"[{i}/{len(article_urls)}] {art_url}")
            slug_match = re.search(r"/articles/\d+/([a-z0-9-]+)", art_url)
            if not slug_match:
                unresolved.append({"url": art_url, "reason": "could not parse article slug from URL"})
                continue
            slug = slug_match.group(1)
            _, art_html = bh.fetch(art_url, "article")
            rec, err = parse_article(art_html, art_url, slug)
            if not rec:
                unresolved.append({"url": art_url, "reason": err})
                log.info(f"  parse failed: {err}")
                continue
            fp = fingerprint(rec)
            if args.merge and fp in seen_fp:
                log.info(f"  already have {rec['race']} ({rec.get('date','—')}) — skipping")
                continue
            seen_fp.add(fp)
            races_existing.append(rec)
            added.append(rec)
            log.info(f"  OK: {rec['race']} ({rec['race_grade']}) — {rec['winner']} by {rec['winner_sire']}")

        races_existing.sort(key=lambda r: r.get("date") or "0000-00-00", reverse=True)
        existing["races"] = races_existing
        existing["updated_at"] = time.strftime("%Y-%m-%d")
        if unresolved:
            existing["unresolved_articles"] = unresolved
        elif "unresolved_articles" in existing:
            del existing["unresolved_articles"]
        STAKES_JSON.write_text(json.dumps(existing, indent=2))
        log.info(f"\nDone. Added {len(added)} new race(s); {len(unresolved)} unresolved; total {len(races_existing)}")
        if unresolved:
            for u in unresolved:
                log.info(f"  - {u['url']}: {u['reason']}")
        log.info(f"\nNext: python3 build_stakes.py")
    finally:
        bh.close()


if __name__ == "__main__":
    sys.exit(main() or 0)
