"""
Nightly refresh orchestrator — turns the manual dry-run workflow into a
scheduled pipeline.

What this does:
  1. Fetches each known data source in sequence (farms + sales).
  2. Writes each source's facts to its canonical JSON file.
  3. Re-runs combine_rosters + build_ui + build_mare_matcher.
  4. Runs the golden-set regression tests.
  5. Compares new scores to the prior scores.json; emits a diff report:
     - stallions that entered/left the roster
     - scores that moved by ≥5 points
     - tier transitions (commercial_appeal ↔ tier1_full, etc.)
  6. On any golden-set failure, aborts and restores the prior scores.json
     from the last successful snapshot.

Scheduling:
  Cron / launchd / systemd-timer / GitHub Actions — all fine. The entry point
  is `main()`; it's idempotent and can be re-run safely. Recommend
  invoking once per 24 hours during stallion-shopping season (Nov-Feb)
  and once per 72 hours in the off-season.

Deployment:
  In production this runs in a container with:
    - a persistent volume for the outputs/ directory
    - network access to the scraped hosts
    - credentials (if any) for sites that require auth in the future
    - an outbound channel for alerts (Slack webhook, email relay)

What this script stubs rather than implements:
  The scraper *fetchers* are stubs here because the live scrapers require
  a real browser session (for farm websites with JS rendering) or
  licensed APIs (TJCIS/Equibase eventually). In production:
    - farm-site fetchers: headless Playwright per farm
    - sales-company fetchers: BloodHorse session pages via httpx
    - licensed feeds: direct API clients

  The orchestration around those fetchers is real and testable today.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

HERE = Path(__file__).parent
SCORES_JSON = HERE / "scores.json"
SCORES_SNAPSHOTS = HERE / "scores_snapshots"
SCORES_SNAPSHOTS.mkdir(exist_ok=True)

log = logging.getLogger("nightly")


# ---------------------------------------------------------------------------
# Source definitions — the full list of data targets
# ---------------------------------------------------------------------------

@dataclass
class DataSource:
    name: str                 # human-readable name for logs
    output_file: str          # canonical JSON filename produced
    fetcher: Callable         # () -> dict | None; writes to output_file
    max_age_hours: int = 168  # re-fetch if the file is older than this (default weekly)


def _skip_fetcher(name: str):
    """Placeholder fetcher used for sources whose scraper isn't automated yet.
    Keeps the orchestration model consistent — the existing file is kept.
    """
    def _run():
        log.info(f"[{name}] skip (no automated fetcher yet; existing file preserved)")
        return None
    return _run


DATA_SOURCES: list[DataSource] = [
    # Farm rosters
    DataSource("Spendthrift",   "spendthrift-dryrun.csv",   _skip_fetcher("Spendthrift"),   max_age_hours=168),
    DataSource("WinStar",       "winstar-dryrun.csv",       _skip_fetcher("WinStar"),       max_age_hours=168),
    DataSource("Lane's End",    "lanesend-dryrun.csv",      _skip_fetcher("Lane's End"),    max_age_hours=168),
    DataSource("Ashford",       "ashford-dryrun.csv",       _skip_fetcher("Ashford"),       max_age_hours=168),
    DataSource("Three Chimneys","threechimneys-dryrun.csv", _skip_fetcher("Three Chimneys"),max_age_hours=168),
    DataSource("Gainesway",     "gainesway-dryrun.csv",     _skip_fetcher("Gainesway"),     max_age_hours=168),
    DataSource("Hill 'n' Dale", "hillndale-dryrun.csv",     _skip_fetcher("Hill 'n' Dale"), max_age_hours=168),
    DataSource("Claiborne",     "claiborne-dryrun.csv",     _skip_fetcher("Claiborne"),     max_age_hours=168),
    DataSource("Darley",        "darley-dryrun.csv",        _skip_fetcher("Darley"),        max_age_hours=168),
    DataSource("Taylor Made",   "taylormade-dryrun.csv",    _skip_fetcher("Taylor Made"),   max_age_hours=168),
    DataSource("Airdrie",       "airdrie-dryrun.csv",       _skip_fetcher("Airdrie"),       max_age_hours=168),
    DataSource("Calumet",       "calumet-dryrun.csv",       _skip_fetcher("Calumet"),       max_age_hours=168),

    # Sale results — fetched less often since most sales happen once a year
    DataSource("Keeneland Sep 2025",         "keeneland-yearling-avgs.json",     _skip_fetcher("Kee Sep"),     max_age_hours=720),
    DataSource("FT Saratoga 2025",           "ft-saratoga-2025.json",            _skip_fetcher("FT Saratoga"), max_age_hours=720),
    DataSource("FT Kentucky October 2025",   "ft-kentucky-october-2025.json",    _skip_fetcher("FT Ky Oct"),   max_age_hours=720),
    DataSource("FT Midlantic Fall 2025",     "ft-midlantic-fall-2025.json",      _skip_fetcher("FT Midlantic"),max_age_hours=720),
    DataSource("OBS October 2025",           "obs-october-2025-yearlings.json",  _skip_fetcher("OBS Oct"),     max_age_hours=720),
    DataSource("OBS Spring 2025",            "obs-2yo-avgs.json",                _skip_fetcher("OBS Spring"),  max_age_hours=720),
    DataSource("OBS June 2025",              "obs-june-2025.json",               _skip_fetcher("OBS June"),    max_age_hours=720),
    DataSource("OBS March 2026",             "obs-march-2026.json",              _skip_fetcher("OBS March"),   max_age_hours=720),

    # Prestige rankings — refresh weekly during season
    DataSource("BloodHorse Sires 2025",      "bloodhorse-leading-sires-2025.json", _skip_fetcher("BH Sires"),  max_age_hours=168),
    DataSource("BloodHorse BMS 2026",        "bloodhorse-bms-earnings-2026.json",  _skip_fetcher("BH BMS"),    max_age_hours=168),
]


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def _file_age_hours(path: Path) -> float:
    if not path.exists():
        return float("inf")
    seconds = (datetime.now().timestamp() - path.stat().st_mtime)
    return seconds / 3600


def refresh_sources() -> dict:
    """Re-fetch any source whose output is older than its max_age_hours.

    Production path uses the scrapers module when available (requires httpx,
    bs4, and playwright installed in the container). Falls back to the stub
    fetchers otherwise — useful for development without those deps.
    """
    report = {"fetched": [], "skipped_fresh": [], "errors": []}
    try:
        from scrapers import ALL_SCRAPERS   # real scrapers
    except ImportError:
        ALL_SCRAPERS = None
        log.info("scrapers module unavailable — falling back to stubs")

    if ALL_SCRAPERS is not None:
        # Use real scrapers keyed by output_filename
        scraper_by_file = {c.output_filename: c for c in ALL_SCRAPERS}
        for src in DATA_SOURCES:
            out = HERE / src.output_file
            age = _file_age_hours(out)
            if age < src.max_age_hours:
                report["skipped_fresh"].append({"name": src.name, "age_hours": round(age, 1)})
                continue
            cls = scraper_by_file.get(src.output_file)
            if not cls:
                report["errors"].append({"name": src.name, "error": "no scraper registered"})
                continue
            try:
                result = cls(output_dir=HERE).run()
                report["fetched"].append({"name": src.name, "rows": result.rows_written,
                                          "duration": result.duration_seconds,
                                          "warnings": len(result.warnings)})
            except Exception as e:
                report["errors"].append({"name": src.name, "error": str(e)})
                log.exception(f"[{src.name}] scraper failed")
        return report

    # Stub fallback
    for src in DATA_SOURCES:
        out = HERE / src.output_file
        age = _file_age_hours(out)
        if age < src.max_age_hours:
            report["skipped_fresh"].append({"name": src.name, "age_hours": round(age, 1)})
            continue
        try:
            src.fetcher()
            report["fetched"].append(src.name)
        except Exception as e:
            report["errors"].append({"name": src.name, "error": str(e)})
            log.exception(f"[{src.name}] fetcher failed")
    return report


def run_tests() -> bool:
    """Run golden-set tests if present; skip otherwise (worker bundle may
    ship without the test file — tests are expected to live in CI instead)."""
    test_file = HERE / "test_spendthrift_golden.py"
    if not test_file.exists():
        log.info("Golden-set tests not present in worker bundle; skipping.")
        return True
    proc = subprocess.run(
        ["python3", "test_spendthrift_golden.py"],
        cwd=str(HERE),
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        log.error("Golden-set tests failed:\n" + proc.stdout + proc.stderr)
        return False
    log.info("Golden-set tests green")
    return True


def rebuild() -> None:
    """Re-run combine_rosters + build_ui + build_mare_matcher + similar."""
    for script in (
        "combine_rosters.py",
        "build_ui.py",
        "build_mare_matcher.py",
        "compute_similar_stallions.py",
        "compute_score_history.py",
        "compute_hip_pinhooks.py",
        "compute_consignor_pinhooks.py",
        "score_catalog.py",
    ):
        script_path = HERE / script
        if not script_path.exists():
            log.warning(f"{script} not present in worker bundle; skipping")
            continue
        proc = subprocess.run(["python3", script], cwd=str(HERE), capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(f"{script} failed:\n{proc.stderr}")


def diff_scores(old_path: Path, new_path: Path) -> dict:
    """Compute a diff report between two scores.json snapshots.

    Returns a structure suitable both for logging and for serving to the UI
    (movers.json). Entries carry enough context (farm, fee, current score,
    tier) to render a panel without a second lookup.
    """
    old = {s["name"]: s for s in json.loads(old_path.read_text())["stallions"]} if old_path.exists() else {}
    new = {s["name"]: s for s in json.loads(new_path.read_text())["stallions"]}

    def _entry(s: dict, extras: dict | None = None) -> dict:
        base = {
            "name":  s.get("name"),
            "farm":  s.get("farm"),
            "fee_usd": s.get("fee_usd"),
            "score": s.get("score", {}).get("value"),
            "tier":  s.get("score", {}).get("tier"),
            "grade": s.get("score", {}).get("grade"),
        }
        if extras:
            base.update(extras)
        return base

    entered = [_entry(new[n]) for n in sorted(new) if n not in old]
    left    = [_entry(old[n]) for n in sorted(old) if n not in new]
    moved, tier_tx = [], []
    for name in set(old) & set(new):
        o, n = old[name], new[name]
        delta = n["score"]["value"] - o["score"]["value"]
        if abs(delta) >= 5:
            moved.append(_entry(n, {
                "delta": round(delta, 1),
                "from": o["score"]["value"],
                "to":   n["score"]["value"],
            }))
        if o["score"]["tier"] != n["score"]["tier"]:
            tier_tx.append(_entry(n, {
                "tier_from": o["score"]["tier"],
                "tier_to":   n["score"]["tier"],
            }))
    moved.sort(key=lambda x: abs(x["delta"]), reverse=True)
    return {"entered": entered, "left": left,
            "moved": moved, "tier_transitions": tier_tx}


def write_movers(diff: dict, prior_snap: Path | None) -> Path:
    """Persist the cycle's diff as movers.json alongside scores.json so the
    sync step pushes it with the rest of the data. Includes timestamps so
    the UI can render \"as of …\" and fade stale data."""
    movers_path = HERE / "movers.json"
    payload = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "prior_snapshot": prior_snap.name if prior_snap else None,
        "counts": {
            "entered": len(diff.get("entered", [])),
            "left":    len(diff.get("left", [])),
            "moved":   len(diff.get("moved", [])),
            "tier_transitions": len(diff.get("tier_transitions", [])),
        },
        **diff,
    }
    movers_path.write_text(json.dumps(payload, indent=2))
    return movers_path


def snapshot_scores() -> Path:
    """Copy current scores.json into the snapshot archive with a timestamp."""
    if not SCORES_JSON.exists():
        return None
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    snap = SCORES_SNAPSHOTS / f"scores-{ts}.json"
    shutil.copy(SCORES_JSON, snap)
    return snap


def latest_snapshot() -> Optional[Path]:
    snaps = sorted(SCORES_SNAPSHOTS.glob("scores-*.json"))
    return snaps[-1] if snaps else None


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # 1. Snapshot current scores before we mutate anything
    prior_snap = snapshot_scores()
    log.info(f"Prior snapshot: {prior_snap}")

    # 2. Refresh any stale data sources
    fetch_report = refresh_sources()
    log.info(f"Fetch report: {json.dumps(fetch_report, indent=2)}")

    # 3. Rebuild the combined roster + UI artifacts
    try:
        rebuild()
    except Exception as e:
        log.error(f"Rebuild failed: {e}")
        if prior_snap:
            log.warning(f"Restoring scores.json from {prior_snap}")
            shutil.copy(prior_snap, SCORES_JSON)
        sys.exit(2)

    # 4. Run golden-set tests
    if not run_tests():
        if prior_snap:
            log.warning(f"Tests failed; restoring scores.json from {prior_snap}")
            shutil.copy(prior_snap, SCORES_JSON)
        sys.exit(3)

    # 5. Diff the new scores against the prior snapshot
    if prior_snap and prior_snap.exists():
        diff = diff_scores(prior_snap, SCORES_JSON)
        log.info(f"Score diff since prior run:\n{json.dumps(diff, indent=2)}")
    else:
        log.info("No prior snapshot to diff against")
        diff = {"entered": [], "left": [], "moved": [], "tier_transitions": []}

    # 6. Persist the diff as movers.json so the UI can surface "recent movers"
    movers_path = write_movers(diff, prior_snap)
    log.info(f"Wrote movers report: {movers_path}")

    log.info("Nightly refresh complete.")


if __name__ == "__main__":
    main()
