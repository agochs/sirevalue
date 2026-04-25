"""Push the fresh scores.json back to the Vercel-deployed GitHub repo.

Reads scores.json (produced by build_ui.py running inside the worker) and
commits it to `public/data/scores.json` on the configured branch. The push
triggers a Vercel rebuild, which redeploys the site with the updated data.

Environment variables required:
  GITHUB_TOKEN       - personal-access token or GitHub App token (repo write)
  GIT_REPO_URL       - e.g. https://github.com/goaty/sirevalue.git
  GIT_BRANCH         - usually "main"
  GIT_AUTHOR_NAME    - commit author name (default: SireValue Bot)
  GIT_AUTHOR_EMAIL   - commit author email
  OUTPUT_DIR         - local dir where scores.json was written
                       (default: /app/data)

Usage: run after nightly_refresh.py completes.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

log = logging.getLogger("sync")


def _redact(s: str) -> str:
    """Strip GitHub tokens from any string before logging.
    Covers the `https://x-access-token:TOKEN@github.com/...` pattern we use,
    plus bare `github_pat_*` / `ghp_*` / `ghs_*` / `gho_*` tokens.
    """
    s = re.sub(r"(https://[^:@/\s]+:)([^@/\s]+)(@)", r"\1<redacted>\3", s)
    s = re.sub(r"\b(github_pat_|ghp_|ghs_|gho_)[A-Za-z0-9_]+", r"\1<redacted>", s)
    return s


def run(cmd: list[str], cwd: str | None = None, env: dict | None = None) -> str:
    """Run a subprocess, raising on failure. Returns stdout."""
    log.info(_redact(f"+ {' '.join(cmd)}"))
    r = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)
    if r.returncode != 0:
        log.error(_redact(r.stdout + r.stderr))
        raise RuntimeError(f"command failed: {_redact(' '.join(cmd))}")
    return r.stdout


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    token = os.environ.get("GITHUB_TOKEN")
    repo_url = os.environ.get("GIT_REPO_URL")
    branch = os.environ.get("GIT_BRANCH", "main")
    author_name = os.environ.get("GIT_AUTHOR_NAME", "SireValue Bot")
    author_email = os.environ.get("GIT_AUTHOR_EMAIL", "bot@sirevalue.app")
    output_dir = Path(os.environ.get("OUTPUT_DIR", "/app/data"))

    if not token or not repo_url:
        log.error("GITHUB_TOKEN and GIT_REPO_URL are required environment variables")
        return 2

    scores_path = output_dir / "scores.json"
    if not scores_path.exists():
        log.error(f"scores.json not found at {scores_path}")
        return 3

    # Files to sync to the repo. `scores.json` is required; the others are
    # best-effort (absent files just aren't copied).
    #   (source_path_in_OUTPUT_DIR, target_path_in_repo, required)
    data_files = [
        ("scores.json",                     "public/data/scores.json",              True),
        ("movers.json",                     "public/data/movers.json",              False),
        ("bloodhorse-leading-sires-2025.json",
                                            "public/data/progeny-sires-2025.json",  False),
        ("bloodhorse-bms-earnings-2026.json",
                                            "public/data/progeny-bms-2026.json",    False),
        ("upcoming-sales.json",             "public/data/upcoming-sales.json",      False),
        ("recent-sale-results.json",        "public/data/recent-sale-results.json", False),
        ("similar-stallions.json",          "public/data/similar-stallions.json",   False),
    ]

    # Construct the authenticated URL (tokens are URL-safe; no escaping needed
    # for the typical ghp_* token format).
    # Example: https://x-access-token:ghp_XXX@github.com/user/repo.git
    auth_url = repo_url.replace("https://", f"https://x-access-token:{token}@")

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        run(["git", "clone", "--depth=1", "--branch", branch, auth_url, str(tmp)])

        # Configure author
        run(["git", "config", "user.name", author_name], cwd=str(tmp))
        run(["git", "config", "user.email", author_email], cwd=str(tmp))

        # Copy each data file into the target path in the repo
        copied_targets: list[str] = []
        for src_name, tgt_rel, required in data_files:
            src = output_dir / src_name
            if not src.exists():
                if required:
                    log.error(f"required file missing: {src}")
                    return 4
                log.info(f"optional file missing, skipping: {src_name}")
                continue
            target = tmp / tgt_rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, target)
            copied_targets.append(tgt_rel)

        # Check if there's anything to commit
        status = run(["git", "status", "--porcelain"], cwd=str(tmp))
        if not status.strip():
            log.info("data files unchanged; skipping push")
            return 0

        # Commit + push
        for tgt_rel in copied_targets:
            run(["git", "add", tgt_rel], cwd=str(tmp))
        msg = f"Refresh data ({subprocess.check_output(['date','-Iseconds'], text=True).strip()})"
        run(["git", "commit", "-m", msg], cwd=str(tmp))
        run(["git", "push", "origin", branch], cwd=str(tmp))
        log.info(f"Pushed: {msg}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
