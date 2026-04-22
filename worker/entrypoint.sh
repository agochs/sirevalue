#!/usr/bin/env bash
# Worker entrypoint. Runs the refresh pipeline on an internal schedule.
# This is the simplest shape for Fly.io free-tier deployment; no external
# cron needed.

set -euo pipefail

INTERVAL_HOURS="${INTERVAL_HOURS:-24}"
INTERVAL_SECONDS=$(( INTERVAL_HOURS * 3600 ))

echo "[entrypoint] Starting SireValue worker. Cycle every ${INTERVAL_HOURS}h."

# If running one-shot (e.g. manually invoked via fly machines run), bail after
# a single pass.
if [[ "${ONE_SHOT:-0}" == "1" ]]; then
  echo "[entrypoint] ONE_SHOT mode — running once and exiting."
  python /app/nightly_refresh.py
  python /app/sync_to_repo.py
  exit 0
fi

# Continuous loop: refresh, sync, sleep. Crashes on failure so Fly restarts.
while true; do
  echo "[entrypoint] $(date -Iseconds) — running nightly refresh"
  python /app/nightly_refresh.py
  echo "[entrypoint] $(date -Iseconds) — syncing to repo"
  python /app/sync_to_repo.py
  echo "[entrypoint] $(date -Iseconds) — sleeping ${INTERVAL_SECONDS}s"
  sleep "${INTERVAL_SECONDS}"
done
