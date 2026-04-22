# SireValue Production Deployment

End-to-end, starting from the files in this folder.

## Prerequisites

- A [GitHub](https://github.com/) account
- A [Vercel](https://vercel.com/) account (free)
- A [Fly.io](https://fly.io/) account (free tier is fine to start)
- Locally: `git`, Node 18+, Python 3.11+, `flyctl` (`brew install flyctl`)

## Step 1 — Stage the repo locally

```bash
cd sirevalue
# (optional) verify everything parses
python3 _build_production_html.py    # regenerates /public HTML from ../
ls -la public/ public/data/ api/ worker/
```

The repo is already structured for Vercel:
- `/public/*.html` — served at `/stallion-card.html`, `/mare-matcher.html`, etc.
- `/public/data/scores.json` — fetched by the HTMLs at runtime
- `/api/stallion.js` — serverless function at `/api/stallion?name=...`
- `/vercel.json` — rewrites `/stallion/:name` to `/stallion-card.html?stallion=:name`

## Step 2 — Fill the worker/ folder with the pipeline code

The worker runs the scraping pipeline you built in `outputs/`. Copy these files
from your local `outputs/` into `sirevalue/worker/`:

```bash
# Logic + orchestration
cp ../scrapers.py        worker/
cp ../nightly_refresh.py worker/
cp ../value_score.py     worker/
cp ../leading_sires.py   worker/
cp ../combine_rosters.py worker/
cp ../build_ui.py        worker/
cp ../build_mare_matcher.py worker/

# Seed data (the worker reads these on first run; they get overwritten
# on subsequent cycles if the scrapers produce new values)
cp ../*.csv  worker/      # all farm rosters
cp ../*.json worker/      # all sale-results and ranking data
```

The seed data matters because some farms are "stub scrapers" today — the
worker falls back to the checked-in CSV if the scraper can't produce fresh
data. As you implement each farm's scraper fully, it takes precedence.

## Step 3 — Push to GitHub

```bash
cd sirevalue
git init
git add .
git commit -m "Initial SireValue deployment repo"
gh repo create YOUR_USER/sirevalue --public --source=. --push
# or: create the repo on GitHub manually, then:
#   git remote add origin https://github.com/YOUR_USER/sirevalue.git
#   git push -u origin main
```

## Step 4 — Deploy the UI on Vercel

```bash
npm i -g vercel
vercel login
vercel link              # links this directory to a new Vercel project
vercel deploy --prod     # deploys to production
```

Vercel auto-detects the setup — no framework, no build step. It serves
`/public/` as static assets and the `/api/` functions run serverless.

Output: `https://sirevalue-xxxxx.vercel.app` (you can add a custom domain
in Vercel's UI later).

## Step 5 — Deploy the worker on Fly.io

```bash
cd worker
flyctl launch --no-deploy    # generates fly.toml (already staged here)
# Set secrets Fly will inject as env vars:
flyctl secrets set \
  GITHUB_TOKEN=ghp_...  \
  GIT_REPO_URL=https://github.com/YOUR_USER/sirevalue.git \
  GIT_BRANCH=main

flyctl deploy
```

The GitHub token needs `contents: write` on the repo. Generate at
https://github.com/settings/personal-access-tokens. Set expiration to
1 year and calendar a renewal.

## Step 6 — Watch it work

First cycle runs immediately when the worker starts; then every
`INTERVAL_HOURS` (default 24).

```bash
flyctl logs -a sirevalue-worker
```

You'll see:
```
[entrypoint] Starting SireValue worker. Cycle every 24h.
[entrypoint] 2026-04-22T13:00:01Z — running nightly refresh
... scraper logs ...
[entrypoint] 2026-04-22T13:05:47Z — syncing to repo
+ git clone --depth=1 --branch main ...
Pushed: Refresh scores.json (2026-04-22T13:05:50Z)
[entrypoint] 2026-04-22T13:05:50Z — sleeping 86400s
```

A minute later, Vercel's build completes and the updated data is live.

## Rollback

Data issue? The script commits to git, so rollback is a git revert:

```bash
git revert HEAD    # undo the latest refresh
git push
```

Vercel redeploys the previous state.

## Cost expectations (hobby-scale)

| Month 1 | Vercel | Fly | GitHub | Total |
|---|---|---|---|---|
| Idle (no traffic) | $0 | ~$3 | $0 | **~$3** |
| ~1,000 UI loads/mo | $0 | ~$3 | $0 | **~$3** |
| ~100,000 UI loads/mo | $0 (hobby limit) | ~$5 | $0 | **~$5** |

Custom domain adds $12/yr at most registrars. Vercel Pro ($20/mo)
becomes relevant only when you need team seats, analytics, or serve
high-bandwidth image assets.

## Troubleshooting

**"Failed to load data"** in the UI — the worker hasn't pushed yet, or
scores.json doesn't exist in the repo. Check `/public/data/scores.json` on
your GitHub repo page; if absent, manually commit the `scores.json` from
your local outputs/ to seed it.

**Fly.io deploy keeps OOMing** — bump `memory_mb` in `fly.toml`. Playwright
with chromium needs at least 512MB; 768MB is safer.

**Worker can't push** — check that `GITHUB_TOKEN` has `contents: write` on
the repo. The error message in Fly logs will include the HTTP status from
the git push.

**Vercel builds but data is stale** — Vercel caches `/data/scores.json`
for 5 minutes (s-maxage=300 in vercel.json). A redeploy from git push
invalidates that cache. If you pushed but the UI still shows old data,
hit Vercel's "Redeploy" button to force a cache bust.
