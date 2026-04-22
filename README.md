# SireValue — deployment repo

Vercel-hosted UI + external scraper worker. The worker produces fresh
`scores.json`, commits it back to this repo, Vercel rebuilds and serves.

## What's in this folder

```
sirevalue/
├── README.md              ← you are here
├── DEPLOYMENT.md          ← step-by-step production setup
├── vercel.json            ← Vercel project config (rewrites, headers)
├── package.json           ← minimal Node manifest Vercel looks for
├── public/                ← static assets Vercel serves as-is
│   ├── index.html         ← landing page with leaderboard
│   ├── stallion-card.html ← browse + score breakdown (fetches data)
│   ├── mare-matcher.html  ← mare → stallion recommender (fetches data)
│   └── data/scores.json   ← the live data blob — updated by the worker
├── api/
│   └── stallion.js        ← serverless function: per-stallion OpenGraph
└── worker/                ← separate deployment target (Fly.io / Railway)
    ├── Dockerfile
    ├── requirements.txt
    ├── fly.toml
    ├── entrypoint.sh
    ├── sync_to_repo.py    ← commits scores.json back to this repo
    └── (copy scrapers.py, value_score.py, leading_sires.py,
          combine_rosters.py, build_ui.py, nightly_refresh.py,
          and all *.json / *.csv data files from outputs/)
```

## The flow

```
┌──────────────────────┐   cron(daily)   ┌────────────────────────┐
│  Fly.io worker       │ ─────────────▶  │ scrapers.run_all()     │
│  (Python + Playwright│                 │ combine_rosters.main() │
│   long-running)      │ ◀───────────── │ build_ui.main()        │
└──────────────────────┘                 └────────────────────────┘
         │
         │ git commit scores.json + HTML
         │ git push
         ▼
┌──────────────────────┐     push-trigger   ┌────────────────────┐
│  GitHub repo         │ ─────────────────▶ │  Vercel build      │
│                      │                    │  npm install (noop)│
└──────────────────────┘                    │  serve /public     │
                                            └────────────────────┘
                                                     │
                                                     ▼
                                            ┌────────────────────┐
                                            │  sirevalue.app     │
                                            │  - /               │
                                            │  - /stallion-card  │
                                            │  - /mare-matcher   │
                                            │  - /api/stallion/… │
                                            └────────────────────┘
```

## Pricing

Rough monthly cost for the whole stack:

| Piece | Provider | Tier | Monthly |
|---|---|---|---|
| UI hosting + CDN | Vercel | Hobby | **$0** |
| Scraper worker | Fly.io | shared-cpu-1x, 256MB | **~$2–5** |
| Git hosting | GitHub | public / free | **$0** |
| Domain (optional) | any registrar | - | $10–15/yr |

**Total: ~$5/month** if you're happy on free tiers; ~$25/month on Vercel
Pro (custom domain + analytics), which you won't need until real traffic.

See `DEPLOYMENT.md` for the full step-by-step.
