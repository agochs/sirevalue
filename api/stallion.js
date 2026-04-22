// Serverless function — returns one stallion's record with OpenGraph headers.
// Triggered on /api/stallion/<name>. Lets us render a nice preview when
// people share a stallion link on X, Slack, email, etc.
//
// Deploy: Vercel picks this up automatically at /api/stallion
// Request: GET /api/stallion?name=Into+Mischief
// Response: JSON of that stallion's record, or 404.

import fs from 'node:fs';
import path from 'node:path';

let cache = null;
let cacheTime = 0;
const CACHE_TTL_MS = 60_000;   // 1 minute; data rarely changes mid-cron-cycle

function loadScores() {
  const now = Date.now();
  if (cache && (now - cacheTime) < CACHE_TTL_MS) return cache;
  const p = path.join(process.cwd(), 'public', 'data', 'scores.json');
  const raw = fs.readFileSync(p, 'utf-8');
  cache = JSON.parse(raw);
  cacheTime = now;
  return cache;
}

export default function handler(req, res) {
  const name = (req.query.name || '').trim();
  if (!name) {
    res.status(400).json({ error: 'name query param required' });
    return;
  }
  try {
    const data = loadScores();
    const stallion = data.stallions.find(
      s => s.name.toLowerCase() === name.toLowerCase()
    );
    if (!stallion) {
      res.status(404).json({ error: 'stallion not found' });
      return;
    }
    res.setHeader('Cache-Control', 'public, max-age=60, s-maxage=300');
    res.setHeader('Content-Type', 'application/json');
    res.status(200).json({
      stallion,
      model_version: data.model_version,
      share_url: `https://sirevalue.app/stallion-card.html?s=${encodeURIComponent(stallion.name)}`,
      og: {
        title: `${stallion.name} — ${stallion.score.grade} ${stallion.score.value.toFixed(1)} / SireValue`,
        description: stallion.pedigree
          ? `${stallion.pedigree}. Fee ${stallion.fee_usd ? '$'+stallion.fee_usd.toLocaleString() : (stallion.fee_qualifier||'—')}. Score ${stallion.score.value.toFixed(1)} (${stallion.score.grade}), tier ${stallion.score.tier}.`
          : `Score ${stallion.score.value.toFixed(1)} (${stallion.score.grade})`,
      },
    });
  } catch (e) {
    res.status(500).json({ error: 'internal error', detail: String(e) });
  }
}
