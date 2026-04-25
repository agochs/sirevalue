// Server-rendered share page for /stallion/:name
//
// When a stallion link is pasted into Slack / X / iMessage / etc., the
// crawler hits this endpoint and reads the OG meta tags. Humans get
// redirected client-side to /stallion-card.html?s=:name where the SPA
// renders the full card.
//
// Why a separate endpoint: stallion-card.html is one static file with JS
// rendering different stallions based on URL params, so its OG tags can
// only be generic. Crawlers don't run JS — they need server-rendered tags
// per stallion to make link previews useful.

import fs from 'node:fs';
import path from 'node:path';

let cache = null;
let cacheTime = 0;
const CACHE_TTL_MS = 60_000;

function loadScores() {
  const now = Date.now();
  if (cache && (now - cacheTime) < CACHE_TTL_MS) return cache;
  const p = path.join(process.cwd(), 'public', 'data', 'scores.json');
  cache = JSON.parse(fs.readFileSync(p, 'utf-8'));
  cacheTime = now;
  return cache;
}

function escape(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

export default function handler(req, res) {
  const name = (req.query.name || '').trim();
  if (!name) {
    res.status(400).send('name required');
    return;
  }

  let stallion = null;
  try {
    const data = loadScores();
    stallion = data.stallions.find(
      s => s.name.toLowerCase() === name.toLowerCase()
    );
  } catch (e) {
    // Fall through to a generic page if data load fails
  }

  // Build OG content. Even if the stallion isn't found, return a usable page
  // (with generic OG) and redirect — better than a hard 404 on a shared link.
  const targetUrl = `/stallion-card.html?s=${encodeURIComponent(stallion ? stallion.name : name)}`;
  const title = stallion
    ? `${stallion.name} — ${stallion.score.grade} ${stallion.score.value.toFixed(1)} · SireValue`
    : `${name} · SireValue`;
  const description = stallion
    ? (() => {
        const parts = [];
        if (stallion.pedigree) parts.push(stallion.pedigree);
        const fee = stallion.fee_usd ? '$' + stallion.fee_usd.toLocaleString()
                  : (stallion.fee_qualifier || null);
        if (fee) parts.push(`Fee ${fee}`);
        parts.push(`Score ${stallion.score.value.toFixed(1)} (${stallion.score.grade}), ${(stallion.score.tier||'').replace(/_/g,' ')}`);
        const me = (stallion.score.components.market_efficiency || {}).inputs;
        if (me && me.ratio) parts.push(`Yearling ROI ${me.ratio.toFixed(1)}×`);
        return parts.join(' · ');
      })()
    : 'SireValue · transparent, market-based valuations for North American commercial thoroughbred stallions.';
  const canonicalUrl = `https://sirevalue.vercel.app/stallion/${encodeURIComponent(stallion ? stallion.name : name)}`;

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>${escape(title)}</title>
<meta name="description" content="${escape(description)}"/>
<link rel="canonical" href="${escape(canonicalUrl)}"/>
<meta property="og:type" content="website"/>
<meta property="og:title" content="${escape(title)}"/>
<meta property="og:description" content="${escape(description)}"/>
<meta property="og:url" content="${escape(canonicalUrl)}"/>
<meta property="og:site_name" content="SireValue"/>
<meta name="twitter:card" content="summary"/>
<meta name="twitter:title" content="${escape(title)}"/>
<meta name="twitter:description" content="${escape(description)}"/>
<meta http-equiv="refresh" content="0; url=${escape(targetUrl)}"/>
<style>
  body { font-family: -apple-system, sans-serif; padding: 40px; color: #0d1b2a; }
  a { color: #b8860b; }
</style>
</head>
<body>
<p>Loading <strong>${escape(stallion ? stallion.name : name)}</strong>… <a href="${escape(targetUrl)}">click here if you're not redirected</a>.</p>
<script>location.replace(${JSON.stringify(targetUrl)});</script>
</body>
</html>`;

  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.setHeader('Cache-Control', 'public, max-age=300, s-maxage=600');
  res.status(stallion ? 200 : 200).send(html);
}
