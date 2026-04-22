"""
Build mare-matcher.html — lets a user input their mare's pedigree (sire +
damsire) and budget, and produces a ranked list of stallions from the
roster with a match score.

Match score = Value Score (base) +/- pedigree-relationship adjustments:
  - Same sire (mare's sire == stallion's sire)       : -30 pts (half-sibling)
  - Same damsire (mare's damsire == stallion's damsire) : -10 pts
  - Mare's damsire == stallion's sire                : mild reinforcement (+3)
  - Fee outside user's budget                        : excluded from list
"""

import json
from pathlib import Path

HERE = Path(__file__).parent
SCORES_JSON = HERE / "scores.json"
OUTPUT_HTML = HERE / "mare-matcher.html"


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Mare Matcher — Stallion Value Score</title>
<style>
:root {
  --ink: #0d1b2a; --ink-soft: #415a77;
  --cream: #fdfbf4; --card: #ffffff;
  --gold: #b8860b; --gold-soft: #d4a147;
  --rule: #e0d9c6;
  --good: #2f855a; --weak: #9b2c2c;
}
* { box-sizing: border-box; }
html, body { margin: 0; padding: 0; }
body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  background: var(--cream); color: var(--ink);
  line-height: 1.45; -webkit-font-smoothing: antialiased;
}
.container { max-width: 920px; margin: 0 auto; padding: 32px 20px 80px; }
.masthead {
  border-bottom: 2px solid var(--ink);
  padding-bottom: 12px; margin-bottom: 20px;
  display: flex; justify-content: space-between; align-items: baseline;
}
.masthead .brand {
  font-family: "Iowan Old Style", Georgia, serif;
  font-size: 22px; font-weight: 700;
}
.masthead .tier {
  font-size: 12px; color: var(--ink-soft); text-transform: uppercase; letter-spacing: 1px;
}
.intro {
  background: var(--card); border: 1px solid var(--rule); border-radius: 8px;
  padding: 16px; margin-bottom: 18px; font-size: 14px; color: var(--ink-soft);
}
.form {
  background: var(--card); border: 1px solid var(--rule); border-radius: 8px;
  padding: 16px; margin-bottom: 20px;
  display: grid; grid-template-columns: 1fr 1fr; gap: 12px;
}
.form label {
  display: block; font-size: 11px; color: var(--ink-soft);
  text-transform: uppercase; letter-spacing: 1px; margin-bottom: 4px;
}
.form input, .form select {
  width: 100%; padding: 8px 10px; font-size: 14px;
  border: 1px solid var(--rule); border-radius: 6px;
  background: var(--cream); color: var(--ink);
}
.form datalist { display: none; }
.form .row-wide { grid-column: 1 / -1; }
.result-count { margin: 16px 0 8px; font-size: 13px; color: var(--ink-soft); }
.result-row {
  background: var(--card); border: 1px solid var(--rule); border-radius: 8px;
  padding: 12px 14px; margin-bottom: 10px;
  display: grid; grid-template-columns: auto 1fr auto; gap: 14px; align-items: center;
}
.rank {
  font-family: "Iowan Old Style", Georgia, serif;
  font-weight: 700; font-size: 20px; color: var(--ink-soft); min-width: 28px;
}
.middle h3 { margin: 0 0 2px; font-size: 18px; font-family: "Iowan Old Style", Georgia, serif; }
.middle .ped { font-style: italic; color: var(--ink-soft); font-size: 13px; }
.middle .farm { font-size: 11px; letter-spacing: 1px; text-transform: uppercase; color: var(--ink-soft); }
.score-block {
  text-align: right; font-size: 13px; color: var(--ink-soft);
}
.score-block .match {
  font-family: "Iowan Old Style", Georgia, serif;
  font-size: 24px; font-weight: 700; color: var(--ink);
}
.score-block .delta { font-size: 12px; }
.score-block .delta.up { color: var(--good); }
.score-block .delta.down { color: var(--weak); }
.badge {
  display: inline-block; padding: 2px 8px; border-radius: 10px;
  font-size: 10px; letter-spacing: 0.5px; text-transform: uppercase;
  margin-left: 4px; font-weight: 600;
}
.badge-inbreed { background: #fdecea; color: #8b2a1a; }
.badge-reinforce { background: #e6f4ea; color: #1e6b2f; }
.badge-pedigree-match { background: #fbf6e4; color: #6b5016; }
.badge-tier { background: #f7f2e4; color: #6b5016; }
.footer {
  margin-top: 24px; font-size: 11px; color: var(--ink-soft);
  padding-top: 14px; border-top: 1px solid var(--rule);
}
</style>
</head>
<body>
<div class="container">
  <div class="masthead">
    <div class="brand">Mare Matcher</div>
    <div class="tier">Stallion Value Score &middot; Beta</div>
  </div>

  <div class="intro">
    Enter your mare's pedigree. We'll rank every stallion in the roster by
    a combined score: their Value Score + pedigree-compatibility adjustments
    (penalty for inbreeding, mild bonus for reinforcing crosses). Blank inputs
    are ignored.
  </div>

  <div class="form">
    <div>
      <label for="m-sire">Mare's sire</label>
      <input id="m-sire" type="text" list="sire-opts" placeholder="e.g., Tapit" autocomplete="off"/>
      <datalist id="sire-opts"></datalist>
    </div>
    <div>
      <label for="m-damsire">Mare's damsire (BMS)</label>
      <input id="m-damsire" type="text" list="damsire-opts" placeholder="e.g., Distorted Humor" autocomplete="off"/>
      <datalist id="damsire-opts"></datalist>
    </div>
    <div>
      <label for="m-2ndds">Mare's 2nd damsire (granddam's sire, optional)</label>
      <input id="m-2ndds" type="text" list="sire-opts" placeholder="deeper pedigree — 3x3 / 4x3 checks" autocomplete="off"/>
    </div>
    <div>
      <label for="m-sire-sire">Mare's sire-of-sire (optional)</label>
      <input id="m-sire-sire" type="text" list="sire-opts" placeholder="for 3x2 / 4x3 pedigree checks" autocomplete="off"/>
    </div>
    <div>
      <label for="m-min">Minimum fee ($)</label>
      <input id="m-min" type="number" min="0" step="500" value="0"/>
    </div>
    <div>
      <label for="m-max">Maximum fee ($)</label>
      <input id="m-max" type="number" min="0" step="500" placeholder="no cap"/>
    </div>
    <div class="row-wide">
      <label>Match adjustments</label>
      <div style="font-size: 12px; color: var(--ink-soft);">
        Same sire (half-sibling): <b>&minus;30</b>. Shared damsire: <b>&minus;10</b>. Duplicated higher-gen ancestor 3&times;3: <b>&minus;5</b>. 4&times;3 / 3&times;4: <b>&minus;3</b>.
        Bonus: reinforcing cross (your BMS is his sire / your sire is his BMS): <b>&plus;3</b>.
        URL updates live — share the page to share your mare's profile.
      </div>
    </div>
  </div>

  <div id="result-count" class="result-count"></div>
  <div id="results"></div>

  <div class="footer">
    <span id="model-version"></span> &middot; Matches across <span id="stallion-count"></span> stallions in the roster.
  </div>
</div>

<script id="data" type="application/json">__DATA_JSON__</script>
<script>
(function() {
  var data = JSON.parse(document.getElementById('data').textContent);
  var stallions = data.stallions;
  document.getElementById('model-version').textContent = 'Model ' + data.model_version;
  document.getElementById('stallion-count').textContent = stallions.length;

  // Populate datalists with the distinct sires + damsires already in our data
  var sires = new Set(), bms = new Set();
  stallions.forEach(function(s) {
    if (s.sire) sires.add(s.sire);
    if (s.damsire) bms.add(s.damsire);
  });
  var sireOpts = document.getElementById('sire-opts');
  Array.from(sires).sort().forEach(function(x) {
    var o = document.createElement('option'); o.value = x; sireOpts.appendChild(o);
  });
  var damOpts = document.getElementById('damsire-opts');
  Array.from(bms).sort().forEach(function(x) {
    var o = document.createElement('option'); o.value = x; damOpts.appendChild(o);
  });

  function norm(s) { return (s||'').trim().toLowerCase(); }

  function computeMatch(stallion, mareSire, mareBMS, mare2ndDS, mareSireSire) {
    // Stallion ancestors we know from our data:
    //   sire, damsire
    // Mare ancestors the user gives us:
    //   sire (gen 1), sire-of-sire (gen 2), damsire (gen 2), 2nd damsire (gen 3)
    var adjustments = [];
    var adj = 0;

    // Primary inbreeding checks
    if (mareSire && stallion.sire && norm(stallion.sire) === norm(mareSire)) {
      adj -= 30;
      adjustments.push({ kind: 'inbreed', label: 'Same sire (half-sibling)', delta: -30 });
    }
    if (mareBMS && stallion.damsire && norm(stallion.damsire) === norm(mareBMS)) {
      adj -= 10;
      adjustments.push({ kind: 'inbreed', label: 'Shared BMS', delta: -10 });
    }

    // Higher-generation pedigree checks
    // 3x3: stallion's sire matches mare's sire-of-sire (both gen-2 ancestors)
    //      OR stallion's damsire matches mare's 2nd damsire (both gen-3)
    if (mareSireSire && stallion.sire && norm(stallion.sire) === norm(mareSireSire)) {
      adj -= 5;
      adjustments.push({ kind: 'inbreed', label: '3x3 duplicate (his sire = your sire-of-sire)', delta: -5 });
    }
    if (mare2ndDS && stallion.damsire && norm(stallion.damsire) === norm(mare2ndDS)) {
      adj -= 5;
      adjustments.push({ kind: 'inbreed', label: '3x3 duplicate (his BMS = your 2nd damsire)', delta: -5 });
    }
    // 4x3 / 3x4 cross: stallion's sire matches mare's 2nd damsire OR stallion's damsire matches mare's sire-of-sire
    if (mare2ndDS && stallion.sire && norm(stallion.sire) === norm(mare2ndDS)) {
      adj -= 3;
      adjustments.push({ kind: 'inbreed', label: '4x3 cross (his sire = your 2nd damsire)', delta: -3 });
    }
    if (mareSireSire && stallion.damsire && norm(stallion.damsire) === norm(mareSireSire)) {
      adj -= 3;
      adjustments.push({ kind: 'inbreed', label: '3x4 cross (his BMS = your sire-of-sire)', delta: -3 });
    }

    // Reinforcing crosses (positive nicks)
    if (mareBMS && stallion.sire && norm(stallion.sire) === norm(mareBMS)) {
      adj += 3;
      adjustments.push({ kind: 'reinforce', label: "Reinforcing: your BMS is his sire", delta: +3 });
    }
    if (mareSire && stallion.damsire && norm(stallion.damsire) === norm(mareSire)) {
      adj += 3;
      adjustments.push({ kind: 'reinforce', label: "Reinforcing: your sire is his BMS", delta: +3 });
    }

    return { adj: adj, adjustments: adjustments, match: stallion.score.value + adj };
  }

  // URL-sync helpers: let users share their mare profile via the URL
  function readFromUrl() {
    var params = new URLSearchParams(location.search);
    ['m-sire', 'm-damsire', 'm-2ndds', 'm-sire-sire', 'm-min', 'm-max'].forEach(function(id) {
      var v = params.get(id);
      if (v != null) document.getElementById(id).value = v;
    });
  }
  function writeToUrl() {
    var params = new URLSearchParams();
    ['m-sire', 'm-damsire', 'm-2ndds', 'm-sire-sire', 'm-min', 'm-max'].forEach(function(id) {
      var v = document.getElementById(id).value.trim();
      if (v) params.set(id, v);
    });
    var qs = params.toString();
    var newUrl = location.pathname + (qs ? '?' + qs : '');
    history.replaceState(null, '', newUrl);
  }

  function render() {
    writeToUrl();
    var mSire = document.getElementById('m-sire').value.trim();
    var mBMS = document.getElementById('m-damsire').value.trim();
    var m2ndDS = document.getElementById('m-2ndds').value.trim();
    var mSireSire = document.getElementById('m-sire-sire').value.trim();
    var mMin = parseInt(document.getElementById('m-min').value, 10) || 0;
    var mMaxRaw = document.getElementById('m-max').value.trim();
    var mMax = mMaxRaw ? parseInt(mMaxRaw, 10) : null;

    var list = stallions
      .filter(function(s) {
        if (s.fee_usd == null) return true;   // always include "Private" fees
        if (s.fee_usd < mMin) return false;
        if (mMax && s.fee_usd > mMax) return false;
        return true;
      })
      .map(function(s) {
        var m = computeMatch(s, mSire, mBMS, m2ndDS, mSireSire);
        return { s: s, m: m };
      });

    list.sort(function(a, b) { return b.m.match - a.m.match; });

    document.getElementById('result-count').textContent =
      'Showing ' + list.length + ' matching stallions.';

    var html = list.slice(0, 40).map(function(row, i) {
      var s = row.s, m = row.m;
      var deltaHtml = '';
      if (m.adj !== 0) {
        var cls = m.adj > 0 ? 'up' : 'down';
        deltaHtml = '<span class="delta ' + cls + '">' + (m.adj > 0 ? '+' : '') + m.adj.toFixed(0) + ' adj</span>';
      }
      var badges = m.adjustments.map(function(a) {
        var cls = a.kind === 'inbreed' ? 'badge-inbreed' :
                  (a.kind === 'reinforce' ? 'badge-reinforce' : 'badge-pedigree-match');
        return '<span class="badge ' + cls + '">' + a.label + '</span>';
      }).join(' ');
      var feeDisp = s.fee_usd ? '$' + s.fee_usd.toLocaleString() : (s.fee_qualifier || '\u2014');
      return ''
        + '<div class="result-row">'
        +   '<div class="rank">' + (i + 1) + '</div>'
        +   '<div class="middle">'
        +     '<h3>' + s.name + ' <span class="badge badge-tier">' + s.score.grade + ' ' + s.score.value.toFixed(1) + '</span></h3>'
        +     '<div class="ped">' + (s.pedigree || '&nbsp;') + '</div>'
        +     '<div class="farm">' + s.farm + ' &middot; ' + feeDisp + '</div>'
        +     (badges ? '<div style="margin-top:6px;">' + badges + '</div>' : '')
        +   '</div>'
        +   '<div class="score-block">'
        +     '<div class="match">' + m.match.toFixed(1) + '</div>'
        +     '<div>match score</div>'
        +     deltaHtml
        +   '</div>'
        + '</div>';
    }).join('');
    document.getElementById('results').innerHTML = html;
  }

  // Bind inputs
  ['m-sire','m-damsire','m-2ndds','m-sire-sire','m-min','m-max'].forEach(function(id) {
    document.getElementById(id).addEventListener('input', render);
  });

  // Load from URL query params (shared-profile mechanism) then render
  readFromUrl();
  render();
})();
</script>
</body>
</html>
"""


def main():
    data = json.loads(SCORES_JSON.read_text(encoding="utf-8"))
    compact = json.dumps(data, separators=(",", ":")).replace("</script", "<\\/script")
    html = HTML_TEMPLATE.replace("__DATA_JSON__", compact)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    print(f"Wrote {OUTPUT_HTML.name}: {OUTPUT_HTML.stat().st_size:,} bytes")


if __name__ == "__main__":
    main()
