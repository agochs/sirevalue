"""Generate Vercel-ready HTML files from the local build outputs.

Takes stallion-card.html and mare-matcher.html (which have the data embedded
inline as a 230KB JSON blob) and produces stripped-down versions that fetch
/data/scores.json at runtime instead. Run this from the outputs/ directory."""

import re
from pathlib import Path

HERE = Path(__file__).parent
OUTPUTS = HERE.parent   # outputs/

PAIRS = [
    (OUTPUTS / "stallion-card.html", HERE / "public" / "stallion-card.html"),
    (OUTPUTS / "mare-matcher.html",  HERE / "public" / "mare-matcher.html"),
]

BOOT_REPLACEMENT = """async function bootSireValue() {
  var dataEl = document.getElementById('data');
  var src = dataEl.getAttribute('data-src');
  var resp = await fetch(src);
  if (!resp.ok) throw new Error('failed to load scores: ' + resp.status);
  var data = await resp.json();"""

CLOSING_REPLACEMENT = """}
bootSireValue().catch(function(e){
  document.body.innerHTML += '<div style="padding:20px;color:#9b2c2c;font-family:sans-serif">Error loading data: ' + e.message + '</div>';
});
</script>
</body>"""


def convert(src_html: str) -> str:
    # 1. Strip the huge inline data blob; keep a placeholder tag with data-src.
    out = re.sub(
        r'<script id="data" type="application/json">.*?</script>',
        '<script id="data" type="application/json" data-src="/data/scores.json"></script>',
        src_html, flags=re.DOTALL,
    )
    # 2. Replace the IIFE opener that parses embedded JSON with an async fetch.
    out = out.replace(
        '(function() {\n  var data = JSON.parse(document.getElementById(\'data\').textContent);',
        BOOT_REPLACEMENT,
        1,
    )
    # 3. Replace the IIFE closer with an async boot call + error handler.
    out = out.replace('})();\n</script>\n</body>', CLOSING_REPLACEMENT, 1)
    return out


def main():
    for src, dst in PAIRS:
        if not src.exists():
            print(f"[skip] {src} not found")
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        converted = convert(src.read_text(encoding="utf-8"))
        dst.write_text(converted, encoding="utf-8")
        print(f"wrote {dst} ({len(converted):,} bytes)")


if __name__ == "__main__":
    main()
