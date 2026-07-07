#!/usr/bin/env python3
"""Render PAPER.md into a self-contained, readable HTML page.

Reusable: re-run after editing PAPER.md to regenerate the reader.
  python3 tools/render_reader.py            # -> build/paper_reader.html
Design: light editorial-technical reading layout, sticky contents rail,
per-section maturity badges, the Section 3/4 diagrams inlined.
No external assets (fonts/JS/CSS) — safe for Artifact CSP and offline use.
"""
import re, subprocess, html, sys
from pathlib import Path
import markdown

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "PAPER.md"
OUT = ROOT / "build" / "paper_reader.html"
DIAG = ROOT / "diagrams"

def git_short():
    try:
        return subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"],
                                       text=True).strip()
    except Exception:
        return "uncommitted"

# --- section maturity (matched by leading text of each top-level heading) ---
MATURITY = [
    ("SECTION 1", "Complete", "powered run · results bound", "done"),
    ("SECTION 2", "Demonstrated", "control boundary to L3", "done"),
    ("SECTION 3", "Scaffold", "north-star locked · honest gaps", "scaffold"),
    ("SECTION 4", "Stub", "thesis locked · numbers out of scope", "stub"),
    ("Appendix S2-A", "Reference spec", "executable target (SSOT)", "ref"),
    ("Appendix S3-A", "Reference spec", "executable target (SSOT)", "ref"),
]
def maturity_for(text):
    for key, label, sub, cls in MATURITY:
        if text.strip().startswith(key):
            return label, sub, cls
    return None

def load_svg(name):
    return (DIAG / name).read_text(encoding="utf-8")

md_text = SRC.read_text(encoding="utf-8")

# inject diagram placeholders right after the two vision-section subtitles
md_text = md_text.replace(
    "### Integrable, Scalable Agent Data Engineering on Spark Connect + Kubernetes",
    "### Integrable, Scalable Agent Data Engineering on Spark Connect + Kubernetes\n\n[[[SVG-SECTION3]]]\n",
    1)
md_text = md_text.replace(
    "### Stub — thesis locked; quantitative validation is a separate experiment",
    "### Stub — thesis locked; quantitative validation is a separate experiment\n\n[[[SVG-SECTION4]]]\n",
    1)

md = markdown.Markdown(extensions=["tables", "fenced_code", "sane_lists", "attr_list", "toc"],
                       extension_configs={"toc": {"permalink": False, "toc_depth": "1-2"}})
body = md.convert(md_text)

# --- build the contents rail from h1/h2 the toc extension id'd ---
heads = re.findall(r'<h([12]) id="([^"]+)">(.*?)</h[12]>', body, flags=re.S)
def strip_tags(s): return re.sub(r"<[^>]+>", "", s)
toc_items = []
for level, hid, raw in heads:
    txt = strip_tags(raw).strip()
    cls = ""
    m = maturity_for(txt)
    if m: cls = m[2]
    toc_items.append((int(level), hid, txt, cls))
toc_html = ['<nav class="toc" aria-label="Contents">']
for level, hid, txt, cls in toc_items:
    if level == 1:
        dot = f'<span class="dot {cls}"></span>' if cls else ""
        toc_html.append(f'<a class="l1" href="#{hid}">{dot}<span>{html.escape(txt)}</span></a>')
    else:
        toc_html.append(f'<a class="l2" href="#{hid}">{html.escape(txt)}</a>')
toc_html.append("</nav>")
toc_html = "\n".join(toc_html)

# --- badge each top-level section heading ---
def badge_h1(m):
    hid, inner = m.group(1), m.group(2)
    txt = strip_tags(inner).strip()
    mat = maturity_for(txt)
    if not mat:
        return m.group(0)
    label, sub, cls = mat
    chip = f'<span class="badge {cls}" title="{html.escape(sub)}">{html.escape(label)}</span>'
    return f'<h1 id="{hid}" class="section">{inner} {chip}</h1>'
body = re.sub(r'<h1 id="([^"]+)">(.*?)</h1>', badge_h1, body, flags=re.S)

def badge_h2(m):
    hid, inner = m.group(1), m.group(2)
    mat = maturity_for(strip_tags(inner).strip())
    if not mat:
        return m.group(0)
    label, sub, cls = mat
    chip = f'<span class="badge {cls}" title="{html.escape(sub)}">{html.escape(label)}</span>'
    return f'<h2 id="{hid}" class="section-appendix">{inner} {chip}</h2>'
body = re.sub(r'<h2 id="([^"]+)">(.*?)</h2>', badge_h2, body, flags=re.S)

# --- inline the diagrams ---
def figure(svg, cap):
    return f'<figure class="diagram"><div class="diagram-frame">{svg}</div><figcaption>{cap}</figcaption></figure>'
body = body.replace("<p>[[[SVG-SECTION3]]]</p>",
    figure(load_svg("section3_open_governed_platform.svg"),
           "The open governed reference architecture. Solid = demonstrated · dashed = configured but unrun · dotted = frontier."))
body = body.replace("<p>[[[SVG-SECTION4]]]</p>",
    figure(load_svg("section4_omnigent_orchestration.svg"),
           "Omnigent: one custodian over a credential-free heterogeneous fleet. Credential custody (dotted) is the frontier keystone."))
body = body.replace("<p>[[[SVG-COST]]]</p>",
    figure(load_svg("cost_tokens_conciseness.svg"),
           "The cost of the declarative paradigm, arm B relative to arm A (= 100%): SDP writes far less code and spends more tokens. Bars show B as a percentage of A; absolute medians beneath."))
body = body.replace("<p>[[[SVG-CONTROLBOUNDARY]]]</p>",
    figure(load_svg("section2_control_boundary.svg"),
           "The control boundary: the agent authors inert desired-state and never crosses into execution; a governed dry-run gate and reconciler validate and run it. Only structural feedback returns."))
body = body.replace("<p>[[[SVG-RUNLOOP]]]</p>",
    figure(load_svg("section1_run_loop.svg"),
           "The study run-loop for one cell. Arm B passes a structural SDP dry-run before execution; arm A has no gate. Failures feed back to the agent up to an iteration cap."))
body = body.replace("<p>[[[SVG-TAXONOMY]]]</p>",
    figure(load_svg("section1_defect_taxonomy.svg"),
           "What a structural gate can and cannot catch: structural defects are caught pre-data; semantic defects are un-gateable by construction and ship as silent defects."))

# --- wrap wide tables so the page never scrolls sideways ---
body = body.replace("<table>", '<div class="tablewrap"><table>').replace("</table>", "</table></div>")

CSS = """
<style>
:root{
  --paper:#f5f8f6; --surface:#ffffff; --panel:#eef3f0;
  --ink:#182220; --body:#26312e; --muted:#5d6b66; --faint:#8496905e;
  --rule:#dbe4e0; --rule-strong:#c4d1cc;
  --accent:#0f6e56; --accent-deep:#083f33; --accent-soft:#e3f2ec;
  --done:#3B6D11; --done-bg:#eef4e2; --scaffold:#8a5a0b; --scaffold-bg:#f7efdd;
  --stub:#534AB7; --stub-bg:#eceafb; --ref:#185FA5; --ref-bg:#e6f0fb;
  --serif:"Iowan Old Style","Charter","Cambria",Georgia,"Times New Roman",serif;
  --sans:ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  --mono:ui-monospace,"SF Mono","JetBrains Mono",Menlo,Consolas,monospace;
}
*{box-sizing:border-box}
body{margin:0;background:var(--paper);color:var(--body);
  font-family:var(--serif);font-size:17px;line-height:1.62;
  -webkit-font-smoothing:antialiased;text-rendering:optimizeLegibility}
a{color:var(--accent);text-decoration:none}
a:hover{text-decoration:underline}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-radius:3px}

.masthead{background:linear-gradient(180deg,#ffffff, #f2f7f4);border-bottom:1px solid var(--rule);
  padding:34px 24px 26px}
.masthead .inner{max-width:1120px;margin:0 auto}
.eyebrow{font-family:var(--sans);font-size:12px;letter-spacing:.14em;text-transform:uppercase;
  color:var(--accent);font-weight:600;margin:0 0 10px}
.masthead h1.title{font-family:var(--sans);font-weight:600;font-size:30px;line-height:1.12;
  letter-spacing:-.01em;color:var(--ink);margin:0;text-wrap:balance;max-width:20ch}
.masthead .sub{font-size:16px;color:var(--muted);margin:10px 0 0;max-width:60ch}
.metarow{display:flex;flex-wrap:wrap;gap:8px 18px;align-items:center;margin-top:18px;
  font-family:var(--sans);font-size:12.5px;color:var(--muted)}
.metarow .k{font-variant-numeric:tabular-nums}
.legend{display:flex;gap:14px;flex-wrap:wrap;margin-left:auto}
.legend span{display:inline-flex;align-items:center;gap:6px}
.swatch{width:22px;height:0;border-top-width:2px;border-top-style:solid;display:inline-block}
.swatch.solid{border-color:#5d6b66}.swatch.dash{border-color:#5d6b66;border-top-style:dashed}
.swatch.dot{border-color:#5d6b66;border-top-style:dotted}

.wrap{max-width:1120px;margin:0 auto;padding:0 24px;
  display:grid;grid-template-columns:262px minmax(0,1fr);gap:44px;align-items:start}
.rail{position:sticky;top:0;max-height:100vh;overflow:auto;padding:26px 0 40px}
.rail h2{font-family:var(--sans);font-size:11px;letter-spacing:.13em;text-transform:uppercase;
  color:var(--muted);font-weight:600;margin:0 0 10px}
.toc{display:flex;flex-direction:column;gap:1px;font-family:var(--sans)}
.toc a{padding:4px 8px;border-radius:6px;color:var(--body);display:flex;align-items:center;gap:8px}
.toc a:hover{background:var(--panel);text-decoration:none}
.toc .l1{font-size:13.5px;font-weight:500;color:var(--ink);margin-top:9px}
.toc .l2{font-size:12.5px;color:var(--muted);padding-left:20px}
.dot{width:8px;height:8px;border-radius:50%;flex:0 0 auto}
.dot.done{background:var(--done)}.dot.scaffold{background:var(--scaffold)}
.dot.stub{background:var(--stub)}.dot.ref{background:var(--ref)}

main{padding:30px 0 90px;min-width:0}
.reading-map{background:var(--surface);border:1px solid var(--rule);border-left:3px solid var(--accent);
  border-radius:10px;padding:16px 20px;margin:0 0 30px;font-size:15.5px}
.reading-map b{font-family:var(--sans);font-weight:600;font-size:12px;letter-spacing:.1em;
  text-transform:uppercase;color:var(--accent);display:block;margin-bottom:6px}
.reading-map p{margin:.4em 0}
.reading-map .pill{font-family:var(--sans);font-size:12px;font-weight:500;padding:1px 7px;border-radius:20px;white-space:nowrap}
.pill.done{color:var(--done);background:var(--done-bg)}
.pill.scaffold{color:var(--scaffold);background:var(--scaffold-bg)}
.pill.stub{color:var(--stub);background:var(--stub-bg)}
.pill.ref{color:var(--ref);background:var(--ref-bg)}

main :is(h1,h2,h3,h4){font-family:var(--sans);color:var(--ink);line-height:1.22;
  text-wrap:balance;font-weight:600}
h1.section{font-size:26px;letter-spacing:-.01em;margin:56px 0 6px;padding-top:22px;
  border-top:2px solid var(--rule-strong);display:flex;flex-wrap:wrap;align-items:baseline;gap:12px}
h1.section:first-of-type{border-top:none;margin-top:6px}
h2.section-appendix{font-size:22px;margin:52px 0 6px;padding-top:22px;
  border-top:2px solid var(--rule-strong);display:flex;flex-wrap:wrap;align-items:baseline;gap:12px}
main h2{font-size:19px;margin:34px 0 8px}
main h3{font-size:16px;margin:26px 0 6px;color:var(--accent-deep)}
main h4{font-size:14px;font-family:var(--sans);text-transform:uppercase;letter-spacing:.05em;
  color:var(--muted);margin:20px 0 6px}
main p{margin:.7em 0;max-width:70ch}
main li{max-width:70ch}
main ul,main ol{padding-left:22px}
main li{margin:.28em 0}
main hr{border:none;border-top:1px solid var(--rule);margin:28px 0}
strong{color:var(--ink);font-weight:600}
em{color:var(--body)}

.badge{font-family:var(--sans);font-size:12px;font-weight:600;letter-spacing:.02em;
  padding:3px 10px;border-radius:20px;line-height:1.4;white-space:nowrap;position:relative;top:-2px}
.badge.done{color:var(--done);background:var(--done-bg);border:1px solid #cfe0b0}
.badge.scaffold{color:var(--scaffold);background:var(--scaffold-bg);border:1px solid #ecd8a6}
.badge.stub{color:var(--stub);background:var(--stub-bg);border:1px solid #d5d0f2}
.badge.ref{color:var(--ref);background:var(--ref-bg);border:1px solid #c4ddf6}

blockquote{margin:16px 0;padding:12px 18px;background:var(--accent-soft);
  border-radius:8px;border:none;color:var(--accent-deep);font-family:var(--sans);font-size:15px;line-height:1.55}
blockquote p{margin:.35em 0;max-width:none}
blockquote strong{color:var(--accent-deep)}

code{font-family:var(--mono);font-size:.84em;background:var(--panel);
  padding:1.5px 5px;border-radius:4px;color:#2f3d39;word-break:break-word}
pre{background:#0f1614;color:#dbe7e2;padding:14px 16px;border-radius:10px;overflow-x:auto;
  font-size:13px;line-height:1.5}
pre code{background:none;color:inherit;padding:0;font-size:13px}

.tablewrap{overflow-x:auto;margin:16px 0;border:1px solid var(--rule);border-radius:10px}
table{border-collapse:collapse;width:100%;font-family:var(--sans);font-size:13.5px;
  font-variant-numeric:tabular-nums}
th,td{text-align:left;padding:8px 12px;border-bottom:1px solid var(--rule);vertical-align:top}
thead th{background:var(--panel);color:var(--ink);font-weight:600;white-space:nowrap;
  position:sticky;top:0}
tbody tr:last-child td{border-bottom:none}
tbody tr:hover{background:#f3f7f5}
td code{font-size:12px}

figure.diagram{margin:22px 0 26px}
.diagram-frame{background:var(--surface);border:1px solid var(--rule);border-radius:12px;
  padding:18px 20px}
.diagram-frame svg{display:block;width:100%;height:auto}
figure.diagram figcaption{font-family:var(--sans);font-size:12.5px;color:var(--muted);
  margin-top:10px;text-align:center;text-wrap:balance}

.totop{position:fixed;right:20px;bottom:20px;font-family:var(--sans);font-size:12px;
  background:var(--ink);color:#fff;padding:8px 12px;border-radius:20px;opacity:.82}
.totop:hover{opacity:1;text-decoration:none;color:#fff}

@media (max-width:900px){
  .wrap{grid-template-columns:1fr;gap:0}
  .rail{position:static;max-height:none;overflow:visible;padding:8px 0 4px;
    border-bottom:1px solid var(--rule);margin-bottom:8px}
  .rail .toc{max-height:280px;overflow:auto}
  main{padding-top:18px}
  .legend{margin-left:0}
}
@media print{.rail,.totop{display:none}.wrap{display:block}.masthead{padding:0 0 12px}}
</style>
"""

DOC = f"""{CSS}
<title>Safe, Governed AI Data Engineering — working paper</title>
<header class="masthead"><div class="inner">
  <p class="eyebrow">Working paper · internal</p>
  <h1 class="title">Safe, Governed AI Data Engineering on Spark</h1>
  <p class="sub">A four-part working paper — the imperative-vs-SDP safety study, the control boundary,
    the open governed platform, and fleet orchestration.</p>
  <div class="metarow">
    <span class="k">Updated 2026-07-07</span><span>·</span>
    <span class="k">git {git_short()}</span><span>·</span>
    <span>4 sections + 2 reference appendices</span>
    <span class="legend">
      <span><span class="swatch solid"></span>demonstrated</span>
      <span><span class="swatch dash"></span>configured, unrun</span>
      <span><span class="swatch dot"></span>frontier</span>
    </span>
  </div>
</div></header>

<div class="wrap">
  <aside class="rail"><h2>Contents</h2>{toc_html}</aside>
  <main>
    <div class="reading-map">
      <b>How to read this</b>
      <p>Start with <b style="display:inline;text-transform:none;letter-spacing:0;font-size:inherit">Section&nbsp;1</b> — it is the finished, powered study; that is where the evidence lives.
         Maturity is flagged on every section header and in the contents rail:</p>
      <p><span class="pill done">Section 1 — complete</span> &nbsp;
         <span class="pill done">Section 2 — demonstrated to L3</span> &nbsp;
         <span class="pill scaffold">Section 3 — scaffold</span> &nbsp;
         <span class="pill stub">Section 4 — stub</span> &nbsp;
         <span class="pill ref">Appendices — reference specs</span></p>
      <p>Read §1→§2 as results, §3 as a locked north-star with honest gaps, §4 as a thesis. The two
         appendices are executable reference targets — skim unless you are building against them.</p>
    </div>
    {body}
  </main>
</div>
<a class="totop" href="#top">↑ top</a>
"""

# --standalone wraps the fragment in a full HTML document (for GitHub Pages);
# a bare path arg overrides the output location.
if "--standalone" in sys.argv:
    DOC = ('<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n'
           '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
           '<title>Safe, Governed AI Data Engineering on Spark</title>\n'
           '</head>\n<body>\n' + DOC + '\n</body>\n</html>\n')

paths = [a for a in sys.argv[1:] if not a.startswith("--")]
OUTP = Path(paths[0]).expanduser() if paths else OUT
OUTP.parent.mkdir(parents=True, exist_ok=True)
OUTP.write_text(DOC, encoding="utf-8")
print(f"wrote {OUTP}  ({len(DOC):,} bytes, {len(toc_items)} toc entries"
      f"{', standalone' if '--standalone' in sys.argv else ''})")
