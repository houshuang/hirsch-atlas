#!/usr/bin/env python3
"""Build the Hirsch Argument Atlas corpus site from corpus_consolidated.json.

Generates a static site covering all 10 books (1977–2024) with:
- Corpus overview landing page with timeline
- Argument evolution page (cross-book clusters)
- Per-book summary pages
- Hash-routed claim detail page
- Multi-book entity pages (thinkers, concepts, cases)

Usage:
    .venv-otak/bin/python3 prototypes/hirsch/build_corpus_site.py
"""

import json
import html
from pathlib import Path

BASE = Path(__file__).parent
DATA_FILE = BASE / "data" / "corpus_consolidated.json"
EXTERNAL_FILE = BASE / "data" / "external_research.json"
SITE = BASE / "site"
SITE.mkdir(exist_ok=True)
(SITE / "books").mkdir(exist_ok=True)

with open(DATA_FILE) as f:
    D = json.load(f)

# External research (separate provenance — never mixed into D)
EXT = None
if EXTERNAL_FILE.exists():
    with open(EXTERNAL_FILE) as f:
        EXT = json.load(f)
    print(f"Loaded external research: {EXT['stats']['matched_to_topics']} findings")

# ── Book metadata ────────────────────────────────────────────────

BOOKS = {b["slug"]: b for b in D["books_processed"]}
BOOKS_ORDERED = D["books_processed"]  # already sorted by year

BOOK_COLORS = {
    "poc": "#7c6a3e", "cl": "#8b6914", "swn": "#6b5e4f", "kd": "#4a6080",
    "the-making-of-americans": "#2d6a2e", "wkm": "#9a2e2e",
    "how-to-educate-a-citizen": "#6b4a8a", "ae": "#2e6a6a",
    "sk": "#8a4a2e", "re": "#3e5e7c",
}

EVO_COLORS = {
    "repeated": "#6b6b6b", "refined": "#4a6080", "evolved": "#8b6914",
    "new_evidence": "#2d6a2e", "narrowed": "#9a2e2e", "broadened": "#6b4a8a",
}

EVO_LABELS = {
    "repeated": "Repeated", "refined": "Refined", "evolved": "Evolved",
    "new_evidence": "New Evidence", "narrowed": "Narrowed", "broadened": "Broadened",
}

# ── Indexes ──────────────────────────────────────────────────────

claims_by_id = {c["_global_id"]: c for c in D["all_claims"]}

evidence_by_claim = {}
for ev in D["all_evidence"]:
    cid = ev.get("supports_claim_global", "")
    if cid:
        evidence_by_claim.setdefault(cid, []).append(ev)

warrants_by_claim = {}
for w in D["all_warrants"]:
    cid = w.get("claim_id_global", "")
    if cid:
        warrants_by_claim.setdefault(cid, []).append(w)

counter_args_by_claim = {}
for ca in D["all_counter_arguments"]:
    cid = ca.get("targets_claim_global", "")
    if cid:
        counter_args_by_claim.setdefault(cid, []).append(ca)

objections_by_claim = {}
for obj in D["all_objections_raised"]:
    cid = obj.get("targets_claim_global", "")
    if cid:
        objections_by_claim.setdefault(cid, []).append(obj)

deps_from = {}
deps_to = {}
for d in D["all_dependencies"]:
    fid = d.get("from_id_global", "")
    tid = d.get("to_id_global", "")
    if fid:
        deps_from.setdefault(fid, []).append(d)
    if tid:
        deps_to.setdefault(tid, []).append(d)

missing_steps_for = {}
for ms in D["all_missing_steps"]:
    fid = ms.get("from_id_global", "")
    tid = ms.get("to_id_global", "")
    if fid:
        missing_steps_for.setdefault(fid, []).append(ms)
    if tid and tid != fid:
        missing_steps_for.setdefault(tid, []).append(ms)

chains_by_book_chapter = {}
for ac in D["all_argument_chains"]:
    key = (ac.get("book", ""), ac.get("chapter", ""))
    chains_by_book_chapter.setdefault(key, []).append(ac)

claims_by_book = {}
for c in D["all_claims"]:
    claims_by_book.setdefault(c.get("book", ""), []).append(c)

claims_by_book_chapter = {}
for c in D["all_claims"]:
    key = (c.get("book", ""), c.get("chapter", ""))
    claims_by_book_chapter.setdefault(key, []).append(c)

cluster_for_claim = {}
for cl in D.get("claim_clusters", []):
    for m in cl["members"]:
        cluster_for_claim[m["claim_id"]] = cl
for cl in D.get("cross_book_clusters", []):
    for m in cl["members"]:
        cluster_for_claim[m["claim_id"]] = cl

# ── Importance scoring ───────────────────────────────────────────
# Composite score combining cross-book recurrence + graph structure

from collections import Counter

# Build score components
_in_degree = Counter()
for d in D["all_dependencies"]:
    tid = d.get("to_id_global", "")
    if tid:
        _in_degree[tid] += 1

_evidence_count = Counter()
for ev in D["all_evidence"]:
    cid = ev.get("supports_claim_global", "")
    if cid:
        _evidence_count[cid] += 1

_counter_count = Counter()
for ca in D["all_counter_arguments"]:
    cid = ca.get("targets_claim_global", "")
    if cid:
        _counter_count[cid] += 1

# Cross-book recurrence: how many books does this claim's cluster span?
_cross_book_score = {}
for c in D["all_claims"]:
    gid = c["_global_id"]
    cl = cluster_for_claim.get(gid)
    _cross_book_score[gid] = cl.get("book_count", 1) if cl else 1

# Normalize and compute composite
_max = lambda d, default=1: max(d.values()) if d else default
_m_in = _max(_in_degree)
_m_ev = _max(_evidence_count)
_m_ca = _max(_counter_count)
_m_bk = max(_cross_book_score.values()) if _cross_book_score else 1

importance_scores = {}
for c in D["all_claims"]:
    gid = c["_global_id"]
    bk = (_cross_book_score.get(gid, 1) - 1) / max(_m_bk - 1, 1)  # 0 for single-book, 1 for max-book
    ind = _in_degree.get(gid, 0) / _m_in
    ev = _evidence_count.get(gid, 0) / _m_ev
    ca = _counter_count.get(gid, 0) / _m_ca
    mc = 0.3 if c.get("is_main_conclusion") else 0.0

    # Weights: cross-book recurrence is the strongest signal
    score = 0.35 * bk + 0.20 * ind + 0.15 * ev + 0.10 * ca + mc
    importance_scores[gid] = round(score, 4)

# Top claims by importance
TOP_CLAIMS = sorted(
    D["all_claims"],
    key=lambda c: importance_scores.get(c["_global_id"], 0),
    reverse=True
)[:50]

# ── Helpers ──────────────────────────────────────────────────────

def e(text):
    return html.escape(str(text)) if text else ""

def book_label(slug):
    b = BOOKS.get(slug, {})
    return f'{b.get("short", slug)} ({b.get("year", "?")})'

def badge(text, color=None):
    style = f' style="background:{color};color:#fff"' if color else ""
    return f'<span class="badge"{style}>{e(text)}</span>'

def evo_badge(evo_type):
    color = EVO_COLORS.get(evo_type, "#6b6b6b")
    label = EVO_LABELS.get(evo_type, evo_type or "unclassified")
    return f'<span class="evo-badge" style="background:{color}">{e(label)}</span>'

def book_dot(slug, active=True):
    b = BOOKS.get(slug, {})
    color = BOOK_COLORS.get(slug, "#6b6b6b")
    opacity = "1" if active else "0.15"
    year = str(b.get("year", "?"))
    return f'<span class="book-dot" style="background:{color};opacity:{opacity}" title="{e(b.get("short", slug))} ({year})">{year[2:]}</span>'

def timeline_dots(active_slugs):
    dots = []
    for b in BOOKS_ORDERED:
        dots.append(book_dot(b["slug"], b["slug"] in active_slugs))
    return '<span class="timeline-dots">' + "".join(dots) + '</span>'


# ── HTML scaffolding ─────────────────────────────────────────────

CORPUS_CSS = """
/* ── Evolution & Timeline ── */
.evo-badge { display: inline-block; padding: 0.15em 0.5em; border-radius: 3px; color: #fff;
  font-family: var(--font-ui); font-size: 0.72rem; font-weight: 600; letter-spacing: 0.02em; }
.timeline-dots { display: inline-flex; gap: 2px; align-items: center; }
.book-dot { display: inline-flex; align-items: center; justify-content: center; width: 22px; height: 22px;
  border-radius: 50%; font-family: var(--font-ui); font-size: 0.55rem; font-weight: 700;
  color: #fff; transition: transform 0.15s; cursor: default; flex-shrink: 0; }
.book-dot:hover { transform: scale(1.3); z-index: 1; }

.evo-card { background: var(--bg-surface); border: 1px solid var(--border); border-radius: 6px;
  padding: 1em 1.2em; margin-bottom: 0.8em; }
.evo-card-header { display: flex; align-items: center; gap: 0.6em; flex-wrap: wrap; margin-bottom: 0.4em; }
.evo-card-claim { font-family: var(--font-body); font-size: 0.92rem; line-height: 1.5; color: var(--text); }
.evo-card-summary { font-family: var(--font-ui); font-size: 0.8rem; color: var(--text-secondary);
  margin-top: 0.3em; font-style: italic; }
.evo-card-members { margin-top: 0.5em; padding-left: 1em; border-left: 2px solid var(--border-light); }
.evo-card-member { font-family: var(--font-ui); font-size: 0.78rem; color: var(--text-secondary);
  padding: 0.15em 0; line-height: 1.4; }
.evo-card-member .member-book { font-weight: 600; color: var(--text); min-width: 4em; display: inline-block; }
.evo-card details summary { cursor: pointer; font-family: var(--font-ui); font-size: 0.78rem;
  color: var(--text-tertiary); }

/* ── Book timeline grid ── */
.book-timeline { display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
  gap: 0.8em; margin: 1.5em 0; }
.book-tile { background: var(--bg-surface); border: 1px solid var(--border); border-radius: 6px;
  padding: 0.8em; text-align: center; text-decoration: none; color: var(--text); transition: border-color 0.15s; }
.book-tile:hover { border-color: var(--accent); }
.book-tile-year { font-family: var(--font-ui); font-size: 1.4rem; font-weight: 700; color: var(--accent); }
.book-tile-title { font-family: var(--font-ui); font-size: 0.75rem; color: var(--text-secondary);
  margin-top: 0.2em; line-height: 1.3; }
.book-tile-stats { font-family: var(--font-ui); font-size: 0.68rem; color: var(--text-tertiary);
  margin-top: 0.4em; }

/* ── Book page ── */
.book-header { margin-bottom: 1.5em; }
.book-header h1 { font-size: 1.6rem; }
.book-header .book-year { font-family: var(--font-ui); font-size: 1.1rem; color: var(--accent); font-weight: 600; }
.chapter-grid { display: grid; grid-template-columns: 1fr; gap: 0.6em; }

/* ── Filter bar ── */
.filter-bar { display: flex; gap: 0.4em; flex-wrap: wrap; margin: 1em 0; }
.filter-btn { font-family: var(--font-ui); font-size: 0.75rem; padding: 0.3em 0.7em;
  border: 1px solid var(--border); border-radius: 3px; background: var(--bg-surface);
  color: var(--text-secondary); cursor: pointer; transition: all 0.15s; }
.filter-btn:hover, .filter-btn.active { background: var(--accent); color: #fff; border-color: var(--accent); }

/* ── Corpus stats bar ── */
.corpus-stats { display: flex; flex-wrap: wrap; gap: 1.5em; margin: 1em 0; padding: 1em;
  background: var(--bg-aside); border-radius: 6px; }
.corpus-stat { text-align: center; }
.corpus-stat-number { font-family: var(--font-ui); font-size: 1.6rem; font-weight: 700; color: var(--accent); display: block; }
.corpus-stat-label { font-family: var(--font-ui); font-size: 0.7rem; color: var(--text-tertiary); }

/* ── Entity cards (multi-book) ── */
.entity-books { display: flex; gap: 3px; margin-top: 0.3em; flex-wrap: wrap; }
.entity-book-tag { font-family: var(--font-ui); font-size: 0.62rem; padding: 0.1em 0.35em;
  border-radius: 2px; background: var(--accent-light); color: var(--accent); font-weight: 600; }

/* ── External research (blue/cool layer) ── */
.ext-card { background: var(--bg-surface); border: 1px solid #c4d4e4; border-left: 3px solid #4a6080;
  border-radius: 6px; padding: 0.8em 1em; margin-bottom: 0.6em; }
.ext-card-text { font-size: 0.85rem; color: var(--text); line-height: 1.5; }
.ext-card-meta { display: flex; gap: 0.5em; flex-wrap: wrap; margin-top: 0.3em; align-items: center; }
.ext-badge { display: inline-block; padding: 0.1em 0.4em; border-radius: 2px;
  font-family: var(--font-ui); font-size: 0.65rem; font-weight: 600; }
.ext-badge-type { background: #eef2f7; color: #4a6080; }
.ext-badge-strength { background: #e8f5e8; color: #2d6a2e; }
.ext-badge-critique { background: #fdf0f0; color: #9a2e2e; }
.ext-provenance { font-family: var(--font-ui); font-size: 0.68rem; color: var(--text-tertiary);
  font-style: italic; padding: 0.5em 0; border-top: 1px solid var(--border-light); margin-top: 0.8em; }

/* ── Entry paths (landing page) ── */
.entry-paths { display: grid; grid-template-columns: 1fr 1fr; gap: 1.2em; margin: 1.5em 0; }
@media (max-width: 40em) { .entry-paths { grid-template-columns: 1fr; } }
.entry-path { display: block; padding: 1.5em; border-radius: 8px; text-decoration: none;
  transition: transform 0.15s, box-shadow 0.15s; }
.entry-path:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0,0,0,0.08); }
.entry-path-hirsch { background: var(--accent-light); border: 2px solid var(--accent); color: var(--text); }
.entry-path-context { background: #eef2f7; border: 2px solid #4a6080; color: var(--text); }
.entry-path h2 { font-family: var(--font-ui); font-size: 1.1rem; margin-bottom: 0.3em; }
.entry-path-hirsch h2 { color: var(--accent); }
.entry-path-context h2 { color: #4a6080; }
.entry-path p { font-size: 0.85rem; color: var(--text-secondary); margin: 0; }
.entry-path .entry-stat { font-family: var(--font-ui); font-size: 0.75rem; color: var(--text-tertiary); margin-top: 0.5em; }

/* ── Topic section ── */
.topic-section { margin-bottom: 2em; }
.topic-header { display: flex; align-items: baseline; gap: 0.8em; margin-bottom: 0.5em; }
.topic-header h2 { font-size: 1.1rem; margin: 0; }
.topic-split { display: grid; grid-template-columns: 1fr 1fr; gap: 1.5em; }
@media (max-width: 50em) { .topic-split { grid-template-columns: 1fr; } }
.topic-col-label { font-family: var(--font-ui); font-size: 0.72rem; font-weight: 600;
  text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.5em; padding-bottom: 0.3em;
  border-bottom: 2px solid; }
.topic-col-hirsch .topic-col-label { color: var(--accent); border-color: var(--accent); }
.topic-col-ext .topic-col-label { color: #4a6080; border-color: #4a6080; }

/* ── Sidebar book list ── */
.sidebar-book { font-size: 0.78rem; }
.sidebar-book-year { font-weight: 600; min-width: 2.5em; display: inline-block; }
"""

def page_header(breadcrumbs=None, css_path="style.css"):
    bc = ""
    if breadcrumbs:
        parts = []
        for label, url in breadcrumbs:
            if url:
                parts.append(f'<a href="{url}">{e(label)}</a>')
            else:
                parts.append(e(label))
        bc = '<span class="sep">/</span>'.join(parts)

    root = css_path.replace("style.css", "")
    ctx_link = f'<a href="{root}context.html" style="color:#4a6080">Scholarly Context</a>' if EXT else ""
    return f'''<header class="page-header">
  <div class="header-inner">
    <div class="site-title"><a href="{root}index.html">Hirsch Argument Atlas</a></div>
    <nav class="header-nav">
      <a href="{root}evolution.html">Evolution</a>
      <a href="{root}debates.html" style="color:var(--counter)">Debates</a>
      <a href="{root}thinkers.html">Thinkers</a>
      <a href="{root}concepts.html">Concepts</a>
      <a href="{root}cases.html">Cases</a>
      {ctx_link}
    </nav>
    {f'<nav class="breadcrumb">{bc}</nav>' if bc else ""}
  </div>
</header>'''

def page_footer():
    return f'''<footer class="page-footer">
  Hirsch Argument Atlas &mdash; 10 books (1977&ndash;2024), {D["stats"]["claims"]:,} claims extracted and analyzed
  &middot; Built with care, not with frameworks
</footer>'''

def book_sidebar(current_book=None, depth=0):
    prefix = "../" * depth
    links = []
    for b in BOOKS_ORDERED:
        active = ' class="sidebar-link active"' if b["slug"] == current_book else ' class="sidebar-link"'
        links.append(f'<a{active} href="{prefix}books/{b["slug"]}.html">'
                     f'<span class="sidebar-book-year">{b["year"]}</span> '
                     f'{e(b["short"])} '
                     f'<span class="sidebar-count">{b["claims"]}</span></a>')

    return f'''<aside class="sidebar">
  <div class="sidebar-section">
    <div class="sidebar-heading">Books</div>
    {"".join(links)}
  </div>
  <div class="sidebar-section">
    <div class="sidebar-heading">Explore</div>
    <a class="sidebar-link" href="{prefix}evolution.html">Argument Evolution <span class="sidebar-count">{len(D["cross_book_clusters"])}</span></a>
    <a class="sidebar-link" href="{prefix}thinkers.html">Thinkers <span class="sidebar-count">{D["stats"]["canonical_thinkers"]}</span></a>
    <a class="sidebar-link" href="{prefix}concepts.html">Concepts <span class="sidebar-count">{D["stats"]["canonical_concepts"]}</span></a>
    <a class="sidebar-link" href="{prefix}cases.html">Cases <span class="sidebar-count">{D["stats"]["canonical_cases"]}</span></a>
  </div>
</aside>'''

def html_page(title, body, css_path="style.css", extra_head=""):
    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{e(title)} &mdash; Hirsch Argument Atlas</title>
<link rel="stylesheet" href="{css_path}">
<style>{CORPUS_CSS}</style>
{extra_head}
</head>
<body>
{body}
</body>
</html>'''


# ═══════════════════════════════════════════════════════════════════
# PAGE: Corpus Landing
# ═══════════════════════════════════════════════════════════════════

def build_landing():
    s = D["stats"]
    ext_count = EXT["stats"]["matched_to_topics"] if EXT else 0

    # Book timeline grid
    book_tiles = ""
    for b in BOOKS_ORDERED:
        book_tiles += f'''<a class="book-tile" href="books/{b["slug"]}.html">
  <div class="book-tile-year">{b["year"]}</div>
  <div class="book-tile-title">{e(b["title"])}</div>
  <div class="book-tile-stats">{b["chapters"]} ch &middot; {b["claims"]:,} claims</div>
</a>\n'''

    # Top cross-book arguments (5+ books)
    top_args = [c for c in D["cross_book_clusters"] if c["book_count"] >= 5]
    top_html = ""
    for cl in top_args[:6]:
        top_html += f'''<div class="evo-card">
  <div class="evo-card-header">
    {timeline_dots(cl["books"])}
    {evo_badge(cl.get("evolution_type", ""))}
  </div>
  <div class="evo-card-claim"><a href="claim.html#{e(cl['canonical_claim']['claim_id'])}" style="color:inherit;text-decoration:none;border-bottom:1px solid var(--border-light)">{e(cl["canonical_claim"]["text"])}</a></div>
  {f'<div class="evo-card-summary">{e(cl.get("evolution_summary", ""))}</div>' if cl.get("evolution_summary") else ""}
</div>\n'''

    # Context entry path (only if external data loaded)
    context_path = ""
    if EXT:
        context_path = f'''<a class="entry-path entry-path-context" href="context.html">
  <h2>Scholarly Context</h2>
  <p>External research that supports, challenges, or extends Hirsch's arguments &mdash;
  from RCTs to international comparisons to critiques.</p>
  <div class="entry-stat">{ext_count} findings &middot; {EXT["stats"]["evidence_types"].get("Randomized Trial", 0)} RCTs &middot; {EXT["stats"]["evidence_types"].get("Critique / Debate", 0)} critiques</div>
</a>'''

    body = f'''{page_header()}

<div class="hero">
  <h1>The Hirsch Argument Atlas</h1>
  <div class="subtitle">E.D. Hirsch Jr. &mdash; 10 books, 47 years, one argument</div>
  <p class="thesis">
    This atlas maps the complete argument of E.D. Hirsch Jr. across 10 books spanning 1977 to 2024.
    Every claim is extracted, every piece of evidence tracked, and every counter-argument surfaced.
    A separate layer of external scholarly research provides independent context.
  </p>
</div>

<div class="main-grid">
<main class="content">

  <div class="entry-paths">
    <a class="entry-path entry-path-hirsch" href="evolution.html">
      <h2>Hirsch's Argument</h2>
      <p>How one author's core thesis &mdash; that shared knowledge is the foundation of literacy &mdash;
      evolved across ten books and five decades.</p>
      <div class="entry-stat">{s["claims"]:,} claims &middot; {len(D["cross_book_clusters"])} cross-book arguments &middot; {s["evidence"]:,} evidence items</div>
    </a>
    {context_path}
  </div>

  <div class="section">
    <div class="section-label">The Books</div>
    <div class="book-timeline">{book_tiles}</div>
  </div>

  <hr class="section-break">

  <div class="section">
    <div class="section-label">Core Arguments (5+ books)</div>
    <p>Arguments that appear in five or more books &mdash; the intellectual constants.
    <a href="evolution.html">See all {len(D["cross_book_clusters"])} &rarr;</a></p>
    {top_html}
  </div>

</main>
{book_sidebar(depth=0)}
</div>

{page_footer()}'''

    return html_page("Complete Works (1977–2024)", body)


# ═══════════════════════════════════════════════════════════════════
# PAGE: Argument Evolution
# ═══════════════════════════════════════════════════════════════════

def _evo_card(cl):
    """Render a single evolution card."""
    etype = cl.get("evolution_type", "none")
    canonical = cl["canonical_claim"]["text"]
    members_html = ""
    for m in cl["members"]:
        b = BOOKS.get(m["book"], {})
        short = b.get("short", m["book"])
        text = m["text"]
        if len(text) > 120:
            text = text[:117] + "..."
        members_html += f'<div class="evo-card-member"><span class="member-book">{e(short)}</span> <a href="claim.html#{e(m["claim_id"])}" style="color:var(--text-secondary)">{e(text)}</a></div>\n'

    show_details = len(cl["members"]) > 2
    if show_details:
        members_block = f'<details><summary>{cl["size"]} variants across {cl["book_count"]} books</summary><div class="evo-card-members">{members_html}</div></details>'
    else:
        members_block = f'<div class="evo-card-members">{members_html}</div>'

    return f'''<div class="evo-card" data-evo="{etype}">
  <div class="evo-card-header">
    {timeline_dots(cl["books"])}
    {evo_badge(etype)}
    <span style="font-family:var(--font-ui);font-size:0.72rem;color:var(--text-tertiary)">{cl["size"]} variants</span>
  </div>
  <div class="evo-card-claim"><a href="claim.html#{e(cl['canonical_claim']['claim_id'])}" style="color:inherit;text-decoration:none;border-bottom:1px solid var(--border-light)">{e(canonical)}</a></div>
  {f'<div class="evo-card-summary">{e(cl.get("evolution_summary", ""))}</div>' if cl.get("evolution_summary") else ""}
  {members_block}
</div>\n'''


def build_evolution():
    clusters = D["cross_book_clusters"]

    from collections import Counter
    evo_counts = Counter(c.get("evolution_type", "none") for c in clusters)

    # Filter bar
    filter_btns = '<button class="filter-btn active" data-filter="all">All ({0})</button>'.format(len(clusters))
    for etype in ["repeated", "refined", "evolved", "broadened", "new_evidence", "narrowed"]:
        cnt = evo_counts.get(etype, 0)
        if cnt > 0:
            color = EVO_COLORS.get(etype, "#6b6b6b")
            filter_btns += f'<button class="filter-btn" data-filter="{etype}" style="border-color:{color}">{EVO_LABELS[etype]} ({cnt})</button>'

    # Group by theme (from external research topic tagging)
    THEME_ORDER = [
        ("reading_knowledge", "Reading Is Knowledge", "Whether reading comprehension depends on domain-specific background knowledge"),
        ("curriculum_content", "Curriculum & Content", "What should be taught — shared knowledge, specific content, curriculum design"),
        ("testing_assessment", "Testing & Assessment", "How knowledge is measured, test score trends"),
        ("achievement_gap", "Achievement Gaps & Equity", "Whether knowledge-based curricula narrow or widen gaps"),
        ("progressive_critique", "Progressive Education Critique", "Child-centered, constructivist, and skills-based approaches"),
        ("international_comparison", "International Comparisons", "Cross-national evidence from France, Japan, Finland, etc."),
        ("cognitive_science", "Cognitive Science", "Evidence from cognitive psychology about learning and transfer"),
        ("teacher_pedagogy", "Teaching & Pedagogy", "How to teach knowledge effectively"),
    ]

    # Build cluster→topic mapping
    cluster_topics = {}
    if EXT:
        for cid_str, topics in EXT.get("cluster_topics", {}).items():
            try:
                cluster_topics[int(cid_str)] = topics
            except ValueError:
                pass

    by_theme = {tid: [] for tid, _, _ in THEME_ORDER}
    by_theme["other"] = []
    assigned = set()

    for cl in clusters:
        topics = cluster_topics.get(cl["cluster_id"], [])
        if topics:
            primary = topics[0]
            if primary in by_theme:
                by_theme[primary].append(cl)
                assigned.add(cl["cluster_id"])
            else:
                by_theme["other"].append(cl)
                assigned.add(cl["cluster_id"])
        # Unassigned clusters go to "other"

    for cl in clusters:
        if cl["cluster_id"] not in assigned:
            by_theme["other"].append(cl)

    # Build sections
    sections_html = ""
    for tid, label, desc in THEME_ORDER:
        theme_clusters = by_theme.get(tid, [])
        if not theme_clusters:
            continue
        # Sort by book_count desc
        theme_clusters.sort(key=lambda x: (-x["book_count"], -x["size"]))

        shown = theme_clusters[:5]
        hidden = theme_clusters[5:]

        cards = "".join(_evo_card(cl) for cl in shown)
        hidden_cards = ""
        if hidden:
            hidden_cards = f'''<details>
  <summary style="font-family:var(--font-ui);font-size:0.78rem;color:var(--text-tertiary);cursor:pointer;margin-top:0.5em">+ {len(hidden)} more arguments in this theme</summary>
  {"".join(_evo_card(cl) for cl in hidden)}
</details>'''

        sections_html += f'''<div class="topic-section" id="evo-{tid}">
  <div class="section-label" style="margin-top:1.5em">{e(label)} ({len(theme_clusters)})</div>
  <p style="font-size:0.82rem;color:var(--text-secondary);margin-bottom:0.8em">{e(desc)}</p>
  {cards}
  {hidden_cards}
</div>\n'''

    # "Other" section
    other = by_theme["other"]
    if other:
        other.sort(key=lambda x: (-x["book_count"], -x["size"]))
        shown = other[:5]
        hidden = other[5:]
        cards = "".join(_evo_card(cl) for cl in shown)
        hidden_cards = ""
        if hidden:
            hidden_cards = f'<details><summary style="font-family:var(--font-ui);font-size:0.78rem;color:var(--text-tertiary);cursor:pointer;margin-top:0.5em">+ {len(hidden)} more</summary>{"".join(_evo_card(cl) for cl in hidden)}</details>'
        sections_html += f'''<div class="topic-section">
  <div class="section-label" style="margin-top:1.5em">Other Arguments ({len(other)})</div>
  {cards}
  {hidden_cards}
</div>\n'''

    filter_script = '''<script>
document.querySelectorAll('.filter-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    const filter = btn.dataset.filter;
    document.querySelectorAll('.evo-card').forEach(card => {
      if (filter === 'all' || card.dataset.evo === filter) {
        card.style.display = '';
      } else {
        card.style.display = 'none';
      }
    });
  });
});
</script>'''

    body = f'''{page_header(breadcrumbs=[("Atlas", "index.html"), ("Argument Evolution", None)])}

<div class="main-grid">
<main class="content">

  <h1>Argument Evolution</h1>
  <p class="lead">How Hirsch's arguments changed &mdash; or didn't &mdash; across 10 books and 47 years.
  Each card shows an argument that appears in multiple books, with a timeline showing which books contain it
  and an AI-classified evolution type.</p>

  <div class="corpus-stats" style="margin-bottom: 0.5em;">
    <div class="corpus-stat"><span class="corpus-stat-number">{len(clusters)}</span><span class="corpus-stat-label">Cross-Book Arguments</span></div>
    <div class="corpus-stat"><span class="corpus-stat-number">{evo_counts.get("repeated", 0)}</span><span class="corpus-stat-label">Repeated</span></div>
    <div class="corpus-stat"><span class="corpus-stat-number">{evo_counts.get("refined", 0)}</span><span class="corpus-stat-label">Refined</span></div>
    <div class="corpus-stat"><span class="corpus-stat-number">{evo_counts.get("evolved", 0)}</span><span class="corpus-stat-label">Evolved</span></div>
    <div class="corpus-stat"><span class="corpus-stat-number">{evo_counts.get("broadened", 0)}</span><span class="corpus-stat-label">Broadened</span></div>
    <div class="corpus-stat"><span class="corpus-stat-number">{evo_counts.get("new_evidence", 0)}</span><span class="corpus-stat-label">New Evidence</span></div>
  </div>

  <div class="filter-bar">{filter_btns}</div>

  {sections_html}

</main>
{book_sidebar(depth=0)}
</div>

{page_footer()}
{filter_script}'''

    return html_page("Argument Evolution", body)


# ═══════════════════════════════════════════════════════════════════
# PAGE: Per-Book Summary
# ═══════════════════════════════════════════════════════════════════

def build_book_page(slug):
    b = BOOKS[slug]
    book_claims = claims_by_book.get(slug, [])
    book_evidence = [ev for ev in D["all_evidence"] if ev.get("book") == slug]
    book_counters = [ca for ca in D["all_counter_arguments"] if ca.get("book") == slug]
    book_chains = [ac for ac in D["all_argument_chains"] if ac.get("book") == slug]
    summaries = D["book_chapter_summaries"].get(slug, {})

    # Chapter list
    chapters = sorted(set(c["chapter"] for c in book_claims),
                      key=lambda ch: (-1 if ch == "prologue" else (100 if ch.startswith("e") else
                                      (200 if ch.startswith("a") else (int(ch) if ch.isdigit() else 999)))))

    chapter_html = ""
    for ch in chapters:
        ch_claims = claims_by_book_chapter.get((slug, ch), [])
        ch_evidence = [ev for ev in book_evidence if ev.get("chapter") == ch]
        mc = [c for c in ch_claims if c.get("is_main_conclusion")]
        summary = summaries.get(ch, "")
        if len(summary) > 200:
            summary = summary[:197] + "..."

        ch_label = ch.replace("-", " ").title() if not ch.isdigit() else f"Chapter {ch}"

        # Main conclusions preview
        mc_preview = ""
        for c in mc[:3]:
            text = c["text"]
            if len(text) > 100:
                text = text[:97] + "..."
            mc_preview += f'<div style="font-size:0.82rem;padding:0.2em 0;"><a href="../claim.html#{e(c["_global_id"])}">{e(text)}</a></div>'

        ch_slug = ch.replace(" ", "-").lower()
        chapter_html += f'''<div class="flow-card">
  <div class="flow-card-text" style="font-weight:600; font-family:var(--font-ui); font-size:0.85rem;">
    <a href="{slug}/{ch_slug}.html" style="color:inherit;text-decoration:none">{e(ch_label)}</a>
  </div>
  <div style="font-size:0.82rem; color:var(--text-secondary); margin:0.3em 0;">{e(summary)}</div>
  {mc_preview}
  <div class="flow-card-meta">
    <span class="flow-card-chapter">{len(ch_claims)} claims</span>
    <span class="flow-card-chapter">{len(mc)} conclusions</span>
    <span class="flow-card-chapter">{len(ch_evidence)} evidence</span>
    <a href="{slug}/{ch_slug}.html" style="font-family:var(--font-ui);font-size:0.72rem;margin-left:auto">All claims &rarr;</a>
  </div>
</div>\n'''

    # Cross-book clusters involving this book
    book_clusters = [cl for cl in D["cross_book_clusters"] if slug in cl["books"]]
    book_clusters.sort(key=lambda cl: (-cl["book_count"], -cl["size"]))
    cluster_html = ""
    for cl in book_clusters[:10]:
        cluster_html += f'''<div class="evo-card">
  <div class="evo-card-header">
    {timeline_dots(cl["books"])}
    {evo_badge(cl.get("evolution_type", ""))}
  </div>
  <div class="evo-card-claim" style="font-size:0.85rem;"><a href="../claim.html#{e(cl['canonical_claim']['claim_id'])}" style="color:inherit;text-decoration:none;border-bottom:1px solid var(--border-light)">{e(cl["canonical_claim"]["text"][:120])}</a></div>
</div>\n'''

    body = f'''{page_header(
        breadcrumbs=[("Atlas", "../index.html"), (f'{b["short"]} ({b["year"]})', None)],
        css_path="../style.css",
    )}

<div class="main-grid">
<main class="content">

  <div class="book-header">
    <span class="book-year">{b["year"]}</span>
    <h1>{e(b["title"])}</h1>
  </div>

  <div class="stats-bar">
    <div class="stat-item"><span class="stat-number">{len(book_claims)}</span> claims</div>
    <div class="stat-item"><span class="stat-number">{len(book_evidence)}</span> evidence</div>
    <div class="stat-item"><span class="stat-number">{len(book_counters)}</span> counter-args</div>
    <div class="stat-item"><span class="stat-number">{len(book_chains)}</span> arg chains</div>
    <div class="stat-item"><span class="stat-number">{len(chapters)}</span> chapters</div>
  </div>

  <div class="section">
    <div class="section-label">Chapters</div>
    <div class="chapter-grid">{chapter_html}</div>
  </div>

  {f"""<hr class="section-break">
  <div class="section">
    <div class="section-label">Cross-Book Arguments ({len(book_clusters)})</div>
    <p style="font-size:0.85rem;color:var(--text-secondary);">Arguments from this book that also appear in other books:</p>
    {cluster_html}
    {f'<p style="font-size:0.8rem"><a href="../evolution.html">See all cross-book arguments &rarr;</a></p>' if len(book_clusters) > 10 else ""}
  </div>""" if book_clusters else ""}

</main>
{book_sidebar(current_book=slug, depth=1)}
</div>

{page_footer()}'''

    return html_page(f'{b["short"]} ({b["year"]})', body, css_path="../style.css")


# ═══════════════════════════════════════════════════════════════════
# PAGE: Chapter Detail
# ═══════════════════════════════════════════════════════════════════

def build_chapter_page(slug, ch):
    b = BOOKS[slug]
    ch_claims = claims_by_book_chapter.get((slug, ch), [])
    ch_evidence = [ev for ev in D["all_evidence"] if ev.get("book") == slug and ev.get("chapter") == ch]
    ch_counters = [ca for ca in D["all_counter_arguments"] if ca.get("book") == slug and ca.get("chapter") == ch]
    ch_missing = [ms for ms in D["all_missing_steps"] if ms.get("book") == slug and ms.get("chapter") == ch]
    ch_chains = chains_by_book_chapter.get((slug, ch), [])
    summary = D["book_chapter_summaries"].get(slug, {}).get(ch, "")

    ch_label = ch.replace("-", " ").title() if not ch.isdigit() else f"Chapter {ch}"
    main_conclusions = [c for c in ch_claims if c.get("is_main_conclusion")]

    # Track which claims appear in chains (to avoid showing them twice)
    claims_in_chains = set()
    for ac in ch_chains:
        for nid in ac.get("chain_global", []):
            claims_in_chains.add(nid)

    # ── Argument chains as vertical flows (primary content) ──
    # Sort: strong first, then by length
    ch_chains_sorted = sorted(ch_chains, key=lambda ac: (
        0 if ac.get("strength") == "strong" else (1 if ac.get("strength") == "moderate" else 2),
        -len(ac.get("chain_global", []))
    ))

    chains_html = ""
    for ac in ch_chains_sorted:
        strength = ac.get("strength", "moderate")
        strength_class = f"strength-{strength}"
        nodes = ac.get("chain_global", [])
        conclusion_id = ac.get("conclusion_id_global", "")

        chain_nodes = ""
        for i, node_id in enumerate(nodes):
            c = claims_by_id.get(node_id)
            if not c:
                continue
            text = c["text"]
            is_conclusion = (node_id == conclusion_id) or (i == len(nodes) - 1)
            is_main = c.get("is_main_conclusion", False)
            ev_n = len(evidence_by_claim.get(node_id, []))
            ca_n = len(counter_args_by_claim.get(node_id, []))

            node_class = "chain-node-v"
            extra_style = ""
            if is_conclusion:
                extra_style = "border-color:var(--accent);font-weight:600;"
            if is_main:
                extra_style += "border-width:2px;"

            meta = ""
            if ev_n or ca_n:
                bits = []
                if ev_n:
                    bits.append(f'{ev_n} ev')
                if ca_n:
                    bits.append(f'{ca_n} ca')
                meta = f'<span style="font-family:var(--font-ui);font-size:0.65rem;color:var(--text-tertiary);margin-left:0.5em">{" · ".join(bits)}</span>'

            chain_nodes += f'''<a href="../../claim.html#{e(node_id)}" class="{node_class}" style="{extra_style}text-decoration:none;color:inherit;display:block;padding:0.5em 0.7em;border:1px solid var(--border);border-radius:4px;margin:0.15em 0;font-size:0.82rem;line-height:1.4">
  {e(text)}{meta}
</a>\n'''
            if i < len(nodes) - 1:
                chain_nodes += '<div style="text-align:center;color:var(--text-tertiary);font-size:0.7rem;line-height:1">&#8595;</div>\n'

        chains_html += f'''<div class="chain-section" style="margin-bottom:1.5em">
  <div class="chain-title">{e(ac.get("name", ""))} <span class="chain-strength {strength_class}">{e(strength)}</span></div>
  <div style="padding-left:0.5em;border-left:2px solid var(--border-light)">{chain_nodes}</div>
</div>\n'''

    # ── Claims NOT in any chain (collapsed) ──
    unchained = [c for c in ch_claims if c["_global_id"] not in claims_in_chains]
    unchained_html = ""
    if unchained:
        for c in unchained[:30]:
            gid = c["_global_id"]
            unchained_html += f'''<div class="claim-card" style="margin-bottom:0.3em"><a href="../../claim.html#{e(gid)}">
  <div class="claim-card-text" style="font-size:0.82rem">{e(c["text"])}</div>
  <div class="claim-card-meta"><span class="badge badge-{e(c.get('claim_level',''))}">{e(c.get('claim_level',''))}</span></div>
</a></div>\n'''
        if len(unchained) > 30:
            unchained_html += f'<p style="font-size:0.78rem;color:var(--text-tertiary)">+ {len(unchained) - 30} more</p>'

    # ── Missing steps (logical gaps) ──
    critical_gaps = [ms for ms in ch_missing if ms.get("severity") == "critical"]
    other_gaps = [ms for ms in ch_missing if ms.get("severity") != "critical"]
    gaps_html = ""
    for ms in critical_gaps + other_gaps[:5]:
        sev = ms.get("severity", "minor")
        sev_color = "#9a2e2e" if sev == "critical" else ("#8b6914" if sev == "significant" else "#6b6b6b")
        gaps_html += f'''<div class="gap-box" style="margin-bottom:0.4em">
  <div style="font-size:0.82rem">{e(ms.get("missing_step", ""))}</div>
  <span class="ext-badge" style="background:{sev_color};margin-top:0.2em">{e(sev)}</span>
</div>\n'''

    # ── Counter-arguments (grouped by type) ──
    ca_by_type = {}
    for ca in ch_counters:
        ca_by_type.setdefault(ca.get("objection_type", "other"), []).append(ca)
    ca_html = ""
    for otype in ["empirical_challenge", "alternative_explanation", "value_disagreement", "methodological_concern", "scope_limitation", "internal_inconsistency"]:
        items = ca_by_type.get(otype, [])
        if not items:
            continue
        ca_html += f'<div style="margin-top:0.8em"><span class="ext-badge ext-badge-critique">{e(otype.replace("_"," "))}</span> ({len(items)})</div>\n'
        for ca in items[:3]:
            claim = claims_by_id.get(ca.get("targets_claim_global", ""))
            ca_html += f'''<div class="counter-box" style="margin:0.3em 0">
  <div class="counter-objection" style="font-size:0.82rem">{e(ca.get("objection", ""))}</div>
  {f'<div style="font-family:var(--font-ui);font-size:0.68rem;color:var(--text-tertiary);margin-top:0.2em">Targets: <a href="../../claim.html#{e(ca.get("targets_claim_global",""))}">{e(claim["text"][:70]) if claim else "?"}...</a></div>' if claim else ""}
</div>\n'''
        if len(items) > 3:
            ca_html += f'<p style="font-size:0.72rem;color:var(--text-tertiary)">+ {len(items)-3} more</p>'

    body = f'''{page_header(
        breadcrumbs=[("Atlas", "../../index.html"), (f'{b["short"]} ({b["year"]})', f'../../books/{slug}.html'), (ch_label, None)],
        css_path="../../style.css",
    )}

<div class="main-grid">
<main class="content">

  <div class="chapter-header">
    <div class="chapter-number" style="font-family:var(--font-ui);font-size:0.8rem;color:var(--text-tertiary)">{e(b["short"])} ({b["year"]}) &mdash; {e(ch_label)}</div>
    <h1 style="font-size:1.4rem">{e(ch_label)}</h1>
    {f'<div class="chapter-summary">{e(summary)}</div>' if summary else ""}
  </div>

  <div class="stats-bar">
    <div class="stat-item"><span class="stat-number">{len(ch_claims)}</span> claims</div>
    <div class="stat-item"><span class="stat-number">{len(ch_chains)}</span> argument chains</div>
    <div class="stat-item"><span class="stat-number">{len(ch_evidence)}</span> evidence</div>
    <div class="stat-item"><span class="stat-number">{len(ch_counters)}</span> counter-arguments</div>
    <div class="stat-item"><span class="stat-number">{len(ch_missing)}</span> logical gaps</div>
  </div>

  {f"""<div class="section">
    <div class="section-label">Argument Chains ({len(ch_chains)})</div>
    <p style="font-size:0.82rem;color:var(--text-secondary);margin-bottom:0.8em">How the chapter's premises build toward conclusions. Each chain shows a line of reasoning from top to bottom. Click any node for full evidence and counter-arguments.</p>
    {chains_html}
  </div>""" if chains_html else ""}

  {f"""<hr class="section-break">
  <div class="section">
    <div class="section-label counter-label">Counter-Arguments ({len(ch_counters)})</div>
    {ca_html}
  </div>""" if ca_html else ""}

  {f"""<hr class="section-break">
  <div class="section">
    <div class="section-label gap-label">Logical Gaps ({len(ch_missing)})</div>
    <p style="font-size:0.82rem;color:var(--text-secondary);margin-bottom:0.5em">Unstated assumptions required for the arguments to work.</p>
    {gaps_html}
  </div>""" if gaps_html else ""}

  {f"""<hr class="section-break">
  <details>
    <summary style="font-family:var(--font-ui);font-size:0.82rem;cursor:pointer;color:var(--text-secondary)">Other Claims Not in Chains ({len(unchained)})</summary>
    <div style="margin-top:0.5em">{unchained_html}</div>
  </details>""" if unchained_html else ""}

  <div class="nav-links">
    <a class="nav-link" href="../../books/{slug}.html">&larr; {e(b["short"])} ({b["year"]})</a>
    <a class="nav-link next" href="../../evolution.html">Argument evolution &rarr;</a>
  </div>

</main>
{book_sidebar(current_book=slug, depth=2)}
</div>

{page_footer()}'''

    return html_page(f'{ch_label} — {b["short"]}', body, css_path="../../style.css")


# ═══════════════════════════════════════════════════════════════════
# PAGE: Claim Detail (hash-routed, all books)
# ═══════════════════════════════════════════════════════════════════

def build_claim_page():
    # Build compact claim data for embedding
    # Only include fields needed for rendering
    compact_claims = {}
    for c in D["all_claims"]:
        compact_claims[c["_global_id"]] = {
            "t": c.get("text", ""),
            "b": c.get("book", ""),
            "ch": c.get("chapter", ""),
            "l": c.get("claim_level", ""),
            "mc": c.get("is_main_conclusion", False),
            "cf": c.get("confidence"),
            "sp": c.get("source_passage", ""),
            "pr": c.get("page_range", ""),
        }

    compact_evidence = {}
    for ev in D["all_evidence"]:
        cid = ev.get("supports_claim_global", "")
        if cid:
            compact_evidence.setdefault(cid, []).append({
                "d": ev.get("description", ""),
                "t": ev.get("evidence_type", ""),
                "s": ev.get("source_reference", ""),
            })

    compact_warrants = {}
    for w in D["all_warrants"]:
        cid = w.get("claim_id_global", "")
        if cid:
            compact_warrants.setdefault(cid, []).append({
                "t": w.get("warrant_text", ""),
                "e": w.get("is_explicit", False),
                "v": w.get("vulnerability", ""),
            })

    compact_counters = {}
    for ca in D["all_counter_arguments"]:
        cid = ca.get("targets_claim_global", "")
        if cid:
            compact_counters.setdefault(cid, []).append({
                "o": ca.get("objection", ""),
                "t": ca.get("objection_type", ""),
            })

    compact_deps_from = {}
    compact_deps_to = {}
    for d in D["all_dependencies"]:
        fid = d.get("from_id_global", "")
        tid = d.get("to_id_global", "")
        entry = {"e": d.get("explanation", ""), "r": d.get("relationship", "")}
        if fid:
            compact_deps_from.setdefault(fid, []).append({**entry, "id": tid})
        if tid:
            compact_deps_to.setdefault(tid, []).append({**entry, "id": fid})

    compact_clusters = {}
    for cl in D.get("cross_book_clusters", []) + [c for c in D.get("claim_clusters", []) if c["book_count"] <= 1]:
        for m in cl["members"]:
            compact_clusters[m["claim_id"]] = {
                "s": cl["size"],
                "bs": cl.get("books", []),
                "bc": cl.get("book_count", 1),
                "et": cl.get("evolution_type", ""),
                "es": cl.get("evolution_summary", ""),
                "ms": [{"b": mm["book"], "t": mm["text"][:120], "id": mm["claim_id"]} for mm in cl["members"]],
            }

    # Missing steps
    compact_missing = {}
    for ms in D["all_missing_steps"]:
        for fid in [ms.get("from_id_global", ""), ms.get("to_id_global", "")]:
            if fid:
                compact_missing.setdefault(fid, []).append({
                    "s": ms.get("missing_step", "")[:200],
                    "sv": ms.get("severity", "minor"),
                })

    # Argument chains (per-claim: which chains contain this claim)
    compact_chains = {}
    for ac in D["all_argument_chains"]:
        chain_nodes = ac.get("chain_global", [])
        for nid in chain_nodes:
            if nid not in compact_chains:
                compact_chains[nid] = []
            compact_chains[nid].append({
                "n": ac.get("name", ""),
                "st": ac.get("strength", ""),
                "ns": chain_nodes,
            })

    books_meta = {b["slug"]: {"t": b["title"], "y": b["year"], "s": b["short"]} for b in BOOKS_ORDERED}

    claim_data = {
        "c": compact_claims,
        "ev": compact_evidence,
        "w": compact_warrants,
        "ca": compact_counters,
        "df": compact_deps_from,
        "dt": compact_deps_to,
        "cl": compact_clusters,
        "ms": compact_missing,
        "ac": compact_chains,
        "bk": books_meta,
    }

    data_json = json.dumps(claim_data, ensure_ascii=False, separators=(',', ':'))

    script = '''<script>
const D = ''' + data_json + ''';

function esc(s) { const d=document.createElement('div'); d.textContent=s||''; return d.innerHTML; }
function badge(l) { return '<span class="badge badge-'+esc(l)+'">'+esc(l)+'</span>'; }
function bookLabel(slug) { const b=D.bk[slug]; return b ? b.s+' ('+b.y+')' : slug; }

function renderClaim(id) {
  const c = D.c[id];
  if (!c) { document.getElementById('claim-content').innerHTML = '<p>Claim not found: '+esc(id)+'</p>'; return; }

  const bk = D.bk[c.b] || {};
  let h = '';

  // Header
  h += '<div class="claim-header">';
  h += '<h1 class="claim-title">' + (c.sp ? '&ldquo;'+esc(c.t)+'&rdquo;' : esc(c.t)) + '</h1>';
  h += '<div class="claim-meta">' + badge(c.l);
  if (c.mc) h += '<span class="chain-badge">Main conclusion</span>';
  h += '</div>';
  h += '<div class="chapter-ref">'+esc(bk.s||c.b)+' ('+esc(bk.y||'?')+')';
  if (c.pr) h += ' &middot; pp. '+esc(c.pr);
  h += ' &middot; '+esc(id)+'</div></div>';

  // Source passage
  if (c.sp) {
    h += '<div class="section"><div class="section-label">In Hirsch\\u2019s Words</div>';
    h += '<blockquote class="source-passage">'+esc(c.sp)+'</blockquote></div>';
  }

  // Dependencies
  const depsTo = D.dt[id] || [];
  if (depsTo.length) {
    h += '<div class="section"><div class="section-label">Why This Matters</div>';
    h += '<ul class="stakes-list">';
    for (const d of depsTo) {
      const t = D.c[d.id];
      if (!t) continue;
      h += '<li><span class="dependency-text"><a href="claim.html#'+esc(d.id)+'">'+esc(t.t)+'</a></span>';
      h += '<span class="dependency-reason">'+esc(d.e)+'</span></li>';
    }
    h += '</ul></div>';
  }

  const depsFrom = D.df[id] || [];
  if (depsFrom.length) {
    h += '<div class="section"><div class="section-label">This Claim Depends On</div>';
    h += '<ul class="stakes-list">';
    for (const d of depsFrom) {
      const t = D.c[d.id];
      if (!t) continue;
      h += '<li><span class="dependency-text"><a href="claim.html#'+esc(d.id)+'">'+esc(t.t)+'</a></span>';
      h += '<span class="dependency-reason">'+esc(d.r)+': '+esc(d.e)+'</span></li>';
    }
    h += '</ul></div>';
  }

  // Evidence
  const ev = D.ev[id] || [];
  if (ev.length) {
    h += '<hr class="section-break"><div class="section">';
    h += '<div class="section-label evidence-label">Evidence ('+ev.length+')</div>';
    for (const e of ev) {
      h += '<div class="evidence-card"><div class="evidence-card-header"><div>';
      h += '<div class="evidence-description">'+esc(e.d)+'</div>';
      if (e.s) h += '<div class="evidence-source">'+esc(e.s)+'</div>';
      h += '</div><span class="evidence-type">'+esc(e.t.replace(/_/g,' '))+'</span></div></div>';
    }
    h += '</div>';
  }

  // Warrants
  const w = D.w[id] || [];
  if (w.length) {
    h += '<hr class="section-break"><div class="section">';
    h += '<div class="section-label warrant-label">Warrants ('+w.length+')</div>';
    for (const ww of w) {
      h += '<div class="warrant-box"><div class="warrant-header">Warrant</div>';
      h += '<div class="warrant-text">'+esc(ww.t)+'</div>';
      h += '<div class="warrant-explicit">'+(ww.e?'Explicit':'Implicit')+'</div>';
      if (ww.v) h += '<div class="vulnerability"><div class="vulnerability-label">Vulnerability</div><div class="vulnerability-text">'+esc(ww.v)+'</div></div>';
      h += '</div>';
    }
    h += '</div>';
  }

  // Counter-arguments
  const ca = D.ca[id] || [];
  if (ca.length) {
    h += '<hr class="section-break"><div class="section">';
    h += '<div class="section-label counter-label">Counter-Arguments ('+ca.length+')</div>';
    for (const c of ca) {
      h += '<div class="counter-box"><div class="counter-header"><span>Counter-argument</span>';
      h += '<span class="counter-type">'+esc(c.t.replace(/_/g,' '))+'</span></div>';
      h += '<div class="counter-objection">'+esc(c.o)+'</div></div>';
    }
    h += '</div>';
  }

  // Argument chains containing this claim
  const chains = D.ac[id] || [];
  if (chains.length) {
    h += '<hr class="section-break"><div class="section">';
    h += '<div class="section-label">Argument Chains ('+chains.length+')</div>';
    for (const ch of chains) {
      h += '<div style="margin-bottom:1em"><div style="font-family:var(--font-ui);font-size:0.82rem;font-weight:600">'+esc(ch.n)+' <span class="chain-strength strength-'+esc(ch.st)+'">'+esc(ch.st)+'</span></div>';
      h += '<div style="padding-left:0.5em;border-left:2px solid var(--border-light)">';
      for (let i=0; i<ch.ns.length; i++) {
        const nid = ch.ns[i];
        const nc = D.c[nid];
        const txt = nc ? nc.t : nid;
        const short = txt.length > 90 ? txt.substring(0,87)+'...' : txt;
        const isMe = nid === id;
        if (isMe) {
          h += '<div style="padding:0.4em 0.6em;margin:0.15em 0;border:2px solid var(--accent);border-radius:4px;font-size:0.8rem;font-weight:600;background:var(--accent-light)">'+esc(short)+'</div>';
        } else if (nc) {
          h += '<a href="claim.html#'+esc(nid)+'" style="display:block;padding:0.4em 0.6em;margin:0.15em 0;border:1px solid var(--border);border-radius:4px;font-size:0.8rem;text-decoration:none;color:var(--text-secondary)">'+esc(short)+'</a>';
        }
        if (i < ch.ns.length-1) h += '<div style="text-align:center;color:var(--text-tertiary);font-size:0.65rem">&#8595;</div>';
      }
      h += '</div></div>';
    }
    h += '</div>';
  }

  // Logical gaps / missing steps
  const gaps = D.ms[id] || [];
  if (gaps.length) {
    h += '<hr class="section-break"><div class="section">';
    h += '<div class="section-label gap-label">Logical Gaps ('+gaps.length+')</div>';
    h += '<p style="font-size:0.82rem;color:var(--text-secondary);margin-bottom:0.5em">Unstated assumptions required for this argument to work:</p>';
    for (const g of gaps) {
      const sevColor = g.sv==='critical' ? '#9a2e2e' : (g.sv==='significant' ? '#8b6914' : '#6b6b6b');
      h += '<div class="gap-box" style="margin-bottom:0.4em"><div style="font-size:0.82rem">'+esc(g.s)+'</div>';
      h += '<span style="display:inline-block;padding:0.1em 0.4em;border-radius:2px;font-family:var(--font-ui);font-size:0.65rem;font-weight:600;background:'+sevColor+';color:#fff;margin-top:0.2em">'+esc(g.sv)+'</span></div>';
    }
    h += '</div>';
  }

  // Cross-book cluster
  const cl = D.cl[id];
  if (cl && cl.bc > 1) {
    h += '<hr class="section-break"><div class="section">';
    h += '<div class="section-label">Same Argument Across Books</div>';
    if (cl.et) h += '<div style="margin-bottom:0.5em">'+badge(cl.et)+' '+(cl.es?'<span style="font-style:italic;font-size:0.85rem;color:var(--text-secondary)">'+esc(cl.es)+'</span>':'')+'</div>';
    h += '<p style="font-size:0.85rem;color:var(--text-secondary)">This argument appears in '+cl.bc+' books ('+cl.s+' variants):</p>';
    for (const m of cl.ms) {
      if (m.id === id) continue;
      h += '<div class="evo-card-member"><span class="member-book">'+esc(bookLabel(m.b))+'</span> ';
      h += '<a href="claim.html#'+esc(m.id)+'">'+esc(m.t)+'</a></div>';
    }
    h += '</div>';
  }

  // Nav
  h += '<div class="nav-links">';
  h += '<a class="nav-link" href="books/'+esc(c.b)+'.html">&larr; '+esc(bookLabel(c.b))+'</a>';
  h += '<a class="nav-link next" href="index.html">Atlas overview &rarr;</a>';
  h += '</div>';

  document.getElementById('claim-content').innerHTML = h;
  document.title = c.t.substring(0,60) + '... — Hirsch Argument Atlas';
  window.scrollTo(0,0);
}

function onHashChange() {
  const id = location.hash.substring(1);
  if (id && D.c[id]) renderClaim(id);
  else document.getElementById('claim-content').innerHTML = '<p class="loading">Select a claim from a book page or the evolution view.</p>';
}
window.addEventListener('hashchange', onHashChange);
window.addEventListener('DOMContentLoaded', onHashChange);
</script>'''

    body = f'''{page_header(breadcrumbs=[("Atlas", "index.html"), ("Claim", None)])}

<div class="main-grid">
<main class="content" id="claim-content">
  <p class="loading">Loading claim...</p>
</main>
{book_sidebar(depth=0)}
</div>

{page_footer()}
{script}'''

    return html_page("Claim Detail", body)


# ═══════════════════════════════════════════════════════════════════
# PAGE: Thinkers
# ═══════════════════════════════════════════════════════════════════

def build_thinkers_page():
    thinkers = D["thinkers"]

    # Multi-book first, then single-book
    multi = [t for t in thinkers if t["book_count"] > 1]
    single = [t for t in thinkers if t["book_count"] <= 1]

    def thinker_card(t):
        book_tags = ""
        for slug in t["books"]:
            b = BOOKS.get(slug, {})
            book_tags += f'<a href="books/{slug}.html" class="entity-book-tag" style="text-decoration:none;color:var(--accent)">{e(b.get("short", slug))} {b.get("year", "")}</a>'

        stances = set()
        engagement = ""
        for app in t["appearances"]:
            stances.add(app.get("author_stance", ""))
            if not engagement and app.get("engagement"):
                engagement = app["engagement"]

        stance_tags = ""
        for s in sorted(stances):
            if s:
                stance_tags += f' <span class="stance-tag {s}">{e(s.replace("_", " "))}</span>'

        if len(engagement) > 200:
            engagement = engagement[:197] + "..."

        anchor = t["name"].replace(" ", "_").replace("'", "")
        return f'''<a class="entity-card" href="entity.html#thinker:{anchor}" style="text-decoration:none;color:inherit;display:block">
  <h3>{e(t["name"])}{stance_tags}</h3>
  <div class="entity-card-detail">{e(engagement)}</div>
  <div class="entity-books">{book_tags}</div>
</a>\n'''

    multi_html = '<div class="entity-grid">\n' + "".join(thinker_card(t) for t in multi) + '</div>'
    single_html = '<div class="entity-grid">\n' + "".join(thinker_card(t) for t in single[:50]) + '</div>'

    body = f'''{page_header(breadcrumbs=[("Atlas", "index.html"), ("Thinkers", None)])}

<div class="main-grid full-width">
<main class="content">

  <h1>Thinkers ({len(thinkers)})</h1>
  <p class="lead">The intellectual landscape Hirsch engages with across {len(BOOKS_ORDERED)} books. Sorted by how many books they appear in.</p>

  <div class="section-label" style="margin-top:1em">Multi-Book Thinkers ({len(multi)})</div>
  {multi_html}

  <div class="section-label" style="margin-top:1.5em">Single-Book Thinkers (showing 50 of {len(single)})</div>
  {single_html}

</main>
</div>

{page_footer()}'''

    return html_page("Thinkers", body)


# ═══════════════════════════════════════════════════════════════════
# PAGE: Concepts
# ═══════════════════════════════════════════════════════════════════

def build_concepts_page():
    concepts = D["concepts"]
    multi = [c for c in concepts if c["book_count"] > 1]
    single = [c for c in concepts if c["book_count"] <= 1]

    def concept_card(c):
        book_tags = ""
        for slug in c["books"]:
            b = BOOKS.get(slug, {})
            book_tags += f'<a href="books/{slug}.html" class="entity-book-tag" style="text-decoration:none;color:var(--accent)">{e(b.get("short", slug))} {b.get("year", "")}</a>'

        definition = ""
        if c["appearances"]:
            definition = c["appearances"][0].get("author_definition", "")
        if len(definition) > 200:
            definition = definition[:197] + "..."

        anchor = c["term"].replace(" ", "_").replace("'", "")
        return f'''<a class="entity-card" href="entity.html#concept:{anchor}" style="text-decoration:none;color:inherit;display:block">
  <h3>{e(c["term"])}</h3>
  <div class="entity-card-detail">{e(definition)}</div>
  <div class="entity-books">{book_tags}</div>
</a>\n'''

    multi_html = '<div class="entity-grid">\n' + "".join(concept_card(c) for c in multi) + '</div>'
    single_html = '<div class="entity-grid">\n' + "".join(concept_card(c) for c in single[:50]) + '</div>'

    body = f'''{page_header(breadcrumbs=[("Atlas", "index.html"), ("Concepts", None)])}

<div class="main-grid full-width">
<main class="content">

  <h1>Key Concepts ({len(concepts)})</h1>
  <p class="lead">The vocabulary of Hirsch's argument across {len(BOOKS_ORDERED)} books. Sorted by how many books each concept appears in.</p>

  <div class="section-label" style="margin-top:1em">Multi-Book Concepts ({len(multi)})</div>
  {multi_html}

  <div class="section-label" style="margin-top:1.5em">Single-Book Concepts (showing 50 of {len(single)})</div>
  {single_html}

</main>
</div>

{page_footer()}'''

    return html_page("Concepts", body)


# ═══════════════════════════════════════════════════════════════════
# PAGE: Cases
# ═══════════════════════════════════════════════════════════════════

def build_cases_page():
    cases = D["cases"]
    multi = [c for c in cases if c["book_count"] > 1]
    single = [c for c in cases if c["book_count"] <= 1]

    def case_card(c):
        book_tags = ""
        for slug in c["books"]:
            b = BOOKS.get(slug, {})
            book_tags += f'<a href="books/{slug}.html" class="entity-book-tag" style="text-decoration:none;color:var(--accent)">{e(b.get("short", slug))} {b.get("year", "")}</a>'

        desc = ""
        if c["appearances"]:
            desc = c["appearances"][0].get("description", "")
        if len(desc) > 200:
            desc = desc[:197] + "..."

        anchor = c["name"].replace(" ", "_").replace("'", "")
        return f'''<a class="entity-card" href="entity.html#case:{anchor}" style="text-decoration:none;color:inherit;display:block">
  <h3>{e(c["name"])}</h3>
  <div class="entity-card-detail">{e(desc)}</div>
  <div class="entity-books">{book_tags}</div>
</a>\n'''

    multi_html = '<div class="entity-grid">\n' + "".join(case_card(c) for c in multi) + '</div>'
    single_html = '<div class="entity-grid">\n' + "".join(case_card(c) for c in single[:50]) + '</div>'

    body = f'''{page_header(breadcrumbs=[("Atlas", "index.html"), ("Cases", None)])}

<div class="main-grid full-width">
<main class="content">

  <h1>Cases &amp; Examples ({len(cases)})</h1>
  <p class="lead">The real-world evidence Hirsch marshals across {len(BOOKS_ORDERED)} books &mdash; national education systems, historical episodes, specific reforms.</p>

  <div class="section-label" style="margin-top:1em">Multi-Book Cases ({len(multi)})</div>
  {multi_html}

  <div class="section-label" style="margin-top:1.5em">Single-Book Cases (showing 50 of {len(single)})</div>
  {single_html}

</main>
</div>

{page_footer()}'''

    return html_page("Cases", body)


# ═══════════════════════════════════════════════════════════════════
# PAGE: Debates — Most Contested Claims
# ═══════════════════════════════════════════════════════════════════

def build_debates_page():
    """Build debates.html showing the most contested claims in Hirsch's corpus."""

    # Score each claim by contestedness
    from collections import Counter, defaultdict

    ca_count = Counter()
    ca_by_claim = defaultdict(list)
    for ca in D["all_counter_arguments"]:
        cid = ca.get("targets_claim_global", "")
        if cid:
            ca_count[cid] += 1
            ca_by_claim[cid].append(ca)

    ms_count = Counter()
    ms_by_claim = defaultdict(list)
    for ms in D["all_missing_steps"]:
        for fid in [ms.get("from_id_global", ""), ms.get("to_id_global", "")]:
            if fid:
                sev = ms.get("severity", "minor")
                weight = 3 if sev == "critical" else (2 if sev == "significant" else 1)
                ms_count[fid] += weight
                ms_by_claim[fid].append(ms)

    # Composite contestedness: counter-args + weighted missing steps
    contested_scores = {}
    all_claim_ids = set(ca_count.keys()) | set(ms_count.keys())
    for cid in all_claim_ids:
        contested_scores[cid] = ca_count.get(cid, 0) * 2 + ms_count.get(cid, 0)

    # Top 40
    top_contested = sorted(contested_scores.items(), key=lambda x: -x[1])[:40]

    cards_html = ""
    for cid, score in top_contested:
        c = claims_by_id.get(cid)
        if not c:
            continue

        # Counter-arguments grouped by type
        cas = ca_by_claim.get(cid, [])
        ca_types = defaultdict(list)
        for ca in cas:
            ca_types[ca.get("objection_type", "other")].append(ca)

        ca_html = ""
        for otype in ["empirical_challenge", "alternative_explanation", "value_disagreement", "methodological_concern", "scope_limitation", "internal_inconsistency"]:
            items = ca_types.get(otype, [])
            if not items:
                continue
            ca_html += f'<div style="margin-top:0.4em"><span class="ext-badge ext-badge-critique">{e(otype.replace("_"," "))}</span></div>'
            for ca in items[:2]:
                text = ca.get("objection", "")
                if len(text) > 150:
                    text = text[:147] + "..."
                ca_html += f'<div style="font-size:0.78rem;color:var(--text-secondary);padding:0.15em 0 0.15em 0.8em;border-left:2px solid var(--counter-border)">{e(text)}</div>'
            if len(items) > 2:
                ca_html += f'<div style="font-size:0.68rem;color:var(--text-tertiary);padding-left:0.8em">+ {len(items)-2} more</div>'

        # Missing steps
        gaps = ms_by_claim.get(cid, [])
        gaps_html = ""
        critical = [g for g in gaps if g.get("severity") == "critical"]
        if critical:
            for g in critical[:2]:
                gaps_html += f'<div style="font-size:0.78rem;color:var(--counter);padding:0.15em 0 0.15em 0.8em;border-left:2px solid var(--counter-border)">{e(g.get("missing_step", "")[:120])}</div>'

        book_info = BOOKS.get(c.get("book", ""), {})

        cards_html += f'''<div class="evo-card" style="border-left:3px solid var(--counter)">
  <div style="display:flex;gap:0.5em;align-items:center;flex-wrap:wrap;margin-bottom:0.3em">
    <span class="entity-book-tag">{e(book_info.get("short",""))} {book_info.get("year","")}</span>
    <span style="font-family:var(--font-ui);font-size:0.68rem;color:var(--text-tertiary)">{len(cas)} counter-arguments · {len(critical)} critical gaps</span>
  </div>
  <div class="evo-card-claim"><a href="claim.html#{e(cid)}" style="color:inherit;text-decoration:none;border-bottom:1px solid var(--border-light)">{e(c["text"])}</a></div>
  {ca_html}
  {gaps_html}
</div>\n'''

    # Type distribution
    all_ca_types = Counter()
    for ca in D["all_counter_arguments"]:
        all_ca_types[ca.get("objection_type", "other")] += 1

    body = f'''{page_header(breadcrumbs=[("Atlas", "index.html"), ("Debates", None)])}

<div class="main-grid">
<main class="content">

  <h1 style="color:var(--counter)">Where Is Hirsch Most Vulnerable?</h1>
  <p class="lead">The 40 most-contested claims in Hirsch's corpus, ranked by number of counter-arguments
  and severity of logical gaps. These are the claims where the argument is thinnest.</p>

  <div class="corpus-stats" style="margin:1em 0">
    <div class="corpus-stat"><span class="corpus-stat-number">{len(D["all_counter_arguments"])}</span><span class="corpus-stat-label">Counter-Arguments</span></div>
    <div class="corpus-stat"><span class="corpus-stat-number">{sum(1 for ms in D["all_missing_steps"] if ms.get("severity")=="critical")}</span><span class="corpus-stat-label">Critical Gaps</span></div>
    <div class="corpus-stat"><span class="corpus-stat-number">{all_ca_types.get("empirical_challenge",0)}</span><span class="corpus-stat-label">Empirical Challenges</span></div>
    <div class="corpus-stat"><span class="corpus-stat-number">{all_ca_types.get("value_disagreement",0)}</span><span class="corpus-stat-label">Value Disagreements</span></div>
    <div class="corpus-stat"><span class="corpus-stat-number">{all_ca_types.get("alternative_explanation",0)}</span><span class="corpus-stat-label">Alt. Explanations</span></div>
  </div>

  {cards_html}

</main>
{book_sidebar(depth=0)}
</div>

{page_footer()}'''

    return html_page("Debates", body)


# ═══════════════════════════════════════════════════════════════════
# PAGE: Entity Detail (hash-routed SPA for thinkers, concepts, cases)
# ═══════════════════════════════════════════════════════════════════

def build_entity_page():
    """Build a hash-routed entity detail page embedding all thinker/concept/case data."""

    # Build lookup keyed by anchor (name with spaces→underscores)
    def to_anchor(name):
        return name.replace(" ", "_").replace("'", "")

    entity_data = {
        "thinkers": {},
        "concepts": {},
        "cases": {},
        "books": {b["slug"]: {"t": b["title"], "y": b["year"], "s": b["short"]} for b in BOOKS_ORDERED},
    }

    for t in D["thinkers"]:
        anchor = to_anchor(t["name"])
        entity_data["thinkers"][anchor] = {
            "name": t["name"],
            "books": t["books"],
            "bc": t["book_count"],
            "apps": [{
                "b": a.get("book", ""),
                "ki": a.get("key_idea", ""),
                "eng": a.get("engagement", ""),
                "st": a.get("author_stance", ""),
                "kp": a.get("key_passages", []),
            } for a in t["appearances"]],
        }

    for c in D["concepts"]:
        anchor = to_anchor(c["term"])
        entity_data["concepts"][anchor] = {
            "term": c["term"],
            "books": c["books"],
            "bc": c["book_count"],
            "apps": [{
                "b": a.get("book", ""),
                "def": a.get("author_definition", ""),
                "imp": a.get("importance", ""),
                "sp": a.get("source_passage", ""),
            } for a in c["appearances"]],
        }

    for c in D["cases"]:
        anchor = to_anchor(c["name"])
        entity_data["cases"][anchor] = {
            "name": c["name"],
            "books": c["books"],
            "bc": c["book_count"],
            "apps": [{
                "b": a.get("book", ""),
                "desc": a.get("description", ""),
                "role": a.get("role_in_argument", ""),
                "kp": a.get("key_passages", []),
                "cs": a.get("claims_supported", []),
            } for a in c["appearances"]],
        }

    data_json = json.dumps(entity_data, ensure_ascii=False, separators=(',', ':'))

    script = '''<script>
const E = ''' + data_json + ''';

function esc(s) { const d=document.createElement('div'); d.textContent=s||''; return d.innerHTML; }
function bookLabel(slug) { const b=E.books[slug]; return b ? b.s+' ('+b.y+')' : slug; }
function stanceBadge(s) {
  if (!s) return '';
  const colors = {agrees:'#2d6a2e', disagrees:'#9a2e2e', builds_on:'#4a6080',
    partially_agrees:'#8b6914', historicizes:'#6b6b6b', critiques:'#9a2e2e'};
  const c = colors[s] || '#6b6b6b';
  return '<span style="display:inline-block;padding:0.1em 0.4em;border-radius:2px;font-family:var(--font-ui);font-size:0.65rem;font-weight:600;background:'+c+';color:#fff;margin-left:0.3em">'+esc(s.replace(/_/g,' '))+'</span>';
}

function renderThinker(key) {
  const t = E.thinkers[key];
  if (!t) return '<p>Thinker not found.</p>';
  let h = '<h1>'+esc(t.name)+'</h1>';
  h += '<p style="font-family:var(--font-ui);font-size:0.82rem;color:var(--text-secondary)">Referenced in '+t.bc+' books</p>';

  // Group appearances by book
  const byBook = {};
  for (const a of t.apps) {
    if (!byBook[a.b]) byBook[a.b] = [];
    byBook[a.b].push(a);
  }

  for (const slug of t.books) {
    const apps = byBook[slug] || [];
    h += '<div style="margin-top:1.5em;padding-top:1em;border-top:1px solid var(--border-light)">';
    h += '<h2 style="font-size:1rem;font-family:var(--font-ui)">'+esc(bookLabel(slug))+'</h2>';
    for (const a of apps) {
      h += '<div style="margin:0.6em 0">';
      if (a.st) h += stanceBadge(a.st);
      if (a.ki) h += '<div style="font-weight:600;margin:0.3em 0;font-size:0.9rem">'+esc(a.ki)+'</div>';
      if (a.eng) h += '<div style="font-size:0.85rem;color:var(--text-secondary)">'+esc(a.eng)+'</div>';
      for (const p of (a.kp||[])) {
        const pt = typeof p === 'string' ? p : (p.text || p.passage || JSON.stringify(p));
        h += '<blockquote class="source-passage" style="font-size:0.82rem;margin:0.4em 0 0.4em 0.5em">'+esc(pt)+'</blockquote>';
      }
      h += '</div>';
    }
    h += '</div>';
  }
  return h;
}

function renderConcept(key) {
  const c = E.concepts[key];
  if (!c) return '<p>Concept not found.</p>';
  let h = '<h1>'+esc(c.term)+'</h1>';
  h += '<p style="font-family:var(--font-ui);font-size:0.82rem;color:var(--text-secondary)">Appears in '+c.bc+' books</p>';

  const byBook = {};
  for (const a of c.apps) {
    if (!byBook[a.b]) byBook[a.b] = [];
    byBook[a.b].push(a);
  }

  for (const slug of c.books) {
    const apps = byBook[slug] || [];
    h += '<div style="margin-top:1.5em;padding-top:1em;border-top:1px solid var(--border-light)">';
    h += '<h2 style="font-size:1rem;font-family:var(--font-ui)">'+esc(bookLabel(slug))+'</h2>';
    for (const a of apps) {
      h += '<div style="margin:0.6em 0">';
      if (a.imp) h += '<span class="badge" style="background:var(--accent);color:#fff">'+esc(a.imp)+'</span> ';
      if (a.def) h += '<div style="font-size:0.88rem;margin:0.3em 0">'+esc(a.def)+'</div>';
      if (a.sp) h += '<blockquote class="source-passage" style="font-size:0.82rem;margin:0.4em 0 0.4em 0.5em">'+esc(a.sp)+'</blockquote>';
      h += '</div>';
    }
    h += '</div>';
  }
  return h;
}

function renderCase(key) {
  const c = E.cases[key];
  if (!c) return '<p>Case not found.</p>';
  let h = '<h1>'+esc(c.name)+'</h1>';
  h += '<p style="font-family:var(--font-ui);font-size:0.82rem;color:var(--text-secondary)">Used in '+c.bc+' books</p>';

  const byBook = {};
  for (const a of c.apps) {
    if (!byBook[a.b]) byBook[a.b] = [];
    byBook[a.b].push(a);
  }

  for (const slug of c.books) {
    const apps = byBook[slug] || [];
    h += '<div style="margin-top:1.5em;padding-top:1em;border-top:1px solid var(--border-light)">';
    h += '<h2 style="font-size:1rem;font-family:var(--font-ui)">'+esc(bookLabel(slug))+'</h2>';
    for (const a of apps) {
      h += '<div style="margin:0.6em 0">';
      if (a.role) h += '<span class="badge" style="background:var(--meta);color:#fff">'+esc(a.role.replace(/_/g,' '))+'</span> ';
      if (a.desc) h += '<div style="font-size:0.88rem;margin:0.3em 0">'+esc(a.desc)+'</div>';
      for (const p of (a.kp||[])) {
        const pt = typeof p === 'string' ? p : (p.text || p.passage || JSON.stringify(p));
        h += '<blockquote class="source-passage" style="font-size:0.82rem;margin:0.4em 0 0.4em 0.5em">'+esc(pt)+'</blockquote>';
      }
      h += '</div>';
    }
    h += '</div>';
  }
  return h;
}

function onHashChange() {
  const hash = location.hash.substring(1);
  const [type, key] = hash.split(':');
  const el = document.getElementById('entity-content');
  let backLink = '', html = '';

  if (type === 'thinker' && E.thinkers[key]) {
    backLink = '<a href="thinkers.html">&larr; All Thinkers</a>';
    html = renderThinker(key);
  } else if (type === 'concept' && E.concepts[key]) {
    backLink = '<a href="concepts.html">&larr; All Concepts</a>';
    html = renderConcept(key);
  } else if (type === 'case' && E.cases[key]) {
    backLink = '<a href="cases.html">&larr; All Cases</a>';
    html = renderCase(key);
  } else {
    html = '<p class="loading">Select an entity from the Thinkers, Concepts, or Cases pages.</p>';
  }

  el.innerHTML = '<div class="nav-links" style="margin-bottom:1em">'+backLink+'</div>' + html;
  window.scrollTo(0,0);
}

window.addEventListener('hashchange', onHashChange);
window.addEventListener('DOMContentLoaded', onHashChange);
</script>'''

    body = f'''{page_header(breadcrumbs=[("Atlas", "index.html"), ("Entity", None)])}

<div class="main-grid">
<main class="content" id="entity-content">
  <p class="loading">Loading...</p>
</main>
{book_sidebar(depth=0)}
</div>

{page_footer()}
{script}'''

    return html_page("Entity Detail", body)


# ═══════════════════════════════════════════════════════════════════
# PAGE: Scholarly Context
# ═══════════════════════════════════════════════════════════════════

def build_context_page():
    """Build the scholarly context page — external research organized by Hirsch topic.

    Each topic shows a side-by-side: Hirsch's clusters (amber) vs external findings (blue).
    """
    if not EXT:
        return None

    topics = EXT["topics"]
    items_by_id = {i["id"]: i for i in EXT["items"]}
    by_topic = EXT["by_topic"]
    clusters_per_topic = EXT.get("clusters_per_topic", {})
    clusters_by_id = {cl["cluster_id"]: cl for cl in D["cross_book_clusters"]}

    # Sort topics by total content (Hirsch clusters + external items)
    topic_order = sorted(
        topics.keys(),
        key=lambda t: -(len(clusters_per_topic.get(t, [])) + len(by_topic.get(t, []))),
    )

    sections_html = ""
    for tid in topic_order:
        topic = topics[tid]
        t_clusters = clusters_per_topic.get(tid, [])
        t_items = [items_by_id[iid] for iid in by_topic.get(tid, []) if iid in items_by_id]

        if not t_clusters and not t_items:
            continue

        # Hirsch column
        hirsch_html = ""
        for cid in t_clusters[:8]:
            cl = clusters_by_id.get(cid)
            if not cl:
                continue
            text = cl["canonical_claim"]["text"]
            if len(text) > 120:
                text = text[:117] + "..."
            hirsch_html += f'''<div class="evo-card" style="padding:0.6em 0.8em;margin-bottom:0.4em;">
  <div class="evo-card-header" style="margin-bottom:0.2em">
    {timeline_dots(cl["books"])}
    {evo_badge(cl.get("evolution_type", ""))}
  </div>
  <div style="font-size:0.82rem"><a href="claim.html#{e(cl['canonical_claim']['claim_id'])}" style="color:inherit">{e(text)}</a></div>
</div>\n'''
        if len(t_clusters) > 8:
            hirsch_html += f'<p style="font-size:0.78rem;color:var(--text-tertiary)">+ {len(t_clusters) - 8} more clusters</p>'

        # External column
        ext_html = ""
        # Sort: critiques first, then RCTs/meta, then rest
        type_order = {"Critique / Debate": 0, "Randomized Trial": 1, "Meta-analysis": 1,
                      "Empirical Study": 2, "Longitudinal / Quasi-Experimental": 2, "Implementation Case": 3}
        t_items.sort(key=lambda x: type_order.get(x.get("evidence_type", ""), 5))

        for item in t_items[:10]:
            text = item["text"]
            if len(text) > 200:
                text = text[:197] + "..."
            etype = item.get("evidence_type", "")
            strength = item.get("strength", "")
            is_critique = etype == "Critique / Debate"
            badge_class = "ext-badge-critique" if is_critique else "ext-badge-type"

            urls = item.get("source_urls", [])
            first_url = urls[0] if urls else ""
            section = item.get("section", "")
            doc = item.get("document", "")
            source_line = f'{e(doc)}' + (f' &middot; {e(section[:50])}' if section else "")
            if first_url:
                source_line = f'<a href="{e(first_url)}" target="_blank" rel="noopener" style="color:#4a6080">{source_line} &#8599;</a>'

            ext_html += f'''<div class="ext-card" style="cursor:{'pointer' if first_url else 'default'}"{f' onclick="window.open(&#39;{e(first_url)}&#39;,&#39;_blank&#39;)"' if first_url else ''}>
  <div class="ext-card-text">{e(text)}</div>
  <div class="ext-card-meta">
    <span class="ext-badge {badge_class}">{e(etype)}</span>
    <span class="ext-badge ext-badge-strength">{e(strength)}</span>
    <span style="font-family:var(--font-ui);font-size:0.62rem;color:var(--text-tertiary)">{source_line}</span>
  </div>
</div>\n'''
        if len(t_items) > 10:
            ext_html += f'<p style="font-size:0.78rem;color:var(--text-tertiary)">+ {len(t_items) - 10} more findings</p>'

        sections_html += f'''<div class="topic-section" id="topic-{tid}">
  <div class="topic-header">
    <h2>{e(topic["label"])}</h2>
    <span style="font-family:var(--font-ui);font-size:0.75rem;color:var(--text-tertiary)">{len(t_clusters)} Hirsch clusters &middot; {len(t_items)} external findings</span>
  </div>
  <p style="font-size:0.85rem;color:var(--text-secondary);margin-bottom:0.8em;">{e(topic["description"])}</p>
  <div class="topic-split">
    <div class="topic-col-hirsch">
      <div class="topic-col-label">What Hirsch argues</div>
      {hirsch_html if hirsch_html else '<p style="font-size:0.82rem;color:var(--text-tertiary)">No clusters tagged for this topic.</p>'}
    </div>
    <div class="topic-col-ext">
      <div class="topic-col-label">External research</div>
      {ext_html if ext_html else '<p style="font-size:0.82rem;color:var(--text-tertiary)">No external findings matched.</p>'}
    </div>
  </div>
</div>\n'''

    # Topic nav
    topic_nav = ""
    for tid in topic_order:
        topic = topics[tid]
        n = len(clusters_per_topic.get(tid, [])) + len(by_topic.get(tid, []))
        if n > 0:
            topic_nav += f'<a href="#topic-{tid}" class="filter-btn">{e(topic["label"])} ({n})</a> '

    body = f'''{page_header(breadcrumbs=[("Atlas", "index.html"), ("Scholarly Context", None)])}

<div class="main-grid">
<main class="content">

  <h1 style="color:#4a6080">Scholarly Context</h1>
  <p class="lead">External research that supports, challenges, or extends Hirsch's arguments.
  Each topic shows Hirsch's own claims alongside independent findings from the broader research literature.</p>

  <div class="ext-provenance">
    Source: Knowledge-Based Curricula research compendium (29 documents, 5,247 items).
    This compendium is sympathetically oriented toward knowledge-rich approaches &mdash;
    it is not a neutral survey. Critiques and debates are included but the framing
    favors knowledge-based design. All items are external to Hirsch's own books.
  </div>

  <div class="corpus-stats" style="margin:1em 0;">
    <div class="corpus-stat"><span class="corpus-stat-number">{len(EXT["items"])}</span><span class="corpus-stat-label">External Findings</span></div>
    <div class="corpus-stat"><span class="corpus-stat-number">{EXT["stats"]["evidence_types"].get("Randomized Trial", 0)}</span><span class="corpus-stat-label">RCTs</span></div>
    <div class="corpus-stat"><span class="corpus-stat-number">{EXT["stats"]["evidence_types"].get("Meta-analysis", 0)}</span><span class="corpus-stat-label">Meta-analyses</span></div>
    <div class="corpus-stat"><span class="corpus-stat-number">{EXT["stats"]["evidence_types"].get("Critique / Debate", 0)}</span><span class="corpus-stat-label">Critiques</span></div>
    <div class="corpus-stat"><span class="corpus-stat-number">{EXT["stats"]["clusters_tagged"]}</span><span class="corpus-stat-label">Hirsch Clusters Linked</span></div>
  </div>

  <div class="filter-bar" style="margin-bottom:1.5em">{topic_nav}</div>

  {sections_html}

  <hr class="section-break">

  <div class="topic-section" id="topic-nordic" style="border-top: 3px solid #2e6a6a; padding-top: 1.5em;">
    <div class="topic-header">
      <h2 style="color:#2e6a6a">Nordic Counter-Perspectives</h2>
      <span style="font-family:var(--font-ui);font-size:0.75rem;color:var(--text-tertiary)">Curated from Norwegian academic sources in the otak knowledge graph</span>
    </div>
    <p style="font-size:0.85rem;color:var(--text-secondary);margin-bottom:1em;">
      Hirsch has apparently never engaged with the continental European <em>Bildung/Didaktik</em> tradition &mdash;
      a philosophically sophisticated alternative that rejects the knowledge-vs-skills binary he assumes.
      Norwegian and Nordic education research offers the strongest counter-perspective to Hirsch's framing,
      because Scandinavia has pursued exactly the kind of competence-based, skills-oriented curriculum he warns against,
      while maintaining high PISA scores and social equity.
    </p>

    <div class="topic-split">
      <div class="topic-col-hirsch">
        <div class="topic-col-label">What Hirsch argues</div>
        <div class="evo-card" style="padding:0.6em 0.8em;margin-bottom:0.4em;">
          <div style="font-size:0.82rem">Skills-based curricula always fail: they dilute content, widen achievement gaps, and produce students who can neither read nor think critically.</div>
          <div style="font-size:0.7rem;color:var(--text-tertiary);margin-top:0.2em">Core argument across all 10 books (1977&ndash;2024)</div>
        </div>
        <div class="evo-card" style="padding:0.6em 0.8em;margin-bottom:0.4em;">
          <div style="font-size:0.82rem">There is no such thing as &ldquo;deep learning&rdquo; or &ldquo;critical thinking&rdquo; as a transferable general skill. All competence is domain-specific.</div>
          <div style="font-size:0.7rem;color:var(--text-tertiary);margin-top:0.2em">8 books, 1987&ndash;2024 &mdash; Hirsch's #1 recurring claim</div>
        </div>
        <div class="evo-card" style="padding:0.6em 0.8em;margin-bottom:0.4em;">
          <div style="font-size:0.82rem">Curricula must specify content with precision. Vague competence goals lead to fragmented instruction and unequal outcomes.</div>
          <div style="font-size:0.7rem;color:var(--text-tertiary);margin-top:0.2em">6 books, 1996&ndash;2023</div>
        </div>
      </div>

      <div class="topic-col-ext" style="border-color:#2e6a6a">
        <div class="topic-col-label" style="color:#2e6a6a;border-color:#2e6a6a">Nordic research responds</div>

        <div class="ext-card" style="border-left-color:#2e6a6a">
          <div class="ext-card-text"><strong>The Bildung alternative:</strong> According to Klafki, the core of <em>danning</em> (formation) is the <em>fusion</em> of knowledge content (material) and the learning process (formal) &mdash; not a choice between them. The Nordic tradition rejects Hirsch's binary.</div>
          <div class="ext-card-meta">
            <span class="ext-badge ext-badge-type">Philosophical framework</span>
            <span style="font-family:var(--font-ui);font-size:0.62rem;color:var(--text-tertiary)">Klafki, via Norwegian academic sources</span>
          </div>
        </div>

        <div class="ext-card" style="border-left-color:#2e6a6a">
          <div class="ext-card-text"><strong>Norway's LK20 proves Hirsch right?</strong> &ldquo;The curriculum provides stronger regulation regarding which skills students should develop than the specific knowledge content they should encounter.&rdquo; &mdash; This empirical finding from Sundby &amp; Karseth validates Hirsch's concern about vague competence goals.</div>
          <div class="ext-card-meta">
            <span class="ext-badge ext-badge-type">Empirical study</span>
            <span class="ext-badge ext-badge-strength">Academic</span>
            <span style="font-family:var(--font-ui);font-size:0.62rem;color:var(--text-tertiary)">Sundby &amp; Karseth, <em>The Curriculum Journal</em> (2022)</span>
          </div>
        </div>

        <div class="ext-card" style="border-left-color:#2e6a6a">
          <div class="ext-card-text"><strong>&ldquo;Å utforske&rdquo; (to explore) is the most frequently used verb in LK20.</strong> The curriculum's verb choices reveal a systematic preference for process over content &mdash; exactly the pattern Hirsch predicts will erode shared knowledge.</div>
          <div class="ext-card-meta">
            <span class="ext-badge ext-badge-type">Corpus analysis</span>
            <span class="ext-badge ext-badge-strength">Academic</span>
            <span style="font-family:var(--font-ui);font-size:0.62rem;color:var(--text-tertiary)">Karseth et al. (2020), via utdanningsnytt</span>
          </div>
        </div>

        <div class="ext-card" style="border-left-color:#2e6a6a">
          <div class="ext-card-text"><strong>Teachers were overruled:</strong> &ldquo;Intercultural competence fits more easily into modern curricula emphasizing broad competencies than the specific knowledge-based content proposed by teachers.&rdquo; &mdash; When Norwegian English teachers proposed specific content, the curriculum framework couldn't accommodate it.</div>
          <div class="ext-card-meta">
            <span class="ext-badge ext-badge-critique">Critique</span>
            <span style="font-family:var(--font-ui);font-size:0.62rem;color:var(--text-tertiary)">utdanningsnytt / Bedre Skole (2024)</span>
          </div>
        </div>

        <div class="ext-card" style="border-left-color:#2e6a6a">
          <div class="ext-card-text"><strong>The dybdelæring paradox:</strong> &ldquo;There is a potential conflict between the requirement for deep learning and the requirement to provide students with basic subject competence.&rdquo; &mdash; Norwegian educators themselves recognize the tension that Hirsch identifies, but frame it as a design challenge rather than a fatal flaw.</div>
          <div class="ext-card-meta">
            <span class="ext-badge ext-badge-type">Professional debate</span>
            <span style="font-family:var(--font-ui);font-size:0.62rem;color:var(--text-tertiary)">utdanningsnytt</span>
          </div>
        </div>

        <div class="ext-card" style="border-left-color:#2e6a6a">
          <div class="ext-card-text"><strong>Sweden corrected course:</strong> Lgr22 (2022) explicitly strengthened subject content after a decade of skills-focused curricula. The revision aimed to &ldquo;promote good subject knowledge that has a value in itself.&rdquo; A Nordic system did what Hirsch prescribes &mdash; but without abandoning the Bildung framework.</div>
          <div class="ext-card-meta">
            <span class="ext-badge ext-badge-type">Policy change</span>
            <span style="font-family:var(--font-ui);font-size:0.62rem;color:var(--text-tertiary)">Skolverket / Swedish National Agency for Education (2022)</span>
          </div>
        </div>

      </div>
    </div>

    <div class="ext-provenance" style="border-color:#2e6a6a">
      Sources: Norwegian academic journals (Nordic Journal of Literacy Research, Utdanning og Praksis, Nordisk tidsskrift for pedagogikk og kritikk),
      Norwegian education press (utdanningsnytt.no / Bedre Skole), PhD theses via NVA, and Swedish curriculum documents (Skolverket).
      Claims retrieved from the otak knowledge graph (~28K claims from Norwegian education sources).
      These represent the strongest philosophical alternative to Hirsch's framing: the Nordic position that knowledge and skills
      can be integrated through <em>Bildung</em>, rather than being forced into the binary that structures Hirsch's argument.
    </div>
  </div>

</main>
{book_sidebar(depth=0)}
</div>

{page_footer()}'''

    return html_page("Scholarly Context", body)


# ═══════════════════════════════════════════════════════════════════
# Build all
# ═══════════════════════════════════════════════════════════════════

def main():
    import shutil

    print("Building Hirsch Argument Atlas (corpus)...")

    # Clean old WKM-only chapter pages
    old_chapters = SITE / "chapters"
    if old_chapters.exists():
        shutil.rmtree(old_chapters)
        print("  Removed old chapters/ directory")

    with open(SITE / "index.html", "w") as f:
        f.write(build_landing())
    print("  index.html")

    with open(SITE / "evolution.html", "w") as f:
        f.write(build_evolution())
    print("  evolution.html")

    with open(SITE / "claim.html", "w") as f:
        f.write(build_claim_page())
    size_mb = (SITE / "claim.html").stat().st_size / 1024 / 1024
    print(f"  claim.html ({size_mb:.1f} MB)")

    # Per-book + per-chapter pages
    total_chapters = 0
    for b in BOOKS_ORDERED:
        slug = b["slug"]
        with open(SITE / "books" / f'{slug}.html', "w") as f:
            f.write(build_book_page(slug))
        print(f"  books/{slug}.html")

        # Chapter pages under books/{slug}/
        book_claims = claims_by_book.get(slug, [])
        chapters = sorted(set(c["chapter"] for c in book_claims),
                          key=lambda ch: (-1 if ch == "prologue" else (100 if ch.startswith("e") else
                                          (200 if ch.startswith("a") else (int(ch) if ch.isdigit() else 999)))))

        ch_dir = SITE / "books" / slug
        ch_dir.mkdir(exist_ok=True)
        for ch in chapters:
            ch_slug = ch.replace(" ", "-").lower()
            with open(ch_dir / f"{ch_slug}.html", "w") as f:
                f.write(build_chapter_page(slug, ch))
            total_chapters += 1
        print(f"    + {len(chapters)} chapter pages")

    with open(SITE / "thinkers.html", "w") as f:
        f.write(build_thinkers_page())
    print("  thinkers.html")

    with open(SITE / "concepts.html", "w") as f:
        f.write(build_concepts_page())
    print("  concepts.html")

    with open(SITE / "cases.html", "w") as f:
        f.write(build_cases_page())
    print("  cases.html")

    with open(SITE / "debates.html", "w") as f:
        f.write(build_debates_page())
    print("  debates.html")

    with open(SITE / "entity.html", "w") as f:
        f.write(build_entity_page())
    size_kb = (SITE / "entity.html").stat().st_size / 1024
    print(f"  entity.html ({size_kb:.0f} KB)")

    # Scholarly context page (only if external data available)
    if EXT:
        ctx = build_context_page()
        if ctx:
            with open(SITE / "context.html", "w") as f:
                f.write(ctx)
            print("  context.html (scholarly context)")

    total = 3 + len(BOOKS_ORDERED) + total_chapters + 3 + (1 if EXT else 0)
    print(f"\nDone! {total} pages in {SITE}")
    print(f"  {D['stats']['claims']:,} claims across {len(BOOKS_ORDERED)} books, {total_chapters} chapter pages")
    if EXT:
        print(f"  + scholarly context: {EXT['stats']['matched_to_topics']} external findings")


if __name__ == "__main__":
    main()
