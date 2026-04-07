# Hirsch Argument Atlas — Project Plan

**Started**: 2026-03-29
**Status**: All 10 Hirsch books extracted (10,232 claims, 101 chapters, 1977-2024). Sliding window pipeline calibrated to 0.95+ recall. WKM consolidated with importance scoring + static website prototype. Next: cross-book consolidation and synthesis.
**Goal**: A public website that exhaustively maps E.D. Hirsch's argument structure across all his books, with evidence chains, reference deduplication, cross-book evolution, and counter-arguments.

---

## Why This Project

Otak has been optimized for ingestion scale (27K claims, 8K sources) but hasn't produced something genuinely useful. The research consistently says: selectivity beats exhaustiveness, claims alone aren't enough (need argument structure), and usefulness comes from frontiers not archives.

The Hirsch project flips the approach: **deep extraction from few sources** instead of shallow extraction from many. One thinker, ~10 books, 40 years of argument evolution. Bounded, coherent, public-interest, citation-rich.

Nobody has done this — exhaustively mapping one thinker's complete argument structure across all their works with structured evidence chains.

---

## Books (Priority Order)

| Priority | Book | Year | Status |
|----------|------|------|--------|
| 1 | **Why Knowledge Matters** | 2016 | Have PDF + ePub. Most rigorous, France data. |
| 2 | **Cultural Literacy** | 1987 | Need to acquire. Founding document, 5000-item list. |
| 3 | **The Schools We Need** | 1996 | Need to acquire. Deepest critique of progressive ed. |
| 4 | **The Knowledge Deficit** | 2006 | Need to acquire. Most focused on reading mechanism. |
| 5 | **How to Educate a Citizen** | 2020 | Need to acquire. Latest evolution. |
| 6 | **American Ethnicity** | 2022 | Free PDF from coreknowledge.org. |
| 7 | **Shared Knowledge** | 2023 | Free PDF from coreknowledge.org. |
| 8 | **The Ratchet Effect** | 2024/25 | Self-published, recent. |
| 9 | **The Philosophy of Composition** | 1977 | Pivotal — where the insight originated. |
| 10 | **Validity in Interpretation** | 1967 | Academic hermeneutics. Lowest priority. |

Start with book 1 (Why Knowledge Matters). Add books 2-4 before launching publicly.

---

## Data Model

Based on the three critiques (education scholar, argument mapper, UX designer).

### Node Types (10)

| Type | Description | Example |
|------|-------------|---------|
| **claim** | An assertible proposition — the atomic unit. Extract aggressively. | "Reading comprehension depends on background knowledge, not transferable skills" |
| **evidence** | A specific study, dataset, example, or observation | "Recht & Leslie 1988: baseball study" |
| **warrant** | The principle connecting evidence to claim | "If comprehension requires knowledge, then tests that ignore curriculum are measuring home environment" |
| **objection** | A challenge to a claim, warrant, or evidence-claim link | "The French decline could be explained by immigration, not curriculum change" |
| **response** | A reply to an objection | "The decline among wealthy native-born families cannot be blamed on immigration" |
| **framework** | A composite: a coherent package of related claims | "The Three Tyrannical Ideas" (contains naturalism + individualism + skill-centrism) |
| **concept** | A contested/defined term with the author's definition and alternatives | "developmental appropriateness: Hirsch argues this lacks scientific basis; proponents argue..." |
| **case** | A specific country/school/implementation with narrative + extracted claims | "France 1975-1985: national knowledge curriculum → highest-achieving, most egalitarian in Europe" |
| **thinker** | An intellectual position the author engages with (not just a name) | "Dewey's *How We Think* (1910): critical thinking as unifying principle → Hirsch says this was debunked" |
| **reference** | A parsed, enriched citation from the endnotes | "Recht & Leslie 1988, Journal of Educational Psychology" |

### Key Design Principles (from calibration, 2026-03-30)

1. **Extract aggressively, defer importance judgment.** Every assertion the author makes is a claim. Importance is a computed property from the argument graph (how many things depend on this? does it have evidence? is it load-bearing?), not an extraction-time judgment.
2. **Unsupported load-bearing claims are the most interesting output.** If a main conclusion depends on a warrant that has no evidence, the public site should highlight this.
3. **Cases hold narratives + claims.** Rich descriptive passages (the French reform story) are preserved as case narratives. Claims are extracted from the interpretive parts. Purely factual context supports claims.
4. **Not everything is a claim.** Anecdotes, contextual descriptions, and metadata ("Peter told me") are not claims. But they may be evidence, case material, or context for a claim.
5. **Per-chapter extraction → cross-chapter convergence as post-processing.** Get per-chapter extraction reliable first. Then cluster/dedup/connect across chapters using limbic.amygdala embedding + LLM verification.

### Edge Types (8)

| Type | Meaning | Example |
|------|---------|---------|
| **supports** | Evidential support | Evidence E → Claim C |
| **opposes** | Direct contradiction | Claim X opposes Claim Y |
| **undermines** | Attacks the warrant, not the claim | "Natural experiment assumption is flawed" undermines the A1→A6 link |
| **depends-on** | Logical prerequisite | A6 depends-on A3 (domain specificity) |
| **objects-to** | Dialectical challenge | Objection O objects-to Claim C |
| **responds-to** | Dialectical reply | Response R responds-to Objection O |
| **refines** | More specific version | Later book's version refines earlier version |
| **instantiates** | A case exemplifying a principle | France (A1) instantiates the general principle |

### Node Attributes

Every node has:
- `id`: Globally unique, hierarchical. `hirsch.wkm.prologue.domain-specificity`
- `claim_level`: `empirical` | `theoretical` | `normative` | `methodological` | `pragmatic` | `meta`
- `source_locations`: Array of `{book, chapter, page, passage}` — because arguments span chapters and books
- `confidence`: How strongly the author asserts this (from hedging language)
- `status`: `stated` | `developed` | `supported` | `challenged` | `conceded` | `abandoned` (tracks across books)

### Canonical Arguments with Book Contributions

Like otak's canonical findings pattern. One canonical node accumulates evidence from multiple locations:

```
canonical: "Domain specificity refutes general skills transfer"
  ├── WKM prologue (preview, p.7)
  ├── WKM chapter 3 (full development, cognitive science evidence)
  ├── Cultural Literacy chapter 4 (first statement, 1987)
  └── The Knowledge Deficit chapter 2 (focused treatment, 2006)
```

### Framework as Composite Node

A framework contains components and can be attacked as a unit or per-component:
```
F1: "The Three Tyrannical Ideas"
  ├── F1a: Naturalism
  ├── F1b: Individualism
  └── F1c: Skill-centrism

A5 attacks F1a specifically
A1 attacks F1 as a whole
```

---

## Extraction Pipeline

### Design Principles

1. **Quality over speed.** One well-extracted book is worth more than ten poorly-extracted books.
2. **Do it right once.** The pipeline should produce extraction quality high enough that it doesn't need re-running. Human calibration is built in, not bolted on.
3. **Argument structure, not just content.** The extraction must capture the dependency graph (what depends on what, where the inference gaps are).
4. **Two-phase extraction.** First pass gets content (claims, evidence, terms). Second pass gets structure (dependencies, warrants, inference chains, objections).

### Phase 1: Book Skeleton (Per Book, ~$0.05)

Before extracting any chapters, extract the book-level argument skeleton from the Prologue/Introduction and Table of Contents. This produces:

- Book thesis (1-3 sentences)
- Chapter-level argument map (what each chapter claims, how chapters connect)
- The author's own preview of the argument structure
- Forward references between chapters
- Key frameworks introduced

This skeleton guides the chapter-by-chapter extraction — we know what arguments to expect.

**Method**: Single LLM call on Prologue + TOC. Human reviews and corrects the skeleton before proceeding.

### Phase 2: Chapter Extraction (Per Chapter, ~$0.10-0.20)

Two sequential LLM calls per chapter:

**Call 1 — Content Extraction** ($0.05-0.10)
Input: Chapter text + chapter's endnotes + book skeleton
Output:
- Claims (main + supporting) with source passages and endnote refs
- Evidence items (studies, data, examples) with evidence type
- Definitions of key terms
- Actors mentioned and their roles
- Cross-chapter references

Prompt design: The extraction prompt must explicitly ask for:
- The claim's type (empirical/theoretical/normative/methodological/pragmatic)
- Source passages (verbatim quotes grounding each claim)
- Endnote numbers for every evidence item
- Qualifier language (hedges, concessions)

**Call 2 — Structure Extraction** ($0.05-0.10)
Input: The output of Call 1 + the book skeleton + results from previously extracted chapters
Output:
- Dependency graph: which claims depend on which
- Warrants connecting evidence to claims (the often-implicit "why does this evidence support this claim?")
- Missing inference steps (explicitly ask: "what intermediate claims are needed between X and Y?")
- Objections Hirsch addresses + his responses
- Objections a reasonable critic would raise that Hirsch does NOT address

Why two calls: Separating content from structure avoids attention degradation. Call 1 focuses on "what does Hirsch say?" Call 2 focuses on "how does it fit together?" Trying to do both at once is what produced the 60%-content-0%-structure result.

### Phase 3: Self-Critique ($0.03-0.05 per chapter)

After extraction, run a critique pass — a separate LLM call that reviews the extraction:

Input: The chapter text + the extraction from Phase 2
Prompt: "You are reviewing an argument extraction. Identify: (1) arguments in the text that were missed, (2) extracted claims that distort what the author wrote, (3) missing inference steps in the dependency graph, (4) warrants that are assumed but not stated, (5) the strongest objection a critic would raise to each main claim."

This replaces the need for multiple external critique agents on every chapter. The critic prompt is designed based on what our three critiques found missing.

**Key insight from the Prologue calibration**: the three biggest blind spots were (a) missing intermediate inference steps, (b) missing warrants, and (c) no counter-arguments. The self-critique prompt is specifically designed to catch these.

### Phase 4: Human Calibration (Per Chapter, ~15-30 min)

The calibration interface shows:
- Left panel: original text with highlighted passages
- Right panel: extracted argument graph with dependency arrows
- For each node: rate completeness (1-5) + accuracy (1-5)
- Flag missing arguments
- Flag wrong relationships

The calibration is NOT optional. It's built into the pipeline. No chapter is "done" until a human has reviewed it.

**Calibration cadence**: Chapters 1-3 get full human review (establishing the extraction quality bar). If quality is consistently high (avg 4+/5), chapters 4-8 get spot-check review (review 50% of claims, all objections, all warrants).

### Phase 4B: Cross-Chapter Convergence (Per Book, after all chapters extracted)

This is a POST-PROCESSING step, separate from per-chapter extraction. Uses `limbic.amygdala` for embedding + clustering + LLM verification.

**Pipeline:**
1. **Embed all claims** from all chapters using `EmbeddingModel.embed_batch()`
2. **Find candidate pairs** via pairwise cosine similarity (threshold ~0.80-0.85)
3. **LLM-classify each pair** into relationship types:
   - `duplicate` — same claim stated identically or near-identically → merge into canonical
   - `refines` — one is a more specific/developed version of the other → link with temporal ordering
   - `develops` — one adds evidence to the other → link as evidence accumulation
   - `overlaps` — they address related but distinct aspects → link as related
   - `distinct` — false positive, no real relationship
4. **Create canonical arguments** that merge duplicates, preserving per-chapter source locations
5. **Build cross-chapter dependency graph** — claims from Chapter 7 may support claims from Chapter 1
6. **Compute importance scores** — importance = f(in-degree in dependency graph, evidence count, warrant count, cross-chapter appearances). NOT a human or LLM judgment at extraction time.
7. **Flag unsupported load-bearing claims** — main conclusions that depend on warrants without evidence

**limbic.amygdala usage:**
- `embed_batch()` for all claims
- Pairwise cosine similarity for candidate detection (same as otak's canonical synthesis)
- Greedy centroid clustering for canonical argument detection
- LLM judge for relationship classification (cascade: embedding pre-filter → LLM verification)

**Potential limbic extension needed:**
- `argument_convergence()` module — takes a list of claims with chapter metadata, returns canonical arguments with relationship graph. Generalizes otak's `canonical_synthesis.py` pattern for argument-level (not just claim-level) convergence.

### Phase 5: Reference Parsing (Per Book, ~$0.50)

After all chapters are extracted:
1. **Parse endnotes** via LLM (Gemini Flash, batches of 25): raw citation → structured {author, title, year, type, container}
2. **Crossref enrichment** via `habanero`: add DOIs, journal metadata, abstracts
3. **Dedup across books**: DOI match first, then fuzzy title+author+year match
4. **Link to claims**: each reference connects to the claims that cite it

### Phase 6: Cross-Book Synthesis (After 2+ Books)

Once two books are extracted:
1. **Canonical argument detection**: embedding similarity on claims across books → LLM-verified dedup
2. **Evolution tracking**: same claim stated differently → link as `refines` with temporal ordering
3. **Evidence accumulation**: merge evidence for the same canonical argument across books
4. **Dialectical index**: which objections are raised in which books, which get responses

---

## Cost Estimates

| Phase | Per Chapter | Per Book (8 chapters) | 4 Books |
|-------|-----------|----------|---------|
| Skeleton | — | $0.05 | $0.20 |
| Content extraction | $0.10 | $0.80 | $3.20 |
| Structure extraction | $0.10 | $0.80 | $3.20 |
| Self-critique | $0.05 | $0.40 | $1.60 |
| Reference parsing | — | $0.50 | $2.00 |
| Cross-book synthesis | — | — | $1.00 |
| **LLM Total** | | **$2.55** | **$11.20** |
| Human calibration | 20 min | 3 hrs | 12 hrs |

Cost is negligible. Human time is the real investment. Budget ~12 hours of calibration time for the first 4 books.

---

## Web Presentation (Separate from Extraction)

### Core Principle

"Read like a book about Hirsch's arguments, with the structure of a database underneath."

### Pages

1. **Landing page**: One-sentence thesis → visual thesis map → three entry paths (Arguments / Evidence / Books)
2. **Argument pages**: Three-layer progressive disclosure (card → inline expansion → full page)
3. **Evidence pages**: Study details, where Hirsch cites it, evidence strength
4. **Book pages**: Per-book summary, what's new vs previous books
5. **Claim evolution pages**: Timeline showing how one argument develops across all books (killer feature)
6. **Bibliography**: Frequency-weighted, topic-clustered, with Crossref/Scholar links
7. **Counter-arguments**: Per-claim, same level as evidence (not ghettoized)

### Design Inspiration

- Our World in Data: topic page structure, clean design
- Gwern.net: inline sidenotes, evidence density, typography
- GiveWell: three-layer depth, evidence strength indicators
- New Things Under the Sun: living literature review format
- Cold Takes: multi-part series with roadmap

### Anti-Patterns

- Force-directed graph visualizations (useless for reading)
- Database search interface (the current otak viewer)
- Kialo-style fragmented debate trees (kills narrative flow)
- Wikipedia "Criticism" ghetto (counter-arguments must be inline)

### Technology

Static site generator (likely Hugo or Astro) + lightweight JS for interactivity. Data model as JSON, rendered at build time. Not a database-backed app.

---

## Reusable Code from Otak

| Component | Source | Reuse? |
|-----------|--------|--------|
| PDF parsing + chapter splitting | `scripts/ingest_book.py` | **Yes** — already configured for this book |
| Notes section parsing | `scripts/ingest_book.py` | **Yes** |
| Endnote parsing | `scripts/ingest_book.py` | **Yes** |
| Extraction schema | `scripts/ingest_book.py` | **Revise** — needs the new node/edge types |
| LLM provider client | `scripts/llm_providers.py` | **Yes** — Gemini Flash for extraction |
| Embedding/search | `limbic.amygdala` | **Yes** — for cross-book dedup |
| Crossref enrichment | `scripts/enrich_crossref.py` | **Yes** — for reference parsing |
| Viewer | `site/` | **Yes** — static HTML, no server needed |
| Storage | JSON files | **Yes** — corpus_consolidated.json |

---

## Cross-Project Lessons

See [`mdg-lessons-2026-03-30.md`](mdg-lessons-2026-03-30.md) — 12 lessons from the MDG project (27 iterative phases, ~9,400 proposals). Key transfers: few-shot prompting (P0), cross-model validation with confidence thresholds (P0), semantic whitening before convergence (P1), reprocess pipeline with `--from` (P1), compound claim splitting (P1).

---

## Immediate Next Steps

1. ~~Manual extraction of Prologue~~ DONE (2026-03-29)
2. ~~Three critiques of extraction~~ DONE (2026-03-29)
3. ~~Calibration interface v1~~ DONE (2026-03-29)
4. **Revise data model** based on critiques → this document
5. **Build extraction pipeline** — two-phase + self-critique
6. **Run on Prologue** — automated extraction, compare to manual
7. **Human calibration** of automated Prologue extraction
8. **Run on Chapter 1** — first chapter-scale test
9. **Iterate extraction prompts** based on Chapter 1 calibration
10. **Process remaining chapters** of Why Knowledge Matters
11. **Build reference parser** for endnotes
12. **Acquire second book** (Cultural Literacy or The Schools We Need)
13. **Cross-book synthesis** — first test of canonical argument detection
14. **Web frontend prototype** — landing page + one argument page

---

## Calibration Results (2026-03-30)

### Prologue v2: 6/14 of user-flagged items captured (43%)
### Chapter 1 v3: 5/10 of user-flagged claims captured (50%)

**Failure modes identified:**
1. **Factual assertions treated as context** — "math scores stable for 17-year-olds", "13-year-olds made progress" — pipeline skips factual setup
2. **Concessions/nuances skipped** — where Hirsch grants something to the other side ("testing DID improve mechanics") before arguing it's insufficient
3. **Prescriptive policy claims missed** — "tests should be based on knowledge-based curricula" (the actionable conclusion)
4. **Compound claims not split** — "a reading test probes initiation into the public sphere" + "any policy that lowers scores is failed" = 2 claims, extracted as 0-1

**New insight from calibration: Research questions as first-class output.**
The user's notes contained 6 deep research questions that the public site should surface:
- How do 13-year-old vs 17-year-old trends fit the theory?
- When are knowledge gaps actually decisive?
- What evidence exists for how schools spend instructional time?
- What does "reading ability" mean and how is it measured?
- What's the iron-man argument for skills-based teaching?
- What's the scholarly reaction to key cited studies?

These questions should be a node type or annotation on the site — "open questions" attached to specific claims.

**New insight: Studies cited in main text (not just endnotes) are likely load-bearing and should be extracted with special care.**

## Research Insights (2026-03-30)

Full analysis: `prototypes/hirsch/research-insights-2026-03-30.md` — synthesizes 25 research notes + 28 knowledge-based-curricula files. Key sections:
- **A. Extraction improvements** — 5 prompt fixes for the 50%→80% recall problem
- **B. Counter-arguments** — Bildung tradition, methodological critiques, Counsell framework
- **C. Supporting evidence** — 10 strongest empirical findings + 5 natural experiments
- **D. Norwegian context** — Sundby & Karseth, Nordic debate mapping
- **G. 13 open questions** the Atlas should surface
- **H. Researchers** to source counter-arguments from (Tier 1 + Tier 2)
- **I. 8 papers/books** to acquire

## Open Questions

1. **How many "canonical arguments" does Hirsch have?** Estimate: 15-25 core claims that recur across books, with 50-100 supporting claims.
2. **What's the right counter-argument source?** Buras (2008), Kohn (1999), Meier (1995) are the main critics. Should we extract from their books too?
3. **How to handle the "What Your Xth Grader Needs to Know" series?** These are curriculum content, not arguments. Probably reference them but don't extract.
4. **Should the site be opinionated or neutral?** The critiques suggest presenting strongest versions of both sides. But this is an "atlas of Hirsch's thought" — neutrality might mean "clear about what is Hirsch's position and what is counter-evidence."
5. **Argdown as authoring format?** The argument mapper suggested it. Could be useful for hand-editing the structure after automated extraction.

---

## Key Decisions Made

| Decision | Rationale |
|----------|-----------|
| Two-phase extraction (content then structure) | Avoids the "60% content, 0% structure" problem from single-pass extraction |
| Self-critique as pipeline step | Catches the three main blind spots (inference gaps, missing warrants, no counter-arguments) without needing multiple external agents per chapter |
| Human calibration built in, not optional | Quality is paramount. 12 hours total for 4 books is acceptable |
| Canonical arguments across books | The killer feature — showing how claims evolve. Follows otak's canonical findings pattern |
| Static site, not database app | Public-facing, fast, linkable, no infrastructure to maintain |
| Purpose-built SQLite, not otak graph | Different data model (7 node types, 8 edge types, argument-centric not claim-centric) |
| LLM for reference parsing, Crossref for enrichment | $0.50 per book, 95%+ accuracy, simpler than GROBID |
