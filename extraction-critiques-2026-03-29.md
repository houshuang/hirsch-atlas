# Extraction Critiques — Prologue, 2026-03-29

Three independent critiques of the manual Prologue extraction. Full text in task output files.

## Education Scholar Critique

**Verdict: "Captures roughly 60% of the argumentative content and almost none of the argumentative structure."**

### Missing arguments (HIGH priority)
1. **The verbal-gap-as-knowledge-gap mechanism** — arguably the central empirical claim. Without it, A1 and A2 are just correlations.
2. **Six frustrations enumerated and mapped to three ideas** — the structural backbone of the prologue, never made explicit.
3. **Mutual reinforcement of the three ideas** — not three independent errors but an interlocking system (naturalism → individualism → skill-centrism fills the void).
4. **Why these ideas persist despite being wrong** — sociology of belief, alignment with American values. Explains the "tyranny" in the title.
5. **Intellectual history genealogy** — Rousseau → Romantic tradition → Dewey → education schools.

### Granularity fixes
- A1 + A2 should merge into one argument with two evidence instances (same mechanism, different countries)
- A5 conflates three distinct claims (learning styles, differentiated curriculum, Finland/Japan comparison)
- A7 is not an argument — reclassify as metadata

### Structural gaps
- No relationships between arguments captured
- Warrants missing throughout — especially for A3 (the mechanism) and A6 (why communal knowledge → autonomy)
- Normative/empirical/causal boundary not sharp enough
- No counter-arguments from the other side

## Argument Mapping Critique

**Verdict: "The extraction captures the content well. What it lacks is the structure of argumentation itself."**

### Dependency graph (not a flat list)
```
A3 (domain specificity) = theoretical backbone
  ↓
A1 + A2 = parallel empirical supports (via unstated intermediate: "content removal causes harm")
  ↓
A5 = defeater of opposing position (not direct support)
A4 = internal critique (fails on own terms) = also a defeater
  ↓
A6 = prescriptive conclusion (depends on A3 + A1/A2)
A7 = not an argument at all
```

### Four missing inference steps
1. **The causal mechanism** — from "France changed policy" to "curriculum change caused decline" (the load-bearing assumption)
2. **Generalization from cases to principle** — two countries at different times making similar changes = natural experiment (methodological claim)
3. **From "skills don't transfer" to "knowledge is what matters"** — requires intermediate steps
4. **From "communal knowledge is valuable" to "schools should teach specific shared curriculum"** — policy premises missing

### Structural changes needed
- 7 node types: Claim, Evidence, Warrant, Objection, Response, Framework, Definition
- 8 edge types: supports, opposes, undermines, depends-on, objects-to, responds-to, refines, instantiates
- Frameworks as composite nodes (can be attacked as unit or per-component)
- claim_level attribute: empirical, theoretical, normative, methodological, pragmatic, meta

### Scalability (critical for book-scale)
- Arguments span chapters — need canonical arguments with chapter contributions
- Build top-level argument skeleton from Prologue first, then populate chapter by chapter
- Expect 200-400 nodes for full book
- Running dialectical index tracking which objections are open/responded/conceded

## UX/Presentation Critique

**Verdict: "This site should read like a book about Hirsch's arguments, with the structure of a database underneath."**

### Landing page
- One-sentence thesis + visual thesis map (horizontal flow) + three entry paths (Arguments / Evidence / Books)
- NOT a list of arguments

### Three-layer progressive disclosure per argument
1. **Card** (always visible): claim + type badge + book appearances
2. **Inline expansion** (on click): evidence, reasoning, qualifications — in natural language, NOT Toulmin tables
3. **Full page** (separate URL): every passage, every citation, counter-arguments

### Cross-book connections (the killer feature)
- "Claim Recurrence" timelines showing evolution across 10 books
- Book comparison views (shared/new/dropped arguments, evidence growth)

### Evidence presentation
- Inline expansion with anchor links (Gwern pattern)
- Never separate pages, never hover-only

### Critic layer
- Per-claim counter-arguments at same level as evidence, NOT ghettoized in "Criticism" section

### Sites to steal from
1. Our World in Data — topic page structure, clean design
2. Gwern.net — inline sidenotes, evidence density, typography
3. New Things Under the Sun — living literature review format
4. GiveWell — three-layer depth, evidence strength indicators
5. Stanford Encyclopedia of Philosophy — authoritative treatment of positions
6. Cold Takes — multi-part series structure with roadmap

### Anti-patterns to avoid
- Knowledge graph explorer (force-directed graphs = useless)
- Database search interface (otak viewer)
- Wall of cards (Notion-style, everything equally unimportant)
- Kialo-style debate tree (kills narrative flow)
- Wikipedia "Criticism" ghetto
