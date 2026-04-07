# Lessons from MDG for the Hirsch Argument Atlas

**Date**: 2026-03-30
**Source**: Analysis of `/Users/stian/src/mdg/` — 27 iterative phases, ~60 scripts, production SvelteKit SPA

## Context

MDG (Miljøpartiet De Grønne local program analysis) is a production system for extracting, classifying, embedding, and presenting ~9,400 structured policy proposals from 128 Norwegian municipal election programs. It went through 27 iterative phases, solving many of the same problems Hirsch now faces: LLM extraction quality, validation methodology, novelty/similarity computation, cross-document linkage, and web presentation.

Both projects share the same core pipeline: **LLM extraction → embedding → similarity → classification → web presentation.** The most transferable lessons are about validation methodology, embedding hygiene, and iterative development — all domain-independent.

---

## Lesson 1: Validation Framework — The Six Layers

MDG's most mature contribution is its layered validation framework. Each layer catches a different class of error:

| Layer | MDG | Hirsch Equivalent |
|-------|-----|-------------------|
| L1: Coverage | 127/360 kommuner tracked | Chapter coverage (8/8 chapters extracted?) |
| L2: Text extraction | Word count vs source, encoding checks | PDF parsing quality — page ranges correct? Endnotes separated? |
| L3: Atomicity | Proposals 60-100 chars median, flag >200 or <30 | Claim granularity — compound claims not split, or over-split |
| L4: Cleanliness | Zero encoding artifacts (Ã/Â regex) | OCR artifacts, quote marks, citation format consistency |
| L5: Classification | Inter-annotator κ=0.862 on topics | Claim_level accuracy (empirical/theoretical/normative), edge type accuracy |
| L6: Interfaces | 15+ routes, dimensional scores | Argument page quality, dependency graph navigability |

**Actionable:** Define these layers explicitly before scaling beyond Chapter 1. The current calibration (43-50% recall) is an L5 problem but L2-L3 problems may be hiding underneath.

---

## Lesson 2: Cross-Model Validation with Confidence Thresholds

MDG's strongest quality move was cross-model validation:
- Re-classify a sample with an independent LLM call
- Only apply fixes above a confidence threshold (0.90 for topics, 0.95 for subjective dimensions)
- Log all changes to a changelog with before/after values

**Results:** 1,188 fixes for $1.25. Topic reclassifications (300), specificity adjustments (387), is_local corrections (469).

**Actionable:** After extraction, run a separate validation pass where a different model (or same model with different prompt) reviews each claim:
- Is this really a claim vs context/anecdote? (the factual-assertion-as-context failure mode)
- Is the claim_level correct?
- Are the dependency edges plausible?
- Only apply auto-fixes above 0.90 confidence

This is cheap (~$0.01/chapter) and directly addresses the 50% recall problem — many "missed" claims may be extracted but misclassified as context.

---

## Lesson 3: Semantic Whitening is Essential Before Similarity

MDG discovered that raw E5 embeddings have anisotropy — all vectors cluster tightly (cosine sim 0.77-0.88), making similarity scores meaningless. PCA whitening fixed this (mean cosine 0.80 → 0.24).

**Directly relevant to Phase 4B** (cross-chapter convergence). When finding candidate pairs for canonical argument detection:
- Raw embedding similarity will produce massive false positives
- Must apply whitening before pairwise comparison
- `limbic.amygdala` already has this (`semantic_whitening` in embed module)

**Also:** Strip page numbers and chapter references from claim text before embedding to avoid spurious matches on "Chapter 3" or "p. 47" (MDG learned this with kommune names and dates).

---

## Lesson 4: Heuristic Classifiers are Tempting but Fragile

MDG's national↔local linkage classifier used keyword heuristics (contradiction pairs like "forby"/"tillate"). Result: **37.5% false positive rate** at the initial threshold.

**Lesson:** The edge type classification (supports, opposes, undermines, depends-on) should use LLM classification, not heuristics. The cost difference is negligible at Hirsch's scale (hundreds of edges, not thousands).

---

## Lesson 5: Subjective Dimensions Need Special Treatment

MDG found that subjective dimensions have lower inter-annotator agreement (specificity κ=0.619 vs topic κ=0.862). They handled this by:
- Using higher confidence thresholds for subjective fixes (0.95 vs 0.90)
- Adding keyword-based heuristics as a second signal
- Accepting that some dimensions will always be noisier

**For Hirsch:** The `confidence` field and `claim_level` are subjective. Expect lower agreement. Use hedging language detection as a heuristic signal. Accept ~60-70% accuracy and design the UI to handle misclassification gracefully.

---

## Lesson 6: The Reprocess Pipeline Pattern

MDG built `reprocess_kommune.py` — a unified pipeline with `--from` to resume from any phase:
```
extract → load-db → embed → linkages → analytics → spa-build → deploy
```

**Actionable:** Build an equivalent for Hirsch:
```
parse-pdf → extract-content → extract-structure → self-critique → validate → embed → converge → build-site
```

MDG's experiment log shows this was built in Phase 8 and immediately accelerated development. The `--from` flag prevents the "re-run everything" tax when iterating on one phase.

---

## Lesson 7: Few-Shot Examples in Extraction Prompts

MDG's SOTA comparison identified zero-shot prompting as their biggest gap. Ornstein et al. (2025) showed few-shot outperforms zero-shot for political text classification.

**Directly actionable:** The current extraction prompts are zero-shot. The calibration data (24 human-flagged items) can serve as few-shot examples:
- Include 3-5 examples of claims the pipeline MISSED (factual assertions, concessions, prescriptive claims)
- Include 1-2 examples of correctly extracted claims for contrast
- This directly attacks the 50% recall problem

---

## Lesson 8: Novelty Scoring → Importance Scoring

MDG's novelty formula: `(0.4 * global_distance + 0.6 * within_topic_distance) * specificity_weight`

**Transfer to Hirsch as computed importance:**
- `global_distance` → how unique is this claim across all of Hirsch's work? (high = novel contribution)
- `within_topic_distance` → how unique within its domain? (high = distinctive angle)
- `specificity_weight` → replace with `evidence_weight` (claims with evidence > bare assertions)

This gives a computed importance score without LLM judgment at extraction time — exactly what PROJECT.md calls for ("importance is a computed property from the argument graph").

---

## Lesson 9: Pre-Computed Analytics for Static Site

MDG pre-computes all analytics as JSON files and bundles a SQLite DB for browser-side querying via sql.js (SQLite in WebAssembly). The 10MB browser DB serves 9,400 proposals with FTS + semantic search, no server needed.

**Validated architecture for Hirsch's static site:**
- Pre-compute the argument dependency graph as JSON
- Pre-compute cross-chapter convergence data
- Bundle a small SQLite DB with claims + edges for browser-side search
- sql.js means full relational queries in the browser with zero backend

---

## Lesson 10: One Bias Per Phase

MDG's 27-phase experiment log shows a pattern: **each phase discovers one systematic bias, implements one targeted fix, and measures the impact.**

- Phase 4: Discovered embedding anisotropy → whitening → cosine sim spread improved
- Phase 8: Discovered PDF encoding bugs → switched to PyMuPDF → 0 artifacts
- Phase 10: Discovered specificity miscalibration (79% "concrete") → keyword heuristics → 65.5%
- Phase 11: Discovered false positive linkages → raised threshold 0.90→0.92

**For Hirsch:** Don't try to fix all 4 failure modes at once. Pick one (e.g., "factual assertions treated as context"), build a targeted fix, measure recall on the 24 ground truth items, then move to the next. The autoresearch plan in SESSION-LOG.md is the right approach — MDG validates it.

---

## Lesson 11: Compound Splitting is Real

MDG implemented compound splitting heuristics and found ~5% of proposals needed splitting. This maps to Hirsch's "compound claims not split" failure mode.

**Actionable:** Post-extraction compound detection pass:
- Flag claims >150 chars or containing discourse markers ("and", "but", "moreover", "furthermore")
- LLM-verify: "Does this contain two distinct claims? If so, split."
- Cost: negligible (a few cents per chapter)

---

## Lesson 12: Fewer Categories Beat Many for LLM Classification

MDG tried MARPOR's 56-category hierarchical scheme (F1 0.14-0.21). Their flat 22-topic scheme achieved κ=0.862.

**For Hirsch:** The current 6 claim_levels (empirical, theoretical, normative, methodological, pragmatic, meta) may be too fine-grained for reliable classification. Consider collapsing to 3-4 for extraction, or accepting lower agreement on this dimension.

---

## Priority Actions

| Priority | Action | Source | Expected Impact |
|----------|--------|--------|-----------------|
| **P0** | Add few-shot examples from calibration data to prompts | L7 | Recall 50% → 65-70% |
| **P0** | Add cross-model validation pass after extraction | L2 | Catches misclassified claims, ~$0.01/ch |
| **P1** | Build reprocess pipeline with `--from` resume | L6 | Faster prompt iteration |
| **P1** | Apply semantic whitening before convergence | L3 | Prevents false positive canonicals |
| **P1** | Add compound claim detection + splitting | L11 | Fixes one of four failure modes |
| **P2** | Use LLM for edge classification | L4 | Higher accuracy on edge types |
| **P2** | Compute importance from graph structure | L8 | MDG novelty formula transferable |
| **P2** | Pre-compute analytics + sql.js for browser | L9 | Validated static site architecture |
| **P3** | Define 6-layer validation framework | L1 | Systematic quality tracking |
| **P3** | Accept lower accuracy on subjective dims | L5 | Don't over-invest in claim_level |

---

## What MDG Did That Hirsch Should NOT Copy

1. **Heuristic classification** — At Hirsch's scale (hundreds of items), LLM classification is affordable and more accurate than keyword heuristics.
2. **22-topic flat taxonomy** — Right for policy proposals, wrong for argument structure. Hirsch's 10 node types and 8 edge types are the right granularity.
3. **Zero-shot extraction** — MDG acknowledged this as their biggest gap. Start with few-shot from day one using existing calibration data.
4. **Deferred structural extraction** — MDG added rhetorical dimensions in Phase 10 as an add-on. For Hirsch, warrants/objections/dependencies are core, not optional. Extract them in the main pipeline.
