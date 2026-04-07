#!/usr/bin/env python3
"""Cross-book and cross-chapter consolidation for the Hirsch Argument Atlas.

Loads merged.json from all 10 books, deduplicates entities (thinkers, concepts,
cases) across the full corpus, clusters similar claims, and classifies how
arguments evolved from 1977 to 2024.

Usage:
    .venv-otak/bin/python3 prototypes/hirsch/consolidate.py              # full corpus
    .venv-otak/bin/python3 prototypes/hirsch/consolidate.py --book wkm   # single book
    .venv-otak/bin/python3 prototypes/hirsch/consolidate.py --no-llm     # skip evolution classification
"""

import argparse
import json
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"

# ---------------------------------------------------------------------------
# Book registry — ordered chronologically
# ---------------------------------------------------------------------------

BOOKS = {
    "poc":  {"title": "The Philosophy of Composition", "year": 1977, "short": "PoC"},
    "cl":   {"title": "Cultural Literacy", "year": 1987, "short": "CL"},
    "swn":  {"title": "The Schools We Need", "year": 1996, "short": "SWN"},
    "kd":   {"title": "The Knowledge Deficit", "year": 2006, "short": "KD"},
    "the-making-of-americans": {"title": "The Making of Americans", "year": 2010, "short": "MoA"},
    "wkm":  {"title": "Why Knowledge Matters", "year": 2016, "short": "WKM"},
    "how-to-educate-a-citizen": {"title": "How to Educate a Citizen", "year": 2020, "short": "HtEC"},
    "ae":   {"title": "American Ethnicity", "year": 2022, "short": "AE"},
    "sk":   {"title": "Shared Knowledge", "year": 2023, "short": "SK"},
    "re":   {"title": "The Ratchet Effect", "year": 2024, "short": "RE"},
}

BOOKS_BY_YEAR = sorted(BOOKS.items(), key=lambda x: x[1]["year"])

# Stopwords for inverted-index optimization (filtered from candidate generation)
STOPWORDS = frozenset(
    "the a an is are was were be been being have has had do does did will would "
    "shall should may might can could of in to for on with at by from as into "
    "through during before after above below between under over about against "
    "and or but not nor so yet both either neither each every all any few more "
    "most other some such no than too very also just only even still already "
    "that this these those which what who whom whose where when how why if then "
    "because since while although though however therefore moreover furthermore "
    "it its they them their he she his her we our you your i my me us him "
    "one two three many much s t d re ve ll m".split()
)


# ---------------------------------------------------------------------------
# Word overlap similarity
# ---------------------------------------------------------------------------

def _word_set(text: str) -> set:
    return set(re.findall(r'[a-z]+', text.lower()))


def _content_words(text: str) -> set:
    return _word_set(text) - STOPWORDS


def _word_overlap(text_a: str, text_b: str) -> float:
    words_a = _word_set(text_a)
    words_b = _word_set(text_b)
    if not words_a or not words_b:
        return 0.0
    smaller = min(len(words_a), len(words_b))
    if smaller == 0:
        return 0.0
    return len(words_a & words_b) / smaller


def _name_similarity(name_a: str, name_b: str) -> float:
    a = name_a.strip().lower()
    b = name_b.strip().lower()
    if a == b:
        return 1.0
    if a in b or b in a:
        return 0.90
    return _word_overlap(name_a, name_b)


# ---------------------------------------------------------------------------
# Chapter ordering helper
# ---------------------------------------------------------------------------

def _chapter_sort_key(ch: str) -> tuple:
    if ch == "prologue":
        return (-1, "")
    if ch.startswith("epilogue"):
        return (100, ch)
    if ch.startswith("appendix"):
        return (200, ch)
    if ch.startswith("introduction"):
        return (-2, "")
    if ch.startswith("afterword"):
        return (101, ch)
    try:
        return (int(ch), "")
    except ValueError:
        return (999, ch)


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

def load_book_chapters(book_slug: str) -> dict:
    """Load all merged.json files for one book, return {chapter_id: data}."""
    book_dir = DATA_DIR / book_slug
    chapters = {}
    for entry in sorted(book_dir.iterdir()):
        merged = entry / "merged.json"
        if merged.is_file():
            with open(merged) as f:
                chapters[entry.name] = json.load(f)
    return chapters


def load_all_books(book_slugs: list[str] | None = None) -> dict:
    """Load chapters from multiple books. Returns {book_slug: {chapter_id: data}}."""
    if book_slugs is None:
        book_slugs = [slug for slug, _ in BOOKS_BY_YEAR]
    all_books = {}
    for slug in book_slugs:
        book_dir = DATA_DIR / slug
        if book_dir.is_dir():
            chapters = load_book_chapters(slug)
            if chapters:
                all_books[slug] = chapters
    return all_books


# ---------------------------------------------------------------------------
# Entity deduplication
# ---------------------------------------------------------------------------

def _find_merge_groups(items: list, name_field: str, threshold: float) -> list:
    n = len(items)
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    for i in range(n):
        for j in range(i + 1, n):
            name_i = items[i].get(name_field, "")
            name_j = items[j].get(name_field, "")
            if _name_similarity(name_i, name_j) >= threshold:
                union(i, j)

    groups = defaultdict(list)
    for i in range(n):
        groups[find(i)].append((i, items[i]))

    return list(groups.values())


def merge_thinkers(all_thinkers: list, threshold: float = 0.80) -> tuple:
    groups = _find_merge_groups(all_thinkers, "name", threshold)
    canonical = []
    merges = 0

    for group in groups:
        if len(group) > 1:
            merges += len(group) - 1
        items = [item for _, item in group]
        best_name = max((it.get("name", "") for it in items), key=len)

        appearances = []
        for it in items:
            entry = {
                "book": it.get("book", "?"),
                "chapter": it.get("chapter", "?"),
                "key_idea": it.get("key_idea", ""),
                "engagement": it.get("engagement", ""),
                "author_stance": it.get("author_stance", ""),
                "key_passages": it.get("key_passages", []),
            }
            appearances.append(entry)

        book_set = sorted(set(it.get("book", "?") for it in items))
        book_years = sorted(set(
            BOOKS[b]["year"] for b in book_set if b in BOOKS
        ))

        canonical.append({
            "name": best_name,
            "appearances": appearances,
            "books": book_set,
            "book_years": book_years,
            "book_count": len(book_set),
        })

    canonical.sort(key=lambda x: (-x["book_count"], -len(x["appearances"]), x["name"]))
    return canonical, merges


def merge_concepts(all_concepts: list, threshold: float = 0.80) -> tuple:
    groups = _find_merge_groups(all_concepts, "term", threshold)
    canonical = []
    merges = 0

    for group in groups:
        if len(group) > 1:
            merges += len(group) - 1
        items = [item for _, item in group]
        best_term = max((it.get("term", "") for it in items), key=len)

        appearances = []
        for it in items:
            entry = {
                "book": it.get("book", "?"),
                "chapter": it.get("chapter", "?"),
                "author_definition": it.get("author_definition", ""),
                "importance": it.get("importance", ""),
            }
            if it.get("source_passage"):
                entry["source_passage"] = it["source_passage"]
            appearances.append(entry)

        book_set = sorted(set(it.get("book", "?") for it in items))
        book_years = sorted(set(
            BOOKS[b]["year"] for b in book_set if b in BOOKS
        ))

        canonical.append({
            "term": best_term,
            "appearances": appearances,
            "books": book_set,
            "book_years": book_years,
            "book_count": len(book_set),
        })

    canonical.sort(key=lambda x: (-x["book_count"], -len(x["appearances"]), x["term"]))
    return canonical, merges


def merge_cases(all_cases: list, threshold: float = 0.70) -> tuple:
    groups = _find_merge_groups(all_cases, "name", threshold)
    canonical = []
    merges = 0

    for group in groups:
        if len(group) > 1:
            merges += len(group) - 1
        items = [item for _, item in group]
        best_name = max((it.get("name", "") for it in items), key=len)

        appearances = []
        for it in items:
            entry = {
                "book": it.get("book", "?"),
                "chapter": it.get("chapter", "?"),
                "description": it.get("description", ""),
                "key_passages": it.get("key_passages", []),
                "role_in_argument": it.get("role_in_argument", ""),
            }
            if it.get("claims_supported"):
                entry["claims_supported"] = it["claims_supported"]
            if it.get("contestable_aspects"):
                entry["contestable_aspects"] = it["contestable_aspects"]
            appearances.append(entry)

        book_set = sorted(set(it.get("book", "?") for it in items))
        book_years = sorted(set(
            BOOKS[b]["year"] for b in book_set if b in BOOKS
        ))

        canonical.append({
            "name": best_name,
            "appearances": appearances,
            "books": book_set,
            "book_years": book_years,
            "book_count": len(book_set),
        })

    canonical.sort(key=lambda x: (-x["book_count"], -len(x["appearances"]), x["name"]))
    return canonical, merges


# ---------------------------------------------------------------------------
# Claim clustering — inverted-index optimized for 10K+ scale
# ---------------------------------------------------------------------------

def cluster_claims(all_claims: list, threshold: float = 0.55) -> tuple:
    """Cluster similar claims using inverted-index candidate generation.

    Instead of checking all O(n²) pairs, builds a word→[claim_indices] map
    and only checks pairs sharing content words. This reduces pairs from
    ~52M to ~2-5M at 10K claims.
    """
    n = len(all_claims)
    t0 = time.time()

    # Pre-compute word sets (full and content-only)
    word_sets = [_word_set(c.get("text", "")) for c in all_claims]
    content_sets = [ws - STOPWORDS for ws in word_sets]

    # Build inverted index on content words
    word_to_claims = defaultdict(list)
    for i, cws in enumerate(content_sets):
        for w in cws:
            word_to_claims[w].append(i)

    # Filter out very common words (>5% of claims) to avoid huge candidate sets
    max_freq = max(n // 20, 50)
    word_to_claims = {w: idxs for w, idxs in word_to_claims.items() if len(idxs) <= max_freq}

    # Generate candidate pairs (claims sharing at least one content word)
    candidates = set()
    for idxs in word_to_claims.values():
        for a_pos in range(len(idxs)):
            for b_pos in range(a_pos + 1, len(idxs)):
                i, j = idxs[a_pos], idxs[b_pos]
                if i < j:
                    candidates.add((i, j))
                else:
                    candidates.add((j, i))

    t_index = time.time()
    print(f"  Inverted index: {len(candidates):,} candidate pairs from {n} claims ({t_index - t0:.1f}s)")

    # Score candidate pairs
    pairs_matched = 0
    adjacency = defaultdict(list)
    for i, j in candidates:
        ws_i, ws_j = word_sets[i], word_sets[j]
        if not ws_i or not ws_j:
            continue
        smaller = min(len(ws_i), len(ws_j))
        if smaller == 0:
            continue
        overlap = len(ws_i & ws_j) / smaller
        if overlap >= threshold:
            adjacency[i].append((j, overlap))
            adjacency[j].append((i, overlap))
            pairs_matched += 1

    t_score = time.time()
    print(f"  Pair scoring: {pairs_matched:,} matches ({t_score - t_index:.1f}s)")

    # Greedy centroid clustering
    claim_order = sorted(range(n), key=lambda i: -len(adjacency.get(i, [])))
    cluster_assignment = [None] * n
    cluster_centroids = {}
    cluster_members = defaultdict(list)
    next_cluster_id = 0

    for i in claim_order:
        if cluster_assignment[i] is not None:
            continue

        best_cluster = None
        best_score = 0.0
        for neighbor, score in adjacency.get(i, []):
            cid = cluster_assignment[neighbor]
            if cid is not None and score > best_score:
                centroid_ws = cluster_centroids[cid]
                ws_i = word_sets[i]
                if centroid_ws and ws_i:
                    smaller = min(len(centroid_ws), len(ws_i))
                    if smaller > 0:
                        centroid_overlap = len(centroid_ws & ws_i) / smaller
                        if centroid_overlap >= threshold:
                            best_cluster = cid
                            best_score = centroid_overlap

        if best_cluster is not None:
            cluster_assignment[i] = best_cluster
            cluster_members[best_cluster].append(i)
            all_in_cluster = cluster_members[best_cluster]
            longest_idx = max(all_in_cluster, key=lambda idx: len(all_claims[idx].get("text", "")))
            cluster_centroids[best_cluster] = word_sets[longest_idx]
        else:
            cid = next_cluster_id
            next_cluster_id += 1
            cluster_assignment[i] = cid
            cluster_members[cid].append(i)
            cluster_centroids[cid] = word_sets[i]

    # Build cluster objects
    clusters = []
    cluster_map = {}
    multi_member = 0
    multi_book = 0

    for cid, indices in sorted(cluster_members.items()):
        members = []
        for idx in indices:
            c = all_claims[idx]
            members.append({
                "book": c.get("book", "?"),
                "chapter": c.get("chapter", "?"),
                "claim_id": c.get("_global_id", ""),
                "text": c.get("text", ""),
                "claim_level": c.get("claim_level", ""),
            })
            cluster_map[idx] = cid

        canonical_idx = max(indices, key=lambda i: len(all_claims[i].get("text", "")))
        canonical = all_claims[canonical_idx]

        book_set = sorted(set(m["book"] for m in members))
        book_years = sorted(set(BOOKS[b]["year"] for b in book_set if b in BOOKS))

        if len(members) > 1:
            multi_member += 1
        if len(book_set) > 1:
            multi_book += 1

        cluster = {
            "cluster_id": cid,
            "size": len(members),
            "books": book_set,
            "book_years": book_years,
            "book_count": len(book_set),
            "canonical_claim": {
                "book": canonical.get("book", "?"),
                "chapter": canonical.get("chapter", "?"),
                "claim_id": canonical.get("_global_id", ""),
                "text": canonical.get("text", ""),
                "claim_level": canonical.get("claim_level", ""),
            },
            "members": members,
        }
        clusters.append(cluster)

    for i, claim in enumerate(all_claims):
        claim["cluster_id"] = cluster_map.get(i)

    clusters.sort(key=lambda x: (-x["book_count"], -x["size"]))

    t_cluster = time.time()
    stats = {
        "total_claims": n,
        "candidate_pairs": len(candidates),
        "pairs_matched": pairs_matched,
        "total_clusters": len(clusters),
        "singleton_clusters": len(clusters) - multi_member,
        "multi_member_clusters": multi_member,
        "multi_book_clusters": multi_book,
        "time_seconds": round(t_cluster - t0, 1),
    }

    return clusters, all_claims, stats


# ---------------------------------------------------------------------------
# LLM-based evolution classification for cross-book clusters
# ---------------------------------------------------------------------------

def classify_evolution(clusters: list, max_clusters: int = 700) -> list:
    """Classify how arguments evolved across books using Gemini Flash.

    Only processes multi-book clusters. Batches into LLM calls of ~30 clusters.
    """
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))
    from llm_providers import generate_sync

    cross_book = [c for c in clusters if c["book_count"] >= 2]
    if not cross_book:
        print("  No cross-book clusters to classify.")
        return clusters

    cross_book = cross_book[:max_clusters]
    print(f"  Classifying evolution for {len(cross_book)} cross-book clusters...")

    BATCH_SIZE = 30
    total_cost = 0.0
    classified = 0

    schema = {
        "type": "object",
        "properties": {
            "classifications": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "cluster_id": {"type": "integer"},
                        "evolution_type": {
                            "type": "string",
                            "enum": ["repeated", "refined", "evolved", "new_evidence", "narrowed", "broadened"]
                        },
                        "summary": {"type": "string"},
                    },
                    "required": ["cluster_id", "evolution_type", "summary"],
                },
            },
        },
        "required": ["classifications"],
    }

    system = """You are analyzing how an author's arguments evolved across multiple books spanning decades.

For each cluster of similar claims appearing in different books, classify the evolution type:
- "repeated": Same core argument, minor wording changes only
- "refined": Core preserved but evidence, nuance, or precision added
- "evolved": Significant shift in position, emphasis, or framing
- "new_evidence": Same claim but backed by new studies/data in later books
- "narrowed": Broad claim in early work, more qualified/specific later
- "broadened": Specific claim in early work, generalized later

Write a 1-sentence summary of HOW the argument changed (or didn't).
Reference the specific books by their short codes and years."""

    cluster_id_map = {c["cluster_id"]: c for c in cross_book}

    for batch_start in range(0, len(cross_book), BATCH_SIZE):
        batch = cross_book[batch_start:batch_start + BATCH_SIZE]

        prompt_parts = []
        for cl in batch:
            members_text = []
            for m in cl["members"]:
                book_info = BOOKS.get(m["book"], {})
                label = f"{book_info.get('short', m['book'])} ({book_info.get('year', '?')})"
                members_text.append(f"  [{label}] {m['text']}")

            prompt_parts.append(
                f"CLUSTER {cl['cluster_id']} (books: {', '.join(str(y) for y in cl['book_years'])}):\n"
                + "\n".join(members_text)
            )

        prompt = "Classify the evolution of each argument cluster:\n\n" + "\n\n".join(prompt_parts)

        try:
            result, meta = generate_sync(
                prompt, system, schema,
                model="gemini3-flash", max_tokens=4096, thinking_budget=0
            )
            total_cost += meta.get("cost", 0)

            for item in result.get("classifications", []):
                cid = item["cluster_id"]
                if cid in cluster_id_map:
                    cluster_id_map[cid]["evolution_type"] = item["evolution_type"]
                    cluster_id_map[cid]["evolution_summary"] = item["summary"]
                    classified += 1
        except Exception as e:
            print(f"  WARNING: LLM batch failed: {e}")

    print(f"  Classified {classified}/{len(cross_book)} clusters. Cost: ${total_cost:.4f}")
    return clusters


# ---------------------------------------------------------------------------
# Collect all items across books
# ---------------------------------------------------------------------------

def collect_all(all_books: dict) -> dict:
    """Collect all items from all books+chapters with book-scoped global IDs."""
    all_claims = []
    all_evidence = []
    all_warrants = []
    all_counter_arguments = []
    all_thinkers = []
    all_concepts = []
    all_cases = []
    all_dependencies = []
    all_objections = []
    all_missing_steps = []
    all_argument_chains = []
    all_cross_chapter_refs = []
    book_chapter_summaries = {}  # {book_slug: {chapter: summary}}

    for book_slug, _ in BOOKS_BY_YEAR:
        if book_slug not in all_books:
            continue
        chapters = all_books[book_slug]
        book_chapter_summaries[book_slug] = {}

        ordered = sorted(chapters.keys(), key=_chapter_sort_key)

        for chapter in ordered:
            data = chapters[chapter]
            book_chapter_summaries[book_slug][chapter] = data.get("chapter_summary", "")

            prefix = f"{book_slug}:ch{chapter}"

            for claim in data.get("claims", []):
                claim["book"] = book_slug
                claim["chapter"] = chapter
                claim["_global_id"] = f"{prefix}_{claim.get('id', '')}"
                all_claims.append(claim)

            for ev in data.get("evidence", []):
                ev["book"] = book_slug
                ev["chapter"] = chapter
                ev["_global_id"] = f"{prefix}_{ev.get('id', '')}"
                sc = ev.get("supports_claim", "")
                if sc:
                    ev["supports_claim_global"] = f"{prefix}_{sc}"
                all_evidence.append(ev)

            for w in data.get("warrants", []):
                w["book"] = book_slug
                w["chapter"] = chapter
                w["_global_id"] = f"{prefix}_{w.get('id', '')}"
                if w.get("claim_id"):
                    w["claim_id_global"] = f"{prefix}_{w['claim_id']}"
                if w.get("evidence_id"):
                    w["evidence_id_global"] = f"{prefix}_{w['evidence_id']}"
                all_warrants.append(w)

            for ca in data.get("counter_arguments", []):
                ca["book"] = book_slug
                ca["chapter"] = chapter
                ca["_global_id"] = f"{prefix}_{ca.get('id', '')}"
                if ca.get("targets_claim"):
                    ca["targets_claim_global"] = f"{prefix}_{ca['targets_claim']}"
                all_counter_arguments.append(ca)

            for obj in data.get("objections_raised", []):
                obj["book"] = book_slug
                obj["chapter"] = chapter
                obj["_global_id"] = f"{prefix}_{obj.get('id', '')}"
                if obj.get("targets_claim"):
                    obj["targets_claim_global"] = f"{prefix}_{obj['targets_claim']}"
                all_objections.append(obj)

            for t in data.get("thinkers", []):
                t["book"] = book_slug
                t["chapter"] = chapter
                all_thinkers.append(t)

            for c in data.get("concepts", []):
                c["book"] = book_slug
                c["chapter"] = chapter
                all_concepts.append(c)

            for c in data.get("cases", []):
                c["book"] = book_slug
                c["chapter"] = chapter
                all_cases.append(c)

            for dep in data.get("dependencies", []):
                dep["book"] = book_slug
                dep["chapter"] = chapter
                if dep.get("from_id"):
                    dep["from_id_global"] = f"{prefix}_{dep['from_id']}"
                if dep.get("to_id"):
                    dep["to_id_global"] = f"{prefix}_{dep['to_id']}"
                all_dependencies.append(dep)

            for ms in data.get("missing_steps", []):
                ms["book"] = book_slug
                ms["chapter"] = chapter
                if ms.get("from_id"):
                    ms["from_id_global"] = f"{prefix}_{ms['from_id']}"
                if ms.get("to_id"):
                    ms["to_id_global"] = f"{prefix}_{ms['to_id']}"
                all_missing_steps.append(ms)

            for ac in data.get("argument_chains", []):
                ac["book"] = book_slug
                ac["chapter"] = chapter
                if ac.get("chain"):
                    ac["chain_global"] = [f"{prefix}_{cid}" for cid in ac["chain"]]
                if ac.get("conclusion_id"):
                    ac["conclusion_id_global"] = f"{prefix}_{ac['conclusion_id']}"
                all_argument_chains.append(ac)

            for ref in data.get("cross_chapter_refs", []):
                ref["book"] = book_slug
                ref["source_chapter"] = chapter
                all_cross_chapter_refs.append(ref)

    return {
        "claims": all_claims,
        "evidence": all_evidence,
        "warrants": all_warrants,
        "counter_arguments": all_counter_arguments,
        "thinkers": all_thinkers,
        "concepts": all_concepts,
        "cases": all_cases,
        "dependencies": all_dependencies,
        "objections": all_objections,
        "missing_steps": all_missing_steps,
        "argument_chains": all_argument_chains,
        "cross_chapter_refs": all_cross_chapter_refs,
        "book_chapter_summaries": book_chapter_summaries,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Hirsch Argument Atlas — Consolidation")
    parser.add_argument("--book", type=str, help="Process single book by slug (e.g., wkm)")
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM evolution classification")
    args = parser.parse_args()

    if args.book:
        book_slugs = [args.book]
        mode = f"single book: {args.book}"
        output_file = DATA_DIR / args.book / "book_consolidated.json"
    else:
        book_slugs = None  # all books
        mode = "full corpus (10 books)"
        output_file = DATA_DIR / "corpus_consolidated.json"

    print("=" * 70)
    print(f"Hirsch Argument Atlas — Consolidation [{mode}]")
    print("=" * 70)

    # 1. Load
    all_books = load_all_books(book_slugs)
    print(f"\nLoaded {len(all_books)} books:")
    for slug, chapters in all_books.items():
        info = BOOKS.get(slug, {})
        total_claims = sum(len(ch.get("claims", [])) for ch in chapters.values())
        print(f"  {info.get('short', slug):>5s} ({info.get('year', '?')}) — {len(chapters):2d} chapters, {total_claims:5d} claims  [{slug}]")

    # 2. Collect
    all_data = collect_all(all_books)

    counts = {
        "claims": len(all_data["claims"]),
        "evidence": len(all_data["evidence"]),
        "warrants": len(all_data["warrants"]),
        "counter_arguments": len(all_data["counter_arguments"]),
        "objections": len(all_data["objections"]),
        "dependencies": len(all_data["dependencies"]),
        "missing_steps": len(all_data["missing_steps"]),
        "argument_chains": len(all_data["argument_chains"]),
        "cross_chapter_refs": len(all_data["cross_chapter_refs"]),
        "thinkers": len(all_data["thinkers"]),
        "concepts": len(all_data["concepts"]),
        "cases": len(all_data["cases"]),
    }

    print(f"\n--- Raw totals ---")
    for k, v in counts.items():
        print(f"  {k:25s} {v:6,d}")

    # 3. Entity dedup
    print(f"\n{'=' * 70}")
    print("ENTITY DEDUPLICATION")
    print("=" * 70)

    canonical_thinkers, thinker_merges = merge_thinkers(all_data["thinkers"])
    print(f"\nThinkers: {counts['thinkers']} raw → {len(canonical_thinkers)} canonical ({thinker_merges} merges)")
    multi_book_thinkers = [t for t in canonical_thinkers if t["book_count"] > 1]
    if multi_book_thinkers:
        print(f"  Multi-book thinkers ({len(multi_book_thinkers)}):")
        for t in multi_book_thinkers[:20]:
            years = ", ".join(str(y) for y in t["book_years"])
            print(f"    {t['name']:30s} {t['book_count']} books ({years})")

    canonical_concepts, concept_merges = merge_concepts(all_data["concepts"])
    print(f"\nConcepts: {counts['concepts']} raw → {len(canonical_concepts)} canonical ({concept_merges} merges)")
    multi_book_concepts = [c for c in canonical_concepts if c["book_count"] > 1]
    if multi_book_concepts:
        print(f"  Multi-book concepts ({len(multi_book_concepts)}):")
        for c in multi_book_concepts[:20]:
            years = ", ".join(str(y) for y in c["book_years"])
            print(f"    {c['term']:40s} {c['book_count']} books ({years})")

    canonical_cases, case_merges = merge_cases(all_data["cases"])
    print(f"\nCases: {counts['cases']} raw → {len(canonical_cases)} canonical ({case_merges} merges)")
    multi_book_cases = [c for c in canonical_cases if c["book_count"] > 1]
    if multi_book_cases:
        print(f"  Multi-book cases ({len(multi_book_cases)}):")
        for c in multi_book_cases[:20]:
            years = ", ".join(str(y) for y in c["book_years"])
            print(f"    {c['name']:40s} {c['book_count']} books ({years})")

    # 4. Claim clustering
    print(f"\n{'=' * 70}")
    print("CLAIM CLUSTERING")
    print("=" * 70)

    clusters, annotated_claims, cluster_stats = cluster_claims(all_data["claims"])
    print(f"\n  Total claims:            {cluster_stats['total_claims']:,}")
    print(f"  Candidate pairs:         {cluster_stats['candidate_pairs']:,}")
    print(f"  Pairs matched:           {cluster_stats['pairs_matched']:,}")
    print(f"  Total clusters:          {cluster_stats['total_clusters']:,}")
    print(f"  Singleton clusters:      {cluster_stats['singleton_clusters']:,}")
    print(f"  Multi-member clusters:   {cluster_stats['multi_member_clusters']:,}")
    print(f"  Multi-book clusters:     {cluster_stats['multi_book_clusters']:,}")
    print(f"  Time:                    {cluster_stats['time_seconds']}s")

    # Show top cross-book clusters
    cross_book_clusters = [c for c in clusters if c["book_count"] > 1]
    if cross_book_clusters:
        print(f"\n  Top cross-book clusters (showing up to 20):")
        for cl in cross_book_clusters[:20]:
            canonical_text = cl["canonical_claim"]["text"]
            if len(canonical_text) > 70:
                canonical_text = canonical_text[:67] + "..."
            years = ", ".join(str(y) for y in cl["book_years"])
            print(f"    [{cl['book_count']} books, {years}] {canonical_text}")
            for m in cl["members"]:
                if m["claim_id"] != cl["canonical_claim"]["claim_id"]:
                    mt = m["text"]
                    if len(mt) > 60:
                        mt = mt[:57] + "..."
                    book_short = BOOKS.get(m["book"], {}).get("short", m["book"])
                    print(f"      {book_short:>5s}: {mt}")

    # 5. Evolution classification (LLM)
    if not args.no_llm and cross_book_clusters:
        print(f"\n{'=' * 70}")
        print("ARGUMENT EVOLUTION CLASSIFICATION")
        print("=" * 70)
        clusters = classify_evolution(clusters)

        # Show evolution summary
        evo_counts = defaultdict(int)
        for c in cross_book_clusters:
            evo_type = c.get("evolution_type", "unclassified")
            evo_counts[evo_type] += 1
        print(f"\n  Evolution types:")
        for etype, cnt in sorted(evo_counts.items(), key=lambda x: -x[1]):
            print(f"    {etype:20s} {cnt}")

        print(f"\n  Notable evolutions:")
        for cl in cross_book_clusters[:10]:
            if cl.get("evolution_summary"):
                canonical_text = cl["canonical_claim"]["text"]
                if len(canonical_text) > 60:
                    canonical_text = canonical_text[:57] + "..."
                print(f"    [{cl.get('evolution_type', '?'):10s}] {canonical_text}")
                print(f"      → {cl['evolution_summary']}")

    # 6. Build output
    print(f"\n{'=' * 70}")
    print("OUTPUT")
    print("=" * 70)

    clean_claims = [{k: v for k, v in c.items()} for c in annotated_claims]

    books_processed = []
    for slug, _ in BOOKS_BY_YEAR:
        if slug in all_books:
            info = BOOKS[slug]
            books_processed.append({
                "slug": slug,
                "title": info["title"],
                "year": info["year"],
                "short": info["short"],
                "chapters": len(all_books[slug]),
                "claims": sum(len(ch.get("claims", [])) for ch in all_books[slug].values()),
            })

    output = {
        "corpus": "E.D. Hirsch Jr. — Complete Works (1977–2024)",
        "books_processed": books_processed,
        "book_chapter_summaries": all_data["book_chapter_summaries"],
        "thinkers": canonical_thinkers,
        "concepts": canonical_concepts,
        "cases": canonical_cases,
        "claim_clusters": [c for c in clusters if c["size"] > 1],
        "cross_book_clusters": [c for c in clusters if c["book_count"] > 1],
        "all_claims": clean_claims,
        "all_evidence": all_data["evidence"],
        "all_warrants": all_data["warrants"],
        "all_counter_arguments": all_data["counter_arguments"],
        "all_objections_raised": all_data["objections"],
        "all_dependencies": all_data["dependencies"],
        "all_missing_steps": all_data["missing_steps"],
        "all_argument_chains": all_data["argument_chains"],
        "all_cross_chapter_refs": all_data["cross_chapter_refs"],
        "stats": {
            "books_processed": len(all_books),
            "total_chapters": sum(len(chs) for chs in all_books.values()),
            **counts,
            "canonical_thinkers": len(canonical_thinkers),
            "thinker_merges": thinker_merges,
            "multi_book_thinkers": len(multi_book_thinkers),
            "canonical_concepts": len(canonical_concepts),
            "concept_merges": concept_merges,
            "multi_book_concepts": len(multi_book_concepts),
            "canonical_cases": len(canonical_cases),
            "case_merges": case_merges,
            "multi_book_cases": len(multi_book_cases),
            "claim_clustering": cluster_stats,
        },
    }

    with open(output_file, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    file_size = output_file.stat().st_size
    print(f"\n  Written to: {output_file}")
    print(f"  File size:  {file_size / 1024 / 1024:.1f} MB")

    # Summary
    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print("=" * 70)
    print(f"  Books:       {len(all_books)} ({books_processed[0]['year']}–{books_processed[-1]['year']})")
    print(f"  Chapters:    {sum(len(chs) for chs in all_books.values())}")
    print(f"  Claims:      {counts['claims']:,} ({cluster_stats['multi_member_clusters']} clusters, {cluster_stats['multi_book_clusters']} cross-book)")
    print(f"  Evidence:    {counts['evidence']:,}")
    print(f"  Warrants:    {counts['warrants']:,}")
    print(f"  Counter-args:{counts['counter_arguments']:,}")
    print(f"  Thinkers:    {counts['thinkers']} → {len(canonical_thinkers)} ({thinker_merges} merges, {len(multi_book_thinkers)} multi-book)")
    print(f"  Concepts:    {counts['concepts']} → {len(canonical_concepts)} ({concept_merges} merges, {len(multi_book_concepts)} multi-book)")
    print(f"  Cases:       {counts['cases']} → {len(canonical_cases)} ({case_merges} merges, {len(multi_book_cases)} multi-book)")
    print(f"  Arg chains:  {counts['argument_chains']:,}")
    print(f"  Dependencies:{counts['dependencies']:,}")


if __name__ == "__main__":
    main()
