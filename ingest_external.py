#!/usr/bin/env python3
"""Extract external research findings and match to Hirsch argument clusters.

Reads research-data.js from knowledge-based-curricula, filters to substantive
findings, and matches to Hirsch's cross-book argument clusters by keyword overlap.

Outputs external_research.json — a SEPARATE file that never touches corpus_consolidated.json.

Usage:
    .venv-otak/bin/python3 prototypes/hirsch/ingest_external.py
"""

import json
import re
from collections import defaultdict
from pathlib import Path

RESEARCH_DATA = Path("/Users/stian/src/research/knowledge-based-curricula/data/research-data.js")
CORPUS_DATA = Path(__file__).parent / "data" / "corpus_consolidated.json"
OUTPUT = Path(__file__).parent / "data" / "external_research.json"

# ── Theme mapping: external themes → Hirsch argument topics ──────
# These map the external research's theme taxonomy to keyword groups
# that overlap with Hirsch's vocabulary.

HIRSCH_TOPICS = {
    "reading_knowledge": {
        "label": "Reading Is Knowledge",
        "description": "Whether reading comprehension is a general skill or depends on domain-specific background knowledge",
        "keywords": {"reading", "comprehension", "background knowledge", "domain", "skill",
                     "decoding", "vocabulary", "literacy", "fluency", "general skill"},
    },
    "curriculum_content": {
        "label": "Curriculum & Content",
        "description": "What should be taught — shared knowledge, specific content, curriculum design",
        "keywords": {"curriculum", "content", "knowledge-rich", "core knowledge", "sequence",
                     "coherent", "scope", "syllabus", "subject matter", "common core",
                     "standards", "specific content"},
    },
    "achievement_gap": {
        "label": "Achievement Gaps & Equity",
        "description": "Whether knowledge-based curricula narrow or widen achievement gaps",
        "keywords": {"achievement gap", "equity", "disadvantaged", "poverty", "socioeconomic",
                     "gap", "low-income", "minority", "demographic", "inequality", "disparity"},
    },
    "progressive_critique": {
        "label": "Progressive Education Critique",
        "description": "Hirsch's critique of child-centered, constructivist, skills-based approaches",
        "keywords": {"progressive", "child-centered", "constructivist", "discovery", "dewey",
                     "romantic", "naturalism", "developmentally appropriate", "skills-based",
                     "guide on the side", "student-centered"},
    },
    "testing_assessment": {
        "label": "Testing & Assessment",
        "description": "How knowledge is measured, test score trends, assessment design",
        "keywords": {"test", "assessment", "sat", "naep", "score", "measure", "evaluation",
                     "standardized", "verbal", "decline"},
    },
    "international_comparison": {
        "label": "International Comparisons",
        "description": "Cross-national evidence from France, Japan, Finland, etc.",
        "keywords": {"france", "french", "japan", "japanese", "finland", "finnish", "singapore",
                     "korea", "korean", "china", "chinese", "england", "pisa", "timss",
                     "international", "nordic", "scandinavian", "norway", "norwegian", "sweden"},
    },
    "cognitive_science": {
        "label": "Cognitive Science",
        "description": "Evidence from cognitive psychology about learning, memory, transfer",
        "keywords": {"cognitive", "memory", "transfer", "schema", "chunking", "spaced",
                     "retrieval", "working memory", "long-term memory", "attention",
                     "willingham", "piaget", "cognitive load"},
    },
    "teacher_pedagogy": {
        "label": "Teaching & Pedagogy",
        "description": "How to teach knowledge effectively — direct instruction, lesson design",
        "keywords": {"teacher", "pedagogy", "instruction", "direct instruction", "lesson",
                     "classroom", "teaching", "professional development", "explicit"},
    },
}


def load_research_data():
    with open(RESEARCH_DATA) as f:
        raw = f.read()
    raw = raw.replace("window.RESEARCH_DATA = ", "", 1).rstrip().rstrip(";")
    return json.loads(raw)


def load_corpus():
    with open(CORPUS_DATA) as f:
        return json.load(f)


def _words(text: str) -> set:
    return set(re.findall(r'[a-z]+', text.lower()))


def _bigrams(text: str) -> set:
    """Extract 2-word phrases for better matching on compound terms."""
    words = re.findall(r'[a-z]+', text.lower())
    return set(f"{words[i]} {words[i+1]}" for i in range(len(words) - 1))


def match_topic(text: str) -> list:
    """Match text to Hirsch topics by keyword overlap. Returns [(topic_id, score)]."""
    text_words = _words(text)
    text_bigrams = _bigrams(text)
    matches = []
    for topic_id, topic in HIRSCH_TOPICS.items():
        score = 0
        for kw in topic["keywords"]:
            if " " in kw:  # multi-word keyword
                if kw in text_bigrams or kw in text.lower():
                    score += 2  # bigram matches are more specific
            elif kw in text_words:
                score += 1
        if score >= 2:  # require at least 2 keyword hits
            matches.append((topic_id, score))
    matches.sort(key=lambda x: -x[1])
    return matches


def match_to_clusters(items: list, clusters: list) -> dict:
    """Match external items to Hirsch cross-book clusters by topic affinity.

    Returns {cluster_id: [item_indices]}.
    """
    # Pre-compute cluster topics
    cluster_topics = {}
    for cl in clusters:
        # Combine all member texts for matching
        all_text = " ".join(m.get("text", "") for m in cl["members"])
        all_text += " " + cl["canonical_claim"].get("text", "")
        topics = match_topic(all_text)
        cluster_topics[cl["cluster_id"]] = {t[0] for t in topics}

    # Match items to clusters via shared topics
    cluster_matches = defaultdict(list)
    for idx, item in enumerate(items):
        item_topics = {t[0] for t, _ in [match_topic(item.get("text", ""))]}
        # Actually compute properly
        item_topic_matches = match_topic(item.get("text", ""))
        item_topic_set = {t for t, _ in item_topic_matches}

        for cl in clusters:
            cid = cl["cluster_id"]
            shared = item_topic_set & cluster_topics.get(cid, set())
            if shared:
                cluster_matches[cid].append(idx)

    return cluster_matches


def main():
    print("=" * 60)
    print("External Research Ingestion")
    print("=" * 60)

    # 1. Load
    research = load_research_data()
    corpus = load_corpus()
    all_items = research["items"]
    clusters = corpus["cross_book_clusters"]

    print(f"Research items: {len(all_items)}")
    print(f"Hirsch cross-book clusters: {len(clusters)}")

    # 2. Filter to substantive findings
    KEEP_TYPES = {
        "Critique / Debate",
        "Randomized Trial",
        "Meta-analysis",
        "Empirical Study",
        "Longitudinal / Quasi-Experimental",
        "Implementation Case",
    }
    MIN_LENGTH = 80

    filtered = []
    for item in all_items:
        if item.get("evidenceType") not in KEEP_TYPES:
            continue
        text = item.get("text", "")
        if len(text) < MIN_LENGTH:
            continue
        # Skip items that are just headers or metadata
        if text.startswith("##") or text.startswith("---"):
            continue
        filtered.append(item)

    print(f"After filtering: {len(filtered)} items")

    # Distribution
    from collections import Counter
    type_counts = Counter(i["evidenceType"] for i in filtered)
    for t, c in type_counts.most_common():
        print(f"  {t}: {c}")

    # 3. Match to Hirsch topics
    topic_counts = Counter()
    items_with_topics = []
    for item in filtered:
        topics = match_topic(item["text"])
        if topics:
            item_out = {
                "id": item["id"],
                "text": item["text"][:500],  # cap length
                "evidence_type": item["evidenceType"],
                "strength": item["strength"],
                "themes": item.get("themes", []),
                "countries": item.get("countries", []),
                "source_urls": item.get("sourceUrls", []),
                "section": item.get("sectionTitle", ""),
                "document": item.get("documentShortTitle", ""),
                "hirsch_topics": [{"topic": t, "score": s} for t, s in topics[:3]],
            }
            items_with_topics.append(item_out)
            for t, _ in topics:
                topic_counts[t] += 1

    print(f"\nItems matched to Hirsch topics: {len(items_with_topics)}")
    print("Topic distribution:")
    for t, c in topic_counts.most_common():
        label = HIRSCH_TOPICS[t]["label"]
        print(f"  {label}: {c}")

    # 4. Group by primary topic
    by_topic = defaultdict(list)
    for item in items_with_topics:
        primary = item["hirsch_topics"][0]["topic"]
        by_topic[primary].append(item)

    # 5. Tag clusters with Hirsch topics
    print(f"\nTagging {len(clusters)} cross-book clusters with topics...")
    cluster_topics = {}
    clusters_per_topic = defaultdict(list)

    for cl in clusters:
        all_text = " ".join(m.get("text", "") for m in cl["members"])
        all_text += " " + cl["canonical_claim"].get("text", "")
        topics = match_topic(all_text)
        if topics:
            cluster_topics[cl["cluster_id"]] = [t for t, _ in topics[:3]]
            for t, _ in topics[:2]:  # primary + secondary
                clusters_per_topic[t].append(cl["cluster_id"])

    tagged = sum(1 for v in cluster_topics.values() if v)
    print(f"Clusters tagged: {tagged}/{len(clusters)}")
    for tid, cids in sorted(clusters_per_topic.items(), key=lambda x: -len(x[1])):
        label = HIRSCH_TOPICS[tid]["label"]
        print(f"  {label}: {len(cids)} clusters")

    # 6. Output
    output = {
        "source": "knowledge-based-curricula research compendium",
        "source_path": str(RESEARCH_DATA),
        "generated_at": "2026-03-31",
        "provenance": "External research from a sympathetically-oriented compendium on knowledge-based curricula. NOT Hirsch's own claims. Keep clearly separated.",
        "topics": {tid: {"label": t["label"], "description": t["description"]}
                   for tid, t in HIRSCH_TOPICS.items()},
        "items": items_with_topics,
        "by_topic": {tid: [i["id"] for i in items] for tid, items in by_topic.items()},
        "cluster_topics": {str(k): v for k, v in cluster_topics.items()},
        "clusters_per_topic": {k: v for k, v in clusters_per_topic.items()},
        "stats": {
            "total_research_items": len(all_items),
            "filtered_items": len(filtered),
            "matched_to_topics": len(items_with_topics),
            "clusters_tagged": tagged,
            "evidence_types": dict(type_counts),
            "topic_counts": dict(topic_counts),
        },
    }

    with open(OUTPUT, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    size = OUTPUT.stat().st_size / 1024
    print(f"\nWritten to: {OUTPUT} ({size:.0f} KB)")

    # Show sample: top topic with its clusters and research items
    print(f"\n--- Sample topic linkage ---")
    for tid in list(clusters_per_topic.keys())[:3]:
        label = HIRSCH_TOPICS[tid]["label"]
        cids = clusters_per_topic[tid]
        topic_items = by_topic.get(tid, [])
        print(f"\n  {label}:")
        print(f"    Hirsch clusters: {len(cids)}")
        for cid in cids[:2]:
            cl = next((c for c in clusters if c["cluster_id"] == cid), None)
            if cl:
                print(f"      → {cl['canonical_claim']['text'][:80]}...")
        print(f"    External findings: {len(topic_items)}")
        for item in topic_items[:2]:
            print(f"      [{item['evidence_type'][:15]}] {item['text'][:80]}...")


if __name__ == "__main__":
    main()
