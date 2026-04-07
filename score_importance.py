#!/usr/bin/env python3
"""Importance scoring and cross-chapter link discovery for the Hirsch Argument Atlas.

Task 1: Pure computational importance scoring based on graph structure.
Task 2: LLM-based cross-chapter link classification using claim clusters.

Usage:
    .venv-otak/bin/python3 prototypes/hirsch/score_importance.py           # importance only
    .venv-otak/bin/python3 prototypes/hirsch/score_importance.py --links   # + cross-chapter links
"""

import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

# ── Paths ──

DATA_PATH = Path(__file__).parent / "data" / "wkm" / "book_consolidated.json"

# ── Load data ──


def load_data() -> dict:
    with open(DATA_PATH) as f:
        return json.load(f)


def save_data(data: dict):
    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Saved to {DATA_PATH}")


# ── Task 1: Importance Scoring ──


def compute_importance(data: dict) -> dict:
    """Compute importance scores for all claims. Returns updated data with scores and analysis."""

    claims = data["all_claims"]
    evidence = data["all_evidence"]
    warrants = data["all_warrants"]
    counter_args = data["all_counter_arguments"]
    dependencies = data["all_dependencies"]
    clusters = data["claim_clusters"]
    argument_chains = data["all_argument_chains"]

    # Build lookup by global ID
    claim_by_gid = {c["_global_id"]: c for c in claims}

    # ── 1. In-degree: how many claims depend on this one? ──
    # A dependency "from_id depends-on to_id" means to_id is depended upon.
    # A dependency "from_id supports to_id" means to_id is supported by from_id.
    # We count ALL incoming edges (things pointing TO this claim via to_id_global).
    in_degree = Counter()
    for dep in dependencies:
        to_gid = dep.get("to_id_global")
        if to_gid:
            in_degree[to_gid] += 1

    # ── 2. Evidence count per claim ──
    evidence_count = Counter()
    for ev in evidence:
        claim_gid = ev.get("supports_claim_global")
        if claim_gid:
            evidence_count[claim_gid] += 1

    # ── 3. Warrant count per claim ──
    warrant_count = Counter()
    for w in warrants:
        claim_gid = w.get("claim_id_global")
        if claim_gid:
            warrant_count[claim_gid] += 1

    # ── 4. Cross-chapter appearances ──
    # Build cluster membership: claim_gid -> set of chapters in its cluster
    cross_chapter_score = {}
    cluster_chapters = {}  # cluster_id -> set of chapters
    claim_to_cluster = {}  # claim_gid -> cluster_id

    for cl in clusters:
        cid = cl["cluster_id"]
        chapters = set(cl["chapters"])
        cluster_chapters[cid] = chapters

        # Canonical claim
        canon = cl["canonical_claim"]
        claim_to_cluster[canon["claim_id"]] = cid

        # Members
        for m in cl["members"]:
            claim_to_cluster[m["claim_id"]] = cid

    for c in claims:
        gid = c["_global_id"]
        cid = claim_to_cluster.get(gid)
        if cid and len(cluster_chapters.get(cid, set())) > 1:
            cross_chapter_score[gid] = len(cluster_chapters[cid])
        else:
            cross_chapter_score[gid] = 0

    # ── 5. Downstream impact (transitive closure) ──
    # Build directed graph: if A depends-on B, then B -> A (B impacts A)
    # Also: if A supports B, then A -> B (A contributes to B)
    # We want: for each claim, how many claims are transitively downstream?
    # "downstream" means: claims that directly or indirectly depend on this claim.
    # depends-on: from depends on to, so to -> from (to impacts from)
    # supports: from supports to, so from -> to (from contributes to to)
    # For downstream impact, we follow depends-on edges: if X depends-on Y, Y has downstream impact on X.

    # Build adjacency: Y -> [X] where X depends-on Y
    depends_on_graph = defaultdict(set)  # depended-upon -> set of dependents
    for dep in dependencies:
        if dep["relationship"] == "depends-on":
            from_gid = dep.get("from_id_global")
            to_gid = dep.get("to_id_global")
            if from_gid and to_gid:
                depends_on_graph[to_gid].add(from_gid)

    # Compute transitive closure size for each node via BFS
    downstream_impact = {}
    for gid in claim_by_gid:
        visited = set()
        queue = [gid]
        while queue:
            node = queue.pop(0)
            for child in depends_on_graph.get(node, set()):
                if child not in visited:
                    visited.add(child)
                    queue.append(child)
        downstream_impact[gid] = len(visited)

    # ── 6. Counter-argument count ──
    counter_count = Counter()
    for ca in counter_args:
        claim_gid = ca.get("targets_claim_global")
        if claim_gid:
            counter_count[claim_gid] += 1

    # ── Composite score ──
    # Weights (tuned to emphasize structural importance)
    W_INDEGREE = 0.25
    W_DOWNSTREAM = 0.25
    W_EVIDENCE = 0.15
    W_WARRANT = 0.10
    W_CROSS_CHAPTER = 0.15
    W_COUNTER = 0.10

    # Find max values for normalization
    max_indegree = max(in_degree.values()) if in_degree else 1
    max_downstream = max(downstream_impact.values()) if downstream_impact else 1
    max_evidence = max(evidence_count.values()) if evidence_count else 1
    max_warrant = max(warrant_count.values()) if warrant_count else 1
    max_cross = max(cross_chapter_score.values()) if any(v > 0 for v in cross_chapter_score.values()) else 1
    max_counter = max(counter_count.values()) if counter_count else 1

    scores = {}
    for c in claims:
        gid = c["_global_id"]
        ind = in_degree.get(gid, 0) / max_indegree
        dwn = downstream_impact.get(gid, 0) / max_downstream
        ev = evidence_count.get(gid, 0) / max_evidence
        war = warrant_count.get(gid, 0) / max_warrant
        cross = cross_chapter_score.get(gid, 0) / max_cross
        cnt = counter_count.get(gid, 0) / max_counter

        composite = (
            W_INDEGREE * ind
            + W_DOWNSTREAM * dwn
            + W_EVIDENCE * ev
            + W_WARRANT * war
            + W_CROSS_CHAPTER * cross
            + W_COUNTER * cnt
        )

        scores[gid] = {
            "composite": round(composite, 4),
            "in_degree": in_degree.get(gid, 0),
            "in_degree_norm": round(ind, 4),
            "downstream_impact": downstream_impact.get(gid, 0),
            "downstream_norm": round(dwn, 4),
            "evidence_count": evidence_count.get(gid, 0),
            "evidence_norm": round(ev, 4),
            "warrant_count": warrant_count.get(gid, 0),
            "warrant_norm": round(war, 4),
            "cross_chapter_count": cross_chapter_score.get(gid, 0),
            "cross_chapter_norm": round(cross, 4),
            "counter_argument_count": counter_count.get(gid, 0),
            "counter_norm": round(cnt, 4),
        }

    # ── Apply scores to claims ──
    for c in claims:
        gid = c["_global_id"]
        c["importance"] = scores[gid]

    # ── Analysis ──

    # Top 20 by composite score
    sorted_claims = sorted(claims, key=lambda c: c["importance"]["composite"], reverse=True)
    top_20 = [
        {
            "global_id": c["_global_id"],
            "chapter": c["chapter"],
            "text": c["text"],
            "score": c["importance"]["composite"],
            "in_degree": c["importance"]["in_degree"],
            "downstream": c["importance"]["downstream_impact"],
            "evidence": c["importance"]["evidence_count"],
            "counters": c["importance"]["counter_argument_count"],
            "cross_chapters": c["importance"]["cross_chapter_count"],
        }
        for c in sorted_claims[:20]
    ]

    # Unsupported load-bearing: high in_degree + downstream but low evidence + warrants
    load_bearing_unsupported = []
    for c in claims:
        imp = c["importance"]
        structural = imp["in_degree"] + imp["downstream_impact"]
        support = imp["evidence_count"] + imp["warrant_count"]
        if structural >= 3 and support == 0:
            load_bearing_unsupported.append({
                "global_id": c["_global_id"],
                "chapter": c["chapter"],
                "text": c["text"],
                "in_degree": imp["in_degree"],
                "downstream": imp["downstream_impact"],
                "evidence": imp["evidence_count"],
                "warrants": imp["warrant_count"],
                "structural_load": structural,
            })
    load_bearing_unsupported.sort(key=lambda x: x["structural_load"], reverse=True)

    # Well-supported peripheral: high evidence but no dependents
    well_supported_peripheral = []
    for c in claims:
        imp = c["importance"]
        if imp["evidence_count"] >= 2 and imp["in_degree"] == 0 and imp["downstream_impact"] == 0:
            well_supported_peripheral.append({
                "global_id": c["_global_id"],
                "chapter": c["chapter"],
                "text": c["text"],
                "evidence_count": imp["evidence_count"],
                "warrant_count": imp["warrant_count"],
                "counter_arguments": imp["counter_argument_count"],
            })
    well_supported_peripheral.sort(key=lambda x: x["evidence_count"], reverse=True)

    # Argument chains ranked by total evidence strength
    chain_rankings = []
    for chain in argument_chains:
        chain_gids = chain.get("chain_global", [])
        total_evidence = sum(evidence_count.get(gid, 0) for gid in chain_gids)
        total_warrants = sum(warrant_count.get(gid, 0) for gid in chain_gids)
        avg_importance = (
            sum(scores.get(gid, {}).get("composite", 0) for gid in chain_gids) / len(chain_gids)
            if chain_gids
            else 0
        )
        chain_rankings.append({
            "name": chain["name"],
            "chapter": chain["chapter"],
            "length": len(chain_gids),
            "total_evidence": total_evidence,
            "total_warrants": total_warrants,
            "avg_importance": round(avg_importance, 4),
            "conclusion": chain.get("conclusion_id_global"),
            "strength": chain.get("strength"),
        })
    chain_rankings.sort(key=lambda x: (x["total_evidence"], x["avg_importance"]), reverse=True)

    # Chapter-by-chapter stats
    chapter_stats = {}
    for ch in data["chapters_processed"]:
        ch_claims = [c for c in claims if c["chapter"] == ch]
        if ch_claims:
            avg_imp = sum(c["importance"]["composite"] for c in ch_claims) / len(ch_claims)
            max_imp = max(c["importance"]["composite"] for c in ch_claims)
            top_claim = max(ch_claims, key=lambda c: c["importance"]["composite"])
            chapter_stats[ch] = {
                "claim_count": len(ch_claims),
                "avg_importance": round(avg_imp, 4),
                "max_importance": round(max_imp, 4),
                "top_claim": {
                    "global_id": top_claim["_global_id"],
                    "text": top_claim["text"][:120],
                    "score": top_claim["importance"]["composite"],
                },
                "evidence_count": sum(1 for e in evidence if e["chapter"] == ch),
                "counter_arg_count": sum(1 for ca in counter_args if ca["chapter"] == ch),
            }

    # Build the analysis section
    book_analysis = {
        "scoring_weights": {
            "in_degree": W_INDEGREE,
            "downstream_impact": W_DOWNSTREAM,
            "evidence_count": W_EVIDENCE,
            "warrant_count": W_WARRANT,
            "cross_chapter": W_CROSS_CHAPTER,
            "counter_arguments": W_COUNTER,
        },
        "normalization_maxima": {
            "max_in_degree": max_indegree,
            "max_downstream_impact": max_downstream,
            "max_evidence_count": max_evidence,
            "max_warrant_count": max_warrant,
            "max_cross_chapter": max_cross,
            "max_counter_arguments": max_counter,
        },
        "top_20_claims": top_20,
        "unsupported_load_bearing_claims": load_bearing_unsupported[:10],
        "well_supported_peripheral_claims": well_supported_peripheral[:10],
        "argument_chains_ranked": chain_rankings[:20],
        "chapter_stats": chapter_stats,
        "summary": {
            "total_claims": len(claims),
            "claims_with_evidence": sum(1 for gid in evidence_count if evidence_count[gid] > 0),
            "claims_with_warrants": sum(1 for gid in warrant_count if warrant_count[gid] > 0),
            "claims_with_counter_args": sum(1 for gid in counter_count if counter_count[gid] > 0),
            "claims_in_cross_chapter_clusters": sum(1 for gid, v in cross_chapter_score.items() if v > 0),
            "claims_with_dependents": sum(1 for gid in in_degree if in_degree[gid] > 0),
            "claims_with_downstream_impact": sum(1 for gid, v in downstream_impact.items() if v > 0),
            "unsupported_load_bearing_total": len(load_bearing_unsupported),
            "well_supported_peripheral_total": len(well_supported_peripheral),
        },
    }

    data["book_analysis"] = book_analysis
    return data


# ── Task 2: Cross-Chapter Link Discovery ──


def discover_cross_chapter_links(data: dict) -> dict:
    """Use LLM to classify relationships between claims in cross-chapter clusters."""

    # Import here so Task 1 can run without LLM dependencies
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))
    from llm_providers import generate_sync

    clusters = data["claim_clusters"]
    cross_clusters = [c for c in clusters if len(c.get("chapters", [])) > 1]

    print(f"\nCross-chapter link discovery: {len(cross_clusters)} clusters")

    # Build all cross-chapter pairs
    pairs = []
    for cl in cross_clusters:
        all_in_cluster = [cl["canonical_claim"]] + cl["members"]
        for i in range(len(all_in_cluster)):
            for j in range(i + 1, len(all_in_cluster)):
                a = all_in_cluster[i]
                b = all_in_cluster[j]
                if a["chapter"] != b["chapter"]:
                    pairs.append({
                        "cluster_id": cl["cluster_id"],
                        "claim_a": {
                            "id": a["claim_id"],
                            "chapter": a["chapter"],
                            "text": a["text"],
                        },
                        "claim_b": {
                            "id": b["claim_id"],
                            "chapter": b["chapter"],
                            "text": b["text"],
                        },
                    })

    print(f"Total cross-chapter pairs to classify: {len(pairs)}")

    # Batch pairs into groups for efficient LLM calls (~15 pairs per call)
    BATCH_SIZE = 15
    batches = [pairs[i : i + BATCH_SIZE] for i in range(0, len(pairs), BATCH_SIZE)]

    system_prompt = """You are analyzing E.D. Hirsch Jr.'s "Why Knowledge Matters" to map the argument structure across chapters.

For each pair of claims from different chapters, classify their relationship. Both claims are in the same semantic cluster, meaning they are topically related. Your job is to determine the PRECISE relationship.

Relationship types:
- restates: Same claim in different words. Most common for cross-chapter repetitions.
- develops: One claim adds evidence, nuance, or elaboration to the other. The later chapter version builds on the earlier.
- supports: One claim provides evidence or reasoning that supports the other.
- depends-on: One claim requires the other as a logical premise. Without claim A, claim B's argument would collapse.
- refines: The later chapter version is more specific, qualified, or precise than the earlier version.

For direction: claim_a is always the earlier chapter occurrence (or canonical). The relationship reads as "claim_a [relationship] claim_b" — e.g., "claim_a restates claim_b" means they say the same thing."""

    json_schema = {
        "type": "object",
        "properties": {
            "classifications": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "pair_index": {"type": "integer"},
                        "relationship": {
                            "type": "string",
                            "enum": ["restates", "develops", "supports", "depends-on", "refines"],
                        },
                        "direction": {
                            "type": "string",
                            "enum": ["a_to_b", "b_to_a"],
                            "description": "Which claim is the source of the relationship",
                        },
                        "confidence": {"type": "number"},
                        "explanation": {"type": "string"},
                    },
                    "required": ["pair_index", "relationship", "direction", "confidence", "explanation"],
                },
            }
        },
        "required": ["classifications"],
    }

    all_links = []
    total_cost = 0.0

    for batch_idx, batch in enumerate(batches):
        pair_text = ""
        for i, pair in enumerate(batch):
            pair_text += f"\n--- Pair {i} (Cluster {pair['cluster_id']}) ---\n"
            pair_text += f"Claim A [ch.{pair['claim_a']['chapter']}] ({pair['claim_a']['id']}): {pair['claim_a']['text']}\n"
            pair_text += f"Claim B [ch.{pair['claim_b']['chapter']}] ({pair['claim_b']['id']}): {pair['claim_b']['text']}\n"

        prompt = f"Classify the relationship between each pair of claims:\n{pair_text}"

        try:
            result, meta = generate_sync(
                prompt=prompt,
                system_prompt=system_prompt,
                json_schema=json_schema,
                model="gemini3-flash",
                max_tokens=4096,
                thinking_budget=0,
            )
            cost = meta.get("cost", 0)
            total_cost += cost

            for clf in result.get("classifications", []):
                idx = clf["pair_index"]
                if 0 <= idx < len(batch):
                    pair = batch[idx]
                    # Determine from/to based on direction
                    if clf["direction"] == "a_to_b":
                        from_id = pair["claim_a"]["id"]
                        to_id = pair["claim_b"]["id"]
                    else:
                        from_id = pair["claim_b"]["id"]
                        to_id = pair["claim_a"]["id"]

                    all_links.append({
                        "from_id": from_id,
                        "to_id": to_id,
                        "relationship": clf["relationship"],
                        "confidence": clf.get("confidence", 0.8),
                        "explanation": clf.get("explanation", ""),
                        "cluster_id": pair["cluster_id"],
                        "from_chapter": pair["claim_a"]["chapter"] if clf["direction"] == "a_to_b" else pair["claim_b"]["chapter"],
                        "to_chapter": pair["claim_b"]["chapter"] if clf["direction"] == "a_to_b" else pair["claim_a"]["chapter"],
                    })

            print(f"  Batch {batch_idx + 1}/{len(batches)}: {len(result.get('classifications', []))} links (${cost:.4f})")

        except Exception as e:
            print(f"  Batch {batch_idx + 1}/{len(batches)}: ERROR - {e}")

    # Store cross-chapter links
    data["cross_chapter_links"] = all_links

    # Update stats
    rel_counts = Counter(l["relationship"] for l in all_links)
    data["book_analysis"]["cross_chapter_links"] = {
        "total_links": len(all_links),
        "relationship_distribution": dict(rel_counts),
        "clusters_processed": len(cross_clusters),
        "pairs_classified": len(pairs),
        "llm_cost": round(total_cost, 4),
    }

    print(f"\nCross-chapter links: {len(all_links)} discovered")
    print(f"  Relationship distribution: {dict(rel_counts)}")
    print(f"  LLM cost: ${total_cost:.4f}")

    return data


# ── Display ──


def print_report(data: dict):
    """Print the importance analysis report."""
    analysis = data["book_analysis"]

    print("\n" + "=" * 80)
    print("HIRSCH ARGUMENT ATLAS — IMPORTANCE ANALYSIS")
    print("=" * 80)

    # Summary
    s = analysis["summary"]
    print(f"\nTotal claims: {s['total_claims']}")
    print(f"  With evidence: {s['claims_with_evidence']}")
    print(f"  With warrants: {s['claims_with_warrants']}")
    print(f"  With counter-arguments: {s['claims_with_counter_args']}")
    print(f"  In cross-chapter clusters: {s['claims_in_cross_chapter_clusters']}")
    print(f"  With dependents (in-degree > 0): {s['claims_with_dependents']}")
    print(f"  With downstream impact: {s['claims_with_downstream_impact']}")
    print(f"  Unsupported load-bearing: {s['unsupported_load_bearing_total']}")
    print(f"  Well-supported peripheral: {s['well_supported_peripheral_total']}")

    # Top 20
    print(f"\n{'─' * 80}")
    print("TOP 20 CLAIMS BY IMPORTANCE")
    print(f"{'─' * 80}")
    for i, c in enumerate(analysis["top_20_claims"], 1):
        print(f"\n{i:2d}. [{c['chapter']:>10s}] score={c['score']:.4f}  "
              f"in={c['in_degree']}  down={c['downstream']}  "
              f"ev={c['evidence']}  cnt={c['counters']}  "
              f"xch={c['cross_chapters']}")
        print(f"    {c['global_id']}: {c['text'][:120]}")

    # Unsupported load-bearing
    print(f"\n{'─' * 80}")
    print("TOP 10 UNSUPPORTED LOAD-BEARING CLAIMS")
    print("(High structural importance, NO evidence or warrants)")
    print(f"{'─' * 80}")
    for i, c in enumerate(analysis["unsupported_load_bearing_claims"][:10], 1):
        print(f"\n{i:2d}. [{c['chapter']:>10s}] structural_load={c['structural_load']}  "
              f"in={c['in_degree']}  down={c['downstream']}  "
              f"ev={c['evidence']}  war={c['warrants']}")
        print(f"    {c['global_id']}: {c['text'][:120]}")

    # Well-supported peripheral
    print(f"\n{'─' * 80}")
    print("TOP 10 WELL-SUPPORTED PERIPHERAL CLAIMS")
    print("(Lots of evidence, but nothing depends on them)")
    print(f"{'─' * 80}")
    for i, c in enumerate(analysis["well_supported_peripheral_claims"][:10], 1):
        print(f"\n{i:2d}. [{c['chapter']:>10s}] evidence={c['evidence_count']}  "
              f"warrants={c['warrant_count']}  counters={c['counter_arguments']}")
        print(f"    {c['global_id']}: {c['text'][:120]}")

    # Argument chains
    print(f"\n{'─' * 80}")
    print("TOP 10 ARGUMENT CHAINS BY EVIDENCE STRENGTH")
    print(f"{'─' * 80}")
    for i, ch in enumerate(analysis["argument_chains_ranked"][:10], 1):
        print(f"\n{i:2d}. [{ch['chapter']:>10s}] \"{ch['name']}\"")
        print(f"    length={ch['length']}  evidence={ch['total_evidence']}  "
              f"warrants={ch['total_warrants']}  avg_importance={ch['avg_importance']:.4f}  "
              f"strength={ch['strength']}")

    # Chapter stats
    print(f"\n{'─' * 80}")
    print("CHAPTER-BY-CHAPTER SUMMARY")
    print(f"{'─' * 80}")
    print(f"{'Chapter':<14s} {'Claims':>6s} {'Avg Imp':>8s} {'Max Imp':>8s} {'Evidence':>8s} {'Counters':>8s}")
    print(f"{'─' * 14} {'─' * 6} {'─' * 8} {'─' * 8} {'─' * 8} {'─' * 8}")
    for ch in data["chapters_processed"]:
        if ch in analysis["chapter_stats"]:
            st = analysis["chapter_stats"][ch]
            print(f"{ch:<14s} {st['claim_count']:>6d} {st['avg_importance']:>8.4f} "
                  f"{st['max_importance']:>8.4f} {st['evidence_count']:>8d} {st['counter_arg_count']:>8d}")

    # Cross-chapter links if present
    if "cross_chapter_links" in analysis:
        xcl = analysis["cross_chapter_links"]
        print(f"\n{'─' * 80}")
        print("CROSS-CHAPTER LINKS")
        print(f"{'─' * 80}")
        print(f"Total links discovered: {xcl['total_links']}")
        print(f"Clusters processed: {xcl['clusters_processed']}")
        print(f"Pairs classified: {xcl['pairs_classified']}")
        print(f"LLM cost: ${xcl['llm_cost']:.4f}")
        print(f"Relationship distribution:")
        for rel, count in sorted(xcl["relationship_distribution"].items(), key=lambda x: -x[1]):
            print(f"  {rel}: {count}")


# ── Main ──


def main():
    do_links = "--links" in sys.argv

    print("Loading data...")
    data = load_data()
    print(f"  {len(data['all_claims'])} claims, {len(data['all_dependencies'])} dependencies, "
          f"{len(data['all_evidence'])} evidence, {len(data['claim_clusters'])} clusters")

    print("\nComputing importance scores...")
    t0 = time.time()
    data = compute_importance(data)
    print(f"  Done in {time.time() - t0:.1f}s")

    if do_links:
        print("\nDiscovering cross-chapter links...")
        t0 = time.time()
        data = discover_cross_chapter_links(data)
        print(f"  Done in {time.time() - t0:.1f}s")

    print_report(data)

    print("\nSaving updated data...")
    save_data(data)


if __name__ == "__main__":
    main()
