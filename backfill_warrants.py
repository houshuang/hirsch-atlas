#!/usr/bin/env python3
"""Backfill warrants for main conclusions that have evidence but no warrant.

Finds evidence-claim pairs where the claim is a main conclusion and no warrant
exists for that pair, then generates warrants via LLM in batches.

Usage:
    .venv-otak/bin/python3 prototypes/hirsch/backfill_warrants.py
    .venv-otak/bin/python3 prototypes/hirsch/backfill_warrants.py --dry-run
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))

from llm_providers import generate_sync

DATA_DIR = Path(__file__).parent / "data" / "wkm"
CONSOLIDATED_FILE = DATA_DIR / "book_consolidated.json"

SYSTEM_PROMPT = (
    "You are generating warrants — the unstated principles that connect evidence "
    "to claims. For each claim-evidence pair, identify the reasoning principle that "
    "makes the evidence relevant to the claim, whether the author states it explicitly, "
    "and how a critic could attack the connection."
)

WARRANT_SCHEMA = {
    "type": "object",
    "properties": {
        "warrants": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "evidence_id": {"type": "string"},
                    "claim_id": {"type": "string"},
                    "warrant_text": {"type": "string"},
                    "is_explicit": {"type": "boolean"},
                    "vulnerability": {"type": "string"},
                },
                "required": [
                    "evidence_id",
                    "claim_id",
                    "warrant_text",
                    "is_explicit",
                    "vulnerability",
                ],
            },
        }
    },
    "required": ["warrants"],
}


def load_consolidated() -> dict:
    with open(CONSOLIDATED_FILE) as f:
        return json.load(f)


def load_chapter(chapter_id: str) -> dict:
    path = DATA_DIR / chapter_id / "merged.json"
    with open(path) as f:
        return json.load(f)


def save_chapter(chapter_id: str, data: dict):
    path = DATA_DIR / chapter_id / "merged.json"
    with open(path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def find_gaps(consolidated: dict) -> list[dict]:
    """Find evidence-claim pairs for main conclusions that lack warrants.

    Returns list of dicts with keys: chapter, claim_id, claim_text,
    evidence_id, evidence_description, evidence_type, source_reference.
    """
    mc_ids = {
        c["_global_id"]
        for c in consolidated["all_claims"]
        if c.get("is_main_conclusion")
    }

    # Build claim lookup
    claim_by_gid = {c["_global_id"]: c for c in consolidated["all_claims"]}

    # Existing warrant coverage: set of (evidence_global_id, claim_global_id)
    covered = set()
    for w in consolidated["all_warrants"]:
        ev_gid = w.get("evidence_id_global", "")
        cid_gid = w.get("claim_id_global", "")
        covered.add((ev_gid, cid_gid))

    # Find uncovered pairs for main conclusions
    gaps = []
    for ev in consolidated["all_evidence"]:
        cid = ev.get("supports_claim_global", "")
        if cid not in mc_ids:
            continue
        ev_gid = ev["_global_id"]
        if (ev_gid, cid) in covered:
            continue
        claim = claim_by_gid.get(cid, {})
        gaps.append(
            {
                "chapter": ev["chapter"],
                "claim_id": claim.get("id", ""),
                "claim_global_id": cid,
                "claim_text": claim.get("text", ""),
                "claim_level": claim.get("claim_level", ""),
                "evidence_id": ev.get("id", ""),
                "evidence_global_id": ev_gid,
                "evidence_description": ev.get("description", ""),
                "evidence_type": ev.get("evidence_type", ""),
                "source_reference": ev.get("source_reference", ""),
            }
        )

    return gaps


def build_prompt(batch: list[dict]) -> str:
    """Build a prompt for a batch of evidence-claim pairs."""
    lines = [
        "Generate a warrant for each of the following evidence-claim pairs from "
        "E.D. Hirsch Jr.'s 'Why Knowledge Matters'. A warrant is the unstated "
        "(or sometimes stated) reasoning principle that makes the evidence relevant "
        "to the claim.\n",
        "For each pair, produce:",
        "- evidence_id: use the evidence ID given",
        "- claim_id: use the claim ID given",
        "- warrant_text: the reasoning principle connecting evidence to claim",
        "- is_explicit: true if the author explicitly states this reasoning, false if implicit",
        "- vulnerability: how a critic could attack this inferential connection\n",
        f"There are {len(batch)} pairs. Return exactly {len(batch)} warrants.\n",
    ]

    for i, gap in enumerate(batch, 1):
        lines.append(f"--- Pair {i} ---")
        lines.append(f"Evidence ID: {gap['evidence_id']}")
        lines.append(f"Evidence (ch {gap['chapter']}): {gap['evidence_description']}")
        lines.append(f"Evidence type: {gap['evidence_type']}")
        if gap["source_reference"]:
            lines.append(f"Source: {gap['source_reference']}")
        lines.append(f"Claim ID: {gap['claim_id']}")
        lines.append(f"Claim (ch {gap['chapter']}): {gap['claim_text']}")
        lines.append(f"Claim level: {gap['claim_level']}")
        lines.append("")

    return "\n".join(lines)


def generate_warrants(gaps: list[dict], batch_size: int = 10, dry_run: bool = False):
    """Generate warrants for all gaps in batches."""
    total_cost = 0.0
    all_new_warrants = []  # (chapter, warrant_dict) pairs

    batches = [gaps[i : i + batch_size] for i in range(0, len(gaps), batch_size)]
    print(f"\nProcessing {len(gaps)} pairs in {len(batches)} batches of up to {batch_size}...")

    for bi, batch in enumerate(batches):
        prompt = build_prompt(batch)
        chapters_in_batch = sorted(set(g["chapter"] for g in batch))
        print(f"\n  Batch {bi + 1}/{len(batches)} ({len(batch)} pairs, chapters {chapters_in_batch})")

        if dry_run:
            print(f"    [DRY RUN] Would send {len(prompt)} chars to LLM")
            continue

        t0 = time.time()
        result, meta = generate_sync(
            prompt=prompt,
            system_prompt=SYSTEM_PROMPT,
            json_schema=WARRANT_SCHEMA,
            model="gemini3-flash",
            max_tokens=4096,
            thinking_budget=0,
        )
        elapsed = time.time() - t0
        cost = meta.get("cost_usd", 0.0)
        total_cost += cost

        warrants = result.get("warrants", [])
        print(f"    Got {len(warrants)} warrants in {elapsed:.1f}s (${cost:.4f})")

        # Match returned warrants to batch items by evidence_id + claim_id
        batch_lookup = {(g["evidence_id"], g["claim_id"]): g for g in batch}
        matched_keys = set()
        matched = 0
        unmatched_warrants = []
        for w in warrants:
            key = (w.get("evidence_id", ""), w.get("claim_id", ""))
            gap = batch_lookup.get(key)
            if gap:
                all_new_warrants.append((gap["chapter"], w))
                matched_keys.add(key)
                matched += 1
            else:
                unmatched_warrants.append(w)

        # Fallback: match remaining warrants by position to unmatched gaps
        if unmatched_warrants:
            unmatched_gaps = [g for g in batch if (g["evidence_id"], g["claim_id"]) not in matched_keys]
            for w, gap in zip(unmatched_warrants, unmatched_gaps):
                w["evidence_id"] = gap["evidence_id"]
                w["claim_id"] = gap["claim_id"]
                all_new_warrants.append((gap["chapter"], w))
                matched += 1
                print(f"    Positional match: {gap['evidence_id']}->{gap['claim_id']}")

        if matched < len(batch):
            print(f"    WARNING: {len(batch) - matched} pairs unmatched")

    return all_new_warrants, total_cost


def assign_ids_and_save(new_warrants: list[tuple[str, dict]], consolidated: dict):
    """Assign unique IDs to new warrants and save to chapter files + consolidated."""
    # Group by chapter
    by_chapter = defaultdict(list)
    for chapter, w in new_warrants:
        by_chapter[chapter].append(w)

    total_saved = 0

    for chapter, warrants in sorted(by_chapter.items()):
        chapter_data = load_chapter(chapter)
        existing_warrants = chapter_data.get("warrants", [])

        # Find max existing warrant number to avoid collisions
        max_num = 0
        for ew in existing_warrants:
            wid = ew.get("id", "")
            if wid.startswith("W"):
                try:
                    num = int(wid[1:])
                    max_num = max(max_num, num)
                except ValueError:
                    pass
        # Use a separate namespace to avoid confusion with batch-repeated W1/W2/W3
        next_num = max(max_num, len(existing_warrants)) + 100

        for w in warrants:
            next_num += 1
            w["id"] = f"W{next_num}"
            existing_warrants.append(w)

        chapter_data["warrants"] = existing_warrants
        save_chapter(chapter, chapter_data)
        total_saved += len(warrants)
        print(f"  ch {chapter:>12s}: +{len(warrants)} warrants (now {len(existing_warrants)} total)")

    # Update consolidated: add new warrants with global IDs
    for chapter, warrants in sorted(by_chapter.items()):
        for w in warrants:
            global_w = dict(w)
            global_w["chapter"] = chapter
            global_w["_global_id"] = f"ch{chapter}_{w['id']}"
            if w.get("claim_id"):
                global_w["claim_id_global"] = f"ch{chapter}_{w['claim_id']}"
            if w.get("evidence_id"):
                global_w["evidence_id_global"] = f"ch{chapter}_{w['evidence_id']}"
            consolidated["all_warrants"].append(global_w)

    # Update stats
    consolidated["stats"]["total_warrants"] = len(consolidated["all_warrants"])

    with open(CONSOLIDATED_FILE, "w") as f:
        json.dump(consolidated, f, indent=2, ensure_ascii=False)

    print(f"\n  Updated {CONSOLIDATED_FILE.name}: {consolidated['stats']['total_warrants']} total warrants")
    return total_saved


def main():
    parser = argparse.ArgumentParser(description="Backfill warrants for main conclusions")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without calling LLM")
    parser.add_argument("--batch-size", type=int, default=10, help="Claims per LLM batch (default: 10)")
    args = parser.parse_args()

    print("=" * 60)
    print("Hirsch Argument Atlas — Warrant Backfill")
    print("=" * 60)

    consolidated = load_consolidated()
    print(f"\nLoaded: {len(consolidated['all_claims'])} claims, "
          f"{len(consolidated['all_evidence'])} evidence, "
          f"{len(consolidated['all_warrants'])} warrants")

    mc_count = sum(1 for c in consolidated["all_claims"] if c.get("is_main_conclusion"))
    print(f"Main conclusions: {mc_count}")

    gaps = find_gaps(consolidated)
    if not gaps:
        print("\nNo gaps found — all evidence-claim pairs for main conclusions have warrants.")
        return

    # Group by chapter for display
    by_chapter = defaultdict(list)
    for g in gaps:
        by_chapter[g["chapter"]].append(g)

    print(f"\nFound {len(gaps)} evidence-claim pairs needing warrants across {len(by_chapter)} chapters:")
    for ch, items in sorted(by_chapter.items()):
        claim_ids = sorted(set(g["claim_id"] for g in items))
        print(f"  ch {ch:>12s}: {len(items)} pairs ({len(claim_ids)} claims: {', '.join(claim_ids)})")

    new_warrants, total_cost = generate_warrants(gaps, batch_size=args.batch_size, dry_run=args.dry_run)

    if args.dry_run:
        print(f"\n[DRY RUN] Would generate warrants for {len(gaps)} pairs")
        return

    if not new_warrants:
        print("\nNo warrants generated.")
        return

    print(f"\n{'=' * 60}")
    print("SAVING")
    print("=" * 60)

    total_saved = assign_ids_and_save(new_warrants, consolidated)

    # Show examples
    print(f"\n{'=' * 60}")
    print("EXAMPLES")
    print("=" * 60)
    for chapter, w in new_warrants[:3]:
        print(f"\n  [{chapter}] {w.get('claim_id', '')} <- {w.get('evidence_id', '')}")
        print(f"  Warrant: {w['warrant_text'][:120]}...")
        print(f"  Explicit: {w.get('is_explicit', '?')}")
        print(f"  Vulnerability: {w.get('vulnerability', '')[:120]}...")

    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print("=" * 60)
    print(f"  Pairs processed:    {len(gaps)}")
    print(f"  Warrants generated: {len(new_warrants)}")
    print(f"  Warrants saved:     {total_saved}")
    print(f"  Total cost:         ${total_cost:.4f}")
    print(f"  Chapters updated:   {len(by_chapter)}")


if __name__ == "__main__":
    main()
