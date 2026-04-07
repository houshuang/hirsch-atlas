"""Hirsch Argument Atlas — Evidence Enrichment Backfill.

Parses endnotes from the PDF and identifies evidence items (studies, datasets,
empirical sources) that weren't captured during the original extraction.
Adds new evidence items to each chapter's merged.json.

Usage:
    .venv-otak/bin/python3 prototypes/hirsch/backfill_evidence.py /tmp/hirsch-wkm.pdf
    .venv-otak/bin/python3 prototypes/hirsch/backfill_evidence.py /tmp/hirsch-wkm.pdf --chapter 3
    .venv-otak/bin/python3 prototypes/hirsch/backfill_evidence.py /tmp/hirsch-wkm.pdf --dry-run
"""
import argparse
import json
import logging
import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "prototypes", "hirsch"))

from extract import parse_book, parse_endnotes, chapter_slug
from llm_providers import generate_sync

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("backfill-evidence")

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "wkm")

BATCH_SIZE = 30  # max endnotes per LLM call

NEW_EVIDENCE_SCHEMA = {
    "type": "object",
    "properties": {
        "new_evidence": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "description": {"type": "string"},
                    "evidence_type": {
                        "type": "string",
                        "enum": [
                            "empirical_study", "statistical_data", "historical_example",
                            "international_comparison", "natural_experiment", "anecdote",
                            "expert_testimony",
                        ],
                    },
                    "source_reference": {"type": "string"},
                    "endnote_numbers": {"type": "array", "items": {"type": "integer"}},
                    "supports_claim": {
                        "type": "string",
                        "description": "Best matching claim ID from existing claims, or 'general' if unclear",
                    },
                },
                "required": ["description", "evidence_type", "source_reference", "endnote_numbers"],
            },
        },
    },
    "required": ["new_evidence"],
}

SYSTEM_PROMPT = """\
You are an expert research assistant analyzing endnotes from E.D. Hirsch's \
"Why Knowledge Matters" to find evidence items that were missed during \
initial extraction.

An "evidence item" is a specific study, dataset, statistical finding, \
historical example, natural experiment, or empirical source that supports \
or challenges a claim. It must be concrete and citable — not a general \
reference to a concept, not a page-number-only citation, not a "see also" \
to a general book, and not an attribution of a quote or idea.

DO NOT include:
- General book references without specific empirical content
- Citations that merely attribute a quote or concept to an author
- Cross-references to other chapters or sections
- References to general theoretical works (e.g. "Dewey, Democracy and Education")
- Items that are clearly already captured in the existing evidence list

Only return genuinely NEW evidence items with real empirical content."""


def build_prompt(endnotes_batch: list[dict], existing_evidence: list[dict],
                 claims: list[dict], chapter_title: str) -> str:
    """Build the prompt for identifying new evidence from endnotes."""
    # Format existing evidence compactly
    existing_lines = []
    for e in existing_evidence:
        ref = e.get("source_reference", "")
        desc = e.get("description", "")
        endnotes = e.get("endnote_numbers", [])
        existing_lines.append(f"- [{e['id']}] {desc[:120]} | ref: {ref} | endnotes: {endnotes}")

    # Format claims compactly for supports_claim matching
    claim_lines = []
    for c in claims:
        claim_lines.append(f"- [{c['id']}] {c.get('claim', c.get('text', ''))[:150]}")

    # Format endnotes
    endnote_lines = []
    for en in endnotes_batch:
        endnote_lines.append(f"[{en['number']}] {en['text']}")

    return f"""Chapter: {chapter_title}

## Existing Evidence Items (already captured — do NOT re-extract these)
{chr(10).join(existing_lines)}

## Chapter Claims (use these IDs for supports_claim)
{chr(10).join(claim_lines)}

## Endnotes to Analyze
{chr(10).join(endnote_lines)}

Which studies, datasets, or empirical sources in these endnotes are NOT \
already captured in the evidence list? Return only genuinely new evidence \
items with specific empirical content. If none are new, return an empty array.

For supports_claim, pick the claim ID that the evidence most directly \
supports or challenges. Use "general" only if no claim is a reasonable match."""


def next_evidence_id(existing: list[dict]) -> int:
    """Find the next numeric suffix for evidence IDs."""
    max_num = 0
    for e in existing:
        eid = e.get("id", "")
        if eid.startswith("E") and eid[1:].isdigit():
            max_num = max(max_num, int(eid[1:]))
        elif eid.startswith("E_new"):
            try:
                max_num = max(max_num, int(eid[5:]))
            except ValueError:
                pass
    return max_num + 1


def process_chapter(chapter_data: dict, dry_run: bool = False) -> dict:
    """Process one chapter: parse endnotes, find missing evidence, update merged.json."""
    ch_num = chapter_data["number"]
    ch_title = chapter_data["title"]
    slug = chapter_slug(ch_num)
    merged_path = os.path.join(DATA_DIR, slug, "merged.json")

    if not os.path.exists(merged_path):
        log.warning("No merged.json for %s — skipping", slug)
        return {"chapter": slug, "skipped": True, "reason": "no merged.json"}

    endnotes = parse_endnotes(chapter_data.get("notes", ""))
    if not endnotes:
        log.info("[%s] No endnotes — skipping", slug)
        return {"chapter": slug, "skipped": True, "reason": "no endnotes"}

    with open(merged_path) as f:
        merged = json.load(f)

    existing_evidence = merged.get("evidence", [])
    claims = merged.get("claims", [])
    existing_count = len(existing_evidence)

    log.info("[%s] %s — %d endnotes, %d existing evidence",
             slug, ch_title, len(endnotes), existing_count)

    # Batch endnotes if needed
    all_new = []
    total_cost = 0.0
    batches = [endnotes[i:i + BATCH_SIZE] for i in range(0, len(endnotes), BATCH_SIZE)]

    for batch_idx, batch in enumerate(batches):
        if len(batches) > 1:
            log.info("[%s] Processing endnote batch %d/%d (notes %d-%d)",
                     slug, batch_idx + 1, len(batches),
                     batch[0]["number"], batch[-1]["number"])

        prompt = build_prompt(batch, existing_evidence, claims, ch_title)

        if dry_run:
            log.info("[%s] DRY RUN — would send %d chars to LLM", slug, len(prompt))
            continue

        result, meta = generate_sync(
            prompt=prompt,
            system_prompt=SYSTEM_PROMPT,
            json_schema=NEW_EVIDENCE_SCHEMA,
            model="gemini3-flash",
            max_tokens=4096,
            thinking_budget=0,
        )

        cost = meta.get("total_cost_usd", 0.0)
        total_cost += cost

        if result and "new_evidence" in result:
            batch_new = result["new_evidence"]
            all_new.extend(batch_new)
            log.info("[%s] Batch %d: found %d new evidence items (cost: $%.4f)",
                     slug, batch_idx + 1, len(batch_new), cost)
        else:
            log.warning("[%s] Batch %d: no result or parse error", slug, batch_idx + 1)

    if dry_run:
        return {
            "chapter": slug,
            "existing_evidence": existing_count,
            "endnotes": len(endnotes),
            "dry_run": True,
        }

    # Assign IDs and add to merged data
    next_id = next_evidence_id(existing_evidence)
    for i, ev in enumerate(all_new):
        ev["id"] = f"E_new{next_id + i}"

    if all_new:
        merged["evidence"] = existing_evidence + all_new
        with open(merged_path, "w") as f:
            json.dump(merged, f, indent=2, ensure_ascii=False)
        log.info("[%s] Wrote %d new evidence items to merged.json (total: %d)",
                 slug, len(all_new), len(merged["evidence"]))

        # Print examples for verification
        for ev in all_new[:3]:
            log.info("  NEW [%s] %s — %s (endnotes: %s, supports: %s)",
                     ev["id"], ev["evidence_type"], ev["description"][:80],
                     ev.get("endnote_numbers", []), ev.get("supports_claim", "?"))
        if len(all_new) > 3:
            log.info("  ... and %d more", len(all_new) - 3)
    else:
        log.info("[%s] No new evidence found", slug)

    return {
        "chapter": slug,
        "existing_evidence": existing_count,
        "new_evidence": len(all_new),
        "total_evidence": existing_count + len(all_new),
        "endnotes": len(endnotes),
        "cost": total_cost,
    }


def main():
    parser = argparse.ArgumentParser(description="Backfill evidence from endnotes")
    parser.add_argument("pdf", help="Path to the PDF")
    parser.add_argument("--chapter", help="Process only this chapter (slug, e.g. '3' or 'prologue')")
    parser.add_argument("--dry-run", action="store_true", help="Parse and report but don't call LLM")
    args = parser.parse_args()

    book = parse_book(args.pdf)

    # Filter chapters
    chapters = book["chapters"]
    if args.chapter:
        chapters = [ch for ch in chapters if chapter_slug(ch["number"]) == args.chapter]
        if not chapters:
            log.error("Chapter '%s' not found", args.chapter)
            sys.exit(1)

    results = []
    total_cost = 0.0
    total_new = 0
    total_existing = 0
    t0 = time.time()

    for ch in chapters:
        result = process_chapter(ch, dry_run=args.dry_run)
        results.append(result)
        if not result.get("skipped") and not result.get("dry_run"):
            total_cost += result.get("cost", 0.0)
            total_new += result.get("new_evidence", 0)
            total_existing += result.get("existing_evidence", 0)

    elapsed = time.time() - t0

    # Summary
    print("\n" + "=" * 60)
    print("EVIDENCE BACKFILL SUMMARY")
    print("=" * 60)
    for r in results:
        if r.get("skipped"):
            print(f"  {r['chapter']:>14s}: SKIPPED ({r.get('reason', '')})")
        elif r.get("dry_run"):
            print(f"  {r['chapter']:>14s}: {r['endnotes']} endnotes, {r['existing_evidence']} existing (dry run)")
        else:
            new = r.get("new_evidence", 0)
            marker = f" (+{new})" if new > 0 else ""
            print(f"  {r['chapter']:>14s}: {r['existing_evidence']} existing → "
                  f"{r['total_evidence']} total{marker}  "
                  f"[{r['endnotes']} endnotes, ${r.get('cost', 0):.4f}]")

    if not args.dry_run:
        print(f"\nTotal: {total_existing} existing + {total_new} new = {total_existing + total_new}")
        print(f"Cost: ${total_cost:.4f}")
        print(f"Time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
