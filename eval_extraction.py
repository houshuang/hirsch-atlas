#!/usr/bin/env python3
"""Eval harness for Hirsch extraction autoresearch.

Runs Phase 1 extraction on Prologue and Chapter 1, then scores recall
against human-annotated ground truth using LLM judge.

Output: SCORE=X.XXXX (fraction of ground truth items captured)

Usage:
    .venv-otak/bin/python3 prototypes/hirsch/eval_extraction.py
    .venv-otak/bin/python3 prototypes/hirsch/eval_extraction.py --score-only  # skip extraction, use existing
    .venv-otak/bin/python3 prototypes/hirsch/eval_extraction.py --model gemini25-flash
"""
import argparse
import json
import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))

from llm_providers import generate_sync

HIRSCH_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HIRSCH_DIR, "data")
GROUND_TRUTH_PATH = os.path.join(HIRSCH_DIR, "ground_truth.json")
PDF_PATH = "/tmp/hirsch-wkm.pdf"

SCORING_SCHEMA = {
    "type": "object",
    "properties": {
        "matches": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "ground_truth_id": {"type": "string"},
                    "matched": {"type": "boolean", "description": "true if this ground truth item is captured by at least one extracted claim/concept/case"},
                    "best_match_id": {"type": "string", "description": "ID of the best matching extracted item, or 'none'"},
                    "match_quality": {"type": "string", "enum": ["exact", "partial", "none"], "description": "exact=same claim, partial=related but missing key aspect, none=not captured"},
                    "explanation": {"type": "string", "description": "Brief explanation of match or why it was missed"}
                },
                "required": ["ground_truth_id", "matched", "best_match_id", "match_quality"]
            }
        }
    },
    "required": ["matches"]
}

SCORING_SYSTEM = """You are evaluating whether a set of GROUND TRUTH items (claims the human reviewer identified as important) are captured by an automated extraction.

For each ground truth item, check if ANY extracted item (claim, concept, case, thinker, evidence, or objection) captures the same idea. Be generous with matching:
- "exact" = the extracted item says essentially the same thing, possibly in different words
- "partial" = the extracted item captures part of it but misses a key aspect (e.g., captures the claim but not the prescriptive part, or captures the fact but not the mechanism)
- "none" = no extracted item captures this idea

A ground truth item counts as "matched" if match_quality is "exact" OR "partial".

IMPORTANT: Check ALL entity types (claims, concepts, cases, thinkers, evidence, objections), not just claims. A ground truth "concept" might be captured as a concept entity, and a ground truth "claim" about France might be captured as a case entity."""


def load_ground_truth():
    with open(GROUND_TRUTH_PATH) as f:
        return json.load(f)


def format_extraction_for_scoring(phase1_result: dict) -> str:
    """Format Phase 1 output as a concise inventory for scoring."""
    lines = []
    for c in phase1_result.get("claims", []):
        lines.append(f"CLAIM {c['id']}: {c['text']}")
    for c in phase1_result.get("concepts", []):
        lines.append(f"CONCEPT {c['id']}: {c['term']} — {c['author_definition']}")
    for c in phase1_result.get("cases", []):
        lines.append(f"CASE {c['id']}: {c['name']} — {c['description'][:200]}")
    for t in phase1_result.get("thinkers", []):
        lines.append(f"THINKER {t['id']}: {t['name']} — {t['key_idea']}")
    for e in phase1_result.get("evidence", []):
        lines.append(f"EVIDENCE {e['id']}: {e['description']}")
    for o in phase1_result.get("objections_raised", []):
        lines.append(f"OBJECTION {o['id']}: {o['objection']}")
    return "\n".join(lines)


def format_gt_for_scoring(items: list) -> str:
    lines = []
    for item in items:
        lines.append(f"{item['id']} [{item['type']}]: {item['text']}")
    return "\n".join(lines)


def score_chapter(chapter_key: str, gt_items: list, phase1_result: dict) -> dict:
    """Score one chapter's extraction against ground truth."""
    extraction_text = format_extraction_for_scoring(phase1_result)
    gt_text = format_gt_for_scoring(gt_items)

    prompt = f"""CHAPTER: {chapter_key}

GROUND TRUTH ITEMS (human-identified important claims/concepts):
{gt_text}

EXTRACTED ITEMS:
{extraction_text}

For each ground truth item, determine if it is captured by any extracted item."""

    result, meta = generate_sync(
        prompt, SCORING_SYSTEM, SCORING_SCHEMA,
        model="gemini3-flash", max_tokens=4096, thinking_budget=0
    )
    return result, meta


def run_extraction(chapter_key: str, model: str = "gemini3-flash") -> dict:
    """Run Phase 1 extraction for a chapter, return result dict."""
    from extract import parse_book, extract_phase1, load_json

    book_data = parse_book(PDF_PATH)
    skeleton_data = load_json(os.path.join(DATA_DIR, "wkm", "skeleton.json"))
    if not skeleton_data:
        raise RuntimeError("No skeleton found. Run extract.py skeleton first.")
    skeleton = skeleton_data["result"]

    chapter = None
    for ch in book_data["chapters"]:
        if ch["number"].lower() == chapter_key.lower():
            chapter = ch
            break
    if not chapter:
        raise RuntimeError(f"Chapter '{chapter_key}' not found")

    p1_data = extract_phase1(chapter, skeleton, model=model)
    return p1_data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--score-only", action="store_true",
                        help="Use existing Phase 1 output instead of re-extracting")
    parser.add_argument("--model", default="gemini3-flash",
                        help="Model for Phase 1 extraction")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    gt = load_ground_truth()
    chapters_to_eval = ["prologue", "1"]
    total_matched = 0
    total_items = 0
    total_exact = 0
    total_partial = 0
    total_cost = 0.0
    t0 = time.time()

    for ch_key in chapters_to_eval:
        gt_items = gt["chapters"][ch_key]["items"]

        if args.score_only:
            p1_path = os.path.join(DATA_DIR, "wkm", ch_key, "phase1_content.json")
            with open(p1_path) as f:
                p1_data = json.load(f)
            phase1 = p1_data["result"]
            n_claims = len(phase1.get("claims", []))
        else:
            # Save to a temp location so we don't overwrite calibrated data
            p1_data = run_extraction(ch_key, model=args.model)
            phase1 = p1_data["result"]
            n_claims = len(phase1.get("claims", []))
            total_cost += p1_data.get("metadata", {}).get("total_cost_usd", 0)

            # Save the extraction for inspection
            eval_dir = os.path.join(DATA_DIR, "wkm", ch_key, "eval_latest")
            os.makedirs(eval_dir, exist_ok=True)
            with open(os.path.join(eval_dir, "phase1_content.json"), "w") as f:
                json.dump(p1_data, f, indent=2, ensure_ascii=False)

        # Score
        score_result, score_meta = score_chapter(ch_key, gt_items, phase1)
        total_cost += score_meta.get("total_cost_usd", 0)

        matches = score_result.get("matches", [])
        ch_matched = sum(1 for m in matches if m.get("matched"))
        ch_exact = sum(1 for m in matches if m.get("match_quality") == "exact")
        ch_partial = sum(1 for m in matches if m.get("match_quality") == "partial")
        ch_total = len(gt_items)

        total_matched += ch_matched
        total_exact += ch_exact
        total_partial += ch_partial
        total_items += ch_total

        if args.verbose:
            print(f"\n--- {ch_key} ({n_claims} claims extracted, {ch_matched}/{ch_total} GT matched) ---")
            for m in matches:
                status = "MATCH" if m.get("matched") else "MISS "
                qual = m.get("match_quality", "?")
                print(f"  {status} [{qual:7s}] {m['ground_truth_id']} → {m.get('best_match_id', 'none')}")
                if args.verbose and m.get("explanation"):
                    print(f"           {m['explanation']}")

    elapsed = time.time() - t0
    # Weighted score: exact=1.0, partial=0.5, miss=0.0
    weighted = (total_exact * 1.0 + total_partial * 0.5) / total_items if total_items > 0 else 0
    recall = total_matched / total_items if total_items > 0 else 0
    print(f"\nSCORE={weighted:.4f} RECALL={recall:.4f} TOTAL={total_matched}/{total_items} EXACT={total_exact} PARTIAL={total_partial} COST=${total_cost:.4f} TIME={elapsed:.1f}s")


if __name__ == "__main__":
    main()
