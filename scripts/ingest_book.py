"""Ingest a book (EPUB/PDF) into the Otak knowledge tree.

Extracts claims from book chapters using deep argument-structure extraction.
Designed for non-fiction books that build complex arguments with evidence chains.

Usage:
    .venv-otak/bin/python3 scripts/ingest_book.py <path-to-book> [--chapter N] [--dry-run] [--graph otak_v2]

Examples:
    # Dry-run Chapter 1 only
    .venv-otak/bin/python3 scripts/ingest_book.py ~/Library/CloudStorage/Dropbox/Book/Why*.epub --chapter 1 --dry-run

    # Process all chapters
    .venv-otak/bin/python3 scripts/ingest_book.py ~/Library/CloudStorage/Dropbox/Book/Why*.epub --graph otak_v2
"""
import argparse
import glob
import json
import logging
import os
import re
import sys
import time
import numpy as np
import pymupdf

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from llm_providers import generate_sync
from otak_db import OtakDB
from pipeline_config import DB_PATH, SEARCH_LIMIT_TREE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("ingest_book")


# ── Book Parsing ──

# Chapters we actually want to extract from (substantive content)
CONTENT_PREFIXES = ("Prologue", "1.", "2.", "3.", "4.", "5.", "6.", "7.", "8.", "Epilogue",
                    "Appendix I", "Appendix II", "Appendix III")

# Mapping from TOC titles to notes section headings
NOTES_HEADING_MAP = {
    "Prologue": "Prologue",
    "1": "Chapter 1", "2": "Chapter 2", "3": "Chapter 3", "4": "Chapter 4",
    "5": "Chapter 5", "6": "Chapter 6", "7": "Chapter 7", "8": "Chapter 8",
    "Epilogue": "Epilogue",
    "Appendix I": "Appendix I", "Appendix II": "Appendix II", "Appendix III": "Appendix III",
}


def parse_book(path: str) -> dict:
    """Parse an EPUB or PDF into chapters with metadata.

    Returns:
        {
            "title": str,
            "author": str,
            "chapters": [
                {"number": str, "title": str, "text": str, "start_page": int, "end_page": int, "notes": str}
            ],
            "full_notes": str,
        }
    """
    doc = pymupdf.open(path)
    toc = doc.get_toc()

    if not toc:
        raise RuntimeError(f"No TOC found in {path}. Cannot split into chapters.")

    # Extract metadata
    meta = doc.metadata or {}
    book_title = meta.get("title", "") or os.path.basename(path)
    book_author = meta.get("author", "")

    # Build chapter list from TOC (level 1 entries only)
    raw_chapters = []
    l1_entries = [(title, page) for level, title, page in toc if level == 1]
    for i, (title, start_page) in enumerate(l1_entries):
        end_page = l1_entries[i + 1][1] if i + 1 < len(l1_entries) else doc.page_count + 1
        raw_chapters.append({"title": title, "start_page": start_page, "end_page": end_page})

    # Extract text for each chapter
    for ch in raw_chapters:
        text = ""
        for pg_num in range(ch["start_page"] - 1, ch["end_page"] - 1):
            if pg_num < doc.page_count:
                text += doc[pg_num].get_text()
        ch["text"] = text.strip()

    # Extract Notes section
    notes_text = ""
    for ch in raw_chapters:
        if ch["title"] == "Notes":
            notes_text = ch["text"]
            break

    # Parse notes by chapter heading
    notes_by_chapter = _parse_notes_sections(notes_text)

    # Filter to content chapters and attach notes
    chapters = []
    for ch in raw_chapters:
        if not any(ch["title"].startswith(p) for p in CONTENT_PREFIXES):
            continue

        # Determine chapter key for notes lookup
        ch_key = _chapter_key(ch["title"])
        notes_heading = NOTES_HEADING_MAP.get(ch_key, "")
        ch_notes = notes_by_chapter.get(notes_heading, "")

        chapters.append({
            "number": ch_key,
            "title": ch["title"],
            "text": ch["text"],
            "start_page": ch["start_page"],
            "end_page": ch["end_page"],
            "notes": ch_notes,
        })

    doc.close()

    log.info("Parsed %d content chapters from %s", len(chapters), os.path.basename(path))
    for ch in chapters:
        log.info("  [%s] %s — %d chars, %d chars notes",
                 ch["number"], ch["title"], len(ch["text"]), len(ch["notes"]))

    return {
        "title": book_title,
        "author": book_author,
        "chapters": chapters,
        "full_notes": notes_text,
    }


def _chapter_key(title: str) -> str:
    """Extract chapter key from TOC title for notes mapping."""
    if title.startswith("Prologue"):
        return "Prologue"
    if title.startswith("Epilogue"):
        return "Epilogue"
    m = re.match(r'^(\d+)\.', title)
    if m:
        return m.group(1)
    if title.startswith("Appendix"):
        m = re.match(r'Appendix\s+(I+)', title)
        if m:
            return f"Appendix {m.group(1)}"
    return title


def _parse_notes_sections(notes_text: str) -> dict:
    """Split notes text into sections by chapter heading.

    Returns: {"Prologue": "1. ...\n2. ...", "Chapter 1": "1. ...\n2. ...", ...}
    """
    if not notes_text:
        return {}

    # Pattern: a line that is exactly a chapter heading
    heading_pattern = re.compile(
        r'^(Prologue|Chapter\s+\d+|Epilogue|Appendix\s+I{1,3})$',
        re.MULTILINE
    )

    sections = {}
    matches = list(heading_pattern.finditer(notes_text))
    for i, match in enumerate(matches):
        heading = match.group(1)
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(notes_text)
        section_text = notes_text[start:end].strip()
        sections[heading] = section_text

    return sections


# ── Endnote Parsing ──

def parse_endnotes(notes_text: str) -> list:
    """Parse individual endnote entries from a chapter's notes section.

    Returns list of {"number": int, "text": str} for each endnote.
    """
    if not notes_text:
        return []

    entries = []
    # Endnotes start with a number followed by a period or dot
    # Pattern: number at start of line (or after blank) followed by period and space
    pattern = re.compile(r'(?:^|\n)(\d+)\.\s+', re.MULTILINE)
    matches = list(pattern.finditer(notes_text))

    for i, match in enumerate(matches):
        num = int(match.group(1))
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(notes_text)
        text = notes_text[start:end].strip()
        # Collapse internal newlines
        text = re.sub(r'\n+', ' ', text)
        entries.append({"number": num, "text": text})

    return entries


# ── Extraction Schema ──

BOOK_EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "chapter_summary": {
            "type": "string",
            "description": "2-3 sentence summary of the chapter's core argument"
        },
        "main_claims": {
            "type": "array",
            "description": "Core propositions the author advances in this chapter. Aim for 4-8 per chapter. Each must be a falsifiable proposition.",
            "items": {
                "type": "object",
                "properties": {
                    "claim_id": {"type": "string", "description": "Short ID like M1, M2, etc."},
                    "text_en": {"type": "string", "description": "The claim as a clear proposition in English"},
                    "claim_type": {
                        "type": "string",
                        "enum": ["theoretical_claim", "empirical_claim", "policy_claim", "causal_claim", "historical_claim", "methodological_claim"]
                    },
                    "confidence": {"type": "number", "description": "0.0-1.0 how strongly the evidence supports this claim"},
                    "evidence_level": {
                        "type": "string",
                        "enum": ["systematic_review", "controlled_study", "observational", "qualitative", "expert_opinion", "historical_analysis", "logical_argument"]
                    },
                    "source_passage": {"type": "string", "description": "Key verbatim quote from the chapter supporting this claim"},
                    "debate_stance": {
                        "type": "string",
                        "enum": ["pro_knowledge", "anti_skills_focus", "pro_curriculum", "anti_testing", "neutral"],
                        "description": "Hirsch's position in the knowledge vs skills debate"
                    },
                },
                "required": ["claim_id", "text_en", "claim_type", "confidence", "evidence_level", "source_passage", "debate_stance"],
            },
        },
        "subclaims": {
            "type": "array",
            "description": "Supporting arguments, intermediate conclusions, and premises. Aim for 2-4 subclaims per main claim (10-25 total). Every step in the reasoning chain should appear here.",
            "items": {
                "type": "object",
                "properties": {
                    "claim_id": {"type": "string", "description": "Short ID like S1, S2, etc."},
                    "text_en": {"type": "string", "description": "The subclaim as a proposition"},
                    "supports_claim": {"type": "string", "description": "ID of the main claim this supports (e.g. M1)"},
                    "claim_type": {
                        "type": "string",
                        "enum": ["theoretical_claim", "empirical_claim", "policy_claim", "causal_claim", "historical_claim", "methodological_claim"]
                    },
                    "confidence": {"type": "number"},
                    "evidence_level": {
                        "type": "string",
                        "enum": ["systematic_review", "controlled_study", "observational", "qualitative", "expert_opinion", "historical_analysis", "logical_argument"]
                    },
                    "source_passage": {"type": "string", "description": "Verbatim quote supporting this subclaim"},
                },
                "required": ["claim_id", "text_en", "supports_claim", "claim_type", "confidence", "evidence_level"],
            },
        },
        "evidence": {
            "type": "array",
            "description": "Specific studies, data points, and historical examples cited. Aim for 6-15 per chapter. Include every study, dataset, or concrete example cited.",
            "items": {
                "type": "object",
                "properties": {
                    "evidence_id": {"type": "string", "description": "Short ID like E1, E2, etc."},
                    "description": {"type": "string", "description": "What the study/data/example shows"},
                    "source_reference": {"type": "string", "description": "Author, title, date — as cited in the book"},
                    "evidences_claim": {"type": "string", "description": "ID of claim this evidences (e.g. M1 or S3)"},
                    "evidence_type": {
                        "type": "string",
                        "enum": ["empirical_study", "statistical_data", "historical_example", "international_comparison", "natural_experiment", "anecdote", "expert_testimony"]
                    },
                    "endnote_numbers": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "description": "Endnote numbers that contain the full citation for this evidence"
                    },
                },
                "required": ["evidence_id", "description", "evidences_claim", "evidence_type"],
            },
        },
        "argument_structure": {
            "type": "array",
            "description": "Relationships between claims, subclaims, and evidence. Aim for 8-20 links. Every main claim should have at least 2 incoming links.",
            "items": {
                "type": "object",
                "properties": {
                    "from_id": {"type": "string", "description": "Source claim/evidence ID"},
                    "to_id": {"type": "string", "description": "Target claim/evidence ID"},
                    "relationship": {
                        "type": "string",
                        "enum": ["supports", "opposes", "evidenced_by", "assumes", "responds_to_objection", "implies", "contradicts"],
                    },
                    "explanation": {"type": "string", "description": "Brief explanation of the relationship"},
                },
                "required": ["from_id", "to_id", "relationship"],
            },
        },
        "actors": {
            "type": "array",
            "description": "People, institutions, and organizations mentioned",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "role": {"type": "string", "description": "Role in the argument: cited_approvingly, cited_critically, argued_against, historical_figure, institution"},
                    "context": {"type": "string", "description": "Brief note on why they appear"},
                },
                "required": ["name", "role"],
            },
        },
        "research_questions": {
            "type": "array",
            "description": "Questions the chapter addresses or raises (2-5)",
            "items": {
                "type": "object",
                "properties": {
                    "text_en": {"type": "string", "description": "The question in English"},
                    "addressed_by": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "IDs of claims that address this question"
                    },
                },
                "required": ["text_en", "addressed_by"],
            },
        },
        "key_terms": {
            "type": "array",
            "description": "Important terms/concepts defined or used in a specific way",
            "items": {
                "type": "object",
                "properties": {
                    "term": {"type": "string"},
                    "definition": {"type": "string", "description": "How the author defines or uses this term"},
                },
                "required": ["term", "definition"],
            },
        },
    },
    "required": ["chapter_summary", "main_claims", "subclaims", "evidence", "argument_structure", "actors", "research_questions"],
}


# ── Extraction Prompt ──

BOOK_SYSTEM_PROMPT = """You are extracting the argument structure from a chapter of a non-fiction book.

This is NOT a news article or blog post — it's a sustained intellectual argument built across many pages.
The author constructs complex chains of reasoning: premises → intermediate conclusions → main claims,
supported by empirical evidence, historical examples, and logical argument.

YOUR GOAL: Capture the FULL argument structure, not just surface-level claims.

EXTRACTION PRINCIPLES:

1. **MAIN CLAIMS** (3-8 per chapter): These are the chapter's core propositions — what the author
   wants you to believe after reading. They should be falsifiable statements, not topic labels.
   BAD: "Testing is discussed"
   GOOD: "Current standardized reading tests are invalid because they measure background knowledge rather than a generalizable reading skill"

2. **SUBCLAIMS** (8-20 per chapter): The intermediate arguments that build toward main claims.
   This is the MOST IMPORTANT part. A book chapter builds arguments through CHAINS of reasoning,
   not just top-level assertions. Each main claim typically has 2-5 supporting subclaims.

   Think of it as: WHY should I believe the main claim? What premises does the author establish first?
   What intermediate conclusions does the author reach on the way to the main conclusion?

   For example, main claim "Reading tests are invalid" might be supported by:
   - "Reading comprehension depends primarily on domain knowledge, not decodable strategies" (premise)
   - "Strategy instruction reaches a quick plateau of effectiveness" (intermediate finding)
   - "Tests that sample random topics measure family background more than school quality" (consequence)
   - "Content-free reading standards provide no instructional guidance to teachers" (structural problem)
   - "Test prep displaces knowledge-building instruction in history, science, and arts" (mechanism)

   Every significant argument step in the chapter should appear as either a main claim or subclaim.

3. **EVIDENCE** (5-15 per chapter): Specific empirical ammunition. Include:
   - What exactly the study/data shows (not just "a study supports this")
   - Who conducted it, approximate date if given
   - The endnote numbers so citations can be resolved later
   Keep descriptions precise: "NAEP scores for 17-year-olds declined from 290 to 287 between 1988-2012"
   not "test scores went down"

4. **ARGUMENT STRUCTURE** (5-15 links): Map the logical flow. The author may:
   - Build up to a conclusion through premises (supports)
   - Anticipate and respond to objections (responds_to_objection)
   - Show how one claim logically implies another (implies)
   - Identify hidden assumptions (assumes)
   - Contradict opposing views (contradicts/opposes)
   Every main claim should have at least one incoming link. Most subclaims should too.

5. **DEBATE STANCE**: This author is explicitly arguing for knowledge-based education and against
   skills-focused approaches. Capture which side of the debate each claim falls on.

6. **ACTORS**: People and institutions. Note whether they're cited approvingly (allies) or
   critically (opponents in the argument).

CONFIDENCE CALIBRATION:
- Claims backed by multiple cited studies: 0.80-0.95
- Claims backed by a single study or historical example: 0.65-0.80
- Logical arguments without direct empirical backing: 0.50-0.70
- Rhetorical claims or value judgments: 0.40-0.60
- Speculative or extrapolative claims: 0.30-0.50

IMPORTANT:
- Extract claims as PROPOSITIONS, not as descriptions of what the author says.
  BAD: "Hirsch argues that testing is flawed"
  GOOD: "Standardized reading tests that sample random passage topics produce scores driven by students' prior knowledge rather than taught skills"
- source_passage must be a VERBATIM quote from the chapter text.
- Be thorough — a book chapter contains much more argument structure than a news article.
  Aim for 25-40 total items (main + sub + evidence) per chapter.
- Endnote numbers appear as superscript numbers in the text. Note them in evidence entries."""


def build_book_extraction_prompt(chapter: dict, book_info: dict, endnotes: list) -> tuple[str, str]:
    """Build (system_prompt, user_prompt) for a book chapter extraction."""
    endnotes_text = ""
    if endnotes:
        endnotes_text = "\n\nENDNOTES FOR THIS CHAPTER:\n"
        for en in endnotes:
            endnotes_text += f"  {en['number']}. {en['text']}\n"

    user_prompt = f"""Book: {book_info['title']}
Author: {book_info['author']}
Chapter: {chapter['title']}
Pages: {chapter['start_page']}-{chapter['end_page']}

CHAPTER TEXT:
{chapter['text']}
{endnotes_text}"""

    return BOOK_SYSTEM_PROMPT, user_prompt


# ── Extraction ──

def extract_chapter(chapter: dict, book_info: dict, endnotes: list) -> tuple[dict, dict]:
    """Extract claims from a single chapter. Returns (result, meta)."""
    system_prompt, user_prompt = build_book_extraction_prompt(chapter, book_info, endnotes)

    log.info("Extracting chapter '%s' (%d chars text + %d endnotes)...",
             chapter["title"], len(chapter["text"]), len(endnotes))

    t0 = time.time()
    result, meta = generate_sync(
        prompt=user_prompt,
        system_prompt=system_prompt,
        json_schema=BOOK_EXTRACTION_SCHEMA,
        model="gemini3-flash",
        max_tokens=16384,
    )
    elapsed = time.time() - t0

    n_main = len(result.get("main_claims", []))
    n_sub = len(result.get("subclaims", []))
    n_ev = len(result.get("evidence", []))
    n_links = len(result.get("argument_structure", []))
    n_actors = len(result.get("actors", []))
    n_questions = len(result.get("research_questions", []))

    log.info("  Extracted: %d main claims, %d subclaims, %d evidence, %d argument links, %d actors, %d questions (%.1fs, $%.4f)",
             n_main, n_sub, n_ev, n_links, n_actors, n_questions, elapsed, meta.get("total_cost_usd", 0))

    return result, meta


# ── Display ──

def print_extraction(result: dict, chapter_title: str):
    """Pretty-print extraction results for review."""
    print(f"\n{'='*80}")
    print(f"  CHAPTER: {chapter_title}")
    print(f"{'='*80}")

    print(f"\n  SUMMARY: {result.get('chapter_summary', 'N/A')}")

    print(f"\n  MAIN CLAIMS ({len(result.get('main_claims', []))}):")
    print(f"  {'-'*40}")
    for c in result.get("main_claims", []):
        print(f"  [{c['claim_id']}] {c['text_en']}")
        print(f"       Type: {c.get('claim_type', '?')} | Confidence: {c.get('confidence', '?')} | Evidence: {c.get('evidence_level', '?')} | Stance: {c.get('debate_stance', '?')}")
        passage = c.get("source_passage", "")
        if passage:
            if len(passage) > 120:
                passage = passage[:120] + "..."
            print(f"       Quote: \"{passage}\"")
        print()

    print(f"  SUBCLAIMS ({len(result.get('subclaims', []))}):")
    print(f"  {'-'*40}")
    for s in result.get("subclaims", []):
        print(f"  [{s['claim_id']}] {s['text_en']}")
        print(f"       Supports: {s.get('supports_claim', '?')} | Type: {s.get('claim_type', '?')} | Confidence: {s.get('confidence', '?')}")
        print()

    print(f"  EVIDENCE ({len(result.get('evidence', []))}):")
    print(f"  {'-'*40}")
    for e in result.get("evidence", []):
        print(f"  [{e['evidence_id']}] {e['description']}")
        print(f"       Evidences: {e.get('evidences_claim', '?')} | Type: {e.get('evidence_type', '?')}")
        if e.get("source_reference"):
            print(f"       Ref: {e['source_reference']}")
        if e.get("endnote_numbers"):
            print(f"       Endnotes: {e['endnote_numbers']}")
        print()

    print(f"  ARGUMENT STRUCTURE ({len(result.get('argument_structure', []))}):")
    print(f"  {'-'*40}")
    for a in result.get("argument_structure", []):
        print(f"  {a['from_id']} --[{a['relationship']}]--> {a['to_id']}", end="")
        if a.get("explanation"):
            print(f"  ({a['explanation']})", end="")
        print()

    print(f"\n  ACTORS ({len(result.get('actors', []))}):")
    print(f"  {'-'*40}")
    for a in result.get("actors", []):
        print(f"  {a['name']} ({a.get('role', '?')})", end="")
        if a.get("context"):
            print(f" — {a['context']}", end="")
        print()

    print(f"\n  RESEARCH QUESTIONS ({len(result.get('research_questions', []))}):")
    print(f"  {'-'*40}")
    for q in result.get("research_questions", []):
        print(f"  Q: {q['text_en']}")
        print(f"     Addressed by: {q.get('addressed_by', [])}")

    if result.get("key_terms"):
        print(f"\n  KEY TERMS ({len(result.get('key_terms', []))}):")
        print(f"  {'-'*40}")
        for t in result.get("key_terms", []):
            print(f"  {t['term']}: {t['definition']}")

    print()


# ── Quality Evaluation ──

def evaluate_extraction(result: dict, chapter: dict) -> dict:
    """Evaluate extraction quality with basic heuristics."""
    issues = []
    scores = {}

    main_claims = result.get("main_claims", [])
    subclaims = result.get("subclaims", [])
    evidence = result.get("evidence", [])
    arg_structure = result.get("argument_structure", [])

    # Count checks
    if len(main_claims) < 3:
        issues.append(f"Only {len(main_claims)} main claims — expected 3-8 for a book chapter")
    if len(main_claims) > 10:
        issues.append(f"{len(main_claims)} main claims — too many, should be 3-8 core propositions")
    scores["main_claims_count"] = len(main_claims)

    if len(subclaims) < len(main_claims):
        issues.append(f"Only {len(subclaims)} subclaims for {len(main_claims)} main claims — argument structure may be shallow")
    scores["subclaims_count"] = len(subclaims)

    if len(evidence) < 3:
        issues.append(f"Only {len(evidence)} evidence items — Hirsch cites extensively")
    scores["evidence_count"] = len(evidence)

    if len(arg_structure) < 3:
        issues.append(f"Only {len(arg_structure)} argument links — may be missing logical connections")
    scores["argument_links_count"] = len(arg_structure)

    # Check that subclaims reference valid main claims
    main_ids = {c["claim_id"] for c in main_claims}
    for s in subclaims:
        if s.get("supports_claim") and s["supports_claim"] not in main_ids:
            issues.append(f"Subclaim {s['claim_id']} references non-existent main claim {s['supports_claim']}")

    # Check that evidence references valid claims
    all_ids = main_ids | {s["claim_id"] for s in subclaims}
    for e in evidence:
        if e.get("evidences_claim") and e["evidences_claim"] not in all_ids:
            issues.append(f"Evidence {e['evidence_id']} references non-existent claim {e['evidences_claim']}")

    # Check source passages exist in chapter text
    passages_found = 0
    passages_total = 0
    for c in main_claims:
        if c.get("source_passage"):
            passages_total += 1
            # Check if a significant substring appears in chapter text (allowing for minor differences)
            passage_words = c["source_passage"].split()[:6]
            search_str = " ".join(passage_words)
            if search_str.lower() in chapter["text"].lower():
                passages_found += 1

    if passages_total > 0:
        passage_rate = passages_found / passages_total
        scores["passage_verification_rate"] = round(passage_rate, 2)
        if passage_rate < 0.5:
            issues.append(f"Only {passages_found}/{passages_total} source passages verified in text — possible hallucination")

    # Check confidence distribution
    all_conf = [c.get("confidence", 0) for c in main_claims + subclaims]
    if all_conf:
        avg_conf = sum(all_conf) / len(all_conf)
        scores["avg_confidence"] = round(avg_conf, 2)
        if avg_conf > 0.9:
            issues.append(f"Average confidence {avg_conf:.2f} is suspiciously high — calibration may be off")

    scores["issues"] = issues
    return scores


# ── Graph Commit ──

def commit_to_graph(all_results: list, book_data: dict, graph_name: str) -> float:
    """Commit extracted book chapters to the knowledge graph.

    Returns total LLM cost for placement.
    """
    from ingest_source import (
        get_tree_skeleton, get_candidate_domain_ids,
        PLACEMENT_SCHEMA, find_or_create_actor,
    )

    db = OtakDB(DB_PATH)
    total_cost = 0.0

    # Create book source node
    book_title = book_data["title"]
    book_author = book_data["author"]
    isbn = ""
    isbn_match = re.search(r'978\d{10}', str(book_data.get("path", "")))
    if isbn_match:
        isbn = isbn_match.group(0)

    source_id = db.create(
        type="source",
        name=book_title,
        data={
            "source_type": "book_chapter",
            "author": book_author,
            "publisher": "Harvard Education Press",
            "language": "en",
            "status": "placed",
            "isbn": isbn,
        },
    )
    log.info("Created source: %s (%s)", book_title, source_id[:8])

    # Flatten all claims across chapters for batch placement
    flat_claims = []  # list of {text_en, claim_type, confidence, evidence_level, chapter, ...}
    claim_id_map = {}  # (chapter_idx, local_id) → flat_index

    for ch_idx, ch_result in enumerate(all_results):
        ext = ch_result["extraction"]
        ch_title = ch_result["chapter"]

        for mc in ext.get("main_claims", []):
            flat_idx = len(flat_claims)
            claim_id_map[(ch_idx, mc["claim_id"])] = flat_idx
            flat_claims.append({
                "text_en": mc["text_en"],
                "claim_type": mc.get("claim_type", "theoretical_claim"),
                "confidence": mc.get("confidence", 0.6),
                "evidence_level": _map_evidence_level(mc.get("evidence_level", "")),
                "source_passage": mc.get("source_passage", ""),
                "debate_stance": _map_debate_stance(mc.get("debate_stance", "")),
                "chapter": ch_title,
                "local_id": mc["claim_id"],
                "ch_idx": ch_idx,
                "is_subclaim": False,
                "supports_id": None,
            })

        for sc in ext.get("subclaims", []):
            flat_idx = len(flat_claims)
            claim_id_map[(ch_idx, sc["claim_id"])] = flat_idx
            flat_claims.append({
                "text_en": sc["text_en"],
                "claim_type": sc.get("claim_type", "theoretical_claim"),
                "confidence": sc.get("confidence", 0.6),
                "evidence_level": _map_evidence_level(sc.get("evidence_level", "")),
                "source_passage": sc.get("source_passage", ""),
                "debate_stance": "neutral",
                "chapter": ch_title,
                "local_id": sc["claim_id"],
                "ch_idx": ch_idx,
                "is_subclaim": True,
                "supports_id": sc.get("supports_claim"),
            })

    log.info("Flattened %d claims across %d chapters", len(flat_claims), len(all_results))

    # Embedding pre-routing
    candidate_domain_ids = None
    try:
        candidate_domain_ids = get_candidate_domain_ids(flat_claims, db)
    except Exception as e:
        log.warning("Pre-routing failed (%s), using full tree", e)

    if candidate_domain_ids:
        skeleton = get_tree_skeleton(db, domain_ids=candidate_domain_ids)
        log.info("Pre-routing: %d candidate domains", len(candidate_domain_ids))
    else:
        skeleton = get_tree_skeleton(db)

    # Place claims via LLM (batch all at once)
    claims_text = "\n".join(
        f"[{i}] {c['text_en']} (type: {c['claim_type']}, conf: {c['confidence']})"
        for i, c in enumerate(flat_claims)
    )

    system_prompt = """You place claims from a book into a hierarchical knowledge tree.

For each claim, choose the SINGLE best matching branch using the include/exclude descriptions.

If claims don't fit ANY existing branch, set target_branch_id to "NEW" and
propose new domains in the new_domains array. When using "NEW", also set
new_domain_name to the exact name of the new domain from the new_domains array.

If the tree shown is a SUBSET and a claim clearly belongs to a domain not shown,
set target_branch_id to "NONE".

Use 8-character branch ID prefixes from the tree skeleton."""

    prompt = f"""Current tree structure:
{skeleton}

Claims from book "{book_data['title']}" by {book_data['author']}:
{claims_text}

Place each claim into the best branch. Use "NONE" if no branch fits, "NEW" to create a domain."""

    log.info("Placing %d claims...", len(flat_claims))
    result, meta = generate_sync(prompt, system_prompt, PLACEMENT_SCHEMA, model="gemini3-flash", thinking_budget=0)
    total_cost += meta["total_cost_usd"]

    # Fallback for NONE claims
    none_indices = []
    if candidate_domain_ids:
        for p in result.get("placements", []):
            if p.get("target_branch_id") == "NONE":
                none_indices.append(p["claim_index"])

    if none_indices:
        log.info("Fallback: %d claims got NONE, re-running with full tree", len(none_indices))
        full_skeleton = get_tree_skeleton(db)
        fallback_text = "\n".join(
            f"[{i}] {flat_claims[i]['text_en']} (type: {flat_claims[i]['claim_type']}, conf: {flat_claims[i]['confidence']})"
            for i in none_indices
        )
        fb_prompt = f"Current tree structure:\n{full_skeleton}\n\nClaims to place:\n{fallback_text}\n\nPlace each claim."
        fb_result, fb_meta = generate_sync(fb_prompt, system_prompt, PLACEMENT_SCHEMA, model="gemini3-flash", thinking_budget=0)
        total_cost += fb_meta["total_cost_usd"]

        # Merge
        final = [p for p in result.get("placements", []) if p.get("target_branch_id") != "NONE"]
        final.extend(fb_result.get("placements", []))
        result["placements"] = final
        result["new_domains"] = result.get("new_domains", []) + fb_result.get("new_domains", [])

    # Create new domains
    new_domain_map = {}  # name -> dict with "id"
    for nd in result.get("new_domains", []):
        domain_id = db.create(
            type="branch",
            name=nd["name"],
            data={
                "include_description": nd["include_description"],
                "exclude_description": nd["exclude_description"],
                "needs_split": "no",
                "child_count": 0,
            },
        )
        new_domain_map[nd["name"]] = {"id": domain_id, "name": nd["name"]}
        log.info("  New domain: %s (%s)", nd["name"], domain_id[:8])

    # Build branch lookup: 8-char prefix -> dict
    branches = db.where("branch", limit=SEARCH_LIMIT_TREE)
    branch_map = {b["id"][:8]: b for b in branches}
    for ddict in new_domain_map.values():
        branch_map[ddict["id"][:8]] = ddict

    # Create claim nodes
    extraction_ids = []
    new_claim_ids = []  # for embedding persistence
    canonical_map = {}  # flat_index → canonical_id
    placed = 0
    errors = 0

    for p in result.get("placements", []):
        idx = p.get("claim_index", -1)
        if idx < 0 or idx >= len(flat_claims):
            errors += 1
            continue

        cd = flat_claims[idx]
        target_prefix = p.get("target_branch_id", "")

        if target_prefix == "NEW":
            dname = p.get("new_domain_name", "")
            target_dict = new_domain_map.get(dname) or (list(new_domain_map.values())[0] if new_domain_map else None)
            if not target_dict:
                errors += 1
                continue
            target_id = target_dict["id"]
        elif target_prefix == "NONE":
            errors += 1
            continue
        else:
            ref = branch_map.get(target_prefix)
            if not ref:
                errors += 1
                continue
            target_id = ref["id"]

        # Build path: target branch's path + target_id
        target_node = db.get(target_id)
        canonical_path = (target_node["path"] + [target_id]) if target_node else [target_id]

        # Create canonical claim
        content_parts = []
        if cd.get("source_passage"):
            content_parts.append(f'**Source passage:** "{cd["source_passage"]}"')
        content_parts.append(f"**Chapter:** {cd['chapter']}")
        content = "\n\n".join(content_parts)

        claim_fields = {
            "item_type": "claim",
            "formality": "candidate",
            "confidence": cd["confidence"],
            "source_ref": f"isbn:{isbn}" if isbn else book_title,
        }
        if cd.get("evidence_level"):
            claim_fields["evidence_level"] = cd["evidence_level"]
        if cd.get("debate_stance") and cd["debate_stance"] != "neutral":
            claim_fields["debate_stance"] = cd["debate_stance"]
        if cd.get("claim_type"):
            claim_fields["claim_type"] = cd["claim_type"]

        canonical_id = db.create(
            type="claim",
            name=cd["text_en"],
            content=content,
            data=claim_fields,
            path=canonical_path,
        )
        new_claim_ids.append(canonical_id)
        canonical_map[idx] = canonical_id

        # Create extraction node under canonical
        ext_id = db.create(
            type="claim",
            name=cd["text_en"],
            content=content,
            data={
                "item_type": "extraction",
                "formality": "raw",
                "confidence": cd["confidence"],
                "source_ref": f"isbn:{isbn}" if isbn else book_title,
            },
            path=canonical_path + [canonical_id],
        )
        extraction_ids.append(ext_id)
        placed += 1

    log.info("Placed %d claims (%d errors)", placed, errors)

    # Persist embeddings for new claims
    if new_claim_ids:
        embed_model = db.get_embed_model()
        names = []
        valid_ids = []
        for cid in new_claim_ids:
            node = db.get(cid)
            if node:
                names.append(node["name"])
                valid_ids.append(cid)
        if names:
            vecs = np.array(embed_model.embed_batch(names))
            db.set_embeddings_batch(valid_ids, vecs)
            log.info("Persisted embeddings for %d new claims", len(valid_ids))

    # Link source → extractions
    for eid in extraction_ids:
        db.link(source_id, eid, "linked_claims")

    # Create evidence nodes
    ev_count = 0
    ev_ids = []
    for ch_idx, ch_result in enumerate(all_results):
        ext = ch_result["extraction"]
        for ev in ext.get("evidence", []):
            target_claim_id = ev.get("evidences_claim")
            flat_idx = claim_id_map.get((ch_idx, target_claim_id))
            parent_id = canonical_map.get(flat_idx) if flat_idx is not None else None
            if not parent_id:
                continue

            ev_text = ev["description"]
            if ev.get("source_reference"):
                ev_text += f" (Ref: {ev['source_reference']})"

            parent_node = db.get(parent_id)
            ev_path = (parent_node["path"] + [parent_id]) if parent_node else [parent_id]

            ev_id = db.create(
                type="claim",
                name=ev_text,
                data={
                    "item_type": "evidence",
                    "formality": "raw",
                    "confidence": 0.7,
                    "source_ref": f"isbn:{isbn}" if isbn else book_title,
                },
                path=ev_path,
            )
            ev_ids.append(ev_id)
            ev_count += 1

            # Link evidence → claim
            try:
                db.link(parent_id, ev_id, "evidenced_by")
            except Exception:
                pass

    log.info("Created %d evidence nodes", ev_count)

    # Persist embeddings for evidence nodes
    if ev_ids:
        embed_model = db.get_embed_model()
        ev_names = []
        ev_valid_ids = []
        for eid in ev_ids:
            node = db.get(eid)
            if node:
                ev_names.append(node["name"])
                ev_valid_ids.append(eid)
        if ev_names:
            vecs = np.array(embed_model.embed_batch(ev_names))
            db.set_embeddings_batch(ev_valid_ids, vecs)
            log.info("Persisted embeddings for %d evidence nodes", len(ev_valid_ids))

    # Create argument links from argument_structure
    link_count = 0
    for ch_idx, ch_result in enumerate(all_results):
        ext = ch_result["extraction"]
        for link in ext.get("argument_structure", []):
            from_idx = claim_id_map.get((ch_idx, link["from_id"]))
            to_idx = claim_id_map.get((ch_idx, link["to_id"]))
            from_id = canonical_map.get(from_idx) if from_idx is not None else None
            to_id = canonical_map.get(to_idx) if to_idx is not None else None
            if not from_id or not to_id:
                continue

            rel = link.get("relationship", "supports")
            link_type = _rel_to_field(rel)
            if not link_type:
                continue

            try:
                db.link(from_id, to_id, link_type)
                link_count += 1
            except Exception:
                pass

    log.info("Created %d argument links", link_count)

    # Create actors
    actor_count = 0
    for ch_idx, ch_result in enumerate(all_results):
        ext = ch_result["extraction"]
        for actor in ext.get("actors", []):
            try:
                find_or_create_actor(actor["name"], db)
                actor_count += 1
            except Exception:
                pass

    log.info("Processed %d actors", actor_count)

    # Create research questions
    q_count = 0
    q_ids = []
    for ch_idx, ch_result in enumerate(all_results):
        ext = ch_result["extraction"]
        for q in ext.get("research_questions", []):
            addressed_by_ids = []
            for cid in q.get("addressed_by", []):
                fi = claim_id_map.get((ch_idx, cid))
                cn_id = canonical_map.get(fi) if fi is not None else None
                if cn_id:
                    addressed_by_ids.append(cn_id)

            # Place under first addressed claim's branch
            parent_id = None
            if addressed_by_ids:
                first_claim = db.get(addressed_by_ids[0])
                if first_claim and first_claim["path"]:
                    parent_id = first_claim["path"][-1]
            if not parent_id:
                continue

            parent_node = db.get(parent_id)
            q_path = (parent_node["path"] + [parent_id]) if parent_node else [parent_id]

            q_id = db.create(
                type="claim",
                name=q["text_en"],
                data={"item_type": "question", "formality": "raw"},
                path=q_path,
            )
            q_ids.append(q_id)

            # Link claims → question via "addresses"
            for addr_id in addressed_by_ids:
                try:
                    db.link(addr_id, q_id, "addresses")
                except Exception:
                    pass

            q_count += 1

    log.info("Created %d research questions", q_count)

    # Persist embeddings for research questions
    if q_ids:
        embed_model = db.get_embed_model()
        q_names = []
        q_valid_ids = []
        for qid in q_ids:
            node = db.get(qid)
            if node:
                q_names.append(node["name"])
                q_valid_ids.append(qid)
        if q_names:
            vecs = np.array(embed_model.embed_batch(q_names))
            db.set_embeddings_batch(q_valid_ids, vecs)
            log.info("Persisted embeddings for %d question nodes", len(q_valid_ids))

    log.info("Commit complete: %d claims, %d evidence, %d links, %d actors, %d questions ($%.4f)",
             placed, ev_count, link_count, actor_count, q_count, total_cost)

    return total_cost


def _map_evidence_level(level: str) -> str:
    """Map book extraction evidence levels to schema enum values."""
    mapping = {
        "systematic_review": "systematic_review",
        "controlled_study": "controlled_study",
        "observational": "observational",
        "qualitative": "qualitative",
        "expert_opinion": "expert_opinion",
        "historical_analysis": "expert_opinion",
        "logical_argument": "expert_opinion",
    }
    return mapping.get(level, "")


def _map_debate_stance(stance: str) -> str:
    """Map book-specific debate stances to schema enum values."""
    mapping = {
        "pro_knowledge": "pro",
        "anti_skills_focus": "contra",
        "pro_curriculum": "pro",
        "anti_testing": "contra",
        "neutral": "neutral",
    }
    return mapping.get(stance, "neutral")


def _rel_to_field(relationship: str) -> str:
    """Map argument relationship to knowledge_item ref field."""
    mapping = {
        "supports": "supports",
        "opposes": "opposes",
        "evidenced_by": "evidenced_by",
        "implies": "supports",
        "assumes": "supports",
        "responds_to_objection": "opposes",
        "contradicts": "opposes",
    }
    return mapping.get(relationship, "")


# ── CLI ──

def main():
    parser = argparse.ArgumentParser(description="Ingest a book into Otak")
    parser.add_argument("path", help="Path to EPUB or PDF file (supports glob patterns)")
    parser.add_argument("--chapter", type=int, help="Process only this chapter number (0=Prologue)")
    parser.add_argument("--dry-run", action="store_true", help="Extract and print without writing to graph")
    parser.add_argument("--graph", default="otak_v2", help="Graph name (default: otak_v2)")
    parser.add_argument("--output", help="Write JSON results to this file")
    args = parser.parse_args()

    # Resolve glob patterns in path
    path = args.path
    if "*" in path or "?" in path:
        matches = glob.glob(path)
        if not matches:
            log.error("No files match pattern: %s", path)
            sys.exit(1)
        path = matches[0]
        log.info("Resolved glob to: %s", path)

    if not os.path.isfile(path):
        log.error("File not found: %s", path)
        sys.exit(1)

    # Parse book
    book_data = parse_book(path)
    log.info("Book: %s by %s", book_data["title"], book_data["author"])
    log.info("Chapters: %d", len(book_data["chapters"]))

    # Select chapters
    chapters = book_data["chapters"]
    if args.chapter is not None:
        ch_key = str(args.chapter) if args.chapter > 0 else "Prologue"
        chapters = [ch for ch in chapters if ch["number"] == ch_key]
        if not chapters:
            log.error("Chapter '%s' not found. Available: %s",
                      ch_key, [c["number"] for c in book_data["chapters"]])
            sys.exit(1)

    # Process chapters
    all_results = []
    total_cost = 0.0

    for chapter in chapters:
        # Parse endnotes for this chapter
        endnotes = parse_endnotes(chapter["notes"])
        log.info("Chapter '%s': %d endnotes parsed", chapter["title"], len(endnotes))

        # Extract
        result, meta = extract_chapter(chapter, book_data, endnotes)
        cost = meta.get("total_cost_usd", 0)
        total_cost += cost

        if args.dry_run:
            print_extraction(result, chapter["title"])
            eval_result = evaluate_extraction(result, chapter)
            print(f"  QUALITY EVALUATION:")
            print(f"  {'-'*40}")
            for k, v in eval_result.items():
                if k != "issues":
                    print(f"  {k}: {v}")
            if eval_result["issues"]:
                print(f"\n  ISSUES ({len(eval_result['issues'])}):")
                for issue in eval_result["issues"]:
                    print(f"    - {issue}")
            else:
                print(f"  No issues detected.")
            print()

        all_results.append({
            "chapter": chapter["title"],
            "chapter_number": chapter["number"],
            "extraction": result,
            "endnotes": endnotes,
            "meta": {
                "cost_usd": cost,
                "model": meta.get("model", ""),
                "input_tokens": meta.get("input_tokens", 0),
                "output_tokens": meta.get("output_tokens", 0),
            },
        })

    # Summary
    total_main = sum(len(r["extraction"].get("main_claims", [])) for r in all_results)
    total_sub = sum(len(r["extraction"].get("subclaims", [])) for r in all_results)
    total_ev = sum(len(r["extraction"].get("evidence", [])) for r in all_results)
    total_links = sum(len(r["extraction"].get("argument_structure", [])) for r in all_results)
    log.info("Extraction complete: %d chapters, %d main claims, %d subclaims, %d evidence, %d links (%.4f USD)",
             len(all_results), total_main, total_sub, total_ev, total_links, total_cost)

    # Write output
    if args.output:
        with open(args.output, "w") as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        log.info("Results written to %s", args.output)

    # Commit to graph
    if not args.dry_run:
        commit_cost = commit_to_graph(all_results, book_data, args.graph)
        total_cost += commit_cost
        log.info("Total cost (extraction + commit): $%.4f", total_cost)


if __name__ == "__main__":
    main()
