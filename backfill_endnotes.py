"""Backfill endnote_numbers on evidence items by fuzzy-matching source_reference to endnote text.

For each chapter:
  1. Load merged.json evidence items
  2. Parse endnotes from the PDF
  3. For evidence items WITHOUT endnote_numbers but WITH source_reference,
     search endnotes for matching author names / title words
  4. Update merged.json in place
  5. Update book_consolidated.json

Conservative matching: requires distinctive n-grams or multiple author surnames
to appear in the endnote text (case-insensitive, punctuation-stripped).

Usage:
    .venv-otak/bin/python3 prototypes/hirsch/backfill_endnotes.py /tmp/hirsch-wkm.pdf
    .venv-otak/bin/python3 prototypes/hirsch/backfill_endnotes.py /tmp/hirsch-wkm.pdf --dry-run
"""
import argparse
import json
import os
import re
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "prototypes", "hirsch"))

from extract import parse_book, parse_endnotes

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "wkm")

# Map from parse_book chapter["number"] to directory slug
CHAPTER_SLUG_MAP = {
    "Prologue": "prologue",
    "1": "1", "2": "2", "3": "3", "4": "4",
    "5": "5", "6": "6", "7": "7", "8": "8",
    "Epilogue": "epilogue",
    "Appendix I": "appendix-i",
    "Appendix II": "appendix-ii",
    "Appendix III": "appendix-iii",
}

# Words that are too common to be meaningful in n-gram matches
STOPWORDS = {
    "a", "an", "the", "of", "in", "on", "at", "to", "for", "and", "or", "but",
    "is", "are", "was", "were", "be", "been", "by", "from", "with", "as", "not",
    "no", "its", "it", "this", "that", "than", "has", "had", "have", "do", "does",
    "did", "will", "would", "can", "could", "may", "might", "shall", "should",
    "all", "each", "every", "both", "few", "more", "most", "other", "some", "such",
    "new", "old", "also", "about", "into", "over", "after", "before", "between",
    "under", "through", "during", "their", "our", "your", "his", "her", "we", "they",
    "what", "which", "who", "when", "where", "how", "why",
}

# Common academic/bibliographic words that are not distinctive for matching
GENERIC_ACADEMIC = {
    "education", "educational", "school", "schools", "students", "student",
    "reading", "research", "study", "studies", "report", "review", "journal",
    "press", "university", "vol", "volume", "chapter", "american", "national",
    "policy", "public", "science", "academic", "teaching", "teacher", "teachers",
    "learning", "test", "testing", "tests", "assessment", "curriculum",
    "children", "child", "program", "programs", "york", "washington",
    "london", "paris", "oxford", "cambridge", "institute", "center", "department",
    "international", "united", "states", "dc",
}


def normalize(text: str) -> str:
    """Lowercase, strip punctuation except hyphens, collapse whitespace."""
    text = text.lower()
    text = re.sub(r"[^\w\s\-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_distinctive_ngram(ngram: str) -> bool:
    """Check if an n-gram is distinctive enough to be a reliable match signal.

    Requires at least one word that is not a stopword or generic academic term,
    and that distinctive word must be 4+ characters.
    """
    words = ngram.split()
    noise = STOPWORDS | GENERIC_ACADEMIC
    distinctive = [w for w in words if w not in noise and len(w) >= 4]
    return len(distinctive) >= 1


def extract_distinctive_ngrams(text: str, min_n: int = 2, max_n: int = 6) -> list[str]:
    """Extract distinctive word n-grams from text, longest first."""
    words = normalize(text).split()
    ngrams = []
    for n in range(max_n, min_n - 1, -1):
        for i in range(len(words) - n + 1):
            ng = " ".join(words[i : i + n])
            if is_distinctive_ngram(ng):
                ngrams.append(ng)
    return ngrams


def extract_author_surnames(source_ref: str) -> list[str]:
    """Extract likely author surnames from a source reference string.

    Only extracts words that look like proper names (capitalized, not common
    English/French/academic words). Conservative to avoid false positives.
    """
    surnames = []
    # Remove parenthetical years, quoted titles, and common noise
    cleaned = re.sub(r"\(\d{4}\)", "", source_ref)
    cleaned = re.sub(r"\b(et al\.?|eds?\.?|ed\.?)\b", "", cleaned)
    # Remove quoted/titled portions (these are title words, not author names)
    cleaned = re.sub(r"['\u2018\u2019\u201c\u201d\"'].+?['\u2018\u2019\u201c\u201d\"']", "", cleaned)

    # Split on common delimiters between authors
    parts = re.split(r"[,;:]|\band\b", cleaned)
    for part in parts:
        part = part.strip()
        if not part:
            continue
        words = part.split()
        for w in words:
            w_clean = re.sub(r"[^\w]", "", w)
            # Comprehensive list of words that look capitalized but aren't surnames
            noise_caps = {
                # Common English words
                "The", "And", "For", "With", "From", "New", "York", "Not",
                "How", "Why", "What", "Are", "Our", "Can", "Has", "When",
                "Where", "Who", "Its", "All", "More", "Also", "Their",
                "Some", "Most", "Many", "Between", "Through", "After",
                "Before", "Over", "Under", "Into", "About", "Than", "Both",
                "Each", "Every", "Other", "Very", "Just", "Only", "First",
                # Academic/bibliographic
                "Press", "University", "Journal", "Review", "Education",
                "Educational", "American", "National", "Report", "Studies",
                "Research", "Assessment", "Washington", "London", "Paris",
                "Oxford", "Cambridge", "Schools", "School", "Reading",
                "Science", "Policy", "Public", "Note", "Vol", "Chapter",
                "Center", "Institute", "International", "United", "States",
                "Testing", "Test", "Teachers", "Teacher", "Students",
                "Achievement", "Gap", "Progress", "Stopped",
                "Black", "White", "Service", "Council", "Great", "City",
                "Core", "Knowledge", "Foundation", "Department",
                "State", "Standards", "Committee", "Board", "Bureau",
                "Common", "Children", "Nation", "Risk", "Letter",
                "Education", "Curriculum", "Learning", "Teaching",
                # French common words
                "Les", "Des", "Une", "Loi", "Sur", "Pour", "Dans",
                "Par", "Avec", "Cette", "Ses", "Aux", "Est",
                # Other
                "Recent", "Years", "Elementary", "Ministry", "Culture",
                "Sports", "Technology", "Course", "Study", "Grade",
                "Grades", "Rankings", "Evaluation", "General",
            }
            if len(w_clean) >= 3 and w_clean[0].isupper() and w_clean not in noise_caps:
                surnames.append(w_clean.lower())
    return list(dict.fromkeys(surnames))  # deduplicate preserving order


def score_endnote_match(
    source_ref: str,
    endnote_text: str,
    surnames: list[str],
    ngrams: list[str],
) -> tuple[float, str]:
    """Score how well a source reference matches an endnote.

    Returns (score, reason). Score 0 = no match. Higher = better match.
    """
    norm_en = normalize(endnote_text)

    # Strategy 1: Long distinctive n-gram (3+ words) — strong signal
    for ng in ngrams:
        word_count = len(ng.split())
        if word_count >= 3 and ng in norm_en:
            return (3.0 + word_count * 0.5, f"ngram({word_count}w): \"{ng}\"")

    # Strategy 2: Multiple author surnames in same endnote
    if len(surnames) >= 2:
        found = [s for s in surnames if s in norm_en]
        if len(found) >= 2:
            return (2.0 + len(found) * 0.5, f"surnames: {found}")

    # Strategy 3: Surname + a distinctive 2-word n-gram containing that surname
    # The n-gram must include the surname to ensure they co-occur in context
    if surnames:
        for s in surnames:
            if s in norm_en:
                for ng in ngrams:
                    ng_words = ng.split()
                    if len(ng_words) >= 2 and s in ng_words and ng in norm_en:
                        return (1.5, f"surname \"{s}\" + ngram \"{ng}\"")

    # Strategy 4: 2-word proper name bigram (both capitalized, neither is a common word)
    # Catches things like "Sonia Sotomayor", "Neville Bennett" etc.
    ref_words = source_ref.split()
    generic_proper = {
        "The", "And", "For", "New", "Les", "La", "Le", "Der", "Die", "Das",
        "Education", "National", "Commission", "Excellence", "Common",
        "Core", "Knowledge", "Department", "Council", "Great", "City",
        "American", "United", "States", "School", "Schools", "Testing",
        "Report", "Review", "Studies", "Research", "Science", "Public",
        "State", "Standards", "Foundation", "Children", "Nation", "Risk",
        "Achievement", "Gap", "Service", "Progress", "Board", "Bureau",
        "Institute", "Center", "International", "Ministry", "Culture",
    }
    for i in range(len(ref_words) - 1):
        w1 = re.sub(r"[^\w]", "", ref_words[i])
        w2 = re.sub(r"[^\w]", "", ref_words[i + 1])
        if not w1 or not w2:
            continue
        # Both capitalized, both 4+ chars, neither is generic
        if (w1[0].isupper() and w2[0].isupper()
                and len(w1) >= 4 and len(w2) >= 4
                and w1 not in generic_proper and w2 not in generic_proper):
            bigram = normalize(f"{w1} {w2}")
            if bigram in norm_en:
                return (2.0, f"proper-bigram: \"{bigram}\"")

    # Strategy 5: Single highly-distinctive surname (7+ chars, rare)
    # Lower confidence — only accept names that are very unlikely to appear by
    # coincidence. Reject common English/French words that look like surnames.
    common_long_words = {
        "japanese", "american", "national", "education", "educational",
        "research", "students", "children", "teachers", "teaching",
        "learning", "reading", "schools", "assessment", "assessments",
        "achievement", "standards", "curriculum", "foundation",
        "knowledge", "progress", "department", "university",
        "international", "institute", "mathematics", "information",
        "committee", "commission", "political", "economic", "cultural",
        "historical", "republican", "democratic", "european", "scientific",
        "technical", "practical", "elementary", "secondary", "connecticut",
        "unpublished", "evaluation", "rankings", "visitor", "visitors",
        "webster", "johnson", "accounts",
    }
    if len(surnames) == 1:
        s = surnames[0]
        if len(s) >= 7 and s not in common_long_words and s in norm_en:
            return (1.0, f"distinctive-surname: \"{s}\"")

    return (0.0, "")


def match_endnotes(
    source_ref: str,
    source_passage: str,
    endnotes: list[dict],
) -> tuple[list[int], list[str]]:
    """Find endnotes that match a source reference.

    Returns (list of matching endnote numbers, list of match reasons).
    Conservative: picks the best-matching endnote(s), not all partial matches.
    """
    if not source_ref or not endnotes:
        return [], []

    # Skip self-references, vague references, and non-references
    skip_patterns = [
        r"^hirsch\b",
        r"^e\.?d\.?\s*hirsch",
        r"^author\b",
        r"^personal",
        r"^not specified",
        r"^internet",
        r"^teacher social media",
        r"^endnote\b",
        r"^n/?a$",
        r"^none\b",
        r"^various\b",
        r"^see\b",
        r"^general\b",
        r"^core knowledge\b",
        r"^webster",
    ]
    ref_lower = source_ref.lower().strip()
    for pat in skip_patterns:
        if re.match(pat, ref_lower):
            return [], []

    # Also skip very short references (< 5 chars)
    if len(source_ref.strip()) < 5:
        return [], []

    surnames = extract_author_surnames(source_ref)
    ngrams = extract_distinctive_ngrams(source_ref, min_n=2, max_n=6)

    # Score every endnote
    scored = []
    for en in endnotes:
        score, reason = score_endnote_match(source_ref, en["text"], surnames, ngrams)
        if score > 0:
            scored.append((score, en["number"], reason))

    if not scored:
        return [], []

    # Sort by score descending
    scored.sort(key=lambda x: -x[0])

    # Take the best match, plus any others within 0.5 of the best score
    # (handles cases where a source is cited in multiple endnotes)
    best_score = scored[0][0]
    threshold = best_score - 0.5

    results = [(num, reason) for score, num, reason in scored if score >= threshold]

    # Cap at 3 matches — more than that is almost certainly false positives
    if len(results) > 3:
        results = results[:3]

    return [r[0] for r in results], [r[1] for r in results]


def run_backfill(pdf_path: str, dry_run: bool = False):
    print(f"Parsing book from {pdf_path}...")
    book = parse_book(pdf_path)

    total_enriched = 0
    total_targets = 0
    total_skipped = 0
    examples_match = []
    examples_nomatch = []
    chapter_stats = []

    # Build endnote lookup per chapter
    endnotes_by_slug = {}
    for ch in book["chapters"]:
        slug = CHAPTER_SLUG_MAP.get(ch["number"])
        if slug:
            endnotes_by_slug[slug] = parse_endnotes(ch.get("notes", ""))

    # Process each chapter
    for slug in sorted(os.listdir(DATA_DIR)):
        merged_path = os.path.join(DATA_DIR, slug, "merged.json")
        if not os.path.isfile(merged_path):
            continue

        with open(merged_path) as f:
            data = json.load(f)

        evidence = data.get("evidence", [])
        endnotes = endnotes_by_slug.get(slug, [])

        if not endnotes:
            continue

        chapter_enriched = 0
        chapter_targets = 0

        for ev in evidence:
            has_en = bool(ev.get("endnote_numbers"))
            has_sr = bool(ev.get("source_reference"))

            if has_en or not has_sr:
                continue

            chapter_targets += 1
            total_targets += 1

            matched_nums, matched_reasons = match_endnotes(
                ev["source_reference"],
                ev.get("source_passage", ""),
                endnotes,
            )

            if matched_nums:
                chapter_enriched += 1
                total_enriched += 1
                ev["endnote_numbers"] = matched_nums

                en_texts = {e["number"]: e["text"][:120] for e in endnotes}
                examples_match.append({
                    "chapter": slug,
                    "evidence_id": ev["id"],
                    "source_ref": ev["source_reference"],
                    "matched_endnotes": matched_nums,
                    "reasons": matched_reasons,
                    "endnote_preview": [
                        f"[{n}] {en_texts.get(n, '?')}" for n in matched_nums
                    ],
                })
            else:
                examples_nomatch.append({
                    "chapter": slug,
                    "evidence_id": ev["id"],
                    "source_ref": ev["source_reference"],
                })

        if chapter_targets > 0:
            chapter_stats.append({
                "chapter": slug,
                "targets": chapter_targets,
                "enriched": chapter_enriched,
            })

        # Write back
        if chapter_enriched > 0 and not dry_run:
            with open(merged_path, "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

    # Update book_consolidated.json
    consolidated_path = os.path.join(DATA_DIR, "book_consolidated.json")
    consolidated_updated = 0
    if os.path.isfile(consolidated_path) and total_enriched > 0:
        with open(consolidated_path) as f:
            consolidated = json.load(f)

        # Build lookup from per-chapter merged.json (after updates)
        enriched_lookup = {}
        for slug in sorted(os.listdir(DATA_DIR)):
            merged_path = os.path.join(DATA_DIR, slug, "merged.json")
            if not os.path.isfile(merged_path):
                continue
            with open(merged_path) as f:
                ch_data = json.load(f)
            for ev in ch_data.get("evidence", []):
                if ev.get("endnote_numbers"):
                    enriched_lookup[(slug, ev["id"])] = ev["endnote_numbers"]

        # Update all_evidence in consolidated
        for ev in consolidated.get("all_evidence", []):
            ch_slug = ev.get("chapter", "")
            ev_id = ev.get("id", "")
            key = (ch_slug, ev_id)
            if key in enriched_lookup and not ev.get("endnote_numbers"):
                ev["endnote_numbers"] = enriched_lookup[key]
                consolidated_updated += 1

        if not dry_run and consolidated_updated > 0:
            with open(consolidated_path, "w") as f:
                json.dump(consolidated, f, indent=2, ensure_ascii=False)

    # Print results
    print(f"\n{'=' * 60}")
    print(f"ENDNOTE BACKFILL {'(DRY RUN)' if dry_run else 'RESULTS'}")
    print(f"{'=' * 60}")
    print(f"Total evidence items targeted: {total_targets}")
    print(f"Total enriched with endnotes:  {total_enriched}")
    print(f"Match rate:                    {total_enriched/max(total_targets,1)*100:.0f}%")
    if consolidated_updated:
        print(f"Consolidated items updated:    {consolidated_updated}")
    print()

    print("Per-chapter breakdown:")
    for cs in chapter_stats:
        bar = "#" * cs["enriched"] + "." * (cs["targets"] - cs["enriched"])
        print(f"  {cs['chapter']:15s}  {cs['enriched']:2d}/{cs['targets']:2d}  [{bar}]")
    print()

    print(f"ALL MATCHES ({len(examples_match)}):")
    for ex in examples_match:
        print(f"  [{ex['chapter']}] {ex['evidence_id']}: \"{ex['source_ref'][:80]}\"")
        for reason, preview in zip(ex["reasons"], ex["endnote_preview"]):
            print(f"      -> {reason}")
            print(f"         {preview}")
        print()

    print(f"UNMATCHED ({len(examples_nomatch)}):")
    for ex in examples_nomatch:
        print(f"  [{ex['chapter']}] {ex['evidence_id']}: \"{ex['source_ref']}\"")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill endnote numbers on evidence items")
    parser.add_argument("pdf_path", help="Path to the book PDF")
    parser.add_argument("--dry-run", action="store_true", help="Don't write changes")
    args = parser.parse_args()

    run_backfill(args.pdf_path, dry_run=args.dry_run)
