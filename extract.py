"""Hirsch Argument Atlas — Three-Phase Extraction Pipeline.

Extracts argument structure from book chapters using separate content and structure passes,
followed by a self-critique pass. Designed to capture not just what the author claims,
but how claims depend on each other, what warrants connect evidence to claims,
and where the inference gaps are.

Usage:
    .venv-otak/bin/python3 prototypes/hirsch/extract.py skeleton /tmp/hirsch-wkm.pdf
    .venv-otak/bin/python3 prototypes/hirsch/extract.py chapter /tmp/hirsch-wkm.pdf --chapter prologue
    .venv-otak/bin/python3 prototypes/hirsch/extract.py chapter /tmp/hirsch-wkm.pdf --chapter prologue --model sonnet
    .venv-otak/bin/python3 prototypes/hirsch/extract.py all /tmp/hirsch-wkm.pdf
    .venv-otak/bin/python3 prototypes/hirsch/extract.py phase phase2 /tmp/hirsch-wkm.pdf --chapter prologue
"""
import argparse
import json
import logging
import os
import re
import sys
import time

# Add project root to path for imports
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))

from ingest_book import parse_book as _parse_book_orig, parse_endnotes, _parse_notes_sections, _chapter_key
from llm_providers import generate_sync


def _parse_epub(path: str) -> dict:
    """Parse an ePub file into the standard book dict format.

    Uses ebooklib to read the ePub and BeautifulSoup to strip HTML.
    Handles both flat and nested (section) TOC structures.
    Returns: {"title", "author", "chapters": [{"number", "title", "text",
              "start_page", "end_page", "notes"}], "full_notes"}
    """
    import ebooklib
    from ebooklib import epub
    from bs4 import BeautifulSoup

    book = epub.read_epub(path)

    # Extract metadata
    title_meta = book.get_metadata("DC", "title")
    book_title = title_meta[0][0] if title_meta else os.path.basename(path)
    author_meta = book.get_metadata("DC", "creator")
    book_author = author_meta[0][0] if author_meta else ""

    # Build a map from href (without fragment) to spine item content
    item_text_cache = {}
    def _get_text_for_href(href: str) -> str:
        base_href = href.split("#")[0]
        if base_href in item_text_cache:
            return item_text_cache[base_href]
        for item in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
            if item.get_name().endswith(base_href) or base_href.endswith(item.get_name()):
                soup = BeautifulSoup(item.get_content(), "html.parser")
                text = soup.get_text(separator="\n")
                # Clean up excessive whitespace while preserving paragraph breaks
                text = re.sub(r'\n{3,}', '\n\n', text).strip()
                item_text_cache[base_href] = text
                return text
        # Try matching by the filename portion only
        base_name = os.path.basename(base_href)
        for item in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
            if os.path.basename(item.get_name()) == base_name:
                soup = BeautifulSoup(item.get_content(), "html.parser")
                text = soup.get_text(separator="\n")
                text = re.sub(r'\n{3,}', '\n\n', text).strip()
                item_text_cache[base_href] = text
                return text
        return ""

    def _get_html_for_href(href: str) -> str:
        """Get raw HTML content for an href (for notes parsing)."""
        base_href = href.split("#")[0]
        for item in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
            if item.get_name().endswith(base_href) or base_href.endswith(item.get_name()):
                return item.get_content().decode("utf-8", errors="replace")
            base_name = os.path.basename(base_href)
            if os.path.basename(item.get_name()) == base_name:
                return item.get_content().decode("utf-8", errors="replace")
        return ""

    # Collect all TOC entries, flattening sections into their parent chapter
    # Each section (tuple) = a chapter with sub-sections in one HTML file
    # Each plain Link = a standalone item (intro, notes, etc.)
    raw_chapters = []
    notes_href = None

    # Content patterns for filtering
    content_pattern = re.compile(
        r'^(?:(\d+)\.\s+|Chapter\s+(\d+)|Prologue|Epilogue|Introduction|Preface|'
        r'Appendix|Afterword|Critical\s+Guide|Summary)',
        re.IGNORECASE
    )

    for toc_item in book.toc:
        if isinstance(toc_item, tuple):
            # Section: (parent_link, [child_links])
            section_link, children = toc_item
            title = section_link.title
            href = section_link.href
            if content_pattern.match(title):
                # The section itself is a chapter (e.g. "1. Introduction: ...")
                raw_chapters.append({"title": title, "href": href})
            else:
                # Parent doesn't match (e.g. "Part I: ...") — check children
                for child in children:
                    ctitle = child.title
                    chref = child.href
                    if ctitle.strip().lower() == "notes":
                        notes_href = chref
                    elif content_pattern.match(ctitle):
                        raw_chapters.append({"title": ctitle, "href": chref})
        else:
            # Plain link
            title = toc_item.title
            href = toc_item.href
            if title.strip().lower() == "notes":
                notes_href = href
            elif content_pattern.match(title):
                raw_chapters.append({"title": title, "href": href})

    # Extract notes text and parse by chapter
    notes_text = ""
    notes_by_chapter = {}
    if notes_href:
        notes_html = _get_html_for_href(notes_href)
        if notes_html:
            soup = BeautifulSoup(notes_html, "html.parser")
            # Parse notes by chapter headings (h2 tags or similar)
            current_heading = None
            current_notes = []
            for elem in soup.body.children if soup.body else []:
                if hasattr(elem, 'name') and elem.name in ('h1', 'h2', 'h3'):
                    if current_heading and current_notes:
                        notes_by_chapter[current_heading] = "\n".join(current_notes)
                    heading_text = elem.get_text().strip()
                    current_heading = heading_text
                    current_notes = []
                elif hasattr(elem, 'get_text'):
                    text = elem.get_text().strip()
                    if text:
                        current_notes.append(text)
            if current_heading and current_notes:
                notes_by_chapter[current_heading] = "\n".join(current_notes)
            notes_text = _get_text_for_href(notes_href)

    # Build chapters with text and notes
    chapters = []
    running_chars = 0
    CHARS_PER_PAGE = 2000

    for ch in raw_chapters:
        text = _get_text_for_href(ch["href"])
        if not text:
            continue

        title = ch["title"]
        # Determine chapter key and notes key
        m_num = re.match(r'^(\d+)\.\s+', title)
        m_chapter = re.match(r'^Chapter\s+(\d+)', title, re.IGNORECASE)
        if m_num:
            ch_key = m_num.group(1)
        elif m_chapter:
            ch_key = m_chapter.group(1)
        elif title.lower().startswith("prologue"):
            ch_key = "Prologue"
        elif title.lower().startswith("epilogue"):
            ch_key = "Epilogue"
        elif title.lower().startswith("introduction") or title.lower().startswith("preface"):
            ch_key = "Introduction"
        elif title.lower().startswith("appendix"):
            ch_key = "Appendix"
        elif title.lower().startswith("afterword"):
            ch_key = "Afterword"
        elif title.lower().startswith("critical guide"):
            ch_key = "Appendix"
        elif title.lower().startswith("summary"):
            ch_key = "7"
        else:
            ch_key = title.split(".")[0].strip() if "." in title else title[:20]

        # Match notes — try various key formats
        ch_notes = ""
        for nkey, nval in notes_by_chapter.items():
            nkey_lower = nkey.lower().strip().replace("\n", " ")
            # For numeric keys: match "Chapter N" anywhere in heading
            if ch_key.isdigit():
                if f"chapter {ch_key}" in nkey_lower:
                    ch_notes = nval
                    break
            else:
                # For named keys: require heading to START with the key
                # to avoid "Introduction" matching "Chapter 1.Introduction: ..."
                if nkey_lower.startswith(ch_key.lower()):
                    ch_notes = nval
                    break

        # Estimate page numbers from character position
        est_start_page = running_chars // CHARS_PER_PAGE + 1
        running_chars += len(text)
        est_end_page = running_chars // CHARS_PER_PAGE + 1

        chapters.append({
            "number": ch_key,
            "title": title,
            "text": text,
            "start_page": est_start_page,
            "end_page": est_end_page,
            "notes": ch_notes,
        })

    return {
        "title": book_title,
        "author": book_author,
        "chapters": chapters,
        "full_notes": notes_text,
    }


_CURRENT_PDF_PATH = None  # Set by parse_book, used by _build_page_map / _get_text_for_pages


# ── Manual TOCs for PDFs without embedded TOC metadata ──
# Each entry: {"title": str, "author": str, "chapters": [(key, title, pdf_start_0idx, pdf_end_0idx)],
#              "notes_range": (pdf_start_0idx, pdf_end_0idx) or None}
# pdf pages are 0-indexed. end is exclusive.

MANUAL_TOCS = {
    "american-ethnicity.pdf": {
        "title": "American Ethnicity",
        "author": "E. D. Hirsch, Jr.",
        "chapters": [
            ("Preface", "Preface: Literacy and a More Perfect Union", 10, 22),
            ("1", "Chapter 1: The Shanker Principle", 22, 42),
            ("2", "Chapter 2: Developmentalism", 42, 64),
            ("3", "Chapter 3: Developmentalism's Successful Attack on 'Rote Learning'", 64, 72),
            ("4", "Chapter 4: Two Experienced Educators Describe What Works and What Doesn't", 72, 90),
            ("5", "Chapter 5: Conclusion of Part I: The End of School-based Inequality", 90, 102),
            ("6", "Chapter 6: The Most Decisive Educational Experiment in History", 102, 108),
            ("7", "Chapter 7: Ethnicity and Literacy: Six Decades of Research", 108, 134),
            ("8", "Chapter 8: The Nationalizing of Modern Ethnicity", 134, 148),
            ("9", "Chapter 9: Answer to the Learned Despisers of Specificity", 148, 164),
            ("10", "Chapter 10: American Ethnicity Through Grade 8", 164, 166),
            ("Appendix", "Appendix: Miss Peabody Introduces Developmentalism in 1890", 236, 248),
        ],
        "notes_range": (248, 273),
    },
    "shared-knowledge.pdf": {
        "title": "Shared Knowledge",
        "author": "E. D. Hirsch, Jr.",
        "chapters": [
            ("Preface", "Preface: Knowledge-Centered Schooling", 8, 14),
            ("1", "Chapter 1: The Slate and the Seedling, The Mirror and the Lamp", 14, 19),
            ("2", "Chapter 2: The South Bronx Miracle", 19, 24),
            ("3", "Chapter 3: Shared Knowledge is Key to Our Unity and Literacy", 24, 29),
            ("4", "Chapter 4: Democracy Requires a Stable Written Language", 29, 32),
            ("5", "Chapter 5: Biculturalism is a Key to Modern Democracy", 32, 36),
            ("6", "Chapter 6: Low Literacy Leads to Political Extremism", 36, 45),
            ("7", "Chapter 7: Hyper-Individualism Impairs Democracy", 45, 51),
            ("8", "Chapter 8: Shared Knowledge Enables New Learning", 51, 55),
            ("9", "Chapter 9: Wisdom from Two Teachers and Four States", 55, 74),
            ("10", "Chapter 10: 'Robust' Longitudinal Data on Shared-Knowledge Schooling", 74, 84),
            ("11", "Chapter 11: Updating Our Ideas I: Critical Thinking Is Not a General Skill", 84, 89),
            ("12", "Chapter 12: Updating Our Ideas II: Reading is Not a General Skill", 89, 95),
            ("13", "Chapter 13: A Recent Refutation of the 'Readability' Concept", 95, 108),
            ("14", "Chapter 14: Time Marches On: Fostering Our National Renewal", 108, 119),
            ("Appendix", "Appendix: One Example of Educational Success: Early Grades from the Core Knowledge Sequence", 119, 121),
        ],
        "notes_range": (230, 234),
    },
    "philosophy-of-composition.pdf": {
        "title": "The Philosophy of Composition",
        "author": "E. D. Hirsch, Jr.",
        "chapters": [
            ("Preface", "Preface", 13, 16),
            ("Introduction", "Introduction: Distinctive Features of Written Speech", 17, 48),
            ("1", "Chapter 1: The Normative Character of Written Speech", 49, 66),
            ("2", "Chapter 2: Progressive Tendencies in the History of Language and of Prose", 67, 88),
            ("3", "Chapter 3: Refining the Concept of Readability", 89, 106),
            ("4", "Chapter 4: The Psychological Bases of Readability", 107, 154),
            ("5", "Chapter 5: Some Practical Implications", 155, 190),
            ("6", "Chapter 6: The Valid Assessment of Writing Ability", 191, 208),
        ],
        "notes_range": None,  # Footnotes are inline in the scanned text
    },
}


def _parse_manual_toc_pdf(path: str, spec: dict) -> dict:
    """Parse a PDF using a manually-specified TOC.

    spec: entry from MANUAL_TOCS with title, author, chapters, notes_range.
    """
    import pymupdf

    doc = pymupdf.open(path)

    # Extract notes
    notes_text = ""
    notes_by_chapter = {}
    if spec.get("notes_range"):
        n_start, n_end = spec["notes_range"]
        for pg in range(n_start, min(n_end, doc.page_count)):
            notes_text += doc[pg].get_text()

        # Parse notes by chapter heading
        heading_pattern = re.compile(
            r'^(Preface[:\s].*|Chapter\s+\d+[:\s].*|Appendix[:\s].*)$',
            re.MULTILINE
        )
        matches = list(heading_pattern.finditer(notes_text))
        for i, match in enumerate(matches):
            heading = match.group(1).strip()
            start = match.end()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(notes_text)
            section_text = notes_text[start:end].strip()
            # Normalize: "Chapter 1: The Shanker..." → "Chapter 1"
            m_ch = re.match(r'(Chapter\s+\d+)', heading)
            m_pre = re.match(r'(Preface)', heading)
            m_app = re.match(r'(Appendix)', heading)
            if m_ch:
                notes_by_chapter[m_ch.group(1)] = section_text
            elif m_pre:
                notes_by_chapter["Preface"] = section_text
            elif m_app:
                notes_by_chapter["Appendix"] = section_text

    chapters = []
    for key, title, start_pg, end_pg in spec["chapters"]:
        text = ""
        for pg in range(start_pg, min(end_pg, doc.page_count)):
            text += doc[pg].get_text()

        # Map notes
        if key == "Preface":
            notes_key = "Preface"
        elif key == "Appendix":
            notes_key = "Appendix"
        elif key.isdigit():
            notes_key = f"Chapter {key}"
        elif key == "Introduction":
            notes_key = "Introduction"
        else:
            notes_key = key
        ch_notes = notes_by_chapter.get(notes_key, "")

        chapters.append({
            "number": key,
            "title": title,
            "text": text.strip(),
            "start_page": start_pg + 1,  # 1-indexed for consistency
            "end_page": end_pg + 1,
            "notes": ch_notes,
        })

    doc.close()

    log.info("Parsed %d content chapters from %s (manual TOC)", len(chapters), os.path.basename(path))
    for ch in chapters:
        log.info("  [%s] %s — %d chars, %d chars notes",
                 ch["number"], ch["title"], len(ch["text"]), len(ch["notes"]))

    return {
        "title": spec["title"],
        "author": spec["author"],
        "chapters": chapters,
        "full_notes": notes_text,
    }


def _parse_cultural_literacy(path: str) -> dict:
    """Custom parser for the scanned Cultural Literacy PDF (no TOC, OCR artifacts).

    Page boundaries determined empirically from the 280-page Internet Archive scan.
    The appendix list (pages 175-238) is excluded — it's a 5,000-item reference list,
    not argumentative content.
    """
    import pymupdf

    doc = pymupdf.open(path)

    # Chapter boundaries: (key, title, pdf_start_page, pdf_end_page)
    # pdf pages are 0-indexed here, converted to 1-indexed in output
    chapters_spec = [
        ("Preface",  "Preface", 16, 21),
        ("1", "Chapter I: Literacy and Cultural Literacy", 24, 56),
        ("2", "Chapter II: The Discovery of the Schema", 56, 93),
        ("3", "Chapter III: National Language and National Culture", 93, 117),
        ("4", "Chapter IV: American Diversity and Public Discourse", 117, 133),
        ("5", "Chapter V: Cultural Literacy and the Schools", 133, 157),
        ("6", "Chapter VI: The Practical Outlook", 157, 169),
        ("Appendix", "Appendix: What Literate Americans Know (introductory essay)", 169, 175),
    ]

    # Extract notes section (pages 239-261)
    notes_text = ""
    for pg in range(239, min(262, doc.page_count)):
        notes_text += doc[pg].get_text()

    # Parse notes by chapter heading — custom regex for OCR'd text
    # OCR produces headings like "Ex  CHAPTER  I" or "Ex   PREFACE"
    notes_by_chapter = {}
    heading_pattern = re.compile(
        r'(?:^|\n)\s*\S*\s*(PREFACE|CHAPTER\s+[IVX]+|APPENDIX)\s*(?:\n|$)',
        re.IGNORECASE
    )
    matches = list(heading_pattern.finditer(notes_text))
    for i, match in enumerate(matches):
        heading = re.sub(r'\s+', ' ', match.group(1).strip())
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(notes_text)
        section_text = notes_text[start:end].strip()
        # Normalize heading: "CHAPTER  I" -> "Chapter I"
        normalized = heading.title()
        notes_by_chapter[normalized] = section_text

    chapters = []
    for key, title, start_pg, end_pg in chapters_spec:
        text = ""
        for pg in range(start_pg, min(end_pg, doc.page_count)):
            text += doc[pg].get_text()

        # Map notes keys — try both Arabic and Roman numeral forms
        notes_key = None
        if key == "Preface":
            notes_key = "Preface"
        elif key == "Appendix":
            notes_key = "Appendix"
        elif key.isdigit():
            roman = {"1": "I", "2": "II", "3": "III", "4": "IV", "5": "V", "6": "VI"}.get(key, key)
            for candidate in [f"Chapter {roman}", f"Chapter {key}", f"Chapter {roman.lower()}"]:
                if candidate in notes_by_chapter:
                    notes_key = candidate
                    break
            if notes_key is None:
                # Fuzzy match — OCR may mangle headings (e.g., "HI" for "III")
                for nk in notes_by_chapter:
                    if "Chapter" in nk and (roman in nk or key in nk):
                        notes_key = nk
                        break
        ch_notes = notes_by_chapter.get(notes_key, "") if notes_key else ""

        chapters.append({
            "number": key,
            "title": title,
            "text": text.strip(),
            "start_page": start_pg + 1,  # 1-indexed for consistency with TOC-based parser
            "end_page": end_pg + 1,
            "notes": ch_notes,
        })

    doc.close()

    log.info("Parsed %d content chapters from %s (custom Cultural Literacy parser)", len(chapters), os.path.basename(path))
    for ch in chapters:
        log.info("  [%s] %s — %d chars, %d chars notes",
                 ch["number"], ch["title"], len(ch["text"]), len(ch["notes"]))

    return {
        "title": "Cultural Literacy",
        "author": "E. D. Hirsch, Jr.",
        "chapters": chapters,
        "full_notes": notes_text,
    }


def parse_book(path: str) -> dict:
    """Parse a book file (PDF, ePub, or pre-parsed JSON) into a standard dict format.

    For PDFs: uses PyMuPDF with TOC-based chapter detection.
    For ePubs: uses ebooklib with TOC/spine-based extraction.
    For JSON: loads pre-parsed book data directly (e.g., from OCR).
    Returns: {"title", "author", "chapters": [...], "full_notes"}
    """
    if path.endswith('.json'):
        with open(path) as f:
            data = json.load(f)
        log.info("Loaded pre-parsed book from %s: %d chapters", os.path.basename(path), len(data.get("chapters", [])))
        return data

    global _CURRENT_PDF_PATH

    if path.lower().endswith(".epub"):
        result = _parse_epub(path)
        log.info("Parsed %d content chapters from %s (ePub)", len(result["chapters"]), os.path.basename(path))
        for ch in result["chapters"]:
            log.info("  [%s] %s — %d chars, %d chars notes",
                     ch["number"], ch["title"], len(ch["text"]), len(ch["notes"]))
        return result

    import pymupdf

    _CURRENT_PDF_PATH = os.path.abspath(path)

    doc = pymupdf.open(path)
    toc = doc.get_toc()
    # Filter out useless TOC entries (e.g. "Blank Page")
    real_toc = [e for e in toc if e[1].strip().lower() not in ("blank page", "")]
    if not real_toc:
        doc.close()
        # Check for manual TOC first
        basename = os.path.basename(path)
        if basename in MANUAL_TOCS:
            return _parse_manual_toc_pdf(path, MANUAL_TOCS[basename])
        # Check if it's Cultural Literacy by looking at early pages
        doc2 = pymupdf.open(path)
        page6_text = doc2[6].get_text() if doc2.page_count > 6 else ""
        doc2.close()
        if "Cultural" in page6_text and "Literacy" in page6_text and "Hirsch" in page6_text:
            return _parse_cultural_literacy(path)
        return _parse_book_orig(path)

    meta = doc.metadata or {}
    book_title = meta.get("title", "") or os.path.basename(path)
    book_author = meta.get("author", "")

    # Build chapter list from TOC level-1 entries
    l1 = [(title, page) for level, title, page in toc if level == 1]
    raw = []
    for i, (title, start) in enumerate(l1):
        end = l1[i + 1][1] if i + 1 < len(l1) else doc.page_count + 1
        raw.append({"title": title, "start_page": start, "end_page": end})

    for ch in raw:
        text = ""
        for pg in range(ch["start_page"] - 1, ch["end_page"] - 1):
            if pg < doc.page_count:
                text += doc[pg].get_text()
        ch["text"] = text.strip()

    # Extract full notes section
    notes_text = ""
    for ch in raw:
        if ch["title"].strip().lower() == "notes":
            notes_text = ch["text"]
            break
    notes_by_chapter = _parse_notes_sections(notes_text)

    # Content filter — match Prologue, Chapter N, Epilogue, Appendix
    content_pattern = re.compile(
        r'^(Prologue|Chapter\s+\d+|Epilogue|Appendix\s+I{1,3})\b', re.IGNORECASE
    )

    chapters = []
    for ch in raw:
        m = content_pattern.match(ch["title"])
        if not m:
            continue
        # Extract chapter key for notes lookup
        prefix = m.group(1)
        if prefix.lower() == "prologue":
            ch_key = "Prologue"
            notes_key = "Prologue"
        elif prefix.lower() == "epilogue":
            ch_key = "Epilogue"
            notes_key = "Epilogue"
        elif prefix.lower().startswith("chapter"):
            num = re.search(r'\d+', prefix).group()
            ch_key = num
            notes_key = f"Chapter {num}"
        elif prefix.lower().startswith("appendix"):
            ch_key = prefix
            notes_key = prefix
        else:
            ch_key = prefix
            notes_key = prefix

        ch_notes = notes_by_chapter.get(notes_key, "")
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("hirsch-extract")

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# ── Slugs ──

def book_slug(title: str) -> str:
    mapping = {"Why Knowledge Matters": "wkm", "Cultural Literacy": "cl",
               "The Schools We Need": "swn", "The Knowledge Deficit": "kd",
               "The Making of Americans": "the-making-of-americans",
               "How to Educate a Citizen": "how-to-educate-a-citizen",
               "American Ethnicity": "ae", "Shared Knowledge": "sk",
               "The Philosophy of Composition": "poc",
               "The Ratchet Effect": "re"}
    return mapping.get(title, re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')[:30])

def chapter_slug(key: str) -> str:
    return key.lower().replace(" ", "-")

def ensure_dirs(bslug: str, cslug: str) -> str:
    path = os.path.join(DATA_DIR, bslug, cslug)
    os.makedirs(path, exist_ok=True)
    return path

def save_json(data, path: str):
    with open(path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log.info("Saved %s (%d bytes)", os.path.basename(path), os.path.getsize(path))

def load_json(path: str):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


# ═══════════════════════════════════════════════════════════════════
# SCHEMAS
# ═══════════════════════════════════════════════════════════════════

SKELETON_SCHEMA = {
    "type": "object",
    "properties": {
        "book_thesis": {
            "type": "string",
            "description": "The book's central thesis in 1-3 sentences. Must be a proposition, not a topic."
        },
        "core_frameworks": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"},
                    "description": {"type": "string"},
                    "components": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"},
                                "name": {"type": "string"},
                                "description": {"type": "string"}
                            },
                            "required": ["id", "name", "description"]
                        }
                    },
                    "stance": {"type": "string", "enum": ["author_advocates", "author_opposes", "author_critiques_then_revises"]}
                },
                "required": ["id", "name", "description", "components", "stance"]
            }
        },
        "chapter_previews": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "chapter": {"type": "string"},
                    "predicted_main_argument": {"type": "string"},
                    "role_in_book": {"type": "string", "enum": [
                        "establishes_problem", "provides_evidence", "develops_theory",
                        "addresses_objections", "proposes_solution", "synthesizes", "case_study"
                    ]},
                    "expected_evidence_types": {"type": "array", "items": {"type": "string"}},
                    "connects_to": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["chapter", "predicted_main_argument", "role_in_book"]
            }
        },
        "key_terms": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "term": {"type": "string"},
                    "definition": {"type": "string"},
                    "source_passage": {"type": "string"}
                },
                "required": ["term", "definition"]
            }
        },
        "intellectual_genealogy": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "relationship": {"type": "string", "enum": ["builds_on", "opposes", "revises", "historical_predecessor"]},
                    "contribution": {"type": "string"}
                },
                "required": ["name", "relationship", "contribution"]
            }
        }
    },
    "required": ["book_thesis", "core_frameworks", "chapter_previews", "key_terms"]
}

PHASE1_SCHEMA = {
    "type": "object",
    "properties": {
        "chapter_summary": {
            "type": "string",
            "description": "2-3 sentence summary of the chapter's core argument as a connected line of reasoning."
        },
        "claims": {
            "type": "array",
            "description": "ALL claims the author makes — every assertion, not just the big ones. Include main conclusions, intermediate premises, factual assertions, concessions, prescriptive conclusions, historical claims, and normative positions. Aim for 25-40 per chapter. If in doubt, INCLUDE it. Better to over-extract than miss a claim. Even seemingly obvious factual statements ('American scores declined between 1960 and 1980') are claims if the author uses them as premises. Split compound sentences that contain multiple distinct claims.",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "C1, C2, etc."},
                    "text": {"type": "string", "description": "The claim as a clear proposition. NOT 'Hirsch argues X' but 'X'."},
                    "claim_level": {"type": "string", "enum": ["empirical", "theoretical", "normative", "methodological", "pragmatic", "meta"]},
                    "is_main_conclusion": {"type": "boolean", "description": "true if chapter-level conclusion (3-6 per chapter)"},
                    "confidence": {"type": "number", "description": "0.0-1.0 based on author's hedging language"},
                    "source_passage": {"type": "string", "description": "Verbatim quote from the chapter"}
                },
                "required": ["id", "text", "claim_level", "is_main_conclusion", "confidence", "source_passage"]
            }
        },
        "evidence": {
            "type": "array",
            "description": "Specific studies, datasets, examples cited. 5-15 per chapter.",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "E1, E2, etc."},
                    "description": {"type": "string", "description": "What it shows — precisely, not vaguely"},
                    "evidence_type": {"type": "string", "enum": [
                        "empirical_study", "statistical_data", "historical_example",
                        "international_comparison", "natural_experiment", "anecdote",
                        "expert_testimony", "thought_experiment"
                    ]},
                    "source_reference": {"type": "string", "description": "Author(s), title, year as cited"},
                    "endnote_numbers": {"type": "array", "items": {"type": "integer"}},
                    "source_passage": {"type": "string"},
                    "supports_claim": {"type": "string", "description": "Claim ID this most directly supports"}
                },
                "required": ["id", "description", "evidence_type", "supports_claim"]
            }
        },
        "concepts": {
            "type": "array",
            "description": "Key concepts, contested terms, and ideas the author defines, uses, or argues about. These are not just vocabulary — they are intellectual positions. Include the author's definition AND how it differs from common usage or opposing definitions. 3-10 per chapter.",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "CON1, CON2, etc."},
                    "term": {"type": "string"},
                    "author_definition": {"type": "string", "description": "How the author defines or uses this term"},
                    "common_alternative": {"type": "string", "description": "How others might define it differently, if relevant"},
                    "source_passage": {"type": "string"},
                    "importance": {"type": "string", "enum": ["central", "supporting", "contextual"],
                                   "description": "central=load-bearing for the argument, supporting=used but not pivotal, contextual=background"}
                },
                "required": ["id", "term", "author_definition", "importance"]
            }
        },
        "cases": {
            "type": "array",
            "description": "Specific countries, schools, implementations, or historical episodes discussed as evidence or examples. Capture the author's FULL account — every passage where they discuss this case. The narrative is valuable and may later be compared against other accounts.",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "CASE1, CASE2, etc."},
                    "name": {"type": "string", "description": "Short name: 'France pre-1989', 'US silver age 1940s-50s'"},
                    "description": {"type": "string", "description": "Full narrative: what happened, according to the author. Be thorough — include the mechanisms, the timeline, the outcomes. Multiple sentences."},
                    "key_passages": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Verbatim quotes — every significant passage where the author discusses this case. These are the raw material for later cross-referencing."
                    },
                    "time_period": {"type": "string"},
                    "role_in_argument": {"type": "string", "enum": ["positive_example", "negative_example", "natural_experiment", "historical_precedent", "comparison"]},
                    "claims_supported": {"type": "array", "items": {"type": "string"}, "description": "Which claim IDs this case supports"},
                    "contestable_aspects": {"type": "array", "items": {"type": "string"}, "description": "Which parts of the author's account might be described differently by other scholars"}
                },
                "required": ["id", "name", "description", "key_passages", "role_in_argument"]
            }
        },
        "thinkers": {
            "type": "array",
            "description": "Intellectual figures whose IDEAS (not just names) are engaged with. Capture the author's full characterization of their ideas — this may later be compared against the thinker's actual writings.",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "TH1, TH2, etc."},
                    "name": {"type": "string"},
                    "key_idea": {"type": "string", "description": "The specific idea as the author characterizes it"},
                    "source_work": {"type": "string", "description": "The thinker's work cited, if any"},
                    "author_stance": {"type": "string", "enum": ["agrees", "disagrees", "partially_agrees", "builds_on", "historicizes"]},
                    "engagement": {"type": "string", "description": "How the author uses or responds to this thinker's idea — be thorough"},
                    "key_passages": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Verbatim quotes where the author discusses this thinker's ideas"
                    }
                },
                "required": ["id", "name", "key_idea", "author_stance", "engagement", "key_passages"]
            }
        },
        "actors": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "role": {"type": "string", "enum": ["cited_approvingly", "cited_critically", "argued_against", "historical_figure", "institution"]},
                    "context": {"type": "string"}
                },
                "required": ["name", "role", "context"]
            }
        },
        "objections_raised": {
            "type": "array",
            "description": "Objections the AUTHOR explicitly addresses in this chapter.",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "OBJ1, OBJ2"},
                    "objection": {"type": "string"},
                    "source": {"type": "string", "description": "Who raises it (if attributed)"},
                    "response": {"type": "string"},
                    "targets_claim": {"type": "string"}
                },
                "required": ["id", "objection", "response", "targets_claim"]
            }
        },
        "cross_chapter_refs": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "target_chapter": {"type": "string"},
                    "reference_text": {"type": "string"}
                },
                "required": ["target_chapter", "reference_text"]
            }
        }
    },
    "required": ["chapter_summary", "claims", "evidence", "concepts", "cases", "thinkers", "actors", "objections_raised"]
}

PHASE2_SCHEMA = {
    "type": "object",
    "properties": {
        "dependencies": {
            "type": "array",
            "description": "Logical relationships between claims. Map the full chain from premises to conclusions.",
            "items": {
                "type": "object",
                "properties": {
                    "from_id": {"type": "string"},
                    "to_id": {"type": "string"},
                    "relationship": {"type": "string", "enum": [
                        "depends-on", "supports", "opposes", "undermines",
                        "instantiates", "refines", "objects-to", "responds-to"
                    ]},
                    "explanation": {"type": "string"}
                },
                "required": ["from_id", "to_id", "relationship", "explanation"]
            }
        },
        "warrants": {
            "type": "array",
            "description": "For each evidence→claim link: the unstated principle that makes the evidence support the claim.",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "W1, W2, etc."},
                    "evidence_id": {"type": "string"},
                    "claim_id": {"type": "string"},
                    "warrant_text": {"type": "string", "description": "The general principle connecting evidence to claim"},
                    "is_explicit": {"type": "boolean", "description": "true if author states this warrant explicitly"},
                    "vulnerability": {"type": "string", "description": "How could a critic attack this warrant?"}
                },
                "required": ["id", "evidence_id", "claim_id", "warrant_text", "is_explicit", "vulnerability"]
            }
        },
        "missing_steps": {
            "type": "array",
            "description": "Inference gaps where the author jumps from A to C without B.",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "MS1, MS2, etc."},
                    "from_id": {"type": "string"},
                    "to_id": {"type": "string"},
                    "missing_step": {"type": "string", "description": "The intermediate claim needed"},
                    "step_type": {"type": "string", "enum": [
                        "empirical_assumption", "causal_mechanism", "normative_premise",
                        "generalization", "definition_shift", "scope_expansion"
                    ]},
                    "severity": {"type": "string", "enum": ["critical", "significant", "minor"]}
                },
                "required": ["id", "from_id", "to_id", "missing_step", "step_type", "severity"]
            }
        },
        "counter_arguments": {
            "type": "array",
            "description": "Strongest objections a reasonable critic would raise that the author does NOT address.",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "CA1, CA2, etc."},
                    "targets_claim": {"type": "string"},
                    "objection": {"type": "string", "description": "The strongest version, stated charitably"},
                    "objection_type": {"type": "string", "enum": [
                        "empirical_challenge", "alternative_explanation", "scope_limitation",
                        "value_disagreement", "methodological_concern", "internal_inconsistency"
                    ]}
                },
                "required": ["id", "targets_claim", "objection", "objection_type"]
            }
        },
        "argument_chains": {
            "type": "array",
            "description": "The 2-4 main reasoning flows in this chapter, each as an ordered sequence of claim IDs.",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "chain": {"type": "array", "items": {"type": "string"}, "description": "Ordered claim IDs from premise to conclusion"},
                    "conclusion_id": {"type": "string"},
                    "strength": {"type": "string", "enum": ["strong", "moderate", "weak"]}
                },
                "required": ["name", "chain", "conclusion_id", "strength"]
            }
        }
    },
    "required": ["dependencies", "warrants", "missing_steps", "counter_arguments", "argument_chains"]
}

PHASE3_SCHEMA = {
    "type": "object",
    "properties": {
        "missed_arguments": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "description": {"type": "string"},
                    "source_passage": {"type": "string"},
                    "importance": {"type": "string", "enum": ["critical", "significant", "minor"]},
                    "suggested_claim": {"type": "string"},
                    "suggested_claim_level": {"type": "string", "enum": ["empirical", "theoretical", "normative", "methodological", "pragmatic", "meta"]}
                },
                "required": ["description", "source_passage", "importance", "suggested_claim"]
            }
        },
        "distorted_claims": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "claim_id": {"type": "string"},
                    "problem": {"type": "string"},
                    "source_passage": {"type": "string"},
                    "suggested_fix": {"type": "string"}
                },
                "required": ["claim_id", "problem", "suggested_fix"]
            }
        },
        "missing_warrants": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "evidence_id": {"type": "string"},
                    "claim_id": {"type": "string"},
                    "missing_warrant": {"type": "string"}
                },
                "required": ["evidence_id", "claim_id", "missing_warrant"]
            }
        },
        "missing_counter_arguments": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "targets_claim": {"type": "string"},
                    "counter_argument": {"type": "string"},
                    "why_important": {"type": "string"}
                },
                "required": ["targets_claim", "counter_argument", "why_important"]
            }
        },
        "granularity_issues": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "claim_ids": {"type": "array", "items": {"type": "string"}},
                    "action": {"type": "string", "enum": ["split", "merge"]},
                    "explanation": {"type": "string"}
                },
                "required": ["claim_ids", "action", "explanation"]
            }
        },
        "level_corrections": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "claim_id": {"type": "string"},
                    "current_level": {"type": "string"},
                    "correct_level": {"type": "string"},
                    "explanation": {"type": "string"}
                },
                "required": ["claim_id", "current_level", "correct_level"]
            }
        },
        "overall_assessment": {
            "type": "object",
            "properties": {
                "content_completeness": {"type": "integer", "description": "1-5"},
                "structure_completeness": {"type": "integer", "description": "1-5"},
                "warrant_coverage": {"type": "integer", "description": "1-5"},
                "counter_argument_coverage": {"type": "integer", "description": "1-5"},
                "summary": {"type": "string"}
            },
            "required": ["content_completeness", "structure_completeness", "warrant_coverage", "counter_argument_coverage", "summary"]
        }
    },
    "required": ["missed_arguments", "distorted_claims", "missing_warrants", "missing_counter_arguments", "granularity_issues", "overall_assessment"]
}


# ═══════════════════════════════════════════════════════════════════
# PROMPTS
# ═══════════════════════════════════════════════════════════════════

SKELETON_SYSTEM = """You are extracting the ARGUMENT SKELETON of a non-fiction book from its introduction/prologue.

Do NOT extract claims or evidence. Extract the STRUCTURE: what the author promises to argue, what frameworks they introduce, how chapters connect, and what intellectual tradition they position themselves within.

Read the prologue as a ROADMAP for the book's argument, not as content to mine for claims."""

PHASE1_SYSTEM = """You are extracting the CONTENT of a book chapter — what the author claims and what evidence they cite.

YOUR #1 GOAL IS COMPLETENESS. It is better to extract too many claims than too few. Every assertion the author makes — even seemingly obvious factual statements — should be captured if the author uses it as part of an argument. A typical chapter should yield 25-40 claims. If you find fewer than 25, you are under-extracting — go back and look for factual premises, concessions, prescriptive conclusions, and compound claims that need splitting.

CRITICAL RULES:
1. Every claim must be a PROPOSITION — something that could be true or false. NOT "Hirsch discusses testing" but "Current standardized reading tests are educationally invalid because they measure background knowledge rather than a transferable reading skill."
2. Include both main CONCLUSIONS (3-6 per chapter) and intermediate PREMISES. ALSO include: factual assertions used as premises ("American scores declined 1960-1980"), historical characterizations ("France Americanized its school system overnight in 1989"), normative positions ("knowledge parity should be understood as a civil right"), and theoretical claims ("educational individualism has always required the general-skills idea").
3. CONCEPTS are first-class objects. When the author defines a term, uses a term with specific meaning, or argues about what a concept means, extract it as a concept with the author's definition. Contested concepts (like "developmental appropriateness," "communal knowledge," "general skills") are especially important.
4. CASES are first-class objects. When the author discusses a specific country, school, or historical episode as evidence, extract it as a case with its time period and role.
5. THINKERS are first-class objects. When the author engages with another thinker's IDEAS (not just mentions their name), extract the specific idea and how the author responds to it.
6. claim_level matters:
   - empirical: backed by data or observation ("French test scores declined after 1989")
   - theoretical: about how things work ("Skills are domain-specific")
   - normative: about what should be ("Schools should teach shared knowledge")
   - methodological: about how to study/measure ("These tests are consequentially invalid")
   - pragmatic: about what to do in practice ("Curriculum should be 50% shared, 50% local")
   - meta: about the argument itself ("This book updates Cultural Literacy with new evidence")
4. Source passages must be VERBATIM quotes from the text.
5. Capture the author's own objection-response pairs (OBJ items) — where they raise and rebut a counter-argument.
6. Do NOT map relationships between claims. That is a separate phase.
7. confidence: 0.9+ only for claims the author states as established fact with multiple citations. 0.7-0.8 for single-study claims. 0.5-0.7 for logical arguments. 0.3-0.5 for speculative claims.
9. DO NOT judge importance during extraction. Extract every assertion. Importance will be computed later from the argument graph. Over-extract.
10. Factual premises that seem "obvious" are still claims: "American test scores declined between 1960-1980" is a claim. Extract them.
11. PRESCRIPTIVE/ACTIONABLE claims are separate from DESCRIPTIVE claims. If the author says "The gap is a knowledge gap" AND "Knowledge-based schooling can ameliorate it," these are TWO claims — one descriptive, one prescriptive. Always extract both halves.
12. The author's CHARACTERIZATIONS of events are claims. When Hirsch describes the French reform as "Americanization" or says French teacher schools were "indoctrinated" with progressive ideas for 20 years — these are interpretive assertions, not neutral descriptions. Another scholar might describe the same events very differently. Extract characterizations as claims with claim_level=empirical or =historical.
13. Be SPECIFIC, not general. Don't extract "ideas control educational outcomes" when the author says "three specific ideas — naturalism, individualism, and skill-centrism — caused the American educational decline." Name the specific things.
14. MECHANISM claims matter. "20 years of teacher indoctrination in French education schools prepared the ground for the 1989 reform" is a causal mechanism claim. Extract it.
15. When the author states a THESIS — a major claim that the entire book or chapter will argue for — capture it verbatim. Hirsch's thesis "only by systematically imparting to all children the knowledge that is commonly possessed by successful citizens can all children gain the possibility of success" is a central claim, not just context.

FEW-SHOT EXAMPLES — learn from these real cases:

EXAMPLE 1 (compound claim — split into two):
Source text: "Since a valid reading test probes a student's degree of initiation into the public sphere—a fundamental aim of education—any policy that lowers or neglects to improve test scores in reading is a failed educational policy."
CORRECT extraction: TWO separate claims:
  C_a: "A valid reading test probes a student's degree of initiation into the public sphere, which is a fundamental aim of education." (claim_level: methodological)
  C_b: "Any educational policy that lowers or neglects to improve reading test scores is a failed educational policy." (claim_level: normative)
WRONG: One merged claim. The first half links reading tests to civic purpose; the second half is a normative policy judgment. They are logically independent — someone could accept one and reject the other.

EXAMPLE 2 (concession is a SEPARATE factual claim):
Source text: "the testing regimens have clearly helped improve the mechanics of early reading" ... "Younger students can now decode texts with more fluency and accuracy than they did before NCLB."
CORRECT extraction: TWO claims:
  C_a: "The testing regimens introduced by NCLB have clearly helped improve the mechanics of early reading." (claim_level: empirical, is a concession)
  C_b: "Younger students can now decode texts with more fluency and accuracy than they did before NCLB." (claim_level: empirical, separate factual detail)
WRONG: Merging both into one claim about NCLB improving early reading. The concession (C_a) and the specific factual observation (C_b) are distinct — C_b adds specific detail (fluency AND accuracy) that C_a does not contain. Each factual assertion deserves its own claim.

EXAMPLE 3 (generalization from cases must be its own claim):
When the author cites France pre-1989, Japan, Finland, and Korea as examples of nations achieving high and equitable outcomes via communal knowledge curricula, extract the GENERAL PRINCIPLE as a separate claim: "Whole nations have successfully achieved high and equitable educational outcomes by following communal knowledge curricula delivered through national curricula." The individual countries are evidence/cases, but the generalization across them is a claim that could be true or false independently.
WRONG: Extracting each country as a case but not extracting the overarching generalization as its own claim.

EXAMPLE 4 (be specific — name ALL items in a list):
Source text: the author attributes the American educational decline to "three specific ideas — naturalism, individualism, and skill-centrism."
CORRECT: "Three specific ideas — naturalism, individualism, and skill-centrism — caused the American educational decline." (all three named)
WRONG: "Individualistic, child-centered ideas caused the decline" — this collapses three distinct ideas into a vague description and drops "skill-centrism" entirely. The author deliberately enumerates three; your extraction must preserve all three.

EXAMPLE 5 (concept attack must include the "lacks basis" claim):
When the author critiques a concept like "developmental appropriateness," extract BOTH the concept definition AND the author's attack on its validity as a separate claim. If Hirsch says developmental appropriateness "lacks scientific basis and has been used to justify withholding academic content from young children," the "lacks scientific basis" part is itself a claim (empirical/methodological) — not just context for the concept definition."""

PHASE2_SYSTEM = """You are mapping the ARGUMENT STRUCTURE of a book chapter. You are given a complete inventory of claims and evidence from a previous extraction pass.

Your job is NOT to find new claims. It is to map how existing claims relate to each other.

For EVERY claim marked is_main_conclusion=true, work through these steps:
1. What other claims must be true for this conclusion to follow? (dependencies with depends-on)
2. What evidence supports it, and what is the WARRANT — the unstated principle that makes the evidence relevant? A warrant answers: "WHY does this evidence count as support?"
3. Are there inference GAPS — places where the author jumps from A to C without establishing B?
4. What is the STRONGEST objection a reasonable, informed critic would raise that the author does NOT address?

WARRANT EXAMPLES:
- Evidence: "France changed curriculum in 1989 and scores declined for 20 years."
  Claim: "The curriculum change caused the decline."
  WARRANT: "In a natural experiment where only one major variable changed (curriculum), and the timing aligns, the changed variable is the most likely cause." (Natural experiment assumption)
  VULNERABILITY: "Other variables may have changed too — teacher training, demographics, economic conditions."

- Evidence: "Cognitive science shows skills are domain-specific."
  Claim: "Teaching general critical thinking is futile."
  WARRANT: "If skills cannot transfer across domains, then practicing domain-independent skills cannot produce domain-specific competence." (Logical deduction)
  VULNERABILITY: "There may be partially-transferable meta-cognitive skills (self-monitoring, planning) even if content skills don't transfer."

MISSING STEP EXAMPLES:
- From "France declined after curriculum change" to "US should adopt national curriculum":
  MISSING: "What worked in France's centralized system would also work in the US federated system." (scope_expansion, severity: significant)

COUNTER-ARGUMENT EXAMPLES:
- Claim: "The French decline proves skills-based education fails."
  COUNTER: "France's implementation may have been poor — the theory could be right even if this execution failed." (alternative_explanation)

Be systematic. Process every main conclusion. Do not skip any."""

PHASE3_SYSTEM = """You are a critical reviewer of argument extractions. You have the original chapter text and a structured extraction (claims, evidence, structural relationships, warrants, counter-arguments).

Your job is to find what is WRONG or MISSING. You are specifically looking for:

1. MISSED ARGUMENTS: Read the chapter text carefully. Is there an argument in any paragraph that has no corresponding claim? Especially look for:
   - Intermediate reasoning steps (the "because" and "therefore" moves)
   - The author's logical framework (how ideas relate as a SYSTEM, not just individually)
   - Normative claims hiding inside empirical arguments
   - The "so what" — implications the author draws but that weren't extracted

2. DISTORTED CLAIMS: Does any extracted claim misrepresent what the author actually wrote? Check source passages.

3. MISSING WARRANTS: For each main evidence→claim link, is there a warrant? If not, what should it be?

4. MISSING COUNTER-ARGUMENTS: For each main conclusion, what is the strongest objection NOT identified?

5. GRANULARITY: Are any claims really two claims conflated? Are any two claims really the same thing?

6. LEVEL CORRECTIONS: Is any claim's claim_level wrong? (e.g., a normative claim labeled empirical)

Be concrete. Cite claim IDs and verbatim passages."""


# ═══════════════════════════════════════════════════════════════════
# EXTRACTION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════

def extract_skeleton(book_data: dict, model: str = "gemini3-flash") -> dict:
    """Phase 0: Extract book-level argument skeleton from prologue + TOC."""
    prologue = None
    for ch in book_data["chapters"]:
        if ch["number"].lower() in ("prologue", "introduction"):
            prologue = ch
            break
    if not prologue:
        prologue = book_data["chapters"][0]

    toc_lines = [f"- {ch['number']}: {ch['title']}" for ch in book_data["chapters"]]
    toc_text = "\n".join(toc_lines)

    endnotes = parse_endnotes(prologue.get("notes", ""))
    endnotes_text = "\n".join(f"[{e['number']}] {e['text']}" for e in endnotes) if endnotes else "(no endnotes)"

    prompt = f"""Book: {book_data['title']} by {book_data['author']}

TABLE OF CONTENTS:
{toc_text}

PROLOGUE TEXT:
{prologue['text']}

PROLOGUE ENDNOTES:
{endnotes_text}

Extract the book's argument skeleton: thesis, frameworks, chapter previews, key terms, and intellectual genealogy."""

    log.info("Extracting skeleton for '%s'...", book_data["title"])
    result, meta = generate_sync(prompt, SKELETON_SYSTEM, SKELETON_SCHEMA, model=model, max_tokens=8192)
    log.info("Skeleton: %d frameworks, %d chapter previews, %d terms — $%.4f in %.1fs",
             len(result.get("core_frameworks", [])), len(result.get("chapter_previews", [])),
             len(result.get("key_terms", [])), meta["total_cost_usd"], meta["duration_s"])
    return {"result": result, "metadata": meta}


COMPLETENESS_SWEEP_SYSTEM = """You are reviewing an extraction of claims from a book chapter. The first pass captured many claims but may have missed some.

Your job is to find ADDITIONAL claims that were missed. Work through each of these 5 targeted detection questions systematically:

1. **PRESUPPOSITIONS**: What factual statements does the author take for granted as premises? These are often phrased as background ("math scores were stable") but are actually claims. Check each paragraph: what does the author ASSUME is true without arguing for it?

2. **CONCESSIONS**: Where does the author grant a point to the other side before arguing it is insufficient? Phrases like "certainly", "to be sure", "admittedly", "it is true that", "no doubt" signal concessions. Each concession is a separate claim the author accepts.

3. **PRESCRIPTIVE CONCLUSIONS**: What does the author say SHOULD be done? Look for "must", "should", "the only way", "need to", "ought to". Prescriptive claims are separate from the descriptive claims that support them.

4. **COMPOUND CLAIMS**: Which sentences in the text contain TWO or more distinct propositions? Especially sentences with dashes, semicolons, or "since...therefore" structures. If a sentence makes two separable assertions, extract each one individually.

5. **SPECIFIC VOCABULARY**: Does the author use distinctive philosophical terms (e.g., "consequential validity", "initiation into the public sphere", "communal knowledge") that should be preserved verbatim in the extraction rather than paraphrased? If the first pass paraphrased a term the author coined or uses precisely, add a concept entry or a claim using the exact wording.

For each detection question, compare what you find against the ALREADY EXTRACTED list. Return ONLY the additional claims/concepts that are genuinely MISSING. Do not re-extract things already captured."""

COMPLETENESS_SWEEP_SCHEMA = {
    "type": "object",
    "properties": {
        "additional_claims": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "NEW1, NEW2, etc."},
                    "text": {"type": "string"},
                    "claim_level": {"type": "string", "enum": ["empirical", "theoretical", "normative", "methodological", "pragmatic", "meta"]},
                    "is_main_conclusion": {"type": "boolean"},
                    "confidence": {"type": "number"},
                    "source_passage": {"type": "string"},
                    "page_range": {"type": "string"},
                    "why_missed": {"type": "string", "description": "Why was this missed? (compound_claim, factual_premise, concession, specific_framing, generalization, before_after)"}
                },
                "required": ["id", "text", "claim_level", "is_main_conclusion", "confidence", "source_passage", "why_missed"]
            }
        },
        "additional_concepts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "term": {"type": "string"},
                    "author_definition": {"type": "string"},
                    "common_alternative": {"type": "string"},
                    "source_passage": {"type": "string"},
                    "importance": {"type": "string", "enum": ["central", "supporting", "contextual"]}
                },
                "required": ["id", "term", "author_definition", "importance"]
            }
        }
    },
    "required": ["additional_claims"]
}


def _build_page_map(chapter: dict) -> list:
    """Build a list of (char_offset, page_number) from PDF page data.

    Uses pymupdf to get per-page text lengths, mapping character offsets
    to book page numbers.
    """
    try:
        import pymupdf
        pdf_path = _CURRENT_PDF_PATH
        if not pdf_path:
            return []
        doc = pymupdf.open(pdf_path)
        running = 0
        page_map = []
        for pg in range(chapter["start_page"] - 1, chapter["end_page"] - 1):
            if pg < doc.page_count:
                page_map.append((running, pg + 1))  # (char_offset, pdf_page)
                running += len(doc[pg].get_text())
        doc.close()
        return page_map
    except Exception:
        return []


def _char_offset_to_page(offset: int, page_map: list) -> int:
    """Convert a character offset to a page number using the page map."""
    if not page_map:
        return 0
    for i in range(len(page_map) - 1, -1, -1):
        if offset >= page_map[i][0]:
            return page_map[i][1]
    return page_map[0][1] if page_map else 0


def _split_into_windows(text: str, window_size: int = 6000, overlap: int = 1000) -> list:
    """Split text into overlapping windows, breaking at paragraph boundaries.

    Returns list of (window_text, char_start_offset) tuples.
    """
    if len(text) <= window_size:
        return [(text, 0)]

    windows = []
    start = 0
    while start < len(text):
        end = start + window_size
        if end >= len(text):
            windows.append((text[start:], start))
            break
        # Try to break at a paragraph boundary (double newline) near the end
        search_start = max(start + window_size - 500, start)
        para_break = text.rfind("\n\n", search_start, end + 500)
        if para_break > search_start:
            end = para_break + 2
        windows.append((text[start:end], start))
        start = end - overlap
    return windows


def _word_set(text: str) -> set:
    """Extract lowercase word set from text for similarity comparison."""
    return set(re.findall(r'[a-z]+', text.lower()))


def _dedup_claims(claims: list, threshold: float = 0.60) -> list:
    """Deduplicate claims where >threshold fraction of words overlap. Keep the longer one."""
    if not claims:
        return claims
    kept = []
    for claim in claims:
        words = _word_set(claim.get("text", ""))
        is_dup = False
        for i, existing in enumerate(kept):
            existing_words = _word_set(existing.get("text", ""))
            if not words or not existing_words:
                continue
            intersection = words & existing_words
            smaller = min(len(words), len(existing_words))
            if smaller == 0:
                continue
            overlap_ratio = len(intersection) / smaller
            if overlap_ratio > threshold:
                if len(claim.get("text", "")) > len(existing.get("text", "")):
                    kept[i] = claim
                is_dup = True
                break
        if not is_dup:
            kept.append(claim)
    return kept


def _dedup_by_field(items: list, field: str, threshold: float = 0.60) -> list:
    """Deduplicate items by a text field using word overlap."""
    if not items:
        return items
    kept = []
    for item in items:
        text = item.get(field, "")
        words = _word_set(text)
        is_dup = False
        for existing in kept:
            existing_words = _word_set(existing.get(field, ""))
            if not words or not existing_words:
                continue
            smaller = min(len(words), len(existing_words))
            if smaller == 0:
                continue
            overlap_ratio = len(words & existing_words) / smaller
            if overlap_ratio > threshold:
                is_dup = True
                break
        if not is_dup:
            kept.append(item)
    return kept


def _renumber_ids(result: dict) -> dict:
    """Re-number all IDs sequentially after dedup/merge."""
    for i, c in enumerate(result.get("claims", []), 1):
        c["id"] = f"C{i}"
    for i, e in enumerate(result.get("evidence", []), 1):
        e["id"] = f"E{i}"
    for i, c in enumerate(result.get("concepts", []), 1):
        c["id"] = f"CON{i}"
    for i, c in enumerate(result.get("cases", []), 1):
        c["id"] = f"CASE{i}"
    for i, t in enumerate(result.get("thinkers", []), 1):
        t["id"] = f"TH{i}"
    for i, o in enumerate(result.get("objections_raised", []), 1):
        o["id"] = f"OBJ{i}"
    return result


def extract_phase1(chapter: dict, skeleton: dict, model: str = "gemini3-flash") -> dict:
    """Phase 1: Content extraction via sliding window + completeness sweep.

    Split chapter into overlapping windows, extract from each, merge,
    deduplicate, then run completeness sweep.
    """
    endnotes = parse_endnotes(chapter.get("notes", ""))
    endnotes_text = "\n".join(f"[{e['number']}] {e['text']}" for e in endnotes) if endnotes else "(no endnotes)"

    skeleton_summary = f"Book thesis: {skeleton.get('book_thesis', 'N/A')}\n"
    for fw in skeleton.get("core_frameworks", []):
        skeleton_summary += f"Framework {fw['id']}: {fw['name']} ({fw['stance']}) — {fw['description']}\n"
        for c in fw.get("components", []):
            skeleton_summary += f"  {c['id']}: {c['name']} — {c['description']}\n"

    # Find chapter preview from skeleton
    ch_preview = ""
    for cp in skeleton.get("chapter_previews", []):
        if chapter["number"].lower() in cp["chapter"].lower() or chapter["title"].lower() in cp["chapter"].lower():
            ch_preview = f"\nThis chapter's role: {cp.get('role_in_book', 'unknown')}. Expected argument: {cp.get('predicted_main_argument', 'N/A')}"
            break

    # Build page map for this chapter
    page_map = _build_page_map(chapter)

    # Split chapter text into overlapping windows
    windows = _split_into_windows(chapter["text"], window_size=6000, overlap=1000)
    log.info("Phase 1 sliding window: %d windows from %d chars for '%s'",
             len(windows), len(chapter["text"]), chapter["title"])

    # Extract from each window
    all_results = []
    total_cost = 0.0
    total_time = 0.0

    for wi, (window_text, char_offset) in enumerate(windows):
        # Compute page range for this window
        page_start = _char_offset_to_page(char_offset, page_map)
        page_end = _char_offset_to_page(char_offset + len(window_text), page_map)
        page_hint = f" (approximately pages {page_start}-{page_end})" if page_map else ""
        window_prompt = f"""BOOK CONTEXT:
{skeleton_summary}{ch_preview}

CHAPTER: {chapter['number']} — {chapter['title']}

SECTION TEXT (part {wi + 1} of {len(windows)} from this chapter){page_hint}:
{window_text}

ENDNOTES FOR THIS CHAPTER:
{endnotes_text}

Extract all claims, evidence, definitions, actors, and objection-response pairs from this section of the chapter. Aim for 5-15 claims per section. Focus on WHAT the author says — do not map relationships between claims."""

        log.info("Phase 1 window %d/%d (%d chars, pages ~%s-%s)...",
                 wi + 1, len(windows), len(window_text),
                 page_start if page_map else "?", page_end if page_map else "?")
        w_result, w_meta = generate_sync(window_prompt, PHASE1_SYSTEM, PHASE1_SCHEMA, model=model, max_tokens=16384)
        n_w_claims = len(w_result.get("claims", []))

        # Stamp page_range on claims that don't have one
        if page_map:
            for c in w_result.get("claims", []):
                if not c.get("page_range"):
                    c["page_range"] = f"{page_start}-{page_end}"

        log.info("  Window %d: %d claims — $%.4f in %.1fs",
                 wi + 1, n_w_claims, w_meta["total_cost_usd"], w_meta["duration_s"])
        all_results.append(w_result)
        total_cost += w_meta["total_cost_usd"]
        total_time += w_meta["duration_s"]

    # Merge all window results
    result = {
        "chapter_summary": all_results[0].get("chapter_summary", "") if all_results else "",
        "claims": [],
        "evidence": [],
        "concepts": [],
        "cases": [],
        "thinkers": [],
        "actors": [],
        "objections_raised": [],
        "cross_chapter_refs": [],
    }
    for wr in all_results:
        result["claims"].extend(wr.get("claims", []))
        result["evidence"].extend(wr.get("evidence", []))
        result["concepts"].extend(wr.get("concepts", []))
        result["cases"].extend(wr.get("cases", []))
        result["thinkers"].extend(wr.get("thinkers", []))
        result["actors"].extend(wr.get("actors", []))
        result["objections_raised"].extend(wr.get("objections_raised", []))
        result["cross_chapter_refs"].extend(wr.get("cross_chapter_refs", []))

    pre_dedup_claims = len(result["claims"])

    # Deduplicate
    result["claims"] = _dedup_claims(result["claims"])
    result["evidence"] = _dedup_by_field(result["evidence"], "description")
    result["concepts"] = _dedup_by_field(result["concepts"], "term", threshold=0.80)
    result["cases"] = _dedup_by_field(result["cases"], "name", threshold=0.80)
    result["thinkers"] = _dedup_by_field(result["thinkers"], "name", threshold=0.80)
    result["objections_raised"] = _dedup_by_field(result["objections_raised"], "objection")

    # Re-number IDs sequentially
    _renumber_ids(result)

    n_claims_after_dedup = len(result["claims"])
    log.info("Phase 1 pass 1 (sliding window): %d claims before dedup, %d after — $%.4f in %.1fs",
             pre_dedup_claims, n_claims_after_dedup, total_cost, total_time)

    # Pass 2: completeness sweep
    existing_claims = "\n".join(f"  {c['id']}: {c['text']}" for c in result.get("claims", []))
    existing_concepts = "\n".join(f"  {c['id']}: {c['term']}" for c in result.get("concepts", []))
    existing_cases = "\n".join(f"  {c['id']}: {c['name']}" for c in result.get("cases", []))

    sweep_prompt = f"""CHAPTER: {chapter['number']} — {chapter['title']}

CHAPTER TEXT:
{chapter['text']}

ALREADY EXTRACTED CLAIMS:
{existing_claims}

ALREADY EXTRACTED CONCEPTS:
{existing_concepts}

ALREADY EXTRACTED CASES:
{existing_cases}

Find additional claims and concepts that were MISSED by the first extraction pass. Read the chapter text carefully and identify assertions not captured above."""

    log.info("Phase 1 pass 2 (completeness sweep)...")
    sweep_result, sweep_meta = generate_sync(
        sweep_prompt, COMPLETENESS_SWEEP_SYSTEM, COMPLETENESS_SWEEP_SCHEMA,
        model=model, max_tokens=8192
    )
    total_cost += sweep_meta["total_cost_usd"]
    total_time += sweep_meta["duration_s"]

    # Merge sweep results into main result
    new_claims = sweep_result.get("additional_claims", [])
    new_concepts = sweep_result.get("additional_concepts", [])

    if new_claims:
        max_id = max(int(c["id"].lstrip("C")) for c in result["claims"] if c["id"].startswith("C"))
        for i, nc in enumerate(new_claims):
            nc["id"] = f"C{max_id + i + 1}"
            nc.pop("why_missed", None)
        result["claims"].extend(new_claims)

    if new_concepts:
        max_cid = 0
        for c in result.get("concepts", []):
            m = re.search(r'\d+', c["id"])
            if m:
                max_cid = max(max_cid, int(m.group()))
        for i, nc in enumerate(new_concepts):
            nc["id"] = f"CON{max_cid + i + 1}"
        result.setdefault("concepts", []).extend(new_concepts)

    n_total = len(result.get("claims", []))
    n_main = sum(1 for c in result.get("claims", []) if c.get("is_main_conclusion"))
    n_ev = len(result.get("evidence", []))
    n_obj = len(result.get("objections_raised", []))
    log.info("Phase 1 total: %d claims (%d main, +%d sweep, %d deduped), %d evidence, %d objections — $%.4f in %.1fs",
             n_total, n_main, len(new_claims), pre_dedup_claims - n_claims_after_dedup,
             n_ev, n_obj, total_cost, total_time)

    # Combine metadata
    combined_meta = {
        "total_cost_usd": total_cost,
        "duration_s": total_time,
        "num_windows": len(windows),
        "pre_dedup_claims": pre_dedup_claims,
        "post_dedup_claims": n_claims_after_dedup,
        "sweep_claims": len(new_claims),
        "sweep_concepts": len(new_concepts),
    }
    return {"result": result, "metadata": combined_meta}


def format_phase1_for_phase2(phase1: dict) -> str:
    """Format Phase 1 output as a structured inventory for Phase 2's prompt."""
    lines = ["CLAIMS INVENTORY:"]
    for c in phase1.get("claims", []):
        marker = " [MAIN CONCLUSION]" if c.get("is_main_conclusion") else ""
        lines.append(f"  {c['id']} ({c.get('claim_level', '?')}){marker}: {c['text']}")

    lines.append("\nEVIDENCE INVENTORY:")
    for e in phase1.get("evidence", []):
        lines.append(f"  {e['id']} ({e.get('evidence_type', '?')}) supports {e.get('supports_claim', '?')}: {e['description']}")
        if e.get("source_reference"):
            lines.append(f"    Source: {e['source_reference']}")

    lines.append("\nCONCEPTS:")
    for c in phase1.get("concepts", []):
        lines.append(f"  {c['id']} ({c.get('importance', '?')}): {c['term']} — {c['author_definition']}")
        if c.get("common_alternative"):
            lines.append(f"    Alternative def: {c['common_alternative']}")

    lines.append("\nCASES:")
    for c in phase1.get("cases", []):
        lines.append(f"  {c['id']} ({c.get('role_in_argument', '?')}): {c['name']} — {c['description'][:100]}")

    lines.append("\nTHINKERS:")
    for t in phase1.get("thinkers", []):
        lines.append(f"  {t['id']} ({t.get('author_stance', '?')}): {t['name']} — {t['key_idea'][:100]}")

    lines.append("\nOBJECTIONS THE AUTHOR ADDRESSES:")
    for o in phase1.get("objections_raised", []):
        lines.append(f"  {o['id']} targets {o.get('targets_claim', '?')}: {o['objection']}")
        lines.append(f"    Author's response: {o['response']}")

    return "\n".join(lines)


def _group_claims_by_page(claims: list) -> list:
    """Group claims into batches by page range for windowed Phase 2 processing.

    Returns list of (batch_claims, page_start, page_end) tuples, each with ~15-25 claims.
    """
    # Sort claims by page_range (parse first page number)
    def page_sort_key(c):
        pr = c.get("page_range", "0")
        try:
            return int(pr.split("-")[0])
        except (ValueError, IndexError):
            return 0

    sorted_claims = sorted(claims, key=page_sort_key)

    # Split into batches of ~20 claims
    BATCH_SIZE = 20
    batches = []
    for i in range(0, len(sorted_claims), BATCH_SIZE):
        batch = sorted_claims[i:i + BATCH_SIZE]
        pages = [page_sort_key(c) for c in batch if page_sort_key(c) > 0]
        p_start = min(pages) if pages else 0
        p_end = max(pages) if pages else 0
        batches.append((batch, p_start, p_end))
    return batches


def _get_text_for_pages(chapter: dict, page_start: int, page_end: int) -> str:
    """Extract chapter text corresponding to a page range."""
    try:
        import pymupdf
        pdf_path = _CURRENT_PDF_PATH
        if not pdf_path:
            raise FileNotFoundError("No PDF path set")
        doc = pymupdf.open(pdf_path)
        text_parts = []
        for pg in range(max(page_start - 1, chapter["start_page"] - 1),
                        min(page_end, chapter["end_page"] - 1)):
            if pg < doc.page_count:
                text_parts.append(doc[pg].get_text())
        doc.close()
        return "\n".join(text_parts)
    except Exception:
        # Fallback: return proportional slice of chapter text
        total_pages = chapter["end_page"] - chapter["start_page"]
        if total_pages <= 0:
            return chapter["text"][:8000]
        frac_start = max(0, (page_start - chapter["start_page"]) / total_pages)
        frac_end = min(1, (page_end - chapter["start_page"] + 1) / total_pages)
        char_start = int(frac_start * len(chapter["text"]))
        char_end = int(frac_end * len(chapter["text"]))
        return chapter["text"][char_start:char_end]


def extract_phase2(chapter: dict, skeleton: dict, phase1: dict,
                   previous_chapters: list = None, model: str = "gemini3-flash") -> dict:
    """Phase 2: Structure extraction via windowed batches.

    Groups claims by page range, processes ~20 claims at a time with relevant
    chapter text, then merges all structural results.
    """
    prev_context = ""
    if previous_chapters:
        prev_lines = ["\nPREVIOUS CHAPTERS' MAIN CONCLUSIONS:"]
        for prev in previous_chapters:
            for c in prev.get("claims", []):
                if c.get("is_main_conclusion"):
                    prev_lines.append(f"  {prev.get('_chapter_key', '?')}.{c['id']}: {c['text']}")
        prev_context = "\n".join(prev_lines)

    skeleton_summary = f"Book thesis: {skeleton.get('book_thesis', 'N/A')}"

    # Group claims into page-based batches
    all_claims = phase1.get("claims", [])
    batches = _group_claims_by_page(all_claims)

    log.info("Phase 2 (structure) for '%s' — %d claims in %d batches...",
             chapter["title"], len(all_claims), len(batches))

    # Merged results
    merged = {
        "dependencies": [],
        "warrants": [],
        "missing_steps": [],
        "counter_arguments": [],
        "argument_chains": [],
    }
    total_cost = 0.0
    total_time = 0.0

    for bi, (batch_claims, p_start, p_end) in enumerate(batches):
        # Build inventory for this batch, marking which are main conclusions
        batch_lines = ["CLAIMS IN THIS SECTION:"]
        for c in batch_claims:
            marker = " [MAIN CONCLUSION]" if c.get("is_main_conclusion") else ""
            batch_lines.append(f"  {c['id']} ({c.get('claim_level', '?')}){marker}: {c['text']}")

        # Include evidence that supports claims in this batch
        batch_claim_ids = {c["id"] for c in batch_claims}
        batch_lines.append("\nRELEVANT EVIDENCE:")
        for e in phase1.get("evidence", []):
            if e.get("supports_claim") in batch_claim_ids:
                batch_lines.append(f"  {e['id']} ({e.get('evidence_type', '?')}) supports {e['supports_claim']}: {e['description']}")

        batch_inventory = "\n".join(batch_lines)

        # Get relevant chapter text for this page range
        if p_start > 0 and p_end > 0:
            section_text = _get_text_for_pages(chapter, p_start, p_end)
        else:
            # No page info — use proportional slice
            frac = bi / max(len(batches), 1)
            start_char = int(frac * len(chapter["text"]))
            end_char = min(start_char + 8000, len(chapter["text"]))
            section_text = chapter["text"][start_char:end_char]

        n_main = sum(1 for c in batch_claims if c.get("is_main_conclusion"))

        prompt = f"""{skeleton_summary}

CHAPTER: {chapter['number']} — {chapter['title']}
SECTION: pages {p_start}-{p_end} (batch {bi+1} of {len(batches)}, {len(batch_claims)} claims, {n_main} main conclusions)

{batch_inventory}
{prev_context}

CHAPTER TEXT FOR THIS SECTION:
{section_text}

Map the argument structure for these claims: dependencies, warrants connecting evidence to claims, inference gaps, and counter-arguments. Process EVERY claim marked [MAIN CONCLUSION]."""

        log.info("  Phase 2 batch %d/%d (pages %s-%s, %d claims, %d main)...",
                 bi + 1, len(batches), p_start, p_end, len(batch_claims), n_main)
        try:
            result, meta = generate_sync(prompt, PHASE2_SYSTEM, PHASE2_SCHEMA, model=model, max_tokens=8192)
            merged["dependencies"].extend(result.get("dependencies", []))
            merged["warrants"].extend(result.get("warrants", []))
            merged["missing_steps"].extend(result.get("missing_steps", []))
            merged["counter_arguments"].extend(result.get("counter_arguments", []))
            merged["argument_chains"].extend(result.get("argument_chains", []))
            total_cost += meta.get("total_cost_usd", 0)
            total_time += meta.get("duration_s", 0)
            log.info("    %d deps, %d warrants, %d missing, %d counter — $%.4f",
                     len(result.get("dependencies", [])), len(result.get("warrants", [])),
                     len(result.get("missing_steps", [])), len(result.get("counter_arguments", [])),
                     meta.get("total_cost_usd", 0))
        except Exception as e:
            log.warning("  Phase 2 batch %d/%d failed: %s", bi + 1, len(batches), e)

    log.info("Phase 2 total: %d dependencies, %d warrants, %d missing steps, %d counter-args, %d chains — $%.4f in %.1fs",
             len(merged["dependencies"]), len(merged["warrants"]),
             len(merged["missing_steps"]), len(merged["counter_arguments"]),
             len(merged["argument_chains"]), total_cost, total_time)
    return {"result": merged, "metadata": {"total_cost_usd": total_cost, "duration_s": total_time}}


def format_for_phase3(phase1: dict, phase2: dict) -> str:
    """Format combined Phase 1 + Phase 2 for the critique prompt."""
    lines = [format_phase1_for_phase2(phase1)]

    lines.append("\n\nSTRUCTURAL ANALYSIS:")
    lines.append("\nDependencies:")
    for d in phase2.get("dependencies", []):
        lines.append(f"  {d['from_id']} --{d['relationship']}--> {d['to_id']}: {d.get('explanation', '')}")

    lines.append("\nWarrants:")
    for w in phase2.get("warrants", []):
        lines.append(f"  {w['id']}: {w['evidence_id']} → {w['claim_id']}: {w['warrant_text']}")
        lines.append(f"    Vulnerability: {w.get('vulnerability', 'N/A')}")

    lines.append("\nMissing Steps Identified:")
    for ms in phase2.get("missing_steps", []):
        lines.append(f"  {ms['id']} ({ms.get('severity', '?')}): {ms['from_id']} → {ms['to_id']}: {ms['missing_step']}")

    lines.append("\nCounter-Arguments Identified:")
    for ca in phase2.get("counter_arguments", []):
        lines.append(f"  {ca['id']} targets {ca['targets_claim']}: {ca['objection']}")

    lines.append("\nArgument Chains:")
    for ac in phase2.get("argument_chains", []):
        lines.append(f"  {ac['name']} ({ac.get('strength', '?')}): {' → '.join(ac.get('chain', []))} → {ac['conclusion_id']}")

    return "\n".join(lines)


def extract_phase3(chapter: dict, phase1: dict, phase2: dict, model: str = "gemini3-flash") -> dict:
    """Phase 3: Self-critique — find what's missing or wrong."""
    extraction_summary = format_for_phase3(phase1, phase2)

    prompt = f"""CHAPTER: {chapter['number']} — {chapter['title']}

FULL CHAPTER TEXT:
{chapter['text']}

EXTRACTION TO REVIEW:
{extraction_summary}

Review this extraction against the chapter text. Find missed arguments, distorted claims, missing warrants, missing counter-arguments, granularity issues, and claim level errors. Be thorough and specific."""

    log.info("Phase 3 (critique) for '%s'...", chapter["title"])
    result, meta = generate_sync(prompt, PHASE3_SYSTEM, PHASE3_SCHEMA, model=model, max_tokens=8192)
    assessment = result.get("overall_assessment", {})
    log.info("Phase 3: content=%d/5, structure=%d/5, warrants=%d/5, counter-args=%d/5 — $%.4f in %.1fs",
             assessment.get("content_completeness", 0), assessment.get("structure_completeness", 0),
             assessment.get("warrant_coverage", 0), assessment.get("counter_argument_coverage", 0),
             meta["total_cost_usd"], meta["duration_s"])
    if assessment.get("summary"):
        log.info("Phase 3 summary: %s", assessment["summary"])
    return {"result": result, "metadata": meta}


def merge_phases(phase1: dict, phase2: dict, phase3: dict) -> dict:
    """Combine all three phases into a single merged extraction."""
    merged = {
        "chapter_summary": phase1.get("chapter_summary", ""),
        "claims": list(phase1.get("claims", [])),
        "evidence": phase1.get("evidence", []),
        "concepts": phase1.get("concepts", []),
        "cases": phase1.get("cases", []),
        "thinkers": phase1.get("thinkers", []),
        "actors": phase1.get("actors", []),
        "objections_raised": phase1.get("objections_raised", []),
        "cross_chapter_refs": phase1.get("cross_chapter_refs", []),
        # Phase 2
        "dependencies": phase2.get("dependencies", []),
        "warrants": phase2.get("warrants", []),
        "missing_steps": phase2.get("missing_steps", []),
        "counter_arguments": phase2.get("counter_arguments", []),
        "argument_chains": phase2.get("argument_chains", []),
        # Phase 3
        "critique": phase3.get("overall_assessment", {}),
        "critique_issues": {
            "missed_arguments": phase3.get("missed_arguments", []),
            "distorted_claims": phase3.get("distorted_claims", []),
            "missing_warrants": phase3.get("missing_warrants", []),
            "missing_counter_arguments": phase3.get("missing_counter_arguments", []),
            "granularity_issues": phase3.get("granularity_issues", []),
            "level_corrections": phase3.get("level_corrections", []),
        }
    }

    # Apply level corrections from Phase 3
    corrections = {lc["claim_id"]: lc["correct_level"] for lc in phase3.get("level_corrections", [])}
    for claim in merged["claims"]:
        if claim["id"] in corrections:
            claim["claim_level"] = corrections[claim["id"]]

    return merged


# ═══════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════

def run_skeleton(args):
    book_data = parse_book(args.path)
    bslug = book_slug(book_data["title"])
    os.makedirs(os.path.join(DATA_DIR, bslug), exist_ok=True)

    out = extract_skeleton(book_data, model=args.model)
    save_json(out, os.path.join(DATA_DIR, bslug, "skeleton.json"))


def run_chapter(args):
    book_data = parse_book(args.path)
    bslug = book_slug(book_data["title"])

    # Find chapter
    chapter = None
    for ch in book_data["chapters"]:
        if ch["number"].lower() == args.chapter.lower():
            chapter = ch
            break
    if not chapter:
        log.error("Chapter '%s' not found. Available: %s", args.chapter,
                  [ch["number"] for ch in book_data["chapters"]])
        return

    cslug = chapter_slug(chapter["number"])
    out_dir = ensure_dirs(bslug, cslug)

    # Load skeleton
    skeleton_path = os.path.join(DATA_DIR, bslug, "skeleton.json")
    skeleton_data = load_json(skeleton_path)
    if not skeleton_data:
        log.error("No skeleton found at %s. Run 'skeleton' first.", skeleton_path)
        return
    skeleton = skeleton_data["result"]

    total_cost = 0.0

    # Phase 1
    p1_path = os.path.join(out_dir, "phase1_content.json")
    if not args.force and os.path.exists(p1_path):
        log.info("Phase 1 already exists, loading from disk. Use --force to re-run.")
        p1_data = load_json(p1_path)
    else:
        p1_data = extract_phase1(chapter, skeleton, model=args.model)
        save_json(p1_data, p1_path)
    total_cost += p1_data.get("metadata", {}).get("total_cost_usd", 0)
    phase1 = p1_data["result"]

    # Collect previous chapters for cross-chapter context
    previous = []
    for ch in book_data["chapters"]:
        if ch["number"] == chapter["number"]:
            break
        prev_path = os.path.join(DATA_DIR, bslug, chapter_slug(ch["number"]), "phase1_content.json")
        prev_data = load_json(prev_path)
        if prev_data:
            prev_result = prev_data["result"]
            prev_result["_chapter_key"] = ch["number"]
            previous.append(prev_result)

    # Phase 2
    p2_path = os.path.join(out_dir, "phase2_structure.json")
    phase2_model = args.phase2_model or args.model
    if not args.force and os.path.exists(p2_path):
        log.info("Phase 2 already exists, loading from disk.")
        p2_data = load_json(p2_path)
    else:
        p2_data = extract_phase2(chapter, skeleton, phase1, previous, model=phase2_model)
        save_json(p2_data, p2_path)
    total_cost += p2_data.get("metadata", {}).get("total_cost_usd", 0)
    phase2 = p2_data["result"]

    # Phase 3
    p3_path = os.path.join(out_dir, "phase3_critique.json")
    if args.skip_phase3:
        log.info("Skipping Phase 3 (self-critique).")
        phase3 = {"overall_assessment": {"content_completeness": 0, "structure_completeness": 0,
                                          "warrant_coverage": 0, "counter_argument_coverage": 0,
                                          "summary": "Phase 3 skipped"}}
    elif not args.force and os.path.exists(p3_path):
        log.info("Phase 3 already exists, loading from disk.")
        p3_data = load_json(p3_path)
        phase3 = p3_data["result"]
        total_cost += p3_data.get("metadata", {}).get("total_cost_usd", 0)
    else:
        p3_data = extract_phase3(chapter, phase1, phase2, model=args.model)
        save_json(p3_data, p3_path)
        phase3 = p3_data["result"]
        total_cost += p3_data.get("metadata", {}).get("total_cost_usd", 0)

    # Merge
    merged = merge_phases(phase1, phase2, phase3)
    save_json(merged, os.path.join(out_dir, "merged.json"))

    # Summary
    log.info("═══ CHAPTER COMPLETE: %s ═══", chapter["title"])
    log.info("Claims: %d (%d main conclusions)", len(merged["claims"]),
             sum(1 for c in merged["claims"] if c.get("is_main_conclusion")))
    log.info("Evidence: %d items", len(merged["evidence"]))
    log.info("Dependencies: %d, Warrants: %d, Missing steps: %d",
             len(merged["dependencies"]), len(merged["warrants"]), len(merged["missing_steps"]))
    log.info("Counter-arguments: %d (author-addressed: %d, critic-generated: %d)",
             len(merged["objections_raised"]) + len(merged["counter_arguments"]),
             len(merged["objections_raised"]), len(merged["counter_arguments"]))
    log.info("Argument chains: %d", len(merged["argument_chains"]))
    if merged.get("critique", {}).get("summary"):
        log.info("Self-critique: %s", merged["critique"]["summary"])
    log.info("Total cost: $%.4f", total_cost)


def run_phase(args):
    """Re-run a single phase for a chapter."""
    book_data = parse_book(args.path)
    bslug = book_slug(book_data["title"])

    chapter = None
    for ch in book_data["chapters"]:
        if ch["number"].lower() == args.chapter.lower():
            chapter = ch
            break
    if not chapter:
        log.error("Chapter '%s' not found.", args.chapter)
        return

    cslug = chapter_slug(chapter["number"])
    out_dir = ensure_dirs(bslug, cslug)
    skeleton = load_json(os.path.join(DATA_DIR, bslug, "skeleton.json"))
    if not skeleton:
        log.error("No skeleton found. Run 'skeleton' first.")
        return
    skeleton = skeleton["result"]

    if args.phase_name == "phase1":
        p1 = extract_phase1(chapter, skeleton, model=args.model)
        save_json(p1, os.path.join(out_dir, "phase1_content.json"))

    elif args.phase_name == "phase2":
        p1_data = load_json(os.path.join(out_dir, "phase1_content.json"))
        if not p1_data:
            log.error("Phase 1 not found. Run phase1 first.")
            return
        phase2_model = args.phase2_model or args.model
        p2 = extract_phase2(chapter, skeleton, p1_data["result"], model=phase2_model)
        save_json(p2, os.path.join(out_dir, "phase2_structure.json"))

    elif args.phase_name == "phase3":
        p1_data = load_json(os.path.join(out_dir, "phase1_content.json"))
        p2_data = load_json(os.path.join(out_dir, "phase2_structure.json"))
        if not p1_data or not p2_data:
            log.error("Phase 1 and 2 required for Phase 3.")
            return
        p3 = extract_phase3(chapter, p1_data["result"], p2_data["result"], model=args.model)
        save_json(p3, os.path.join(out_dir, "phase3_critique.json"))

    elif args.phase_name == "merge":
        p1 = load_json(os.path.join(out_dir, "phase1_content.json"))
        p2 = load_json(os.path.join(out_dir, "phase2_structure.json"))
        p3 = load_json(os.path.join(out_dir, "phase3_critique.json"))
        if not all([p1, p2, p3]):
            log.error("All three phases required for merge.")
            return
        merged = merge_phases(p1["result"], p2["result"], p3["result"])
        save_json(merged, os.path.join(out_dir, "merged.json"))


def run_all(args):
    """Extract all chapters."""
    book_data = parse_book(args.path)
    bslug = book_slug(book_data["title"])

    # Skeleton first
    skeleton_path = os.path.join(DATA_DIR, bslug, "skeleton.json")
    os.makedirs(os.path.join(DATA_DIR, bslug), exist_ok=True)
    if not os.path.exists(skeleton_path):
        out = extract_skeleton(book_data, model=args.model)
        save_json(out, skeleton_path)

    # Then each chapter
    for ch in book_data["chapters"]:
        args.chapter = ch["number"]
        args.force = False
        args.skip_phase3 = False
        run_chapter(args)


def main():
    parser = argparse.ArgumentParser(description="Hirsch Argument Atlas — extraction pipeline")
    parser.add_argument("--model", default="gemini3-flash", help="LLM model for extraction")
    parser.add_argument("--phase2-model", default=None, help="Override model for Phase 2 (structure)")

    sub = parser.add_subparsers(dest="command", required=True)

    p_sk = sub.add_parser("skeleton", help="Extract book-level argument skeleton")
    p_sk.add_argument("path")

    p_ch = sub.add_parser("chapter", help="Extract one chapter (all three phases)")
    p_ch.add_argument("path")
    p_ch.add_argument("--chapter", required=True)
    p_ch.add_argument("--force", action="store_true", help="Re-run even if output exists")
    p_ch.add_argument("--skip-phase3", action="store_true")

    p_all = sub.add_parser("all", help="Extract all chapters")
    p_all.add_argument("path")

    p_ph = sub.add_parser("phase", help="Re-run a single phase")
    p_ph.add_argument("phase_name", choices=["phase1", "phase2", "phase3", "merge"])
    p_ph.add_argument("path")
    p_ph.add_argument("--chapter", required=True)

    args = parser.parse_args()

    if args.command == "skeleton":
        run_skeleton(args)
    elif args.command == "chapter":
        run_chapter(args)
    elif args.command == "all":
        run_all(args)
    elif args.command == "phase":
        run_phase(args)


if __name__ == "__main__":
    main()
