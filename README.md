# Hirsch Knowledge Atlas

An argument extraction and cross-book analysis of E.D. Hirsch Jr.'s complete works (1977–2024). Uses LLMs to extract claims, evidence, and warrants from 10 books, then consolidates them into a navigable knowledge atlas showing how arguments evolve across 47 years.

## What's here

- **10,312 claims** extracted from 10 books across 102 chapters
- **656 cross-book clusters** showing argument evolution (repeated, refined, evolved, broadened, narrowed)
- **256 multi-book entities** (thinkers, concepts, case studies)
- **122-page static website** — ready to browse, no server needed
- **Complete extraction pipeline** — reusable for any thinker's body of work

## Quick start

```bash
# Browse the generated atlas
cd site && python3 -m http.server 8080
# → open http://localhost:8080

# Or run extraction on a new book (requires Gemini API key)
pip install limbic trafilatura ebooklib beautifulsoup4
export GEMINI_API_KEY=your-key
python extract.py --book books/your-book/ --title "Book Title"
```

## Pipeline

1. **Extract** (`extract.py`) — Two-phase LLM extraction: content pass (claims, evidence, terms) → structure pass (dependencies, warrants, objections). Self-critique to catch missed inferences.
2. **Consolidate** (`consolidate.py`) — Embed all claims with limbic, find similar claims across books via pairwise cosine + semantic whitening, classify evolution relationships.
3. **Build site** (`build_corpus_site.py`) — Generate 122 static HTML pages with argument graphs, evolution timelines, and entity pages.

## Dependencies

- [limbic](https://github.com/houshuang/limbic) — embeddings, search, similarity, LLM integration
- Gemini Flash API — extraction and classification (~$0 with free tier)
- Standard Python: trafilatura, ebooklib, BeautifulSoup4

## Cost

The entire corpus was extracted for approximately $0 using Gemini Flash free tier, plus ~12 hours of calibration and iteration time.

## Books analyzed

| Year | Title |
|------|-------|
| 1977 | The Philosophy of Composition |
| 1987 | Cultural Literacy |
| 1988 | The Dictionary of Cultural Literacy |
| 1996 | The Schools We Need |
| 2006 | The Knowledge Deficit |
| 2009 | The Making of Americans |
| 2016 | Why Knowledge Matters |
| 2020 | The Knowledge Gap (Natalie Wexler, foreword by Hirsch) |
| 2022 | How to Educate a Citizen |
| 2024 | Teaching Common Knowledge |

## License

Code: MIT. Book content analysis constitutes fair use for research purposes.
