# J-STAGE collector (`jstage.py`)

A small, stdlib-only helper for collecting article metadata and PDFs from
[J-STAGE](https://www.jstage.jst.go.jp/) for the **Journal of Equine Science**
(journal code `jes`). Built for a research data-collection loop.

No third-party dependencies — only the Python standard library
(`urllib`, `html.parser`, `re`, `json`, `argparse`, `time`, `pathlib`).

## J-STAGE URL structure

| Resource        | URL                                                                                |
| --------------- | ---------------------------------------------------------------------------------- |
| Volume contents | `https://www.jstage.jst.go.jp/browse/jes/<VOL>/<ISSUE>/_contents/-char/en`         |
| Article detail  | `https://www.jstage.jst.go.jp/article/jes/<VOL>/<ISSUE>/<DOCID>/_article/-char/en` |
| Article PDF     | `https://www.jstage.jst.go.jp/article/jes/<VOL>/<ISSUE>/<DOCID>/_pdf/-char/en`     |

`<ISSUE>` `0` is the whole-volume aggregate (lists every article in the volume).
`<DOCID>` looks like `37_2509`.

## CLI usage

```sh
# Print every article in volume 37 (all issues) as a JSON list.
python3 scripts/journals/jstage.py contents --vol 37

# A specific issue.
python3 scripts/journals/jstage.py contents --vol 37 --issue 1

# Download one article PDF (polite UA, 3-attempt retry, skip-if-exists).
python3 scripts/journals/jstage.py download \
  --pdf-url "https://www.jstage.jst.go.jp/article/jes/37/1/37_2511/_pdf/-char/en" \
  --dest ./pdfs/37_2511.pdf

# Disable the default 1s pre-request politeness delay.
python3 scripts/journals/jstage.py --delay 0 contents --vol 37
```

Each `contents` entry is a JSON object:

```json
{
  "doc_id": "37_2509",
  "vol": 37,
  "issue": 1,
  "title": "...",
  "authors": ["First AUTHOR", "Second AUTHOR"],
  "article_type": "Full Paper",
  "pages": "21-26",
  "release_date": "March 14, 2026",
  "article_url": "https://www.jstage.jst.go.jp/article/jes/37/1/37_2509/_article/-char/en",
  "pdf_url": "https://www.jstage.jst.go.jp/article/jes/37/1/37_2509/_pdf/-char/en"
}
```

## Library usage

```python
import jstage

# Parse a contents page you already have in memory.
articles = jstage.parse_contents(html)

# Fetch + parse (HTTP layer is injectable for testing).
articles = jstage.fetch_contents(vol=37, issue=0)

# Download a PDF (retries with exponential backoff, skips if dest exists).
jstage.download_pdf(articles[0]["pdf_url"], "out/paper.pdf")
```

### Injectable HTTP for testing

`fetch_contents(..., fetch=...)` and `download_pdf(..., fetch=...)` accept a
callable so the network can be mocked in unit tests:

```python
articles = jstage.fetch_contents(37, fetch=lambda url: saved_html)
jstage.download_pdf(url, dest, fetch=lambda url: b"%PDF-...")
```

## Politeness

- A realistic research User-Agent is sent on every request.
- The CLI sleeps `--delay` seconds (default `1.0`) before each request.
- `download_pdf` retries up to 3 times with exponential backoff and skips the
  download entirely if the destination file already exists.

## Development

```sh
# Tests (run from this directory; -o addopts="" bypasses the scripts-level
# pc_keiba_auto_update coverage gate, which does not cover this new module).
cd scripts/journals
uv run --with pytest --with pytest-cov python -m pytest test_jstage.py -q \
  -o addopts="" --cov=jstage --cov-report=term-missing

# Lint + type checks (run from scripts/ so the repo configs apply).
cd scripts
uvx ruff check journals/jstage.py journals/test_jstage.py
uvx basedpyright journals/jstage.py journals/test_jstage.py
uvx ty check journals/jstage.py journals/test_jstage.py
```
