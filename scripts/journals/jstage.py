"""Collect article metadata and PDFs from J-STAGE.

Targets the "Journal of Equine Science" (journal code ``jes``) but the parsing
and fetching logic is generic across J-STAGE-hosted journals that share the
same server-rendered contents DOM.

Stdlib only (``urllib``, ``html.parser``, ``re``, ``json``, ``argparse``,
``time``, ``pathlib``). The HTTP layer is injectable so the parser and the
fetch/download helpers can be unit-tested without touching the network.

URL structure (verified against the live site)::

    contents : https://www.jstage.jst.go.jp/browse/jes/<VOL>/<ISSUE>/_contents/-char/en
    article  : https://www.jstage.jst.go.jp/article/jes/<VOL>/<ISSUE>/<DOCID>/_article/-char/en
    pdf      : https://www.jstage.jst.go.jp/article/jes/<VOL>/<ISSUE>/<DOCID>/_pdf/-char/en

``<ISSUE>`` 0 is the whole-volume aggregate.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from types import TracebackType
from typing import Protocol, TypedDict, override

JOURNAL_CODE = "jes"
BASE_URL = "https://www.jstage.jst.go.jp"
USER_AGENT = "Mozilla/5.0 (compatible; equine-research-bot/1.0; +https://www.jstage.jst.go.jp)"
DEFAULT_DELAY_SECONDS = 1.0
DEFAULT_TIMEOUT_SECONDS = 30.0

# DOCID embedded in an article/pdf path, e.g. ".../jes/37/1/37_2509/_article/...".
_ARTICLE_PATH_RE = re.compile(
    r"/article/" + re.escape(JOURNAL_CODE) + r"/(\d+)/(\d+)/([^/]+)/_(article|pdf)/",
)


class Article(TypedDict):
    """One parsed article entry from a contents page."""

    doc_id: str
    vol: int
    issue: int
    title: str
    authors: list[str]
    article_type: str
    pages: str
    release_date: str
    article_url: str
    pdf_url: str


FetchText = Callable[[str], str]
FetchBytes = Callable[[str], bytes]


class _HttpHeaders(Protocol):
    def get_content_charset(self) -> str | None: ...


class _HttpResponse(Protocol):
    """Minimal view of the object returned by ``opener.open``."""

    headers: _HttpHeaders

    def read(self) -> bytes: ...

    def __enter__(self) -> _HttpResponse: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...


def _open(url: str, timeout: float) -> _HttpResponse:
    """Open an https(s) URL with the research User-Agent.

    Rejects non-http(s) schemes up front so the opener is never handed an
    unexpected ``file:``/``ftp:`` URL.
    """
    if not url.startswith(("http://", "https://")):
        message = f"refusing to fetch non-http(s) URL: {url!r}"
        raise ValueError(message)
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    opener = urllib.request.build_opener(
        urllib.request.HTTPHandler,
        urllib.request.HTTPSHandler,
    )
    return opener.open(request, timeout=timeout)


def default_fetch_text(url: str, *, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> str:
    """Fetch ``url`` and decode the body as text (UTF-8, replacement on error)."""
    with _open(url, timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


def default_fetch_bytes(url: str, *, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> bytes:
    """Fetch ``url`` and return the raw response body."""
    with _open(url, timeout) as resp:
        return resp.read()


def contents_url(vol: int, issue: int = 0) -> str:
    """Build the contents URL for a volume/issue."""
    return f"{BASE_URL}/browse/{JOURNAL_CODE}/{vol}/{issue}/_contents/-char/en"


class _ContentsParser(HTMLParser):
    """Extract article entries from a J-STAGE volume/issue contents page.

    The contents page renders each article as an ``<li>`` inside
    ``<ul class="search-resultslisting">`` with these inner blocks:

    - ``div.searchlist-title > a[href=..._article...]`` — canonical title + URL
    - ``div.searchlist-authortags[title=...]`` — full comma-separated author list
    - ``div.searchlist-additional-info`` — article type, pages, release date
    - ``div.lft > span > a[href=..._pdf...]`` — PDF download link
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.articles: list[Article] = []
        self._current: dict[str, str] | None = None
        self._capture: str | None = None
        self._buffer: list[str] = []

    @staticmethod
    def _class_of(attrs: list[tuple[str, str | None]]) -> str:
        for name, value in attrs:
            if name == "class" and value is not None:
                return value
        return ""

    @staticmethod
    def _attr(attrs: list[tuple[str, str | None]], key: str) -> str | None:
        for name, value in attrs:
            if name == key:
                return value
        return None

    @override
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        cls = self._class_of(attrs)
        classes = cls.split()
        if tag == "li" and self._capture is None:
            self._current = {}
            return
        if self._current is None:
            return
        if tag == "div" and "searchlist-title" in classes:
            self._capture = "title-div"
            return
        if tag == "a" and self._capture == "title-div":
            href = self._attr(attrs, "href")
            self._record_article_link(href)
            self._begin_text("title")
            return
        if tag == "div" and "searchlist-authortags" in classes:
            self._current["authors_title"] = self._attr(attrs, "title") or ""
            return
        if tag == "div" and "searchlist-additional-info" in classes:
            self._begin_text("info")
            return
        if tag == "a" and self._capture is None:
            self._maybe_record_pdf_link(self._attr(attrs, "href"))

    def _record_article_link(self, href: str | None) -> None:
        if href is None or self._current is None:
            return
        match = _ARTICLE_PATH_RE.search(href)
        if match is None or match.group(4) != "article":
            return
        self._current["article_url"] = href
        # Groups 1-3 are \d+/\d+/docid, kept as strings until assembly.
        self._current["vol"] = match.group(1)
        self._current["issue"] = match.group(2)
        self._current["doc_id"] = match.group(3)

    def _maybe_record_pdf_link(self, href: str | None) -> None:
        if href is None or self._current is None:
            return
        match = _ARTICLE_PATH_RE.search(href)
        if match is not None and match.group(4) == "pdf":
            self._current["pdf_url"] = href

    def _begin_text(self, key: str) -> None:
        self._capture = key
        self._buffer = []

    @override
    def handle_data(self, data: str) -> None:
        if self._capture in {"title", "info"}:
            self._buffer.append(data)

    @override
    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._capture == "title" and self._current is not None:
            self._current["title"] = " ".join("".join(self._buffer).split())
            self._capture = "title-div"
            return
        if tag == "div" and self._capture == "title-div":
            self._capture = None
            return
        if tag == "div" and self._capture == "info" and self._current is not None:
            self._current["info_text"] = "".join(self._buffer)
            self._capture = None
            return
        if tag == "li" and self._capture is None and self._current is not None:
            self._finalize_current()

    def _finalize_current(self) -> None:
        current = self._current
        self._current = None
        if current is None or "doc_id" not in current:
            return
        self.articles.append(_assemble_article(current))


def _assemble_article(raw: dict[str, str]) -> Article:
    info = raw.get("info_text", "")
    return {
        "doc_id": raw["doc_id"],
        "vol": int(raw["vol"]),
        "issue": int(raw["issue"]),
        "title": raw.get("title", ""),
        "authors": _parse_authors(raw.get("authors_title", "")),
        "article_type": _parse_article_type(info),
        "pages": _parse_pages(info),
        "release_date": _parse_release_date(info),
        "article_url": raw.get("article_url", ""),
        "pdf_url": raw.get("pdf_url", ""),
    }


def _parse_authors(authors_title: str) -> list[str]:
    cleaned = unescape(authors_title).strip()
    if not cleaned:
        return []
    return [a.strip() for a in cleaned.split(",") if a.strip()]


def _parse_article_type(info_text: str) -> str:
    # The info block is tag-stripped, so "Article type:" runs until the
    # following "<year>Volume" segment (or end of line). J-STAGE wraps the
    # label in em-dashes, e.g. "—Full Paper—".
    match = re.search(r"Article type:\s*(.+?)\s*(?:\d{4}Volume|\n|$)", info_text)
    if match is None:
        return ""
    return match.group(1).strip().strip("—").strip()


def _parse_pages(info_text: str) -> str:
    match = re.search(r"Pages\s+([\dA-Za-z]+(?:\s*-\s*[\dA-Za-z]+)?)", info_text)
    if match is None:
        return ""
    return re.sub(r"\s*-\s*", "-", match.group(1).strip())


def _parse_release_date(info_text: str) -> str:
    match = re.search(r"Released on J-STAGE:\s*(.+?)\s*$", info_text)
    if match is None:
        return ""
    return match.group(1).strip()


def parse_contents(html: str) -> list[Article]:
    """Parse a volume/issue contents page into a list of article dicts."""
    parser = _ContentsParser()
    parser.feed(html)
    parser.close()
    return parser.articles


def fetch_contents(
    vol: int,
    issue: int = 0,
    *,
    fetch: FetchText = default_fetch_text,
) -> list[Article]:
    """Fetch the contents page for ``vol``/``issue`` and parse it.

    The HTTP layer is injectable via ``fetch`` so this is unit-testable
    without network access.
    """
    html = fetch(contents_url(vol, issue))
    return parse_contents(html)


def download_pdf(
    pdf_url: str,
    dest_path: str,
    *,
    fetch: FetchBytes = default_fetch_bytes,
    attempts: int = 3,
    backoff: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Download a PDF to ``dest_path``.

    - Skips the download if ``dest_path`` already exists (idempotent).
    - Retries up to ``attempts`` times with exponential backoff.
    - Returns ``dest_path``.
    """
    dest = Path(dest_path)
    if dest.exists():
        return dest_path
    dest.parent.mkdir(parents=True, exist_ok=True)

    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            data = fetch(pdf_url)
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = exc
            if attempt < attempts - 1:
                sleep(backoff * (2**attempt))
            continue
        dest.write_bytes(data)
        return dest_path

    message = f"failed to download {pdf_url} after {attempts} attempts"
    raise RuntimeError(message) from last_error


def _cmd_contents(args: argparse.Namespace) -> int:
    delay = float(args.delay)
    if delay > 0:
        time.sleep(delay)
    articles = fetch_contents(int(args.vol), int(args.issue))
    json.dump(articles, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


def _cmd_download(args: argparse.Namespace) -> int:
    delay = float(args.delay)
    if delay > 0:
        time.sleep(delay)
    path = download_pdf(args.pdf_url, args.dest)
    sys.stdout.write(f"{path}\n")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="jstage",
        description="Collect article metadata and PDFs from J-STAGE (jes).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        help="polite delay in seconds before the request (default: %(default)s)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    contents = sub.add_parser("contents", help="print article metadata as a JSON list")
    contents.add_argument("--vol", type=int, required=True, help="volume number")
    contents.add_argument(
        "--issue",
        type=int,
        default=0,
        help="issue number (0 = whole-volume aggregate, default)",
    )
    contents.set_defaults(func=_cmd_contents)

    download = sub.add_parser("download", help="download a single article PDF")
    download.add_argument("--pdf-url", dest="pdf_url", required=True, help="PDF URL")
    download.add_argument("--dest", required=True, help="destination file path")
    download.set_defaults(func=_cmd_download)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    func: Callable[[argparse.Namespace], int] = args.func
    return func(args)


if __name__ == "__main__":
    raise SystemExit(main())
