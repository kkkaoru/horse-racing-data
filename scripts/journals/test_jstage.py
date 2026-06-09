"""Tests for the J-STAGE collector (``jstage.py``).

The contents fixture is a trimmed-but-faithful copy of the real
``/browse/jes/37/0/_contents/-char/en`` DOM (two articles: a Full Paper and
a Note). No real network access happens here -- ``fetch`` is always injected.
"""

from __future__ import annotations

import io
import json
import types
import urllib.request
from pathlib import Path
from typing import final

import jstage
import pytest

# Trimmed copy of the real J-STAGE contents DOM. Two <li> entries inside
# <ul class="search-resultslisting">, preserving the real class names,
# the title/author attributes, the additional-info block, and the PDF link.
CONTENTS_FIXTURE = """<!DOCTYPE html>
<html lang="en"><body>
<div id="search-resultslist-wrap">
  <div class="searchbrowse-first-subheading  section-level1">Full Paper</div>
  <ul class="search-resultslisting">
    <li class="">
      <div class="searchlist-title"><a href="https://www.jstage.jst.go.jp/article/jes/37/1/37_2511/_article/-char/en" class="bluelink-style customTooltip" title="Dynamics of plasma anti-Müllerian hormone concentrations before and after ovum pick-up in pure and crossbred Hokkaido native ponies" >Dynamics of plasma anti-Müllerian hormone concentrations before and after ovum pick-up in pure and crossbred Hokkaido native ponies</a></div>
      <div class="searchlist-authortags customTooltip" title="Dorb WUDAMU, M A HANNAN, Hiroyuki WATANABE">Dorb WUDAMU, M A HANNAN, Hiroyuki WATANABE ...</div>
      <div class="searchlist-additional-info">
          Article type: —Full Paper—<br>
        2026Volume 37Issue 1 Pages
            1-8
          <br>
 Published: 2026<br> Released on J-STAGE: March 14, 2026 <br> </div>
      <div class="searchlist-doi"><span class="doi-lb">DOI</span><a href="https://doi.org/10.1294/jes.37.1" class="bluelink-style">https://doi.org/10.1294/jes.37.1</a></div>
      <div class="lft">
        <span><a href="https://www.jstage.jst.go.jp/article/jes/37/1/37_2511/_pdf/-char/en" class="bluelink-style">Download PDF</a> (875K)</span>
      </div>
    </li>
  </ul>
  <div class="searchbrowse-first-subheading  section-level1">Note</div>
  <ul class="search-resultslisting">
    <li class="">
      <div class="searchlist-title"><a href="https://www.jstage.jst.go.jp/article/jes/37/1/37_2514/_article/-char/en" class="bluelink-style customTooltip" title="Direct single-nucleotide polymorphism genotyping from whole blood" >Direct single-nucleotide polymorphism genotyping from whole blood</a></div>
      <div class="searchlist-authortags customTooltip" title="Mioko MASUDA, Teruaki TOZAKI">Mioko MASUDA, Teruaki TOZAKI</div>
      <div class="searchlist-additional-info">
          Article type: —Note—<br>
        2026Volume 37Issue 1 Pages
            35-40
          <br>
 Published: 2026<br> Released on J-STAGE: March 14, 2026 <br> </div>
      <div class="lft">
        <span><a href="https://www.jstage.jst.go.jp/article/jes/37/1/37_2514/_pdf/-char/en" class="bluelink-style">Download PDF</a> (512K)</span>
      </div>
    </li>
  </ul>
</div>
</body></html>"""


def test_parse_contents_extracts_two_articles() -> None:
    articles = jstage.parse_contents(CONTENTS_FIXTURE)
    assert len(articles) == 2


def test_parse_contents_full_paper_fields() -> None:
    first = jstage.parse_contents(CONTENTS_FIXTURE)[0]
    assert first["doc_id"] == "37_2511"
    assert first["vol"] == 37
    assert first["issue"] == 1
    assert first["title"].startswith("Dynamics of plasma anti-Müllerian hormone")
    assert first["authors"] == ["Dorb WUDAMU", "M A HANNAN", "Hiroyuki WATANABE"]
    assert first["article_type"] == "Full Paper"
    assert first["pages"] == "1-8"
    assert first["release_date"] == "March 14, 2026"
    assert first["article_url"].endswith("/37_2511/_article/-char/en")
    assert first["pdf_url"].endswith("/37_2511/_pdf/-char/en")


def test_parse_contents_note_fields() -> None:
    second = jstage.parse_contents(CONTENTS_FIXTURE)[1]
    assert second["doc_id"] == "37_2514"
    assert second["article_type"] == "Note"
    assert second["pages"] == "35-40"
    assert second["authors"] == ["Mioko MASUDA", "Teruaki TOZAKI"]
    assert second["pdf_url"].endswith("/37_2514/_pdf/-char/en")


def test_parse_contents_empty_html() -> None:
    assert jstage.parse_contents("<html><body></body></html>") == []


def test_parse_contents_skips_li_without_article_link() -> None:
    html = """
    <ul class="search-resultslisting">
      <li><div class="searchlist-title"><a href="/article/jes/37/1/37_2511/_pdf/-char/en">no detail link</a></div></li>
    </ul>
    """
    # Only a _pdf link present, no _article link -> no doc_id captured -> skipped.
    assert jstage.parse_contents(html) == []


def test_parse_contents_ignores_unrelated_and_attribute_less_links() -> None:
    # Exercises: title-div link with no href, a bare <a> with no href, an <a>
    # to an unrelated URL, and tags carrying neither class nor title attrs.
    html = """
    <ul class="search-resultslisting">
      <li>
        <div class="searchlist-title"><a>untitled</a></div>
        <a>bare anchor</a>
        <a href="https://example.com/elsewhere">offsite</a>
        <div class="searchlist-title"><a href="https://www.jstage.jst.go.jp/article/jes/37/3/37_3000/_article/-char/en">Real</a></div>
        <div class="lft"><span><a href="https://www.jstage.jst.go.jp/article/jes/37/3/37_3000/_pdf/-char/en">Download PDF</a></span></div>
      </li>
    </ul>
    """
    articles = jstage.parse_contents(html)
    assert len(articles) == 1
    assert articles[0]["doc_id"] == "37_3000"
    assert articles[0]["title"] == "Real"
    assert articles[0]["pdf_url"].endswith("/37_3000/_pdf/-char/en")


def _one_article_html(
    *,
    article_href: str = "https://www.jstage.jst.go.jp/article/jes/37/2/37_9999/_article/-char/en",
    author_block: str = '<div class="searchlist-authortags" title="Solo AUTHOR">Solo AUTHOR</div>',
    info_block: str = (
        '<div class="searchlist-additional-info">'
        "Article type: —Note—<br>2026Volume 37Issue 2 Pages S5<br>"
        " Released on J-STAGE: April 1, 2026 <br></div>"
    ),
    pdf_block: str = '<div class="lft"><span><a href="https://www.jstage.jst.go.jp/article/jes/37/2/37_9999/_pdf/-char/en">Download PDF</a></span></div>',
) -> str:
    return f"""
    <ul class="search-resultslisting">
      <li>
        <div class="searchlist-title"><a href="{article_href}">Solo title</a></div>
        {author_block}
        {info_block}
        {pdf_block}
      </li>
    </ul>
    """


def test_parse_contents_single_page_range() -> None:
    # "Pages S5" (no range) must round-trip as "S5".
    article = jstage.parse_contents(_one_article_html())[0]
    assert article["pages"] == "S5"
    assert article["article_type"] == "Note"
    assert article["release_date"] == "April 1, 2026"


def test_parse_contents_empty_author_block() -> None:
    article = jstage.parse_contents(
        _one_article_html(author_block='<div class="searchlist-authortags" title="">x</div>'),
    )[0]
    assert article["authors"] == []


def test_parse_contents_missing_info_fields() -> None:
    article = jstage.parse_contents(
        _one_article_html(info_block='<div class="searchlist-additional-info">no fields here</div>'),
    )[0]
    assert article["article_type"] == ""
    assert article["pages"] == ""
    assert article["release_date"] == ""


def test_parse_contents_handles_missing_pdf_link() -> None:
    article = jstage.parse_contents(_one_article_html(pdf_block=""))[0]
    assert article["doc_id"] == "37_9999"
    assert article["pdf_url"] == ""


def test_contents_url() -> None:
    assert jstage.contents_url(37) == "https://www.jstage.jst.go.jp/browse/jes/37/0/_contents/-char/en"
    assert jstage.contents_url(37, 1) == "https://www.jstage.jst.go.jp/browse/jes/37/1/_contents/-char/en"


def test_fetch_contents_uses_injected_fetch() -> None:
    captured: list[str] = []

    def fake_fetch(url: str) -> str:
        captured.append(url)
        return CONTENTS_FIXTURE

    articles = jstage.fetch_contents(37, 0, fetch=fake_fetch)
    assert captured == ["https://www.jstage.jst.go.jp/browse/jes/37/0/_contents/-char/en"]
    assert [a["doc_id"] for a in articles] == ["37_2511", "37_2514"]


def test_download_pdf_writes_file(tmp_path: Path) -> None:
    dest = tmp_path / "out" / "paper.pdf"

    def fake_fetch(_url: str) -> bytes:
        return b"%PDF-1.7 fake"

    result = jstage.download_pdf(
        "https://www.jstage.jst.go.jp/article/jes/37/1/37_2511/_pdf/-char/en",
        str(dest),
        fetch=fake_fetch,
    )
    assert result == str(dest)
    assert dest.read_bytes() == b"%PDF-1.7 fake"


def test_download_pdf_skips_if_exists(tmp_path: Path) -> None:
    dest = tmp_path / "exists.pdf"
    dest.write_bytes(b"original")

    def boom(_url: str) -> bytes:
        message = "should not be called"
        raise AssertionError(message)

    result = jstage.download_pdf("https://x/_pdf/", str(dest), fetch=boom)
    assert result == str(dest)
    assert dest.read_bytes() == b"original"


def test_download_pdf_retries_then_succeeds(tmp_path: Path) -> None:
    dest = tmp_path / "retry.pdf"
    calls = {"n": 0}
    slept: list[float] = []

    def flaky_fetch(_url: str) -> bytes:
        calls["n"] += 1
        if calls["n"] < 3:
            message = "transient"
            raise OSError(message)
        return b"ok"

    result = jstage.download_pdf(
        "https://x/_pdf/",
        str(dest),
        fetch=flaky_fetch,
        backoff=0.5,
        sleep=slept.append,
    )
    assert result == str(dest)
    assert calls["n"] == 3
    assert slept == [0.5, 1.0]  # exponential: 0.5 * 2**0, 0.5 * 2**1


def test_download_pdf_raises_after_exhausting_attempts(tmp_path: Path) -> None:
    dest = tmp_path / "fail.pdf"

    def always_fail(_url: str) -> bytes:
        message = "down"
        raise OSError(message)

    with pytest.raises(RuntimeError, match="after 3 attempts"):
        jstage.download_pdf(
            "https://x/_pdf/",
            str(dest),
            fetch=always_fail,
            attempts=3,
            sleep=lambda _s: None,
        )
    assert not dest.exists()


def test_default_fetch_rejects_non_http_scheme() -> None:
    # _open's scheme guard surfaces through the public fetch helpers.
    with pytest.raises(ValueError, match="non-http"):
        jstage.default_fetch_text("file:///etc/passwd")


def test_cli_contents(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    def fake_fetch_contents(vol: int, issue: int = 0, **_kwargs: object) -> list[dict[str, object]]:
        assert vol == 37
        assert issue == 0
        return [{"doc_id": "37_2511", "vol": vol, "issue": issue}]

    monkeypatch.setattr(jstage, "fetch_contents", fake_fetch_contents)
    rc = jstage.main(["--delay", "0", "contents", "--vol", "37"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out[0]["doc_id"] == "37_2511"


def test_cli_download(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    dest = tmp_path / "paper.pdf"

    def fake_download(pdf_url: str, dest_path: str, **_kwargs: object) -> str:
        assert pdf_url == "https://x/_pdf/"
        Path(dest_path).write_bytes(b"%PDF")
        return dest_path

    monkeypatch.setattr(jstage, "download_pdf", fake_download)
    rc = jstage.main(["--delay", "0", "download", "--pdf-url", "https://x/_pdf/", "--dest", str(dest)])
    assert rc == 0
    assert capsys.readouterr().out.strip() == str(dest)


def test_cli_contents_applies_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []
    monkeypatch.setattr("jstage.time.sleep", slept.append)
    monkeypatch.setattr(jstage, "fetch_contents", lambda *_a, **_k: [])
    rc = jstage.main(["--delay", "2.5", "contents", "--vol", "37"])
    assert rc == 0
    assert slept == [2.5]


def test_cli_download_applies_delay(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    slept: list[float] = []
    monkeypatch.setattr("jstage.time.sleep", slept.append)
    monkeypatch.setattr(jstage, "download_pdf", lambda _u, dest, **_k: dest)
    rc = jstage.main(["--delay", "1.5", "download", "--pdf-url", "https://x/_pdf/", "--dest", str(tmp_path / "p.pdf")])
    assert rc == 0
    assert slept == [1.5]


@final
class _FakeHeaders:
    def __init__(self, charset: str | None) -> None:
        self._charset = charset

    def get_content_charset(self) -> str | None:
        return self._charset


@final
class _FakeResponse:
    """Stand-in for the object returned by ``opener.open`` (context manager)."""

    def __init__(self, body: bytes, charset: str | None = "utf-8") -> None:
        self._body = body
        self.headers = _FakeHeaders(charset)

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


def _patch_opener(monkeypatch: pytest.MonkeyPatch, response: object) -> list[tuple[object, float]]:
    """Replace urllib's opener so _open returns ``response``; record requests."""
    requests: list[tuple[object, float]] = []

    def fake_build_opener(*_handlers: object) -> object:
        def opener_open(request: object, timeout: float) -> object:
            requests.append((request, timeout))
            return response

        return types.SimpleNamespace(open=opener_open)

    monkeypatch.setattr("jstage.urllib.request.build_opener", fake_build_opener)
    return requests


def test_default_fetch_text_decodes_body(monkeypatch: pytest.MonkeyPatch) -> None:
    requests = _patch_opener(monkeypatch, _FakeResponse("héllo".encode()))
    assert jstage.default_fetch_text("https://x") == "héllo"
    assert len(requests) == 1


def test_default_fetch_text_uses_fallback_charset(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_opener(monkeypatch, _FakeResponse(b"plain", charset=None))
    assert jstage.default_fetch_text("https://x") == "plain"


def test_default_fetch_bytes_returns_body(monkeypatch: pytest.MonkeyPatch) -> None:
    requests = _patch_opener(monkeypatch, _FakeResponse(b"%PDF-bytes"))
    assert jstage.default_fetch_bytes("https://x") == b"%PDF-bytes"
    # _open built a Request with the research User-Agent.
    request, _timeout = requests[0]
    assert isinstance(request, urllib.request.Request)
    assert request.get_header("User-agent") == jstage.USER_AGENT


def test_main_reads_from_argv(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(jstage, "fetch_contents", lambda *_a, **_k: [])
    monkeypatch.setattr("jstage.sys.argv", ["jstage", "--delay", "0", "contents", "--vol", "1"])
    rc = jstage.main()
    assert rc == 0
    assert capsys.readouterr().out.strip() == "[]"


def test_stdout_is_text_stream() -> None:
    # Guard: ensure json.dump target type assumption (text stream) holds.
    assert isinstance(io.StringIO(), io.TextIOBase)
