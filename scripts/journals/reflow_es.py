"""Reflow the body of a "馬の科学" (Equine Science) journal markdown file.

These files contain a hand-cleaned prefix (H1 title, a ``| 項目 | 値 |`` metadata
table, a ``## 目次`` section, a ``>`` note, ``---``) followed by a
``## 全文（PDF 抽出テキスト・要約なし）`` header and then page blocks delimited by
``<!-- ===== page N/M ===== -->`` (with occasional ``<!-- (above page via OCR) -->``).

The PDF-extracted body has unnatural line breaks: table-of-contents titles get
split from their dotted-leader/author/page tails, and prose sentences are wrapped
across many short lines. This module reflows ONLY the body (everything AFTER the
``## 全文`` header), joining continuation lines so the result reads naturally.

Guarantees (this is a whitespace / line-break reflow, never a summary):

* The prefix through the ``## 全文`` header line is preserved byte-for-byte.
* Every page / OCR marker is preserved, surrounded by exactly one blank line.
* C0 control characters (U+0000-U+001F, except newline and tab) are stripped from
  the BODY only — they are PDF/CID-extraction garbage, never information. The
  prefix keeps them (it has none in practice and must stay byte-for-byte).
* A content-preservation guard compares the multiset of all characters EXCLUDING
  whitespace, leader-dot characters (… ‥ ・ and ASCII ``.``) and the stripped C0
  controls before and after; if they differ the file is NOT rewritten and the
  count diff is printed.

Stdlib only (``argparse``, ``re``, ``sys``, ``collections``, ``pathlib``).
The transform is deterministic and idempotent.

CLI::

    python reflow_es.py <file.md>            # rewrite in place
    python reflow_es.py --check <file.md>    # report whether it would change
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from collections import Counter
from pathlib import Path

# The marker that separates the hand-cleaned prefix from the reflowable body.
# Everything up to and including this line is emitted unchanged.
FULLTEXT_HEADER = "## 全文（PDF 抽出テキスト・要約なし）"

# HTML-comment markers that must each be isolated by exactly one blank line.
_PAGE_MARKER_RE = re.compile(r"^<!--\s*=+\s*page\s+\d+/\d+\s*=+\s*-->\s*$")
_OCR_MARKER_RE = re.compile(r"^<!--\s*\(above page via OCR\)\s*-->\s*$")

# Leader-dot glyphs used in the source PDFs (Japanese ellipsis runs + middle dot)
# plus the ASCII period. These are decorative in TOC leaders and are excluded
# from the content-preservation comparison.
LEADER_CHARS = "…‥・."

# A "leader line" is a wrapped TOC tail: it begins with two or more leader-dot
# glyphs (optionally indented) and carries only the dotted leader, an author
# name, more dots and a trailing page number. We detect it conservatively by the
# *leading* run of leader dots so OCR-garbled author noise never triggers it.
_LEADER_LINE_RE = re.compile(r"^[ 　\t]*[…‥]{2,}")

# A "leader run" anywhere in a line marks it as a complete table-of-contents
# entry (title … author … page). Such a line must NOT swallow the line that
# follows it, otherwise every already-complete 目次 entry blobs into one line.
# We require a run of 2+ ellipsis/middle-dot glyphs (NOT bare ASCII '.', which
# appears in ordinary prose abbreviations / DOIs / English references).
_LEADER_RUN_RE = re.compile(r"[…‥・]{2,}")

# C0 control characters that are PDF/CID-extraction garbage. We strip everything
# in U+0000-U+001F except newline and tab from the body; carriage returns are
# normalised away by the same rule. These are added to the guard ignore set so
# their intentional removal never trips content preservation.
_CONTROL_CHARS = frozenset(chr(code) for code in range(0x20) if chr(code) not in "\n\t")
_CONTROL_STRIP_RE = re.compile(
    "[" + "".join(re.escape(ch) for ch in _CONTROL_CHARS) + "]"
)

# Sentence-final punctuation: when the previous line ends with one of these we do
# not join the next line onto it.
_SENTENCE_FINAL = "。！？!?"

# A bullet / heading / blockquote / horizontal-rule / table line starts a new
# logical block, so it is never joined onto the previous line and the previous
# line is never extended into it.
_BLOCK_START_RE = re.compile(r"^(#{1,6}\s|[-*+]\s|\d+[.)]\s|>|\||---|===|\s*$)")

# ASCII word characters; used to decide whether to insert a single space when
# joining two ASCII-bounded fragments (vs. zero space for CJK boundaries).
_ASCII_WORD_RE = re.compile(r"[0-9A-Za-z]")

# The PDF body wraps at a fixed column: across the corpus, soft-wrapped body
# lines have a display width clustered tightly around ~44 (median 44, p25-p90
# all 42-45). A line whose display width reaches this threshold plausibly hit
# the right margin and the following line is its soft-wrap continuation; a
# shorter line ended deliberately (article title, running head, page-number
# footer, the last line of a paragraph) and must NOT swallow the next line.
# 38 leaves margin below the ~42 cluster floor while still excluding short
# standalone lines.
_WRAP_COLUMN_MIN = 38


def split_prefix_body(text: str) -> tuple[str, str | None]:
    """Split ``text`` at the ``## 全文`` header.

    Returns ``(prefix, body)`` where ``prefix`` ends with the header line and a
    trailing newline, and ``body`` is everything after it. If the header is not
    present, returns ``(text, None)`` and the caller leaves the file untouched.
    """

    lines = text.split("\n")
    for index, line in enumerate(lines):
        if line.strip() == FULLTEXT_HEADER:
            prefix = "\n".join(lines[: index + 1]) + "\n"
            body = "\n".join(lines[index + 1 :])
            return prefix, body
    return text, None


def content_counter(text: str) -> Counter[str]:
    """Multiset of characters excluding whitespace, leader dots and C0 controls.

    This is the invariant the reflow must preserve: only whitespace, decorative
    leader dots and stripped control-character garbage may change in count.
    Public so the guarantee can be asserted independently.
    """

    ignore = set(LEADER_CHARS) | set(" \t\r\n　 ") | _CONTROL_CHARS
    return Counter(ch for ch in text if ch not in ignore)


def _is_marker(line: str) -> bool:
    return bool(_PAGE_MARKER_RE.match(line) or _OCR_MARKER_RE.match(line))


def _is_leader_line(line: str) -> bool:
    """True for a wrapped TOC tail that begins with a run of leader dots."""

    return bool(_LEADER_LINE_RE.match(line))


def _ends_with_digit(line: str) -> bool:
    stripped = line.rstrip()
    return bool(stripped) and stripped[-1].isdigit()


def _ends_sentence(line: str) -> bool:
    stripped = line.rstrip()
    return bool(stripped) and stripped[-1] in _SENTENCE_FINAL


def _starts_block(line: str) -> bool:
    return bool(_BLOCK_START_RE.match(line))


def _contains_leader_run(line: str) -> bool:
    """True if ``line`` contains an ellipsis/middle-dot leader run.

    Such a run (title … author … page) marks an already-complete table-of-
    contents entry, so the line must not absorb the following entry.
    """

    return bool(_LEADER_RUN_RE.search(line))


def display_width(line: str) -> int:
    """East-Asian display width of ``line`` (full/wide/ambiguous CJK = 2)."""

    return sum(
        2 if unicodedata.east_asian_width(ch) in ("W", "F", "A") else 1
        for ch in line.strip()
    )


def _is_full_width_line(line: str) -> bool:
    """True if ``line`` plausibly reached the PDF wrap column (right margin)."""

    return display_width(line) >= _WRAP_COLUMN_MIN


def _join(prev: str, addition: str) -> str:
    """Concatenate ``addition`` onto ``prev``.

    CJK / mixed boundaries are concatenated with no space (Japanese text never
    needs an inter-character space). Only an ASCII-word-to-ASCII-word boundary
    gets a single space so e.g. English words stay separated.
    """

    left = prev.rstrip()
    right = addition.strip()
    if not right:
        return left
    if not left:
        return right
    needs_space = bool(_ASCII_WORD_RE.search(left[-1])) and bool(
        _ASCII_WORD_RE.search(right[0])
    )
    return left + (" " if needs_space else "") + right


def _should_join_continuation(prev: str, nxt: str) -> bool:
    """Decide whether ``nxt`` is a soft-wrapped continuation of ``prev``.

    ``prev`` is the running paragraph line as accumulated so far. Using the
    accumulator (rather than the last raw segment) makes the transform
    IDEMPOTENT: on a re-run the merged paragraph IS the input line, so the same
    decision is taken and nothing changes.

    A soft wrap is signalled by ``prev`` reaching the PDF wrap column (mechanical
    break, not authored). We refuse to join when ``prev`` looks complete
    (sentence-final punctuation, trailing digit, or an embedded leader run that
    marks a finished TOC entry) or when ``nxt`` begins a new logical block. The
    width gate is what stops a short standalone line (article title, running
    head, the lone final line of a paragraph) from swallowing the following line
    — so a title like ``感染症対策`` never glues onto the first prose line.
    """

    if not prev.strip() or not nxt.strip():
        return False
    if _starts_block(nxt) or _is_marker(nxt):
        return False
    if _starts_block(prev):
        return False
    if _ends_sentence(prev) or _ends_with_digit(prev):
        return False
    if _contains_leader_run(prev):
        return False
    return _is_full_width_line(prev)


def _flush_paragraph(buffer: list[str]) -> list[str]:
    """Reflow a run of consecutive non-blank, non-marker lines into joined text.

    A leader line is always merged onto the line before it (a TOC tail belongs to
    its title regardless of the title's length). Other lines are merged onto the
    running paragraph when the join rules say the break was a mechanical wrap.
    """

    if not buffer:
        return []
    out: list[str] = [buffer[0]]
    for line in buffer[1:]:
        prev = out[-1]
        if _is_leader_line(line) or _should_join_continuation(prev, line):
            out[-1] = _join(prev, line)
        else:
            out.append(line)
    return out


def _emit_marker(out: list[str], marker: str) -> None:
    """Append ``marker`` to ``out`` isolated by exactly one blank line on each
    side.

    The body always follows the `## 全文` header line, so a single leading blank
    line is emitted before the very first marker too (it isolates the marker
    from the header just like every interior marker).
    """

    while out and out[-1] == "":
        out.pop()
    out.append("")
    out.append(marker.rstrip())
    out.append("")


def reflow_body(body: str) -> str:
    """Reflow the post-header body text.

    Page / OCR markers are re-emitted with one blank line on each side. Between
    markers, consecutive text lines are gathered into paragraph buffers and
    reflowed; 2+ blank lines collapse to one paragraph break.

    C0 control-character garbage (everything in U+0000-U+001F except newline and
    tab, carriage returns included) is stripped before line splitting.
    """

    body = _CONTROL_STRIP_RE.sub("", body)
    lines = body.split("\n")
    out: list[str] = []
    buffer: list[str] = []

    def flush() -> None:
        if buffer:
            out.extend(_flush_paragraph(buffer))
            buffer.clear()

    for line in lines:
        if _is_marker(line):
            flush()
            _emit_marker(out, line)
        elif line.strip() == "":
            flush()
            # Collapse runs of blank lines into a single blank separator.
            if out and out[-1] != "":
                out.append("")
        else:
            buffer.append(line)
    flush()

    # Normalise to a single trailing newline.
    while out and out[-1] == "":
        out.pop()
    return "\n".join(out) + "\n"


def reflow_text(text: str) -> str:
    """Reflow a full file's text. The prefix is preserved byte-for-byte.

    If the ``## 全文`` header is absent the input is returned unchanged.
    """

    prefix, body = split_prefix_body(text)
    if body is None:
        return text
    return prefix + reflow_body(body)


class PreservationError(RuntimeError):
    """Raised when the reflow would change substantive (non-whitespace,
    non-leader-dot) characters."""


def _format_count_diff(before: Counter[str], after: Counter[str]) -> str:
    keys = sorted(set(before) | set(after))
    rows = [
        f"  {key!r}: {before.get(key, 0)} -> {after.get(key, 0)}"
        for key in keys
        if before.get(key, 0) != after.get(key, 0)
    ]
    return "\n".join(rows)


def reflow_checked(text: str) -> str:
    """Reflow ``text`` and assert content preservation.

    Raises :class:`PreservationError` (with the per-character count diff) if the
    multiset of substantive characters changes.
    """

    result = reflow_text(text)
    before = content_counter(text)
    after = content_counter(result)
    if before != after:
        diff = _format_count_diff(before, after)
        raise PreservationError(
            "content-preservation guard FAILED; not writing. Count diffs:\n" + diff
        )
    return result


def process_file(path: Path, *, check_only: bool) -> bool:
    """Reflow ``path`` in place (or report-only when ``check_only``).

    Returns True when the file content would change. Writing only happens after
    the preservation guard passes, so a guard failure aborts without mutation.
    """

    original = path.read_text(encoding="utf-8")
    result = reflow_checked(original)
    changed = result != original
    if check_only:
        verb = "would change" if changed else "already normalised (no change)"
        print(f"{path}: {verb}")
        return changed
    if changed:
        path.write_text(result, encoding="utf-8")
        print(f"{path}: reflowed (content-preservation guard passed)")
    else:
        print(f"{path}: no change")
    return changed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Reflow the body of a 馬の科学 journal markdown file."
    )
    parser.add_argument("file", type=Path, help="path to the .md file")
    parser.add_argument(
        "--check",
        action="store_true",
        help="report whether the file would change without writing",
    )
    args = parser.parse_args(argv)

    try:
        process_file(args.file, check_only=args.check)
    except PreservationError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
