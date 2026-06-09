"""Tests for the 馬の科学 body reflow (``reflow_es.py``).

These are deliberately NOT DRY: each behaviour gets its own self-contained
fixture so a failure points at exactly one rule. No network, no real journal
files are touched — every input is a tiny hand-written markdown snippet.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest
import reflow_es

# A minimal but structurally faithful prefix: H1, metadata table, 目次, note,
# rule, and the 全文 header. Everything here must survive byte-for-byte.
PREFIX = (
    "# 馬の科学 テスト巻\n"
    "\n"
    "| 項目 | 値 |\n"
    "|------|----|\n"
    "| 誌名 | 馬の科学 |\n"
    "\n"
    "## 目次\n"
    "\n"
    "- 感染症対策　松村富夫 ……1\n"
    "\n"
    "> 本文は以下にPDF抽出テキストをそのまま収録（要約なし）。\n"
    "\n"
    "---\n"
    "\n"
    "## 全文（PDF 抽出テキスト・要約なし）\n"
)


def test_leader_line_is_rejoined_onto_previous_title() -> None:
    # A TOC title on one line, then a line that is ONLY dotted leaders + author
    # + page number. The leader tail must be pulled back up onto the title.
    src = (
        PREFIX
        + "\n<!-- ===== page 2/3 ===== -->\n\n"
        + "ウマコロナウイルス病の流行とその特徴\n"
        + "……………………………………………………… 根本　学 ……24\n"
    )
    out = reflow_es.reflow_text(src)
    assert (
        "ウマコロナウイルス病の流行とその特徴……………………………………………………… 根本　学 ……24" in out
    )
    # The leader line must no longer stand alone.
    assert "\n………………………" not in out


def test_leader_line_rejoins_even_for_short_title() -> None:
    # Width gate must NOT block a leader rejoin: a short title still owns its
    # dotted tail.
    src = PREFIX + "\n<!-- ===== page 2/3 ===== -->\n\n資料\n…… 片山芳也 ……62\n"
    out = reflow_es.reflow_text(src)
    assert "資料…… 片山芳也 ……62" in out


def test_continuation_join_concatenates_wrapped_prose_with_no_space() -> None:
    # Two full-width Japanese prose lines wrapped mechanically must join with no
    # interposed space.
    first = "　早いもので、馬インフルエンザの流行から七年になる。輸入検疫への馬"
    second = "インフルエンザウイルス検出を目的としたＰＣＲ法および簡易診断キットの導入が進んだ。"
    src = PREFIX + "\n<!-- ===== page 3/3 ===== -->\n\n" + first + "\n" + second + "\n"
    out = reflow_es.reflow_text(src)
    assert first + second in out
    # No stray ASCII space was introduced at the CJK join boundary.
    assert "馬 インフルエンザ" not in out


def test_continuation_not_joined_after_sentence_final_punctuation() -> None:
    # When the previous (full-width) line ends with 。 the next line starts a new
    # paragraph and must stay on its own line.
    first = "　これは一つ目の十分に長い段落であり、行末は句点で完全に終わっている。"
    second = "　これは二つ目の段落であり、独立した行として保持されなければならない。"
    src = PREFIX + "\n<!-- ===== page 3/3 ===== -->\n\n" + first + "\n" + second + "\n"
    out = reflow_es.reflow_text(src)
    assert first + "\n" + second in out


def test_continuation_not_joined_after_trailing_digit() -> None:
    # A line ending in a digit (a completed TOC entry / page number) is complete.
    first = "装蹄歴史案内　第3回ペルシャ馬の蹄鉄………………………… 関口　隆 ……22"
    second = "ウマコロナウイルス病の流行とその特徴が次の独立した行になる必要がある。"
    src = PREFIX + "\n<!-- ===== page 2/3 ===== -->\n\n" + first + "\n" + second + "\n"
    out = reflow_es.reflow_text(src)
    assert first + "\n" + second in out


def test_short_title_does_not_swallow_following_prose_line() -> None:
    # The width gate: a short standalone article title must NOT absorb the first
    # prose line that follows it.
    title = "感染症対策"
    prose = "　早いもので、馬インフルエンザの流行から七年になり、長い本文がここから続いていく。"
    src = PREFIX + "\n<!-- ===== page 3/3 ===== -->\n\n" + title + "\n" + prose + "\n"
    out = reflow_es.reflow_text(src)
    assert title + "\n" + prose in out
    assert "感染症対策　早いもので" not in out


def test_ascii_word_boundary_gets_single_space() -> None:
    # When BOTH sides of the join are ASCII word characters a single space is
    # inserted so English words do not run together.
    first = "1）Tokushige Hirotaka : 栗東トレーニング・センター 競走馬診療所 and other long names here"
    second = "Ohta Minoru worked at the Miho training center for many seasons indeed."
    src = PREFIX + "\n<!-- ===== page 5/5 ===== -->\n\n" + first + "\n" + second + "\n"
    out = reflow_es.reflow_text(src)
    assert "names here Ohta Minoru" in out


def test_blank_lines_collapse_to_single() -> None:
    src = (
        PREFIX
        + "\n<!-- ===== page 1/2 ===== -->\n\n"
        + "本文一行目で句点まで書いて段落を閉じる。\n\n\n\n"
        + "次の段落も独立して句点で閉じる。\n"
    )
    out = reflow_es.reflow_text(src)
    assert "\n\n\n" not in out


def test_markers_are_isolated_by_one_blank_line_each() -> None:
    # Page and OCR markers must each be surrounded by exactly one blank line,
    # even if the source crowded them.
    src = (
        PREFIX
        + "\n<!-- ===== page 1/2 ===== -->\n"
        + "Vol.51 という短い行。\n"
        + "<!-- ===== page 2/2 ===== -->\n"
        + "Equine Science\n"
        + "<!-- (above page via OCR) -->\n"
    )
    out = reflow_es.reflow_text(src)
    assert "\n\n<!-- ===== page 1/2 ===== -->\n\n" in out
    assert "\n\n<!-- ===== page 2/2 ===== -->\n\n" in out
    assert "\n\n<!-- (above page via OCR) -->" in out


def test_prefix_is_preserved_byte_for_byte() -> None:
    src = (
        PREFIX + "\n<!-- ===== page 1/1 ===== -->\n\n本文をここに書いて句点で閉じる。\n"
    )
    out = reflow_es.reflow_text(src)
    assert out.startswith(PREFIX)
    # The header appears exactly once and the bytes up to it are unchanged.
    assert out.count(reflow_es.FULLTEXT_HEADER) == 1
    header_end = out.index(reflow_es.FULLTEXT_HEADER) + len(reflow_es.FULLTEXT_HEADER)
    assert out[:header_end] == PREFIX[: PREFIX.index(reflow_es.FULLTEXT_HEADER)] + (
        reflow_es.FULLTEXT_HEADER
    )


def test_missing_header_returns_input_unchanged() -> None:
    # If there is no 全文 header the file is left completely untouched.
    src = "# title only\n\nsome text without the fulltext header\n"
    assert reflow_es.reflow_text(src) == src


def test_reflow_is_idempotent() -> None:
    src = (
        PREFIX
        + "\n<!-- ===== page 3/3 ===== -->\n\n"
        + "感染症対策\n"
        + "　早いもので、長い本文がここから始まり、行をまたいで\n"
        + "続いていくが最後は句点で終わる。\n"
        + "（1）\n巻\n頭\n言\n"
    )
    once = reflow_es.reflow_text(src)
    twice = reflow_es.reflow_text(once)
    assert once == twice


def test_leader_run_line_does_not_swallow_next_entry() -> None:
    # FIX A: a line that already CONTAINS a leader run (title … author …) is a
    # complete TOC entry and must NOT absorb the following complete entry, even
    # though both are full-width. Otherwise the whole 目次 page blobs together.
    entry1 = "競走馬総合研究所の近況 …………………………………………松村富夫…"
    entry2 = "装蹄歴史案内　第7回アルミニウム蹄鉄の普及……………………関口　隆…"
    src = PREFIX + "\n<!-- ===== page 2/3 ===== -->\n\n" + entry1 + "\n" + entry2 + "\n"
    out = reflow_es.reflow_text(src)
    assert entry1 + "\n" + entry2 in out
    # The two entries must remain on separate lines (no blob).
    assert entry1 + entry2 not in out


def test_wrapped_title_still_joins_up_to_its_leader_line() -> None:
    # FIX A must NOT break the good case: a wrapped title (lines with NO leader
    # run) joins down to the leader-prefixed tail, producing one clean entry.
    title1 = (
        "膠質輸液剤（6%ハイドロキシエチルスターチ液）投与がセボフルラン吸入麻酔下の"
    )
    title2 = "　サラブレッド種の血行動態に及ぼす影響"
    leader = "……………………………………………… 徳重裕貴・太田　稔・石川裕博 …… 3"
    src = (
        PREFIX
        + "\n<!-- ===== page 2/3 ===== -->\n\n"
        + title1
        + "\n"
        + title2
        + "\n"
        + leader
        + "\n"
    )
    out = reflow_es.reflow_text(src)
    # All three source lines collapse into a single entry line.
    expected = title1 + title2.strip() + leader.strip()
    assert expected in out


def test_leader_prefixed_line_rejoins_its_predecessor() -> None:
    # A line that STARTS with a leader run rejoins onto the title above it (the
    # behaviour FIX A must preserve), independent of the no-swallow rule.
    title = "ウマコロナウイルス病の流行とその特徴"
    leader = "……………………………………………………… 根本　学 ……24"
    src = PREFIX + "\n<!-- ===== page 2/3 ===== -->\n\n" + title + "\n" + leader + "\n"
    out = reflow_es.reflow_text(src)
    assert title + leader.strip() in out
    # The leader tail no longer stands on its own line.
    assert "\n……………………" not in out


def test_control_chars_are_stripped_from_body() -> None:
    # FIX B: C0 control garbage (U+0000-U+001F except \n, \t) is removed from the
    # body. Carriage returns are normalised away by the same rule.
    body = "本文\x0cに\x1c制御\x03文字\x10\x1b\rが混じる。\n"
    src = PREFIX + "\n<!-- ===== page 1/1 ===== -->\n\n" + body
    out = reflow_es.reflow_text(src)
    assert all(ord(ch) >= 0x20 or ch in "\n\t" for ch in out)
    assert "本文に制御文字が混じる。" in out


def test_guard_ignores_stripped_control_chars() -> None:
    # FIX B: removing control chars must NOT trip the preservation guard — they
    # are in the ignore set. A body that previously aborted now reflows cleanly.
    body = "感染\x03症\x0c対策\x1cの本文。\n"
    src = PREFIX + "\n<!-- ===== page 1/1 ===== -->\n\n" + body
    result = reflow_es.reflow_checked(src)  # must not raise
    assert "感染症対策の本文。" in result
    # The control chars are excluded from the substantive multiset entirely.
    counter = reflow_es.content_counter("感染\x03症\x0c対策\x1c")
    assert counter == Counter({"感": 1, "染": 1, "症": 1, "対": 1, "策": 1})


def test_content_counter_ignores_whitespace_and_leader_dots() -> None:
    # Leader dots and every flavour of whitespace are excluded; real glyphs count.
    counter = reflow_es.content_counter("あ　a …… ・. \t\nい")
    assert counter == Counter({"あ": 1, "a": 1, "い": 1})


def test_display_width_counts_cjk_as_two() -> None:
    assert reflow_es.display_width("abc") == 3
    assert reflow_es.display_width("馬の科学") == 8  # 4 wide chars
    assert reflow_es.display_width("  trimmed  ") == 7  # surrounding ws trimmed


def test_preservation_guard_rejects_a_lossy_transform(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Simulate a buggy reflow that DROPS a substantive character. The guard must
    # raise and the diff must mention the dropped glyph's count change.
    src = (
        PREFIX
        + "\n<!-- ===== page 1/1 ===== -->\n\n感染症対策の本文をここに書いて句点で閉じる。\n"
    )

    def _lossy(text: str) -> str:
        # Drop every occurrence of the kanji 症 — a real content loss.
        return text.replace("症", "")

    monkeypatch.setattr(reflow_es, "reflow_text", _lossy)
    with pytest.raises(reflow_es.PreservationError) as excinfo:
        reflow_es.reflow_checked(src)
    message = str(excinfo.value)
    assert "content-preservation guard FAILED" in message
    assert "'症'" in message


def test_process_file_check_only_does_not_write(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "j.md"
    src = (
        PREFIX
        + "\n<!-- ===== page 1/1 ===== -->\n\n"
        + "短い題\n"
        + "　長い本文がここから始まりやがて行をまたいで\n"
        + "続いていき最後は句点で終わる。\n"
    )
    path.write_text(src, encoding="utf-8")
    changed = reflow_es.process_file(path, check_only=True)
    assert changed is True
    # File on disk is untouched in --check mode.
    assert path.read_text(encoding="utf-8") == src
    assert "would change" in capsys.readouterr().out


def test_process_file_rewrites_in_place(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "j.md"
    src = (
        PREFIX
        + "\n<!-- ===== page 1/1 ===== -->\n\n"
        + "短い題\n"
        + "　長い本文がここから始まりやがて行をまたいで\n"
        + "続いていき最後は句点で終わる。\n"
    )
    path.write_text(src, encoding="utf-8")
    changed = reflow_es.process_file(path, check_only=False)
    assert changed is True
    rewritten = path.read_text(encoding="utf-8")
    assert rewritten != src
    assert rewritten.startswith(PREFIX)
    # Title stayed on its own line; the two prose lines merged into one.
    assert "短い題\n" in rewritten
    assert "やがて行をまたいで続いていき" in rewritten
    assert "reflowed" in capsys.readouterr().out


def test_process_file_no_change_reports_no_change(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "j.md"
    # Already-normalised content: one paragraph per line, markers spaced.
    src = (
        PREFIX
        + "\n<!-- ===== page 1/1 ===== -->\n\n"
        + "本文がすでに一行にまとまっていて句点で終わる。\n"
    )
    path.write_text(src, encoding="utf-8")
    changed = reflow_es.process_file(path, check_only=False)
    assert changed is False
    assert "no change" in capsys.readouterr().out


def test_main_check_returns_zero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "j.md"
    path.write_text(
        PREFIX + "\n<!-- ===== page 1/1 ===== -->\n\n本文。\n", encoding="utf-8"
    )
    assert reflow_es.main([str(path), "--check"]) == 0
    capsys.readouterr()


def test_main_returns_two_and_aborts_when_guard_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "j.md"
    src = PREFIX + "\n<!-- ===== page 1/1 ===== -->\n\n感染症の本文。\n"
    path.write_text(src, encoding="utf-8")

    monkeypatch.setattr(reflow_es, "reflow_text", lambda text: text.replace("症", ""))
    code = reflow_es.main([str(path)])
    assert code == 2
    # The lossy transform must NOT have been written to disk.
    assert path.read_text(encoding="utf-8") == src
    assert "content-preservation guard FAILED" in capsys.readouterr().err
