from __future__ import annotations

from datetime import date
from pathlib import Path
from urllib.error import URLError

import pytest

import keiba_archive_records as subject
from overseas_finish_features import PersonSummary


class _FakeUrlResponse:
    _payload: bytes

    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self) -> _FakeUrlResponse:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        return False


def test_fold_person_name_strips_affiliation_and_width() -> None:
    assert subject.fold_person_name("K．バーク（GB）") == "k.バーク"
    assert subject.fold_person_name("田中 博康(JPN)") == "田中博康"


def test_parse_jra_trainer_owner_names() -> None:
    html = """
    <p class="trainer">K.バーク<span class="division">(GB)</span></p>
    <p class="trainer">田中 博康(JPN)</p>
    <p class="owner">キャロットファーム</p>
    <p class="owner">WERTHEIMER &amp; FRERE</p>
    """
    parsed = subject.parse_jra_trainer_owner_names(html)
    assert parsed.trainers == ("K.バーク (GB)", "田中 博康(JPN)")
    assert parsed.owners == ("キャロットファーム", "WERTHEIMER & FRERE")


def test_parse_netkeiba_trainer_titles() -> None:
    html = (
        '<a href="/trainer/result/recent/a031e/" title="バーク">x</a>'
        '<a href="/trainer/result/recent/01162/" title="田中博康">y</a>'
    )
    assert subject.parse_netkeiba_trainer_names(html) == ("バーク", "田中博康")


def test_parse_netkeiba_owner_titles() -> None:
    html = (
        '<a href="/owner/result/recent/o_abc/" title="キャロットファーム">x</a>'
        '<a href="/owner/o_def/" title="ウェルトハイマー">y</a>'
    )
    assert subject.parse_netkeiba_owner_names(html) == ("キャロットファーム", "ウェルトハイマー")


def test_parse_netkeiba_owner_link_text_when_title_missing() -> None:
    html = (
        '<a href="/owner/o_abc/"><span>キャロットファーム</span></a>'
        '<a href="/owner/o_def/">ウェルトハイマー</a>'
    )
    assert subject.parse_netkeiba_owner_names(html) == ("キャロットファーム", "ウェルトハイマー")


def test_parse_netkeiba_owner_skips_generic_title_and_uses_text() -> None:
    html = (
        '<a href="/owner/race.html?id=o_abc" title="近走成績">近走成績</a>'
        '<a href="/owner/o_abc/">キャロットファーム</a>'
    )
    assert subject.parse_netkeiba_owner_names(html) == ("キャロットファーム",)


def test_parse_netkeiba_owner_prefers_titles_over_text() -> None:
    html = (
        '<a href="/owner/o_abc/" title="キャロットファーム">ignored</a>'
        '<a href="/owner/o_def/">ウェルトハイマー</a>'
    )
    assert subject.parse_netkeiba_owner_names(html) == ("キャロットファーム",)


def test_parse_netkeiba_trainer_skips_generic_title() -> None:
    html = (
        '<a href="/trainer/result/recent/a031e/" title="調教師">x</a>'
        '<a href="/trainer/result/recent/a031e/" title="バーク">y</a>'
    )
    assert subject.parse_netkeiba_trainer_names(html) == ("バーク",)


def test_optimize_published_names_prefers_jra_labels() -> None:
    jra = '<p class="trainer">C.フェルラン(FR)</p><p class="owner">AGA KHAN STUDS SC</p>'
    netkeiba = (
        '<a href="/trainer/result/recent/a064b/" title="フェルラン">x</a>'
        '<a href="/owner/o_aga/" title="アーガーカーン">y</a>'
    )
    published = subject.optimize_published_names(jra_html=jra, netkeiba_html=netkeiba)
    assert published.trainers == ("C.フェルラン(FR)",)
    assert published.owners == ("AGA KHAN STUDS SC",)
    assert published.trainer_extra_stems == (("フェルラン",),)
    assert published.owner_extra_stems == (("アーガーカーン",),)


def test_optimize_published_names_falls_back_to_netkeiba_trainers() -> None:
    published = subject.optimize_published_names(
        jra_html="<div></div>",
        netkeiba_html='<a href="/trainer/result/recent/a031e/" title="バーク">x</a>',
    )
    assert published.trainers == ("バーク",)
    assert published.owners == ()
    assert published.trainer_extra_stems == ((),)
    assert published.owner_extra_stems == ()


def test_optimize_published_names_falls_back_to_netkeiba_owners() -> None:
    published = subject.optimize_published_names(
        jra_html='<p class="trainer">田中 博康(JPN)</p>',
        netkeiba_html='<a href="/owner/o_carrot/" title="キャロットファーム">x</a>',
    )
    assert published.trainers == ("田中 博康(JPN)",)
    assert published.owners == ("キャロットファーム",)
    assert published.trainer_extra_stems == ((),)
    assert published.owner_extra_stems == ((),)


def test_optimize_published_names_keeps_netkeiba_shorts_per_umaban() -> None:
    jra = (
        '<p class="trainer">K.バーク(GB)</p>'
        '<p class="trainer">C.フェルラン(FR)</p>'
        '<p class="owner">WERTHEIMER &amp; FRERE</p>'
        '<p class="owner">DERRICK SMITH,ET AL.</p>'
    )
    netkeiba = (
        '<a href="/trainer/result/recent/a031e/" title="バーク">x</a>'
        '<a href="/trainer/result/recent/a064b/" title="フェルラン">y</a>'
        '<a href="/owner/o_wert/" title="ウェルトハイマー">a</a>'
        '<a href="/owner/o_smith/" title="D.スミス">b</a>'
    )
    published = subject.optimize_published_names(jra_html=jra, netkeiba_html=netkeiba)
    assert published.trainers == ("K.バーク(GB)", "C.フェルラン(FR)")
    assert published.owners == ("WERTHEIMER & FRERE", "DERRICK SMITH,ET AL.")
    assert published.trainer_extra_stems == (("バーク",), ("フェルラン",))
    assert published.owner_extra_stems == (("ウェルトハイマー",), ("D.スミス",))


def test_optimize_published_names_omits_extra_when_folded_names_match() -> None:
    published = subject.optimize_published_names(
        jra_html='<p class="trainer">田中 博康(JPN)</p><p class="owner">キャロットファーム</p>',
        netkeiba_html=(
            '<a href="/trainer/result/recent/01162/" title="田中博康">x</a>'
            '<a href="/owner/o_carrot/" title="キャロットファーム">y</a>'
        ),
    )
    assert published.trainers == ("田中 博康(JPN)",)
    assert published.owners == ("キャロットファーム",)
    assert published.trainer_extra_stems == ((),)
    assert published.owner_extra_stems == ((),)


def test_optimize_published_names_pads_uneven_jra_and_netkeiba_slots() -> None:
    jra = '<p class="trainer">K.バーク(GB)</p><p class="trainer">S.ワッテル(FR)</p>'
    netkeiba = '<a href="/trainer/result/recent/a031e/" title="バーク">x</a>'
    published = subject.optimize_published_names(jra_html=jra, netkeiba_html=netkeiba)
    assert published.trainers == ("K.バーク(GB)", "S.ワッテル(FR)")
    assert published.trainer_extra_stems == (("バーク",), ())


def test_optimize_published_names_uses_leftover_netkeiba_slot() -> None:
    jra = '<p class="trainer">K.バーク(GB)</p>'
    netkeiba = (
        '<a href="/trainer/result/recent/a031e/" title="バーク">x</a>'
        '<a href="/trainer/result/recent/a064b/" title="フェルラン">y</a>'
    )
    published = subject.optimize_published_names(jra_html=jra, netkeiba_html=netkeiba)
    assert published.trainers == ("K.バーク(GB)", "フェルラン")
    assert published.trainer_extra_stems == (("バーク",), ())


def test_resolve_archive_file_matches_alias(tmp_path: Path) -> None:
    trainers = tmp_path / "trainers"
    trainers.mkdir()
    path = trainers / "Christophe Ferland.csv"
    path.write_text("年月日,着順\n2026年1月1日,1\n", encoding="utf-8")
    resolved = subject.resolve_archive_file(
        kind="trainer",
        published_name="C.フェルラン(FR)",
        archive_root=tmp_path,
    )
    assert resolved == path


def test_resolve_archive_file_missing_directory_is_none(tmp_path: Path) -> None:
    assert (
        subject.resolve_archive_file(
            kind="owner",
            published_name="誰か",
            archive_root=tmp_path,
        )
        is None
    )


def test_load_archive_person_summary_counts_before_cutoff(tmp_path: Path) -> None:
    path = tmp_path / "person.csv"
    path.write_text(
        "年月日,レース名,着順,賞金(万円)\n"
        "2026年8月16日,本番(GI),1,100\n"
        "2026年7月1日,安田記念(GI),1,200\n"
        "2026年6月1日,条件戦,3,10\n"
        "not-a-date,条件戦,1,1\n",
        encoding="utf-8",
    )
    summary = subject.load_archive_person_summary(path, before=date(2026, 8, 16))
    assert summary.starts == 2
    assert summary.wins == 1
    assert summary.shows == 2
    assert summary.win_rate == 0.5
    assert summary.prize_points == 210.0
    assert summary.prize_per_start == 105.0


def test_load_archive_person_summary_uses_grade_when_prize_missing(tmp_path: Path) -> None:
    path = tmp_path / "person.csv"
    path.write_text(
        "年月日,レース名,着順\n2026年7月1日,条件戦,1\n",
        encoding="utf-8",
    )
    summary = subject.load_archive_person_summary(path, before=date(2026, 8, 16))
    assert summary.starts == 1
    assert summary.prize_points == 8.0


def test_load_archive_person_summary_empty_or_header_only(tmp_path: Path) -> None:
    empty = tmp_path / "empty.csv"
    empty.write_text("", encoding="utf-8")
    header = tmp_path / "header.csv"
    header.write_text("foo,bar\n1,2\n", encoding="utf-8")
    assert subject.load_archive_person_summary(empty, before=date(2026, 8, 16)).starts == 0
    assert subject.load_archive_person_summary(header, before=date(2026, 8, 16)).starts == 0


def test_prefer_richer_person_keeps_larger_sample() -> None:
    thin = PersonSummary(1, 1, 1, 1.0, 1.0, 8.0, 8.0)
    rich = PersonSummary(100, 20, 40, 0.2, 0.4, 800.0, 8.0)
    assert subject.prefer_richer_person(thin, rich) is rich
    assert subject.prefer_richer_person(rich, thin) is rich


def test_load_archive_people_by_umaban_skips_unresolved(tmp_path: Path) -> None:
    trainers = tmp_path / "trainers"
    trainers.mkdir()
    (trainers / "田中博康.csv").write_text(
        "年月日,レース名,着順\n2026年7月1日,条件戦,2\n",
        encoding="utf-8",
    )
    loaded = subject.load_archive_people_by_umaban(
        kind="trainer",
        published_names=("S.ワッテル(FR)", "田中 博康(JPN)"),
        archive_root=tmp_path,
        before=date(2026, 8, 16),
    )
    assert 1 not in loaded
    assert loaded[2].starts == 1
    assert loaded[2].shows == 1


def test_parse_archive_date_rejects_invalid_calendar() -> None:
    assert subject.parse_archive_date("2026年13月40日") is None
    assert subject.parse_archive_date("no-date") is None


def test_load_archive_skips_unparseable_finish_and_prize(tmp_path: Path) -> None:
    path = tmp_path / "person.csv"
    path.write_text(
        "年月日,レース名,着順,賞金(万円)\n"
        "2026年7月1日,条件戦,中止,abc\n"
        "2026年7月2日,条件戦,2,3.5\n",
        encoding="utf-8",
    )
    summary = subject.load_archive_person_summary(path, before=date(2026, 8, 16))
    assert summary.starts == 2
    assert summary.wins == 0
    assert summary.shows == 1
    assert summary.prize_points == 3.74


def test_resolve_archive_file_unknown_name_is_none(tmp_path: Path) -> None:
    trainers = tmp_path / "trainers"
    trainers.mkdir()
    (trainers / "田中博康.csv").write_text("年月日,着順\n2026年1月1日,1\n", encoding="utf-8")
    assert (
        subject.resolve_archive_file(
            kind="trainer",
            published_name="S.ワッテル(FR)",
            archive_root=tmp_path,
        )
        is None
    )


def test_resolve_archive_file_uses_extra_stem_filename(tmp_path: Path) -> None:
    trainers = tmp_path / "trainers"
    trainers.mkdir()
    path = trainers / "Christophe Ferland.csv"
    path.write_text("年月日,着順\n2026年1月1日,1\n", encoding="utf-8")
    resolved = subject.resolve_archive_file(
        kind="trainer",
        published_name="S.ワッテル(FR)",
        archive_root=tmp_path,
        extra_stems=("Christophe Ferland",),
    )
    assert resolved == path


def test_resolve_archive_file_uses_extra_stem_alias(tmp_path: Path) -> None:
    trainers = tmp_path / "trainers"
    trainers.mkdir()
    path = trainers / "K.バーク.csv"
    path.write_text("年月日,着順\n2026年1月1日,1\n", encoding="utf-8")
    resolved = subject.resolve_archive_file(
        kind="trainer",
        published_name="S.ワッテル(FR)",
        archive_root=tmp_path,
        extra_stems=("バーク",),
    )
    assert resolved == path


def test_resolve_archive_file_owner_alias(tmp_path: Path) -> None:
    owners = tmp_path / "owners"
    owners.mkdir()
    path = owners / "Aga Khan Studs SCEA.csv"
    path.write_text("年月日,着順\n2026年1月1日,1\n", encoding="utf-8")
    resolved = subject.resolve_archive_file(
        kind="owner",
        published_name="AGA KHAN STUDS SC",
        archive_root=tmp_path,
    )
    assert resolved == path


def test_resolve_archive_file_owner_netkeiba_short_alias(tmp_path: Path) -> None:
    owners = tmp_path / "owners"
    owners.mkdir()
    path = owners / "Wertheimer & Frere.csv"
    path.write_text("年月日,着順\n2026年1月1日,1\n", encoding="utf-8")
    resolved = subject.resolve_archive_file(
        kind="owner",
        published_name="PH.BETTS",
        archive_root=tmp_path,
        extra_stems=("ウェルトハイマー",),
    )
    assert resolved == path


def test_resolve_archive_file_coolmore_owner_alias(tmp_path: Path) -> None:
    owners = tmp_path / "owners"
    owners.mkdir()
    path = owners / "Westerberg_Mrs J Magnier_M Tabor_D Smith.csv"
    path.write_text("年月日,着順\n2026年1月1日,1\n", encoding="utf-8")
    resolved = subject.resolve_archive_file(
        kind="owner",
        published_name="DERRICK SMITH,ET AL.",
        archive_root=tmp_path,
    )
    assert resolved == path


def test_resolve_archive_file_ignores_empty_extra_stems(tmp_path: Path) -> None:
    trainers = tmp_path / "trainers"
    trainers.mkdir()
    path = trainers / "田中博康.csv"
    path.write_text("年月日,着順\n2026年1月1日,1\n", encoding="utf-8")
    resolved = subject.resolve_archive_file(
        kind="trainer",
        published_name="田中 博康(JPN)",
        archive_root=tmp_path,
        extra_stems=("",),
    )
    assert resolved == path


def test_load_archive_people_by_umaban_uses_extra_stems(tmp_path: Path) -> None:
    trainers = tmp_path / "trainers"
    trainers.mkdir()
    (trainers / "K.バーク.csv").write_text(
        "年月日,レース名,着順\n2026年7月1日,条件戦,1\n",
        encoding="utf-8",
    )
    loaded = subject.load_archive_people_by_umaban(
        kind="trainer",
        published_names=("S.ワッテル(FR)", "K.バーク(GB)"),
        archive_root=tmp_path,
        before=date(2026, 8, 16),
        extra_stems=(("バーク",), ()),
    )
    assert loaded[1].starts == 1
    assert loaded[1].wins == 1
    assert loaded[2].starts == 1
    assert loaded[2].wins == 1


def test_load_archive_people_by_umaban_skips_empty_published(tmp_path: Path) -> None:
    trainers = tmp_path / "trainers"
    trainers.mkdir()
    (trainers / "田中博康.csv").write_text(
        "年月日,レース名,着順\n2026年7月1日,条件戦,2\n",
        encoding="utf-8",
    )
    loaded = subject.load_archive_people_by_umaban(
        kind="trainer",
        published_names=("", "田中 博康(JPN)"),
        archive_root=tmp_path,
        before=date(2026, 8, 16),
    )
    assert 1 not in loaded
    assert loaded[2].starts == 1


def test_parse_netkeiba_owner_empty_when_only_generic_links() -> None:
    html = '<a href="/owner/"><span>馬主</span></a><a href="/owner/race.html?id=o_x" title="近走成績">近走成績</a>'
    assert subject.parse_netkeiba_owner_names(html) == ()


def test_parse_netkeiba_owner_skips_case_insensitive_generic_title() -> None:
    html = '<a href="/owner/o_x/" title="Owner">馬主</a>'
    assert subject.parse_netkeiba_owner_names(html) == ()


def test_resolve_archive_file_carrot_short_alias(tmp_path: Path) -> None:
    owners = tmp_path / "owners"
    owners.mkdir()
    path = owners / "キャロットファーム.csv"
    path.write_text("年月日,着順\n2026年1月1日,1\n", encoding="utf-8")
    resolved = subject.resolve_archive_file(
        kind="owner",
        published_name="キャロット",
        archive_root=tmp_path,
    )
    assert resolved == path


def test_optimize_published_names_jra_only_has_empty_extras() -> None:
    published = subject.optimize_published_names(
        jra_html='<p class="trainer">武井 亮</p><p class="owner">キャロットファーム</p>',
        netkeiba_html="<div></div>",
    )
    assert published.trainers == ("武井 亮",)
    assert published.owners == ("キャロットファーム",)
    assert published.trainer_extra_stems == ((),)
    assert published.owner_extra_stems == ((),)


def test_load_archive_people_by_umaban_allows_shorter_extra_stems(tmp_path: Path) -> None:
    trainers = tmp_path / "trainers"
    trainers.mkdir()
    (trainers / "K.バーク.csv").write_text(
        "年月日,レース名,着順\n2026年7月1日,条件戦,1\n",
        encoding="utf-8",
    )
    (trainers / "田中博康.csv").write_text(
        "年月日,レース名,着順\n2026年7月1日,条件戦,3\n",
        encoding="utf-8",
    )
    loaded = subject.load_archive_people_by_umaban(
        kind="trainer",
        published_names=("S.ワッテル(FR)", "田中 博康"),
        archive_root=tmp_path,
        before=date(2026, 8, 16),
        extra_stems=(("バーク",),),
    )
    assert loaded[1].starts == 1
    assert loaded[1].wins == 1
    assert loaded[2].starts == 1
    assert loaded[2].shows == 1


def test_parse_jra_card_json_uses_horse_number_order() -> None:
    published = subject.parse_jra_card_json(
        {
            "runners": [
                {"horse_number": 2, "trainer": "S.ワッテル (FR)", "owner": "PH.BETTS"},
                {"horse_number": 1, "trainer": "K.バーク (GB)", "owner": "EXORS OF THE LATE S M OBAID"},
            ]
        }
    )
    assert published.trainers == ("K.バーク (GB)", "S.ワッテル (FR)")
    assert published.owners == ("EXORS OF THE LATE S M OBAID", "PH.BETTS")
    assert published.trainer_extra_stems == ((), ())
    assert published.owner_extra_stems == ((), ())


def test_parse_jra_card_json_fills_missing_horse_numbers() -> None:
    published = subject.parse_jra_card_json(
        {
            "runners": [
                {"horse_number": "3", "trainer": "田中 博康 (JPN)", "owner": "キャロットファーム"},
            ]
        }
    )
    assert published.trainers == ("", "", "田中 博康 (JPN)")
    assert published.owners == ("", "", "キャロットファーム")


def test_parse_jra_card_json_rejects_invalid_payloads() -> None:
    assert subject.parse_jra_card_json("nope").trainers == ()
    assert subject.parse_jra_card_json({"runners": "nope"}).owners == ()
    assert subject.parse_jra_card_json({"runners": [{"horse_number": True}]}).trainers == ()
    assert subject.parse_jra_card_json({"runners": [{"horse_number": 0, "trainer": "x"}]}).owners == ()
    assert subject.parse_jra_card_json({"runners": [None, {"horse_number": "x"}]}).trainers == ()


def test_parse_netkeiba_horse_page_owner_reads_title() -> None:
    html = (
        '<a href="/owner/top.html" title="馬主">馬主</a>'
        '<a href="/owner/486800/" title="キャロットファーム">キャロットファーム</a>'
    )
    assert subject.parse_netkeiba_horse_page_owner(html) == "キャロットファーム"


def test_parse_netkeiba_horse_page_owner_empty_when_missing() -> None:
    assert subject.parse_netkeiba_horse_page_owner("<div>no owner</div>") == ""


def test_load_netkeiba_horse_page_owners_maps_stem(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    (tmp_path / "2021105744.html").write_text(
        '<a href="/owner/486800/" title="キャロットファーム">x</a>',
        encoding="utf-8",
    )
    (tmp_path / "000a0006d.html").write_text(
        '<a href="/owner/a0006d/" title="Wertheimer &amp; Frere">x</a>',
        encoding="utf-8",
    )
    (tmp_path / "notes.txt").write_text("ignore", encoding="utf-8")
    (tmp_path / "empty.html").write_text('<a href="/owner/top.html" title="馬主">馬主</a>', encoding="utf-8")
    loaded = subject.load_netkeiba_horse_page_owners(tmp_path)
    assert loaded == {
        "2021105744": "キャロットファーム",
        "000a0006d": "Wertheimer & Frere",
    }
    assert subject.load_netkeiba_horse_page_owners(missing) == {}


def test_attach_owner_stems_from_horse_pages_adds_alias() -> None:
    published = subject.PublishedPersonNames(
        trainers=("A.オブライエン (IRE)",),
        owners=("DERRICK SMITH,ET AL.",),
        trainer_extra_stems=((),),
        owner_extra_stems=((),),
    )
    attached = subject.attach_owner_stems_from_horse_pages(
        published,
        owner_by_umaban={1: "Westerberg Mrs J Magnier M Tabor D Smith"},
    )
    assert attached.owners == ("DERRICK SMITH,ET AL.",)
    assert attached.owner_extra_stems == (("Westerberg Mrs J Magnier M Tabor D Smith",),)


def test_attach_owner_stems_from_horse_pages_skips_same_fold_and_invalid_umaban() -> None:
    published = subject.PublishedPersonNames(
        trainers=(),
        owners=("キャロットファーム",),
        trainer_extra_stems=(),
        owner_extra_stems=((),),
    )
    attached = subject.attach_owner_stems_from_horse_pages(
        published,
        owner_by_umaban={0: "ignore", 1: "キャロット ファーム"},
    )
    assert attached.owners == ("キャロットファーム",)
    assert attached.owner_extra_stems == ((),)


def test_attach_owner_stems_from_horse_pages_fills_blank_slot() -> None:
    published = subject.PublishedPersonNames(trainers=(), owners=())
    attached = subject.attach_owner_stems_from_horse_pages(
        published,
        owner_by_umaban={2: "P H Betts"},
    )
    assert attached.owners == ("", "P H Betts")
    assert attached.owner_extra_stems == ((), ())


def test_resolve_archive_file_netkeiba_coolmore_owner_alias(tmp_path: Path) -> None:
    owners = tmp_path / "owners"
    owners.mkdir()
    path = owners / "Michael Tabor & Derrick Smith & Mrs John Magnier.csv"
    path.write_text("年月日,着順\n2026年1月1日,1\n", encoding="utf-8")
    resolved = subject.resolve_archive_file(
        kind="owner",
        published_name="DERRICK SMITH,ET AL.",
        archive_root=tmp_path,
        extra_stems=("Westerberg Mrs J Magnier M Tabor D Smith",),
    )
    assert resolved == path


def test_read_html_text_decodes_utf8(tmp_path: Path) -> None:
    path = tmp_path / "page.html"
    path.write_text("キャロットファーム", encoding="utf-8")
    assert subject.read_html_text(path) == "キャロットファーム"


def test_parse_netkeiba_result_table_reads_finish_and_prize() -> None:
    html = """
    <table summary="年度別成績" class="nk_tb_common race_table_01">
    <tr><th>日付</th><th>レース名</th><th>着<br />順</th><th>賞金<br />(万円)</th></tr>
    <tr><td>2026/07/11</td><td>サマーマイル(GII)</td><td>1</td><td>12.5</td></tr>
    <tr><td>2026/06/16</td><td>クイーンアン(GI)</td><td>4</td><td></td></tr>
    <tr><td>not-a-date</td><td>x</td><td>1</td><td>1</td></tr>
    </table>
    """
    starts = subject.parse_netkeiba_result_table(html)
    assert starts[0].raced == date(2026, 7, 11)
    assert starts[0].race_name == "サマーマイル(GII)"
    assert starts[0].finish == 1
    assert starts[0].prize_man_yen == 12.5
    assert starts[1].raced == date(2026, 6, 16)
    assert starts[1].finish == 4
    assert starts[1].prize_man_yen is None
    assert len(starts) == 2


def test_parse_netkeiba_result_table_empty_without_marker() -> None:
    assert subject.parse_netkeiba_result_table("<table><tr><td>x</td></tr></table>") == ()


def test_parse_netkeiba_result_table_skips_short_row_and_unclosed_table() -> None:
    unclosed = '<table summary="年度別成績"><tr><th>日付</th><th>着順</th></tr><tr><td>2026/07/11</td>'
    assert subject.parse_netkeiba_result_table(unclosed) == ()
    short_row = """
    <table summary="年度別成績">
    <tr><th>日付</th><th>着順</th></tr>
    <tr><td>only-date</td></tr>
    </table>
    """
    assert subject.parse_netkeiba_result_table(short_row) == ()
    no_headers = '<table summary="年度別成績"><tr><td>2026/07/11</td></tr></table>'
    assert subject.parse_netkeiba_result_table(no_headers) == ()


def test_parse_jravan_horse_results_reads_official_table() -> None:
    html = """
    <div id="horse--result" class="horsedata">
    <table>
    <tr><th>開催日</th><th>レース名</th><th>着順</th></tr>
    <tr><td><span>2026/</span><span>07/11</span></td><td>サマーマイルステークス（G2）</td><td>1</td></tr>
    <tr><td><span>2026/</span><span>06/16</span></td><td>クイーンアンステークス（G1）</td><td>4</td></tr>
    </table>
    </div>
    <div id="horse--long"></div>
    """
    starts = subject.parse_jravan_horse_results(html)
    assert starts[0].raced == date(2026, 7, 11)
    assert starts[0].race_name == "サマーマイルステークス（G2）"
    assert starts[0].finish == 1
    assert starts[1].finish == 4


def test_parse_jravan_horse_results_empty_without_section() -> None:
    assert subject.parse_jravan_horse_results("<div>no results</div>") == ()


def test_parse_jravan_horse_results_skips_bad_rows() -> None:
    html = """
    <div id="horse--result">
    <table>
    <tr><th>場所</th><th>レース名</th></tr>
    <tr><td>アスコット</td><td>x</td></tr>
    </table>
    </div>
    <div id="horse--long"></div>
    """
    assert subject.parse_jravan_horse_results(html) == ()
    short = """
    <div id="horse--result">
    <table>
    <tr><th>開催日</th><th>着順</th></tr>
    <tr><td>2026/07/11</td></tr>
    <tr><td>bad-date</td><td>1</td></tr>
    </table>
    </div>
    <div id="horse--long"></div>
    """
    assert subject.parse_jravan_horse_results(short) == ()


def test_parse_jra_card_past_runs_for_umaban() -> None:
    payload = {
        "runners": [
            {
                "horse_number": 1,
                "past_runs": [
                    {
                        "date": "2026年7月11日",
                        "race_name": "サマーマイル",
                        "grade": "GⅡ",
                        "finish_position": 1,
                    },
                    {"date": "bad", "race_name": "x", "finish_position": 2},
                ],
            }
        ]
    }
    starts = subject.parse_jra_card_past_runs(payload, umaban=1)
    assert starts[0].raced == date(2026, 7, 11)
    assert starts[0].race_name == "サマーマイル(GⅡ)"
    assert starts[0].finish == 1
    assert subject.parse_jra_card_past_runs(payload, umaban=2) == ()
    assert subject.parse_jra_card_past_runs("nope", umaban=1) == ()
    assert subject.parse_jra_card_past_runs({"runners": "x"}, umaban=1) == ()
    assert subject.parse_jra_card_past_runs({"runners": [None, {"horse_number": 1}]}, umaban=1) == ()
    assert subject.parse_jra_card_past_runs(
        {"runners": [{"horse_number": 1, "past_runs": "x"}]},
        umaban=1,
    ) == ()
    assert subject.parse_jra_card_past_runs(
        {"runners": [{"horse_number": 1, "past_runs": [None, {"date": "2026年7月1日", "finish_position": "2"}]}]},
        umaban=1,
    )[0].finish == 2


def test_merge_archive_starts_keeps_same_race_different_finish() -> None:
    first = subject.ArchiveStart(date(2026, 7, 11), "サマーマイル", 1, None)
    second = subject.ArchiveStart(date(2026, 7, 11), "サマーマイル", 4, None)
    duplicate = subject.ArchiveStart(date(2026, 7, 11), "サマーマイル", 1, None)
    merged = subject.merge_archive_starts((first,), (second, duplicate))
    assert merged == (first, second)


def test_write_archive_csv_round_trips(tmp_path: Path) -> None:
    path = tmp_path / "owners" / "PH.BETTS.csv"
    subject.write_archive_csv(
        path,
        (
            subject.ArchiveStart(date(2026, 3, 29), "Eブラン賞(GⅢ)", 1, None),
            subject.ArchiveStart(date(2025, 8, 30), "カンセー賞", None, 3.5),
        ),
    )
    summary = subject.load_archive_person_summary(path, before=date(2026, 8, 16))
    assert summary.starts == 2
    assert summary.wins == 1
    assert summary.shows == 1


def test_resolve_archive_file_new_missing_person_aliases(tmp_path: Path) -> None:
    trainers = tmp_path / "trainers"
    trainers.mkdir()
    wattel = trainers / "S.ワッテル.csv"
    wattel.write_text("年月日,着順\n2026年1月1日,1\n", encoding="utf-8")
    resolved = subject.resolve_archive_file(
        kind="trainer",
        published_name="S.ワッテル (FR)",
        archive_root=tmp_path,
    )
    assert resolved == wattel


def test_resolve_archive_file_haggas_and_obaid_aliases(tmp_path: Path) -> None:
    trainers = tmp_path / "trainers"
    trainers.mkdir()
    owners = tmp_path / "owners"
    owners.mkdir()
    haggas = trainers / "W.ハガス.csv"
    haggas.write_text("年月日,着順\n2026年1月1日,1\n", encoding="utf-8")
    obaid = owners / "EXORS OF THE LATE S M OBAID.csv"
    obaid.write_text("年月日,着順\n2026年1月1日,2\n", encoding="utf-8")
    assert (
        subject.resolve_archive_file(
            kind="trainer",
            published_name="W.ハガス (GB)",
            archive_root=tmp_path,
        )
        == haggas
    )
    assert (
        subject.resolve_archive_file(
            kind="owner",
            published_name="EXORS OF THE LATE S M OBAID",
            archive_root=tmp_path,
        )
        == obaid
    )


def test_fetch_url_text_decodes_utf8(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_urlopen(request: object, timeout: object) -> _FakeUrlResponse:
        return _FakeUrlResponse("キャロットファーム".encode("utf-8"))

    monkeypatch.setattr(subject, "urlopen", fake_urlopen)
    assert subject.fetch_url_text("https://example.test/page") == "キャロットファーム"


def test_fetch_url_text_wraps_url_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_urlopen(request: object, timeout: object) -> _FakeUrlResponse:
        raise URLError("down")

    monkeypatch.setattr(subject, "urlopen", fake_urlopen)
    try:
        subject.fetch_url_text("https://example.test/page")
    except URLError as exc:
        assert "Failed to fetch https://example.test/page" in str(exc)
    else:
        raise AssertionError("expected URLError")


def test_read_html_source_reads_file_and_missing(tmp_path: Path) -> None:
    path = tmp_path / "card.html"
    path.write_text("<p class='trainer'>田中</p>", encoding="utf-8")
    assert subject.read_html_source(str(path)) == "<p class='trainer'>田中</p>"
    assert subject.read_html_source(str(tmp_path / "missing.html")) == ""


def test_read_html_source_fetches_http(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_urlopen(request: object, timeout: object) -> _FakeUrlResponse:
        return _FakeUrlResponse("<p>ok</p>".encode("utf-8"))

    monkeypatch.setattr(subject, "urlopen", fake_urlopen)
    assert subject.read_html_source("https://example.test/jra") == "<p>ok</p>"


def test_extract_jra_shutuba_url_from_relative_and_absolute() -> None:
    relative = '<a href="/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73">出馬表</a>'
    assert (
        subject.extract_jra_shutuba_url(relative)
        == "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73"
    )
    absolute = (
        '<a href="https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73">'
        "出馬表</a>"
    )
    assert (
        subject.extract_jra_shutuba_url(absolute)
        == "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73"
    )
    assert subject.extract_jra_shutuba_url("<div></div>") == ""
    bare = '<a href="JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73">出馬表</a>'
    assert (
        subject.extract_jra_shutuba_url(bare)
        == "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73"
    )


def test_fetch_netkeiba_horse_owner_empty_id_is_empty() -> None:
    assert subject.fetch_netkeiba_horse_owner("") == ""


def test_fetch_netkeiba_horse_owner_reads_title(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_urlopen(request: object, timeout: object) -> _FakeUrlResponse:
        html = '<a href="/owner/486800/" title="キャロットファーム">x</a>'
        return _FakeUrlResponse(html.encode("utf-8"))

    monkeypatch.setattr(subject, "urlopen", fake_urlopen)
    assert subject.fetch_netkeiba_horse_owner("2021105744") == "キャロットファーム"


def test_planned_overseas_card_resolves_international_stakes_aliases() -> None:
    card = subject.planned_overseas_card("international-stakes")
    assert card is not None
    assert card.slug == "international-stakes-2026"
    assert card.kaisai_tsukihi == "0819"
    assert card.keibajo_code == "A6"
    assert card.race_bango == "04"
    assert card.venue_name == "ヨーク"
    assert subject.planned_overseas_card("インターナショナルS") is not None
    assert subject.planned_overseas_card("") is None
    assert subject.planned_overseas_card("unknown-card") is None


def test_parse_jra_overseas_sale_list_reads_date_and_page() -> None:
    html = """
    <tr><td>2026年8月16日（日曜）</td>
    <td><a href="/keiba/overseas/race/2026jlm/index.html">ジャックルマロワ賞（G1）</a></td></tr>
    <tr><td>no-date</td><td><a href="/keiba/overseas/race/x/">x</a></td></tr>
    """
    listings = subject.parse_jra_overseas_sale_list(html)
    assert listings == (
        ("0816", "ジャックルマロワ賞（G1）", "https://www.jra.go.jp/keiba/overseas/race/2026jlm/index.html"),
    )


def test_prepare_overseas_card_reports_missing_sale_and_extracts_shutuba() -> None:
    card = subject.planned_overseas_card("international-stakes-2026")
    assert card is not None
    missing = subject.prepare_overseas_card(card, sale_list_html="<table></table>", race_page_html="")
    assert missing.on_sale_list is False
    assert missing.jra_card_url == ""
    assert missing.netkeiba_card_url == ""
    assert missing.jravan_card_url == ""
    assert missing.next_command == (
        "uv run python src/scripts/train_and_predict_finish_position_overseas.py "
        "--predict-only --card international-stakes-2026"
    )
    listed = subject.prepare_overseas_card(
        card,
        sale_list_html=(
            "<tr><td>2026年8月19日（水曜）</td>"
            '<td><a href="/keiba/overseas/race/2026intl/index.html">インターナショナルステークス（G1）</a></td></tr>'
        ),
        race_page_html='<a href="/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73">出馬表</a>',
    )
    assert listed.on_sale_list is True
    assert (
        listed.jra_card_url
        == "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73"
    )
    assert listed.next_command == (
        "uv run python src/scripts/train_and_predict_finish_position_overseas.py "
        "--predict-only --card international-stakes-2026 "
        "--jra-url https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73"
    )


def test_sale_list_match_ignores_other_dates() -> None:
    card = subject.planned_overseas_card("international-stakes-2026")
    assert card is not None
    assert (
        subject.sale_list_match(
            card,
            (("0816", "インターナショナルステークス", "https://example.test/wrong"),),
        )
        == ""
    )


def test_planned_card_jra_url_uses_shutuba_when_present() -> None:
    card = subject.planned_overseas_card("jacques-le-marois-2026")
    assert card is not None
    assert (
        subject.planned_card_jra_url(card)
        == "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73"
    )


def test_jacques_le_marois_catalog_has_three_source_urls() -> None:
    card = subject.planned_overseas_card("jacques-le-marois-2026")
    assert card is not None
    assert card.jra_card_url == (
        "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73"
    )
    assert card.netkeiba_card_url == (
        "https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C4010104&rf=race_submenu"
    )
    assert card.jravan_card_url == "https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/"


def test_prepare_jacques_le_marois_includes_source_urls() -> None:
    card = subject.planned_overseas_card("jacques-le-marois-2026")
    assert card is not None
    status = subject.prepare_overseas_card(card, sale_list_html="<table></table>", race_page_html="")
    assert status.jra_card_url == (
        "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73"
    )
    assert status.netkeiba_card_url == (
        "https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C4010104&rf=race_submenu"
    )
    assert status.jravan_card_url == "https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/"
    assert status.next_command == (
        "uv run python src/scripts/train_and_predict_finish_position_overseas.py "
        "--predict-only --card jacques-le-marois-2026 "
        "--jra-url https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73 "
        "--netkeiba-url https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C4010104&rf=race_submenu "
        "--jravan-url https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/"
    )


def test_apply_source_urls_overrides_empty_catalog() -> None:
    card = subject.planned_overseas_card("international-stakes-2026")
    assert card is not None
    updated = subject.apply_source_urls(
        card,
        subject.OverseasSourceUrls(
            jra_url="https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde09999",
            netkeiba_url="https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C6010104",
            jravan_url="https://world.jra-van.jp/race/internationalstakes/2026/racecard/",
        ),
    )
    status = subject.prepare_overseas_card(
        updated, sale_list_html="<table></table>", race_page_html=""
    )
    assert status.jra_card_url == "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde09999"
    assert (
        status.netkeiba_card_url
        == "https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C6010104"
    )
    assert status.jravan_card_url == (
        "https://world.jra-van.jp/race/internationalstakes/2026/racecard/"
    )
    assert status.next_command == (
        "uv run python src/scripts/train_and_predict_finish_position_overseas.py "
        "--predict-only --card international-stakes-2026 "
        "--jra-url https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde09999 "
        "--netkeiba-url https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C6010104 "
        "--jravan-url https://world.jra-van.jp/race/internationalstakes/2026/racecard/"
    )


def test_apply_source_urls_keeps_catalog_when_cli_blank() -> None:
    card = subject.planned_overseas_card("jacques-le-marois-2026")
    assert card is not None
    updated = subject.apply_source_urls(card, subject.OverseasSourceUrls())
    assert updated.jra_card_url == (
        "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73"
    )
    assert updated.netkeiba_card_url == (
        "https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C4010104&rf=race_submenu"
    )
    assert updated.jravan_card_url == "https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/"


def test_normalize_source_url_strips_tracking_query_and_fragment() -> None:
    assert subject.normalize_source_url(
        "https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C4010104&rf=race_submenu#top"
    ) == "https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C4010104"


def test_normalize_source_url_keeps_jra_cname_and_jravan_slash() -> None:
    assert subject.normalize_source_url(
        "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73"
    ) == "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73"
    assert subject.normalize_source_url(
        "https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/"
    ) == "https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/"


def test_normalize_source_url_empty_and_host_case() -> None:
    assert subject.normalize_source_url("  ") == ""
    assert subject.normalize_source_url(
        "HTTPS://World.JRA-VAN.jp/race/jacqueslemarois/2026/racecard/"
    ) == "https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/"


def test_source_url_combo_key_same_for_tracking_query() -> None:
    with_rf = subject.OverseasSourceUrls(
        jra_url="https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73",
        netkeiba_url=(
            "https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C4010104&rf=race_submenu"
        ),
        jravan_url="https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/",
    )
    without_rf = subject.OverseasSourceUrls(
        jra_url="https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73",
        netkeiba_url="https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C4010104",
        jravan_url="https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/",
    )
    other = subject.OverseasSourceUrls(
        jra_url="https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde09999",
        netkeiba_url="https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C4010104",
        jravan_url="https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/",
    )
    assert subject.source_url_combo_key(with_rf) == subject.source_url_combo_key(without_rf)
    assert subject.source_url_combo_key(with_rf) != subject.source_url_combo_key(other)


def test_prefer_existing_source_url_accepts_new_identity() -> None:
    assert subject.prefer_existing_source_url(
        "",
        "https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/",
    ) == "https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/"
    assert subject.prefer_existing_source_url(
        "https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/",
        "https://world.jra-van.jp/race/internationalstakes/2026/racecard/",
    ) == "https://world.jra-van.jp/race/internationalstakes/2026/racecard/"


def test_source_urls_from_card_reads_catalog() -> None:
    card = subject.planned_overseas_card("jacques-le-marois-2026")
    assert card is not None
    urls = subject.source_urls_from_card(card)
    assert urls.jra_url == (
        "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73"
    )
    assert urls.netkeiba_url == (
        "https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C4010104&rf=race_submenu"
    )
    assert urls.jravan_url == "https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/"


def test_apply_source_urls_treats_equivalent_netkeiba_as_same() -> None:
    card = subject.planned_overseas_card("jacques-le-marois-2026")
    assert card is not None
    updated = subject.apply_source_urls(
        card,
        subject.OverseasSourceUrls(
            netkeiba_url="https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C4010104",
        ),
    )
    assert updated.netkeiba_card_url == (
        "https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C4010104&rf=race_submenu"
    )
    again = subject.apply_source_urls(
        updated,
        subject.OverseasSourceUrls(
            jra_url="https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73",
            netkeiba_url=(
                "https://race.netkeiba.com/race/shutuba_abroad.html"
                "?race_id=2026C4010104&rf=race_submenu"
            ),
            jravan_url="https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/",
        ),
    )
    assert again == updated


def test_fetch_url_text_reuses_cache_for_same_normalized_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    def fake_urlopen(request: object, timeout: object) -> _FakeUrlResponse:
        calls.append("hit")
        return _FakeUrlResponse(b"<p>cached</p>")

    monkeypatch.setattr(subject, "urlopen", fake_urlopen)
    first = subject.fetch_url_text(
        "https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C4010104&rf=race_submenu",
        cache_dir=tmp_path,
    )
    second = subject.fetch_url_text(
        "https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C4010104",
        cache_dir=tmp_path,
    )
    assert first == "<p>cached</p>"
    assert second == "<p>cached</p>"
    assert calls == ["hit"]


def test_write_cached_url_text_and_combo_are_idempotent(tmp_path: Path) -> None:
    url = "https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/"
    assert subject.write_cached_url_text(tmp_path, url, "<p>k</p>") is True
    assert subject.write_cached_url_text(tmp_path, url, "<p>k</p>") is False
    urls = subject.OverseasSourceUrls(
        jra_url="https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73",
        netkeiba_url="https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C4010104",
        jravan_url=url,
    )
    assert subject.remember_source_url_combo(tmp_path, urls) is True
    assert subject.remember_source_url_combo(
        tmp_path,
        subject.OverseasSourceUrls(
            jra_url="https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73",
            netkeiba_url=(
                "https://race.netkeiba.com/race/shutuba_abroad.html"
                "?race_id=2026C4010104&rf=race_submenu"
            ),
            jravan_url=url,
        ),
    ) is False


def test_read_html_source_uses_url_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    def fake_urlopen(request: object, timeout: object) -> _FakeUrlResponse:
        calls.append("hit")
        return _FakeUrlResponse(b"<p>jra</p>")

    monkeypatch.setattr(subject, "urlopen", fake_urlopen)
    first = subject.read_html_source(
        "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73",
        cache_dir=tmp_path,
    )
    second = subject.read_html_source(
        "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73",
        cache_dir=tmp_path,
    )
    assert first == "<p>jra</p>"
    assert second == "<p>jra</p>"
    assert calls == ["hit"]


def test_fetch_netkeiba_horse_owner_reuses_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    def fake_urlopen(request: object, timeout: object) -> _FakeUrlResponse:
        calls.append("hit")
        html = '<a href="/owner/486800/" title="キャロットファーム">x</a>'
        return _FakeUrlResponse(html.encode("utf-8"))

    monkeypatch.setattr(subject, "urlopen", fake_urlopen)
    first = subject.fetch_netkeiba_horse_owner("2021105744", cache_dir=tmp_path)
    second = subject.fetch_netkeiba_horse_owner("2021105744", cache_dir=tmp_path)
    assert first == "キャロットファーム"
    assert second == "キャロットファーム"
    assert calls == ["hit"]


def test_write_archive_csv_skips_identical_rewrite(tmp_path: Path) -> None:
    path = tmp_path / "owners" / "PH.BETTS.csv"
    starts = (
        subject.ArchiveStart(date(2026, 3, 29), "Eブラン賞(GⅢ)", 1, None),
        subject.ArchiveStart(date(2025, 8, 30), "カンセー賞", None, 3.5),
    )
    assert subject.write_archive_csv(path, starts) is True
    assert subject.write_archive_csv(path, starts) is False


def test_parse_jravan_racecard_trainers() -> None:
    html = """
    <span class="raceTable__details__line__item--horse__info">K．バーク<br />
    父Dubawi</span>
    <span class="raceTable__details__line__item--horse__info">S．ワッテル<br />
    父Siyouni</span>
    """
    assert subject.parse_jravan_racecard_trainers(html) == ("K．バーク", "S．ワッテル")


def test_parse_jravan_racecard_trainers_empty() -> None:
    assert subject.parse_jravan_racecard_trainers("<div>no table</div>") == ()


def test_attach_trainer_stems_from_jravan_fills_blank_and_pads() -> None:
    published = subject.PublishedPersonNames(trainers=(), owners=())
    attached = subject.attach_trainer_stems_from_jravan(published, ("K．バーク", "S．ワッテル"))
    assert attached.trainers == ("K．バーク", "S．ワッテル")
    assert attached.trainer_extra_stems == ((), ())
    assert attached.owners == ()


def test_attach_trainer_stems_from_jravan_adds_alias() -> None:
    published = subject.PublishedPersonNames(
        trainers=("K.バーク (GB)",),
        owners=("PH.BETTS",),
        trainer_extra_stems=((),),
        owner_extra_stems=((),),
    )
    attached = subject.attach_trainer_stems_from_jravan(published, ("Karl Richard Burke",))
    assert attached.trainers == ("K.バーク (GB)",)
    assert attached.trainer_extra_stems == (("Karl Richard Burke",),)
    assert attached.owners == ("PH.BETTS",)


def test_attach_trainer_stems_from_jravan_skips_same_fold_and_blank() -> None:
    published = subject.PublishedPersonNames(
        trainers=("K.バーク (GB)", "田中 博康"),
        owners=(),
        trainer_extra_stems=((), ()),
    )
    attached = subject.attach_trainer_stems_from_jravan(published, ("K．バーク", ""))
    assert attached.trainers == ("K.バーク (GB)", "田中 博康")
    assert attached.trainer_extra_stems == ((), ())


def test_attach_trainer_stems_from_jravan_empty_keeps_published() -> None:
    published = subject.PublishedPersonNames(trainers=("武井 亮",), owners=())
    attached = subject.attach_trainer_stems_from_jravan(published, ())
    assert attached.trainers == ("武井 亮",)
    assert attached.trainer_extra_stems == ()


def test_attach_trainer_stems_from_jravan_pads_short_extras() -> None:
    published = subject.PublishedPersonNames(trainers=("武井 亮",), owners=())
    attached = subject.attach_trainer_stems_from_jravan(published, ("Takei Ryo",))
    assert attached.trainers == ("武井 亮",)
    assert attached.trainer_extra_stems == (("Takei Ryo",),)


def test_planned_card_jra_url_prefers_card_then_race_page() -> None:
    card = subject.PlannedOverseasCard(
        slug="x",
        kaisai_nen="2026",
        kaisai_tsukihi="0819",
        keibajo_code="A6",
        race_bango="04",
        race_name="インターナショナルステークス",
        venue_name="ヨーク",
        jra_race_page_url="https://www.jra.go.jp/keiba/overseas/race/2026intl/index.html",
        jra_card_url="",
    )
    assert subject.planned_card_jra_url(card) == (
        "https://www.jra.go.jp/keiba/overseas/race/2026intl/index.html"
    )


def test_resolve_archive_file_takei_exact_fold(tmp_path: Path) -> None:
    trainers = tmp_path / "trainers"
    trainers.mkdir()
    path = trainers / "武井亮.csv"
    path.write_text("年月日,着順\n2026年1月1日,1\n", encoding="utf-8")
    resolved = subject.resolve_archive_file(
        kind="trainer",
        published_name="武井 亮",
        archive_root=tmp_path,
    )
    assert resolved == path
