from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

MODULE_PATH = Path(__file__).with_name("keiba_go_scrape_import.py")
SPEC = importlib.util.spec_from_file_location("keiba_go_scrape_import", MODULE_PATH)
assert SPEC is not None
assert SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def entry_html(extra: str) -> str:
    return f"""
    <table><tr class="tBorder">
      <td class="courseNum course_01">1</td>
      <td class="horseNum">1</td>
      <td><a class="horseName">テストホース</a></td>
      {extra}
    </tr></table>
    """


class ParseTodayVenuesTest(unittest.TestCase):
    def test_discovers_exact_date_venues_and_sorted_races(self) -> None:
        html = """
        <a href="/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2F09%2F01&amp;k_babaCode=36">
          門別 <img alt="night">
        </a>
        <button onclick="location.href='/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F09%2F01&amp;k_babaCode=36&amp;k_raceNo=12'">12R</button>
        <button onclick="location.href='/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F09%2F01&amp;k_babaCode=36&amp;k_raceNo=1'">1R</button>
        <a href="/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2F09%2F02&amp;k_babaCode=20">大井</a>
        <button onclick="location.href='/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F09%2F02&amp;k_babaCode=20&amp;k_raceNo=1'">1R</button>
        """

        venues = MODULE.parse_today_venues(html, "20260901")

        self.assertEqual(
            venues,
            [MODULE.VenueDiscovery(baba="36", label="門別", race_numbers=("1", "12"))],
        )

    def test_rejects_venue_without_races(self) -> None:
        html = """
        <a href="/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2F09%2F01&amp;k_babaCode=36">門別</a>
        """

        with self.assertRaisesRegex(
            SystemExit,
            "TodayRaceInfoTop listed babaCode=36 without races for date=20260901",
        ):
            MODULE.parse_today_venues(html, "20260901")

    def test_rejects_date_without_venues(self) -> None:
        with self.assertRaisesRegex(
            SystemExit, "TodayRaceInfoTop listed no venues for date=20260901"
        ):
            MODULE.parse_today_venues("<html></html>", "20260901")

    def test_selects_requested_venue_subset(self) -> None:
        discoveries = [
            MODULE.VenueDiscovery(baba="36", label="門別", race_numbers=("1",)),
            MODULE.VenueDiscovery(baba="10", label="盛岡", race_numbers=("1",)),
        ]

        selected = MODULE.select_venue_discoveries(discoveries, ["10"])

        self.assertEqual(
            selected,
            [MODULE.VenueDiscovery(baba="10", label="盛岡", race_numbers=("1",))],
        )

    def test_rejects_requested_venue_not_on_top_page(self) -> None:
        discoveries = [
            MODULE.VenueDiscovery(baba="36", label="門別", race_numbers=("1",))
        ]

        with self.assertRaisesRegex(
            SystemExit, "babaCode=20 is not listed on TodayRaceInfoTop"
        ):
            MODULE.select_venue_discoveries(discoveries, ["20"])

    def test_keiba_go_urls_follow_official_naming_rules(self) -> None:
        self.assertEqual(
            MODULE.today_top_url("20260901"),
            "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/TodayRaceInfoTop?k_raceDate=2026%2F09%2F01",
        )
        self.assertEqual(
            MODULE.race_list_url("20260901", "36"),
            "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2F09%2F01&k_babaCode=36",
        )
        self.assertEqual(
            MODULE.deba_table_url("20260901", "36", "01"),
            "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F09%2F01&k_raceNo=1&k_babaCode=36",
        )
        self.assertEqual(
            MODULE.rider_profile_url("31337"),
            "https://www.keiba.go.jp/KeibaWeb/DataRoom/RiderMark?k_riderLicenseNo=31337",
        )
        self.assertEqual(
            MODULE.trainer_profile_url("11145"),
            "https://www.keiba.go.jp/KeibaWeb/DataRoom/TrainerMark?k_trainerLicenseNo=11145",
        )


class ParseDebaEntriesTest(unittest.TestCase):
    def test_reads_exact_entity_identifiers_and_current_owner(self) -> None:
        html = """
        <table><tr class="tBorder">
          <td class="courseNum course_01">1</td>
          <td class="horseNum">1</td>
          <td colspan="3"><a class="horseName" href="../DataRoom/HorseMarkInfo?k_lineageLoginCode=30095400817">ヴィルボニータ</a></td>
          <td colspan="1"><a class="jockeyName" href="../DataRoom/RiderMark?k_riderLicenseNo=31337">藤田駕（北海道）</a></td>
          <td colspan="3">チュウワウィザード</td>
          <td colspan="1"><a href="../DataRoom/TrainerMark?k_trainerLicenseNo=11145">柳澤好（北海道）</a></td>
          <td colspan="3">ベルフロレゾン</td>
          <td colspan="1">廣田優生</td>
          <td><a href="../TodayRaceInfo/RaceMarkTable?k_raceDate=2026%2F08%2F20&amp;k_raceNo=10&amp;k_babaCode=36">前走</a></td>
        </tr></table>
        """

        entries = MODULE.parse_deba_entries(html)

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].lineage_login_code, "30095400817")
        self.assertEqual(entries[0].rider_license_no, "31337")
        self.assertEqual(entries[0].trainer_license_no, "11145")
        self.assertEqual(entries[0].trainer, "柳澤好（北海道）")
        self.assertEqual(entries[0].owner, "廣田優生")
        self.assertEqual(
            entries[0].past_races,
            (MODULE.PastRaceKey(ymd="20260820", baba="36", race_number="10"),),
        )

    def test_ignores_historical_exclusion(self) -> None:
        entries = MODULE.parse_deba_entries(
            entry_html('<div class="raceInfo"><span class="pastRank">除外</span></div>')
        )

        self.assertEqual(len(entries), 1)
        self.assertIsNone(entries[0].status)

    def test_reads_current_info_status(self) -> None:
        entries = MODULE.parse_deba_entries(
            entry_html('<td class="info">競走除外</td>')
        )

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].status, "競走除外")


class WakubanCalculationTest(unittest.TestCase):
    def test_calculates_frames_from_field_size_and_horse_number(self) -> None:
        expected_by_field_size = {
            6: [1, 2, 3, 4, 5, 6],
            8: [1, 2, 3, 4, 5, 6, 7, 8],
            9: [1, 2, 3, 4, 5, 6, 7, 8, 8],
            10: [1, 2, 3, 4, 5, 6, 7, 7, 8, 8],
            12: [1, 2, 3, 4, 5, 5, 6, 6, 7, 7, 8, 8],
            15: [1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8],
            16: [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8],
        }
        for horse_count, expected in expected_by_field_size.items():
            with self.subTest(horse_count=horse_count):
                self.assertEqual(
                    [
                        MODULE.calculate_wakuban(horse_count, umaban)
                        for umaban in range(1, horse_count + 1)
                    ],
                    expected,
                )

    def test_rejects_invalid_field_size_and_horse_number(self) -> None:
        for horse_count, umaban in ((0, 1), (17, 1), (8, 0), (8, 9)):
            with (
                self.subTest(horse_count=horse_count, umaban=umaban),
                self.assertRaises(ValueError),
            ):
                MODULE.calculate_wakuban(horse_count, umaban)

    def test_fills_rowspan_omissions_in_deba_table(self) -> None:
        expected = [1, 2, 3, 4, 5, 6, 7, 7, 8, 8]
        rows = []
        for umaban, wakuban in enumerate(expected, start=1):
            show_frame = umaban == 1 or wakuban != expected[umaban - 2]
            frame_cell = (
                f'<td class="courseNum course_0{wakuban}">{wakuban}</td>'
                if show_frame
                else ""
            )
            rows.append(
                '<tr class="tBorder">'
                f'{frame_cell}<td class="horseNum">{umaban}</td>'
                f'<td><a class="horseName">テスト{umaban}</a></td></tr>'
            )

        entries = MODULE.parse_deba_entries(f"<table>{''.join(rows)}</table>")

        self.assertEqual([int(entry.wakuban) for entry in entries], expected)

    def test_rejects_official_frame_that_conflicts_with_calculation(self) -> None:
        html = """
        <table>
          <tr class="tBorder"><td class="courseNum course_08">8</td><td class="horseNum">1</td><td><a class="horseName">A</a></td></tr>
          <tr class="tBorder"><td class="courseNum course_02">2</td><td class="horseNum">2</td><td><a class="horseName">B</a></td></tr>
        </table>
        """

        with self.assertRaisesRegex(SystemExit, "DebaTable wakuban mismatch"):
            MODULE.parse_deba_entries(html)

    def test_race_row_keeps_registered_and_active_field_sizes_separate(self) -> None:
        row = MODULE.blank_ra(
            ymd="20260901",
            yobi="2",
            keibajo="30",
            kai="11",
            nichi="01",
            race_bango="01",
            name="テスト",
            hasso="1200",
            kyori="1200",
            track="23",
            toroku=12,
            shusso=11,
        )

        self.assertEqual(row["toroku_tosu"], "12")
        self.assertEqual(row["shusso_tosu"], "11")


class EntityRelationTest(unittest.TestCase):
    def test_normalizes_person_and_owner_variants(self) -> None:
        self.assertEqual(
            MODULE.normalize_person_name("柳　澤　好　美（北海道）"), "柳沢好美"
        )
        self.assertEqual(
            MODULE.normalize_owner_name("（有）キャロットファーム"),
            "キャロットファーム",
        )

    def test_scores_orthographic_and_abbreviated_person_names(self) -> None:
        self.assertEqual(MODULE.person_name_similarity("櫻井今朝利", "桜井今朝利"), 1.0)
        self.assertGreater(
            MODULE.person_name_similarity("Ｆ．ゴンサルベス", "ゴンサル"),
            0.9,
        )

    def test_parses_full_name_from_profile_heading(self) -> None:
        html = """
        <h4 class="odd_title">藤　田　　凌　駕</h4>
        <h4 class="odd_title mini">（フジタ　リョウガ）</h4>
        """

        self.assertEqual(MODULE.parse_profile_name(html), "藤 田 凌 駕")

    def test_resolves_newest_person_code_from_official_profile(self) -> None:
        index = {
            "藤田凌駕": {
                MODULE.PersonRecord(
                    code="21355",
                    full_name="藤田凌駕",
                    short_name="藤田凌駕",
                    last_seen="20251113",
                ),
                MODULE.PersonRecord(
                    code="05686",
                    full_name="藤田凌駕",
                    short_name="藤田凌駕",
                    last_seen="20260831",
                ),
            }
        }
        html = '<h4 class="odd_title">藤　田　凌　駕</h4>'

        with mock.patch.object(MODULE, "fetch", return_value=html):
            person = MODULE.resolve_person(
                entity_type="jockey",
                license_no="31337",
                index=index,
                profile_cache=Path("profiles"),
                refresh=False,
            )

        self.assertEqual(person.code, "05686")
        self.assertEqual(person.full_name, "藤 田 凌 駕")

    def test_resolves_foreign_abbreviated_person_name(self) -> None:
        index = {
            "ゴンサル": {
                MODULE.PersonRecord(
                    code="05675",
                    full_name="ゴンサル",
                    short_name="ゴンサル",
                    last_seen="20260903",
                )
            }
        }
        html = '<h4 class="odd_title">Ｆ．ゴンサルベス</h4>'

        with mock.patch.object(MODULE, "fetch", return_value=html):
            person = MODULE.resolve_person(
                entity_type="jockey",
                license_no="80172",
                index=index,
                profile_cache=Path("profiles"),
                refresh=False,
            )

        self.assertEqual(person.code, "05675")

    def test_fingerprints_rows_independently_of_input_order(self) -> None:
        first = MODULE.rows_fingerprint([{"id": "2"}, {"id": "1"}])
        second = MODULE.rows_fingerprint([{"id": "1"}, {"id": "2"}])

        self.assertEqual(
            first,
            "6c1a54029b4f46ee452c0f1811f4be1e35e5f57d0d3f9c3c6fa372cdb864d0b3",
        )
        self.assertEqual(second, first)

    def test_writes_json_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.json"

            MODULE.write_json_atomic(path, {"ok": True})

            self.assertEqual(path.read_text(encoding="utf-8"), '{\n  "ok": true\n}')
            self.assertFalse(path.with_suffix(".json.tmp").exists())

    def test_resolves_owner_from_unique_master(self) -> None:
        index = {
            "キャロットファーム": {
                MODULE.OwnerRecord(code="486800", name="キャロットファーム")
            }
        }

        owner = MODULE.resolve_owner("（有）キャロットファーム", {}, index)

        self.assertEqual(
            owner,
            MODULE.OwnerRecord(code="486800", name="キャロットファーム"),
        )

    def test_creates_composite_unknown_owner_relation(self) -> None:
        owner = MODULE.resolve_owner("新規馬主", {}, {})

        self.assertEqual(owner.code, "000000")
        self.assertEqual(owner.name.rstrip("　"), "新規馬主")

    def test_uses_composite_unknown_relation_for_ambiguous_owner(self) -> None:
        index = {
            "同名馬主": {
                MODULE.OwnerRecord(code="111111", name="同名馬主"),
                MODULE.OwnerRecord(code="222222", name="同名馬主"),
            }
        }

        owner = MODULE.resolve_owner("同名馬主", {}, index)

        self.assertEqual(owner.code, "000000")
        self.assertEqual(owner.name.rstrip("　"), "同名馬主")


if __name__ == "__main__":
    unittest.main()
