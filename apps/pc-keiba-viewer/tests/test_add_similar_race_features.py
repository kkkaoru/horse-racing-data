"""Tests for add-similar-race-features.py."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-similar-race-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_similar_race_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_similar_race_features"] = subject
_spec.loader.exec_module(subject)


class _FakeResult:
    def __init__(self, rows: list[tuple[int]]) -> None:
        self._rows: list[tuple[int]] = rows

    def fetchall(self) -> list[tuple[int]]:
        return self._rows


class FakeConn:
    def __init__(self, years: list[tuple[int]] | None = None) -> None:
        self.statements: list[str] = []
        self._years: list[tuple[int]] = years if years is not None else [(2024,), (2025,)]

    def execute(self, query: str) -> object:
        self.statements.append(query)
        if query.startswith("select distinct race_year from "):
            return _FakeResult(self._years)
        return None


# ── parse_args ─────────────────────────────────────────────────────────────────


def test_parse_args_requires_input_and_output(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"


def test_parse_args_missing_required_raises_system_exit() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args(["--input-dir", "/tmp/only-input"])


def test_parse_args_default_category_is_jra(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.category == "jra"


def test_parse_args_accepts_ban_ei_category(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--category",
            "ban-ei",
        ]
    )
    assert args.category == "ban-ei"


def test_parse_args_rejects_unknown_category(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        subject.parse_args(
            [
                "--input-dir",
                str(tmp_path / "in"),
                "--output-dir",
                str(tmp_path / "out"),
                "--category",
                "keirin",
            ]
        )


def test_parse_args_pg_url_default_contains_localhost(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert "127.0.0.1" in args.pg_url


def test_parse_args_from_date_default(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.from_date == "20000101"


def test_parse_args_from_date_override(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--from-date",
            "20150101",
        ]
    )
    assert args.from_date == "20150101"


def test_parse_args_accepts_target_race(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--target-race",
            "06:11",
        ]
    )
    assert args.target_race == "06:11"


def test_parse_args_accepts_resource_args(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--threads",
            "4",
            "--memory-limit",
            "6GB",
        ]
    )
    assert args.threads == 4
    assert args.memory_limit == "6GB"


# ── constants ──────────────────────────────────────────────────────────────────


def test_min_similar_constant() -> None:
    assert subject.MIN_SIMILAR == 30


def test_history_lookback_years_constant() -> None:
    assert subject.HISTORY_LOOKBACK_YEARS == 10


def test_time_decay_rate_constant() -> None:
    assert subject.TIME_DECAY_RATE == 0.001


def test_ban_ei_keibajo_code_constant() -> None:
    assert subject.BAN_EI_KEIBAJO_CODE == "83"


def test_similar_pool_cap_constant() -> None:
    assert subject.SIMILAR_POOL_CAP == 200


# ── install_and_attach_pg ──────────────────────────────────────────────────────


def test_install_and_attach_pg_executes_three_statements() -> None:
    conn = FakeConn()
    subject.install_and_attach_pg(conn, "postgresql://stub/horse_racing")
    assert conn.statements[0] == "install postgres"
    assert conn.statements[1] == "load postgres"
    assert conn.statements[2].startswith("attach 'postgresql://stub/horse_racing'")
    assert "read_only" in conn.statements[2]


# ── surface_sql ────────────────────────────────────────────────────────────────


def test_surface_sql_extracts_first_track_code_char_in_duckdb() -> None:
    con = duckdb.connect(":memory:")
    sql = subject.surface_sql("tc")
    rows = con.execute(
        f"""
        with v(tc) as (values ('10'), ('23'), ('30'), (cast(null as varchar)))
        select tc, {sql} as surface from v
        """
    ).fetchall()
    con.close()
    by_tc = {row[0]: row[1] for row in rows}
    assert by_tc["10"] == "1"
    assert by_tc["23"] == "2"
    assert by_tc["30"] == "3"
    assert by_tc[None] == ""


# ── kyori_band_sql ─────────────────────────────────────────────────────────────


def test_kyori_band_sql_assigns_bands_in_duckdb() -> None:
    con = duckdb.connect(":memory:")
    sql = subject.kyori_band_sql("kyori")
    rows = con.execute(
        f"""
        with v(kyori) as (values (1200), (1300), (1600), (1700), (2000), (2200), (2400), (cast(null as integer)))
        select kyori, {sql} as band from v
        """
    ).fetchall()
    con.close()
    by_kyori = {row[0]: row[1] for row in rows}
    assert by_kyori[1200] == 0
    assert by_kyori[1300] == 0
    assert by_kyori[1600] == 1
    assert by_kyori[1700] == 1
    assert by_kyori[2000] == 2
    assert by_kyori[2200] == 2
    assert by_kyori[2400] == 3
    assert by_kyori[None] is None


# ── season_band_sql ────────────────────────────────────────────────────────────


def test_season_band_sql_assigns_seasons_in_duckdb() -> None:
    con = duckdb.connect(":memory:")
    sql = subject.season_band_sql("tsukihi")
    rows = con.execute(
        f"""
        with v(tsukihi) as (values ('0115'), ('0415'), ('0715'), ('1015'), ('1215'), (cast(null as varchar)), ('1'))
        select tsukihi, {sql} as season from v
        """
    ).fetchall()
    con.close()
    by_tsukihi = {row[0]: row[1] for row in rows}
    assert by_tsukihi["0115"] == 3
    assert by_tsukihi["0415"] == 0
    assert by_tsukihi["0715"] == 1
    assert by_tsukihi["1015"] == 2
    assert by_tsukihi["1215"] == 3
    assert by_tsukihi[None] is None
    assert by_tsukihi["1"] is None


# ── class_group_sql ────────────────────────────────────────────────────────────


def test_class_group_sql_jra_uses_kyoso_joken_code_in_duckdb() -> None:
    con = duckdb.connect(":memory:")
    sql = subject.class_group_sql("jra", "joken", "meisho")
    rows = con.execute(
        f"""
        with v(joken, meisho) as (values ('010', cast(null as varchar)), ('', 'x'), (cast(null as varchar), 'y'))
        select joken, {sql} as cg from v
        """
    ).fetchall()
    con.close()
    by_joken = {row[0]: row[1] for row in rows}
    assert by_joken["010"] == "010"
    assert by_joken[""] == "000"
    assert by_joken[None] == "000"


def test_class_group_sql_banei_uses_kyoso_joken_code() -> None:
    con = duckdb.connect(":memory:")
    sql = subject.class_group_sql("ban-ei", "joken", "meisho")
    rows = con.execute(
        f"""
        with v(joken, meisho) as (values ('703', cast(null as varchar)))
        select {sql} as cg from v
        """
    ).fetchall()
    con.close()
    assert rows[0][0] == "703"


def test_class_group_sql_nar_derives_label_from_meisho_in_duckdb() -> None:
    con = duckdb.connect(":memory:")
    sql = subject.class_group_sql("nar", "joken", "meisho")
    rows = con.execute(
        f"""
        with v(meisho) as (
          values ('「ＯＰ」'), ('新馬戦'), ('未勝利'), ('２歳'), ('３歳'),
                 ('Ａ１'), ('Ｂ２'), ('Ｃ２'), ('特別')
        )
        select meisho, {sql} as cg from v
        """
    ).fetchall()
    con.close()
    by_meisho = {row[0]: row[1] for row in rows}
    assert by_meisho["「ＯＰ」"] == "OP"
    assert by_meisho["新馬戦"] == "NEW"
    assert by_meisho["未勝利"] == "MUKATSU"
    assert by_meisho["２歳"] == "2YO"
    assert by_meisho["３歳"] == "3YO"
    assert by_meisho["Ａ１"] == "A"
    assert by_meisho["Ｂ２"] == "B"
    assert by_meisho["Ｃ２"] == "C"
    assert by_meisho["特別"] == "other"


# ── focused target scope ───────────────────────────────────────────────────────


def test_stage_target_similarity_scope_reads_input_similarity_keys() -> None:
    conn = FakeConn()
    subject.stage_target_similarity_scope(conn, "/tmp/in/race_year=*/*.parquet")
    body = " ".join(conn.statements)
    assert "create or replace temp table target_similarity_scope" in body
    assert "read_parquet('/tmp/in/race_year=*/*.parquet'" in body
    assert "left(coalesce(track_code, ''), 1)" in body
    assert "kyori_band" in body


def test_similar_history_focus_filter_sql_false_is_empty() -> None:
    assert subject.similar_history_focus_filter_sql(False) == ""


def test_similar_history_focus_filter_sql_true_preserves_level4_dims() -> None:
    sql = subject.similar_history_focus_filter_sql(True)
    assert "target_similarity_scope" in sql
    assert "ts.surface = left(coalesce(rec.track_code, ''), 1)" in sql
    assert "ts.kyori_band =" in sql
    assert "rec.race_date <= ts.race_date" in sql
    assert "- 10" in sql


# ── _level_match_predicate ─────────────────────────────────────────────────────


def test_level_1_predicate_matches_all_six_dims() -> None:
    pred = subject._level_match_predicate(1)
    assert "h.keibajo_code = t.keibajo_code" in pred
    assert "h.season_band = t.season_band" in pred
    assert "h.class_group = t.class_group" in pred
    assert "h.surface = t.surface" in pred
    assert "h.kyori_band = t.kyori_band" in pred
    assert "h.race_date < t.race_date" in pred


def test_level_2_predicate_drops_season() -> None:
    pred = subject._level_match_predicate(2)
    assert "h.keibajo_code = t.keibajo_code" in pred
    assert "h.season_band = t.season_band" not in pred
    assert "h.class_group = t.class_group" in pred


def test_level_3_predicate_drops_venue_and_season() -> None:
    pred = subject._level_match_predicate(3)
    assert "h.keibajo_code = t.keibajo_code" not in pred
    assert "h.season_band = t.season_band" not in pred
    assert "h.class_group = t.class_group" in pred


def test_level_4_predicate_is_surface_and_kyori_band_only() -> None:
    pred = subject._level_match_predicate(4)
    assert "h.surface = t.surface" in pred
    assert "h.kyori_band = t.kyori_band" in pred
    assert "h.class_group = t.class_group" not in pred
    assert "h.keibajo_code = t.keibajo_code" not in pred


# ── _level_count_sql ───────────────────────────────────────────────────────────


def test_level_count_sql_level_1_counts_with_all_six_dim_predicate() -> None:
    sql = subject._level_count_sql(1)
    assert "create or replace temp table _level_count_1" in sql
    assert "h.season_band = t.season_band" in sql
    assert "h.keibajo_code = t.keibajo_code" in sql
    assert "count(*) as n1" in sql


def test_level_count_sql_drives_join_off_class_group_for_level_3() -> None:
    sql = subject._level_count_sql(3)
    assert "create or replace temp table _level_count_3" in sql
    assert "h.class_group = t.class_group" in sql
    assert "h.keibajo_code = t.keibajo_code" not in sql
    assert "count(*) as n3" in sql
    assert "h.race_date < t.race_date" in sql


# ── _decay_weight_sql ──────────────────────────────────────────────────────────


def test_decay_weight_sql_uses_supplied_columns_and_rate() -> None:
    sql = subject._decay_weight_sql("t.race_date", "h.race_date")
    assert "exp(-0.001 *" in sql
    assert "strptime(t.race_date, '%Y%m%d')" in sql
    assert "strptime(h.race_date, '%Y%m%d')" in sql


# ── _level_equality_dims ──────────────────────────────────────────────────────


def test_level_equality_dims_level_1_includes_venue_season_class() -> None:
    dims = subject._level_equality_dims(1)
    assert "keibajo_code" in dims
    assert "season_band" in dims
    assert "class_group" in dims


def test_level_equality_dims_level_2_drops_only_season() -> None:
    dims = subject._level_equality_dims(2)
    assert "keibajo_code" in dims
    assert "class_group" in dims
    assert "season_band" not in dims


def test_level_equality_dims_level_3_drops_venue_and_season() -> None:
    dims = subject._level_equality_dims(3)
    assert "class_group" in dims
    assert "keibajo_code" not in dims
    assert "season_band" not in dims


# ── _insert_similar_pool_level (key-collapsed SQL shape) ───────────────────────


class _RecordingConn:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, query: str) -> object:
        self.statements.append(query)
        return None


def test_insert_similar_pool_level_1_collapses_on_venue_season_class_keys() -> None:
    conn = _RecordingConn()
    subject._insert_similar_pool_level(conn, 1)
    sql = "\n".join(conn.statements)
    assert "create or replace temp table _l1_targets" in sql
    assert "create or replace temp table _l1_target_keys" in sql
    assert "create or replace temp table _l1_pool_by_key" in sql
    assert "insert into similar_pool" in sql
    assert "where tml.sim_match_level = 1" in sql
    assert "1 as sim_match_level" in sql
    assert "h.keibajo_code = k.keibajo_code" in sql
    assert "h.season_band = k.season_band" in sql
    assert "h.class_group = k.class_group" in sql
    assert "p.season_band = t.season_band" in sql


def test_insert_similar_pool_level_2_drops_season_band_from_collapse_key() -> None:
    conn = _RecordingConn()
    subject._insert_similar_pool_level(conn, 2)
    sql = "\n".join(conn.statements)
    assert "create or replace temp table _l2_pool_by_key" in sql
    assert "where tml.sim_match_level = 2" in sql
    assert "2 as sim_match_level" in sql
    assert "h.keibajo_code = k.keibajo_code" in sql
    assert "h.class_group = k.class_group" in sql
    assert "h.season_band = k.season_band" not in sql


def test_insert_similar_pool_level_3_drops_venue_and_season_from_collapse_key() -> None:
    conn = _RecordingConn()
    subject._insert_similar_pool_level(conn, 3)
    sql = "\n".join(conn.statements)
    assert "create or replace temp table _l3_pool_by_key" in sql
    assert "where tml.sim_match_level = 3" in sql
    assert "3 as sim_match_level" in sql
    assert "h.class_group = k.class_group" in sql
    assert "h.keibajo_code = k.keibajo_code" not in sql
    assert "h.season_band = k.season_band" not in sql


def test_insert_similar_pool_level_caps_at_pool_cap_ordered_by_race_date_desc() -> None:
    conn = _RecordingConn()
    subject._insert_similar_pool_level(conn, 1)
    sql = "\n".join(conn.statements)
    assert "qualify row_number() over (" in sql
    assert "order by h.race_date desc, h.keibajo_code, h.race_bango" in sql
    assert "<= 200" in sql


# ── stage_similar_history (SQL shape) ──────────────────────────────────────────


def test_stage_similar_history_jra_targets_jvd_tables() -> None:
    conn = FakeConn()
    subject.stage_similar_history(conn, "20000101", "jra")
    body = " ".join(conn.statements)
    assert "pg.race_entry_corner_features rec" in body
    assert "pg.jvd_ra ra" in body
    assert "pg.jvd_um um" in body
    assert "rec.source = 'jra'" in body


def test_stage_similar_history_nar_excludes_ban_ei_venue() -> None:
    conn = FakeConn()
    subject.stage_similar_history(conn, "20000101", "nar")
    body = " ".join(conn.statements)
    assert "pg.nvd_ra ra" in body
    assert "pg.nvd_um um" in body
    assert "rec.source = 'nar'" in body
    assert "rec.keibajo_code <> '83'" in body


def test_stage_similar_history_ban_ei_restricts_to_venue_83() -> None:
    conn = FakeConn()
    subject.stage_similar_history(conn, "20000101", "ban-ei")
    body = " ".join(conn.statements)
    assert "rec.source = 'nar'" in body
    assert "rec.keibajo_code = '83'" in body


def test_stage_similar_history_creates_index() -> None:
    conn = FakeConn()
    subject.stage_similar_history(conn, "20000101", "jra")
    assert any("create index similar_history_idx" in s for s in conn.statements)


def test_stage_similar_history_focused_uses_target_similarity_scope() -> None:
    conn = FakeConn()
    subject.stage_similar_history(conn, "20000101", "jra", focused_target=True)
    body = " ".join(conn.statements)
    assert "target_similarity_scope" in body
    assert "rec.race_date <= ts.race_date" in body


# ── stage_target_entities (SQL shape) ──────────────────────────────────────────


def test_stage_target_entities_has_no_finish_position_filter() -> None:
    conn = FakeConn()
    subject.stage_target_entities(conn, "20000101", "jra")
    body = " ".join(conn.statements)
    assert "pg.jvd_um um" in body
    assert "finish_position is not null" not in body


def test_stage_target_entities_nar_uses_nvd_um() -> None:
    conn = FakeConn()
    subject.stage_target_entities(conn, "20000101", "nar")
    body = " ".join(conn.statements)
    assert "pg.nvd_um um" in body
    assert "rec.keibajo_code <> '83'" in body


def test_stage_target_entities_ban_ei_restricts_to_venue_83() -> None:
    conn = FakeConn()
    subject.stage_target_entities(conn, "20000101", "ban-ei")
    body = " ".join(conn.statements)
    assert "rec.keibajo_code = '83'" in body


def test_target_entities_focus_filter_sql_false_is_empty() -> None:
    assert subject.target_entities_focus_filter_sql(False) == ""


def test_target_entities_focus_filter_sql_true_uses_target_races() -> None:
    sql = subject.target_entities_focus_filter_sql(True)
    assert "target_races tr" in sql
    assert "tr.keibajo_code = rec.keibajo_code" in sql
    assert "tr.race_bango = rec.race_bango" in sql


def test_stage_target_entities_focused_filters_to_target_races() -> None:
    conn = FakeConn()
    subject.stage_target_entities(conn, "20000101", "jra", focused_target=True)
    body = " ".join(conn.statements)
    assert "target_races tr" in body
    assert "tr.race_bango = rec.race_bango" in body


# ── append_features_sql ────────────────────────────────────────────────────────


def test_append_features_sql_contains_phase1_columns() -> None:
    sql = subject.append_features_sql("/tmp/x/race_year=*/*.parquet")
    assert "srf.sim_odds_rank_correlation" in sql
    assert "srf.sim_fav_win_rate" in sql
    assert "srf.sim_odds_correlation_variance" in sql
    assert "srf.sim_match_level" in sql
    assert "coalesce(srf.sim_race_count, 0) as sim_race_count" in sql


def test_append_features_sql_contains_phase2_columns() -> None:
    sql = subject.append_features_sql("/tmp/x/race_year=*/*.parquet")
    assert "js.sim_jockey_win_rate" in sql
    assert "ts.sim_trainer_place_rate" in sql
    assert "ss.sim_sire_offspring_count" in sql
    assert "ds.sim_damsire_win_rate" in sql
    assert "os.sim_owner_race_count" in sql
    assert "uz.sim_umaban_zone_win_rate" in sql


def test_append_features_sql_reads_parquet_glob() -> None:
    sql = subject.append_features_sql("/tmp/x/race_year=*/*.parquet")
    assert "read_parquet('/tmp/x/race_year=*/*.parquet'" in sql


def test_append_features_sql_has_no_year_filter_and_uses_bare_tables() -> None:
    # Per-year scoping now comes from the year-specific input_glob plus the
    # per-year staging tables, so the SQL carries no race_year / kaisai_nen
    # predicate and joins the staging tables by their bare names.
    sql = subject.append_features_sql("/tmp/x/race_year=2024/*.parquet")
    assert "where race_year = " not in sql
    assert "where kaisai_nen = " not in sql
    assert "left join target_entities te" in sql
    assert "left join sim_race_features srf" in sql
    assert "left join sim_jockey_stats js" in sql
    assert "left join sim_trainer_stats ts" in sql
    assert "left join sim_sire_stats ss" in sql
    assert "left join sim_damsire_stats ds" in sql
    assert "left join sim_owner_stats os" in sql
    assert "left join sim_umaban_zone_stats uz" in sql


def test_append_features_sql_reads_year_specific_glob() -> None:
    sql = subject.append_features_sql("/tmp/x/race_year=2024/*.parquet")
    assert "read_parquet('/tmp/x/race_year=2024/*.parquet'" in sql


# ── drop_staging_tables ────────────────────────────────────────────────────────


def test_drop_staging_tables_drops_per_year_intermediates() -> None:
    conn = FakeConn()
    subject.drop_staging_tables(conn)
    body = " ".join(conn.statements)
    assert "drop table if exists similar_pool" in body
    assert "drop table if exists target_races" in body
    assert "drop table if exists target_match_level" in body
    assert "drop table if exists sim_race_features" in body
    assert "drop table if exists sim_jockey_stats" in body
    assert "drop table if exists sim_owner_stats" in body
    assert "drop table if exists target_entities" in body
    assert "drop table if exists _l4_targets" in body
    assert "drop table if exists _level_count_1" in body


def test_drop_staging_tables_preserves_shared_history() -> None:
    conn = FakeConn()
    subject.drop_staging_tables(conn)
    body = " ".join(conn.statements)
    assert "similar_history" not in body
    assert "race_summary" not in body


def test_drop_staging_tables_keeps_stat_tables_in_duckdb(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    _seed_base_parquet(input_dir)
    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    con = duckdb.connect(":memory:")
    _seed_pg_schema(con)
    subject.stage_similar_history(con, "20000101", "jra")
    subject.stage_race_summary(con)
    subject.stage_target_races(con, input_glob)
    subject.stage_target_match_level(con)
    subject.stage_similar_pool(con)
    subject.stage_race_level_features(con)
    subject.stage_entity_features(con)
    subject.stage_target_entities(con, "20000101", "jra")
    subject.drop_staging_tables(con)

    remaining = {
        row[0]
        for row in con.execute(
            "select table_name from information_schema.tables"
        ).fetchall()
    }
    con.close()
    assert "similar_pool" not in remaining
    assert "similar_history" in remaining
    assert "race_summary" in remaining
    assert "pool_results" not in remaining
    assert "_l4_pool_by_key" not in remaining
    assert "sim_race_features" not in remaining
    assert "sim_jockey_stats" not in remaining
    assert "target_entities" not in remaining


# ── _get_years ─────────────────────────────────────────────────────────────────


def test_get_years_reads_distinct_race_year_from_glob() -> None:
    conn = FakeConn([(2023,), (2024,)])
    years = subject._get_years(conn, "/tmp/in/race_year=*/*.parquet")
    assert years == [2023, 2024]
    assert conn.statements[0].startswith(
        "select distinct race_year from read_parquet("
        "'/tmp/in/race_year=*/*.parquet', hive_partitioning=true, union_by_name=true) "
        "order by race_year"
    )


# ── functional: staging pipeline against in-memory DuckDB ──────────────────────


def _seed_pg_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Seed minimal rec + jvd_se + jvd_ra + jvd_um for a JRA functional test.

    History: 3 similar past races at venue=06, 1600m turf (kyori_band=1,
    surface='1'), class '010', spring (season_band=0) in 2024.  Each has 4
    horses; jockey J01 wins one of them.  The target race (2025/0415) shares the
    same key.  With only 3 similar races the count is < MIN_SIMILAR, so the
    target falls back to level 4 (surface + kyori_band only) — which still finds
    the same 3 races.
    """
    con.execute("create schema pg")
    # rec rows: 3 history races (2024) + 1 target race (2025/0415)
    con.execute(
        """
        create table pg.race_entry_corner_features as
        select * from (
          values
            -- history race A: 2024/0401, venue 06, race 11
            ('jra','20240401','2024','0401','06','11','h_a1', 1, '17', 1600, 4, 1, '010', '1', 'J01', 'T01', 'O01'),
            ('jra','20240401','2024','0401','06','11','h_a2', 2, '17', 1600, 4, 2, '010', '1', 'J02', 'T02', 'O02'),
            ('jra','20240401','2024','0401','06','11','h_a3', 3, '17', 1600, 4, 3, '010', '1', 'J03', 'T03', 'O03'),
            ('jra','20240401','2024','0401','06','11','h_a4', 4, '17', 1600, 4, 4, '010', '1', 'J04', 'T04', 'O04'),
            -- history race B: 2024/0408
            ('jra','20240408','2024','0408','06','12','h_b1', 1, '17', 1600, 4, 1, '010', '1', 'J01', 'T01', 'O01'),
            ('jra','20240408','2024','0408','06','12','h_b2', 2, '17', 1600, 4, 2, '010', '1', 'J05', 'T05', 'O05'),
            ('jra','20240408','2024','0408','06','12','h_b3', 3, '17', 1600, 4, 3, '010', '1', 'J06', 'T06', 'O06'),
            ('jra','20240408','2024','0408','06','12','h_b4', 4, '17', 1600, 4, 4, '010', '1', 'J07', 'T07', 'O07'),
            -- history race C: 2024/0415 — favourite (rank 1) loses (finishes 2nd)
            ('jra','20240415','2024','0415','06','11','h_c1', 1, '17', 1600, 4, 2, '010', '1', 'J08', 'T08', 'O08'),
            ('jra','20240415','2024','0415','06','11','h_c2', 2, '17', 1600, 4, 1, '010', '2', 'J09', 'T09', 'O09'),
            ('jra','20240415','2024','0415','06','11','h_c3', 3, '17', 1600, 4, 3, '010', '3', 'J10', 'T10', 'O10'),
            ('jra','20240415','2024','0415','06','11','h_c4', 4, '17', 1600, 4, 4, '010', '4', 'J11', 'T11', 'O11'),
            -- target race: 2025/0415, venue 06, race 11. horse_a ridden by J01.
            ('jra','20250415','2025','0415','06','11','horse_a', 1, '17', 1600, 3, 1, '010', '1', 'J01', 'T01', 'O01'),
            ('jra','20250415','2025','0415','06','11','horse_b', 2, '17', 1600, 3, 2, '010', '1', 'J20', 'T20', 'O20'),
            ('jra','20250415','2025','0415','06','11','horse_c', 3, '17', 1600, 3, 3, '010', '1', 'J21', 'T21', 'O21')
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
                ketto_toroku_bango, umaban, track_code, kyori, shusso_tosu, finish_position,
                kyoso_joken_code, tansho_ninkijun, kishumei_ryakusho, chokyoshimei_ryakusho, banushimei)
        """
    )
    con.execute(
        """
        create table pg.jvd_ra as
        select * from (
          values
            ('2024','0401','06','11','010'),
            ('2024','0408','06','12','010'),
            ('2024','0415','06','11','010'),
            ('2025','0415','06','11','010')
        ) as v(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, kyoso_joken_meisho)
        """
    )
    con.execute(
        """
        create table pg.jvd_se as
        select cast(null as varchar) as kaisai_nen, cast(null as varchar) as kaisai_tsukihi,
               cast(null as varchar) as keibajo_code, cast(null as varchar) as race_bango,
               cast(null as varchar) as ketto_toroku_bango
        where false
        """
    )
    con.execute(
        """
        create table pg.jvd_um as
        select * from (
          values
            ('h_a1','SIRE_X','DAMSIRE_Y'),
            ('horse_a','SIRE_X','DAMSIRE_Y')
        ) as v(ketto_toroku_bango, ketto_joho_01b, ketto_joho_05b)
        """
    )


def _seed_base_parquet(input_dir: Path) -> None:
    """Write a minimal feature parquet for the target race horses."""
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2025', '0415', '06', '11', 'horse_a', '20250415', 2025),
            ('jra', '2025', '0415', '06', '11', 'horse_b', '20250415', 2025),
            ('jra', '2025', '0415', '06', '11', 'horse_c', '20250415', 2025)
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, race_date, race_year)
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()


def test_staging_pipeline_computes_race_level_features(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    _seed_base_parquet(input_dir)
    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    con = duckdb.connect(":memory:")
    _seed_pg_schema(con)
    subject.stage_similar_history(con, "20000101", "jra")
    subject.stage_race_summary(con)
    subject.stage_target_races(con, input_glob)
    subject.stage_target_match_level(con)
    subject.stage_similar_pool(con)
    subject.stage_race_level_features(con)

    rows = con.execute(
        """
        select sim_race_count, sim_match_level, sim_fav_win_rate
        from sim_race_features
        order by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
        """
    ).fetchall()
    con.close()

    assert len(rows) == 1
    # 3 similar races found, < MIN_SIMILAR (30) at every dim level -> level 4.
    assert rows[0][0] == 3
    assert rows[0][1] == 4
    # races A and B had favourite (rank 1) finishing first; race C did not.
    assert rows[0][2] == pytest.approx(2.0 / 3.0, abs=0.05)


def test_staging_pipeline_match_level_falls_back_to_broad(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    _seed_base_parquet(input_dir)
    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    con = duckdb.connect(":memory:")
    _seed_pg_schema(con)
    subject.stage_similar_history(con, "20000101", "jra")
    subject.stage_race_summary(con)
    subject.stage_target_races(con, input_glob)
    subject.stage_target_match_level(con)

    rows = con.execute("select n1, n2, n3, sim_match_level from target_match_level "
                       "join target_level_counts using "
                       "(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)").fetchall()
    con.close()
    assert rows[0][0] == 3
    assert rows[0][3] == 4


def test_staging_pipeline_computes_entity_stats(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    _seed_base_parquet(input_dir)
    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    con = duckdb.connect(":memory:")
    _seed_pg_schema(con)
    subject.stage_similar_history(con, "20000101", "jra")
    subject.stage_race_summary(con)
    subject.stage_target_races(con, input_glob)
    subject.stage_target_match_level(con)
    subject.stage_similar_pool(con)
    subject.stage_entity_features(con)

    rows = con.execute(
        """
        select kishumei_ryakusho, sim_jockey_win_rate, sim_jockey_place_rate, sim_jockey_ride_count
        from sim_jockey_stats
        where kishumei_ryakusho = 'J01'
        """
    ).fetchall()
    con.close()
    # J01 rode in race A (won) and race B (won) within the similar pool -> 2 rides, 2 wins.
    assert rows[0][0] == "J01"
    assert rows[0][1] == pytest.approx(1.0)
    assert rows[0][2] == pytest.approx(1.0)
    assert rows[0][3] == 2


def test_main_end_to_end_appends_similar_features(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    _seed_base_parquet(input_dir)

    def _fake_install_and_attach(con: duckdb.DuckDBPyConnection, _pg_url: str) -> None:
        _seed_pg_schema(con)

    monkeypatch.setattr(subject, "install_and_attach_pg", _fake_install_and_attach)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "add_similar_race_features",
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
            "--threads",
            "2",
            "--memory-limit",
            "1GB",
        ],
    )
    subject.main()

    verify_con = duckdb.connect(":memory:")
    rows = verify_con.execute(
        f"""
        select ketto_toroku_bango, sim_race_count, sim_match_level,
               sim_jockey_win_rate, sim_jockey_ride_count, sim_owner_race_count
        from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')
        order by ketto_toroku_bango
        """
    ).fetchall()
    verify_con.close()

    by_horse = {row[0]: row for row in rows}
    assert len(by_horse) == 3
    # every horse in the target race shares the race-level count + level.
    assert by_horse["horse_a"][1] == 3
    assert by_horse["horse_a"][2] == 4
    assert by_horse["horse_b"][1] == 3
    # horse_a's jockey J01 had 2 prior rides in the pool, both wins.
    assert by_horse["horse_a"][3] == pytest.approx(1.0)
    assert by_horse["horse_a"][4] == 2
    # horse_a's owner O01 raced in pool races A and B -> 2 results.
    assert by_horse["horse_a"][5] == 2
    # horse_b's jockey J20 has no prior pool ride -> NULL.
    assert by_horse["horse_b"][3] is None


def test_main_end_to_end_emits_sire_and_umaban_zone_columns(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    _seed_base_parquet(input_dir)

    def _fake_install_and_attach(con: duckdb.DuckDBPyConnection, _pg_url: str) -> None:
        _seed_pg_schema(con)

    monkeypatch.setattr(subject, "install_and_attach_pg", _fake_install_and_attach)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "add_similar_race_features",
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
            "--threads",
            "2",
            "--memory-limit",
            "1GB",
        ],
    )
    subject.main()

    verify_con = duckdb.connect(":memory:")
    cols = verify_con.execute(
        f"select * from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet') limit 0"
    ).description
    names = {c[0] for c in cols}
    sire_row = verify_con.execute(
        f"""
        select sim_sire_offspring_count, sim_umaban_zone_win_rate
        from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')
        where ketto_toroku_bango = 'horse_a'
        """
    ).fetchall()
    verify_con.close()

    assert "sim_sire_win_rate" in names
    assert "sim_sire_offspring_count" in names
    assert "sim_damsire_win_rate" in names
    assert "sim_umaban_zone_win_rate" in names
    # SIRE_X has one offspring result (h_a1, finished 1st) in pool race A.
    assert sire_row[0][0] == 1


def test_main_end_to_end_clears_preexisting_output_and_writes_hive_partition(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    _seed_base_parquet(input_dir)
    stale_file = output_dir / "race_year=1999" / "data_0.parquet"
    stale_file.parent.mkdir(parents=True)
    stale_file.write_text("stale")

    def _fake_install_and_attach(con: duckdb.DuckDBPyConnection, _pg_url: str) -> None:
        _seed_pg_schema(con)

    monkeypatch.setattr(subject, "install_and_attach_pg", _fake_install_and_attach)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "add_similar_race_features",
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
            "--threads",
            "2",
            "--memory-limit",
            "1GB",
        ],
    )
    subject.main()

    # The pre-existing stale partition was removed before the fresh write.
    assert not stale_file.exists()
    assert (output_dir / "race_year=2025" / "data_0.parquet").exists()

    # partition_by (race_year) writes the year-partitioned hive layout AND
    # OMITS race_year from the file body (matching the base build + every
    # sibling layer). Keeping race_year as an int64 column inside the file
    # would re-introduce the pyarrow schema-merge collision against the
    # path-derived dictionary<int32> partition column that broke
    # pipeline_runner.py's `pd.read_parquet(final_dir)`. We inspect the raw
    # parquet file schema (NOT via DuckDB's read_parquet, which transparently
    # backfills hive partition columns from the path) so we are checking the
    # actual file body.
    import pyarrow.parquet as pq

    file_cols = {
        field.name
        for field in pq.ParquetFile(
            (output_dir / "race_year=2025" / "data_0.parquet").as_posix()
        ).schema_arrow
    }
    assert "sim_race_count" in file_cols
    assert "race_year" not in file_cols


def test_main_end_to_end_output_dir_is_pd_read_parquet_mergeable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Regression for the pipeline_runner.py:`pd.read_parquet(final_dir)` crash.

    The predict container's pipeline_runner reads the final layer's output via
    ``pd.read_parquet(final_dir)``, which delegates to pyarrow's dataset
    reader. pyarrow auto-detects the hive ``race_year=YYYY`` partition,
    materialises that column as ``dictionary<int32>``, and then merges in each
    file's body schema. If the file body also carries a plain int64
    ``race_year`` column, pyarrow raises::

        pyarrow.lib.ArrowTypeError: Unable to merge: Field race_year has
        incompatible types: int64 vs dictionary<values=int32, ...>

    This regression test seeds the layer end-to-end, then re-reads the output
    directory with pyarrow's dataset reader (the same code path
    ``pd.read_parquet`` takes for a directory) and asserts the merge
    succeeds with race_year typed as the path-derived dictionary.
    """
    import pandas as pd

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    _seed_base_parquet(input_dir)

    def _fake_install_and_attach(con: duckdb.DuckDBPyConnection, _pg_url: str) -> None:
        _seed_pg_schema(con)

    monkeypatch.setattr(subject, "install_and_attach_pg", _fake_install_and_attach)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "add_similar_race_features",
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
            "--threads",
            "2",
            "--memory-limit",
            "1GB",
        ],
    )
    subject.main()

    # The predict container's `pd.read_parquet(final_dir)` call against the
    # final layer's output is the exact code path that was broken: it has to
    # merge the path-derived hive partition column with each file body's
    # schema. The previous manual write left race_year inside the file as
    # int64, which collided with the dictionary<int32> partition column at
    # merge time. After the fix this read succeeds and returns the per-horse
    # rows seeded into the input.
    frame = pd.read_parquet(output_dir)
    assert len(frame) == 3
    assert "race_year" in frame.columns
    assert "sim_race_count" in frame.columns


def test_staging_pipeline_level4_caps_pool_to_most_recent_races(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Level-4 (broad) pool is capped to the SIMILAR_POOL_CAP most-recent races.

    Five history races all share surface='1' + kyori_band=1 (1600m) but carry
    DISTINCT class_group (C01..C05) and DISTINCT venues, so they never match the
    target at levels 1-3 (n1=n2=n3=0 < MIN_SIMILAR) and resolve ONLY at level 4.
    The target (2025/0601) is dated after all five. With SIMILAR_POOL_CAP lowered
    to 2 (interpolated at call time inside _insert_similar_pool_level4, so the
    monkeypatch takes effect), the level-4 collapse must keep exactly the 2
    MOST-RECENT history races (2024/0505, 2024/0404) and drop the older three,
    every kept sim_race_date strictly earlier than the target (leak-free).
    """
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2025', '0601', '06', '11', 'tg1', '20250601', 2025),
            ('jra', '2025', '0601', '06', '11', 'tg2', '20250601', 2025)
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, race_date, race_year)
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    monkeypatch.setattr(subject, "SIMILAR_POOL_CAP", 2)

    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.race_entry_corner_features as
        select * from (
          values
            ('jra','20240101','2024','0101','01','11','hh1', 1,'17',1600,2,1,'C01','1','JA','TA','OA'),
            ('jra','20240101','2024','0101','01','11','hh1b',2,'17',1600,2,2,'C01','2','JB','TB','OB'),
            ('jra','20240202','2024','0202','02','12','hh2', 1,'17',1600,2,1,'C02','1','JC','TC','OC'),
            ('jra','20240202','2024','0202','02','12','hh2b',2,'17',1600,2,2,'C02','2','JD','TD','OD'),
            ('jra','20240303','2024','0303','03','13','hh3', 1,'17',1600,2,1,'C03','1','JE','TE','OE'),
            ('jra','20240303','2024','0303','03','13','hh3b',2,'17',1600,2,2,'C03','2','JF','TF','OF'),
            ('jra','20240404','2024','0404','04','14','hh4', 1,'17',1600,2,1,'C04','1','JG','TG','OG'),
            ('jra','20240404','2024','0404','04','14','hh4b',2,'17',1600,2,2,'C04','2','JH','TH','OH'),
            ('jra','20240505','2024','0505','05','15','hh5', 1,'17',1600,2,1,'C05','1','JI','TI','OI'),
            ('jra','20240505','2024','0505','05','15','hh5b',2,'17',1600,2,2,'C05','2','JJ','TJ','OJ'),
            ('jra','20250601','2025','0601','06','11','tg1', 1,'17',1600,2,1,'C99','1','JX','TX','OX'),
            ('jra','20250601','2025','0601','06','11','tg2', 2,'17',1600,2,2,'C99','2','JY','TY','OY')
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
                ketto_toroku_bango, umaban, track_code, kyori, shusso_tosu, finish_position,
                kyoso_joken_code, tansho_ninkijun, kishumei_ryakusho, chokyoshimei_ryakusho, banushimei)
        """
    )
    con.execute(
        """
        create table pg.jvd_ra as
        select * from (
          values
            ('2024','0101','01','11','C01'),
            ('2024','0202','02','12','C02'),
            ('2024','0303','03','13','C03'),
            ('2024','0404','04','14','C04'),
            ('2024','0505','05','15','C05'),
            ('2025','0601','06','11','C99')
        ) as v(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, kyoso_joken_meisho)
        """
    )
    con.execute(
        """
        create table pg.jvd_um as
        select * from (
          values ('tg1','SIRE_X','DAMSIRE_Y')
        ) as v(ketto_toroku_bango, ketto_joho_01b, ketto_joho_05b)
        """
    )
    subject.stage_similar_history(con, "20000101", "jra")
    subject.stage_race_summary(con)
    subject.stage_target_races(con, input_glob)
    subject.stage_target_match_level(con)
    subject.stage_similar_pool(con)

    level_rows = con.execute(
        """
        select n1, n2, n3, sim_match_level
        from target_match_level
        join target_level_counts using
          (source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
        """
    ).fetchall()
    pool_rows = con.execute(
        """
        select sim_match_level, sim_race_date
        from similar_pool
        order by sim_race_date desc
        """
    ).fetchall()
    con.close()

    # All lower levels yield zero matches -> the target resolves only at level 4.
    assert level_rows[0][0] == 0
    assert level_rows[0][1] == 0
    assert level_rows[0][2] == 0
    assert level_rows[0][3] == 4
    # The cap (lowered to 2 via monkeypatch) prunes the 5-race pool to exactly 2.
    assert len(pool_rows) == 2
    assert pool_rows[0][0] == 4
    assert pool_rows[1][0] == 4
    # The kept races are the 2 MOST-RECENT history races before the target.
    assert pool_rows[0][1] == "20240505"
    assert pool_rows[1][1] == "20240404"
    # Leak-free: every kept sim_race_date is strictly earlier than the target.
    assert pool_rows[0][1] < "20250601"
    assert pool_rows[1][1] < "20250601"


def test_staging_pipeline_level1_collapse_fans_identical_pool_to_shared_key_targets(
    tmp_path: Path,
) -> None:
    """Level-1 (exact) collapse fans one capped pool out to targets sharing a key.

    36 history races (2024) all share the SAME level-1 key — venue 06, 1600m turf
    (surface='1', kyori_band=1), class '010', April (season_band=0) — so they
    exceed MIN_SIMILAR (30) and resolve the target at LEVEL 1. TWO target races
    (2025/0415, race_bango 11 and 12) share that identical level-1 key AND the
    same race_date, so the key-collapse ranks history ONCE and fans the same
    capped pool out to both. Both targets must get the identical sim_race_count
    and resolve at level 1.
    """
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2025', '0415', '06', '11', 'tg11', '20250415', 2025),
            ('jra', '2025', '0415', '06', '12', 'tg12', '20250415', 2025)
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, race_date, race_year)
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    # 36 history races (12 April dates x 3 race_bango), one entry each, all sharing
    # the level-1 key (venue 06, 1600m turf, class 010, April) so n1 >= MIN_SIMILAR.
    con.execute(
        """
        create table pg.race_entry_corner_features as
        select
          'jra' as source,
          '2024' || '04' || lpad(cast(d as varchar), 2, '0') as race_date,
          '2024' as kaisai_nen,
          '04' || lpad(cast(d as varchar), 2, '0') as kaisai_tsukihi,
          '06' as keibajo_code,
          cast(b as varchar) as race_bango,
          'hh' || cast(d as varchar) || '_' || cast(b as varchar) as ketto_toroku_bango,
          1 as umaban,
          '17' as track_code,
          1600 as kyori,
          4 as shusso_tosu,
          1 as finish_position,
          '010' as kyoso_joken_code,
          1 as tansho_ninkijun,
          'JH' as kishumei_ryakusho,
          'TH' as chokyoshimei_ryakusho,
          'OH' as banushimei
        from generate_series(1, 12) as g(d)
        cross join (select unnest([10, 11, 12]) as b)
        union all
        select
          'jra', '20250415', '2025', '0415', '06', cast(rb as varchar),
          'tg' || cast(rb as varchar), 1, '17', 1600, 3, 1, '010', 1, 'JX', 'TX', 'OX'
        from (select unnest([11, 12]) as rb)
        """
    )
    con.execute(
        """
        create table pg.jvd_ra as
        select '2024' as kaisai_nen,
               '04' || lpad(cast(d as varchar), 2, '0') as kaisai_tsukihi,
               '06' as keibajo_code, cast(b as varchar) as race_bango,
               '010' as kyoso_joken_meisho
        from generate_series(1, 12) as g(d)
        cross join (select unnest([10, 11, 12]) as b)
        union all
        select '2025', '0415', '06', cast(rb as varchar), '010'
        from (select unnest([11, 12]) as rb)
        """
    )
    con.execute(
        """
        create table pg.jvd_um as
        select * from (
          values ('tg11', 'SIRE_X', 'DAMSIRE_Y')
        ) as v(ketto_toroku_bango, ketto_joho_01b, ketto_joho_05b)
        """
    )
    subject.stage_similar_history(con, "20000101", "jra")
    subject.stage_race_summary(con)
    subject.stage_target_races(con, input_glob)
    subject.stage_target_match_level(con)
    subject.stage_similar_pool(con)
    subject.stage_race_level_features(con)

    feature_rows = con.execute(
        """
        select race_bango, sim_match_level, sim_race_count
        from sim_race_features
        order by race_bango
        """
    ).fetchall()
    con.close()

    # Two target races, each resolving at level 1 (exact) with >= MIN_SIMILAR pool.
    assert len(feature_rows) == 2
    assert feature_rows[0][0] == "11"
    assert feature_rows[1][0] == "12"
    assert feature_rows[0][1] == 1
    assert feature_rows[1][1] == 1
    # Both share the level-1 key + race_date, so the collapse fans the IDENTICAL
    # capped pool to both -> identical sim_race_count (all 36 history races).
    assert feature_rows[0][2] == 36
    assert feature_rows[1][2] == 36
