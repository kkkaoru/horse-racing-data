"""Append exotic-odds implied-probability features to a finish-position feature
parquet directory (NAR + Ban-ei only; JRA has no signal above threshold).

Adds three new per-horse columns derived from final pre-race exotic odds
(data_kubun='5') from the warehouse:

- ``exotic_sanrenpuku_p3``: marginalized 3連複 (sanrenpuku / trio) implied
  probability of the horse finishing in the top-3.  Computed as the sum of
  ``1 / odds`` over all triples containing this horse, then overround-normalized
  within the race.

- ``exotic_wide_p3``: marginalized ワイド (wide / quinella-place) implied
  probability of top-3.  Wide mid-odds = (lo + hi) / 2; marginalized as sum of
  ``1 / mid_odds`` over all pairs containing this horse.

- ``exotic_umaren_p2``: marginalized 馬連 (umaren / quinella) implied
  probability of top-2.  Marginalized as sum of ``1 / odds`` over all pairs
  containing this horse.

All three are NULL when odds are unavailable (race not covered, 2024 NAR gap
for o2/o3, etc.).  The GBDT NULL-routes these gracefully.

Design notes
------------
- Decoding is done entirely in DuckDB SQL with substring arithmetic on the
  packed fixed-width strings.  No per-row Python loops.
- NAR umaren / wide are missing year 2024 (known ingest gap).  sanrenpuku is
  intact.  The module fetches all three and NULLs fill the gap.
- A race-level overround normalization is applied so the implied probs are
  interpretable as relative signals rather than absolute probabilities with
  bookmaker margin.
- The features are appended to the existing parquet; all original columns are
  preserved (schema-extension only — no column reduction).

Usage
-----
Run as a post-processor after the v8 feature pipeline::

    uv run python src/scripts/finish-position-features/add_exotic_odds_features.py \\
        --input-dir tmp/feat-nar-v8-iter17-bataiju \\
        --output-dir tmp/feat-nar-exotic \\
        --pg-url postgresql://horse_racing:***@127.0.0.1:15432/horse_racing \\
        --category nar   # or ban-ei

"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path
from typing import Protocol

import duckdb


class _DuckDBConnectionLike(Protocol):
    def execute(self, query: str) -> object: ...

# keibajo_code ranges for each category.
# Ban-ei is keibajo_code='83'; NAR is 30-48 (plus 50-58, 81-82, 84 edge cases).
NAR_KEIBAJO_CODES: tuple[str, ...] = tuple(
    f"{k:02d}" for k in range(30, 49)
) + tuple(f"{k:02d}" for k in range(50, 59)) + ("81", "82", "84")
BANEI_KEIBAJO_CODE: str = "83"

# Packed-string field widths (chars per combination).
O2_STRIDE: int = 13   # umaren: h1(2)+h2(2)+odds(5)+votes(4)
O3_STRIDE: int = 17   # wide:   h1(2)+h2(2)+lo(5)+hi(5)+votes(3)
O5_STRIDE: int = 15   # sanrenpuku: h1(2)+h2(2)+h3(2)+odds(5)+votes(4)

# Maximum combo counts for 18-horse fields (the safe upper bound).
O2_MAX_COMBOS: int = 153   # C(18,2)
O3_MAX_COMBOS: int = 153   # C(18,2)
O5_MAX_COMBOS: int = 816   # C(18,3)

RACE_PARTITION: str = (
    "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
)
RACE_PARTITION_BY: str = (
    "b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango"
)

DEFAULT_PG_URL: str = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_exotic_odds_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--pg-url",
        type=str,
        default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL),
    )
    parser.add_argument(
        "--category",
        type=str,
        choices=("nar", "ban-ei"),
        default="nar",
    )
    parser.add_argument("--from-date", type=str, default="20100101")
    parser.add_argument("--to-date", type=str, default="20991231")
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# DuckDB / PG helpers
# ---------------------------------------------------------------------------


def install_and_attach_pg(con: _DuckDBConnectionLike, pg_url: str) -> None:
    """Install + load the postgres extension and attach the warehouse DB."""
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{pg_url}' as pg (type postgres, read_only)")


def keibajo_filter_sql(category: str) -> str:
    """Return a SQL IN(...) clause for keibajo_code appropriate to ``category``."""
    if category == "ban-ei":
        return f"keibajo_code = '{BANEI_KEIBAJO_CODE}'"
    codes = ", ".join(f"'{c}'" for c in NAR_KEIBAJO_CODES)
    return f"keibajo_code IN ({codes})"


# ---------------------------------------------------------------------------
# SQL: decode packed strings → per-horse implied-prob margins
# ---------------------------------------------------------------------------


def o5_decode_sql() -> str:
    """Return a DuckDB SQL expression that unnests sanrenpuku packed strings.

    Generates up to O5_MAX_COMBOS entries per race; entries where
    ``odds_raw = '00000'`` or ``odds_raw = '     '`` are discarded (uncovered
    combos in shorter fields).  Returns a table with columns:
    ``(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, h, inv_prob)``
    where ``h`` is the horse number and ``inv_prob = 1 / odds``.
    """
    # Build the unnest over all combination indices.
    slots = ", ".join(str(i) for i in range(O5_MAX_COMBOS))
    return f"""
    with o5_raw as (
        select
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            odds_sanrenpuku
        from pg.nvd_o5
        where data_kubun = '5'
    ),
    o5_slots as (
        select
            r.kaisai_nen,
            r.kaisai_tsukihi,
            r.keibajo_code,
            r.race_bango,
            slot_idx,
            substring(r.odds_sanrenpuku, 1 + slot_idx * {O5_STRIDE}, {O5_STRIDE}) as entry
        from o5_raw r
        cross join unnest([{slots}]) as t(slot_idx)
        where length(r.odds_sanrenpuku) >= (slot_idx + 1) * {O5_STRIDE}
    ),
    o5_parsed as (
        select
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            cast(substring(entry, 1, 2) as integer) as h1,
            cast(substring(entry, 3, 2) as integer) as h2,
            cast(substring(entry, 5, 2) as integer) as h3,
            substring(entry, 7, 5) as odds_raw
        from o5_slots
        where length(trim(substring(entry, 7, 5))) > 0
          and trim(substring(entry, 7, 5)) != '00000'
          and trim(substring(entry, 1, 2)) != '00'
    ),
    o5_inv as (
        select
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            h1 as h,
            1.0 / (try_cast(odds_raw as double) / 10.0) as inv_prob
        from o5_parsed
        where try_cast(odds_raw as double) > 0
        union all
        select
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            h2 as h,
            1.0 / (try_cast(odds_raw as double) / 10.0) as inv_prob
        from o5_parsed
        where try_cast(odds_raw as double) > 0
        union all
        select
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            h3 as h,
            1.0 / (try_cast(odds_raw as double) / 10.0) as inv_prob
        from o5_parsed
        where try_cast(odds_raw as double) > 0
    ),
    o5_marginal as (
        select
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            h as umaban,
            sum(inv_prob) as raw_p3
        from o5_inv
        group by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, h
    ),
    o5_normed as (
        select
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            umaban,
            raw_p3 / nullif(sum(raw_p3) over (
                partition by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
            ), 0) as exotic_sanrenpuku_p3
        from o5_marginal
    )
    select * from o5_normed
    """


def o3_decode_sql() -> str:
    """Return a DuckDB SQL expression for wide (o3) implied-prob features.

    Wide carries (lo_odds, hi_odds) per pair; mid_odds = (lo + hi) / 2 / 10.
    """
    slots = ", ".join(str(i) for i in range(O3_MAX_COMBOS))
    return f"""
    with o3_raw as (
        select
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            odds_wide
        from pg.nvd_o3
        where data_kubun = '5'
    ),
    o3_slots as (
        select
            r.kaisai_nen,
            r.kaisai_tsukihi,
            r.keibajo_code,
            r.race_bango,
            slot_idx,
            substring(r.odds_wide, 1 + slot_idx * {O3_STRIDE}, {O3_STRIDE}) as entry
        from o3_raw r
        cross join unnest([{slots}]) as t(slot_idx)
        where length(r.odds_wide) >= (slot_idx + 1) * {O3_STRIDE}
    ),
    o3_parsed as (
        select
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            cast(substring(entry, 1, 2) as integer) as h1,
            cast(substring(entry, 3, 2) as integer) as h2,
            substring(entry, 5, 5) as lo_raw,
            substring(entry, 10, 5) as hi_raw
        from o3_slots
        where length(trim(substring(entry, 5, 5))) > 0
          and trim(substring(entry, 5, 5)) != '00000'
          and trim(substring(entry, 1, 2)) != '00'
    ),
    o3_mid as (
        select
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            h1,
            h2,
            (try_cast(lo_raw as double) + try_cast(hi_raw as double)) / 20.0 as mid_odds
        from o3_parsed
        where try_cast(lo_raw as double) > 0 and try_cast(hi_raw as double) > 0
    ),
    o3_inv as (
        select
            kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            h1 as h, 1.0 / mid_odds as inv_prob
        from o3_mid where mid_odds > 0
        union all
        select
            kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            h2 as h, 1.0 / mid_odds as inv_prob
        from o3_mid where mid_odds > 0
    ),
    o3_marginal as (
        select
            kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            h as umaban,
            sum(inv_prob) as raw_p3
        from o3_inv
        group by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, h
    ),
    o3_normed as (
        select
            kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban,
            raw_p3 / nullif(sum(raw_p3) over (
                partition by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
            ), 0) as exotic_wide_p3
        from o3_marginal
    )
    select * from o3_normed
    """


def o2_decode_sql() -> str:
    """Return a DuckDB SQL expression for umaren (o2) implied-prob features."""
    slots = ", ".join(str(i) for i in range(O2_MAX_COMBOS))
    return f"""
    with o2_raw as (
        select
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            odds_umaren
        from pg.nvd_o2
        where data_kubun = '5'
    ),
    o2_slots as (
        select
            r.kaisai_nen,
            r.kaisai_tsukihi,
            r.keibajo_code,
            r.race_bango,
            slot_idx,
            substring(r.odds_umaren, 1 + slot_idx * {O2_STRIDE}, {O2_STRIDE}) as entry
        from o2_raw r
        cross join unnest([{slots}]) as t(slot_idx)
        where length(r.odds_umaren) >= (slot_idx + 1) * {O2_STRIDE}
    ),
    o2_parsed as (
        select
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            cast(substring(entry, 1, 2) as integer) as h1,
            cast(substring(entry, 3, 2) as integer) as h2,
            substring(entry, 5, 5) as odds_raw
        from o2_slots
        where length(trim(substring(entry, 5, 5))) > 0
          and trim(substring(entry, 5, 5)) != '00000'
          and trim(substring(entry, 1, 2)) != '00'
    ),
    o2_inv as (
        select
            kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            h1 as h, 1.0 / (try_cast(odds_raw as double) / 10.0) as inv_prob
        from o2_parsed where try_cast(odds_raw as double) > 0
        union all
        select
            kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            h2 as h, 1.0 / (try_cast(odds_raw as double) / 10.0) as inv_prob
        from o2_parsed where try_cast(odds_raw as double) > 0
    ),
    o2_marginal as (
        select
            kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            h as umaban,
            sum(inv_prob) as raw_p2
        from o2_inv
        group by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, h
    ),
    o2_normed as (
        select
            kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban,
            raw_p2 / nullif(sum(raw_p2) over (
                partition by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
            ), 0) as exotic_umaren_p2
        from o2_marginal
    )
    select * from o2_normed
    """


# ---------------------------------------------------------------------------
# Stage functions
# ---------------------------------------------------------------------------


def stage_exotic_features(
    con: duckdb.DuckDBPyConnection,
    category: str,
    from_date: str,
    to_date: str,
) -> None:
    """Compute exotic implied-prob features from the warehouse and store as
    temp tables ``exotic_o5``, ``exotic_o3``, ``exotic_o2``.

    Date filtering: ``from_date`` and ``to_date`` are YYYYMMDD strings.
    The warehouse stores ``kaisai_nen`` (YYYY) and ``kaisai_tsukihi`` (MMDD)
    as separate columns.  We filter by full date using
    ``kaisai_nen || kaisai_tsukihi``.
    keibajo_code is filtered to the appropriate category.
    """
    date_filter = (
        f"kaisai_nen || kaisai_tsukihi between '{from_date}' and '{to_date}'"
    )
    kb_filter = keibajo_filter_sql(category)

    o5_sql = o5_decode_sql()
    con.execute(
        f"""
        create or replace temp table exotic_o5 as
        select * from ({o5_sql}) q
        where {date_filter} and {kb_filter}
        """
    )

    o3_sql = o3_decode_sql()
    con.execute(
        f"""
        create or replace temp table exotic_o3 as
        select * from ({o3_sql}) q
        where {date_filter} and {kb_filter}
        """
    )

    o2_sql = o2_decode_sql()
    con.execute(
        f"""
        create or replace temp table exotic_o2 as
        select * from ({o2_sql}) q
        where {date_filter} and {kb_filter}
        """
    )


def append_features_sql(input_glob: str) -> str:
    """Return a SELECT that appends exotic columns to the base parquet.

    Joins the three temp tables (left join, so NULL when odds missing).
    All original columns are preserved.

    ``union_by_name=true`` is required because different year partitions may
    have slightly different schemas as features were added incrementally.
    """
    return f"""
    with base as (
        select * from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true)
    )
    select
        b.*,
        o5.exotic_sanrenpuku_p3,
        o3.exotic_wide_p3,
        o2.exotic_umaren_p2
    from base b
    left join exotic_o5 o5
        on  b.kaisai_nen      = o5.kaisai_nen
        and b.kaisai_tsukihi  = o5.kaisai_tsukihi
        and b.keibajo_code    = o5.keibajo_code
        and b.race_bango      = o5.race_bango
        and b.umaban          = o5.umaban
    left join exotic_o3 o3
        on  b.kaisai_nen      = o3.kaisai_nen
        and b.kaisai_tsukihi  = o3.kaisai_tsukihi
        and b.keibajo_code    = o3.keibajo_code
        and b.race_bango      = o3.race_bango
        and b.umaban          = o3.umaban
    left join exotic_o2 o2
        on  b.kaisai_nen      = o2.kaisai_nen
        and b.kaisai_tsukihi  = o2.kaisai_tsukihi
        and b.keibajo_code    = o2.keibajo_code
        and b.race_bango      = o2.race_bango
        and b.umaban          = o2.umaban
    """


def write_partitioned(
    con: duckdb.DuckDBPyConnection, sql: str, output_dir: Path
) -> None:
    """Write the SELECT result to Hive-partitioned parquet under output_dir.

    DuckDB's ``COPY ... PARTITION_BY`` removes the partition column from the
    written files, which breaks downstream readers that use
    ``pd.read_parquet(path_to_file)`` and expect ``race_year`` as a column in
    the data (not just inferred from the folder name).  We work around this by
    collecting the distinct ``race_year`` values first and writing each year's
    slice separately so the column is preserved.
    """
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    # Materialise into a temp table to avoid re-executing the SQL per year.
    con.execute(f"create or replace temp table _exotic_output as ({sql})")
    years = [
        row[0]
        for row in con.execute(
            "select distinct race_year from _exotic_output order by race_year"
        ).fetchall()
    ]
    for year in years:
        year_dir = output_dir / f"race_year={year}"
        year_dir.mkdir(parents=True, exist_ok=True)
        out_file = year_dir / "data_0.parquet"
        con.execute(
            f"copy (select * from _exotic_output where race_year = {year}) "
            f"to '{out_file.as_posix()}' (format parquet)"
        )


# ---------------------------------------------------------------------------
# Pure-function helpers (used for probe calculations without file I/O)
# ---------------------------------------------------------------------------


def decode_sanrenpuku_to_horse_inv_probs(
    packed: str,
) -> dict[int, float]:
    """Decode a sanrenpuku packed string → {umaban: sum_inv_prob}.

    Pure Python; used in tests and the tmp probe script without DuckDB.
    Horses with odds=0 or parse errors are skipped.
    """
    result: dict[int, float] = {}
    for k in range(O5_MAX_COMBOS):
        start = k * O5_STRIDE
        if start + O5_STRIDE > len(packed):
            break
        entry = packed[start : start + O5_STRIDE]
        h1_s = entry[0:2].strip()
        h2_s = entry[2:4].strip()
        h3_s = entry[4:6].strip()
        odds_s = entry[6:11].strip()
        if not h1_s or not h2_s or not h3_s or not odds_s or odds_s == "00000":
            continue
        try:
            h1 = int(h1_s)
            h2 = int(h2_s)
            h3 = int(h3_s)
            odds_raw = int(odds_s)
        except ValueError:
            continue
        if odds_raw <= 0 or h1 == 0 or h2 == 0 or h3 == 0:
            continue
        inv = 1.0 / (odds_raw / 10.0)
        for h in (h1, h2, h3):
            result[h] = result.get(h, 0.0) + inv
    return result


def decode_wide_to_horse_inv_probs(packed: str) -> dict[int, float]:
    """Decode a wide (o3) packed string → {umaban: sum_inv_prob}."""
    result: dict[int, float] = {}
    for k in range(O3_MAX_COMBOS):
        start = k * O3_STRIDE
        if start + O3_STRIDE > len(packed):
            break
        entry = packed[start : start + O3_STRIDE]
        h1_s = entry[0:2].strip()
        h2_s = entry[2:4].strip()
        lo_s = entry[4:9].strip()
        hi_s = entry[9:14].strip()
        if not h1_s or not h2_s or not lo_s or not hi_s or lo_s == "00000":
            continue
        try:
            h1 = int(h1_s)
            h2 = int(h2_s)
            lo = int(lo_s)
            hi = int(hi_s)
        except ValueError:
            continue
        if lo <= 0 or hi <= 0 or h1 == 0 or h2 == 0:
            continue
        mid_odds = (lo + hi) / 20.0
        if mid_odds <= 0:
            continue
        inv = 1.0 / mid_odds
        for h in (h1, h2):
            result[h] = result.get(h, 0.0) + inv
    return result


def decode_umaren_to_horse_inv_probs(packed: str) -> dict[int, float]:
    """Decode a umaren (o2) packed string → {umaban: sum_inv_prob}."""
    result: dict[int, float] = {}
    for k in range(O2_MAX_COMBOS):
        start = k * O2_STRIDE
        if start + O2_STRIDE > len(packed):
            break
        entry = packed[start : start + O2_STRIDE]
        h1_s = entry[0:2].strip()
        h2_s = entry[2:4].strip()
        odds_s = entry[4:9].strip()
        if not h1_s or not h2_s or not odds_s or odds_s == "00000":
            continue
        try:
            h1 = int(h1_s)
            h2 = int(h2_s)
            odds_raw = int(odds_s)
        except ValueError:
            continue
        if odds_raw <= 0 or h1 == 0 or h2 == 0:
            continue
        inv = 1.0 / (odds_raw / 10.0)
        for h in (h1, h2):
            result[h] = result.get(h, 0.0) + inv
    return result


def normalize_inv_probs(inv_probs: dict[int, float]) -> dict[int, float]:
    """Overround-normalize a {umaban: raw_implied_prob} dict.

    Returns a new dict where values sum to 1.0 (overround removed).
    Returns an empty dict when the input is empty or total is zero.
    """
    if not inv_probs:
        return {}
    total = sum(inv_probs.values())
    if total <= 0:
        return {}
    return {h: v / total for h, v in inv_probs.items()}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()
    input_glob = f"{args.input_dir.as_posix()}/race_year=*/*.parquet"

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA enable_object_cache=true")
    install_and_attach_pg(con, args.pg_url)
    stage_exotic_features(con, args.category, args.from_date, args.to_date)
    sql = append_features_sql(input_glob)
    write_partitioned(con, sql, args.output_dir)
    con.close()
    print(
        f"[add-exotic-odds-features] wrote exotic features to {args.output_dir} "
        f"category={args.category}"
    )


if __name__ == "__main__":
    main()
