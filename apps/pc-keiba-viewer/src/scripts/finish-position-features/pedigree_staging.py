#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Shared pedigree staging helper for finish-position feature scripts.

Stages horse → sire / damsire mapping from pg.jvd_um (JRA), pg.nvd_um
(NAR / Ban-ei, JV-Data mirror) and pg.nvd_nu (NAR / Ban-ei, N-Data
native).  Previous per-script implementations joined pg.jvd_um only,
leaving every NAR/Ban-ei horse with sire_id = NULL and damsire_id = NULL
(silent-NULL bug); pg.nvd_nu adds the N-Data native master so NAR/Ban-ei
horses missing from the JV-Data mirror are still covered.

The QUALIFY deduplication ensures that when a ketto_toroku_bango appears
in more than one table (edge case), priority decides the winner:
jvd_um (1) > nvd_um (2) > nvd_nu (3).
"""
from __future__ import annotations

import duckdb

# nvd_um stores breeding-registration ids as 8 digits (e.g. 11200715) while
# nvd_nu / jvd_um store the same id as 10 digits (1120000715). Validated 100%
# on the 100,452 local nvd_um/nvd_nu overlap pairs (sire and damsire).
_LEGACY_ID_LENGTH = 8


def normalized_pedigree_id_sql(expr: str) -> str:
    """SQL mapping an 8-digit nvd_um breeding id onto the 10-digit form."""
    return (
        f"case when length({expr}) = {_LEGACY_ID_LENGTH} "
        f"then left({expr}, 4) || '00' || right({expr}, 4) else {expr} end"
    )


def pedigree_id_variants(pedigree_id: str) -> tuple[str, ...]:
    """Every raw spelling that normalises to ``pedigree_id`` (for IN-list pushdown)."""
    if len(pedigree_id) == _LEGACY_ID_LENGTH:
        pedigree_id = f"{pedigree_id[:4]}00{pedigree_id[4:]}"
    if len(pedigree_id) == 10 and pedigree_id[4:6] == "00":
        return (pedigree_id, pedigree_id[:4] + pedigree_id[6:])
    return (pedigree_id,)


def stage_horse_pedigree(con: duckdb.DuckDBPyConnection) -> None:
    """Build horse → sire / damsire mapping from jvd_um, nvd_um and nvd_nu.

    Covers JRA horses (pg.jvd_um) and NAR / Ban-ei horses from both the
    JV-Data mirror (pg.nvd_um) and the N-Data native master (pg.nvd_nu).
    When a ketto_toroku_bango appears in more than one source, the
    lowest-priority row wins: jvd_um (1) > nvd_um (2) > nvd_nu (3).

    Columns produced:
      ketto_toroku_bango  VARCHAR  — registration number
      sire_id             VARCHAR  — ketto_joho_01a (NULL when blank/missing)
      damsire_id          VARCHAR  — ketto_joho_05a (NULL when blank/missing)

    The ids stay raw (8-digit nvd_um vs 10-digit nvd_nu/jvd_um) for models
    trained on them; v2 features apply ``normalized_pedigree_id_sql`` instead.
    """
    con.execute(
        """
        create or replace temp table horse_pedigree as
        with combined as (
          select
            ketto_toroku_bango,
            nullif(trim(ketto_joho_01a), '') as sire_id,
            nullif(trim(ketto_joho_05a), '') as damsire_id,
            1 as priority
          from pg.jvd_um
          where ketto_toroku_bango is not null
          union all
          select
            ketto_toroku_bango,
            nullif(trim(ketto_joho_01a), '') as sire_id,
            nullif(trim(ketto_joho_05a), '') as damsire_id,
            2 as priority
          from pg.nvd_um
          where ketto_toroku_bango is not null
          union all
          select
            ketto_toroku_bango,
            nullif(trim(ketto_joho_01a), '') as sire_id,
            nullif(trim(ketto_joho_05a), '') as damsire_id,
            3 as priority
          from pg.nvd_nu
          where ketto_toroku_bango is not null
        )
        select ketto_toroku_bango, sire_id, damsire_id
        from combined
        qualify row_number() over (partition by ketto_toroku_bango order by priority) = 1
        """
    )
    con.execute(
        "create index horse_pedigree_idx on horse_pedigree (ketto_toroku_bango)"
    )
