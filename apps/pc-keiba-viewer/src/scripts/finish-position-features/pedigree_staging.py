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
