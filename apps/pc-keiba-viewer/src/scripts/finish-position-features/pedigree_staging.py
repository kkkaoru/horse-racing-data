#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Shared pedigree staging helper for finish-position feature scripts.

Stages horse → sire / damsire mapping from both pg.jvd_um (JRA) and
pg.nvd_um (NAR / Ban-ei).  Previous per-script implementations joined
pg.jvd_um only, leaving every NAR/Ban-ei horse with sire_id = NULL and
damsire_id = NULL (silent-NULL bug).

The QUALIFY deduplication ensures that when a ketto_toroku_bango appears
in both tables (edge case), the jvd_um row takes priority (priority=1).
"""
from __future__ import annotations

import duckdb


def stage_horse_pedigree(con: duckdb.DuckDBPyConnection) -> None:
    """Build horse → sire / damsire mapping from both jvd_um and nvd_um.

    Covers JRA horses (pg.jvd_um) and NAR / Ban-ei horses (pg.nvd_um).
    When a ketto_toroku_bango appears in both sources, the jvd_um row wins
    (priority = 1 < 2).

    Columns produced:
      ketto_toroku_bango  VARCHAR  — registration number
      sire_id             VARCHAR  — ketto_joho_01a (NULL when blank/missing)
      damsire_id          VARCHAR  — ketto_joho_04a (NULL when blank/missing)
    """
    con.execute(
        """
        create or replace temp table horse_pedigree as
        with combined as (
          select
            ketto_toroku_bango,
            nullif(trim(ketto_joho_01a), '') as sire_id,
            nullif(trim(ketto_joho_04a), '') as damsire_id,
            1 as priority
          from pg.jvd_um
          where ketto_toroku_bango is not null
          union all
          select
            ketto_toroku_bango,
            nullif(trim(ketto_joho_01a), '') as sire_id,
            nullif(trim(ketto_joho_04a), '') as damsire_id,
            2 as priority
          from pg.nvd_um
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
