import type { Pool } from "pg";

import {
  computeHistoricalWinScore,
  type Win5LegInput,
  type Win5RunnerInput,
} from "./prediction";
import type { Win5Schedule } from "./types";

const parseStoredNumber = (value: string | null | undefined): number | null => {
  const cleaned = (value ?? "").trim();
  if (!cleaned || /^0+$/u.test(cleaned)) {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const parseOdds = (value: string | null | undefined): number | null => {
  const parsed = parseStoredNumber(value);
  return parsed === null || parsed <= 0 ? null : parsed / 10;
};

export const buildWin5LegInputsWithPool = async (
  pool: Pool,
  schedule: Win5Schedule,
): Promise<Win5LegInput[]> => {
  const legInputs: Win5LegInput[] = [];

  for (const leg of schedule.legs) {
    const resolvedLegResult = await pool.query<{
      kaisai_kai: string;
      kaisai_nichime: string;
      kyosomei_hondai: string | null;
    }>(
      `
        select
          kaisai_kai,
          kaisai_nichime,
          kyosomei_hondai
        from jvd_ra
        where kaisai_nen = $1
          and kaisai_tsukihi = $2
          and keibajo_code = $3
          and ltrim(race_bango, '0') = ltrim($4, '0')
        order by kaisai_kai asc, kaisai_nichime asc
        limit 1
      `,
      [schedule.kaisaiNen, schedule.kaisaiTsukihi, leg.keibajoCode, leg.raceBango],
    );
    const resolvedLeg = resolvedLegResult.rows[0];
    const kaisaiKai = resolvedLeg?.kaisai_kai ?? leg.kaisaiKai ?? "00";
    const kaisaiNichime = resolvedLeg?.kaisai_nichime ?? leg.kaisaiNichime ?? "00";
    const enrichedLeg = {
      ...leg,
      kaisaiKai,
      kaisaiNichime,
      raceLabel: leg.raceLabel ?? (resolvedLeg?.kyosomei_hondai?.trim() || undefined),
    };

    const runnersResult = await pool.query<{
      bamei: string | null;
      ketto_toroku_bango: string;
      kishumei_ryakusho: string | null;
      tansho_ninkijun: string | null;
      tansho_odds: string | null;
      umaban: string;
    }>(
      `
        select
          se.umaban,
          se.ketto_toroku_bango,
          se.bamei,
          se.kishumei_ryakusho,
          se.tansho_ninkijun,
          se.tansho_odds
        from jvd_se se
        where se.kaisai_nen = $1
          and se.kaisai_tsukihi = $2
          and se.keibajo_code = $3
          and se.kaisai_kai = $4
          and se.kaisai_nichime = $5
          and ltrim(se.race_bango, '0') = ltrim($6, '0')
          and coalesce(se.ijo_kubun_code, '0') = '0'
        order by se.umaban::int asc
      `,
      [
        schedule.kaisaiNen,
        schedule.kaisaiTsukihi,
        enrichedLeg.keibajoCode,
        kaisaiKai,
        kaisaiNichime,
        enrichedLeg.raceBango,
      ],
    );

    if (runnersResult.rows.length === 0) {
      continue;
    }

    const historyResult = await pool.query<{
      ketto_toroku_bango: string;
      runs: string;
      wins: string;
    }>(
      `
        select
          se.ketto_toroku_bango,
          count(*)::text as runs,
          count(*) filter (where se.kakutei_chakujun = '01')::text as wins
        from jvd_se se
        where se.ketto_toroku_bango in (
          select ketto_toroku_bango
          from jvd_se
          where kaisai_nen = $1
            and kaisai_tsukihi = $2
            and keibajo_code = $3
            and kaisai_kai = $4
            and kaisai_nichime = $5
            and ltrim(race_bango, '0') = ltrim($6, '0')
        )
          and (se.kaisai_nen || se.kaisai_tsukihi) < ($1 || $2)
        group by se.ketto_toroku_bango
      `,
      [
        schedule.kaisaiNen,
        schedule.kaisaiTsukihi,
        enrichedLeg.keibajoCode,
        kaisaiKai,
        kaisaiNichime,
        enrichedLeg.raceBango,
      ],
    );
    const historyMap = new Map(
      historyResult.rows.map((row) => [
        row.ketto_toroku_bango,
        computeHistoricalWinScore({
          runs: Number(row.runs),
          wins: Number(row.wins),
        }),
      ]),
    );

    const runners: Win5RunnerInput[] = runnersResult.rows.map((row) => ({
      horseName: row.bamei?.trim() ?? row.umaban,
      horseNumber: row.umaban.replace(/^0+/u, "") || row.umaban,
      jockeyName: row.kishumei_ryakusho?.trim() ?? null,
      odds: parseOdds(row.tansho_odds),
      popularity: parseStoredNumber(row.tansho_ninkijun),
      historicalScore: historyMap.get(row.ketto_toroku_bango) ?? 0,
    }));

    legInputs.push({ leg: enrichedLeg, runners });
  }

  return legInputs;
};
