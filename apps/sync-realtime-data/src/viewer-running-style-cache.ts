// Run with bun. Writes pc-keiba-viewer compatible running-style caches after D1 inference.

import { putD1QueryCache } from "./d1-query-cache";
import { evaluateRunningStyleCacheCoverage } from "./running-style-entry-coverage";
import { putRunningStyleCache } from "./running-style-cache";
import type { RaceRunningStyleRow, RunningStyleInferenceRace } from "./running-style-d1";
import {
  buildRealtimeRaceKeyFromRunningStyle,
  buildViewerRunningStyleRaceKey,
} from "./running-style-features";
import { getLatestRaceEntries } from "./storage";
import type { Env } from "./types";

export interface ViewerRunningStyleRow {
  bamei: string | null;
  category: string;
  horseNumber: number;
  kaisaiNen: string;
  kettoTorokuBango: string;
  modelVersion: string;
  p_nige: number;
  p_oikomi: number;
  p_sashi: number;
  p_senkou: number;
  predictedAt: string;
  predictedLabel: RaceRunningStyleRow["predictedLabel"];
  raceKey: string;
}

const toViewerRunningStyleRow = (row: RaceRunningStyleRow): ViewerRunningStyleRow => ({
  bamei: row.bamei,
  category: row.category,
  horseNumber: row.horseNumber,
  kaisaiNen: row.kaisaiNen,
  kettoTorokuBango: row.kettoTorokuBango,
  modelVersion: row.modelVersion,
  p_nige: row.pNige,
  p_oikomi: row.pOikomi,
  p_sashi: row.pSashi,
  p_senkou: row.pSenkou,
  predictedAt: row.predictedAt,
  predictedLabel: row.predictedLabel,
  raceKey: row.raceKey,
});

export const putViewerRunningStyleRaceCache = async ({
  ctx,
  env,
  race,
  rows,
}: {
  ctx?: ExecutionContext;
  env: Env;
  race: RunningStyleInferenceRace;
  rows: ReadonlyArray<RaceRunningStyleRow>;
}): Promise<boolean> => {
  if (rows.length === 0) {
    return false;
  }
  const latestEntries = await getLatestRaceEntries(
    env.REALTIME_DB,
    buildRealtimeRaceKeyFromRunningStyle(race),
  );
  const coverage = evaluateRunningStyleCacheCoverage(latestEntries?.horses ?? null, rows);
  if (!coverage.cacheable || coverage.cacheableRows.length === 0) {
    return false;
  }
  const viewerRows = coverage.cacheableRows.map(toViewerRunningStyleRow);
  const urlWritten = await putRunningStyleCache({ env, race, rows: coverage.cacheableRows });
  await putD1QueryCache(
    "running-style-race",
    ["getRaceRunningStylesFromD1", buildViewerRunningStyleRaceKey(race)],
    viewerRows,
    {
      ctx,
      kv: env.DETAIL_SECTION_CACHE_KV,
      raceDay: {
        kaisaiNen: race.kaisaiNen,
        kaisaiTsukihi: race.kaisaiTsukihi,
      },
    },
  );
  return urlWritten;
};

export const putViewerRunningStyleBatchCache = async ({
  ctx,
  raceDay,
  raceKeys,
  rows,
}: {
  ctx?: ExecutionContext;
  raceDay?: { kaisaiNen: string; kaisaiTsukihi: string };
  raceKeys: ReadonlyArray<string>;
  rows: ReadonlyArray<RaceRunningStyleRow>;
}): Promise<void> => {
  const uniqueRaceKeys = Array.from(new Set(raceKeys.filter((raceKey) => raceKey.length > 0)));
  if (uniqueRaceKeys.length === 0 || rows.length === 0) {
    return;
  }
  await putD1QueryCache(
    "running-style-races",
    ["getRaceRunningStylesByRaceKeysFromD1", uniqueRaceKeys],
    rows.map(toViewerRunningStyleRow),
    { ctx, raceDay },
  );
};
