// Run with:
//   CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE=postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing \
//     bun run running-style:date -- --date 05-24
//
// Optional flags:
//   --year 2026
//   --poll-ms 5000
//   --delay-ms 0
//   --max-rounds 120
//   --register-model jra:/path/to/model.json
//   --register-model nar:/path/to/model.flatbin
//   --remote-models
//   --no-sync-models
//   --no-ensure-models
//   --schedule-only

import { getPlatformProxy } from "wrangler";

import {
  collectRunningStyleDateProgress,
  isRunningStyleDateProgressRowComplete,
  isRunningStyleDateProgressRowDisplayReady,
  resolveRunningStyleDateYmd,
  summarizeRunningStyleDateProgress,
  type RunningStyleDateProgressRow,
} from "../src/running-style-date-progress";
import { handleRunningStylePredictionJob } from "../src/running-style-queue";
import {
  planRunningStylePredictionsForDate,
  refreshViewerRunningStyleCachesForDate,
} from "../src/running-style-cron";
import { listRunningStyleRacesByDate } from "../src/running-style-race-list";
import {
  ensureRunningStyleModels,
  listRequiredRunningStyleModelSources,
  parseRegisterModelArg,
  type RunningStyleModelRegisterSpec,
} from "../src/running-style-model-register";
import type { Env, RunningStylePredictionJob } from "../src/types";

export interface RunningStyleDateCliArgs {
  dateYmd: string;
  delayMs: number;
  ensureModels: boolean;
  maxRounds: number;
  pollMs: number;
  registerModels: RunningStyleModelRegisterSpec[];
  remoteModels: boolean;
  scheduleOnly: boolean;
  syncModels: boolean;
}

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined || value.length === 0) {
    throw new Error(`${name} requires a value.`);
  }
  return value;
};

export const parseRunningStyleDateCliArgs = (
  argv: readonly string[],
  now = new Date(),
): RunningStyleDateCliArgs => {
  let dateRaw: string | undefined;
  let year: number | undefined;
  let pollMs = 5000;
  let delayMs = 0;
  let maxRounds = 120;
  let ensureModels = true;
  let syncModels = true;
  let remoteModels = false;
  let scheduleOnly = false;
  const registerModels: RunningStyleModelRegisterSpec[] = [];

  for (let index = 0; index < argv.length; index += 1) {
    const name = argv[index];
    const value = argv[index + 1];
    if (name === "--register-model") {
      registerModels.push(parseRegisterModelArg(requireValue(name, value)));
      index += 1;
      continue;
    }
    if (name === "--remote-models") {
      remoteModels = true;
      continue;
    }
    if (name === "--no-ensure-models") {
      ensureModels = false;
      continue;
    }
    if (name === "--no-sync-models") {
      syncModels = false;
      continue;
    }
    if (name === "--schedule-only") {
      scheduleOnly = true;
      continue;
    }
    if (name === "--date") {
      dateRaw = requireValue(name, value);
      index += 1;
      continue;
    }
    if (name === "--year") {
      year = Number.parseInt(requireValue(name, value), 10);
      if (!Number.isFinite(year)) {
        throw new Error("--year must be a number.");
      }
      index += 1;
      continue;
    }
    if (name === "--poll-ms") {
      pollMs = Number.parseInt(requireValue(name, value), 10);
      index += 1;
      continue;
    }
    if (name === "--delay-ms") {
      delayMs = Number.parseInt(requireValue(name, value), 10);
      index += 1;
      continue;
    }
    if (name === "--max-rounds") {
      maxRounds = Number.parseInt(requireValue(name, value), 10);
      index += 1;
      continue;
    }
    if (name === undefined) {
      break;
    }
    throw new Error(`Unknown argument: ${name}`);
  }

  if (dateRaw === undefined) {
    throw new Error(
      "Usage: bun run running-style:date -- --date MM-DD [--year YYYY] [--register-model jra:/path/model.json]",
    );
  }
  if (!Number.isFinite(pollMs) || pollMs < 0) {
    throw new Error("--poll-ms must be a non-negative number.");
  }
  if (!Number.isFinite(delayMs) || delayMs < 0) {
    throw new Error("--delay-ms must be a non-negative number.");
  }
  if (!Number.isFinite(maxRounds) || maxRounds <= 0) {
    throw new Error("--max-rounds must be a positive number.");
  }

  return {
    dateYmd: resolveRunningStyleDateYmd(dateRaw, year, now),
    delayMs,
    ensureModels,
    maxRounds,
    pollMs,
    registerModels: registerModels.map((spec) => ({ ...spec, remote: remoteModels || spec.remote })),
    remoteModels,
    scheduleOnly,
    syncModels,
  };
};

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

export const toPredictionJob = (
  row: RunningStyleDateProgressRow,
  predictedAt: string,
): RunningStylePredictionJob => {
  const [, datePart, keibajoCode, raceBango] = row.raceKey.split(":");
  const kaisaiNen = datePart?.slice(0, 4) ?? "";
  const kaisaiTsukihi = datePart?.slice(4, 8) ?? "";
  return {
    kaisaiNen,
    kaisaiTsukihi,
    keibajoCode,
    predictedAt,
    raceBango,
    raceKey: row.raceKey,
    source: row.source,
    type: "generate-running-style-predictions",
  };
};

export const formatRunningStyleDateProgressLine = (
  summary: ReturnType<typeof summarizeRunningStyleDateProgress>,
  dateYmd: string,
  round: number,
): string =>
  [
    `[running-style:date] date=${dateYmd}`,
    `round=${round}`,
    `races=${summary.scanned}`,
    `features=${summary.featureReady}`,
    `d1=${summary.d1Ready}`,
    `parquet=${summary.parquetReady}`,
    `cache=${summary.cacheReady}`,
    `display=${summary.displayReady}`,
    `incomplete=${summary.incomplete}`,
  ].join(" ");

export const printIncompleteRows = (rows: ReadonlyArray<RunningStyleDateProgressRow>): void => {
  const incomplete = rows.filter((row) => !isRunningStyleDateProgressRowComplete(row));
  if (incomplete.length === 0) {
    return;
  }
  for (const row of incomplete.slice(0, 20)) {
    console.log(
      `  pending ${row.raceKey} features=${row.expectedHorses} d1=${row.d1Count} parquet=${row.parquetReady ? "ok" : "ng"} cache=${row.cacheReady ? "ok" : "ng"} status=${row.inferenceStatus}`,
    );
  }
  if (incomplete.length > 20) {
    console.log(`  ... ${incomplete.length - 20} more incomplete races`);
  }
};

export const processIncompleteRaces = async (
  env: Env,
  rows: ReadonlyArray<RunningStyleDateProgressRow>,
  delayMs: number,
): Promise<void> => {
  const predictedAt = new Date().toISOString();
  const targets = rows.filter(
    (row) => row.featuresReady && !isRunningStyleDateProgressRowComplete(row),
  );
  for (const [index, row] of targets.entries()) {
    if (index > 0 && delayMs > 0) {
      await sleep(delayMs);
    }
    const job = toPredictionJob(row, predictedAt);
    try {
      const summary = await handleRunningStylePredictionJob(env, job);
      console.log(
        `  processed ${row.raceKey} written=${summary?.writtenCount ?? 0} cache=${summary?.cacheWritten ? "ok" : "ng"} parquet=${summary?.featuresR2Key ?? "-"}`,
      );
    } catch (error) {
      console.error(
        `  failed ${row.raceKey}:`,
        error instanceof Error ? error.message : String(error),
      );
    }
  }
};

const run = async (): Promise<void> => {
  const args = parseRunningStyleDateCliArgs(process.argv.slice(2));
  const { dispose, env } = await getPlatformProxy<Env>({
    configPath: new URL("../wrangler.jsonc", import.meta.url).pathname,
    remoteBindings: true,
  });

  try {
    console.log(`[running-style:date] target=${args.dateYmd}`);
    const raceList = await listRunningStyleRacesByDate(env, args.dateYmd);
    console.log(
      `[running-style:date] race-list source=${raceList.source} races=${raceList.races.length}`,
    );
    if (raceList.races.length === 0) {
      throw new Error(
        `No races found for ${args.dateYmd}. Run discover-urls or ensure race_entry_corner_features has data.`,
      );
    }

    if (args.ensureModels) {
      const requiredSources = listRequiredRunningStyleModelSources(raceList.races);
      const modelSetup = await ensureRunningStyleModels(
        {
          register: args.registerModels.map((spec) => ({
            ...spec,
            remote: args.remoteModels || spec.remote,
          })),
          sources: requiredSources,
          syncLocalFromRemote: args.syncModels,
        },
      );
      console.log(
        `[running-style:date] models registered=${modelSetup.registered.length} synced=${modelSetup.synced.length} sources=${requiredSources.join(",")}`,
      );
    }

    const initialPlan = await planRunningStylePredictionsForDate(env, args.dateYmd, new Date());
    console.log(
      `[running-style:date] scheduled enqueued=${initialPlan.enqueued} completed=${initialPlan.completed} missingFeatures=${initialPlan.missingFeatures}`,
    );

    if (args.scheduleOnly) {
      console.log(`[running-style:date] schedule-only date=${args.dateYmd} races=${initialPlan.scanned}`);
      return;
    }

    for (let round = 1; round <= args.maxRounds; round += 1) {
      const progress = await collectRunningStyleDateProgress(env, args.dateYmd);
      const summary = summarizeRunningStyleDateProgress(progress);
      console.log(formatRunningStyleDateProgressLine(summary, args.dateYmd, round));
      printIncompleteRows(progress);

      if (summary.incomplete === 0) {
        break;
      }

      await processIncompleteRaces(env, progress, args.delayMs);
      if (round < args.maxRounds && args.pollMs > 0) {
        await sleep(args.pollMs);
      }
    }

    const cacheRefresh = await refreshViewerRunningStyleCachesForDate(env, args.dateYmd);
    console.log(
      `[running-style:date] cache-refresh refreshed=${cacheRefresh.refreshed} skipped=${cacheRefresh.skipped}`,
    );

    const finalProgress = await collectRunningStyleDateProgress(env, args.dateYmd);
    const finalSummary = summarizeRunningStyleDateProgress(finalProgress);
    console.log(formatRunningStyleDateProgressLine(finalSummary, args.dateYmd, args.maxRounds + 1));
    printIncompleteRows(finalProgress);

    if (finalSummary.displayReady < finalSummary.scanned) {
      throw new Error(
        `${finalSummary.scanned - finalSummary.displayReady} races are not cached for viewer display on ${args.dateYmd}.`,
      );
    }
    console.log(`[running-style:date] done date=${args.dateYmd} races=${finalSummary.scanned}`);
  } finally {
    await dispose();
  }
};

if (import.meta.main) {
  run().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}
