// Run with bun:
//   bun run src/scripts/walk-forward-win5-eval.ts --start-year 2006 --end-year 2025

import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { Pool } from "pg";

import {
  buildWin5PredictionPayload,
  getWin5PlanForBudget,
  WIN5_DEFAULT_BUDGET_YEN,
} from "../lib/win5/prediction";
import { buildWin5LegsFromRaceJoho } from "../lib/win5/race-joho";
import { parseWin5PayoutField, planCoversWinningCombination } from "../lib/win5/payout-parse";
import type { Win5ValidationResult, Win5ValidationSummary } from "../lib/win5/types";
import {
  buildWin5LegInputsWithPool,
  type Win5ModelScoreLookup,
  type Win5ModelScoreLookupParams,
} from "../lib/win5/leg-inputs";

const DEFAULT_LOCAL_DATABASE_URL =
  "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing";
const DEFAULT_PREDICTIONS_DIR = "tmp/finish-position-eval/predictions-jra-ensemble-xgb-cb/jra";
const DEFAULT_OUTPUT_PATH = "tmp/win5-validation-report.json";
const DEFAULT_AVERAGE_PAYOUT_YEN = 250_000;
const DEFAULT_START_YEAR = 2006;
const DEFAULT_END_YEAR = 2025;
const PERCENTAGE_BASIS = 10_000;
const PERCENTAGE_DIVISOR = 100;

interface CliArgs {
  startYear: number;
  endYear: number;
  outputPath: string | null;
  predictionsDir: string | null;
  baselinePath: string | null;
}

interface WfRow {
  fuseiritsu_flag: string | null;
  haraimodoshi_win5_001: string | null;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  race_joho_1: string | null;
  race_joho_2: string | null;
  race_joho_3: string | null;
  race_joho_4: string | null;
  race_joho_5: string | null;
  tekichu_nashi_flag: string | null;
}

interface PredictionJsonlRow {
  race_id: string;
  ketto_toroku_bango: string;
  predicted_score: number;
}

interface BaselineSummary {
  defaultHitRate: number;
  defaultRecoveryRate: number;
  recommendedHitRate: number;
  recommendedRecoveryRate: number;
}

interface ExtendedSummary extends Win5ValidationSummary {
  baseline?: BaselineSummary;
  delta?: {
    defaultHitRate: number;
    defaultRecoveryRate: number;
    recommendedHitRate: number;
    recommendedRecoveryRate: number;
  };
}

interface SummarizeParams {
  results: Win5ValidationResult[];
  skippedDays: number;
  startYear: number;
  endYear: number;
}

interface EvaluateParams {
  pool: Pool;
  rows: WfRow[];
  averagePayoutYen: number;
  modelScoreLookup: Win5ModelScoreLookup | null;
}

interface EvaluateRowParams extends EvaluateParams {
  row: WfRow;
}

const parseArgs = (): CliArgs => {
  const args = process.argv.slice(2);
  const parsed: CliArgs = {
    startYear: DEFAULT_START_YEAR,
    endYear: DEFAULT_END_YEAR,
    outputPath: DEFAULT_OUTPUT_PATH,
    predictionsDir: DEFAULT_PREDICTIONS_DIR,
    baselinePath: null,
  };
  const handlers = new Map<string, (value: string) => void>([
    ["--start-year", (value) => { parsed.startYear = Number.parseInt(value, 10); }],
    ["--end-year", (value) => { parsed.endYear = Number.parseInt(value, 10); }],
    ["--output", (value) => { parsed.outputPath = value; }],
    ["--predictions-dir", (value) => { parsed.predictionsDir = value; }],
    ["--baseline", (value) => { parsed.baselinePath = value; }],
  ]);
  args.forEach((arg, index) => {
    if (arg === "--no-model-scores") {
      parsed.predictionsDir = null;
      return;
    }
    const handler = handlers.get(arg);
    if (handler) {
      handler(args[index + 1] ?? "");
    }
  });
  return parsed;
};

const getConnectionString = (): string =>
  process.env.DATABASE_URL_LOCAL ??
  process.env.DATABASE_URL ??
  DEFAULT_LOCAL_DATABASE_URL;

const roundRate = (value: number): number =>
  Math.round(value * PERCENTAGE_BASIS) / PERCENTAGE_DIVISOR;

const extractYearFromRaceId = (raceId: string): number | null => {
  const segments = raceId.split(":");
  if (segments.length < 2) {
    return null;
  }
  const year = Number.parseInt(segments[1] ?? "", 10);
  return Number.isFinite(year) ? year : null;
};

const parsePredictionLine = (line: string): PredictionJsonlRow | null => {
  const trimmed = line.trim();
  if (trimmed.length === 0) {
    return null;
  }
  const parsed = JSON.parse(trimmed) as unknown;
  if (typeof parsed !== "object" || parsed === null) {
    return null;
  }
  const candidate = parsed as Partial<PredictionJsonlRow>;
  if (
    typeof candidate.race_id !== "string" ||
    typeof candidate.ketto_toroku_bango !== "string" ||
    typeof candidate.predicted_score !== "number"
  ) {
    return null;
  }
  return {
    race_id: candidate.race_id,
    ketto_toroku_bango: candidate.ketto_toroku_bango,
    predicted_score: candidate.predicted_score,
  };
};

const loadJsonlYearMap = async (params: {
  predictionsDir: string;
  year: number;
}): Promise<Map<string, number>> => {
  const path = resolve(`${params.predictionsDir}/${params.year}.jsonl`);
  const contents = await readFile(path, "utf8").catch(() => null);
  const map = new Map<string, number>();
  if (contents === null) {
    return map;
  }
  contents.split("\n").forEach((line) => {
    const row = parsePredictionLine(line);
    if (row === null) {
      return;
    }
    map.set(`${row.race_id}|${row.ketto_toroku_bango}`, row.predicted_score);
  });
  return map;
};

interface CreateLookupParams {
  predictionsDir: string;
  yearLoader?: (year: number) => Promise<Map<string, number>>;
}

export const createWin5ModelScoreLookup = (
  params: CreateLookupParams,
): Win5ModelScoreLookup => {
  const cache = new Map<number, Map<string, number>>();
  const loader = params.yearLoader ?? ((year: number) =>
    loadJsonlYearMap({ predictionsDir: params.predictionsDir, year }));
  const ensureYear = async (year: number): Promise<Map<string, number>> => {
    const cached = cache.get(year);
    if (cached) {
      return cached;
    }
    const loaded = await loader(year);
    cache.set(year, loaded);
    return loaded;
  };
  return {
    get: (lookupParams: Win5ModelScoreLookupParams): number | null => {
      const year = extractYearFromRaceId(lookupParams.raceId);
      if (year === null) {
        return null;
      }
      const yearMap = cache.get(year);
      if (!yearMap) {
        void ensureYear(year);
        return null;
      }
      return yearMap.get(`${lookupParams.raceId}|${lookupParams.kettoTorokuBango}`) ?? null;
    },
  };
};

const collectYearsFromRows = (rows: readonly WfRow[]): number[] => {
  const years = new Set<number>();
  rows.forEach((row) => {
    const year = Number.parseInt(row.kaisai_nen, 10);
    if (Number.isFinite(year)) {
      years.add(year);
    }
  });
  return Array.from(years).toSorted((left, right) => left - right);
};

const buildLookupForRows = async (params: {
  predictionsDir: string;
  rows: readonly WfRow[];
}): Promise<Win5ModelScoreLookup> => {
  const years = collectYearsFromRows(params.rows);
  const cache = new Map<number, Map<string, number>>();
  await Promise.all(
    years.map(async (year) => {
      const map = await loadJsonlYearMap({ predictionsDir: params.predictionsDir, year });
      cache.set(year, map);
    }),
  );
  return {
    get: (lookupParams: Win5ModelScoreLookupParams): number | null => {
      const year = extractYearFromRaceId(lookupParams.raceId);
      if (year === null) {
        return null;
      }
      const yearMap = cache.get(year);
      if (!yearMap) {
        return null;
      }
      return yearMap.get(`${lookupParams.raceId}|${lookupParams.kettoTorokuBango}`) ?? null;
    },
  };
};

const loadAveragePayout = async (pool: Pool): Promise<number> => {
  const result = await pool.query<{ average_payout: string | null }>(
    `
      select avg(
        nullif(
          btrim(substring(haraimodoshi_win5_001 from 11 for 9)),
          ''
        )::bigint
      )::text as average_payout
      from jvd_wf
      where coalesce(tekichu_nashi_flag, '0') = '0'
        and coalesce(fuseiritsu_flag, '0') = '0'
        and haraimodoshi_win5_001 is not null
    `,
  );
  const value = Number(result.rows[0]?.average_payout ?? DEFAULT_AVERAGE_PAYOUT_YEN);
  return Number.isFinite(value) ? value : DEFAULT_AVERAGE_PAYOUT_YEN;
};

const loadWfRows = async (params: {
  pool: Pool;
  startYear: number;
  endYear: number;
}): Promise<WfRow[]> => {
  const result = await params.pool.query<WfRow>(
    `
      select
        kaisai_nen,
        kaisai_tsukihi,
        race_joho_1,
        race_joho_2,
        race_joho_3,
        race_joho_4,
        race_joho_5,
        haraimodoshi_win5_001,
        tekichu_nashi_flag,
        fuseiritsu_flag
      from jvd_wf
      where kaisai_nen::int between $1 and $2
      order by kaisai_nen asc, kaisai_tsukihi asc
    `,
    [params.startYear, params.endYear],
  );
  return result.rows;
};

const isSkippableRow = (row: WfRow): boolean =>
  row.tekichu_nashi_flag === "1" || row.fuseiritsu_flag === "1";

const evaluateRow = async (
  params: EvaluateRowParams,
): Promise<Win5ValidationResult | null> => {
  const { row, averagePayoutYen, pool, modelScoreLookup } = params;
  if (isSkippableRow(row)) {
    return null;
  }
  const payout = parseWin5PayoutField(row.haraimodoshi_win5_001);
  if (!payout) {
    return null;
  }
  const legs = buildWin5LegsFromRaceJoho([
    row.race_joho_1,
    row.race_joho_2,
    row.race_joho_3,
    row.race_joho_4,
    row.race_joho_5,
  ]);
  if (legs.length !== 5) {
    return null;
  }
  const schedule = {
    fetchedAt: new Date().toISOString(),
    kaisaiNen: row.kaisai_nen,
    kaisaiTsukihi: row.kaisai_tsukihi,
    legs,
    saleDeadline: null,
    source: "jvd_wf" as const,
  };
  const legInputs = await buildWin5LegInputsWithPool({
    pool,
    schedule,
    modelScoreLookup: modelScoreLookup ?? undefined,
  });
  if (legInputs.length !== 5) {
    return null;
  }
  const prediction = buildWin5PredictionPayload({
    averagePayoutYen,
    kaisaiNen: row.kaisai_nen,
    kaisaiTsukihi: row.kaisai_tsukihi,
    legInputs,
  });
  const defaultPlan = getWin5PlanForBudget(prediction, WIN5_DEFAULT_BUDGET_YEN);
  const recommendedPlan = getWin5PlanForBudget(prediction, prediction.recommendedBudgetYen);
  const defaultHit = planCoversWinningCombination(
    defaultPlan.selections,
    payout.winningHorseNumbers,
  );
  const recommendedHit = planCoversWinningCombination(
    recommendedPlan.selections,
    payout.winningHorseNumbers,
  );
  return {
    actualWinners: payout.winningHorseNumbers,
    defaultBudgetYen: WIN5_DEFAULT_BUDGET_YEN,
    defaultCostYen: defaultPlan.totalCostYen,
    defaultHit,
    defaultReturnYen: defaultHit ? payout.payoutYen : 0,
    kaisaiNen: row.kaisai_nen,
    kaisaiTsukihi: row.kaisai_tsukihi,
    payoutYen: payout.payoutYen,
    recommendedBudgetYen: prediction.recommendedBudgetYen,
    recommendedCostYen: recommendedPlan.totalCostYen,
    recommendedHit,
    recommendedReturnYen: recommendedHit ? payout.payoutYen : 0,
  };
};

interface EvaluateAllResult {
  results: Win5ValidationResult[];
  skippedDays: number;
}

const evaluateAll = async (params: EvaluateParams): Promise<EvaluateAllResult> => {
  const results: Win5ValidationResult[] = [];
  const evaluations = await params.rows.reduce<Promise<Win5ValidationResult[]>>(
    async (accumulator, row) => {
      const previous = await accumulator;
      const next = await evaluateRow({
        ...params,
        row,
      });
      return next === null ? previous : [...previous, next];
    },
    Promise.resolve([]),
  );
  results.push(...evaluations);
  return { results, skippedDays: params.rows.length - results.length };
};

const sumNumber = (values: readonly number[]): number =>
  values.reduce((sum, value) => sum + value, 0);

const summarize = (params: SummarizeParams): Win5ValidationSummary => {
  const { results, skippedDays, startYear, endYear } = params;
  const defaultHits = results.filter((result) => result.defaultHit);
  const recommendedHits = results.filter((result) => result.recommendedHit);
  const defaultTotalCostYen = sumNumber(results.map((row) => row.defaultCostYen));
  const defaultTotalReturnYen = sumNumber(results.map((row) => row.defaultReturnYen));
  const recommendedTotalCostYen = sumNumber(results.map((row) => row.recommendedCostYen));
  const recommendedTotalReturnYen = sumNumber(results.map((row) => row.recommendedReturnYen));
  const recommendedBudgetAverageYen = results.length > 0
    ? Math.round(sumNumber(results.map((row) => row.recommendedBudgetYen)) / results.length)
    : 0;
  return {
    defaultBudgetYen: WIN5_DEFAULT_BUDGET_YEN,
    defaultHitCount: defaultHits.length,
    defaultHitRate: results.length > 0 ? roundRate(defaultHits.length / results.length) : 0,
    defaultRecoveryRate:
      defaultTotalCostYen > 0 ? roundRate(defaultTotalReturnYen / defaultTotalCostYen) : 0,
    defaultTotalCostYen,
    defaultTotalReturnYen,
    evaluatedDays: results.length,
    periodEnd: String(endYear),
    periodStart: String(startYear),
    recommendedBudgetAverageYen,
    recommendedHitCount: recommendedHits.length,
    recommendedHitRate:
      results.length > 0 ? roundRate(recommendedHits.length / results.length) : 0,
    recommendedRecoveryRate:
      recommendedTotalCostYen > 0
        ? roundRate(recommendedTotalReturnYen / recommendedTotalCostYen)
        : 0,
    recommendedTotalCostYen,
    recommendedTotalReturnYen,
    skippedDays,
  };
};

const loadBaseline = async (path: string): Promise<BaselineSummary | null> => {
  const contents = await readFile(resolve(path), "utf8").catch(() => null);
  if (contents === null) {
    return null;
  }
  const parsed = JSON.parse(contents) as { summary?: BaselineSummary };
  if (!parsed.summary) {
    return null;
  }
  return {
    defaultHitRate: parsed.summary.defaultHitRate ?? 0,
    defaultRecoveryRate: parsed.summary.defaultRecoveryRate ?? 0,
    recommendedHitRate: parsed.summary.recommendedHitRate ?? 0,
    recommendedRecoveryRate: parsed.summary.recommendedRecoveryRate ?? 0,
  };
};

const withBaseline = (
  summary: Win5ValidationSummary,
  baseline: BaselineSummary | null,
): ExtendedSummary => {
  if (!baseline) {
    return summary;
  }
  return {
    ...summary,
    baseline,
    delta: {
      defaultHitRate: roundRate((summary.defaultHitRate - baseline.defaultHitRate) / 100),
      defaultRecoveryRate: roundRate(
        (summary.defaultRecoveryRate - baseline.defaultRecoveryRate) / 100,
      ),
      recommendedHitRate: roundRate(
        (summary.recommendedHitRate - baseline.recommendedHitRate) / 100,
      ),
      recommendedRecoveryRate: roundRate(
        (summary.recommendedRecoveryRate - baseline.recommendedRecoveryRate) / 100,
      ),
    },
  };
};

const writeReport = async (params: {
  outputPath: string;
  payload: { results: Win5ValidationResult[]; summary: ExtendedSummary };
}): Promise<void> => {
  const path = resolve(params.outputPath);
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(params.payload, null, 2)}\n`, "utf8");
  console.log(`Report written to ${path}`);
};

const isFileMissingError = (error: unknown): boolean => {
  if (typeof error !== "object" || error === null) {
    return false;
  }
  const code = (error as { code?: string }).code;
  return code === "ENOENT";
};

const ensurePredictionsDirAvailable = async (predictionsDir: string): Promise<boolean> => {
  try {
    await readdir(resolve(predictionsDir));
    return true;
  } catch (error) {
    if (isFileMissingError(error)) {
      console.warn(`Predictions directory not found: ${predictionsDir}. Falling back to heuristic.`);
      return false;
    }
    throw error;
  }
};

const main = async (): Promise<void> => {
  const args = parseArgs();
  const pool = new Pool({ connectionString: getConnectionString() });
  try {
    const averagePayoutYen = await loadAveragePayout(pool);
    const rows = await loadWfRows({ pool, startYear: args.startYear, endYear: args.endYear });
    const modelScoreLookup = args.predictionsDir
      && (await ensurePredictionsDirAvailable(args.predictionsDir))
      ? await buildLookupForRows({ predictionsDir: args.predictionsDir, rows })
      : null;
    const { results, skippedDays } = await evaluateAll({
      pool,
      rows,
      averagePayoutYen,
      modelScoreLookup,
    });
    const summary = summarize({
      results,
      skippedDays,
      startYear: args.startYear,
      endYear: args.endYear,
    });
    const baseline = args.baselinePath ? await loadBaseline(args.baselinePath) : null;
    const extendedSummary = withBaseline(summary, baseline);
    console.log(JSON.stringify(extendedSummary, null, 2));
    if (args.outputPath) {
      await writeReport({ outputPath: args.outputPath, payload: { results, summary: extendedSummary } });
    }
  } finally {
    await pool.end();
  }
};

export {
  buildLookupForRows,
  collectYearsFromRows,
  extractYearFromRaceId,
  loadAveragePayout,
  loadWfRows,
  parsePredictionLine,
  summarize,
  withBaseline,
};

if (import.meta.main) {
  await main();
}
