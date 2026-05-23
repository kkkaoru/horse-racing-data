// Run with:
// bun run src/scripts/walk-forward-win5-eval.ts --start-year 2006 --end-year 2025

import { mkdir, writeFile } from "node:fs/promises";
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
import { buildWin5LegInputsWithPool } from "../lib/win5/leg-inputs";

const DEFAULT_LOCAL_DATABASE_URL =
  "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing";

interface CliArgs {
  endYear: number;
  outputPath: string | null;
  startYear: number;
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

const parseArgs = (): CliArgs => {
  const args = process.argv.slice(2);
  let startYear = 2006;
  let endYear = 2025;
  let outputPath: string | null = "tmp/win5-validation-report.json";

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--start-year") {
      startYear = Number.parseInt(args[index + 1] ?? "", 10);
      index += 1;
    } else if (arg === "--end-year") {
      endYear = Number.parseInt(args[index + 1] ?? "", 10);
      index += 1;
    } else if (arg === "--output") {
      outputPath = args[index + 1] ?? null;
      index += 1;
    }
  }

  return { endYear, outputPath, startYear };
};

const getConnectionString = (): string =>
  process.env.DATABASE_URL_LOCAL ??
  process.env.DATABASE_URL ??
  DEFAULT_LOCAL_DATABASE_URL;

const roundRate = (value: number): number => Math.round(value * 10_000) / 100;

const summarizeValidation = (
  results: Win5ValidationResult[],
  skippedDays: number,
  startYear: number,
  endYear: number,
): Win5ValidationSummary => {
  const defaultHits = results.filter((result) => result.defaultHit);
  const recommendedHits = results.filter((result) => result.recommendedHit);
  const defaultTotalCostYen = results.reduce((sum, result) => sum + result.defaultCostYen, 0);
  const defaultTotalReturnYen = results.reduce((sum, result) => sum + result.defaultReturnYen, 0);
  const recommendedTotalCostYen = results.reduce(
    (sum, result) => sum + result.recommendedCostYen,
    0,
  );
  const recommendedTotalReturnYen = results.reduce(
    (sum, result) => sum + result.recommendedReturnYen,
    0,
  );

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
    recommendedBudgetAverageYen:
      results.length > 0
        ? Math.round(
            results.reduce((sum, result) => sum + result.recommendedBudgetYen, 0) / results.length,
          )
        : 0,
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

const main = async (): Promise<void> => {
  const args = parseArgs();
  const pool = new Pool({ connectionString: getConnectionString() });

  try {
    const averageResult = await pool.query<{ average_payout: string | null }>(
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
    const averagePayoutYen = Number(averageResult.rows[0]?.average_payout ?? 250_000);

    const wfResult = await pool.query<WfRow>(
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
      [args.startYear, args.endYear],
    );

    const results: Win5ValidationResult[] = [];
    let skippedDays = 0;

    for (const row of wfResult.rows) {
      if (row.tekichu_nashi_flag === "1" || row.fuseiritsu_flag === "1") {
        skippedDays += 1;
        continue;
      }

      const payout = parseWin5PayoutField(row.haraimodoshi_win5_001);
      if (!payout) {
        skippedDays += 1;
        continue;
      }

      const legs = buildWin5LegsFromRaceJoho([
        row.race_joho_1,
        row.race_joho_2,
        row.race_joho_3,
        row.race_joho_4,
        row.race_joho_5,
      ]);
      if (legs.length !== 5) {
        skippedDays += 1;
        continue;
      }

      const schedule = {
        fetchedAt: new Date().toISOString(),
        kaisaiNen: row.kaisai_nen,
        kaisaiTsukihi: row.kaisai_tsukihi,
        legs,
        saleDeadline: null,
        source: "jvd_wf" as const,
      };

      const legInputs = await buildWin5LegInputsWithPool(pool, schedule);
      if (legInputs.length !== 5) {
        skippedDays += 1;
        continue;
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

      results.push({
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
      });
    }

    const summary = summarizeValidation(results, skippedDays, args.startYear, args.endYear);
    const payload = { results, summary };

    console.log(JSON.stringify(summary, null, 2));

    if (args.outputPath) {
      const outputPath = resolve(args.outputPath);
      await mkdir(dirname(outputPath), { recursive: true });
      await writeFile(outputPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
      console.log(`Report written to ${outputPath}`);
    }
  } finally {
    await pool.end();
  }
};

await main();
