import { Pool } from "pg";

import {
  DEFAULT_RACE_PACE_PREDICTION_MODEL,
  type RacePacePredictionModel,
} from "../lib/race-pace-prediction";
import {
  compareMonth,
  type CliOptions,
  getAggregateSummary,
  getConnectionString,
  getMonths,
  loadEnv,
  runInBatches,
} from "./compare-corner-predictions";

type TrainOptions = {
  concurrency: number;
  output: "json" | "text";
  sourceScope: CliOptions["sourceScope"];
  target: CliOptions["target"];
  testFromDate: string;
  testToDate: string;
  trainFromDate: string;
  trainToDate: string;
};

type CandidateResult = {
  model: RacePacePredictionModel;
  name: string;
  score: number | null;
};

const pad2 = (value: number): string => String(value).padStart(2, "0");

const formatDate = (date: Date): string =>
  `${date.getFullYear()}${pad2(date.getMonth() + 1)}${pad2(date.getDate())}`;

const addDays = (date: Date, days: number): Date => {
  const nextDate = new Date(date);
  nextDate.setDate(nextDate.getDate() + days);
  return nextDate;
};

const addMonths = (date: Date, months: number): Date => {
  const nextDate = new Date(date);
  nextDate.setMonth(nextDate.getMonth() + months);
  return nextDate;
};

const normalizeDate = (value: string): string => value.replaceAll("-", "");

const getDefaultOptions = (): TrainOptions => {
  const today = new Date();
  const testToDate = addDays(today, -1);
  const testFromDate = addMonths(testToDate, -3);
  const trainToDate = addDays(testFromDate, -1);
  const trainFromDate = addMonths(trainToDate, -9);
  return {
    concurrency: 4,
    output: "text",
    sourceScope: "all",
    target: "local",
    testFromDate: formatDate(testFromDate),
    testToDate: formatDate(testToDate),
    trainFromDate: formatDate(trainFromDate),
    trainToDate: formatDate(trainToDate),
  };
};

const printHelp = () => {
  console.log(`Usage:
  bun run src/scripts/train-corner-prediction-model.ts [options]

Options:
  --train-from-date YYYY-MM-DD
  --train-to-date YYYY-MM-DD
  --test-from-date YYYY-MM-DD
  --test-to-date YYYY-MM-DD
  --target local|neon
  --source-scope all|jra|nar
  --concurrency N
  --output text|json
`);
};

const parseArgs = (args: string[]): TrainOptions => {
  const options = getDefaultOptions();
  for (let index = 0; index < args.length; index += 1) {
    const name = args[index];
    const value = args[index + 1];
    if (name === "--help" || name === "-h") {
      printHelp();
      process.exit(0);
    }
    if (!value) {
      throw new Error(`${name} requires a value.`);
    }
    if (name === "--train-from-date") {
      options.trainFromDate = normalizeDate(value);
    } else if (name === "--train-to-date") {
      options.trainToDate = normalizeDate(value);
    } else if (name === "--test-from-date") {
      options.testFromDate = normalizeDate(value);
    } else if (name === "--test-to-date") {
      options.testToDate = normalizeDate(value);
    } else if (name === "--target") {
      if (value !== "local" && value !== "neon") {
        throw new Error("--target must be local or neon.");
      }
      options.target = value;
    } else if (name === "--source-scope") {
      if (value !== "all" && value !== "jra" && value !== "nar") {
        throw new Error("--source-scope must be all, jra, or nar.");
      }
      options.sourceScope = value;
    } else if (name === "--concurrency") {
      options.concurrency = Math.max(1, Number(value));
    } else if (name === "--output") {
      if (value !== "text" && value !== "json") {
        throw new Error("--output must be text or json.");
      }
      options.output = value;
    } else {
      throw new Error(`Unknown argument: ${name}`);
    }
    index += 1;
  }
  return options;
};

const withModel = (
  name: string,
  overrides: Partial<RacePacePredictionModel>,
): { model: RacePacePredictionModel; name: string } => ({
  model: {
    ...DEFAULT_RACE_PACE_PREDICTION_MODEL,
    ...overrides,
  },
  name,
});

const getCandidates = (sourceScope: CliOptions["sourceScope"]) => {
  const candidates = [withModel("baseline", {})];
  if (sourceScope !== "nar") {
    for (const decay of [0.15, 0.3, 0.5, 0.75, 1, 1.25]) {
      for (const lowSample of [0.8, 1, 1.2]) {
        candidates.push(
          withModel(`jra-decay-${decay}-low-${lowSample}`, {
            jraDecayMultiplier: decay,
            jraLowSamplePriorMultiplier: lowSample,
          }),
        );
      }
    }
    for (const horse of [0.85, 1, 1.15]) {
      for (const jockey of [0.75, 1, 1.25]) {
        for (const trainer of [0.75, 1, 1.25]) {
          candidates.push(
            withModel(`jra-horse-${horse}-jockey-${jockey}-trainer-${trainer}`, {
              jraHorseWeightMultiplier: horse,
              jraJockeyWeightMultiplier: jockey,
              jraTrainerWeightMultiplier: trainer,
            }),
          );
        }
      }
    }
    for (const horseNumberPrior of [0.25, 0.5, 1]) {
      for (const lowSample of [0.8, 1, 1.2]) {
        candidates.push(
          withModel(`jra-horse-number-prior-${horseNumberPrior}-low-${lowSample}`, {
            jraLowSampleHorseNumberPriorWeight: horseNumberPrior,
            jraLowSamplePriorMultiplier: lowSample,
          }),
        );
      }
    }
    for (const floor of [0.03, 0.06, 0.1, 0.15, 0.25, 0.4, 0.6]) {
      candidates.push(
        withModel(`jra-popularity-floor-${floor}`, {
          jraPopularityPriorFloorWeight: floor,
        }),
      );
      candidates.push(
        withModel(`jra-popularity-floor-${floor}-low-1.2`, {
          jraLowSamplePriorMultiplier: 1.2,
          jraPopularityPriorFloorWeight: floor,
        }),
      );
    }
  }
  if (sourceScope !== "jra") {
    for (const decay of [0.75, 1, 1.25]) {
      for (const lowSample of [0.8, 1, 1.2]) {
        candidates.push(
          withModel(`nar-decay-${decay}-low-${lowSample}`, {
            narDecayMultiplier: decay,
            narLowSamplePriorMultiplier: lowSample,
          }),
        );
      }
    }
  }
  const unique = new Map(candidates.map((candidate) => [candidate.name, candidate]));
  return [...unique.values()];
};

const evaluateModel = async ({
  fromDate,
  model,
  options,
  pool,
  toDate,
}: {
  fromDate: string;
  model: RacePacePredictionModel;
  options: TrainOptions;
  pool: Pool;
  toDate: string;
}) => {
  const compareOptions: CliOptions = {
    concurrency: options.concurrency,
    fromDate,
    fromYear: null,
    model,
    output: "json",
    sourceScope: options.sourceScope,
    target: options.target,
    toDate,
    toYear: null,
  };
  const monthResults = await runInBatches(
    getMonths(fromDate, toDate),
    options.concurrency,
    (month) => compareMonth(pool, month.year, month.month, compareOptions),
  );
  const races = monthResults.flatMap((result) => result.comparisons);
  return getAggregateSummary(
    monthResults.map((result) => result.summary),
    races,
  );
};

const sortByScoreDesc = (left: CandidateResult, right: CandidateResult): number =>
  (right.score ?? -1) - (left.score ?? -1);

const main = async () => {
  await loadEnv();
  const options = parseArgs(process.argv.slice(2));
  const pool = new Pool({
    connectionString: getConnectionString(options.target),
    max: options.concurrency + 2,
  });
  try {
    const candidates = getCandidates(options.sourceScope);
    const trainResults = await runInBatches(
      candidates,
      1,
      async (candidate): Promise<CandidateResult> => {
        const summary = await evaluateModel({
          fromDate: options.trainFromDate,
          model: candidate.model,
          options,
          pool,
          toDate: options.trainToDate,
        });
        return {
          model: candidate.model,
          name: candidate.name,
          score: summary.averageScore,
        };
      },
    );
    trainResults.sort(sortByScoreDesc);
    const best = trainResults[0];
    if (!best) {
      throw new Error("No train result.");
    }
    const testSummary = await evaluateModel({
      fromDate: options.testFromDate,
      model: best.model,
      options,
      pool,
      toDate: options.testToDate,
    });
    if (options.output === "json") {
      console.log(
        JSON.stringify(
          {
            best,
            options,
            test: testSummary,
            train: trainResults,
          },
          null,
          2,
        ),
      );
      return;
    }
    console.log("corner prediction train/test");
    console.log(
      `train=${options.trainFromDate}-${options.trainToDate} test=${options.testFromDate}-${options.testToDate} source=${options.sourceScope}`,
    );
    console.log(
      `best=${best.name} trainScore=${best.score === null ? "-" : (best.score * 100).toFixed(3)}%`,
    );
    console.log(
      `testScore=${testSummary.averageScore === null ? "-" : (testSummary.averageScore * 100).toFixed(3)}% races=${testSummary.comparedRaces} corners=${testSummary.comparedCorners}`,
    );
    console.log("top candidates");
    for (const result of trainResults.slice(0, 10)) {
      console.log(
        `${result.name},${result.score === null ? "-" : (result.score * 100).toFixed(3)}%`,
      );
    }
  } finally {
    await pool.end();
  }
};

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
