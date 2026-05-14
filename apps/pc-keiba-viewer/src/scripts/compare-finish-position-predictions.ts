import { Pool } from "pg";

import { getConnectionString, loadEnv } from "./compare-corner-predictions";

type Category = "all" | "ban-ei" | "jra" | "nar";

type Options = {
  category: Category;
  concurrency: number;
  fromDate: string;
  target: "local" | "neon";
  toDate: string;
};

type RaceKey = {
  source: string;
  race_date: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
};

type Prediction = {
  actual: number;
  horseNumber: number;
  predictedRank: number;
  score: number;
};

type EvaluationSummary = {
  place1Accuracy: number;
  place2Accuracy: number;
  place3Accuracy: number;
  top3BoxAccuracy: number;
  top3ExactOrderAccuracy: number;
  top3PlaceRelation: number;
  top3WinnerCapture: number;
  top5WinnerCapture: number;
};

type PredictionQueryRow = RaceKey & {
  avg_finish: string | null;
  finish_position: number;
  horseNumber: number;
  odds_score: string | null;
  popularity_score: string | null;
  recent_finish: string | null;
};

type BaneiPredictionQueryRow = {
  finish_position: number;
  horseNumber: number;
  odds_score: string | null;
  popularity_score: string | null;
  race_key: string;
};

const today = new Date();
const defaultToDate = today.toISOString().slice(0, 10).replaceAll("-", "");
const defaultFromDate = new Date(today);
defaultFromDate.setFullYear(defaultFromDate.getFullYear() - 10);

const parseArgs = (args: string[]): Options => {
  const options: Options = {
    category: "all",
    concurrency: 6,
    fromDate: defaultFromDate.toISOString().slice(0, 10).replaceAll("-", ""),
    target: "local",
    toDate: defaultToDate,
  };
  for (let index = 0; index < args.length; index += 1) {
    const name = args[index];
    const value = args[index + 1];
    if (name === "--target") {
      if (value !== "local" && value !== "neon") {
        throw new Error("--target must be local or neon.");
      }
      options.target = value;
      index += 1;
    } else if (name === "--category") {
      if (value !== "all" && value !== "jra" && value !== "nar" && value !== "ban-ei") {
        throw new Error("--category must be all, jra, nar, or ban-ei.");
      }
      options.category = value;
      index += 1;
    } else if (name === "--from-date") {
      if (value === undefined) {
        throw new Error("--from-date requires a value.");
      }
      options.fromDate = value.replaceAll("-", "");
      index += 1;
    } else if (name === "--to-date") {
      if (value === undefined) {
        throw new Error("--to-date requires a value.");
      }
      options.toDate = value.replaceAll("-", "");
      index += 1;
    } else if (name === "--from-year") {
      options.fromDate = `${value}0101`;
      index += 1;
    } else if (name === "--to-year") {
      options.toDate = `${value}1231`;
      index += 1;
    } else if (name === "--concurrency") {
      options.concurrency = Math.max(1, Number(value));
      index += 1;
    } else if (name === "--help" || name === "-h") {
      console.log(`Usage:
  bun run src/scripts/compare-finish-position-predictions.ts [options]

Options:
  --target local|neon
  --category all|jra|nar|ban-ei
  --from-date YYYYMMDD
  --to-date YYYYMMDD
  --from-year YYYY
  --to-year YYYY
  --concurrency N

Default range is the latest 10 years.`);
      process.exit(0);
    } else {
      throw new Error(`Unknown argument: ${name}`);
    }
  }
  return options;
};

const categoryFilter = (category: Category): string => {
  if (category === "jra") {
    return "and source = 'jra'";
  }
  if (category === "nar") {
    return "and source = 'nar' and keibajo_code <> '83'";
  }
  if (category === "ban-ei") {
    return "and source = 'nar' and keibajo_code = '83'";
  }
  return "";
};

const toNumber = (value: string | null): number | null => {
  if (value === null) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const scorePrediction = (row: PredictionQueryRow): number => {
  const values = [
    { value: toNumber(row.avg_finish), weight: 0.18 },
    { value: toNumber(row.recent_finish), weight: 0.1 },
    { value: toNumber(row.popularity_score), weight: 0.6 },
    { value: toNumber(row.odds_score), weight: 0.12 },
  ].filter((item): item is { value: number; weight: number } => item.value !== null);
  if (values.length === 0) {
    return 0.5;
  }
  const weightTotal = values.reduce((total, item) => total + item.weight, 0);
  return values.reduce((total, item) => total + item.value * item.weight, 0) / weightTotal;
};

const raceKey = (row: RaceKey): string =>
  [
    row.source,
    row.race_date,
    row.kaisai_nen,
    row.kaisai_tsukihi,
    row.keibajo_code,
    row.race_bango,
  ].join(":");

const roundPercent = (value: number): number => Math.round(value * 10000) / 100;

const calculateEvaluationSummary = (evaluated: Prediction[][]): EvaluationSummary => {
  if (evaluated.length === 0) {
    return {
      place1Accuracy: 0,
      place2Accuracy: 0,
      place3Accuracy: 0,
      top3BoxAccuracy: 0,
      top3ExactOrderAccuracy: 0,
      top3PlaceRelation: 0,
      top3WinnerCapture: 0,
      top5WinnerCapture: 0,
    };
  }

  let place1Hits = 0;
  let place2Hits = 0;
  let place3Hits = 0;
  let top3WinnerHits = 0;
  let top5WinnerHits = 0;
  let top3ExactOrderHits = 0;
  let top3BoxHits = 0;
  let top3PlaceRelationTotal = 0;

  for (const rows of evaluated) {
    if (rows[0]?.actual === 1) {
      place1Hits += 1;
    }
    if (rows[1]?.actual === 2) {
      place2Hits += 1;
    }
    if (rows[2]?.actual === 3) {
      place3Hits += 1;
    }
    if (rows.slice(0, 3).some((row) => row.actual === 1)) {
      top3WinnerHits += 1;
    }
    if (rows.slice(0, 5).some((row) => row.actual === 1)) {
      top5WinnerHits += 1;
    }

    const predictedTop3 = rows.slice(0, 3);
    const predictedTop3Actuals = predictedTop3.map((row) => row.actual);
    if (
      predictedTop3Actuals[0] === 1 &&
      predictedTop3Actuals[1] === 2 &&
      predictedTop3Actuals[2] === 3
    ) {
      top3ExactOrderHits += 1;
    }

    const actualTop3Set = new Set([1, 2, 3]);
    const matchedTop3Count = predictedTop3.filter((row) => actualTop3Set.has(row.actual)).length;
    if (matchedTop3Count === 3) {
      top3BoxHits += 1;
    }
    top3PlaceRelationTotal += matchedTop3Count / 3;
  }

  return {
    place1Accuracy: roundPercent(place1Hits / evaluated.length),
    place2Accuracy: roundPercent(place2Hits / evaluated.length),
    place3Accuracy: roundPercent(place3Hits / evaluated.length),
    top3BoxAccuracy: roundPercent(top3BoxHits / evaluated.length),
    top3ExactOrderAccuracy: roundPercent(top3ExactOrderHits / evaluated.length),
    top3PlaceRelation: roundPercent(top3PlaceRelationTotal / evaluated.length),
    top3WinnerCapture: roundPercent(top3WinnerHits / evaluated.length),
    top5WinnerCapture: roundPercent(top5WinnerHits / evaluated.length),
  };
};

const loadPredictions = async (pool: Pool, options: Options): Promise<Prediction[][]> => {
  if (options.category === "ban-ei") {
    const result = await pool.query<BaneiPredictionQueryRow>(
      `
        with target as (
          select
            ra.kaisai_nen || ra.kaisai_tsukihi || ':' || ra.keibajo_code || ':' || ra.race_bango race_key,
            nullif(se.umaban, '')::integer umaban,
            nullif(se.kakutei_chakujun, '00')::integer finish_position,
            nullif(se.tansho_ninkijun, '00')::integer tansho_ninkijun,
            nullif(se.tansho_odds, '0000')::numeric / 10 tansho_odds,
            count(*) over (
              partition by ra.kaisai_nen, ra.kaisai_tsukihi, ra.keibajo_code, ra.race_bango
            ) runner_count
          from nvd_se se
          join nvd_ra ra
            on ra.kaisai_nen = se.kaisai_nen
            and ra.kaisai_tsukihi = se.kaisai_tsukihi
            and ra.keibajo_code = se.keibajo_code
            and ra.race_bango = se.race_bango
          where ra.keibajo_code = '83'
            and ra.kaisai_nen || ra.kaisai_tsukihi between $1 and $2
            and nullif(se.kakutei_chakujun, '00') is not null
        )
        select
          race_key,
          umaban "horseNumber",
          finish_position,
          case
            when runner_count > 1 and tansho_ninkijun is not null
            then greatest(0, least(1, (tansho_ninkijun - 1)::numeric / nullif(runner_count - 1, 0)))::text
            else null
          end popularity_score,
          case
            when tansho_odds is not null and tansho_odds > 0
            then greatest(0, least(1, ln(greatest(tansho_odds, 1)) / ln(300)))::text
            else null
          end odds_score
        from target
        where runner_count >= 5
        order by race_key, umaban
      `,
      [options.fromDate, options.toDate],
    );
    const grouped = new Map<string, BaneiPredictionQueryRow[]>();
    for (const row of result.rows) {
      grouped.set(row.race_key, [...(grouped.get(row.race_key) ?? []), row]);
    }
    return [...grouped.values()].map((rows) =>
      rows
        .map((row) => {
          const values = [
            { value: toNumber(row.popularity_score), weight: 0.72 },
            { value: toNumber(row.odds_score), weight: 0.28 },
          ].filter((item): item is { value: number; weight: number } => item.value !== null);
          const weightTotal = values.reduce((total, item) => total + item.weight, 0);
          const score =
            weightTotal > 0
              ? values.reduce((total, item) => total + item.value * item.weight, 0) / weightTotal
              : 0.5;
          return {
            actual: row.finish_position,
            horseNumber: row.horseNumber,
            predictedRank: 0,
            score,
          };
        })
        .toSorted((left, right) => left.score - right.score || left.horseNumber - right.horseNumber)
        .map((row, index) =>
          Object.assign(row, {
            predictedRank: index + 1,
          }),
        ),
    );
  }

  const result = await pool.query<PredictionQueryRow>(
    `
      with target as (
        select
          source,
          race_date,
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          race_bango,
          ketto_toroku_bango,
          umaban,
          finish_position,
          finish_norm,
          tansho_ninkijun,
          tansho_odds,
          count(*) over (
            partition by source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
          ) runner_count
        from race_entry_corner_features
        where race_date between $1 and $2
          ${categoryFilter(options.category)}
          and finish_position is not null
      ),
      history as (
        select
          target.source,
          target.race_date,
          target.kaisai_nen,
          target.kaisai_tsukihi,
          target.keibajo_code,
          target.race_bango,
          target.umaban,
          history.finish_norm,
          row_number() over (
            partition by
              target.source,
              target.race_date,
              target.kaisai_nen,
              target.kaisai_tsukihi,
              target.keibajo_code,
              target.race_bango,
              target.umaban
            order by history.race_date desc
          ) recent_rank
        from target
        join race_entry_corner_features history
          on history.source = target.source
          and history.ketto_toroku_bango = target.ketto_toroku_bango
          and history.race_date < target.race_date
          and history.race_date >= (target.race_date::integer - 100000)::text
          and history.finish_norm is not null
      ),
      history_summary as (
        select
          source,
          race_date,
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          race_bango,
          umaban,
          avg(finish_norm)::text avg_finish,
          avg(finish_norm) filter (where recent_rank <= 5)::text recent_finish
        from history
        group by source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban
      )
      select
        target.source,
        target.race_date,
        target.kaisai_nen,
        target.kaisai_tsukihi,
        target.keibajo_code,
        target.race_bango,
        target.umaban "horseNumber",
        target.finish_position,
        history_summary.avg_finish,
        history_summary.recent_finish,
        case
          when target.runner_count > 1 and target.tansho_ninkijun is not null
          then greatest(0, least(1, (target.tansho_ninkijun - 1)::numeric / nullif(target.runner_count - 1, 0)))::text
          else null
        end popularity_score,
        case
          when target.tansho_odds is not null and target.tansho_odds > 0
          then greatest(0, least(1, ln(greatest(target.tansho_odds, 1)) / ln(300)))::text
          else null
        end odds_score
      from target
      left join history_summary
        on history_summary.source = target.source
        and history_summary.race_date = target.race_date
        and history_summary.kaisai_nen = target.kaisai_nen
        and history_summary.kaisai_tsukihi = target.kaisai_tsukihi
        and history_summary.keibajo_code = target.keibajo_code
        and history_summary.race_bango = target.race_bango
        and history_summary.umaban = target.umaban
      where target.runner_count >= 5
      order by target.race_date desc, target.source, target.keibajo_code, target.race_bango, target.umaban
    `,
    [options.fromDate, options.toDate],
  );
  const grouped = new Map<string, PredictionQueryRow[]>();
  for (const row of result.rows) {
    const key = raceKey(row);
    grouped.set(key, [...(grouped.get(key) ?? []), row]);
  }
  return [...grouped.values()].map((rows) =>
    rows
      .map((row) => ({
        actual: row.finish_position,
        horseNumber: row.horseNumber,
        predictedRank: 0,
        score: scorePrediction(row),
      }))
      .toSorted((left, right) => left.score - right.score || left.horseNumber - right.horseNumber)
      .map((row, index) =>
        Object.assign(row, {
          predictedRank: index + 1,
        }),
      ),
  );
};

const main = async () => {
  const options = parseArgs(process.argv.slice(2));
  await loadEnv();
  const pool = new Pool({ connectionString: getConnectionString(options.target) });
  try {
    const evaluated = (await loadPredictions(pool, options)).filter((rows) => rows.length > 0);
    const evaluationSummary = calculateEvaluationSummary(evaluated);
    const pairScores = evaluated.map((rows) => {
      let correct = 0;
      let total = 0;
      for (let left = 0; left < rows.length; left += 1) {
        for (let right = left + 1; right < rows.length; right += 1) {
          const leftRow = rows[left];
          const rightRow = rows[right];
          if (leftRow === undefined || rightRow === undefined) {
            continue;
          }
          total += 1;
          const predicted = leftRow.predictedRank < rightRow.predictedRank;
          const actual = leftRow.actual < rightRow.actual;
          if (predicted === actual) {
            correct += 1;
          }
        }
      }
      return total > 0 ? correct / total : 0;
    });
    const pairScore =
      pairScores.length > 0
        ? pairScores.reduce((total, score) => total + score, 0) / pairScores.length
        : 0;
    console.log(
      JSON.stringify(
        {
          category: options.category,
          fromDate: options.fromDate,
          pairScore: Math.round(pairScore * 10000) / 100,
          place1Accuracy: evaluationSummary.place1Accuracy,
          place2Accuracy: evaluationSummary.place2Accuracy,
          place3Accuracy: evaluationSummary.place3Accuracy,
          raceCount: evaluated.length,
          target: options.target,
          toDate: options.toDate,
          top1Accuracy: evaluationSummary.place1Accuracy,
          top3BoxAccuracy: evaluationSummary.top3BoxAccuracy,
          top3ExactOrderAccuracy: evaluationSummary.top3ExactOrderAccuracy,
          top3PlaceRelation: evaluationSummary.top3PlaceRelation,
          top3WinnerCapture: evaluationSummary.top3WinnerCapture,
          top5WinnerCapture: evaluationSummary.top5WinnerCapture,
        },
        null,
        2,
      ),
    );
  } finally {
    await pool.end();
  }
};

main().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
