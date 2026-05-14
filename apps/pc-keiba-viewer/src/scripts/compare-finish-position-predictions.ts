import { Pool } from "pg";

import { getConnectionString, loadEnv } from "./compare-corner-predictions";

type Category = "all" | "ban-ei" | "jra" | "nar";

type Options = {
  breakdown: boolean;
  category: Category;
  changedRaceLimit: number;
  changedRaces: boolean;
  concurrency: number;
  fromDate: string;
  historyWeightMultiplier: number;
  oddsWeightMultiplier: number;
  popularityWeightMultiplier: number;
  recentWeightMultiplier: number;
  sameDayJockeyWeight: number;
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
  conditionBand: string;
  distanceBand: string;
  gradeBand: string;
  horseNumber: number;
  predictedRank: number;
  raceKey: string;
  score: number;
  source: string;
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
  grade_code: string | null;
  horseNumber: number;
  kyori: number | null;
  kyoso_joken_code: string | null;
  odds_score: string | null;
  popularity_score: string | null;
  recent_finish: string | null;
  same_day_jockey_win_score: string | null;
};

type BaneiPredictionQueryRow = {
  finish_position: number;
  horseNumber: number;
  odds_score: string | null;
  popularity_score: string | null;
  race_key: string;
  same_day_jockey_win_score: string | null;
};

const today = new Date();
const defaultToDate = today.toISOString().slice(0, 10).replaceAll("-", "");
const defaultFromDate = new Date(today);
defaultFromDate.setFullYear(defaultFromDate.getFullYear() - 10);

const parseArgs = (args: string[]): Options => {
  const options: Options = {
    breakdown: false,
    category: "all",
    changedRaceLimit: 40,
    changedRaces: false,
    concurrency: 6,
    fromDate: defaultFromDate.toISOString().slice(0, 10).replaceAll("-", ""),
    historyWeightMultiplier: 1,
    oddsWeightMultiplier: 1,
    popularityWeightMultiplier: 1,
    recentWeightMultiplier: 1,
    sameDayJockeyWeight: 0.02,
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
    } else if (name === "--same-day-jockey-weight") {
      options.sameDayJockeyWeight = Math.max(0, Number(value));
      index += 1;
    } else if (name === "--history-weight-multiplier") {
      options.historyWeightMultiplier = Math.max(0, Number(value));
      index += 1;
    } else if (name === "--recent-weight-multiplier") {
      options.recentWeightMultiplier = Math.max(0, Number(value));
      index += 1;
    } else if (name === "--popularity-weight-multiplier") {
      options.popularityWeightMultiplier = Math.max(0, Number(value));
      index += 1;
    } else if (name === "--odds-weight-multiplier") {
      options.oddsWeightMultiplier = Math.max(0, Number(value));
      index += 1;
    } else if (name === "--breakdown") {
      options.breakdown = true;
    } else if (name === "--changed-races") {
      options.changedRaces = true;
    } else if (name === "--changed-race-limit") {
      options.changedRaceLimit = Math.max(1, Number(value));
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
  --same-day-jockey-weight N
  --history-weight-multiplier N
  --recent-weight-multiplier N
  --popularity-weight-multiplier N
  --odds-weight-multiplier N
  --breakdown
  --changed-races
  --changed-race-limit N

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

const gradedRaceCodes = new Set(["A", "B", "C", "D", "E", "F", "G", "H", "L"]);

const isNarClassRace = (row: PredictionQueryRow): boolean => {
  const code = row.kyoso_joken_code?.trim() ?? "";
  return code === "000" || /^[ABC]\d?/u.test(code);
};

const getDistanceBand = (distance: number | null): "long" | "middle" | "sprint" | "unknown" => {
  if (distance === null || !Number.isFinite(distance)) {
    return "unknown";
  }
  if (distance <= 1400) {
    return "sprint";
  }
  if (distance >= 2000) {
    return "long";
  }
  return "middle";
};

const getGradeBand = (gradeCode: string | null | undefined): string =>
  gradedRaceCodes.has(gradeCode?.trim() ?? "") ? "graded" : "non_graded";

const getConditionBand = (row: PredictionQueryRow): string => {
  if (row.source === "jra") {
    return getGradeBand(row.grade_code);
  }
  if (row.keibajo_code === "83") {
    return "ban_ei";
  }
  return isNarClassRace(row) ? "nar_class" : "nar_non_class";
};

const getScoreWeights = (row: PredictionQueryRow, options: Options) => {
  const distanceBand = getDistanceBand(row.kyori);
  const appliesRequestedMultipliers = row.source !== "nar" || distanceBand !== "sprint";
  const marketPopularityMultiplier = 1.1;
  const marketOddsMultiplier = row.source === "jra" ? 1.1 : 1;
  const historyMultiplier = row.source === "jra" ? 0.9 : 1;
  const recentMultiplier = row.source === "jra" ? 0.9 : 1;
  const weights = {
    avgFinish:
      0.18 *
      historyMultiplier *
      (appliesRequestedMultipliers ? options.historyWeightMultiplier : 1),
    odds:
      0.12 *
      marketOddsMultiplier *
      (appliesRequestedMultipliers ? options.oddsWeightMultiplier : 1),
    popularity:
      0.6 *
      marketPopularityMultiplier *
      (appliesRequestedMultipliers ? options.popularityWeightMultiplier : 1),
    recentFinish:
      0.1 * recentMultiplier * (appliesRequestedMultipliers ? options.recentWeightMultiplier : 1),
    sameDayJockey: row.source === "nar" ? options.sameDayJockeyWeight : 0,
  };
  const graded = gradedRaceCodes.has(row.grade_code?.trim() ?? "");
  const narClass = row.source === "nar" && isNarClassRace(row);

  if (graded) {
    weights.sameDayJockey *= row.source === "nar" ? 0.7 : 0;
  } else if (narClass) {
    weights.sameDayJockey *= 0.8;
  }

  if (distanceBand === "sprint") {
    weights.sameDayJockey *= 1.2;
  } else if (distanceBand === "long") {
    weights.sameDayJockey *= 0.75;
  }

  return weights;
};

const scorePrediction = (row: PredictionQueryRow, options: Options): number => {
  const weights = getScoreWeights(row, options);
  const values = [
    { value: toNumber(row.avg_finish), weight: weights.avgFinish },
    { value: toNumber(row.recent_finish), weight: weights.recentFinish },
    { value: toNumber(row.popularity_score), weight: weights.popularity },
    { value: toNumber(row.odds_score), weight: weights.odds },
    {
      value: row.source === "nar" ? toNumber(row.same_day_jockey_win_score) : null,
      weight: weights.sameDayJockey,
    },
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

const calculateBreakdowns = (evaluated: Prediction[][]) => {
  const groups = new Map<string, Prediction[][]>();
  for (const rows of evaluated) {
    const first = rows[0];
    if (!first) {
      continue;
    }
    const keys = [
      `source:${first.source}`,
      `grade:${first.gradeBand}`,
      `distance:${first.distanceBand}`,
      `condition:${first.conditionBand}`,
      `condition_distance:${first.conditionBand}:${first.distanceBand}`,
    ];
    for (const key of keys) {
      groups.set(key, [...(groups.get(key) ?? []), rows]);
    }
  }
  return [...groups.entries()]
    .map(([key, rows]) => ({
      key,
      raceCount: rows.length,
      ...calculateEvaluationSummary(rows),
    }))
    .filter((row) => row.raceCount >= 200)
    .toSorted(
      (left, right) => left.key.localeCompare(right.key) || right.raceCount - left.raceCount,
    );
};

const isExactTop3 = (rows: Prediction[]): boolean =>
  rows[0]?.actual === 1 && rows[1]?.actual === 2 && rows[2]?.actual === 3;

const getTop3Actuals = (rows: Prediction[]): number[] => rows.slice(0, 3).map((row) => row.actual);

const getTop3HorseNumbers = (rows: Prediction[]): number[] =>
  rows.slice(0, 3).map((row) => row.horseNumber);

const countChangedGroups = (
  rows: Array<{ conditionBand: string; distanceBand: string; gradeBand: string; outcome: string }>,
) => {
  const groups = new Map<string, { improved: number; worsened: number }>();
  for (const row of rows) {
    const keys = [
      `grade:${row.gradeBand}`,
      `distance:${row.distanceBand}`,
      `condition:${row.conditionBand}`,
      `condition_distance:${row.conditionBand}:${row.distanceBand}`,
    ];
    for (const key of keys) {
      const current = groups.get(key) ?? { improved: 0, worsened: 0 };
      if (row.outcome === "improved") {
        current.improved += 1;
      } else {
        current.worsened += 1;
      }
      groups.set(key, current);
    }
  }
  return [...groups.entries()]
    .map(([key, value]) => ({
      key,
      ...value,
      net: value.improved - value.worsened,
    }))
    .toSorted((left, right) => right.net - left.net || right.improved - left.improved);
};

const calculateChangedRaces = (
  baseline: Prediction[][],
  candidate: Prediction[][],
  limit: number,
) => {
  const baselineByKey = new Map(
    baseline.flatMap((rows) => (rows[0] ? [[rows[0].raceKey, rows] as const] : [])),
  );
  const changed = candidate.flatMap((candidateRows) => {
    const first = candidateRows[0];
    if (!first) {
      return [];
    }
    const baselineRows = baselineByKey.get(first.raceKey);
    if (!baselineRows) {
      return [];
    }
    const baselineExact = isExactTop3(baselineRows);
    const candidateExact = isExactTop3(candidateRows);
    if (baselineExact === candidateExact) {
      return [];
    }
    return [
      {
        baselineTop3Actuals: getTop3Actuals(baselineRows),
        baselineTop3HorseNumbers: getTop3HorseNumbers(baselineRows),
        candidateTop3Actuals: getTop3Actuals(candidateRows),
        candidateTop3HorseNumbers: getTop3HorseNumbers(candidateRows),
        conditionBand: first.conditionBand,
        distanceBand: first.distanceBand,
        gradeBand: first.gradeBand,
        outcome: candidateExact ? "improved" : "worsened",
        raceKey: first.raceKey,
        source: first.source,
      },
    ];
  });
  const improved = changed.filter((row) => row.outcome === "improved");
  const worsened = changed.filter((row) => row.outcome === "worsened");
  return {
    groups: countChangedGroups(changed),
    improvedCount: improved.length,
    improvedSamples: improved.slice(0, limit),
    worsenedCount: worsened.length,
    worsenedSamples: worsened.slice(0, limit),
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
            coalesce(nullif(btrim(se.kishumei_ryakusho, ' 　'), ''), '-') jockey_name,
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
        ),
        winner_rows as (
          select
            race_key,
            split_part(race_key, ':', 1) kaisai_key,
            split_part(race_key, ':', 2) keibajo_code,
            split_part(race_key, ':', 3) race_bango,
            jockey_name
          from target
          where finish_position = 1
        ),
        same_day_jockey_wins as (
          select
            target.race_key,
            target.umaban,
            count(winner_rows.*) win_count
          from target
          left join winner_rows
            on winner_rows.kaisai_key = split_part(target.race_key, ':', 1)
            and winner_rows.keibajo_code = split_part(target.race_key, ':', 2)
            and winner_rows.race_bango::integer < split_part(target.race_key, ':', 3)::integer
            and winner_rows.jockey_name = target.jockey_name
          group by target.race_key, target.umaban
        )
        select
          target.race_key,
          target.umaban "horseNumber",
          target.finish_position,
          case
            when target.runner_count > 1 and target.tansho_ninkijun is not null
            then greatest(0, least(1, (target.tansho_ninkijun - 1)::numeric / nullif(target.runner_count - 1, 0)))::text
            else null
          end popularity_score,
          case
            when target.tansho_odds is not null and target.tansho_odds > 0
            then greatest(0, least(1, ln(greatest(target.tansho_odds, 1)) / ln(300)))::text
            else null
          end odds_score,
          case
            when same_day_jockey_wins.win_count > 0
            then greatest(0, least(1, 0.28 - least(3, same_day_jockey_wins.win_count) * 0.07))::text
            else null
          end same_day_jockey_win_score
        from target
        left join same_day_jockey_wins
          on same_day_jockey_wins.race_key = target.race_key
          and same_day_jockey_wins.umaban = target.umaban
        where target.runner_count >= 5
        order by target.race_key, target.umaban
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
            {
              value: toNumber(row.popularity_score),
              weight: 0.72 * options.popularityWeightMultiplier,
            },
            { value: toNumber(row.odds_score), weight: 0.28 * options.oddsWeightMultiplier },
            { value: null, weight: 0 },
          ].filter((item): item is { value: number; weight: number } => item.value !== null);
          const weightTotal = values.reduce((total, item) => total + item.weight, 0);
          const score =
            weightTotal > 0
              ? values.reduce((total, item) => total + item.value * item.weight, 0) / weightTotal
              : 0.5;
          return {
            actual: row.finish_position,
            conditionBand: "ban_ei",
            distanceBand: "unknown",
            gradeBand: "non_graded",
            horseNumber: row.horseNumber,
            predictedRank: 0,
            raceKey: row.race_key,
            score,
            source: "ban-ei",
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
          grade_code,
          nullif(btrim(kyori::text), '')::integer kyori,
          kyoso_joken_code,
          ketto_toroku_bango,
          umaban,
          finish_position,
          finish_norm,
          tansho_ninkijun,
          tansho_odds,
          coalesce(nullif(btrim(kishumei_ryakusho, ' 　'), ''), '-') jockey_name,
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
      ),
      winner_rows as (
        select
          source,
          race_date,
          keibajo_code,
          race_bango,
          jockey_name
        from target
        where finish_position = 1
      ),
      same_day_jockey_wins as (
        select
          target.source,
          target.race_date,
          target.kaisai_nen,
          target.kaisai_tsukihi,
          target.keibajo_code,
          target.race_bango,
          target.umaban,
          count(winner_rows.*) win_count
        from target
        left join winner_rows
          on winner_rows.source = target.source
          and winner_rows.race_date = target.race_date
          and winner_rows.keibajo_code = target.keibajo_code
          and winner_rows.race_bango::integer < target.race_bango::integer
          and winner_rows.jockey_name = target.jockey_name
        group by
          target.source,
          target.race_date,
          target.kaisai_nen,
          target.kaisai_tsukihi,
          target.keibajo_code,
          target.race_bango,
          target.umaban
      )
      select
        target.source,
        target.race_date,
        target.kaisai_nen,
        target.kaisai_tsukihi,
        target.keibajo_code,
        target.race_bango,
        target.grade_code,
        target.kyori,
        target.kyoso_joken_code,
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
        end odds_score,
        case
          when same_day_jockey_wins.win_count > 0
          then greatest(0, least(1, 0.28 - least(3, same_day_jockey_wins.win_count) * 0.07))::text
          else null
        end same_day_jockey_win_score
      from target
      left join history_summary
        on history_summary.source = target.source
        and history_summary.race_date = target.race_date
        and history_summary.kaisai_nen = target.kaisai_nen
        and history_summary.kaisai_tsukihi = target.kaisai_tsukihi
        and history_summary.keibajo_code = target.keibajo_code
        and history_summary.race_bango = target.race_bango
        and history_summary.umaban = target.umaban
      left join same_day_jockey_wins
        on same_day_jockey_wins.source = target.source
        and same_day_jockey_wins.race_date = target.race_date
        and same_day_jockey_wins.kaisai_nen = target.kaisai_nen
        and same_day_jockey_wins.kaisai_tsukihi = target.kaisai_tsukihi
        and same_day_jockey_wins.keibajo_code = target.keibajo_code
        and same_day_jockey_wins.race_bango = target.race_bango
        and same_day_jockey_wins.umaban = target.umaban
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
        conditionBand: getConditionBand(row),
        distanceBand: getDistanceBand(row.kyori),
        gradeBand: getGradeBand(row.grade_code),
        horseNumber: row.horseNumber,
        predictedRank: 0,
        raceKey: raceKey(row),
        score: scorePrediction(row, options),
        source: row.source === "nar" && row.keibajo_code === "83" ? "ban-ei" : row.source,
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
    const output = {
      breakdowns: options.breakdown ? calculateBreakdowns(evaluated) : undefined,
      category: options.category,
      changedRaces: options.changedRaces
        ? calculateChangedRaces(
            await loadPredictions(pool, {
              ...options,
              historyWeightMultiplier: 1,
              oddsWeightMultiplier: 1,
              popularityWeightMultiplier: 1,
              recentWeightMultiplier: 1,
              sameDayJockeyWeight: 0.02,
            }),
            evaluated,
            options.changedRaceLimit,
          )
        : undefined,
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
    };
    console.log(JSON.stringify(output, null, 2));
  } finally {
    await pool.end();
  }
};

main().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
