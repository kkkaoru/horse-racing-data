// Run with bun (bunx vitest).
// Race-time correlation rows for condition-history-stats, ported from the
// viewer's PostgreSQL getRaceTimeStats (correlation_base / correlation_rows).
//
// The Catalog used to return an empty correlationRows list, so the viewer fell
// back to that PostgreSQL query (11-25s per race on 2026-09-23). The Catalog
// now reads three small aggregates with R2 SQL and scores the runners here with
// the same weights and formulas.

import { doubleSelect, matchedHistoryArmSql, trimmedNameSql } from "./condition-history-stats";
import type {
  ConditionCorrelationDetail,
  ConditionCorrelationRow,
  R2SqlCatalogConfig,
  WinRateHeatmapStatsFilters,
} from "./types";
import {
  currentRaceCteSql,
  currentRaceIdentitySql,
  currentTables,
  exactHistoryKyoriFilterSql,
  finishPositionSql,
  historyTables,
  tableName,
  unionHistorySql,
  validateFilters,
} from "./win-rate-heatmap-stats";

export interface ConditionCorrelationInput {
  careerRows: ReadonlyArray<Record<string, unknown>>;
  entryRows: ReadonlyArray<Record<string, unknown>>;
  targetRows: ReadonlyArray<Record<string, unknown>>;
}

export interface DayCareer {
  requested: string[];
  rows: Record<string, unknown>[];
}

interface CareerStats {
  showCount: number;
  starts: number;
  winCount: number;
}

interface TargetAverages {
  horseShow: number | null;
  horseWin: number | null;
  odds: number | null;
  popularity: number | null;
}

interface RunnerFeatures {
  horseName: string;
  horseNumber: string;
  horseNumberSort: number;
  horseShow: number | null;
  horseWin: number | null;
  jockeyShow: number | null;
  odds: number | null;
  ownerShow: number | null;
  popularity: number | null;
  trainerShow: number | null;
}

interface DetailSpec {
  key: ConditionCorrelationDetail["key"];
  label: string;
  reason: string;
  score: (features: RunnerFeatures, targets: TargetAverages) => number;
  target: (targets: TargetAverages) => number | null;
  value: (features: RunnerFeatures) => number | null;
  weight: number;
}

interface ScoredRow {
  horseNumberSort: number;
  row: ConditionCorrelationRow;
  unroundedScore: number;
}

type CareerKind = "horse" | "jockey" | "owner" | "trainer";

const NEUTRAL_SCORE = 0.5;
const PERCENT = 100;
const ODDS_SCALE = 10;
const POPULARITY_FLOOR = 5;
const ODDS_FLOOR = 10;
const SCORE_DECIMALS = 100;
const VALUE_DECIMALS = 10;
const CAREER_KINDS: readonly CareerKind[] = ["horse", "jockey", "trainer", "owner"];
const CAREER_KEY_COLUMNS: readonly string[] = [
  "horse_key",
  "jockey_key",
  "trainer_key",
  "owner_key",
];
const CAREER_ENTITY_TABLES: readonly string[] = [
  "day_horses",
  "day_jockeys",
  "day_trainers",
  "day_owners",
];

const round = (value: number, scale: number): number => Math.round(value * scale) / scale;

const roundOrNull = (value: number | null, scale: number): number | null =>
  value === null ? null : round(value, scale);

const numberOrNull = (value: unknown): number | null => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const countOf = (value: unknown): number => numberOrNull(value) ?? 0;

// Zero odds/popularity encode unavailable market data, including SQL-decoded sentinels.
const marketNumberOrNull = (value: unknown): number | null => {
  const parsed: number | null = numberOrNull(value);
  return parsed === 0 ? null : parsed;
};

const textOf = (value: unknown): string => (typeof value === "string" ? value : "");

const rateOf = (count: number, starts: number): number | null =>
  starts === 0 ? null : (count * PERCENT) / starts;

const closenessScore = (value: number | null, target: number | null, scale: number): number =>
  value === null || target === null
    ? NEUTRAL_SCORE
    : Math.max(0, 1 - Math.abs(value - target) / scale);

const rateScore = (value: number | null): number =>
  value === null ? NEUTRAL_SCORE : Math.max(0, Math.min(1, value / PERCENT));

const DETAIL_SPECS: readonly DetailSpec[] = [
  {
    key: "horseShow",
    label: "出走馬の複勝率",
    reason: "対象レース1〜3着馬の対象レース前の複勝率平均との差",
    score: (features, targets) => closenessScore(features.horseShow, targets.horseShow, PERCENT),
    target: (targets) => targets.horseShow,
    value: (features) => features.horseShow,
    weight: 0.2,
  },
  {
    key: "horseWin",
    label: "出走馬の勝率",
    reason: "対象レース1〜3着馬の対象レース前の勝率平均との差",
    score: (features, targets) => closenessScore(features.horseWin, targets.horseWin, PERCENT),
    target: (targets) => targets.horseWin,
    value: (features) => features.horseWin,
    weight: 0.1,
  },
  {
    key: "jockeyShow",
    label: "騎手の複勝率",
    reason: "今回騎乗予定騎手の今回レース前の複勝率を評価",
    score: (features) => rateScore(features.jockeyShow),
    target: () => null,
    value: (features) => features.jockeyShow,
    weight: 0.15,
  },
  {
    key: "trainerShow",
    label: "調教師の複勝率",
    reason: "今回出走馬の調教師の今回レース前の複勝率を評価",
    score: (features) => rateScore(features.trainerShow),
    target: () => null,
    value: (features) => features.trainerShow,
    weight: 0.15,
  },
  {
    key: "ownerShow",
    label: "馬主の複勝率",
    reason: "今回出走馬の馬主の今回レース前の複勝率を評価",
    score: (features) => rateScore(features.ownerShow),
    target: () => null,
    value: (features) => features.ownerShow,
    weight: 0.15,
  },
  {
    key: "popularity",
    label: "人気順",
    reason: "対象レース1〜3着馬の人気平均との差",
    score: (features, targets) =>
      closenessScore(
        features.popularity,
        targets.popularity,
        Math.max(targets.popularity ?? 0, POPULARITY_FLOOR),
      ),
    target: (targets) => targets.popularity,
    value: (features) => features.popularity,
    weight: 0.125,
  },
  {
    key: "odds",
    label: "単勝オッズ",
    reason: "対象レース1〜3着馬の単勝オッズ平均との差",
    score: (features, targets) =>
      closenessScore(features.odds, targets.odds, Math.max(targets.odds ?? 0, ODDS_FLOOR)),
    target: (targets) => targets.odds,
    value: (features) => features.odds,
    weight: 0.125,
  },
];

// ---- SQL ----------------------------------------------------------------

// Top-3 finishers of the matched history races (same filters as race time
// stats): win share, show share, and average popularity / odds.
export const buildConditionCorrelationTargetQuery = (
  env: R2SqlCatalogConfig,
  filters: WinRateHeatmapStatsFilters,
): string => {
  const checked = validateFilters(filters);
  return `
WITH ${currentRaceCteSql(env, checked)},
matched_history AS (
  ${unionHistorySql(
    historyTables(checked).map((tables) =>
      matchedHistoryArmSql({
        env,
        extraJoin: "",
        extraWhere: `AND ${finishPositionSql("se")} <= 3
    ${exactHistoryKyoriFilterSql()}`,
        filters: checked,
        selectList: `${finishPositionSql("se")} AS finish_position,
    nullif(${doubleSelect("se.tansho_ninkijun")}, 0) AS popularity,
    nullif(${doubleSelect("se.tansho_odds")}, 0) / ${String(ODDS_SCALE)}.0 AS odds`,
        tables,
      }),
    ),
  )}
)
SELECT
  count(*) AS top3_count,
  sum(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) AS win_count,
  avg(popularity) AS average_popularity,
  avg(odds) AS average_odds
FROM matched_history`;
};

export const buildConditionCorrelationEntriesQuery = (
  env: R2SqlCatalogConfig,
  filters: WinRateHeatmapStatsFilters,
): string => {
  const checked = validateFilters(filters);
  return `
SELECT
  btrim(coalesce(se.umaban, '')) AS umaban,
  ${trimmedNameSql("se.bamei")} AS horse_name,
  nullif(${doubleSelect("se.tansho_ninkijun")}, 0) AS popularity,
  nullif(${doubleSelect("se.tansho_odds")}, 0) / ${String(ODDS_SCALE)}.0 AS odds,
  btrim(coalesce(se.ketto_toroku_bango, '')) AS ketto_toroku_bango,
  ${trimmedNameSql("se.kishumei_ryakusho")} AS jockey_name,
  ${trimmedNameSql("se.chokyoshimei_ryakusho")} AS trainer_name,
  ${trimmedNameSql("se.banushimei")} AS owner_name
FROM ${tableName(env, currentTables(checked.source).runnerTable)} se
WHERE ${currentRaceIdentitySql(checked)}
  AND try_cast(nullif(btrim(coalesce(se.umaban, '')), '') AS INT) IS NOT NULL`;
};

const careerKeysSql = (keys: readonly (string | null)[]): string =>
  keys.map((key) => key ?? "CAST(NULL AS VARCHAR)").join(", ");

const requestedEntitySql = (index: number, table: string, column: string): string =>
  `SELECT ${careerKeysSql(CAREER_KEY_COLUMNS.map((_, position) => (position === index ? column : null)))}, CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT) FROM ${table}`;

// Career (all prior races, no window) starts / wins / top-3 counts are the
// same in every race of a day, so one query serves every race of the source on
// that date. Each prior start is matched against the day's horses, jockeys,
// trainers and owners and aggregated per entity in a single GROUPING SETS scan
// (a per-race UNION of four arms rescanned the whole history four times per
// race). The day's entities are appended with NULL counts so a cached result
// can tell a debut runner (no career rows) from an entrant added after it.
export const buildConditionCorrelationDayCareerQuery = (
  env: R2SqlCatalogConfig,
  filters: WinRateHeatmapStatsFilters,
): string => {
  const checked = validateFilters(filters);
  const runnerTable = tableName(env, currentTables(checked.source).runnerTable);
  return `
WITH day_entries AS (
  SELECT DISTINCT
    btrim(coalesce(ketto_toroku_bango, '')) AS ketto_toroku_bango,
    ${trimmedNameSql("kishumei_ryakusho")} AS jockey_name,
    ${trimmedNameSql("chokyoshimei_ryakusho")} AS trainer_name,
    ${trimmedNameSql("banushimei")} AS owner_name
  FROM ${runnerTable}
  WHERE kaisai_nen = '${checked.date.slice(0, 4)}'
    AND kaisai_tsukihi = '${checked.date.slice(4)}'
),
day_horses AS (
  SELECT DISTINCT ketto_toroku_bango AS horse_key FROM day_entries
  WHERE ketto_toroku_bango <> '' AND regexp_match(ketto_toroku_bango, '^0+$') IS NULL
),
day_jockeys AS (SELECT DISTINCT jockey_name AS jockey_key FROM day_entries),
day_trainers AS (SELECT DISTINCT trainer_name AS trainer_key FROM day_entries),
day_owners AS (SELECT DISTINCT owner_name AS owner_key FROM day_entries),
career_history AS (
  SELECT
    btrim(coalesce(se.ketto_toroku_bango, '')) AS ketto_toroku_bango,
    ${trimmedNameSql("se.kishumei_ryakusho")} AS jockey_name,
    ${trimmedNameSql("se.chokyoshimei_ryakusho")} AS trainer_name,
    ${trimmedNameSql("se.banushimei")} AS owner_name,
    ${finishPositionSql("se")} AS finish_position
  FROM ${runnerTable} se
  WHERE concat(se.kaisai_nen, se.kaisai_tsukihi) < '${checked.date}'
    AND se.kaisai_nen <= '${checked.date.slice(0, 4)}'
    AND ${finishPositionSql("se")} > 0
),
keyed AS (
  SELECT h.horse_key, j.jockey_key, t.trainer_key, o.owner_key, ch.finish_position
  FROM career_history ch
  LEFT JOIN day_horses h ON h.horse_key = ch.ketto_toroku_bango
  LEFT JOIN day_jockeys j ON j.jockey_key = ch.jockey_name
  LEFT JOIN day_trainers t ON t.trainer_key = ch.trainer_name
  LEFT JOIN day_owners o ON o.owner_key = ch.owner_name
  WHERE h.horse_key IS NOT NULL OR j.jockey_key IS NOT NULL
    OR t.trainer_key IS NOT NULL OR o.owner_key IS NOT NULL
)
SELECT ${CAREER_KEY_COLUMNS.join(", ")},
  count(*) AS starts,
  sum(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) AS win_count,
  sum(CASE WHEN finish_position <= 3 THEN 1 ELSE 0 END) AS show_count
FROM keyed
GROUP BY GROUPING SETS (${CAREER_KEY_COLUMNS.map((column) => `(${column})`).join(", ")})
UNION ALL
${unionHistorySql(
  CAREER_ENTITY_TABLES.map((table, index) =>
    requestedEntitySql(index, table, CAREER_KEY_COLUMNS[index]!),
  ),
)}`;
};

const entityKindOf = (row: Record<string, unknown>): [CareerKind, string] | null => {
  const index = CAREER_KEY_COLUMNS.findIndex((column) => typeof row[column] === "string");
  return index < 0 ? null : [CAREER_KINDS[index]!, textOf(row[CAREER_KEY_COLUMNS[index]!])];
};

// Splits the day query into the requested entity keys and per-entity career
// rows shaped like the former per-race query ({kind, entity, counts}).
export const normaliseDayCareerRows = (rows: ReadonlyArray<Record<string, unknown>>): DayCareer =>
  rows.reduce<DayCareer>(
    (career, row) => {
      const entity = entityKindOf(row);
      if (entity === null) return career;
      const [kind, name] = entity;
      return row.starts === null || row.starts === undefined
        ? { ...career, requested: [...career.requested, careerKey(kind, name)] }
        : {
            ...career,
            rows: [
              ...career.rows,
              {
                entity: name,
                kind,
                show_count: row.show_count,
                starts: row.starts,
                win_count: row.win_count,
              },
            ],
          };
    },
    { requested: [], rows: [] },
  );

const entryCareerKeys = (entry: Record<string, unknown>): string[] => {
  const horse = textOf(entry.ketto_toroku_bango);
  return [
    ...(horse === "" || /^0+$/u.test(horse) ? [] : [careerKey("horse", horse)]),
    careerKey("jockey", textOf(entry.jockey_name)),
    careerKey("trainer", textOf(entry.trainer_name)),
    careerKey("owner", textOf(entry.owner_name)),
  ];
};

// A day result computed before an entrant (or a jockey change) was published
// does not cover that runner and must be recomputed.
export const dayCareerCoversEntries = (
  career: DayCareer,
  entryRows: ReadonlyArray<Record<string, unknown>>,
): boolean => {
  const requested = new Set(career.requested);
  return entryRows.every((entry) => entryCareerKeys(entry).every((key) => requested.has(key)));
};

// ---- scoring ------------------------------------------------------------

const careerKey = (kind: string, entity: string): string => `${kind}\u0000${entity}`;

const indexCareerRows = (
  rows: ReadonlyArray<Record<string, unknown>>,
): ReadonlyMap<string, CareerStats> =>
  new Map(
    rows
      .filter((row) => CAREER_KINDS.some((kind) => kind === row.kind))
      .map((row) => [
        careerKey(textOf(row.kind), textOf(row.entity)),
        {
          showCount: countOf(row.show_count),
          starts: countOf(row.starts),
          winCount: countOf(row.win_count),
        },
      ]),
  );

const careerOf = (
  careers: ReadonlyMap<string, CareerStats>,
  kind: CareerKind,
  entity: string,
): CareerStats => careers.get(careerKey(kind, entity)) ?? { showCount: 0, starts: 0, winCount: 0 };

const targetAveragesOf = (row: Record<string, unknown> | undefined): TargetAverages => {
  const top3 = countOf(row?.top3_count);
  return {
    horseShow: top3 === 0 ? null : PERCENT,
    horseWin: rateOf(countOf(row?.win_count), top3),
    odds: marketNumberOrNull(row?.average_odds),
    popularity: marketNumberOrNull(row?.average_popularity),
  };
};

const horseNumberOf = (umaban: string): string => umaban.replace(/^0+/u, "") || "0";

const runnerFeaturesOf = (
  entry: Record<string, unknown>,
  careers: ReadonlyMap<string, CareerStats>,
): RunnerFeatures => {
  const umaban = textOf(entry.umaban);
  const horse = careerOf(careers, "horse", textOf(entry.ketto_toroku_bango));
  const jockey = careerOf(careers, "jockey", textOf(entry.jockey_name));
  const trainer = careerOf(careers, "trainer", textOf(entry.trainer_name));
  const owner = careerOf(careers, "owner", textOf(entry.owner_name));
  return {
    horseName: textOf(entry.horse_name) || "-",
    horseNumber: horseNumberOf(umaban),
    horseNumberSort: Number(umaban),
    horseShow: rateOf(horse.showCount, horse.starts),
    horseWin: rateOf(horse.winCount, horse.starts),
    jockeyShow: rateOf(jockey.showCount, jockey.starts),
    odds: marketNumberOrNull(entry.odds),
    ownerShow: rateOf(owner.showCount, owner.starts),
    popularity: marketNumberOrNull(entry.popularity),
    trainerShow: rateOf(trainer.showCount, trainer.starts),
  };
};

const RAW_VALUE_KEYS: ReadonlySet<ConditionCorrelationDetail["key"]> = new Set([
  "odds",
  "popularity",
]);

const scoreRunner = (features: RunnerFeatures, targets: TargetAverages): ScoredRow => {
  const scored = DETAIL_SPECS.map((spec) => ({ score: spec.score(features, targets), spec }));
  const unroundedScore = scored.reduce((sum, item) => sum + item.score * item.spec.weight, 0);
  return {
    horseNumberSort: features.horseNumberSort,
    row: {
      details: scored.map(({ score, spec }) => ({
        key: spec.key,
        label: spec.label,
        reason: spec.reason,
        score: round(score, SCORE_DECIMALS),
        target: roundOrNull(spec.target(targets), VALUE_DECIMALS),
        value: RAW_VALUE_KEYS.has(spec.key)
          ? spec.value(features)
          : roundOrNull(spec.value(features), VALUE_DECIMALS),
        weight: spec.weight,
      })),
      horseName: features.horseName,
      horseNumber: features.horseNumber,
      score: round(unroundedScore, SCORE_DECIMALS),
    },
    unroundedScore,
  };
};

const compareScoredRows = (left: ScoredRow, right: ScoredRow): number =>
  right.unroundedScore - left.unroundedScore || left.horseNumberSort - right.horseNumberSort;

export const composeConditionCorrelationRows = (
  input: ConditionCorrelationInput,
): ConditionCorrelationRow[] => {
  const careers = indexCareerRows(input.careerRows);
  const targets = targetAveragesOf(input.targetRows[0]);
  return input.entryRows
    .map((entry) => scoreRunner(runnerFeaturesOf(entry, careers), targets))
    .toSorted(compareScoredRows)
    .map((scored) => scored.row);
};
