// Runs with bun. Matched-race selection and target-profile averages for the
// time-score reader, expressed for R2 SQL (no `~`, no `regexp_replace`).
import { digitsOnlyNumericSql, monthWindowConditionSql, raceTimeTenthsSql } from "./time-score-sql";

export type MatchedProfileSource = "jra" | "nar";

export interface MatchedProfileRace {
  keibajoCode: string;
  kyori: string | null;
  kyosoShubetsuCode: string | null;
  kyosoJokenCode: string | null;
  kyosoJokenMeisho: string | null;
  trackCode: string | null;
  gradeCode: string | null;
  kyosomeiHondai: string | null;
}

export interface MatchedProfileSettings {
  includeVenue: boolean;
  includeDistance: boolean;
  includeAge: boolean;
  includeClass: boolean;
  includeConditionKey: boolean;
  includeTrackCode: boolean;
  includeGrade: boolean;
  includeRaceTitle: boolean;
  includeMonthWindow: boolean;
  includeRunnerCount: boolean;
  runnerCount: number | null;
}

export interface MatchedProfileInput {
  namespace: string;
  source: MatchedProfileSource;
  raceDate: string;
  years: number | null;
  race: MatchedProfileRace;
  settings: MatchedProfileSettings;
  limit: number;
}

export interface TargetProfile {
  targetRaceTime: number | null;
  targetLast3f: number | null;
  targetBodyWeight: number | null;
  targetCarriedWeight: number | null;
  targetMargin: number | null;
}

// Mirrors the viewer's CELL_CONDITION_LABEL_PAIRS; both sides of the
// comparison use this same key, so the labels only need to be consistent.
const CELL_CONDITION_LABEL_PAIRS: readonly (readonly [string, string])[] = [
  ["005", "1勝クラス"],
  ["010", "2勝クラス"],
  ["016", "3勝クラス"],
  ["701", "新馬"],
  ["702", "未出走"],
  ["703", "未勝利"],
  ["999", "オープン"],
];
const IDENTIFIER: RegExp = /^[A-Za-z_][A-Za-z0-9_]*$/u;
const DATE: RegExp = /^\d{8}$/u;
const MAX_LIMIT: number = 5000;
const TOP3_POSITIONS: string = "'01', '02', '03'";

const validDate = (date: string): boolean => {
  if (!DATE.test(date)) return false;
  const iso: string = `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}`;
  const parsed: Date = new Date(`${iso}T00:00:00.000Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === iso;
};

const quote = (value: string | null): string | null => {
  if (value === null || value.trim() === "") return null;
  return `'${value.replaceAll("'", "''")}'`;
};

const compactDate = (date: string): string => date.slice(0, 4);

const conditionKeySql = (codeColumn: string, meishoColumn: string): string => {
  const whens: string = CELL_CONDITION_LABEL_PAIRS.map(
    ([code, label]) => `WHEN ${codeColumn} = '${code}' THEN '${label}'`,
  ).join(" ");
  return `CASE ${whens} ELSE nullif(split_part(trim(${meishoColumn}), ' ', 1), '') END`;
};

export const buildMatchedRacePredicates = (input: MatchedProfileInput): string[] => {
  const settings: MatchedProfileSettings = input.settings;
  const race: MatchedProfileRace = input.race;
  const predicates: string[] = [];
  if (settings.includeVenue) predicates.push(`ra.keibajo_code = '${race.keibajoCode}'`);
  const distance: string | null = quote(race.kyori);
  if (settings.includeDistance && distance !== null) predicates.push(`ra.kyori = ${distance}`);
  const age: string | null = quote(race.kyosoShubetsuCode);
  if (settings.includeAge && age !== null) predicates.push(`ra.kyoso_shubetsu_code = ${age}`);
  const klass: string | null = quote(race.kyosoJokenCode);
  if (settings.includeClass && klass !== null) predicates.push(`ra.kyoso_joken_code = ${klass}`);
  if (settings.includeConditionKey) {
    const currentCode: string = `'${(race.kyosoJokenCode ?? "").replaceAll("'", "''")}'`;
    const currentMeisho: string = `'${(race.kyosoJokenMeisho ?? "").replaceAll("'", "''")}'`;
    predicates.push(
      `${conditionKeySql("ra.kyoso_joken_code", "ra.kyoso_joken_meisho")} IS NOT DISTINCT FROM ${conditionKeySql(currentCode, currentMeisho)}`,
    );
  }
  const track: string | null = quote(race.trackCode);
  if (settings.includeTrackCode)
    predicates.push(`ra.track_code IS NOT DISTINCT FROM ${track ?? "NULL"}`);
  const grade: string | null = quote(race.gradeCode);
  if (settings.includeGrade)
    predicates.push(`ra.grade_code IS NOT DISTINCT FROM ${grade ?? "NULL"}`);
  if (settings.includeRaceTitle) {
    const title: string | null = quote(
      race.kyosomeiHondai === null ? null : race.kyosomeiHondai.replace(/^[\s　]+|[\s　]+$/gu, ""),
    );
    predicates.push(
      `(ra.grade_code IN ('A', 'F') AND nullif(replace(btrim(coalesce(ra.kyosomei_hondai, '')), chr(12288), ''), '') IS NOT DISTINCT FROM ${title ?? "NULL"})`,
    );
  }
  const month: string | null = monthWindowConditionSql(
    "ra.kaisai_tsukihi",
    input.raceDate,
    settings.includeMonthWindow,
  );
  if (month !== null) predicates.push(month);
  if (settings.includeRunnerCount && settings.runnerCount !== null) {
    predicates.push(`(SELECT count(*) FROM ${input.namespace}.${runnerTable(input)} rc
    WHERE rc.kaisai_nen = ra.kaisai_nen AND rc.kaisai_tsukihi = ra.kaisai_tsukihi
      AND rc.keibajo_code = ra.keibajo_code AND rc.race_bango = ra.race_bango) = ${String(settings.runnerCount)}`);
  }
  return predicates;
};

const runnerTable = (input: MatchedProfileInput): string =>
  input.source === "jra" ? "jvd_se" : "nvd_se";
const raceTable = (input: MatchedProfileInput): string =>
  input.source === "jra" ? "jvd_ra" : "nvd_ra";

const requireInput = (input: MatchedProfileInput): void => {
  if (
    !IDENTIFIER.test(input.namespace) ||
    (input.source !== "jra" && input.source !== "nar") ||
    !validDate(input.raceDate) ||
    (input.years !== null && (!Number.isSafeInteger(input.years) || input.years <= 0)) ||
    !Number.isSafeInteger(input.limit) ||
    input.limit <= 0 ||
    input.limit > MAX_LIMIT
  )
    throw new Error("Invalid matched profile input");
};

export const buildMatchedRacesSql = (input: MatchedProfileInput): string => {
  requireInput(input);
  const predicates: string[] = [
    `concat(ra.kaisai_nen, ra.kaisai_tsukihi) < '${input.raceDate}'`,
    ...(input.years === null
      ? []
      : [
          `concat(ra.kaisai_nen, ra.kaisai_tsukihi) >= '${String(Number(compactDate(input.raceDate)) - input.years)}${input.raceDate.slice(4)}'`,
        ]),
    ...buildMatchedRacePredicates(input),
  ];
  return `SELECT ra.kaisai_nen, ra.kaisai_tsukihi, ra.keibajo_code, ra.race_bango
FROM ${input.namespace}.${raceTable(input)} ra
WHERE ${predicates.join(" AND ")}
ORDER BY ra.kaisai_nen DESC, ra.kaisai_tsukihi DESC, ra.race_bango ASC
LIMIT ${input.limit + 1}`;
};

export const buildTargetProfileSql = (input: MatchedProfileInput): string => {
  const matched: string = buildMatchedRacesSql({ ...input, limit: MAX_LIMIT });
  return `WITH matched AS (${matched}), runners AS (
  SELECT ${raceTimeTenthsSql("se.soha_time")} AS race_time,
    ${digitsOnlyNumericSql("se.kohan_3f")} AS last3f,
    ${digitsOnlyNumericSql("se.bataiju")} AS body_weight,
    ${digitsOnlyNumericSql("se.futan_juryo")} AS carried_weight,
    ${digitsOnlyNumericSql("se.time_sa")} AS margin
  FROM ${input.namespace}.${runnerTable(input)} se
  INNER JOIN matched m
    ON m.kaisai_nen = se.kaisai_nen AND m.kaisai_tsukihi = se.kaisai_tsukihi
    AND m.keibajo_code = se.keibajo_code AND m.race_bango = se.race_bango
  WHERE se.kakutei_chakujun IN (${TOP3_POSITIONS})
)
SELECT avg(race_time) AS target_race_time, avg(last3f) AS target_last3f,
  avg(body_weight) AS target_body_weight, avg(carried_weight) AS target_carried_weight,
  avg(margin) AS target_margin, count(*) AS matched_runners
FROM runners`;
};

const numberOrNull = (value: unknown): number | null => {
  if (value === null) return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value !== "" && Number.isFinite(Number(value)))
    return Number(value);
  throw new Error("Invalid target profile value");
};

export const readTargetProfile = async (
  input: MatchedProfileInput,
  query: (sql: string) => Promise<unknown[]>,
): Promise<TargetProfile> => {
  requireInput(input);
  const rows: unknown[] = await query(buildTargetProfileSql(input));
  if (rows.length !== 1) throw new Error("Invalid target profile result");
  const row: unknown = rows[0];
  if (typeof row !== "object" || row === null || Array.isArray(row))
    throw new Error("Invalid target profile row");
  return {
    targetRaceTime: numberOrNull(Reflect.get(row, "target_race_time")),
    targetLast3f: numberOrNull(Reflect.get(row, "target_last3f")),
    targetBodyWeight: numberOrNull(Reflect.get(row, "target_body_weight")),
    targetCarriedWeight: numberOrNull(Reflect.get(row, "target_carried_weight")),
    targetMargin: numberOrNull(Reflect.get(row, "target_margin")),
  };
};
