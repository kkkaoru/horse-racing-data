"use client";

import type { RealtimeRacePayload } from "horse-racing-realtime/types";

import type { RaceSource } from "../../../lib/codes";
import { fetchWithRetry } from "../../../lib/fetch-with-retry";
import { getPreferredJockeyName } from "../../../lib/jockey-name";
import {
  isPaddockState,
  normalizePaddockHorseScore,
  type PaddockOfficialRank,
  type PaddockState,
} from "../../../lib/paddock";
import { isCornerPacePredictionSupported } from "../../../lib/race-pace-prediction";
import type {
  BloodlineStatsRow,
  ConditionCorrelationRow,
  CourseInfo,
  FinishPredictionRow,
  OverallScoreRow,
  RaceDetail,
  RaceListItem,
  Runner,
  SimilarRaceStatsRow,
  TimeScoreRow,
} from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";
import { buildCombinedScoreRows, type CombinedScoreRow } from "./bloodline-similar-combined-table";
import { buildRealtimeUrl, isRealtimeRacePayload } from "./realtime-client";

export interface RaceAiDataBase {
  basePostgresqlData: {
    courseInfo: CourseInfo | null;
    race: RaceDetail;
    raceDayRaces: RaceListItem[];
    runners: Runner[];
  };
  baseProcessedData: Record<string, unknown>;
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}

export type RaceAiSectionPayloads = Record<string, unknown>;

export type RaceAiSupplementalPayloads = {
  errors: Record<string, string>;
  paddock: PaddockState | null;
  realtime: RealtimeRacePayload | null;
};

export interface RaceAiDataReadinessItem {
  availableUnits: number;
  key: string;
  label: string;
  missingPercent: number;
  missingUnits: number;
  notes: string[];
  preparedPercent: number;
  status: "missing" | "partial" | "ready";
  totalUnits: number;
}

export interface RaceAiDataReadiness {
  items: RaceAiDataReadinessItem[];
  missingPercent: number;
  preparedPercent: number;
  readyItems: number;
  totalItems: number;
}

type FinishPredictionPayloadForExport = {
  evaluation?: unknown;
  rows: FinishPredictionRow[];
  type: "finish-prediction";
};

type OverallScorePayloadForExport = {
  rows: OverallScoreRow[];
  type: "overall-score";
};

type TimeScorePayloadForExport = {
  bloodlineRows: BloodlineStatsRow[];
  correlationRows: ConditionCorrelationRow[];
  rows: TimeScoreRow[];
  runners: Runner[];
  similarRows: SimilarRaceStatsRow[];
  type: "time-score";
};

export type RaceAiExportData = {
  aiReady: {
    currentOutput: {
      finishPrediction: ReturnType<typeof buildFinishPredictionOutput> | null;
      overallScore: ReturnType<typeof buildOverallScoreOutput> | null;
      sourceSections: string[];
    };
    dataReadiness: RaceAiDataReadiness;
  };
  meta: {
    generatedAt: string;
    purpose: string;
    route: {
      day: string;
      keibajoCode: string;
      month: string;
      raceNumber: string;
      source: RaceSource;
      year: string;
    };
  };
  postgresql: {
    base: RaceAiDataBase["basePostgresqlData"];
    sections: RaceAiSectionPayloads | null;
  };
  processedForDisplay: {
    base: RaceAiDataBase["baseProcessedData"];
    sections: RaceAiSectionPayloads | null;
  };
  realtime: RaceAiSupplementalPayloads | null;
};

const SECTIONS = [
  "results",
  "time-score",
  "training",
  "ability",
  "condition",
  "bloodline",
  "similar",
  "pace-prediction",
  "finish-prediction",
  "overall-score",
];

export const getRaceAiSections = ({
  distance,
  keibajoCode,
  source,
}: {
  distance: string | null;
  keibajoCode: string;
  source: RaceSource;
}): string[] =>
  isCornerPacePredictionSupported({
    distance,
    keibajoCode,
    source,
  })
    ? SECTIONS
    : SECTIONS.filter((section) => section !== "pace-prediction");

const getSectionUrl = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  section,
  year,
}: {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  section: string;
  year: string;
}): string => {
  const query = typeof window === "undefined" ? "" : window.location.search;
  return `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/sections/${section}${query}`;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const roundPercent = (value: number): number => Math.round(value * 10) / 10;

const parsePositiveCount = (value: string | null): number | null => {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

const hasText = (value: string | null | undefined): boolean => Boolean(value?.trim());

const isPayloadAvailable = (payload: unknown): boolean =>
  payload !== null && payload !== undefined && (!isRecord(payload) || !("error" in payload));

const getProcessedCourseUnits = (baseProcessedData: Record<string, unknown>): number => {
  const course = isRecord(baseProcessedData.course) ? baseProcessedData.course : null;
  if (!course) {
    return 0;
  }
  return [
    Array.isArray(course.facts) && course.facts.length > 0,
    hasText(typeof course.imagePath === "string" ? course.imagePath : null),
    hasText(typeof course.text === "string" ? course.text : null) ||
      (Array.isArray(course.paragraphs) && course.paragraphs.length > 0),
  ].filter(Boolean).length;
};

const buildReadinessItem = ({
  availableUnits,
  key,
  label,
  notes = [],
  totalUnits,
}: {
  availableUnits: number;
  key: string;
  label: string;
  notes?: string[];
  totalUnits: number;
}): RaceAiDataReadinessItem => {
  const normalizedTotal = Math.max(1, totalUnits);
  const normalizedAvailable = Math.min(Math.max(0, availableUnits), normalizedTotal);
  const preparedPercent = roundPercent((normalizedAvailable / normalizedTotal) * 100);
  const missingPercent = roundPercent(100 - preparedPercent);
  return {
    availableUnits: normalizedAvailable,
    key,
    label,
    missingPercent,
    missingUnits: normalizedTotal - normalizedAvailable,
    notes,
    preparedPercent,
    status:
      normalizedAvailable === normalizedTotal
        ? "ready"
        : normalizedAvailable > 0
          ? "partial"
          : "missing",
    totalUnits: normalizedTotal,
  };
};

const buildRaceAiDataReadiness = ({
  basePostgresqlData,
  baseProcessedData,
  currentOutput,
  sectionPayloads,
  sections,
  supplementalPayloads,
}: {
  basePostgresqlData: RaceAiDataBase["basePostgresqlData"];
  baseProcessedData: RaceAiDataBase["baseProcessedData"];
  currentOutput: RaceAiExportData["aiReady"]["currentOutput"];
  sectionPayloads: RaceAiSectionPayloads | null;
  sections: string[];
  supplementalPayloads: RaceAiSupplementalPayloads | null;
}): RaceAiDataReadiness => {
  const expectedRunnerCount =
    parsePositiveCount(basePostgresqlData.race.shussoTosu) ?? basePostgresqlData.runners.length;
  const processedCourseUnits = getProcessedCourseUnits(baseProcessedData);
  const courseAvailableUnits = basePostgresqlData.courseInfo
    ? 3
    : Math.max(
        processedCourseUnits,
        [
          basePostgresqlData.race.keibajoCode,
          basePostgresqlData.race.trackCode,
          basePostgresqlData.race.kyori,
        ].filter(hasText).length,
      );
  const items: RaceAiDataReadinessItem[] = [
    buildReadinessItem({
      availableUnits: basePostgresqlData.race ? 1 : 0,
      key: "race",
      label: "レース基本情報",
      totalUnits: 1,
    }),
    buildReadinessItem({
      availableUnits: basePostgresqlData.runners.length,
      key: "runners",
      label: "出走馬",
      notes: [`${basePostgresqlData.runners.length}/${Math.max(1, expectedRunnerCount)} 頭`],
      totalUnits: Math.max(1, expectedRunnerCount),
    }),
    buildReadinessItem({
      availableUnits: courseAvailableUnits,
      key: "courseInfo",
      label: "コース情報",
      notes: [
        basePostgresqlData.courseInfo
          ? "DBコース情報あり"
          : `画面用コース情報 ${processedCourseUnits}/3 / 競馬場・馬場・距離 ${
              [
                basePostgresqlData.race.keibajoCode,
                basePostgresqlData.race.trackCode,
                basePostgresqlData.race.kyori,
              ].filter(hasText).length
            }/3`,
      ],
      totalUnits: 3,
    }),
    buildReadinessItem({
      availableUnits: basePostgresqlData.raceDayRaces.length > 0 ? 1 : 0,
      key: "raceDayRaces",
      label: "同日レース一覧",
      notes: [`${basePostgresqlData.raceDayRaces.length} レース`],
      totalUnits: 1,
    }),
    ...sections.map((section) => {
      const payload = sectionPayloads?.[section];
      const error =
        isRecord(payload) && typeof payload.error === "string"
          ? [`取得エラー: ${payload.error}`]
          : [];
      return buildReadinessItem({
        availableUnits: isPayloadAvailable(payload) ? 1 : 0,
        key: `section:${section}`,
        label: `詳細データ: ${section}`,
        notes: error,
        totalUnits: 1,
      });
    }),
    buildReadinessItem({
      availableUnits: supplementalPayloads?.realtime ? 1 : 0,
      key: "realtime",
      label: "リアルタイムデータ",
      notes: supplementalPayloads?.errors.realtime
        ? [`取得エラー: ${supplementalPayloads.errors.realtime}`]
        : [],
      totalUnits: 1,
    }),
    buildReadinessItem({
      availableUnits: supplementalPayloads?.paddock ? 1 : 0,
      key: "paddock",
      label: "パドックデータ",
      notes: supplementalPayloads?.errors.paddock
        ? [`取得エラー: ${supplementalPayloads.errors.paddock}`]
        : [],
      totalUnits: 1,
    }),
    buildReadinessItem({
      availableUnits: currentOutput.finishPrediction ? 1 : 0,
      key: "aiOutput:finishPrediction",
      label: "AI向け着順予測出力",
      totalUnits: 1,
    }),
    buildReadinessItem({
      availableUnits: currentOutput.overallScore ? 1 : 0,
      key: "aiOutput:overallScore",
      label: "AI向け総合スコア出力",
      totalUnits: 1,
    }),
  ];
  const preparedPercent = roundPercent(
    items.reduce((sum, item) => sum + item.preparedPercent, 0) / Math.max(1, items.length),
  );
  return {
    items,
    missingPercent: roundPercent(100 - preparedPercent),
    preparedPercent,
    readyItems: items.filter((item) => item.status === "ready").length,
    totalItems: items.length,
  };
};

const isRowsPayload = (
  payload: unknown,
  type: string,
  rowKey: string,
): payload is Record<string, unknown> =>
  isRecord(payload) && payload.type === type && rowKey in payload && Array.isArray(payload[rowKey]);

const isFinishPredictionPayload = (payload: unknown): payload is FinishPredictionPayloadForExport =>
  isRowsPayload(payload, "finish-prediction", "rows");

const isOverallScorePayload = (payload: unknown): payload is OverallScorePayloadForExport =>
  isRowsPayload(payload, "overall-score", "rows");

const isTimeScorePayload = (payload: unknown): payload is TimeScorePayloadForExport =>
  isRowsPayload(payload, "time-score", "rows") &&
  Array.isArray(payload.bloodlineRows) &&
  Array.isArray(payload.correlationRows) &&
  Array.isArray(payload.runners) &&
  Array.isArray(payload.similarRows);

const normalizeHorseNumber = (value: string): string =>
  value.replace(/^0+/u, "") || (value ? "0" : "");

const normalizeScoreRange = (value: number, min: number, max: number): number => {
  if (max <= min) {
    return 1;
  }
  return (value - min) / (max - min);
};

const normalizeRankRange = (rank: PaddockOfficialRank | null, min: number, max: number): number => {
  if (rank === null) {
    return 0;
  }
  if (max <= min) {
    return 1;
  }
  return (max - rank) / (max - min);
};

const buildPaddockScoreByHorse = (
  rows: FinishPredictionRow[],
  state: PaddockState | null,
): Map<string, number> => {
  if (state === null) {
    return new Map();
  }
  const scoredRows = rows
    .map((row) => {
      const horseNumber = formatRunnerNumber(row.horseNumber);
      const scores = state.horses[horseNumber]
        ? normalizePaddockHorseScore(state.horses[horseNumber], {
            horseName: row.horseName,
            horseNumber,
          })
        : null;
      return { horseNumber, scores };
    })
    .filter(
      (row): row is { horseNumber: string; scores: NonNullable<typeof row.scores> } =>
        row.scores !== null,
    );
  if (scoredRows.length === 0) {
    return new Map();
  }
  if (scoredRows.length === 1) {
    return new Map([[scoredRows[0]?.horseNumber ?? "", 1]]);
  }
  const totals = scoredRows.map((row) => row.scores.total);
  const ranks = scoredRows
    .map((row) => row.scores.officialRank)
    .filter((rank): rank is PaddockOfficialRank => rank !== null);
  const minTotal = Math.min(...totals);
  const maxTotal = Math.max(...totals);
  const minRank = ranks.length > 0 ? Math.min(...ranks) : 1;
  const maxRank = ranks.length > 0 ? Math.max(...ranks) : 1;
  const rawScores = scoredRows.map((row) => {
    const totalScore = normalizeScoreRange(row.scores.total, minTotal, maxTotal);
    const rankScore = normalizeRankRange(row.scores.officialRank, minRank, maxRank);
    return (totalScore + rankScore) / 2;
  });
  const minRaw = Math.min(...rawScores);
  const maxRaw = Math.max(...rawScores);
  return new Map(
    scoredRows.map((row, index) => [
      row.horseNumber,
      normalizeScoreRange(rawScores[index] ?? 0, minRaw, maxRaw),
    ]),
  );
};

const getRealtimeMaps = (payload: unknown) => {
  if (!isRealtimeRacePayload(payload)) {
    return {
      entryStatusByHorse: new Map<string, string>(),
      jockeyByHorse: new Map<string, string>(),
      oddsByHorse: new Map<string, { odds: number | null; popularity: number | null }>(),
    };
  }
  return {
    entryStatusByHorse: new Map(
      (payload.raceEntries?.horses ?? []).map((horse) => [
        formatRunnerNumber(horse.horseNumber),
        horse.status ?? "",
      ]),
    ),
    jockeyByHorse: new Map(
      (payload.raceEntries?.horses ?? []).map((horse) => [
        formatRunnerNumber(horse.horseNumber),
        horse.jockeyName ?? "",
      ]),
    ),
    oddsByHorse: new Map(
      (payload.odds?.latest.tansho ?? []).map((row) => [
        formatRunnerNumber(row.combination),
        { odds: row.odds ?? null, popularity: row.rank ?? null },
      ]),
    ),
  };
};

const getCombinedScoreByHorse = (sectionPayloads: RaceAiSectionPayloads, realtime: unknown) => {
  const timeScorePayload = sectionPayloads["time-score"];
  if (!isTimeScorePayload(timeScorePayload)) {
    return new Map<string, CombinedScoreRow>();
  }
  const { oddsByHorse } = getRealtimeMaps(realtime);
  return new Map(
    buildCombinedScoreRows({
      bloodlineRows: timeScorePayload.bloodlineRows,
      correlationRows: timeScorePayload.correlationRows,
      realtimeValues: new Map(
        [...oddsByHorse].map(([horseNumber, value]) => [normalizeHorseNumber(horseNumber), value]),
      ),
      rows: timeScorePayload.similarRows,
      runners: timeScorePayload.runners,
      timeRows: timeScorePayload.rows,
    }).map((row) => [formatRunnerNumber(row.horseNumber), row]),
  );
};

const buildFinishPredictionOutput = (
  sectionPayloads: RaceAiSectionPayloads | null,
  supplementalPayloads: RaceAiSupplementalPayloads | null,
) => {
  const finishPayload = sectionPayloads?.["finish-prediction"];
  if (!isFinishPredictionPayload(finishPayload)) {
    return null;
  }
  const realtime = supplementalPayloads?.realtime ?? null;
  const { entryStatusByHorse, jockeyByHorse, oddsByHorse } = getRealtimeMaps(realtime);
  const combinedScoreByHorse = getCombinedScoreByHorse(sectionPayloads ?? {}, realtime);
  const paddockScoreByHorse = buildPaddockScoreByHorse(
    finishPayload.rows,
    supplementalPayloads?.paddock ?? null,
  );
  const rows = finishPayload.rows
    .toSorted((left, right) => {
      const leftStatus = entryStatusByHorse.get(formatRunnerNumber(left.horseNumber)) ?? "";
      const rightStatus = entryStatusByHorse.get(formatRunnerNumber(right.horseNumber)) ?? "";
      if (leftStatus !== "" || rightStatus !== "") {
        return leftStatus === rightStatus ? 0 : leftStatus ? 1 : -1;
      }
      return 0;
    })
    .map((row) => {
      const horseNumber = formatRunnerNumber(row.horseNumber);
      const realtimeOdds = oddsByHorse.get(horseNumber);
      const entryStatus = entryStatusByHorse.get(horseNumber) ?? "";
      return {
        confidence: entryStatus ? null : row.confidence,
        details: row.details,
        entryStatus,
        finishPredictionScore: entryStatus ? null : row.score,
        horseName: row.horseName,
        horseNumber,
        jockeyName: getPreferredJockeyName(row.jockeyName, jockeyByHorse.get(horseNumber)) || "-",
        odds: entryStatus ? null : (realtimeOdds?.odds ?? row.storedOdds),
        overallEvaluationScore: entryStatus
          ? null
          : (combinedScoreByHorse.get(horseNumber)?.score ?? null),
        paddockScore: entryStatus ? null : (paddockScoreByHorse.get(horseNumber) ?? null),
        popularity: entryStatus ? null : (realtimeOdds?.popularity ?? row.storedPopularity),
        predictedRank: entryStatus ? null : row.predictedRank,
        showProbability: entryStatus ? null : row.showProbability,
        winProbability: entryStatus ? null : row.winProbability,
      };
    });
  return {
    evaluation:
      isRecord(finishPayload) && "evaluation" in finishPayload ? finishPayload.evaluation : null,
    rows,
  };
};

const buildOverallScoreOutput = (
  sectionPayloads: RaceAiSectionPayloads | null,
  supplementalPayloads: RaceAiSupplementalPayloads | null,
) => {
  const overallPayload = sectionPayloads?.["overall-score"];
  if (!isOverallScorePayload(overallPayload)) {
    return null;
  }
  const { entryStatusByHorse, oddsByHorse } = getRealtimeMaps(supplementalPayloads?.realtime);
  return {
    rows: overallPayload.rows
      .toSorted((left, right) => {
        const leftStatus = entryStatusByHorse.get(formatRunnerNumber(left.horseNumber)) ?? "";
        const rightStatus = entryStatusByHorse.get(formatRunnerNumber(right.horseNumber)) ?? "";
        if (leftStatus !== "" || rightStatus !== "") {
          return leftStatus === rightStatus ? 0 : leftStatus ? 1 : -1;
        }
        return right.score - left.score || Number(left.horseNumber) - Number(right.horseNumber);
      })
      .map((row) => {
        const horseNumber = formatRunnerNumber(row.horseNumber);
        const realtimeOdds = oddsByHorse.get(horseNumber);
        const entryStatus = entryStatusByHorse.get(horseNumber) ?? "";
        return {
          details: entryStatus ? [] : row.details,
          entryStatus,
          horseName: row.horseName,
          horseNumber,
          jockeyName: row.jockeyName,
          odds: entryStatus ? null : (realtimeOdds?.odds ?? row.storedOdds),
          popularity: entryStatus ? null : (realtimeOdds?.popularity ?? row.storedPopularity),
          score: entryStatus ? null : row.score,
        };
      }),
  };
};

export const fetchRaceAiSectionPayloads = async (
  props: Pick<RaceAiDataBase, "day" | "keibajoCode" | "month" | "raceNumber" | "year">,
  sections: string[],
): Promise<RaceAiSectionPayloads> => {
  const entries = await Promise.all(
    sections.map(async (section) => {
      const response = await fetchWithRetry(getSectionUrl({ ...props, section }));
      if (!response.ok) {
        return [
          section,
          {
            error: `${response.status} ${response.statusText}`.trim(),
          },
        ];
      }
      return [section, await response.json()];
    }),
  );
  return Object.fromEntries(entries);
};

const fetchOptionalJson = async (
  url: string | null,
): Promise<{ error?: string; payload: unknown }> => {
  if (!url) {
    return { error: "url_unavailable", payload: null };
  }
  try {
    const response = await fetchWithRetry(url, { cache: "no-store" });
    if (!response.ok) {
      return { error: `${response.status} ${response.statusText}`.trim(), payload: null };
    }
    return { payload: await response.json() };
  } catch (caught) {
    return { error: caught instanceof Error ? caught.message : String(caught), payload: null };
  }
};

export const fetchRaceAiSupplementalPayloads = async (
  props: Pick<RaceAiDataBase, "day" | "keibajoCode" | "month" | "raceNumber" | "source" | "year">,
): Promise<RaceAiSupplementalPayloads> => {
  const [realtime, paddock] = await Promise.all([
    fetchOptionalJson(
      buildRealtimeUrl({
        apiBaseUrl: "",
        day: props.day,
        keibajoCode: props.keibajoCode,
        month: props.month,
        raceNumber: props.raceNumber,
        source: props.source,
        year: props.year,
      }),
    ),
    fetchOptionalJson(
      `/api/races/${props.year}/${props.month}/${props.day}/${props.keibajoCode}/${props.raceNumber}/paddock`,
    ),
  ]);
  return {
    errors: Object.fromEntries(
      [
        realtime.error ? ["realtime", realtime.error] : null,
        paddock.error ? ["paddock", paddock.error] : null,
      ].filter((entry): entry is [string, string] => entry !== null),
    ),
    paddock: isPaddockState(paddock.payload) ? paddock.payload : null,
    realtime: isRealtimeRacePayload(realtime.payload) ? realtime.payload : null,
  };
};

export const buildRaceAiExportData = ({
  basePostgresqlData,
  baseProcessedData,
  day,
  keibajoCode,
  month,
  raceNumber,
  sections,
  sectionPayloads,
  source,
  supplementalPayloads,
  year,
}: RaceAiDataBase & {
  sectionPayloads: RaceAiSectionPayloads | null;
  sections: string[];
  supplementalPayloads: RaceAiSupplementalPayloads | null;
}): RaceAiExportData => {
  const currentOutput = {
    finishPrediction: buildFinishPredictionOutput(sectionPayloads, supplementalPayloads),
    overallScore: buildOverallScoreOutput(sectionPayloads, supplementalPayloads),
    sourceSections: sections,
  };
  return {
    aiReady: {
      currentOutput,
      dataReadiness: buildRaceAiDataReadiness({
        basePostgresqlData,
        baseProcessedData,
        currentOutput,
        sectionPayloads,
        sections,
        supplementalPayloads,
      }),
    },
    meta: {
      generatedAt: new Date().toISOString(),
      purpose: "AIに渡すためのレース詳細ページ表示データ",
      route: {
        day,
        keibajoCode,
        month,
        raceNumber,
        source,
        year,
      },
    },
    postgresql: {
      base: basePostgresqlData,
      sections: sectionPayloads,
    },
    processedForDisplay: {
      base: baseProcessedData,
      sections: sectionPayloads,
    },
    realtime: supplementalPayloads,
  };
};

export const fetchRaceAiExportData = async (props: RaceAiDataBase): Promise<RaceAiExportData> => {
  const sections = getRaceAiSections({
    distance: props.basePostgresqlData.race.kyori,
    keibajoCode: props.keibajoCode,
    source: props.source,
  });
  const [sectionPayloads, supplementalPayloads] = await Promise.all([
    fetchRaceAiSectionPayloads(props, sections),
    fetchRaceAiSupplementalPayloads(props),
  ]);
  return buildRaceAiExportData({
    ...props,
    sectionPayloads,
    sections,
    supplementalPayloads,
  });
};

export const compactRaceAiDataForPrompt = (data: RaceAiExportData): Record<string, unknown> => ({
  aiReady: data.aiReady,
  meta: data.meta,
  postgresql: {
    base: {
      courseInfo: data.postgresql.base.courseInfo,
      race: data.postgresql.base.race,
      runners: data.postgresql.base.runners,
    },
  },
  processedForDisplay: {
    base: data.processedForDisplay.base,
  },
  realtime: data.realtime,
});
