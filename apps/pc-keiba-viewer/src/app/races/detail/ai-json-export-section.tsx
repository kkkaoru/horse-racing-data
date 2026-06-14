"use client";

import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import type { RaceSource } from "../../../lib/codes";
import { fetchWithRetry } from "../../../lib/fetch-with-retry";
import {
  buildFinishPredictionMarketOverrides,
  buildFinishPredictionRowsFromInputs,
  type CorrectionToggles,
  type FinishPredictionBuildInputs,
  ODDS_POPULARITY_DEFAULT_STRENGTH,
} from "../../../lib/finish-position-prediction";
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

const ALL_CORRECTIONS_ON: CorrectionToggles = {
  formEnabled: true,
  jockeyEnabled: true,
  oddsPopularityStrength: ODDS_POPULARITY_DEFAULT_STRENGTH,
  sameDayJockeyEnabled: true,
  trainerEnabled: true,
};

interface AiJsonExportSectionProps {
  basePostgresqlData: {
    courseInfo: CourseInfo | null;
    race: RaceDetail;
    /**
     * Races that share the current race's venue on the same day. SSR fetches
     * only same-venue rows to keep the response light; if a JSON consumer
     * needs the full cross-venue list, the `/ai/data?parts=raceDayRaces`
     * endpoint returns it on demand.
     */
    sameVenueRaces: RaceListItem[];
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

type SectionPayloads = Record<string, unknown>;

type SupplementalPayloads = {
  errors: Record<string, string>;
  paddock: PaddockState | null;
  realtime: RealtimeRacePayload | null;
};

type FinishPredictionPayloadForExport = {
  evaluation?: unknown;
  inputs: FinishPredictionBuildInputs;
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

type ExportData = {
  aiReady: {
    currentOutput: {
      finishPrediction: ReturnType<typeof buildFinishPredictionOutput> | null;
      overallScore: ReturnType<typeof buildOverallScoreOutput> | null;
      sourceSections: string[];
    };
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
    base: AiJsonExportSectionProps["basePostgresqlData"];
    sections: SectionPayloads | null;
  };
  processedForDisplay: {
    base: AiJsonExportSectionProps["baseProcessedData"];
    sections: SectionPayloads | null;
  };
  realtime: SupplementalPayloads | null;
};

const DYNAMIC_PAYLOAD_REFRESH_MS = 15_000;

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

const isRowsPayload = (
  payload: unknown,
  type: string,
  rowKey: string,
): payload is Record<string, unknown> =>
  isRecord(payload) && payload.type === type && rowKey in payload && Array.isArray(payload[rowKey]);

const isFinishPredictionPayload = (payload: unknown): payload is FinishPredictionPayloadForExport =>
  isRecord(payload) &&
  payload.type === "finish-prediction" &&
  "inputs" in payload &&
  typeof payload.inputs === "object" &&
  payload.inputs !== null;

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

const getCombinedScoreByHorse = (sectionPayloads: SectionPayloads, realtime: unknown) => {
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
  sectionPayloads: SectionPayloads | null,
  supplementalPayloads: SupplementalPayloads | null,
) => {
  const finishPayload = sectionPayloads?.["finish-prediction"];
  if (!isFinishPredictionPayload(finishPayload)) {
    return null;
  }
  const realtime = supplementalPayloads?.realtime ?? null;
  const { entryStatusByHorse, jockeyByHorse, oddsByHorse } = getRealtimeMaps(realtime);
  const tanshoRows = realtime?.odds?.latest.tansho ?? [];
  // JSON export always uses odds-corrected prediction for best-available signal
  const finishRows = buildFinishPredictionRowsFromInputs(
    { ...finishPayload.inputs, correctionToggles: ALL_CORRECTIONS_ON },
    tanshoRows.length > 0 ? buildFinishPredictionMarketOverrides(tanshoRows) : undefined,
  );
  const combinedScoreByHorse = getCombinedScoreByHorse(sectionPayloads ?? {}, realtime);
  const paddockScoreByHorse = buildPaddockScoreByHorse(
    finishRows,
    supplementalPayloads?.paddock ?? null,
  );
  const rows = finishRows
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
  sectionPayloads: SectionPayloads | null,
  supplementalPayloads: SupplementalPayloads | null,
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

const fetchSectionPayloads = async (
  props: Pick<AiJsonExportSectionProps, "day" | "keibajoCode" | "month" | "raceNumber" | "year">,
  sections: string[],
): Promise<SectionPayloads> => {
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

const fetchSupplementalPayloads = async (
  props: Pick<
    AiJsonExportSectionProps,
    "day" | "keibajoCode" | "month" | "raceNumber" | "source" | "year"
  >,
): Promise<SupplementalPayloads> => {
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

export function AiJsonExportSection({
  basePostgresqlData,
  baseProcessedData,
  day,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: AiJsonExportSectionProps) {
  const [copyStatus, setCopyStatus] = useState<"copied" | "error" | "idle">("idle");
  const [jsonText, setJsonText] = useState<string | null>(null);
  const [isGenerating, setIsGenerating] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [showJson, setShowJson] = useState(false);
  const [sectionPayloads, setSectionPayloads] = useState<SectionPayloads | null>(null);
  const [supplementalPayloads, setSupplementalPayloads] = useState<SupplementalPayloads | null>(
    null,
  );
  const copyStatusTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const sections = useMemo(
    () =>
      isCornerPacePredictionSupported({
        distance: basePostgresqlData.race.kyori,
        keibajoCode,
        source,
      })
        ? SECTIONS
        : SECTIONS.filter((section) => section !== "pace-prediction"),
    [basePostgresqlData.race.kyori, keibajoCode, source],
  );

  useEffect(
    () => () => {
      if (copyStatusTimer.current) {
        clearTimeout(copyStatusTimer.current);
      }
    },
    [],
  );

  const exportData = useMemo<ExportData>(
    () => ({
      aiReady: {
        currentOutput: {
          finishPrediction: buildFinishPredictionOutput(sectionPayloads, supplementalPayloads),
          overallScore: buildOverallScoreOutput(sectionPayloads, supplementalPayloads),
          sourceSections: sections,
        },
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
    }),
    [
      basePostgresqlData,
      baseProcessedData,
      day,
      keibajoCode,
      month,
      raceNumber,
      sectionPayloads,
      sections,
      source,
      supplementalPayloads,
      year,
    ],
  );

  const scheduleCopyStatusReset = () => {
    if (copyStatusTimer.current) {
      clearTimeout(copyStatusTimer.current);
    }
    copyStatusTimer.current = setTimeout(() => {
      setCopyStatus("idle");
    }, 1800);
  };

  const loadExportPayloads = useCallback(
    async ({
      refreshDynamic = false,
      refreshSections = false,
      silent = false,
    }: {
      refreshDynamic?: boolean;
      refreshSections?: boolean;
      silent?: boolean;
    } = {}): Promise<{
      sections: SectionPayloads;
      supplemental: SupplementalPayloads;
    }> => {
      if (sectionPayloads && supplementalPayloads && !refreshDynamic && !refreshSections) {
        return { sections: sectionPayloads, supplemental: supplementalPayloads };
      }
      if (!silent) {
        setIsLoading(true);
        setCopyStatus("idle");
      }
      try {
        const [nextSectionPayloads, nextSupplementalPayloads] = await Promise.all([
          sectionPayloads && !refreshSections
            ? sectionPayloads
            : fetchSectionPayloads(
                {
                  day,
                  keibajoCode,
                  month,
                  raceNumber,
                  year,
                },
                sections,
              ),
          supplementalPayloads && !refreshDynamic
            ? supplementalPayloads
            : fetchSupplementalPayloads({
                day,
                keibajoCode,
                month,
                raceNumber,
                source,
                year,
              }),
        ]);
        setSectionPayloads(nextSectionPayloads);
        setSupplementalPayloads(nextSupplementalPayloads);
        return { sections: nextSectionPayloads, supplemental: nextSupplementalPayloads };
      } finally {
        if (!silent) {
          setIsLoading(false);
        }
      }
    },
    [
      day,
      keibajoCode,
      month,
      raceNumber,
      sectionPayloads,
      sections,
      source,
      supplementalPayloads,
      year,
    ],
  );

  const generateJson = useCallback(
    async (payloads: {
      sections: SectionPayloads | null;
      supplemental: SupplementalPayloads | null;
    }): Promise<string> => {
      setIsGenerating(true);
      try {
        await new Promise((resolve) => {
          window.setTimeout(resolve, 0);
        });
        return JSON.stringify(
          {
            ...exportData,
            meta: {
              ...exportData.meta,
              generatedAt: new Date().toISOString(),
            },
            postgresql: {
              ...exportData.postgresql,
              sections: payloads.sections,
            },
            processedForDisplay: {
              ...exportData.processedForDisplay,
              sections: payloads.sections,
            },
            realtime: payloads.supplemental,
            aiReady: {
              currentOutput: {
                finishPrediction: buildFinishPredictionOutput(
                  payloads.sections,
                  payloads.supplemental,
                ),
                overallScore: buildOverallScoreOutput(payloads.sections, payloads.supplemental),
                sourceSections: sections,
              },
            },
          },
          null,
          2,
        );
      } finally {
        setIsGenerating(false);
      }
    },
    [exportData, sections],
  );

  const ensureJson = useCallback(
    async ({
      refreshDynamic = true,
      refreshSections = false,
      silent = false,
    }: {
      refreshDynamic?: boolean;
      refreshSections?: boolean;
      silent?: boolean;
    } = {}): Promise<string> => {
      const payloads = await loadExportPayloads({ refreshDynamic, refreshSections, silent });
      const nextJson = await generateJson(payloads);
      setJsonText(nextJson);
      return nextJson;
    },
    [generateJson, loadExportPayloads],
  );

  useEffect(() => {
    if (!showJson) {
      return undefined;
    }
    const timer = window.setInterval(() => {
      void ensureJson({ refreshDynamic: true, silent: true });
    }, DYNAMIC_PAYLOAD_REFRESH_MS);
    return () => {
      window.clearInterval(timer);
    };
  }, [ensureJson, showJson]);

  const copyJson = async () => {
    try {
      setCopyStatus("idle");
      await navigator.clipboard.writeText(await ensureJson({ refreshDynamic: true }));
      setCopyStatus("copied");
      scheduleCopyStatusReset();
    } catch {
      setCopyStatus("error");
      scheduleCopyStatusReset();
    }
  };

  const toggleJson = async () => {
    if (showJson) {
      setShowJson(false);
      return;
    }
    setShowJson(true);
    await ensureJson({ refreshDynamic: true });
  };

  return (
    <section className="ai-json-export-section">
      <details
        onToggle={(event) => {
          if (event.currentTarget.open) {
            void loadExportPayloads({ refreshDynamic: true });
          }
        }}
      >
        <summary>AI向けJSON出力</summary>
        <div className="ai-json-export-actions">
          <button type="button" onClick={copyJson}>
            JSONをコピー
          </button>
          <button type="button" onClick={() => void toggleJson()}>
            {showJson ? "JSONを隠す" : "JSONを表示"}
          </button>
          <span>
            {isLoading
              ? "取得中"
              : isGenerating
                ? "JSON生成中"
                : copyStatus === "copied"
                  ? "コピーしました"
                  : copyStatus === "error"
                    ? "コピーできませんでした"
                    : sectionPayloads
                      ? supplementalPayloads
                        ? "取得済み"
                        : "取得中"
                      : "未取得"}
          </span>
        </div>
        {showJson && jsonText ? <pre className="ai-json-export-code">{jsonText}</pre> : null}
      </details>
    </section>
  );
}
