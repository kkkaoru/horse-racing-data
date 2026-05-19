import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import { NextResponse } from "next/server";

import {
  getRaceDetail,
  getRaceRunners,
  getRaceSourceByRoute,
  getRacesByDate,
  getRaceTrendHistoricalStarterRows,
} from "../../../../../../../../../db/queries";
import {
  buildRaceKey,
  getRaceRunningStylesByRaceKeysFromD1,
  getRaceRunningStylesFromD1,
} from "../../../../../../../../../db/corner-running-style-queries";
import type { RaceSource } from "../../../../../../../../../lib/codes";
import { fetchWithRetry } from "../../../../../../../../../lib/fetch-with-retry";
import { normalizeJockeyNameForComparison } from "../../../../../../../../../lib/jockey-name";
import type {
  RaceDetail,
  RaceListItem,
  RaceTrendDetail,
  RaceTrendPayload,
  RaceTrendRateRow,
  RaceTrendRunningStyle,
  RaceTrendRunningStyleRow,
  RaceTrendStarterRow,
  Runner,
} from "../../../../../../../../../lib/race-types";

interface RouteContext {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

const REALTIME_API_BASE_URL =
  process.env.NEXT_PUBLIC_REALTIME_DATA_API_BASE_URL ?? "https://sync-realtime-data.kkk4oru.com";

const isRaceSource = (value: string | null): value is RaceSource =>
  value === "jra" || value === "nar";

const normalizeText = (value: string | null | undefined): string | null => {
  const normalized = value?.trim();
  return normalized ? normalized : null;
};

const normalizeNumberText = (value: string | null | undefined): string | null => {
  const normalized = normalizeText(value);
  if (!normalized) {
    return null;
  }
  return normalized.replace(/^0+(?=\d)/, "");
};

const parseStoredInteger = (
  value: string | null | undefined,
  emptyValue: string,
): number | null => {
  const normalized = normalizeText(value);
  if (!normalized || normalized === emptyValue) {
    return null;
  }
  const parsed = Number(normalized.replace(/[^\d]/g, ""));
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

const parseStoredPopularity = (value: string | null | undefined): number | null =>
  parseStoredInteger(value, "00");

const parseStoredWinOdds = (value: string | null | undefined): number | null => {
  const odds = parseStoredInteger(value, "0000");
  return odds === null ? null : odds / 10;
};

const parseCornerPosition = (value: string | null | undefined): number | null =>
  parseStoredInteger(value, "00");

const formatRealtimeInteger = (value: number | null | undefined): string | null =>
  typeof value === "number" && Number.isFinite(value) && value > 0
    ? String(Math.trunc(value))
    : null;

const formatRealtimeWinOdds = (value: number | null | undefined): string | null =>
  typeof value === "number" && Number.isFinite(value) && value > 0
    ? String(Math.round(value * 10))
    : null;

const isNonEmptyString = (value: string | null): value is string => value !== null && value !== "";

const isRealtimeRacePayload = (value: unknown): value is RealtimeRacePayload =>
  typeof value === "object" && value !== null && "raceResults" in value;

const normalizeRaceTrendJockeyName = (value: string | null | undefined): string | null => {
  const normalized = normalizeJockeyNameForComparison(value);
  return normalized === "" ? null : normalized;
};

const getJockeyNameAliases = (value: string): string[] => {
  if (normalizeRaceTrendJockeyName(value) !== "デムーロ") {
    return [value];
  }
  return [value, "デムーロ", "Ｍ．デム", "M.デム"];
};

const toYmd = (year: string, monthDay: string): string => `${year}${monthDay}`;

const toIsoDate = (ymd: string): string =>
  `${ymd.slice(0, 4)}-${ymd.slice(4, 6)}-${ymd.slice(6, 8)}`;

const parseDateInput = (value: string | null, fallbackYmd: string): string => {
  const compact = value?.replaceAll("-", "").trim();
  return compact && /^\d{8}$/.test(compact) ? compact : fallbackYmd;
};

const addDays = (ymd: string, days: number): string => {
  const date = new Date(
    Date.UTC(Number(ymd.slice(0, 4)), Number(ymd.slice(4, 6)) - 1, Number(ymd.slice(6, 8))),
  );
  date.setUTCDate(date.getUTCDate() + days);
  const year = String(date.getUTCFullYear()).padStart(4, "0");
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}${month}${day}`;
};

const enumerateDates = (startYmd: string, endYmd: string): string[] => {
  const dates: string[] = [];
  for (let ymd = startYmd; ymd <= endYmd; ymd = addDays(ymd, 1)) {
    dates.push(ymd);
  }
  return dates;
};

const isYmdInRange = (ymd: string, startYmd: string, endYmd: string): boolean =>
  ymd >= startYmd && ymd <= endYmd;

const runningStyleFromCorners = ({
  corner1,
  corner2,
  corner3,
  corner4,
  runnerCount,
}: {
  corner1: string | null | undefined;
  corner2: string | null | undefined;
  corner3: string | null | undefined;
  corner4: string | null | undefined;
  runnerCount: string | null | undefined;
}): RaceTrendRunningStyle | null => {
  const corner =
    parseCornerPosition(corner1) ??
    parseCornerPosition(corner2) ??
    parseCornerPosition(corner3) ??
    parseCornerPosition(corner4);
  if (corner === null) {
    return null;
  }
  if (corner <= 1) {
    return "nige";
  }
  const parsedRunnerCount = parseStoredInteger(runnerCount, "00");
  if (parsedRunnerCount === null || parsedRunnerCount <= 1) {
    if (corner <= 4) {
      return "senkou";
    }
    if (corner <= 8) {
      return "sashi";
    }
    return "oikomi";
  }
  const ratio = (corner - 1) / Math.max(parsedRunnerCount - 1, 1);
  if (ratio <= 0.35) {
    return "senkou";
  }
  if (ratio <= 0.7) {
    return "sashi";
  }
  return "oikomi";
};

const starterKey = (
  row: Pick<
    RaceTrendStarterRow,
    "source" | "kaisaiNen" | "kaisaiTsukihi" | "keibajoCode" | "raceBango" | "umaban"
  >,
): string =>
  [
    row.source,
    row.kaisaiNen,
    row.kaisaiTsukihi,
    row.keibajoCode,
    row.raceBango,
    row.umaban ?? "",
  ].join(":");

const starterRaceKey = (
  row: Pick<
    RaceTrendStarterRow,
    "source" | "kaisaiNen" | "kaisaiTsukihi" | "keibajoCode" | "raceBango"
  >,
): string =>
  buildRaceKey({
    kaisaiNen: row.kaisaiNen,
    kaisaiTsukihi: row.kaisaiTsukihi,
    keibajoCode: row.keibajoCode,
    raceBango: row.raceBango,
    source: row.source,
  });

const starterRunningStyleKey = (
  row: Pick<
    RaceTrendStarterRow,
    "source" | "kaisaiNen" | "kaisaiTsukihi" | "keibajoCode" | "raceBango" | "umaban"
  >,
): string => `${starterRaceKey(row)}:${normalizeNumberText(row.umaban) ?? ""}`;

const detailFromStarter = (row: RaceTrendStarterRow): RaceTrendDetail => ({
  source: row.source,
  date: toIsoDate(toYmd(row.kaisaiNen, row.kaisaiTsukihi)),
  keibajoCode: row.keibajoCode,
  raceNumber: row.raceBango,
  raceName: row.raceName,
  runningStyle: runningStyleFromCorners(row),
  frameNumber: row.wakuban,
  horseNumber: row.umaban,
  horseName: row.bamei,
  jockeyName: row.jockeyName,
  popularity: parseStoredPopularity(row.tanshoPopularity),
  winOdds: parseStoredWinOdds(row.tanshoOdds),
  finishPosition: row.finishPosition,
  time: row.sohaTime,
});

const calculateMedian = (values: number[]): number | null => {
  if (values.length === 0) {
    return null;
  }
  const sorted = values.toSorted((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    return sorted[middle] ?? null;
  }
  const left = sorted[middle - 1];
  const right = sorted[middle];
  return left === undefined || right === undefined ? null : (left + right) / 2;
};

const aggregateRows = (
  rows: RaceTrendStarterRow[],
  options: {
    endYmd: string;
    getGroupKey: (row: RaceTrendStarterRow) => string | null;
    keibajoCode?: string;
    startYmd: string;
    validKeys: Set<string>;
  },
): RaceTrendRateRow[] => {
  const groups = new Map<string, RaceTrendStarterRow[]>();

  for (const row of rows) {
    const ymd = toYmd(row.kaisaiNen, row.kaisaiTsukihi);
    if (!isYmdInRange(ymd, options.startYmd, options.endYmd)) {
      continue;
    }
    if (options.keibajoCode && row.keibajoCode !== options.keibajoCode) {
      continue;
    }
    const key = options.getGroupKey(row);
    if (!key || !options.validKeys.has(key)) {
      continue;
    }
    const bucket = groups.get(key) ?? [];
    bucket.push(row);
    groups.set(key, bucket);
  }

  return Array.from(groups.entries())
    .map(([key, groupRows]) => {
      const starts = groupRows.length;
      const winCount = groupRows.filter((row) => row.finishPosition === 1).length;
      const quinellaCount = groupRows.filter((row) => row.finishPosition <= 2).length;
      const showCount = groupRows.filter((row) => row.finishPosition <= 3).length;
      return {
        key,
        label: key,
        starts,
        showRate: starts > 0 ? (showCount / starts) * 100 : 0,
        quinellaRate: starts > 0 ? (quinellaCount / starts) * 100 : 0,
        winRate: starts > 0 ? (winCount / starts) * 100 : 0,
        finishPositionMedian: calculateMedian(groupRows.map((row) => row.finishPosition)),
        details: groupRows.map(detailFromStarter).toSorted((a, b) => {
          const dateOrder = b.date.localeCompare(a.date);
          if (dateOrder !== 0) {
            return dateOrder;
          }
          const raceOrder = b.raceNumber.localeCompare(a.raceNumber, "ja", { numeric: true });
          if (raceOrder !== 0) {
            return raceOrder;
          }
          return (a.horseNumber ?? "").localeCompare(b.horseNumber ?? "", "ja", { numeric: true });
        }),
      };
    })
    .toSorted(
      (a, b) =>
        b.showRate - a.showRate ||
        b.quinellaRate - a.quinellaRate ||
        b.winRate - a.winRate ||
        b.starts - a.starts ||
        a.label.localeCompare(b.label, "ja"),
    );
};

interface RaceTrendRunningStyleTarget {
  frameNumber: string | null;
  horseNumber: string | null;
  jockeyKey: string | null;
  jockeyName: string | null;
  raceNumber: string | null;
  runningStyle: RaceTrendRunningStyle | null;
}

const runningStyleGroupKey = (
  value: {
    frameNumber: string | null;
    jockeyKey: string | null;
    raceNumber: string | null;
    runningStyle: RaceTrendRunningStyle | null;
  },
  options: {
    ignoreFrame: boolean;
    ignoreJockey: boolean;
    ignoreRaceNumber: boolean;
    ignoreRunningStyle: boolean;
  },
): string | null => {
  if (!options.ignoreRunningStyle && !value.runningStyle) {
    return null;
  }
  if (!options.ignoreFrame && !value.frameNumber) {
    return null;
  }
  if (!options.ignoreJockey && !value.jockeyKey) {
    return null;
  }
  if (!options.ignoreRaceNumber && !value.raceNumber) {
    return null;
  }
  return [
    options.ignoreRunningStyle ? "*" : value.runningStyle,
    options.ignoreFrame ? "*" : value.frameNumber,
    options.ignoreJockey ? "*" : value.jockeyKey,
    options.ignoreRaceNumber ? "*" : value.raceNumber,
  ].join(":");
};

const runningStyleTargetGroupKey = (
  value: {
    frameNumber: string | null;
    jockeyKey: string | null;
    raceNumber: string | null;
    runningStyle: RaceTrendRunningStyle | null;
  },
  options: {
    ignoreFrame: boolean;
    ignoreJockey: boolean;
    ignoreRaceNumber: boolean;
    ignoreRunningStyle: boolean;
  },
): string | null =>
  runningStyleGroupKey(value, {
    ...options,
    ignoreRunningStyle: options.ignoreRunningStyle || !value.runningStyle,
  });

const runningStyleHistoricalGroupKeys = (
  value: {
    frameNumber: string | null;
    jockeyKey: string | null;
    raceNumber: string | null;
    runningStyle: RaceTrendRunningStyle | null;
  },
  options: {
    ignoreFrame: boolean;
    ignoreJockey: boolean;
    ignoreRaceNumber: boolean;
    ignoreRunningStyle: boolean;
  },
): string[] => {
  const keys = new Set<string>();
  const exactKey = runningStyleGroupKey(value, options);
  if (exactKey) {
    keys.add(exactKey);
  }
  if (!options.ignoreRunningStyle) {
    const fallbackKey = runningStyleGroupKey(value, {
      ...options,
      ignoreRunningStyle: true,
    });
    if (fallbackKey) {
      keys.add(fallbackKey);
    }
  }
  return Array.from(keys);
};

const average = (values: number[]): number | null =>
  values.length === 0 ? null : values.reduce((sum, value) => sum + value, 0) / values.length;

const sortTrendDetails = (details: RaceTrendDetail[]): RaceTrendDetail[] =>
  details.toSorted((a, b) => {
    const dateOrder = b.date.localeCompare(a.date);
    if (dateOrder !== 0) {
      return dateOrder;
    }
    const raceOrder = b.raceNumber.localeCompare(a.raceNumber, "ja", { numeric: true });
    if (raceOrder !== 0) {
      return raceOrder;
    }
    return (a.horseNumber ?? "").localeCompare(b.horseNumber ?? "", "ja", { numeric: true });
  });

const aggregateRunningStyleRows = (
  rows: RaceTrendStarterRow[],
  runningStyleByStarterKey: Map<string, RaceTrendRunningStyle>,
  targets: RaceTrendRunningStyleTarget[],
  options: {
    endYmd: string;
    ignoreFrame: boolean;
    ignoreJockey: boolean;
    ignoreRaceNumber: boolean;
    ignoreRunningStyle: boolean;
    jockeySameVenue: boolean;
    keibajoCode: string;
    startYmd: string;
  },
): RaceTrendRunningStyleRow[] => {
  const targetsByKey = new Map<string, RaceTrendRunningStyleTarget[]>();
  for (const target of targets) {
    const key = runningStyleTargetGroupKey(target, options);
    if (!key) {
      continue;
    }
    const bucket = targetsByKey.get(key) ?? [];
    bucket.push(target);
    targetsByKey.set(key, bucket);
  }
  const groups = new Map<string, RaceTrendStarterRow[]>();
  for (const row of rows) {
    const ymd = toYmd(row.kaisaiNen, row.kaisaiTsukihi);
    if (!isYmdInRange(ymd, options.startYmd, options.endYmd)) {
      continue;
    }
    if (options.jockeySameVenue && row.keibajoCode !== options.keibajoCode) {
      continue;
    }
    const runningStyle = runningStyleByStarterKey.get(starterRunningStyleKey(row)) ?? null;
    const keys = runningStyleHistoricalGroupKeys(
      {
        frameNumber: normalizeNumberText(row.wakuban),
        jockeyKey: normalizeRaceTrendJockeyName(row.jockeyName),
        raceNumber: normalizeNumberText(row.raceBango),
        runningStyle,
      },
      options,
    );
    for (const key of keys) {
      if (!targetsByKey.has(key)) {
        continue;
      }
      const bucket = groups.get(key) ?? [];
      bucket.push(row);
      groups.set(key, bucket);
    }
  }

  return Array.from(targetsByKey.entries())
    .flatMap(([key, targetRows]) => {
      const groupRows = groups.get(key) ?? [];
      const finishPositions = groupRows.map((row) => row.finishPosition);
      const winCount = groupRows.filter((row) => row.finishPosition === 1).length;
      const quinellaCount = groupRows.filter((row) => row.finishPosition <= 2).length;
      const showCount = groupRows.filter((row) => row.finishPosition <= 3).length;
      const popularities = groupRows
        .map((row) => parseStoredPopularity(row.tanshoPopularity))
        .filter((value): value is number => value !== null);
      const winOdds = groupRows
        .map((row) => parseStoredWinOdds(row.tanshoOdds))
        .filter((value): value is number => value !== null);
      const details = sortTrendDetails(
        groupRows.map((row) => ({
          ...detailFromStarter(row),
          runningStyle: runningStyleByStarterKey.get(starterRunningStyleKey(row)) ?? null,
        })),
      );
      return targetRows
        .toSorted((a, b) =>
          (a.horseNumber ?? "").localeCompare(b.horseNumber ?? "", "ja", { numeric: true }),
        )
        .map((target, index) => ({
          key: `${key}:${target.horseNumber ?? index}`,
          targetHorseNumbers: target.horseNumber ? [target.horseNumber] : [],
          runningStyle: target.runningStyle,
          frameNumber: options.ignoreFrame ? null : target.frameNumber,
          jockeyName: options.ignoreJockey ? null : target.jockeyName,
          raceNumber: options.ignoreRaceNumber ? null : target.raceNumber,
          starts: groupRows.length,
          showRate: groupRows.length > 0 ? (showCount / groupRows.length) * 100 : 0,
          quinellaRate: groupRows.length > 0 ? (quinellaCount / groupRows.length) * 100 : 0,
          winRate: groupRows.length > 0 ? (winCount / groupRows.length) * 100 : 0,
          finishPositionAverage: average(finishPositions),
          popularityMedian: calculateMedian(popularities),
          winOddsMedian: calculateMedian(winOdds),
          finishPositionMedian: calculateMedian(finishPositions),
          details,
        }));
    })
    .toSorted(
      (a, b) =>
        b.showRate - a.showRate ||
        b.quinellaRate - a.quinellaRate ||
        b.winRate - a.winRate ||
        b.starts - a.starts ||
        (a.targetHorseNumbers[0] ?? "").localeCompare(b.targetHorseNumbers[0] ?? "", "ja", {
          numeric: true,
        }) ||
        (a.frameNumber ?? "").localeCompare(b.frameNumber ?? "", "ja", { numeric: true }) ||
        (a.jockeyName ?? "").localeCompare(b.jockeyName ?? "", "ja") ||
        (a.raceNumber ?? "").localeCompare(b.raceNumber ?? "", "ja", { numeric: true }),
    );
};

const mapLimit = async <T, U>(
  values: T[],
  limit: number,
  mapper: (value: T) => Promise<U>,
): Promise<U[]> => {
  const entries = values.map((value, index) => ({ index, value }));
  const results: U[] = [];
  let nextIndex = 0;
  const runNext = (): Promise<void> => {
    const entry = entries[nextIndex];
    nextIndex += 1;
    if (!entry) {
      return Promise.resolve();
    }
    return mapper(entry.value).then((result) => {
      results[entry.index] = result;
      return runNext();
    });
  };

  await Promise.all(Array.from({ length: Math.min(limit, entries.length) }, runNext));
  return results;
};

const fetchRealtimePayload = async (race: RaceListItem): Promise<RealtimeRacePayload | null> => {
  const month = race.kaisaiTsukihi.slice(0, 2);
  const day = race.kaisaiTsukihi.slice(2, 4);
  const url = `${REALTIME_API_BASE_URL}/api/${race.source}/races/${race.kaisaiNen}/${month}/${day}/${race.keibajoCode}/${race.raceBango}/realtime`;
  try {
    const response = await fetchWithRetry(url, { cache: "no-store" }, { attempts: 1 });
    if (!response.ok) {
      return null;
    }
    const body: unknown = await response.json();
    return isRealtimeRacePayload(body) ? body : null;
  } catch {
    return null;
  }
};

const buildRealtimeStarterRows = async (race: RaceListItem): Promise<RaceTrendStarterRow[]> => {
  const payload = await fetchRealtimePayload(race);
  const resultHorses = payload?.raceResults?.horses ?? [];
  if (resultHorses.length === 0) {
    return [];
  }
  const runners = await getRaceRunners(
    race.source,
    race.kaisaiNen,
    race.kaisaiTsukihi.slice(0, 2),
    race.kaisaiTsukihi.slice(2, 4),
    race.keibajoCode,
    race.raceBango,
  );
  const runnerByHorseNumber = new Map(
    runners.map((runner) => [normalizeNumberText(runner.umaban), runner]),
  );
  const entryByHorseNumber = new Map(
    (payload?.raceEntries?.horses ?? []).map((entry) => [
      normalizeNumberText(entry.horseNumber),
      entry,
    ]),
  );
  const latestTanshoByHorseNumber = new Map(
    (payload?.odds?.latest.tansho ?? []).map((entry) => [
      normalizeNumberText(entry.combination),
      entry,
    ]),
  );

  return resultHorses.flatMap((resultHorse) => {
    const finishPosition = Number(resultHorse.finishPosition.replace(/[^\d]/g, ""));
    if (!Number.isFinite(finishPosition) || finishPosition <= 0) {
      return [];
    }
    const horseNumber = normalizeNumberText(resultHorse.horseNumber);
    const runner = runnerByHorseNumber.get(horseNumber);
    const entry = entryByHorseNumber.get(horseNumber);
    const latestTansho = latestTanshoByHorseNumber.get(horseNumber);
    return [
      {
        source: race.source,
        kaisaiNen: race.kaisaiNen,
        kaisaiTsukihi: race.kaisaiTsukihi,
        keibajoCode: race.keibajoCode,
        raceBango: race.raceBango,
        raceName:
          normalizeText(race.kyosomeiHondai) ?? normalizeText(race.kyosomeiFukudai) ?? "一般競走",
        runnerCount: String(resultHorses.length),
        wakuban: normalizeNumberText(runner?.wakuban),
        umaban: horseNumber,
        bamei:
          normalizeText(resultHorse.horseName) ??
          normalizeText(runner?.bamei) ??
          normalizeText(entry?.horseName),
        jockeyName: normalizeText(entry?.jockeyName) ?? normalizeText(runner?.kishumeiRyakusho),
        tanshoOdds: formatRealtimeWinOdds(latestTansho?.odds) ?? normalizeText(runner?.tanshoOdds),
        tanshoPopularity:
          formatRealtimeInteger(latestTansho?.rank) ?? normalizeText(runner?.tanshoNinkijun),
        finishPosition,
        sohaTime: normalizeText(resultHorse.time),
        corner1: null,
        corner2: null,
        corner3: null,
        corner4: null,
      },
    ];
  });
};

interface RaceTrendBuildOptions {
  frameEndYmd: string;
  frameStartYmd: string;
  includeRealtimeResults: boolean;
  jockeyEndYmd: string;
  jockeySameVenue: boolean;
  jockeyStartYmd: string;
  runningStyleIgnoreFrame: boolean;
  runningStyleIgnoreJockey: boolean;
  runningStyleIgnoreRaceNumber: boolean;
  runningStyleIgnoreRunningStyle: boolean;
}

const buildRealtimeRowsForTrend = async (
  race: RaceDetail,
  options: RaceTrendBuildOptions,
  historicalRows: RaceTrendStarterRow[],
): Promise<RaceTrendStarterRow[]> => {
  const minYmd =
    [options.jockeyStartYmd, options.frameStartYmd].toSorted()[0] ?? options.jockeyStartYmd;
  const maxYmd =
    [options.jockeyEndYmd, options.frameEndYmd].toSorted().at(-1) ?? options.jockeyEndYmd;
  const dateRaces = (
    await Promise.all(
      enumerateDates(minYmd, maxYmd).map((ymd) =>
        getRacesByDate(ymd.slice(0, 4), ymd.slice(4, 6), ymd.slice(6, 8)),
      ),
    )
  ).flat();
  const historicalRaceKeys = new Set(historicalRows.map(starterRaceKey));
  const candidateRaces = dateRaces.filter((candidate) => {
    if (candidate.source !== race.source || historicalRaceKeys.has(starterRaceKey(candidate))) {
      return false;
    }
    const ymd = toYmd(candidate.kaisaiNen, candidate.kaisaiTsukihi);
    const matchesJockeyRange =
      isYmdInRange(ymd, options.jockeyStartYmd, options.jockeyEndYmd) &&
      (!options.jockeySameVenue || candidate.keibajoCode === race.keibajoCode);
    const matchesFrameRange =
      isYmdInRange(ymd, options.frameStartYmd, options.frameEndYmd) &&
      candidate.keibajoCode === race.keibajoCode;
    return matchesJockeyRange || matchesFrameRange;
  });
  return (await mapLimit(candidateRaces, 6, buildRealtimeStarterRows)).flat();
};

const buildRaceTrendPayload = async (
  race: RaceDetail,
  runners: Runner[],
  options: RaceTrendBuildOptions,
): Promise<RaceTrendPayload> => {
  const currentRunningStylesPromise = getRaceRunningStylesFromD1(
    buildRaceKey({
      kaisaiNen: race.kaisaiNen,
      kaisaiTsukihi: race.kaisaiTsukihi,
      keibajoCode: race.keibajoCode,
      raceBango: race.raceBango,
      source: race.source,
    }),
  ).catch(() => []);
  const trendRunners = runners.map((runner) => {
    return {
      runner,
      effectiveJockeyName: normalizeText(runner.kishumeiRyakusho),
    };
  });
  const jockeyNames = Array.from(
    new Set(
      trendRunners
        .map((entry) => entry.effectiveJockeyName)
        .filter(isNonEmptyString)
        .flatMap(getJockeyNameAliases),
    ),
  );
  const jockeyKeys = Array.from(
    new Set(
      trendRunners
        .map((entry) => normalizeRaceTrendJockeyName(entry.effectiveJockeyName))
        .filter(isNonEmptyString),
    ),
  );
  const frameNumbers = Array.from(
    new Set(runners.map((runner) => normalizeNumberText(runner.wakuban)).filter(isNonEmptyString)),
  );
  const targetHorseNumberByJockey = new Map(
    trendRunners
      .map(
        ({ effectiveJockeyName, runner }) =>
          [
            normalizeRaceTrendJockeyName(effectiveJockeyName),
            normalizeNumberText(runner.umaban),
          ] as const,
      )
      .filter(
        (entry): entry is readonly [string, string] => Boolean(entry[0]) && Boolean(entry[1]),
      ),
  );
  const targetMarketByJockey = new Map<
    string,
    { popularity: number | null; winOdds: number | null }
  >();
  for (const { effectiveJockeyName, runner } of trendRunners) {
    const key = normalizeRaceTrendJockeyName(effectiveJockeyName);
    if (!key) {
      continue;
    }
    targetMarketByJockey.set(key, {
      popularity: parseStoredPopularity(runner.tanshoNinkijun),
      winOdds: parseStoredWinOdds(runner.tanshoOdds),
    });
  }
  const targetHorseNumbersByFrame = new Map<string, string>();
  for (const runner of runners) {
    const frameNumber = normalizeNumberText(runner.wakuban);
    const horseNumber = normalizeNumberText(runner.umaban);
    if (!frameNumber || !horseNumber) {
      continue;
    }
    const current = targetHorseNumbersByFrame.get(frameNumber);
    targetHorseNumbersByFrame.set(frameNumber, current ? `${current},${horseNumber}` : horseNumber);
  }
  const historicalRowsPromise = getRaceTrendHistoricalStarterRows(race, {
    frameEndYmd: options.frameEndYmd,
    frameNumbers,
    frameStartYmd: options.frameStartYmd,
    jockeyEndYmd: options.jockeyEndYmd,
    jockeyNames,
    jockeySameVenue: options.jockeySameVenue,
    jockeyStartYmd: options.jockeyStartYmd,
  });
  const currentRunningStyles = await currentRunningStylesPromise;
  const currentRunningStyleByHorseNumber = new Map(
    currentRunningStyles.map((row) => [String(row.horseNumber), row.predictedLabel]),
  );
  const runningStyleTargets = trendRunners.flatMap(
    ({ effectiveJockeyName, runner }): RaceTrendRunningStyleTarget[] => {
      const horseNumber = normalizeNumberText(runner.umaban);
      const runningStyle = horseNumber
        ? (currentRunningStyleByHorseNumber.get(horseNumber) ?? null)
        : null;
      return [
        {
          frameNumber: normalizeNumberText(runner.wakuban),
          horseNumber,
          jockeyKey: normalizeRaceTrendJockeyName(effectiveJockeyName),
          jockeyName: normalizeText(effectiveJockeyName),
          raceNumber: normalizeNumberText(race.raceBango),
          runningStyle: runningStyle ?? null,
        },
      ];
    },
  );
  const historicalRows = await historicalRowsPromise;
  const realtimeRows = options.includeRealtimeResults
    ? await buildRealtimeRowsForTrend(race, options, historicalRows)
    : [];
  const mergedRows = new Map(historicalRows.map((row) => [starterKey(row), row]));
  for (const row of realtimeRows) {
    mergedRows.set(starterKey(row), row);
  }
  const rows = Array.from(mergedRows.values());
  const historicalRunningStyles = await getRaceRunningStylesByRaceKeysFromD1(
    Array.from(new Set(rows.map(starterRaceKey))),
  ).catch(() => []);
  const runningStyleByStarterKey = new Map(
    historicalRunningStyles.map((row) => [
      `${row.raceKey}:${normalizeNumberText(String(row.horseNumber)) ?? ""}`,
      row.predictedLabel,
    ]),
  );

  return {
    jockeyRows: aggregateRows(rows, {
      startYmd: options.jockeyStartYmd,
      endYmd: options.jockeyEndYmd,
      keibajoCode: options.jockeySameVenue ? race.keibajoCode : undefined,
      validKeys: new Set(jockeyKeys),
      getGroupKey: (row) => normalizeRaceTrendJockeyName(row.jockeyName),
    }).map((row) =>
      Object.assign(row, {
        targetHorseNumber: targetHorseNumberByJockey.get(row.key) ?? null,
        targetPopularity: targetMarketByJockey.get(row.key)?.popularity ?? null,
        targetWinOdds: targetMarketByJockey.get(row.key)?.winOdds ?? null,
      }),
    ),
    frameRows: aggregateRows(rows, {
      startYmd: options.frameStartYmd,
      endYmd: options.frameEndYmd,
      keibajoCode: race.keibajoCode,
      validKeys: new Set(frameNumbers),
      getGroupKey: (row) => normalizeNumberText(row.wakuban),
    }).map((row) =>
      Object.assign(row, { targetHorseNumber: targetHorseNumbersByFrame.get(row.key) ?? null }),
    ),
    runningStyleRows: aggregateRunningStyleRows(
      rows,
      runningStyleByStarterKey,
      runningStyleTargets,
      {
        startYmd: options.jockeyStartYmd,
        endYmd: options.jockeyEndYmd,
        ignoreFrame: options.runningStyleIgnoreFrame,
        ignoreJockey: options.runningStyleIgnoreJockey,
        ignoreRaceNumber: options.runningStyleIgnoreRaceNumber,
        ignoreRunningStyle: options.runningStyleIgnoreRunningStyle,
        jockeySameVenue: options.jockeySameVenue,
        keibajoCode: race.keibajoCode,
      },
    ),
  };
};

export const dynamic = "force-dynamic";

export async function GET(request: Request, context: RouteContext) {
  const { day, keibajoCode, month, raceNumber, year } = await context.params;
  const searchParams = new URL(request.url).searchParams;
  const sourceParam = searchParams.get("source");
  const source = isRaceSource(sourceParam)
    ? sourceParam
    : await getRaceSourceByRoute(year, month, day, keibajoCode, raceNumber);

  if (!source) {
    return NextResponse.json({ error: "race source not found" }, { status: 404 });
  }

  const race = await getRaceDetail(source, year, month, day, keibajoCode, raceNumber);
  if (!race) {
    return NextResponse.json({ error: "race not found" }, { status: 404 });
  }

  const targetYmd = `${year}${month}${day}`;
  const defaultStartYmd = addDays(targetYmd, source === "jra" ? -1 : -3);
  const options = {
    jockeyStartYmd: parseDateInput(searchParams.get("jockeyStart"), defaultStartYmd),
    jockeyEndYmd: parseDateInput(searchParams.get("jockeyEnd"), targetYmd),
    frameStartYmd: parseDateInput(searchParams.get("frameStart"), defaultStartYmd),
    frameEndYmd: parseDateInput(searchParams.get("frameEnd"), targetYmd),
    includeRealtimeResults: searchParams.get("includeRealtimeResults") === "true",
    jockeySameVenue: searchParams.get("jockeySameVenue") !== "false",
    runningStyleIgnoreFrame: searchParams.get("runningStyleIgnoreFrame") === "true",
    runningStyleIgnoreJockey: searchParams.get("runningStyleIgnoreJockey") === "true",
    runningStyleIgnoreRaceNumber: searchParams.get("runningStyleIgnoreRaceNumber") !== "false",
    runningStyleIgnoreRunningStyle: searchParams.get("runningStyleIgnoreRunningStyle") === "true",
  };
  const runners = await getRaceRunners(source, year, month, day, keibajoCode, raceNumber);
  const payload = await buildRaceTrendPayload(race, runners, options);

  return NextResponse.json(payload, {
    headers: {
      "cache-control": "no-store",
    },
  });
}
