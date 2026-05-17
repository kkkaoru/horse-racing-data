import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import { NextResponse } from "next/server";

import {
  getRaceDetail,
  getRaceRunners,
  getRaceSourceByRoute,
  getRacesByDate,
  getRaceTrendHistoricalStarterRows,
} from "../../../../../../../../../db/queries";
import type { RaceSource } from "../../../../../../../../../lib/codes";
import { fetchWithRetry } from "../../../../../../../../../lib/fetch-with-retry";
import type {
  RaceDetail,
  RaceListItem,
  RaceTrendDetail,
  RaceTrendPayload,
  RaceTrendRateRow,
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

const isNonEmptyString = (value: string | null): value is string => value !== null && value !== "";

const isRealtimeRacePayload = (value: unknown): value is RealtimeRacePayload =>
  typeof value === "object" && value !== null && "raceResults" in value;

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

const detailFromStarter = (row: RaceTrendStarterRow): RaceTrendDetail => ({
  source: row.source,
  date: toIsoDate(toYmd(row.kaisaiNen, row.kaisaiTsukihi)),
  keibajoCode: row.keibajoCode,
  raceNumber: row.raceBango,
  raceName: row.raceName,
  frameNumber: row.wakuban,
  horseNumber: row.umaban,
  horseName: row.bamei,
  jockeyName: row.jockeyName,
  finishPosition: row.finishPosition,
  time: row.sohaTime,
});

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
        details: groupRows.map(detailFromStarter).toSorted((a, b) => {
          const dateOrder = b.date.localeCompare(a.date);
          if (dateOrder !== 0) {
            return dateOrder;
          }
          const raceOrder = a.raceNumber.localeCompare(b.raceNumber, "ja", { numeric: true });
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

  return resultHorses.flatMap((resultHorse) => {
    const finishPosition = Number(resultHorse.finishPosition.replace(/[^\d]/g, ""));
    if (!Number.isFinite(finishPosition) || finishPosition <= 0) {
      return [];
    }
    const horseNumber = normalizeNumberText(resultHorse.horseNumber);
    const runner = runnerByHorseNumber.get(horseNumber);
    const entry = entryByHorseNumber.get(horseNumber);
    return [
      {
        source: race.source,
        kaisaiNen: race.kaisaiNen,
        kaisaiTsukihi: race.kaisaiTsukihi,
        keibajoCode: race.keibajoCode,
        raceBango: race.raceBango,
        raceName:
          normalizeText(race.kyosomeiHondai) ?? normalizeText(race.kyosomeiFukudai) ?? "一般競走",
        wakuban: normalizeNumberText(runner?.wakuban),
        umaban: horseNumber,
        bamei:
          normalizeText(resultHorse.horseName) ??
          normalizeText(runner?.bamei) ??
          normalizeText(entry?.horseName),
        jockeyName: normalizeText(entry?.jockeyName) ?? normalizeText(runner?.kishumeiRyakusho),
        finishPosition,
        sohaTime: normalizeText(resultHorse.time),
      },
    ];
  });
};

const buildRaceTrendPayload = async (
  race: RaceDetail,
  runners: Runner[],
  options: {
    frameEndYmd: string;
    frameStartYmd: string;
    jockeyEndYmd: string;
    jockeySameVenue: boolean;
    jockeyStartYmd: string;
  },
): Promise<RaceTrendPayload> => {
  const jockeyNames = Array.from(
    new Set(
      runners.map((runner) => normalizeText(runner.kishumeiRyakusho)).filter(isNonEmptyString),
    ),
  );
  const frameNumbers = Array.from(
    new Set(runners.map((runner) => normalizeNumberText(runner.wakuban)).filter(isNonEmptyString)),
  );
  const targetHorseNumberByJockey = new Map(
    runners
      .map(
        (runner) =>
          [normalizeText(runner.kishumeiRyakusho), normalizeNumberText(runner.umaban)] as const,
      )
      .filter(
        (entry): entry is readonly [string, string] => Boolean(entry[0]) && Boolean(entry[1]),
      ),
  );
  const targetHorseNumbersByFrame = new Map<string, string>();
  for (const runner of runners) {
    const frameNumber = normalizeNumberText(runner.wakuban);
    const horseNumber = normalizeNumberText(runner.umaban);
    if (!frameNumber || !horseNumber) {
      continue;
    }
    const current = targetHorseNumbersByFrame.get(frameNumber);
    targetHorseNumbersByFrame.set(
      frameNumber,
      current ? `${current},${horseNumber}` : horseNumber,
    );
  }
  const historicalRows = await getRaceTrendHistoricalStarterRows(race, {
    ...options,
    frameNumbers,
    jockeyNames,
  });
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
  const candidateRaces = dateRaces.filter((candidate) => {
    if (candidate.source !== race.source) {
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
  const realtimeRows = (await mapLimit(candidateRaces, 6, buildRealtimeStarterRows)).flat();
  const mergedRows = new Map(historicalRows.map((row) => [starterKey(row), row]));
  for (const row of realtimeRows) {
    mergedRows.set(starterKey(row), row);
  }
  const rows = Array.from(mergedRows.values());

  return {
    jockeyRows: aggregateRows(rows, {
      startYmd: options.jockeyStartYmd,
      endYmd: options.jockeyEndYmd,
      keibajoCode: options.jockeySameVenue ? race.keibajoCode : undefined,
      validKeys: new Set(jockeyNames),
      getGroupKey: (row) => normalizeText(row.jockeyName),
    }).map((row) =>
      Object.assign(row, { targetHorseNumber: targetHorseNumberByJockey.get(row.key) ?? null }),
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
    jockeySameVenue: searchParams.get("jockeySameVenue") !== "false",
  };
  const runners = await getRaceRunners(source, year, month, day, keibajoCode, raceNumber);
  const payload = await buildRaceTrendPayload(race, runners, options);

  return NextResponse.json(payload, {
    headers: {
      "cache-control": "no-store",
    },
  });
}
