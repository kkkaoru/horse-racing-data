import { NextResponse } from "next/server";

import {
  getRaceCourseInfo,
  getRaceDetail,
  getRaceRunners,
  getRaceSourceByRoute,
  getRacesByDate,
} from "../../../../../../../../../../db/queries";
import type { RaceSource } from "../../../../../../../../../../lib/codes";
import {
  formatCourseParagraphs,
  getCourseFacts,
  getCourseImagePath,
} from "../../../../../../../../../../lib/course";
import { fetchWithRetry } from "../../../../../../../../../../lib/fetch-with-retry";
import { cleanText } from "../../../../../../../../../../lib/format";
import type {
  RaceDetail,
  RaceListItem,
  Runner,
} from "../../../../../../../../../../lib/race-types";
import {
  getDetailSectionPayload,
  type DetailSection,
} from "../../../../../../../../../races/detail/detail-section-data";

export const dynamic = "force-dynamic";

interface RouteContext {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

const AI_DATA_PARTS = [
  "courseDisplay",
  "courseInfo",
  "finishPrediction",
  "overallScore",
  "race",
  "raceDayRaces",
  "realtime",
  "runners",
  "timeScore",
] as const;

type AiDataPart = (typeof AI_DATA_PARTS)[number];

const REALTIME_PARTS = [
  "entries",
  "oddsFukusho",
  "oddsTansho",
  "results",
  "source",
  "trackCondition",
  "weights",
] as const;

type RealtimePart = (typeof REALTIME_PARTS)[number];

const DEFAULT_REALTIME_PARTS = [
  "entries",
  "oddsTansho",
  "weights",
  "results",
  "trackCondition",
] as const satisfies readonly RealtimePart[];

const DEFAULT_PARTS = [
  "race",
  "runners",
  "courseInfo",
  "courseDisplay",
] as const satisfies readonly AiDataPart[];

const isRaceSource = (value: string | null): value is RaceSource =>
  value === "jra" || value === "nar";

const isValidParams = (
  year: string,
  month: string,
  day: string,
  keibajoCode: string,
  raceNumber: string,
): boolean =>
  /^\d{4}$/.test(year) &&
  /^\d{2}$/.test(month) &&
  /^\d{2}$/.test(day) &&
  /^[0-9A-Z]{2}$/.test(keibajoCode) &&
  /^\d{2}$/.test(raceNumber);

const isAiDataPart = (value: string): value is AiDataPart =>
  AI_DATA_PARTS.some((part) => part === value);

const isRealtimePart = (value: string): value is RealtimePart =>
  REALTIME_PARTS.some((part) => part === value);

const parseParts = (value: string | null): AiDataPart[] => {
  if (!value) {
    return [...DEFAULT_PARTS];
  }
  return Array.from(
    new Set(
      value
        .split(",")
        .map((part) => part.trim())
        .filter(isAiDataPart),
    ),
  );
};

const parseRealtimeParts = (value: string | null): RealtimePart[] => {
  if (!value) {
    return [...DEFAULT_REALTIME_PARTS];
  }
  return Array.from(
    new Set(
      value
        .split(",")
        .map((part) => part.trim())
        .filter(isRealtimePart),
    ),
  );
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const recordValue = (
  record: Record<string, unknown> | null,
  key: string,
): Record<string, unknown> | null => {
  const value = record?.[key];
  return isRecord(value) ? value : null;
};

const arrayValue = (record: Record<string, unknown> | null, key: string): unknown[] => {
  const value = record?.[key];
  return Array.isArray(value) ? value : [];
};

const buildRaceName = (
  race: Pick<RaceDetail, "kyosomeiFukudai" | "kyosomeiHondai" | "kyosomeiKakkonai">,
): string | null => {
  const name = [race.kyosomeiHondai, race.kyosomeiFukudai, race.kyosomeiKakkonai]
    .map((value) => cleanText(value, ""))
    .filter((value) => value.length > 0)
    .join(" ");
  return name || null;
};

const pickRace = (race: RaceDetail) => ({
  babajotaiCodeDirt: race.babajotaiCodeDirt,
  babajotaiCodeShiba: race.babajotaiCodeShiba,
  gradeCode: race.gradeCode,
  hassoJikoku: race.hassoJikoku,
  kaisaiNen: race.kaisaiNen,
  kaisaiTsukihi: race.kaisaiTsukihi,
  keibajoCode: race.keibajoCode,
  kyori: race.kyori,
  kyosoJokenCode: race.kyosoJokenCode,
  kyosoJokenMeisho: race.kyosoJokenMeisho,
  kyosoKigoCode: race.kyosoKigoCode,
  kyosoShubetsuCode: race.kyosoShubetsuCode,
  kyosomeiFukudai: race.kyosomeiFukudai,
  kyosomeiHondai: race.kyosomeiHondai,
  kyosomeiKakkonai: race.kyosomeiKakkonai,
  raceBango: race.raceBango,
  raceName: buildRaceName(race),
  shussoTosu: race.shussoTosu,
  source: race.source,
  tenkoCode: race.tenkoCode,
  torokuTosu: race.torokuTosu,
  trackCode: race.trackCode,
});

const pickRunner = (runner: Runner) => ({
  banushimei: runner.banushimei,
  barei: runner.barei,
  bataiju: runner.bataiju,
  bamei: runner.bamei,
  chokyoshimeiRyakusho: runner.chokyoshimeiRyakusho,
  futanJuryo: runner.futanJuryo,
  kakuteiChakujun: runner.kakuteiChakujun,
  kettoTorokuBango: runner.kettoTorokuBango,
  kishumeiRyakusho: runner.kishumeiRyakusho,
  seibetsuCode: runner.seibetsuCode,
  tanshoNinkijun: runner.tanshoNinkijun,
  tanshoOdds: runner.tanshoOdds,
  umaban: runner.umaban,
  wakuban: runner.wakuban,
  zogenFugo: runner.zogenFugo,
  zogenSa: runner.zogenSa,
});

const pickRaceListItem = (race: RaceListItem) => ({
  hassoJikoku: race.hassoJikoku,
  kaisaiNen: race.kaisaiNen,
  kaisaiTsukihi: race.kaisaiTsukihi,
  keibajoCode: race.keibajoCode,
  kyori: race.kyori,
  kyosoJokenMeisho: race.kyosoJokenMeisho,
  kyosomeiHondai: race.kyosomeiHondai,
  raceBango: race.raceBango,
  raceName: buildRaceName({
    kyosomeiFukudai: race.kyosomeiFukudai,
    kyosomeiHondai: race.kyosomeiHondai,
    kyosomeiKakkonai: null,
  }),
  source: race.source,
  trackCode: race.trackCode,
});

const compactUnknown = (value: unknown, depth = 0): unknown => {
  if (
    value === null ||
    value === undefined ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  if (typeof value === "string") {
    return value.length > 1_200 ? `${value.slice(0, 1_200)}...` : value;
  }
  if (Array.isArray(value)) {
    const rows = value.slice(0, 40).map((item) => compactUnknown(item, depth + 1));
    return value.length > rows.length
      ? [...rows, { omittedItems: value.length - rows.length }]
      : rows;
  }
  if (typeof value !== "object") {
    return null;
  }
  if (depth >= 6) {
    return "[omitted]";
  }
  return Object.fromEntries(
    Object.entries(value)
      .filter(([key]) => key !== "details")
      .map(([key, entryValue]) => [key, compactUnknown(entryValue, depth + 1)]),
  );
};

const searchParamsToSectionQuery = (searchParams: URLSearchParams): Record<string, string> =>
  Object.fromEntries(searchParams.entries().filter(([key]) => key !== "parts" && key !== "source"));

const getSectionForPart = (part: AiDataPart): DetailSection | null => {
  switch (part) {
    case "finishPrediction":
      return "finish-prediction";
    case "overallScore":
      return "overall-score";
    case "timeScore":
      return "time-score";
    default:
      return null;
  }
};

const fetchRealtimePayload = async ({
  day,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}): Promise<unknown> => {
  const realtimeApiBaseUrl =
    process.env.NEXT_PUBLIC_REALTIME_DATA_API_BASE_URL ?? "https://sync-realtime-data.kkk4oru.com";
  const upstreamUrl = `${realtimeApiBaseUrl}/api/${source}/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/realtime`;
  const response = await fetchWithRetry(upstreamUrl, { cache: "no-store" });
  if (!response.ok) {
    return {
      error: `${response.status} ${response.statusText}`.trim(),
    };
  }
  return response.json();
};

const pickRealtimePayload = (payload: unknown, parts: RealtimePart[]) => {
  const record = isRecord(payload) ? payload : null;
  const odds = recordValue(record, "odds");
  const latest = recordValue(odds, "latest");
  return {
    entries: parts.includes("entries")
      ? {
          fetchedAt: recordValue(record, "raceEntries")?.fetchedAt ?? null,
          horses: arrayValue(recordValue(record, "raceEntries"), "horses"),
        }
      : undefined,
    oddsFukusho: parts.includes("oddsFukusho")
      ? {
          fetchedAt: odds?.fetchedAt ?? null,
          rows: arrayValue(latest, "fukusho"),
        }
      : undefined,
    oddsTansho: parts.includes("oddsTansho")
      ? {
          fetchedAt: odds?.fetchedAt ?? null,
          rows: arrayValue(latest, "tansho"),
        }
      : undefined,
    results: parts.includes("results") ? recordValue(record, "raceResults") : undefined,
    source: parts.includes("source") ? recordValue(record, "source") : undefined,
    trackCondition: parts.includes("trackCondition")
      ? recordValue(record, "trackCondition")
      : undefined,
    weights: parts.includes("weights") ? recordValue(record, "horseWeights") : undefined,
  };
};

export async function GET(request: Request, context: RouteContext) {
  const { day, keibajoCode, month, raceNumber, year } = await context.params;
  if (!isValidParams(year, month, day, keibajoCode, raceNumber)) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const searchParams = new URL(request.url).searchParams;
  const sourceParam = searchParams.get("source");
  if (sourceParam && !isRaceSource(sourceParam)) {
    return NextResponse.json({ error: "invalid source" }, { status: 400 });
  }
  const requestedSource = isRaceSource(sourceParam) ? sourceParam : null;
  const source: RaceSource | null = requestedSource
    ? requestedSource
    : await getRaceSourceByRoute(year, month, day, keibajoCode, raceNumber);
  if (!source) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const parts = parseParts(searchParams.get("parts"));
  const realtimeParts = parseRealtimeParts(searchParams.get("realtimeParts"));
  if (parts.length === 0) {
    return NextResponse.json({ error: "invalid parts" }, { status: 400 });
  }

  const race = await getRaceDetail(source, year, month, day, keibajoCode, raceNumber);
  if (!race) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const response: Record<string, unknown> = {
    meta: {
      generatedAt: new Date().toISOString(),
      parts,
      route: { day, keibajoCode, month, raceNumber, source, year },
    },
  };

  if (parts.includes("race")) {
    response.race = pickRace(race);
  }

  if (parts.includes("runners")) {
    response.runners = (
      await getRaceRunners(source, year, month, day, keibajoCode, raceNumber)
    ).map(pickRunner);
  }

  if (parts.includes("courseInfo") || parts.includes("courseDisplay")) {
    const courseInfo = await getRaceCourseInfo(keibajoCode, race.kyori, race.trackCode);
    if (parts.includes("courseInfo")) {
      response.courseInfo = courseInfo;
    }
    if (parts.includes("courseDisplay")) {
      const courseText = cleanText(courseInfo?.courseSetsumei, "");
      response.courseDisplay = {
        facts: getCourseFacts(courseText, race.kyori, race.trackCode),
        imagePath: getCourseImagePath(keibajoCode, race.trackCode, race.kyori),
        paragraphs: courseText ? formatCourseParagraphs(courseText) : [],
      };
    }
  }

  if (parts.includes("raceDayRaces")) {
    response.raceDayRaces = (await getRacesByDate(year, month, day)).map(pickRaceListItem);
  }

  if (parts.includes("realtime")) {
    response.realtime = pickRealtimePayload(
      await fetchRealtimePayload({ day, keibajoCode, month, raceNumber, source, year }),
      realtimeParts,
    );
  }

  const sectionQuery = searchParamsToSectionQuery(searchParams);
  await Promise.all(
    parts.map(async (part) => {
      const section = getSectionForPart(part);
      if (!section) {
        return;
      }
      const payload = await getDetailSectionPayload(section, {
        day,
        keibajoCode,
        month,
        query: sectionQuery,
        raceNumber,
        raceSource: source,
        year,
      });
      response[part] = compactUnknown(payload);
    }),
  );

  return NextResponse.json(response, {
    headers: {
      "Cache-Control": "private, max-age=0, no-store",
    },
  });
}
