import { KEIBAJO_NAMES } from "../codes";
import type { Win5RaceLeg, Win5Schedule } from "./types";

export const JRA_WIN5_RACELIST_URL = "https://www.jra.go.jp/kouza/win5/info/racelist.html";

const JRA_VENUE_NAME_TO_CODE: Record<string, string> = {
  札幌: "01",
  函館: "02",
  福島: "03",
  新潟: "04",
  東京: "05",
  中山: "06",
  中京: "07",
  京都: "08",
  阪神: "09",
  小倉: "10",
};

const HEADER_YEAR_PATTERN = /(\d{4})年\d{1,2}月\d{1,2}日（[^）]+）までのWIN5対象レース/u;
const WIN5LIST_TABLE_PATTERN = /<table class="win5list[\s\S]*?<tbody>([\s\S]*?)<\/tbody>/u;
const TABLE_ROW_PATTERN = /<tr>[\s\S]*?<\/tr>/gu;
const DATE_PATTERN = /(\d{1,2})月(\d{1,2})日/u;
const DEADLINE_PATTERN = /<strong>(\d{1,2}時\d{1,2}分)<\/strong>/u;
const RACE_SPAN_PATTERN = /<span class="race">([^<]+)<\/span>/gu;
const TIME_SPAN_PATTERN = /<span class="time">(\d{1,2}時\d{1,2}分)\s*発走<\/span>/gu;
const RACE_CELL_PATTERN = /([一-龥]+)(\d{1,2})R/u;

const pad2 = (value: number | string): string => String(value).padStart(2, "0");

const stripHtml = (value: string): string =>
  value
    .replace(/<br\s*\/?>/giu, " ")
    .replace(/<[^>]*>/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();

const parseRaceLabel = (
  value: string,
): Pick<Win5RaceLeg, "keibajoCode" | "raceBango" | "keibajoName" | "raceLabel"> | null => {
  const cleaned = stripHtml(value);
  const matched = cleaned.match(RACE_CELL_PATTERN);
  if (!matched?.[1] || !matched[2]) {
    return null;
  }
  const venueName = matched[1];
  const keibajoCode = JRA_VENUE_NAME_TO_CODE[venueName];
  if (!keibajoCode) {
    return null;
  }
  const raceBango = matched[2].replace(/^0+/u, "") || matched[2];
  const keibajoName = KEIBAJO_NAMES[keibajoCode] ?? venueName;
  return {
    keibajoCode,
    keibajoName,
    raceBango,
    raceLabel: `${keibajoName}${raceBango}R`,
  };
};

const parseStartTime = (value: string): string | undefined => {
  const matched = value.match(/(\d{1,2})時(\d{1,2})分/u);
  if (!matched?.[1] || !matched[2]) {
    return undefined;
  }
  return `${pad2(matched[1])}:${pad2(matched[2])}`;
};

export const decodeJraHtml = (bytes: ArrayBuffer | Uint8Array): string => {
  const buffer = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  try {
    return new TextDecoder("shift_jis").decode(buffer);
  } catch {
    return new TextDecoder("utf-8").decode(buffer);
  }
};

export const inferScheduleYearFromHtml = (html: string, fallbackYear: string): string => {
  const matched = html.match(HEADER_YEAR_PATTERN);
  return matched?.[1] ?? fallbackYear;
};

const parseWin5TableRow = (
  rowHtml: string,
  scheduleYear: string,
  fetchedAt: string,
): Win5Schedule | null => {
  const dateMatched = rowHtml.match(DATE_PATTERN);
  if (!dateMatched) {
    return null;
  }

  const raceLabels = [...rowHtml.matchAll(RACE_SPAN_PATTERN)].map(
    (match) => match[1]?.trim() ?? "",
  );
  const startTimes = [...rowHtml.matchAll(TIME_SPAN_PATTERN)].map((match) => match[1] ?? "");
  if (raceLabels.length !== 5) {
    return null;
  }

  const legs = raceLabels
    .map((raceLabel, index) => {
      const parsed = parseRaceLabel(raceLabel);
      if (parsed === null) {
        return null;
      }
      const leg: Win5RaceLeg = Object.assign(
        {
          legIndex: index + 1,
          kaisaiKai: "00",
          kaisaiNichime: "00",
          startTime: parseStartTime(startTimes[index] ?? ""),
        },
        parsed,
      );
      return leg;
    })
    .filter((leg): leg is Win5RaceLeg => leg !== null);

  if (legs.length !== 5) {
    return null;
  }

  const month = pad2(Number(dateMatched[1]));
  const day = pad2(Number(dateMatched[2]));
  const deadlineMatched = rowHtml.match(DEADLINE_PATTERN);

  return {
    fetchedAt,
    kaisaiNen: scheduleYear,
    kaisaiTsukihi: `${month}${day}`,
    legs,
    saleDeadline: deadlineMatched?.[1] ?? null,
    source: "jra_web",
  };
};

export const parseWin5SchedulesFromJraHtml = (
  html: string,
  options?: {
    fallbackYear?: string;
    fetchedAt?: string;
  },
): Win5Schedule[] => {
  const fallbackYear = options?.fallbackYear ?? String(new Date().getFullYear());
  const scheduleYear = inferScheduleYearFromHtml(html, fallbackYear);
  const fetchedAt = options?.fetchedAt ?? new Date().toISOString();
  const tableMatched = html.match(WIN5LIST_TABLE_PATTERN);
  if (!tableMatched?.[1]) {
    return [];
  }

  const schedules: Win5Schedule[] = [];
  for (const rowMatched of tableMatched[1].matchAll(TABLE_ROW_PATTERN)) {
    const schedule = parseWin5TableRow(rowMatched[0], scheduleYear, fetchedAt);
    if (schedule) {
      schedules.push(schedule);
    }
  }
  return schedules;
};

export const fetchWin5SchedulesFromJra = async (options?: {
  fallbackYear?: string;
  fetchedAt?: string;
}): Promise<Win5Schedule[]> => {
  const response = await fetch(JRA_WIN5_RACELIST_URL, {
    headers: {
      Accept: "text/html",
      "User-Agent": "pc-keiba-viewer/1.0",
    },
  });
  if (!response.ok) {
    throw new Error(`Failed to fetch JRA WIN5 racelist: HTTP ${response.status}`);
  }
  const html = decodeJraHtml(await response.arrayBuffer());
  return parseWin5SchedulesFromJraHtml(html, options);
};
