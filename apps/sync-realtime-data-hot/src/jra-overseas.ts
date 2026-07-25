// Run with bun. Resolves JRA-sold overseas races through the official annual
// race list because their accessSD CNAME values have no known checksum formula.

import { formatRaceStartJst } from "./time";

export interface JraOverseasRaceResolverInput {
  kaisaiNen: string;
  kaisaiTsukihi: string;
  kyosomeiHondai: string;
}

export interface JraOverseasRaceResolution {
  debaUrl: string;
  raceStartAtJst: string;
}

export interface JraOverseasRaceListEntry {
  kaisaiTsukihi: string;
  raceName: string;
  racePageUrl: string;
}

export type JraOverseasRaceResolver = (
  input: JraOverseasRaceResolverInput,
) => Promise<JraOverseasRaceResolution | null>;

const JRA_ORIGIN = "https://www.jra.go.jp";
const JRA_OVERSEAS_RACE_LIST_BASE_URL = `${JRA_ORIGIN}/keiba/overseas/racelist`;
const JRA_OVERSEAS_RACE_PAGE_PATH_PATTERN = /^\/keiba\/overseas\/race\/[a-z0-9_-]+\/index\.html$/iu;
const JRA_OVERSEAS_ENTRY_PATH = "/JRADB/accessSD.html";
const JRA_OVERSEAS_ENTRY_CNAME_PREFIX = "pk01dde";
const JRA_USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 sync-realtime-data-hot/1.0";
const TABLE_ROW_PATTERN = /<tr\b[^>]*>([\s\S]*?)<\/tr>/giu;
const ANCHOR_PATTERN = /<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/giu;
const JAPANESE_DATE_PATTERN = /(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日/u;
const JAPAN_POST_TIME_PATTERN =
  /日本時間\s*(\d{1,2})月\s*(\d{1,2})日(?:（[^）]*）|\([^)]*\))?\s*(\d{1,2})時\s*(\d{2})分/u;
const GRADE_SUFFIX_PATTERN = /(?:G|JPN)\d$/iu;
const RACE_NAME_PUNCTUATION_PATTERN = /[\s・･（）()]/gu;
const KING_GEORGE_ROMAN_NUMERAL_PATTERN = /VI(?=世)/giu;
const MONTH_DAY_PAD_WIDTH = 2;

export class JraOverseasFetchError extends Error {
  readonly status: number;

  constructor(url: string, status: number) {
    super(`Failed to fetch ${url}: ${status}`);
    this.name = "JraOverseasFetchError";
    this.status = status;
  }
}

const decodeHtml = (value: string): string =>
  value
    .replace(/&amp;/gu, "&")
    .replace(/&quot;/gu, '"')
    .replace(/&#039;|&apos;/gu, "'")
    .replace(/&nbsp;/gu, " ")
    .replace(/&#x([0-9a-f]+);/giu, (_, hex: string) =>
      String.fromCodePoint(Number.parseInt(hex, 16)),
    )
    .replace(/&#(\d+);/gu, (_, decimal: string) => String.fromCodePoint(Number(decimal)));

const normalizeText = (value: string): string =>
  decodeHtml(value.replace(/<[^>]*>/gu, " "))
    .replace(/\s+/gu, " ")
    .trim();

const normalizeRaceName = (value: string): string =>
  normalizeText(value)
    .normalize("NFKC")
    .replace(KING_GEORGE_ROMAN_NUMERAL_PATTERN, "6")
    .replace(RACE_NAME_PUNCTUATION_PATTERN, "")
    .replace(GRADE_SUFFIX_PATTERN, "")
    .toUpperCase();

const toMonthDay = (month: string, day: string): string =>
  `${month.padStart(MONTH_DAY_PAD_WIDTH, "0")}${day.padStart(MONTH_DAY_PAD_WIDTH, "0")}`;

const toJraUrl = (href: string): URL | null => {
  try {
    return new URL(decodeHtml(href), JRA_ORIGIN);
  } catch {
    return null;
  }
};

const fetchHtml = async (url: string): Promise<string> => {
  const response = await fetch(url, {
    headers: {
      Accept: "text/html,application/xhtml+xml",
      "User-Agent": JRA_USER_AGENT,
    },
  });
  if (!response.ok) {
    throw new JraOverseasFetchError(url, response.status);
  }
  return response.text();
};

const extractRacePageAnchor = (rowHtml: string): JraOverseasRaceListEntry | null => {
  const rowText = normalizeText(rowHtml);
  const date = rowText.match(JAPANESE_DATE_PATTERN);
  if (!date?.[1] || !date[2] || !date[3]) {
    return null;
  }
  const anchor = Array.from(rowHtml.matchAll(ANCHOR_PATTERN))
    .map((match) => ({ raceName: match[2], url: toJraUrl(match[1]!) }))
    .find(
      (candidate) =>
        candidate.url && JRA_OVERSEAS_RACE_PAGE_PATH_PATTERN.test(candidate.url.pathname),
    );
  if (!anchor?.url || !anchor.raceName) {
    return null;
  }
  return {
    kaisaiTsukihi: toMonthDay(date[2], date[3]),
    raceName: normalizeText(anchor.raceName),
    racePageUrl: anchor.url.toString(),
  };
};

export const parseJraOverseasRaceList = (html: string): JraOverseasRaceListEntry[] =>
  Array.from(html.matchAll(TABLE_ROW_PATTERN))
    .map((match) => (match[1] ? extractRacePageAnchor(match[1]) : null))
    .filter((entry): entry is JraOverseasRaceListEntry => entry !== null);

const isRaceNameMatch = (listedRaceName: string, targetRaceName: string): boolean => {
  const listed = normalizeRaceName(listedRaceName);
  const target = normalizeRaceName(targetRaceName);
  return (
    listed.length > 0 && target.length > 0 && (listed.includes(target) || target.includes(listed))
  );
};

const findRaceListEntry = (
  entries: JraOverseasRaceListEntry[],
  input: JraOverseasRaceResolverInput,
): JraOverseasRaceListEntry | null => {
  // JRA and JV occasionally spell the same title differently (for example
  // "VI世" versus full-width "６世"). Match the exact sale date first, then
  // require a unique normalized title containment match; ambiguity is rejected
  // rather than risking another race's accessSD URL.
  const matches = entries.filter(
    (entry) =>
      entry.kaisaiTsukihi === input.kaisaiTsukihi &&
      isRaceNameMatch(entry.raceName, input.kyosomeiHondai),
  );
  return matches.length === 1 ? matches[0]! : null;
};

const extractOverseasEntryUrl = (html: string): string | null => {
  const entryAnchor = Array.from(html.matchAll(ANCHOR_PATTERN))
    .map((match) => ({ href: match[1], text: match[2] }))
    .find(
      (candidate) => candidate.href && candidate.text && normalizeText(candidate.text) === "出馬表",
    );
  if (!entryAnchor?.href) {
    return null;
  }
  const url = toJraUrl(entryAnchor.href);
  if (
    !url ||
    url.origin !== JRA_ORIGIN ||
    url.pathname !== JRA_OVERSEAS_ENTRY_PATH ||
    !url.searchParams.get("CNAME")?.startsWith(JRA_OVERSEAS_ENTRY_CNAME_PREFIX)
  ) {
    return null;
  }
  return url.toString();
};

export const parseJraOverseasRacePage = (
  html: string,
  input: JraOverseasRaceResolverInput,
): JraOverseasRaceResolution | null => {
  const pageText = normalizeText(html);
  const postTime = pageText.match(JAPAN_POST_TIME_PATTERN);
  if (!postTime?.[1] || !postTime[2] || !postTime[3] || !postTime[4]) {
    return null;
  }
  if (toMonthDay(postTime[1], postTime[2]) !== input.kaisaiTsukihi) {
    return null;
  }
  const debaUrl = extractOverseasEntryUrl(html);
  if (!debaUrl) {
    return null;
  }
  const hhmm = `${postTime[3].padStart(MONTH_DAY_PAD_WIDTH, "0")}${postTime[4]}`;
  return {
    debaUrl,
    raceStartAtJst: formatRaceStartJst(input.kaisaiNen, input.kaisaiTsukihi, hhmm),
  };
};

const buildRaceListUrl = (year: string): string =>
  `${JRA_OVERSEAS_RACE_LIST_BASE_URL}/${year}.html`;

export const createJraOverseasRaceResolver = (): JraOverseasRaceResolver => {
  const listCache = new Map<string, Promise<JraOverseasRaceListEntry[]>>();
  const pageCache = new Map<string, Promise<string>>();

  const getRaceList = (year: string): Promise<JraOverseasRaceListEntry[]> => {
    const cached = listCache.get(year);
    if (cached) {
      return cached;
    }
    const pending = fetchHtml(buildRaceListUrl(year)).then(parseJraOverseasRaceList);
    listCache.set(year, pending);
    return pending;
  };

  const getRacePage = (url: string): Promise<string> => {
    const cached = pageCache.get(url);
    if (cached) {
      return cached;
    }
    const pending = fetchHtml(url);
    pageCache.set(url, pending);
    return pending;
  };

  return async (input) => {
    const entry = findRaceListEntry(await getRaceList(input.kaisaiNen), input);
    if (!entry) {
      return null;
    }
    return parseJraOverseasRacePage(await getRacePage(entry.racePageUrl), input);
  };
};
