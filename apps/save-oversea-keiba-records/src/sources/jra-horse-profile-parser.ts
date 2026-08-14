// This file runs with Bun.

export interface ParsedOverseaHorseResult {
  raceDate: string;
  venue: string;
  raceName: string;
  sourceRaceId: string | null;
  sourceRaceUrl: string | null;
  finishPosition: number | null;
  finishPositionText: string;
  jockeyName: string;
  sourceJockeyId: string | null;
  surface: string;
  distanceMetres: number | null;
  going: string;
}

export interface ParsedJraVanHorseProfile {
  sourceHorseId: string;
  horseName: string;
  sourceUrl: string;
  results: ParsedOverseaHorseResult[];
}

const TAG_PATTERN: RegExp = /<[^>]+>/g;
const WHITESPACE_PATTERN: RegExp = /\s+/g;
const CANONICAL_PATTERN: RegExp =
  /<link\s+rel=["']canonical["']\s+href=["'](https:\/\/world\.jra-van\.jp\/db\/horse\/(H\d+)\/)["'][^>]*>/iu;
const HORSE_NAME_PATTERN: RegExp = /class=["']horsefix__title__en["'][^>]*>（([^）]+)）<\/span>/iu;
const RESULT_SECTION_START_PATTERN: RegExp = /<div\s+id=["']horse--result["'][^>]*>/iu;
const NEXT_SECTION_PATTERN: RegExp = /<div\s+id=["']horse--long["'][^>]*>/iu;
const ROW_PATTERN: RegExp = /<tr[^>]*>([\s\S]*?)<\/tr>/giu;
const CELL_PATTERN: RegExp = /<td([^>]*)>([\s\S]*?)<\/td>/giu;
const SOURCE_RACE_PATTERN: RegExp = /href=["']\/schedule\/result\/(R\d+)\/["']/iu;
const SOURCE_JOCKEY_PATTERN: RegExp = /href=["']\/db\/jockey\/(\d+)\/["']/iu;
const DATE_PATTERN: RegExp = /(\d{4})\s*\/\s*(\d{2})\s*\/\s*(\d{2})/u;
const INTEGER_PATTERN: RegExp = /^\d+$/u;

const decodeHtml = (value: string): string =>
  value
    .replaceAll("&amp;", "&")
    .replaceAll("&quot;", '"')
    .replaceAll("&#39;", "'")
    .replaceAll("&nbsp;", " ");

const cleanText = (value: string): string =>
  decodeHtml(value.replace(TAG_PATTERN, " ").replace(WHITESPACE_PATTERN, " ").trim());

const captureRequired = (html: string, pattern: RegExp, fieldName: string): RegExpExecArray => {
  const matched: RegExpExecArray | null = pattern.exec(html);
  if (matched === null) {
    throw new Error(`JRA-VAN horse profile is missing ${fieldName}.`);
  }
  return matched;
};

const resultSection = (html: string): string => {
  const start: number = captureRequired(html, RESULT_SECTION_START_PATTERN, "result section").index;
  const remainder: string = html.slice(start);
  const next: RegExpExecArray | null = NEXT_SECTION_PATTERN.exec(remainder);
  if (next === null) {
    throw new Error("JRA-VAN horse profile is missing the section after results.");
  }
  return remainder.slice(0, next.index);
};

const parseDate = (html: string): string => {
  const text: string = cleanText(html);
  const matched: RegExpMatchArray | null = text.match(DATE_PATTERN);
  const year: string | undefined = matched?.[1];
  const month: string | undefined = matched?.[2];
  const day: string | undefined = matched?.[3];
  if (year === undefined || month === undefined || day === undefined) {
    throw new Error(`JRA-VAN horse result has an invalid date: ${text}`);
  }
  return `${year}-${month}-${day}`;
};

const optionalId = (html: string, pattern: RegExp): string | null =>
  pattern.exec(html)?.[1] ?? null;

const parseResultRow = (html: string, sourceUrl: string): ParsedOverseaHorseResult | null => {
  const cells: RegExpMatchArray[] = Array.from(html.matchAll(CELL_PATTERN));
  if (cells.length === 0) return null;
  if (cells.length !== 9) {
    throw new Error(`JRA-VAN horse result has ${cells.length} cells instead of 9.`);
  }
  const cellHtml = cells.map((cell: RegExpMatchArray): string => cell[2] as string);
  const finishPositionText: string = cleanText(cellHtml[4] as string);
  const distanceText: string = cleanText(cellHtml[7] as string);
  const sourceRaceId: string | null = optionalId(cellHtml[2] as string, SOURCE_RACE_PATTERN);
  return {
    raceDate: parseDate(cellHtml[0] as string),
    venue: cleanText(cellHtml[1] as string),
    raceName: cleanText(cellHtml[2] as string),
    sourceRaceId,
    sourceRaceUrl:
      sourceRaceId === null ? null : new URL(`/schedule/result/${sourceRaceId}/`, sourceUrl).href,
    finishPosition: INTEGER_PATTERN.test(finishPositionText) ? Number(finishPositionText) : null,
    finishPositionText,
    jockeyName: cleanText(cellHtml[5] as string),
    sourceJockeyId: optionalId(cellHtml[5] as string, SOURCE_JOCKEY_PATTERN),
    surface: cleanText(cellHtml[6] as string),
    distanceMetres: INTEGER_PATTERN.test(distanceText) ? Number(distanceText) : null,
    going: cleanText(cellHtml[8] as string),
  };
};

export const parseJraVanHorseProfile = (html: string): ParsedJraVanHorseProfile => {
  const canonical: RegExpExecArray = captureRequired(html, CANONICAL_PATTERN, "canonical URL");
  const sourceUrl: string = canonical[1] as string;
  const sourceHorseId: string = canonical[2] as string;
  const horseName: string = cleanText(
    captureRequired(html, HORSE_NAME_PATTERN, "English horse name")[1] as string,
  );
  const results: ParsedOverseaHorseResult[] = Array.from(
    resultSection(html).matchAll(ROW_PATTERN),
  ).flatMap((row: RegExpMatchArray): ParsedOverseaHorseResult[] => {
    const parsed: ParsedOverseaHorseResult | null = parseResultRow(row[1] as string, sourceUrl);
    return parsed === null ? [] : [parsed];
  });
  return { horseName, results, sourceHorseId, sourceUrl };
};
