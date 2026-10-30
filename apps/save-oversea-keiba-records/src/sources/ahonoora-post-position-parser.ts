// This file runs with Bun. Parses ahonoora.com race-history pages (Shift_JIS) for post positions.
// Markup taken from the live pages, e.g. https://ahonoora.com/bcclassic.html and bcclassic_2025.html.
import iconv from "iconv-lite";

export interface AhonooraRunner {
  readonly finish: number | null;
  // null when the page has no 馬番 column or shows a coupled entry such as "1a".
  readonly umaban: number | null;
  readonly gate: number;
  // JV seibetsu_code: 1 牡, 2 牝, 3 セン. null when the cell is not a known sex and age.
  readonly sexCode: string | null;
  readonly age: number | null;
  readonly weightKg: number | null;
}
export interface AhonooraRace {
  readonly date: string;
  readonly title: string;
  readonly runners: readonly AhonooraRunner[];
}

const SOURCE_ENCODING: string = "cp932";
const SERIES_PATTERN: RegExp = /^[a-z0-9]+$/u;
// The text right before the red grade mark; older pages wrap it in decorative ■ fonts.
const TITLE_PATTERN: RegExp = /([^<>]+?)\s*<FONT COLOR=RED>\(G/iu;
// "2025/11/1　" on current pages, "2009.11. 7　" on older ones.
const DATE_PATTERN: RegExp = /(\d{4})[./](\d{1,2})[./]\s?(\d{1,2})\s*　/u;
const HEADER_PATTERN: RegExp = /<TR[^>]*bgcolor=silver[^>]*>(.*?)<\/TR>/isu;
const ROW_PATTERN: RegExp =
  /<tr align=center(?: bgcolor=\w+)?>(.*?)(?=<tr|<\/TBODY>|<\/TABLE>)/gisu;
const CELL_SPLIT: RegExp = /<td[^>]*>/iu;
const TAG_PATTERN: RegExp = /<[^>]+>/gu;
const FINISH_PATTERN: RegExp = /^(\d+)着$/u;
const NUMBER_PATTERN: RegExp = /^(\d+)番$/u;
const FINISH_COLUMN: string = "着順";
const UMABAN_COLUMN: string = "馬番";
const GATE_COLUMN: string = "枠順";
const SEX_AGE_COLUMN: string = "性齢";
const WEIGHT_COLUMN: string = "斤量";
const SEX_AGE_PATTERN: RegExp = /^(牡|牝|セン|騸)(\d{1,2})$/u;
const WEIGHT_PATTERN: RegExp = /^(\d{2}(?:\.\d)?)$/u;
const SEX_CODES: ReadonlyMap<string, string> = new Map([
  ["牡", "1"],
  ["牝", "2"],
  ["セン", "3"],
  ["騸", "3"],
]);
const TWO_DIGITS: number = 2;

export const decodeAhonoora = (bytes: Uint8Array): string =>
  iconv.decode(Buffer.from(bytes), SOURCE_ENCODING);

export const yearPageNames = (html: string, series: string, minYear: number): string[] => {
  if (!SERIES_PATTERN.test(series)) throw new Error(`Invalid ahonoora series: ${series}`);
  const pattern: RegExp = new RegExp(`HREF="(${series}_(\\d{4})\\.html)"`, "giu");
  return [
    ...new Set(
      [...html.matchAll(pattern)]
        .filter((match) => Number(match[2]) >= minYear)
        .map((match) => String(match[1])),
    ),
  ].toSorted();
};

const pad = (value: string): string => value.padStart(TWO_DIGITS, "0");

const cells = (row: string): string[] =>
  row
    .split(CELL_SPLIT)
    .slice(1)
    .map((cell) => cell.replace(TAG_PATTERN, "").trim());

const numberIn = (pattern: RegExp, cell: string | undefined): number | null => {
  const match: string | undefined = pattern.exec(String(cell))?.[1];
  return match === undefined ? null : Number(match);
};

// Column positions come from each page's own header: older pages have no 馬番 column.
const runnerRows = (html: string): AhonooraRunner[] => {
  const header: string[] = cells(HEADER_PATTERN.exec(html)?.[1] ?? "");
  const finishAt: number = header.indexOf(FINISH_COLUMN);
  const umabanAt: number = header.indexOf(UMABAN_COLUMN);
  const gateAt: number = header.indexOf(GATE_COLUMN);
  const sexAgeAt: number = header.indexOf(SEX_AGE_COLUMN);
  const weightAt: number = header.indexOf(WEIGHT_COLUMN);
  if (finishAt < 0 || gateAt < 0) throw new Error("Ahonoora race page has no 着順/枠順 header.");
  return [...html.matchAll(ROW_PATTERN)].flatMap((match) => {
    const row: string[] = cells(String(match[1]));
    const gate: number | null = numberIn(NUMBER_PATTERN, row[gateAt]);
    const sexAge: RegExpExecArray | null = SEX_AGE_PATTERN.exec(String(row[sexAgeAt]));
    return gate === null
      ? []
      : [
          {
            finish: numberIn(FINISH_PATTERN, row[finishAt]),
            umaban: umabanAt < 0 ? null : numberIn(NUMBER_PATTERN, row[umabanAt]),
            gate,
            sexCode: sexAge === null ? null : (SEX_CODES.get(String(sexAge[1])) ?? null),
            age: sexAge === null ? null : Number(sexAge[2]),
            weightKg: weightAt < 0 ? null : numberIn(WEIGHT_PATTERN, row[weightAt]),
          },
        ];
  });
};

export const parseAhonooraRace = (html: string): AhonooraRace => {
  const title: string | undefined = TITLE_PATTERN.exec(html)?.[1]?.trim();
  const date: RegExpExecArray | null = DATE_PATTERN.exec(html);
  if (title === undefined || title === "" || date === null)
    throw new Error("Ahonoora race page has no title or date.");
  const runners: AhonooraRunner[] = runnerRows(html);
  if (runners.length === 0) throw new Error("Ahonoora race page has no runner rows.");
  if (new Set(runners.map((runner) => runner.gate)).size !== runners.length)
    throw new Error("Ahonoora race page repeats a post position.");
  return {
    date: `${String(date[1])}${pad(String(date[2]))}${pad(String(date[3]))}`,
    title,
    runners,
  };
};
