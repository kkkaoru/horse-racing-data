// This file runs with Bun. Pure parser for a cached secondary-source race result page.
// Table markers, route prefixes, header patterns and cell positions come from an
// operator-owned profile and must not be committed.

export interface SecondaryRaceResultFieldIndexes {
  readonly finishPosition: number;
  readonly gate: number;
  readonly horseNumber: number;
  readonly horseName: number;
  readonly sexAge: number;
  readonly weightCarried: number;
  readonly jockey: number;
  readonly time: number;
  readonly winOdds: number;
  readonly popularity: number;
  readonly trainer: number;
}

export interface SecondaryRaceResultProfile {
  readonly tableMarker: string;
  readonly horsePathPrefix: string;
  readonly jockeyPathPrefix: string;
  readonly trainerPathPrefix: string;
  // Captures year, month and day of the local race date.
  readonly datePattern: string;
  // Captures surface label, direction label and distance in metres.
  readonly coursePattern: string;
  // Leading markers removed from trainer labels (e.g. an overseas badge).
  readonly trainerPrefixes: readonly string[];
  readonly fields: SecondaryRaceResultFieldIndexes;
}

export interface SecondaryRaceResultRunner {
  readonly finishPosition: number | null;
  readonly finishPositionText: string;
  readonly gate: number;
  readonly horseNumber: number;
  readonly horseName: string;
  readonly sourceHorseId: string | null;
  readonly sex: string;
  readonly age: number;
  readonly weightCarriedKg: number;
  readonly jockeyName: string;
  readonly sourceJockeyId: string | null;
  readonly timeText: string;
  readonly winOdds: number | null;
  readonly popularity: number | null;
  readonly trainerName: string;
  readonly sourceTrainerId: string | null;
}

export interface SecondaryRaceResult {
  readonly date: string;
  readonly surface: string;
  readonly direction: string;
  readonly distanceMetres: number;
  readonly runners: readonly SecondaryRaceResultRunner[];
}

const ROW_PATTERN: RegExp = /<tr\b[^>]*>([\s\S]*?)<\/tr>/giu;
const CELL_PATTERN: RegExp = /<td\b[^>]*>([\s\S]*?)<\/td>/giu;
const TAG_PATTERN: RegExp = /<[^>]+>/gu;
const SPACE_PATTERN: RegExp = /\s+/gu;
const INTEGER_PATTERN: RegExp = /^\d+$/u;
const DECIMAL_PATTERN: RegExp = /^\d+(?:\.\d+)?$/u;
const SEX_AGE_PATTERN: RegExp = /^(\D+?)(\d+)$/u;
const SEX_LABELS: ReadonlyMap<string, string> = new Map([["セ", "せん"]]);
const SURFACE_LABELS: ReadonlyMap<string, string> = new Map([["ダ", "ダート"]]);
const DIRECTION_LABELS: ReadonlyMap<string, string> = new Map([["直", "直線"]]);
const DATE_PAD: number = 2;
const MAX_AGE_DIGITS: number = 2;

const clean = (value: string): string =>
  value
    .replaceAll("&amp;", "&")
    .replaceAll("&nbsp;", " ")
    .replace(TAG_PATTERN, " ")
    .replace(SPACE_PATTERN, " ")
    .trim();

const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");

const sourceId = (html: string, prefix: string): string | null =>
  new RegExp(`href=["'][^"']*${escapeRegExp(prefix)}([^/"'?#\\s]+)`, "iu").exec(html)?.[1] ?? null;

const optionalNumber = (html: string, pattern: RegExp): number | null => {
  const value: string = clean(html);
  return pattern.test(value) ? Number(value) : null;
};

const requiredCell = (values: readonly string[], index: number): string => {
  const value: string | undefined = values[index];
  if (value === undefined) throw new Error(`Race result row is missing cell ${String(index)}.`);
  return value;
};

const requiredInteger = (html: string, name: string): number => {
  const value: string = clean(html);
  if (!INTEGER_PATTERN.test(value)) throw new Error(`Race result row has invalid ${name}.`);
  return Number(value);
};

const trainerName = (html: string, prefixes: readonly string[]): string =>
  prefixes.reduce((name: string, prefix: string) => name.replace(prefix, ""), clean(html)).trim();

const resultRows = (html: string, marker: string): readonly string[] => {
  const start: number = html.indexOf(marker);
  if (start < 0) throw new Error("Race result table marker was not found.");
  const end: number = html.indexOf("</table>", start);
  if (end < 0) throw new Error("Race result table is not closed.");
  return Array.from(html.slice(start, end).matchAll(ROW_PATTERN), (match) => match[1] ?? "").filter(
    (row: string) => row.includes("<td"),
  );
};

const parseRunner = (
  row: string,
  profile: SecondaryRaceResultProfile,
): SecondaryRaceResultRunner => {
  const values: readonly string[] = Array.from(
    row.matchAll(CELL_PATTERN),
    (match) => match[1] ?? "",
  );
  const fields: SecondaryRaceResultFieldIndexes = profile.fields;
  const horseCell: string = requiredCell(values, fields.horseName);
  const sexAge: RegExpExecArray | null = SEX_AGE_PATTERN.exec(
    clean(requiredCell(values, fields.sexAge)),
  );
  if (sexAge === null) throw new Error("Race result row has invalid sex and age.");
  const finishPositionText: string = clean(requiredCell(values, fields.finishPosition));
  const jockeyCell: string = requiredCell(values, fields.jockey);
  const trainerCell: string = requiredCell(values, fields.trainer);
  const sex: string = String(sexAge[1]);
  return {
    finishPosition: INTEGER_PATTERN.test(finishPositionText) ? Number(finishPositionText) : null,
    finishPositionText,
    gate: optionalNumber(requiredCell(values, fields.gate), INTEGER_PATTERN) ?? 0,
    horseNumber: requiredInteger(requiredCell(values, fields.horseNumber), "horse number"),
    horseName: clean(horseCell),
    // Horses without a source page keep a null identity; never synthesize one.
    sourceHorseId: sourceId(horseCell, profile.horsePathPrefix),
    sex: SEX_LABELS.get(sex) ?? sex,
    // A malformed published age (e.g. "145") is kept unknown (0 → JV "00"), never guessed.
    age: String(sexAge[2]).length <= MAX_AGE_DIGITS ? Number(sexAge[2]) : 0,
    weightCarriedKg:
      optionalNumber(requiredCell(values, fields.weightCarried), DECIMAL_PATTERN) ?? 0,
    jockeyName: clean(jockeyCell),
    sourceJockeyId: sourceId(jockeyCell, profile.jockeyPathPrefix),
    timeText: clean(requiredCell(values, fields.time)),
    winOdds: optionalNumber(requiredCell(values, fields.winOdds), DECIMAL_PATTERN),
    popularity: optionalNumber(requiredCell(values, fields.popularity), INTEGER_PATTERN),
    trainerName: trainerName(trainerCell, profile.trainerPrefixes),
    sourceTrainerId: sourceId(trainerCell, profile.trainerPathPrefix),
  };
};

export const parseSecondaryRaceResult = (
  html: string,
  profile: SecondaryRaceResultProfile,
): SecondaryRaceResult => {
  const plain: string = clean(html);
  const date: RegExpExecArray | null = new RegExp(profile.datePattern, "u").exec(plain);
  const course: RegExpExecArray | null = new RegExp(profile.coursePattern, "u").exec(plain);
  if (date === null || course === null)
    throw new Error("Race result header could not be verified.");
  const runners: readonly SecondaryRaceResultRunner[] = resultRows(html, profile.tableMarker).map(
    (row: string) => parseRunner(row, profile),
  );
  if (runners.length === 0) throw new Error("Race result table has no runners.");
  const surface: string = String(course[1]);
  const direction: string = String(course[2]);
  return {
    date: `${String(date[1])}-${String(date[2]).padStart(DATE_PAD, "0")}-${String(date[3]).padStart(DATE_PAD, "0")}`,
    surface: SURFACE_LABELS.get(surface) ?? surface,
    direction: DIRECTION_LABELS.get(direction) ?? direction,
    distanceMetres: Number(course[3]),
    runners,
  };
};
