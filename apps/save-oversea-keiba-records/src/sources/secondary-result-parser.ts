// Pure parser for cached secondary-source result tables.
// All source-specific markers, route prefixes, and cell positions are supplied
// by an operator-owned profile and must not be committed.

export type SecondaryPersonKind = "jockey" | "trainer" | "owner";

export interface SecondaryResultFieldIndexes {
  readonly date: number;
  readonly venue: number;
  readonly raceNumber: number;
  readonly raceName: number;
  readonly finishPosition: number;
  readonly distance: number;
  readonly going: number;
  readonly relatedEntity: number;
}

export interface SecondaryResultMarkupProfile {
  readonly tableMarker: string;
  readonly racePathPrefix: string;
  readonly horsePathPrefix: string;
  readonly jockeyPathPrefix: string;
  readonly raceUrlTemplate: string;
  readonly horseFields: SecondaryResultFieldIndexes;
  readonly personFields: SecondaryResultFieldIndexes;
}

export interface SecondaryHorseResult {
  readonly sourceHorseId: string;
  readonly sourceRaceId: string;
  readonly raceDate: string;
  readonly venue: string;
  readonly raceDaySequence: number;
  readonly raceName: string;
  readonly sourceRaceUrl: string;
  readonly finishPosition: number | null;
  readonly finishPositionText: string;
  readonly jockeyName: string;
  readonly sourceJockeyId: string | null;
  readonly surface: string;
  readonly distanceMetres: number;
  readonly going: string;
}

export interface SecondaryPersonResult {
  readonly personKind: SecondaryPersonKind;
  readonly sourcePersonId: string;
  readonly sourceRaceId: string;
  readonly raceDate: string;
  readonly venue: string | null;
  readonly raceNumber: string;
  readonly raceName: string;
  readonly sourceRaceUrl: string;
  readonly sourceHorseId: string | null;
  readonly horseName: string | null;
  readonly finishPosition: number | null;
  readonly finishPositionText: string;
  readonly surface: string | null;
  readonly distanceMetres: number | null;
  readonly going: string | null;
}

const ROW_PATTERN = /<tr\b[^>]*>([\s\S]*?)<\/tr>/giu;
const CELL_PATTERN = /<td\b[^>]*>([\s\S]*?)<\/td>/giu;
const TAG_PATTERN = /<[^>]+>/gu;
const SPACE_PATTERN = /\s+/gu;
const DATE_PATTERN = /^(\d{4})\/(\d{2})\/(\d{2})$/u;
const DISTANCE_PATTERN = /^(\D+?)(\d+)$/u;
const TRAILING_DISTANCE_PATTERN = /(\d+)$/u;
const INTEGER_PATTERN = /^\d+$/u;

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

const tableRows = (html: string, marker: string): string[] => {
  const markerIndex = html.indexOf(marker);
  if (markerIndex < 0) throw new Error("Secondary result table marker was not found.");
  const tableEnd = html.indexOf("</table>", markerIndex);
  if (tableEnd < 0) throw new Error("Secondary result table is not closed.");
  return Array.from(html.slice(markerIndex, tableEnd).matchAll(ROW_PATTERN))
    .map((match) => match[1] ?? "")
    .filter((row) => row.includes("<td"));
};

const cells = (row: string): string[] =>
  Array.from(row.matchAll(CELL_PATTERN), (match) => match[1] ?? "");

const requiredCell = (values: readonly string[], index: number): string => {
  const value = values[index];
  if (value === undefined)
    throw new Error(`Secondary result row is missing cell ${String(index)}.`);
  return value;
};

const requiredId = (html: string, prefix: string, name: string): string => {
  const id = sourceId(html, prefix);
  if (id === null) throw new Error(`Secondary result row is missing ${name}.`);
  return id;
};

const raceDate = (html: string): string => {
  const value = clean(html);
  const match = DATE_PATTERN.exec(value);
  if (match === null) throw new Error(`Secondary result row has invalid date: ${value}`);
  return `${match[1]}-${match[2]}-${match[3]}`;
};

const distance = (html: string): { surface: string; distanceMetres: number } => {
  const value = clean(html);
  const match = DISTANCE_PATTERN.exec(value);
  if (match === null) throw new Error(`Secondary result row has invalid distance: ${value}`);
  return { surface: match[1] ?? "", distanceMetres: Number(match[2]) };
};

const personDistance = (
  html: string,
): { surface: string | null; distanceMetres: number | null } => {
  const value = clean(html);
  if (value === "") throw new Error(`Secondary result row has invalid distance: ${value}`);
  const match = TRAILING_DISTANCE_PATTERN.exec(value);
  if (match === null) return { surface: value, distanceMetres: null };
  const metres = match[0];
  const surface = value.slice(0, -metres.length);
  return { surface: surface || null, distanceMetres: Number(metres) };
};

const finish = (html: string): { finishPosition: number | null; finishPositionText: string } => {
  const finishPositionText = clean(html);
  return {
    finishPosition: INTEGER_PATTERN.test(finishPositionText) ? Number(finishPositionText) : null,
    finishPositionText,
  };
};

const sourceUrl = (raceId: string, template: string): string =>
  template.replaceAll("{RACE_ID}", raceId);

export const parseSecondaryHorseResults = (
  html: string,
  sourceHorseId: string,
  profile: SecondaryResultMarkupProfile,
): SecondaryHorseResult[] => {
  const sequences = new Map<string, number>();
  return tableRows(html, profile.tableMarker).map((row) => {
    const values = cells(row);
    const fields = profile.horseFields;
    const parsedDate = raceDate(requiredCell(values, fields.date));
    const venue = clean(requiredCell(values, fields.venue));
    const key = `${parsedDate}\u0000${venue}`;
    const raceDaySequence = (sequences.get(key) ?? 0) + 1;
    sequences.set(key, raceDaySequence);
    const raceCell = requiredCell(values, fields.raceName);
    const jockeyCell = requiredCell(values, fields.relatedEntity);
    const raceId = requiredId(raceCell, profile.racePathPrefix, "race ID");
    return {
      sourceHorseId,
      sourceRaceId: raceId,
      raceDate: parsedDate,
      venue,
      raceDaySequence,
      raceName: clean(raceCell),
      sourceRaceUrl: sourceUrl(raceId, profile.raceUrlTemplate),
      ...finish(requiredCell(values, fields.finishPosition)),
      jockeyName: clean(jockeyCell),
      sourceJockeyId: sourceId(jockeyCell, profile.jockeyPathPrefix),
      ...distance(requiredCell(values, fields.distance)),
      going: clean(requiredCell(values, fields.going)),
    };
  });
};

export const parseSecondaryPersonResults = (
  html: string,
  personKind: SecondaryPersonKind,
  sourcePersonId: string,
  profile: SecondaryResultMarkupProfile,
): SecondaryPersonResult[] =>
  tableRows(html, profile.tableMarker).map((row) => {
    const values = cells(row);
    const fields = profile.personFields;
    const raceCell = requiredCell(values, fields.raceName);
    const horseCell = requiredCell(values, fields.relatedEntity);
    const raceId = requiredId(raceCell, profile.racePathPrefix, "race ID");
    return {
      personKind,
      sourcePersonId,
      sourceRaceId: raceId,
      raceDate: raceDate(requiredCell(values, fields.date)),
      venue: clean(requiredCell(values, fields.venue)) || null,
      raceNumber: clean(requiredCell(values, fields.raceNumber)),
      raceName: clean(raceCell),
      sourceRaceUrl: sourceUrl(raceId, profile.raceUrlTemplate),
      sourceHorseId: sourceId(horseCell, profile.horsePathPrefix),
      horseName: clean(horseCell) || null,
      ...finish(requiredCell(values, fields.finishPosition)),
      ...personDistance(requiredCell(values, fields.distance)),
      going: clean(requiredCell(values, fields.going)) || null,
    };
  });
