// This file runs with Bun.
// Immutable source records: existing data is never erased by a partial scrape.
import type {
  SecondaryHorseResult,
  SecondaryPersonResult,
} from "../sources/secondary-result-parser";
import type { SqlStatement } from "../types";

export interface HistoryPublicationInput {
  readonly horses: readonly SecondaryHorseResult[];
  readonly people: readonly SecondaryPersonResult[];
}

export interface HistoryTransaction {
  readonly write: (statement: SqlStatement) => Promise<void>;
  readonly countMismatches: (statement: SqlStatement) => Promise<number>;
}

export interface HistoryPublicationPorts {
  readonly withTransaction: (
    run: (transaction: HistoryTransaction) => Promise<void>,
  ) => Promise<void>;
}

interface HistoryCommon {
  readonly sourceRaceId: string;
  readonly raceDate: string;
  readonly raceName: string;
  readonly sourceRaceUrl: string;
  readonly finishPosition: number | null;
  readonly finishPositionText: string;
  readonly distanceMetres: number | null;
}

interface ColumnMapping {
  readonly column: string;
  readonly property: string;
  readonly sqlType: string;
}

interface InsertInput {
  readonly table: string;
  readonly columns: readonly ColumnMapping[];
  readonly rows: readonly HistoryCommon[];
  readonly conflictTarget: string;
}

const COMMON_COLUMNS: readonly ColumnMapping[] = [
  { column: "source_race_id", property: "sourceRaceId", sqlType: "text" },
  { column: "race_date", property: "raceDate", sqlType: "date" },
  { column: "venue", property: "venue", sqlType: "text" },
  { column: "race_name", property: "raceName", sqlType: "text" },
  { column: "source_race_url", property: "sourceRaceUrl", sqlType: "text" },
  { column: "finish_position", property: "finishPosition", sqlType: "smallint" },
  { column: "finish_position_text", property: "finishPositionText", sqlType: "text" },
  { column: "surface", property: "surface", sqlType: "text" },
  { column: "distance_metres", property: "distanceMetres", sqlType: "integer" },
  { column: "going", property: "going", sqlType: "text" },
];
const HORSE_COLUMNS: readonly ColumnMapping[] = [
  ...COMMON_COLUMNS,
  { column: "source_horse_id", property: "sourceHorseId", sqlType: "text" },
  { column: "race_day_sequence", property: "raceDaySequence", sqlType: "smallint" },
  { column: "jockey_name", property: "jockeyName", sqlType: "text" },
  { column: "source_jockey_id", property: "sourceJockeyId", sqlType: "text" },
];
const PERSON_COLUMNS: readonly ColumnMapping[] = [
  ...COMMON_COLUMNS,
  { column: "person_kind", property: "personKind", sqlType: "text" },
  { column: "source_person_id", property: "sourcePersonId", sqlType: "text" },
  { column: "race_number", property: "raceNumber", sqlType: "text" },
  { column: "source_horse_id", property: "sourceHorseId", sqlType: "text" },
  { column: "horse_name", property: "horseName", sqlType: "text" },
];
const SOURCE_ID_PATTERN: RegExp = /^[a-zA-Z0-9]+$/u;
const DATE_PATTERN: RegExp = /^\d{4}-\d{2}-\d{2}$/u;
const PERSON_KINDS: ReadonlySet<string> = new Set(["jockey", "trainer", "owner"]);

const positiveInteger = (value: number): boolean => Number.isSafeInteger(value) && value > 0;
const validId = (value: string): boolean => SOURCE_ID_PATTERN.test(value);

const validateCommon = (row: HistoryCommon): void => {
  if (
    !validId(row.sourceRaceId) ||
    !DATE_PATTERN.test(row.raceDate) ||
    !Number.isFinite(Date.parse(row.raceDate)) ||
    new Date(row.raceDate).toISOString().slice(0, 10) !== row.raceDate
  ) {
    throw new Error("History contains an invalid source identity or calendar date.");
  }
  const url: URL = new URL(row.sourceRaceUrl);
  if (
    url.protocol !== "https:" ||
    url.username !== "" ||
    url.password !== "" ||
    row.raceName.trim() === "" ||
    row.finishPositionText.trim() === ""
  ) {
    throw new Error("History provenance or required published text is missing.");
  }
  if (
    (row.finishPosition !== null && !positiveInteger(row.finishPosition)) ||
    (row.distanceMetres !== null && !positiveInteger(row.distanceMetres))
  ) {
    throw new Error("History numeric values must be positive integers or explicitly unavailable.");
  }
};

const validateHorse = (row: SecondaryHorseResult): void => {
  validateCommon(row);
  if (
    !validId(row.sourceHorseId) ||
    !positiveInteger(row.raceDaySequence) ||
    !positiveInteger(row.distanceMetres) ||
    row.venue.trim() === "" ||
    row.jockeyName.trim() === "" ||
    row.surface.trim() === "" ||
    (row.sourceJockeyId !== null && !validId(row.sourceJockeyId))
  ) {
    throw new Error("Horse history has invalid required fields.");
  }
};

const validatePerson = (row: SecondaryPersonResult): void => {
  validateCommon(row);
  if (
    !PERSON_KINDS.has(row.personKind) ||
    !validId(row.sourcePersonId) ||
    (row.sourceHorseId !== null && !validId(row.sourceHorseId)) ||
    (row.sourceHorseId === null && (row.horseName === null || row.horseName.trim() === ""))
  ) {
    throw new Error("Person history lacks a valid person or identifiable runner.");
  }
};

const buildInsert = ({ table, columns, rows, conflictTarget }: InsertInput): SqlStatement => ({
  text: `insert into ${table} (source, ${columns.map((c: ColumnMapping): string => c.column).join(", ")})
select 'netkeiba', ${columns.map((c: ColumnMapping): string => `r."${c.property}"`).join(", ")}
from jsonb_to_recordset($1::jsonb) as r(${columns.map((c: ColumnMapping): string => `"${c.property}" ${c.sqlType}`).join(", ")})
on conflict ${conflictTarget} do nothing
returning history_id`,
  values: [JSON.stringify(rows)],
});

const buildReadback = (
  { table, columns, rows }: InsertInput,
  projection: "count(*)::integer as mismatches" | "row_to_json(r) as row",
): SqlStatement => ({
  text: `select ${projection}
from jsonb_to_recordset($1::jsonb) as r(${columns.map((c: ColumnMapping): string => `"${c.property}" ${c.sqlType}`).join(", ")})
where not exists (
  select 1 from ${table} h where h.source = 'netkeiba'
  and ${columns.map((c: ColumnMapping): string => `h.${c.column} is not distinct from r."${c.property}"`).join(" and ")}
)`,
  values: [JSON.stringify(rows)],
});

const buildBatches = (input: HistoryPublicationInput): readonly InsertInput[] => {
  input.horses.forEach(validateHorse);
  input.people.forEach(validatePerson);
  const batches: readonly InsertInput[] = [
    {
      table: "oversea_horse_race_history",
      columns: HORSE_COLUMNS,
      rows: input.horses,
      conflictTarget: "(source, source_horse_id, source_race_id) where source_race_id is not null",
    },
    {
      table: "oversea_person_race_history",
      columns: PERSON_COLUMNS,
      rows: input.people.filter(
        (row: SecondaryPersonResult): boolean => row.sourceHorseId !== null,
      ),
      conflictTarget:
        "(source, person_kind, source_person_id, source_race_id, source_horse_id) where source_horse_id is not null",
    },
    {
      table: "oversea_person_race_history",
      columns: PERSON_COLUMNS,
      rows: input.people.filter(
        (row: SecondaryPersonResult): boolean => row.sourceHorseId === null,
      ),
      conflictTarget:
        "(source, person_kind, source_person_id, source_race_id, horse_name) where source_horse_id is null and horse_name is not null",
    },
  ];
  return batches.filter((batch: InsertInput): boolean => batch.rows.length > 0);
};

export const buildHistoryStatements = (input: HistoryPublicationInput): readonly SqlStatement[] =>
  buildBatches(input).map(buildInsert);

export const buildHistoryVerificationStatements = (
  input: HistoryPublicationInput,
): readonly SqlStatement[] =>
  buildBatches(input).map(
    (batch: InsertInput): SqlStatement => buildReadback(batch, "count(*)::integer as mismatches"),
  );

// Missing includes conflicting natural keys: publication must still fail its
// all-column readback rather than overwrite existing source evidence.
export const buildMissingHistoryStatements = (
  input: HistoryPublicationInput,
): readonly SqlStatement[] =>
  buildBatches(input).map(
    (batch: InsertInput): SqlStatement => buildReadback(batch, "row_to_json(r) as row"),
  );

export const publishHistory = async (
  input: HistoryPublicationInput,
  ports: HistoryPublicationPorts,
): Promise<void> => {
  const statements: readonly SqlStatement[] = buildHistoryStatements(input);
  const checks: readonly SqlStatement[] = buildHistoryVerificationStatements(input);
  if (statements.length === 0) return;
  await ports.withTransaction(async (transaction: HistoryTransaction): Promise<void> => {
    for (const statement of statements) await transaction.write(statement);
    for (const statement of checks) {
      if ((await transaction.countMismatches(statement)) !== 0) {
        throw new Error("History content readback differs; transaction must roll back.");
      }
    }
  });
};
