// This file runs with Bun.
//
// Persist secondary-source card identities into existing oversea_* tables.
// oversea_runner_source_id is the per-source mapping (netkeiba IDs live here).
// oversea_runner_identity is the single display-name row per race entry and
// currently holds jra-van names; netkeiba must not overwrite that PK.

import type { SecondarySourceRunner } from "../sources/secondary-source-parser";
import type { SqlStatement } from "../types";

export type OverseaEntitySource = "jra-van" | "netkeiba";
export type OverseaSourceIdSkipReason = "horseNumber" | "horseId";

export interface OverseaRunnerSourceIdRow {
  readonly race_source: string;
  readonly kaisai_nen: string;
  readonly kaisai_tsukihi: string;
  readonly keibajo_code: string;
  readonly race_bango: string;
  readonly umaban: string;
  readonly source: string;
  readonly source_horse_id: string;
  readonly source_jockey_id: string | null;
  readonly source_trainer_id: string | null;
  readonly source_owner_id: string | null;
  readonly gate_number: number | null;
  readonly source_url: string | null;
}

export interface OverseaRunnerIdentityRow {
  readonly race_source: string;
  readonly kaisai_nen: string;
  readonly kaisai_tsukihi: string;
  readonly keibajo_code: string;
  readonly race_bango: string;
  readonly umaban: string;
  readonly source: string;
  readonly source_horse_id: string;
  readonly horse_name_full: string;
  readonly jockey_name_full: string | null;
  readonly trainer_name_full: string | null;
  readonly owner_name_full: string | null;
  readonly source_url: string | null;
}

export interface PlanOverseaRunnerPersistInput {
  readonly runners: readonly SecondarySourceRunner[];
  readonly raceDate: string;
  readonly venueCode: string;
  readonly raceNumber: string;
  readonly source: OverseaEntitySource;
}

export interface OverseaSourceIdPlanLine {
  readonly umaban: string | null;
  readonly sourceHorseIdPresent: boolean;
  readonly planned: boolean;
}

export interface OverseaRunnerPersistPlan {
  readonly sourceIdRows: readonly OverseaRunnerSourceIdRow[];
  readonly identityRows: readonly OverseaRunnerIdentityRow[];
  readonly reportLines: readonly OverseaSourceIdPlanLine[];
  readonly skippedMissingHorseNumber: number;
  readonly skippedMissingHorseId: number;
  readonly writesIdentity: boolean;
}

interface CompactRaceDateParts {
  readonly kaisaiNen: string;
  readonly kaisaiTsukihi: string;
}

interface RaceEntryKey {
  readonly race_source: string;
  readonly kaisai_nen: string;
  readonly kaisai_tsukihi: string;
  readonly keibajo_code: string;
  readonly race_bango: string;
}

interface MapOneRunnerInput {
  readonly runner: SecondarySourceRunner;
  readonly raceKey: RaceEntryKey;
  readonly source: OverseaEntitySource;
  readonly writeIdentity: boolean;
}

interface MappedRunnerPersist {
  readonly sourceIdRow: OverseaRunnerSourceIdRow | null;
  readonly identityRow: OverseaRunnerIdentityRow | null;
  readonly reportLine: OverseaSourceIdPlanLine;
  readonly skipReason: OverseaSourceIdSkipReason | null;
}

interface OptionalTextInput {
  readonly value: string | null;
}

interface OptionalGateInput {
  readonly gate: number | null;
}

interface SkipReasonInput {
  readonly umaban: string | null;
  readonly sourceHorseIdPresent: boolean;
}

interface IdentityRowInput {
  readonly sourceIdRow: OverseaRunnerSourceIdRow;
  readonly horseName: string | null;
  readonly writeIdentity: boolean;
}

interface SourceIdRowInput {
  readonly runner: SecondarySourceRunner;
  readonly raceKey: RaceEntryKey;
  readonly source: OverseaEntitySource;
}

export const OVERSEA_SOURCE_NETKEIBA: OverseaEntitySource = "netkeiba";
const OVERSEA_RACE_SOURCE_JRA: string = "jra";
const OVERSEA_SOURCE_JRA_VAN: OverseaEntitySource = "jra-van";
const OVERSEA_RUNNER_SOURCE_ID_TABLE: string = "oversea_runner_source_id";
const OVERSEA_RUNNER_IDENTITY_TABLE: string = "oversea_runner_identity";

const UMABAN_WIDTH: number = 2;
const RACE_BANGO_WIDTH: number = 2;
const DATE_YEAR_LENGTH: number = 4;
const MINIMUM_GATE_NUMBER: number = 1;
const EMPTY_SQL_TEXT: string = "";
const SOURCE_HORSE_ID_PRESENT_LABEL: string = "present";
const SOURCE_HORSE_ID_ABSENT_LABEL: string = "absent";
const UMABAN_NONE_LABEL: string = "(none)";
const PLAN_REPORT_HEADER: string = "=== Planned oversea_runner_source_id upserts ===";
const IDENTITY_WRITE_YES: string = "identity table write=yes";
const IDENTITY_WRITE_NO: string = "identity table write=no";
const EMPTY_PLAN_LABEL: string = "(none)";

const SOURCE_ID_COLUMNS: readonly string[] = [
  "race_source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "umaban",
  "source",
  "source_horse_id",
  "source_jockey_id",
  "source_trainer_id",
  "source_owner_id",
  "gate_number",
  "source_url",
] satisfies readonly string[];

const SOURCE_ID_CONFLICT_COLUMNS: readonly string[] = [
  "race_source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "umaban",
  "source",
] satisfies readonly string[];

const SOURCE_ID_UPDATE_COLUMNS: readonly string[] = [
  "source_horse_id",
  "source_jockey_id",
  "source_trainer_id",
  "source_owner_id",
  "gate_number",
  "source_url",
] satisfies readonly string[];

const IDENTITY_COLUMNS: readonly string[] = [
  "race_source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "umaban",
  "source",
  "source_horse_id",
  "horse_name_full",
  "jockey_name_full",
  "trainer_name_full",
  "owner_name_full",
  "source_url",
] satisfies readonly string[];

const IDENTITY_CONFLICT_COLUMNS: readonly string[] = [
  "race_source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "umaban",
] satisfies readonly string[];

const IDENTITY_UPDATE_COLUMNS: readonly string[] = [
  "source",
  "source_horse_id",
  "horse_name_full",
  "jockey_name_full",
  "trainer_name_full",
  "owner_name_full",
  "source_url",
] satisfies readonly string[];

const NULLABLE_TEXT_COLUMNS: ReadonlySet<string> = new Set([
  "source_jockey_id",
  "source_trainer_id",
  "source_owner_id",
  "jockey_name_full",
  "trainer_name_full",
  "owner_name_full",
  "source_url",
]);

const NULLABLE_SMALLINT_COLUMNS: ReadonlySet<string> = new Set(["gate_number"]);

/**
 * The identity table PK is one display row per race entry. Only the jra-van
 * display source may refresh that row. netkeiba IDs belong in source_id.
 */
export const shouldWriteOverseaRunnerIdentity = (source: OverseaEntitySource): boolean =>
  source === OVERSEA_SOURCE_JRA_VAN;

const padFixedWidth = (value: number | string, width: number): string =>
  String(value).padStart(width, "0");

const splitCompactRaceDate = (raceDate: string): CompactRaceDateParts => {
  const compact: string = raceDate.replaceAll("-", "");
  return {
    kaisaiNen: compact.slice(0, DATE_YEAR_LENGTH),
    kaisaiTsukihi: compact.slice(DATE_YEAR_LENGTH),
  };
};

const normalizeOptionalText = ({ value }: OptionalTextInput): string | null => {
  if (value === null || value.length === 0) {
    return null;
  }
  return value;
};

const normalizeGateNumber = ({ gate }: OptionalGateInput): number | null => {
  if (gate === null || !Number.isInteger(gate) || gate < MINIMUM_GATE_NUMBER) {
    return null;
  }
  return gate;
};

const resolveSkipReason = ({
  umaban,
  sourceHorseIdPresent,
}: SkipReasonInput): OverseaSourceIdSkipReason | null => {
  if (umaban === null) {
    return "horseNumber";
  }
  if (!sourceHorseIdPresent) {
    return "horseId";
  }
  return null;
};

const sqlTextOrEmpty = (value: string | null): string => (value === null ? EMPTY_SQL_TEXT : value);

const sqlGateOrEmpty = (value: number | null): string =>
  value === null ? EMPTY_SQL_TEXT : String(value);

const valuePlaceholder = (column: string, index: number): string => {
  const parameter: string = `$${String(index + 1)}`;
  if (NULLABLE_SMALLINT_COLUMNS.has(column)) {
    return `NULLIF(${parameter}, '')::smallint`;
  }
  if (NULLABLE_TEXT_COLUMNS.has(column)) {
    return `NULLIF(${parameter}, '')`;
  }
  return parameter;
};

const buildPreservingAssignment = (column: string): string => `${column} = excluded.${column}`;

const buildInsertUpsert = (input: {
  readonly table: string;
  readonly columns: readonly string[];
  readonly conflictColumns: readonly string[];
  readonly updateColumns: readonly string[];
  readonly values: readonly string[];
}): SqlStatement => ({
  text: `INSERT INTO ${input.table} (${input.columns.join(", ")}) VALUES (${input.columns
    .map((column: string, index: number): string => valuePlaceholder(column, index))
    .join(", ")}) ON CONFLICT (${input.conflictColumns.join(", ")}) DO UPDATE SET ${[
    ...input.updateColumns.map(buildPreservingAssignment),
    "updated_at = now()",
  ].join(", ")}`,
  values: input.values,
});

const toRaceEntryKey = (input: PlanOverseaRunnerPersistInput): RaceEntryKey => {
  const date: CompactRaceDateParts = splitCompactRaceDate(input.raceDate);
  return {
    race_source: OVERSEA_RACE_SOURCE_JRA,
    kaisai_nen: date.kaisaiNen,
    kaisai_tsukihi: date.kaisaiTsukihi,
    keibajo_code: input.venueCode,
    race_bango: padFixedWidth(input.raceNumber, RACE_BANGO_WIDTH),
  };
};

const toSourceIdRow = ({
  runner,
  raceKey,
  source,
}: SourceIdRowInput): OverseaRunnerSourceIdRow | null => {
  if (runner.horseNumber === null) {
    return null;
  }
  const sourceHorseId: string | null = normalizeOptionalText({ value: runner.horseId });
  if (sourceHorseId === null) {
    return null;
  }
  return {
    race_source: raceKey.race_source,
    kaisai_nen: raceKey.kaisai_nen,
    kaisai_tsukihi: raceKey.kaisai_tsukihi,
    keibajo_code: raceKey.keibajo_code,
    race_bango: raceKey.race_bango,
    umaban: padFixedWidth(runner.horseNumber, UMABAN_WIDTH),
    source,
    source_horse_id: sourceHorseId,
    source_jockey_id: normalizeOptionalText({ value: runner.jockeyId }),
    source_trainer_id: normalizeOptionalText({ value: runner.trainerId }),
    source_owner_id: null,
    gate_number: normalizeGateNumber({ gate: runner.gate }),
    source_url: null,
  };
};

const toIdentityRow = ({
  sourceIdRow,
  horseName,
  writeIdentity,
}: IdentityRowInput): OverseaRunnerIdentityRow | null => {
  if (!writeIdentity) {
    return null;
  }
  const horseNameFull: string | null = normalizeOptionalText({ value: horseName });
  if (horseNameFull === null) {
    return null;
  }
  return {
    race_source: sourceIdRow.race_source,
    kaisai_nen: sourceIdRow.kaisai_nen,
    kaisai_tsukihi: sourceIdRow.kaisai_tsukihi,
    keibajo_code: sourceIdRow.keibajo_code,
    race_bango: sourceIdRow.race_bango,
    umaban: sourceIdRow.umaban,
    source: sourceIdRow.source,
    source_horse_id: sourceIdRow.source_horse_id,
    horse_name_full: horseNameFull,
    jockey_name_full: null,
    trainer_name_full: null,
    owner_name_full: null,
    source_url: null,
  };
};

const mapOneRunner = ({
  runner,
  raceKey,
  source,
  writeIdentity,
}: MapOneRunnerInput): MappedRunnerPersist => {
  const sourceIdRow: OverseaRunnerSourceIdRow | null = toSourceIdRow({
    runner,
    raceKey,
    source,
  });
  const umaban: string | null =
    runner.horseNumber === null ? null : padFixedWidth(runner.horseNumber, UMABAN_WIDTH);
  const sourceHorseIdPresent: boolean = normalizeOptionalText({ value: runner.horseId }) !== null;
  return {
    sourceIdRow,
    identityRow:
      sourceIdRow === null
        ? null
        : toIdentityRow({
            sourceIdRow,
            horseName: runner.horseName,
            writeIdentity,
          }),
    reportLine: {
      umaban,
      sourceHorseIdPresent,
      planned: sourceIdRow !== null,
    },
    skipReason: resolveSkipReason({ umaban, sourceHorseIdPresent }),
  };
};

const isHorseNumberSkip = (item: MappedRunnerPersist): boolean => item.skipReason === "horseNumber";

const isHorseIdSkip = (item: MappedRunnerPersist): boolean => item.skipReason === "horseId";

const mappedSourceIdRow = (item: MappedRunnerPersist): readonly OverseaRunnerSourceIdRow[] =>
  item.sourceIdRow === null ? [] : [item.sourceIdRow];

const mappedIdentityRow = (item: MappedRunnerPersist): readonly OverseaRunnerIdentityRow[] =>
  item.identityRow === null ? [] : [item.identityRow];

export const planOverseaRunnerPersist = (
  input: PlanOverseaRunnerPersistInput,
): OverseaRunnerPersistPlan => {
  const writeIdentity: boolean = shouldWriteOverseaRunnerIdentity(input.source);
  const raceKey: RaceEntryKey = toRaceEntryKey(input);
  const mapped: readonly MappedRunnerPersist[] = input.runners.map(
    (runner: SecondarySourceRunner): MappedRunnerPersist =>
      mapOneRunner({
        runner,
        raceKey,
        source: input.source,
        writeIdentity,
      }),
  );
  return {
    sourceIdRows: mapped.flatMap(mappedSourceIdRow),
    identityRows: mapped.flatMap(mappedIdentityRow),
    reportLines: mapped.map(
      (item: MappedRunnerPersist): OverseaSourceIdPlanLine => item.reportLine,
    ),
    skippedMissingHorseNumber: mapped.filter(isHorseNumberSkip).length,
    skippedMissingHorseId: mapped.filter(isHorseIdSkip).length,
    writesIdentity: writeIdentity,
  };
};

const formatPlanLine = (line: OverseaSourceIdPlanLine): string => {
  const umabanLabel: string = line.umaban === null ? UMABAN_NONE_LABEL : line.umaban;
  const presenceLabel: string = line.sourceHorseIdPresent
    ? SOURCE_HORSE_ID_PRESENT_LABEL
    : SOURCE_HORSE_ID_ABSENT_LABEL;
  return `umaban=${umabanLabel} source_horse_id=${presenceLabel}`;
};

export const formatOverseaRunnerSourceIdPlanReport = (
  plan: OverseaRunnerPersistPlan,
): readonly string[] => {
  const runnerLines: readonly string[] =
    plan.reportLines.length === 0 ? [EMPTY_PLAN_LABEL] : plan.reportLines.map(formatPlanLine);
  return [
    PLAN_REPORT_HEADER,
    ...runnerLines,
    `skipped missing horseNumber=${String(plan.skippedMissingHorseNumber)} missing horseId=${String(plan.skippedMissingHorseId)}`,
    plan.writesIdentity ? IDENTITY_WRITE_YES : IDENTITY_WRITE_NO,
  ];
};

export const buildOverseaRunnerSourceIdUpsert = (row: OverseaRunnerSourceIdRow): SqlStatement =>
  buildInsertUpsert({
    table: OVERSEA_RUNNER_SOURCE_ID_TABLE,
    columns: SOURCE_ID_COLUMNS,
    conflictColumns: SOURCE_ID_CONFLICT_COLUMNS,
    updateColumns: SOURCE_ID_UPDATE_COLUMNS,
    values: [
      row.race_source,
      row.kaisai_nen,
      row.kaisai_tsukihi,
      row.keibajo_code,
      row.race_bango,
      row.umaban,
      row.source,
      row.source_horse_id,
      sqlTextOrEmpty(row.source_jockey_id),
      sqlTextOrEmpty(row.source_trainer_id),
      sqlTextOrEmpty(row.source_owner_id),
      sqlGateOrEmpty(row.gate_number),
      sqlTextOrEmpty(row.source_url),
    ],
  });

export const buildOverseaRunnerIdentityUpsert = (row: OverseaRunnerIdentityRow): SqlStatement =>
  buildInsertUpsert({
    table: OVERSEA_RUNNER_IDENTITY_TABLE,
    columns: IDENTITY_COLUMNS,
    conflictColumns: IDENTITY_CONFLICT_COLUMNS,
    updateColumns: IDENTITY_UPDATE_COLUMNS,
    values: [
      row.race_source,
      row.kaisai_nen,
      row.kaisai_tsukihi,
      row.keibajo_code,
      row.race_bango,
      row.umaban,
      row.source,
      row.source_horse_id,
      row.horse_name_full,
      sqlTextOrEmpty(row.jockey_name_full),
      sqlTextOrEmpty(row.trainer_name_full),
      sqlTextOrEmpty(row.owner_name_full),
      sqlTextOrEmpty(row.source_url),
    ],
  });

export const buildOverseaPersistStatements = (
  plan: OverseaRunnerPersistPlan,
): readonly SqlStatement[] => [
  ...plan.sourceIdRows.map(buildOverseaRunnerSourceIdUpsert),
  ...plan.identityRows.map(buildOverseaRunnerIdentityUpsert),
];
