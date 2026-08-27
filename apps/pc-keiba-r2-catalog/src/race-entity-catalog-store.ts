// Run with bun. Reads Catalog-managed Parquet directly through the native R2 binding.
import { md5 } from "@noble/hashes/legacy";
import { bytesToHex } from "@noble/hashes/utils";
import { parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";

import {
  normaliseRaceEntityHistoryRow,
  type RaceEntityCursorKey,
  type RaceEntityHistoryRow,
  type RaceEntityTarget,
} from "./race-entity-recent-results";
import type {
  CatalogSource,
  ObjectStore,
  RaceEntityRecentResultsFilters,
  RaceEntityType,
} from "./types";

const MANIFEST_KEY: string = "entity-catalog-serving-v1/manifest.json";
const MANIFEST_VERSION: number = 1;
const MAX_MANIFEST_BYTES: number = 2 * 1024 * 1024;
const MAX_PARQUET_BYTES: number = 8 * 1024 * 1024;
const TIME_PATTERN: RegExp = /^[0-2][0-9][0-5][0-9]$/u;
const BUCKET_PATTERN: RegExp = /^[0-9a-f]$/u;
const encoder: TextEncoder = new TextEncoder();

interface CatalogFile {
  key: string;
  size: number;
}

interface CatalogTableManifest {
  dataPrefix: string;
  partitions: Record<string, CatalogFile[]>;
  snapshotId: string;
}

interface CatalogManifest {
  history: CatalogTableManifest;
  raw: Record<string, CatalogTableManifest>;
  version: number;
}

interface DecodedCatalogFile {
  rows: Record<string, unknown>[];
  size: number;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const textOrNull = (value: unknown): string | null => {
  if (value === null || value === undefined) return null;
  const text = String(value).replaceAll("　", "").trim();
  return text.length === 0 ? null : text;
};

const requiredText = (row: Record<string, unknown>, name: string): string => {
  const value = textOrNull(row[name]);
  if (value === null) throw new Error(`Catalog row is missing ${name}`);
  return value;
};

const nullableNumber = (value: unknown, nullToken?: string): number | null => {
  const text = textOrNull(value);
  if (text === null || text === nullToken) return null;
  const number = Number(text);
  return Number.isFinite(number) ? number : null;
};

const scaledNumber = (value: unknown, nullToken: string, divisor: number): number | null => {
  const number = nullableNumber(value, nullToken);
  return number === null ? null : number / divisor;
};

const parseJson = (text: string): unknown => JSON.parse(text);

const parseCatalogFiles = (value: unknown, dataPrefix: string): CatalogFile[] => {
  if (!Array.isArray(value)) throw new Error("Catalog manifest file list is malformed");
  return value.map((entry) => {
    if (
      !Array.isArray(entry) ||
      entry.length !== 2 ||
      typeof entry[0] !== "string" ||
      typeof entry[1] !== "number" ||
      !Number.isSafeInteger(entry[1]) ||
      entry[1] <= 0
    ) {
      throw new Error("Catalog manifest file is malformed");
    }
    return { key: `${dataPrefix}${entry[0]}`, size: entry[1] };
  });
};

const parseCatalogTable = (value: unknown): CatalogTableManifest => {
  if (!isRecord(value) || typeof value.dataPrefix !== "string") {
    throw new Error("Catalog table manifest is malformed");
  }
  if (typeof value.snapshotId !== "string" || !isRecord(value.partitions)) {
    throw new Error("Catalog table snapshot is malformed");
  }
  const dataPrefix = value.dataPrefix;
  const snapshotId = value.snapshotId;
  const partitions = Object.fromEntries(
    Object.entries(value.partitions).map(([key, files]) => [
      key,
      parseCatalogFiles(files, dataPrefix),
    ]),
  );
  return { dataPrefix, partitions, snapshotId };
};

export const readEntityCatalogManifest = async (store: ObjectStore): Promise<CatalogManifest> => {
  const object = await store.get(MANIFEST_KEY);
  if (object === null || object.size <= 0 || object.size > MAX_MANIFEST_BYTES) {
    throw new Error("Catalog serving manifest is unavailable");
  }
  const value = parseJson(await new Response(object.body).text());
  if (!isRecord(value) || value.version !== MANIFEST_VERSION || !isRecord(value.raw)) {
    throw new Error("Catalog serving manifest is malformed");
  }
  const raw = Object.fromEntries(
    Object.entries(value.raw).map(([name, table]) => [name, parseCatalogTable(table)]),
  );
  return { history: parseCatalogTable(value.history), raw, version: MANIFEST_VERSION };
};

const readCatalogFile = async (
  store: ObjectStore,
  file: CatalogFile,
  columns?: string[],
): Promise<DecodedCatalogFile> => {
  if (file.size <= 0 || file.size > MAX_PARQUET_BYTES) {
    throw new Error("Catalog Parquet file exceeded size limit");
  }
  const object = await store.get(file.key);
  if (object === null || object.size !== file.size) {
    throw new Error("Catalog Parquet file is unavailable or changed");
  }
  const bytes = await new Response(object.body).arrayBuffer();
  if (bytes.byteLength !== file.size) throw new Error("Catalog Parquet read was truncated");
  return {
    rows: await parquetReadObjects({ columns, compressors, file: bytes }),
    size: bytes.byteLength,
  };
};

const partitionFiles = (table: CatalogTableManifest, partition: string): CatalogFile[] =>
  table.partitions[partition] ?? [];

const rawTable = (manifest: CatalogManifest, name: string): CatalogTableManifest => {
  const table = manifest.raw[name];
  if (table === undefined) throw new Error(`Catalog manifest is missing ${name}`);
  return table;
};

const targetColumns = (entityType: RaceEntityType): string[] => {
  const columns = [
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "umaban",
    "ketto_toroku_bango",
    "bamei",
  ];
  if (entityType === "jockey") return columns.concat("kishu_code", "kishumei_ryakusho");
  if (entityType === "trainer") {
    return columns.concat("chokyoshi_code", "chokyoshimei_ryakusho");
  }
  return entityType === "owner" ? columns.concat("banushi_code", "banushimei") : columns;
};

const entityFields = (entityType: RaceEntityType): [string, string] => {
  if (entityType === "horse") return ["ketto_toroku_bango", "bamei"];
  if (entityType === "jockey") return ["kishu_code", "kishumei_ryakusho"];
  if (entityType === "trainer") return ["chokyoshi_code", "chokyoshimei_ryakusho"];
  return ["banushi_code", "banushimei"];
};

const twoDigits = (value: unknown): string =>
  String(value ?? "")
    .trim()
    .padStart(2, "0");

const matchesRace = (
  row: Record<string, unknown>,
  filters: RaceEntityRecentResultsFilters,
): boolean =>
  `${requiredText(row, "kaisai_nen")}${requiredText(row, "kaisai_tsukihi").padStart(4, "0")}` ===
    filters.date &&
  twoDigits(row.keibajo_code) === filters.keibajoCode &&
  twoDigits(row.race_bango) === filters.raceBango;

const horseNumberEquals = (value: unknown, expected: string): boolean =>
  Number(textOrNull(value)) === Number(expected);

export const readEntityCatalogTarget = async (
  store: ObjectStore,
  manifest: CatalogManifest,
  filters: RaceEntityRecentResultsFilters,
): Promise<RaceEntityTarget | null> => {
  const year = filters.date.slice(0, 4);
  const tablePrefix = filters.source === "jra" ? "jvd" : "nvd";
  const runnerFiles = partitionFiles(rawTable(manifest, `${tablePrefix}_se`), year);
  const raceFiles = partitionFiles(rawTable(manifest, `${tablePrefix}_ra`), year);
  const [runnerParts, raceParts] = await Promise.all([
    Promise.all(
      runnerFiles.map((file) => readCatalogFile(store, file, targetColumns(filters.entityType))),
    ),
    Promise.all(
      raceFiles.map((file) =>
        readCatalogFile(store, file, [
          "kaisai_nen",
          "kaisai_tsukihi",
          "keibajo_code",
          "race_bango",
          "kyosomei_hondai",
          "hasso_jikoku",
        ]),
      ),
    ),
  ]);
  const race = raceParts.flatMap(({ rows }) => rows).find((row) => matchesRace(row, filters));
  if (race === undefined) return null;
  const runner = runnerParts
    .flatMap(({ rows }) => rows)
    .find((row) => matchesRace(row, filters) && horseNumberEquals(row.umaban, filters.horseNumber));
  if (runner === undefined) {
    return {
      entityBucket: null,
      entityId: null,
      entityName: null,
      horseId: null,
      horseName: null,
      raceName: textOrNull(race.kyosomei_hondai),
      raceStartTime: textOrNull(race.hasso_jikoku),
      runnerFound: false,
    };
  }
  const [idField, nameField] = entityFields(filters.entityType);
  const entityId = textOrNull(runner[idField]);
  return {
    entityBucket: entityId === null ? null : bytesToHex(md5(encoder.encode(entityId))).slice(0, 1),
    entityId,
    entityName: textOrNull(runner[nameField]),
    horseId: textOrNull(runner.ketto_toroku_bango),
    horseName: textOrNull(runner.bamei),
    raceName: textOrNull(race.kyosomei_hondai),
    raceStartTime: textOrNull(race.hasso_jikoku),
    runnerFound: true,
  };
};

const historyLowerYear = (filters: RaceEntityRecentResultsFilters, entityId: string): number => {
  if (filters.entityType !== "horse") return 1986;
  const registeredYear = Number(entityId.slice(0, 4));
  const fallback = Number(filters.date.slice(0, 4)) - 8;
  const firstRaceYear = Number.isInteger(registeredYear) ? registeredYear + 2 : fallback;
  return Math.max(1986, firstRaceYear);
};

const rowSortKey = (row: Record<string, unknown>): string =>
  `${requiredText(row, "kaisai_nen")}${requiredText(row, "kaisai_tsukihi")}${textOrNull(row.hasso_jikoku) ?? "0000"}`;

const rowResultId = (row: Record<string, unknown>): string => requiredText(row, "result_id");

const isBeforeCursor = (
  row: Record<string, unknown>,
  cursor: RaceEntityCursorKey | null,
): boolean => {
  if (cursor === null) return true;
  const sortKey = rowSortKey(row);
  const resultId = rowResultId(row);
  return (
    sortKey < cursor.raceStartSortKey ||
    (sortKey === cursor.raceStartSortKey && resultId < cursor.resultId)
  );
};

const isBeforeTarget = (
  row: Record<string, unknown>,
  filters: RaceEntityRecentResultsFilters,
  targetStartTime: string | null,
): boolean => {
  const year = requiredText(row, "kaisai_nen");
  const monthDay = requiredText(row, "kaisai_tsukihi");
  const rowDate = `${year}${monthDay}`;
  if (rowDate < filters.date) return true;
  if (rowDate > filters.date) return false;
  const rowTime = textOrNull(row.hasso_jikoku);
  if (
    targetStartTime !== null &&
    TIME_PATTERN.test(targetStartTime) &&
    rowTime !== null &&
    TIME_PATTERN.test(rowTime)
  ) {
    return rowTime < targetStartTime;
  }
  return (
    row.source === filters.source &&
    textOrNull(row.keibajo_code) === filters.keibajoCode &&
    requiredText(row, "race_bango") < filters.raceBango
  );
};

const objectRowToHistoryRow = (row: Record<string, unknown>): RaceEntityHistoryRow => {
  const source = row.source === "jra" ? "jra" : "nar";
  const year = requiredText(row, "kaisai_nen");
  const monthDay = requiredText(row, "kaisai_tsukihi");
  const venue = requiredText(row, "keibajo_code");
  const raceNumber = requiredText(row, "race_bango");
  const weightDifference = nullableNumber(row.zogen_sa, "000");
  return normaliseRaceEntityHistoryRow({
    abnormality_code: textOrNull(row.ijo_kubun_code),
    carried_weight: scaledNumber(row.futan_juryo, "000", 10),
    class_name: textOrNull(row.kyoso_joken_meisho),
    corner_1: textOrNull(row.corner_1),
    corner_2: textOrNull(row.corner_2),
    corner_3: textOrNull(row.corner_3),
    corner_4: textOrNull(row.corner_4),
    dirt_condition_code: textOrNull(row.babajotai_code_dirt),
    distance: nullableNumber(row.kyori),
    field_size: nullableNumber(row.shusso_tosu),
    final_3f_seconds: scaledNumber(row.kohan_3f, "000", 10),
    finish_position: nullableNumber(row.kakutei_chakujun, "00"),
    frame_number: textOrNull(row.wakuban),
    grade_code: textOrNull(row.grade_code),
    horse_id: textOrNull(row.ketto_toroku_bango),
    horse_name: textOrNull(row.bamei),
    horse_number: textOrNull(row.umaban),
    horse_weight: nullableNumber(row.bataiju, "000"),
    horse_weight_diff:
      weightDifference === null
        ? null
        : textOrNull(row.zogen_fugo) === "-"
          ? -weightDifference
          : weightDifference,
    jockey_id: textOrNull(row.kishu_code),
    jockey_name: textOrNull(row.kishumei_ryakusho),
    kaisai_nen: year,
    kaisai_tsukihi: monthDay,
    keibajo_code: venue,
    margin: textOrNull(row.time_sa),
    owner_id: textOrNull(row.banushi_code),
    owner_name: textOrNull(row.banushimei),
    popularity: nullableNumber(row.tansho_ninkijun, "00"),
    race_bango: raceNumber,
    race_id: `${source}:${year}${monthDay}:${venue}:${raceNumber}`,
    race_name: textOrNull(row.kyosomei_hondai),
    race_start_sort_key: rowSortKey(row),
    race_start_time: textOrNull(row.hasso_jikoku),
    race_time_seconds: scaledNumber(row.soha_time, "0000", 10),
    result_id: rowResultId(row),
    source,
    track_code: textOrNull(row.track_code),
    trainer_id: textOrNull(row.chokyoshi_code),
    trainer_name: textOrNull(row.chokyoshimei_ryakusho),
    turf_condition_code: textOrNull(row.babajotai_code_shiba),
    weather_code: textOrNull(row.tenko_code),
    win_odds: scaledNumber(row.tansho_odds, "0000", 10),
  });
};

const descendingRows = (left: RaceEntityHistoryRow, right: RaceEntityHistoryRow): number =>
  right.raceStartSortKey.localeCompare(left.raceStartSortKey) ||
  right.resultId.localeCompare(left.resultId);

export const readEntityCatalogHistory = async (
  store: ObjectStore,
  manifest: CatalogManifest,
  filters: RaceEntityRecentResultsFilters,
  target: RaceEntityTarget & { entityBucket: string; entityId: string },
  cursor: RaceEntityCursorKey | null,
): Promise<RaceEntityHistoryRow[]> => {
  if (!BUCKET_PATTERN.test(target.entityBucket)) throw new Error("entity bucket is malformed");
  const lowerYear = historyLowerYear(filters, target.entityId);
  const upperYear = Number(cursor?.raceStartSortKey.slice(0, 4) ?? filters.date.slice(0, 4));
  const sources: readonly CatalogSource[] =
    filters.entityType === "horse" ? ["jra", "nar"] : [filters.source];
  const rows: RaceEntityHistoryRow[] = [];
  for (let year = upperYear; year >= lowerYear && rows.length <= filters.limit; year -= 1) {
    const yearText = String(year);
    const files = sources.flatMap((source) =>
      partitionFiles(
        manifest.history,
        `${filters.entityType}/${source}/${target.entityBucket}/${yearText}`,
      ),
    );
    const parts = await Promise.all(files.map((file) => readCatalogFile(store, file)));
    parts.forEach(({ rows: decodedRows }) => {
      decodedRows.forEach((row) => {
        if (
          textOrNull(row.entity_id) === target.entityId &&
          isBeforeCursor(row, cursor) &&
          isBeforeTarget(row, filters, target.raceStartTime)
        ) {
          rows.push(objectRowToHistoryRow(row));
        }
      });
    });
  }
  return rows.sort(descendingRows).slice(0, filters.limit + 1);
};
