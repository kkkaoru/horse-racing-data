// Run with bun. Reads bounded entity history partitions through the native R2 binding.
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

const PREFIX = "entity-serving-v1";
const OBJECT_VERSION = 1;
const MAX_COMPRESSED_BYTES = 16 * 1024 * 1024;
const MAX_DECOMPRESSED_CHARACTERS = 64 * 1024 * 1024;
const TIME_PATTERN = /^[0-2][0-9][0-5][0-9]$/u;
const BUCKET_PATTERN = /^[0-9a-f]$/u;
const YEAR_PATTERN = /^(?:19|20)[0-9]{2}$/u;

interface ObjectEnvelope {
  rows: Record<string, unknown>[];
  version: number;
}

interface GenerationManifest {
  version: number;
  years: Record<string, string>;
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
  if (value === null) throw new Error(`entity object row is missing ${name}`);
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

const parseJson = (text: string): unknown => JSON.parse(text) as unknown;

const readGzipJson = async (store: ObjectStore, key: string): Promise<unknown | null> => {
  const object = await store.get(key);
  if (object === null) return null;
  if (object.size > MAX_COMPRESSED_BYTES)
    throw new Error("entity object exceeded compressed limit");
  const decompressed = object.body.pipeThrough(new DecompressionStream("gzip"));
  const text = await new Response(decompressed).text();
  if (text.length > MAX_DECOMPRESSED_CHARACTERS) {
    throw new Error("entity object exceeded decompressed limit");
  }
  return parseJson(text);
};

const readEnvelope = async (store: ObjectStore, key: string): Promise<ObjectEnvelope | null> => {
  const value = await readGzipJson(store, key);
  if (value === null) return null;
  if (!isRecord(value) || value.version !== OBJECT_VERSION || !Array.isArray(value.rows)) {
    throw new Error("entity object envelope is malformed");
  }
  if (!value.rows.every(isRecord)) throw new Error("entity object rows are malformed");
  return { rows: value.rows, version: OBJECT_VERSION };
};

export const readEntityGenerationManifest = async (
  store: ObjectStore,
): Promise<GenerationManifest> => {
  const value = await store.get(`${PREFIX}/generations.json`);
  if (value === null || value.size > 1024 * 1024) {
    throw new Error("entity object generation manifest is unavailable");
  }
  const parsed = parseJson(await new Response(value.body).text());
  if (!isRecord(parsed) || parsed.version !== OBJECT_VERSION || !isRecord(parsed.years)) {
    throw new Error("entity object generation manifest is malformed");
  }
  const years = Object.fromEntries(
    Object.entries(parsed.years).filter(
      (entry): entry is [string, string] =>
        YEAR_PATTERN.test(entry[0]) && typeof entry[1] === "string" && entry[1].length > 0,
    ),
  );
  if (Object.keys(years).length === 0) {
    throw new Error("entity object generation manifest contains no years");
  }
  return { version: OBJECT_VERSION, years };
};

const targetObjectKey = (
  year: string,
  generation: string,
  source: CatalogSource,
  monthDay: string,
): string => `${PREFIX}/data/${year}/${generation}/target/${source}/${monthDay}.json.gz`;

const historyObjectKey = (
  year: string,
  generation: string,
  entityType: RaceEntityType,
  source: CatalogSource,
  bucket: string,
  shard: string,
): string =>
  `${PREFIX}/data/${year}/${generation}/history/${entityType}/${source}/${bucket}-${shard}.json.gz`;

const horseNumberEquals = (value: unknown, expected: string): boolean =>
  Number(textOrNull(value)) === Number(expected);

export const readEntityObjectTarget = async (
  store: ObjectStore,
  manifest: GenerationManifest,
  filters: RaceEntityRecentResultsFilters,
): Promise<RaceEntityTarget | null> => {
  const year = filters.date.slice(0, 4);
  const generation = manifest.years[year];
  if (generation === undefined) return null;
  const envelope = await readEnvelope(
    store,
    targetObjectKey(year, generation, filters.source, filters.date.slice(4)),
  );
  if (envelope === null) return null;
  const row = envelope.rows.find(
    (candidate) =>
      candidate.entity_type === filters.entityType &&
      candidate.source === filters.source &&
      textOrNull(candidate.keibajo_code) === filters.keibajoCode &&
      textOrNull(candidate.race_bango) === filters.raceBango &&
      horseNumberEquals(candidate.umaban, filters.horseNumber),
  );
  if (row === undefined) return null;
  return {
    entityBucket: textOrNull(row.entity_bucket),
    entityId: textOrNull(row.entity_id),
    entityName: textOrNull(row.entity_name),
    horseId: textOrNull(row.ketto_toroku_bango),
    horseName: textOrNull(row.bamei),
    raceName: textOrNull(row.kyosomei_hondai),
    raceStartTime: textOrNull(row.hasso_jikoku),
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

export const readEntityObjectHistory = async (
  store: ObjectStore,
  manifest: GenerationManifest,
  filters: RaceEntityRecentResultsFilters,
  target: RaceEntityTarget & { entityBucket: string; entityId: string },
  cursor: RaceEntityCursorKey | null,
): Promise<RaceEntityHistoryRow[]> => {
  if (!BUCKET_PATTERN.test(target.entityBucket)) throw new Error("entity bucket is malformed");
  if (!/[0-9]$/u.test(target.entityId)) throw new Error("entity shard is malformed");
  const lowerYear = historyLowerYear(filters, target.entityId);
  const upperYear = Number(cursor?.raceStartSortKey.slice(0, 4) ?? filters.date.slice(0, 4));
  const sources: readonly CatalogSource[] =
    filters.entityType === "horse" ? ["jra", "nar"] : [filters.source];
  const rows: RaceEntityHistoryRow[] = [];
  for (let year = upperYear; year >= lowerYear && rows.length <= filters.limit; year -= 1) {
    const yearText = String(year);
    const generation = manifest.years[yearText];
    if (generation === undefined) continue;
    const envelopes = await Promise.all(
      sources.map((source) =>
        readEnvelope(
          store,
          historyObjectKey(
            yearText,
            generation,
            filters.entityType,
            source,
            target.entityBucket,
            target.entityId.slice(-1),
          ),
        ),
      ),
    );
    for (const envelope of envelopes) {
      if (envelope === null) continue;
      for (const row of envelope.rows) {
        if (
          row.entity_id === target.entityId &&
          isBeforeCursor(row, cursor) &&
          isBeforeTarget(row, filters, target.raceStartTime)
        ) {
          rows.push(objectRowToHistoryRow(row));
        }
      }
    }
  }
  return rows.sort(descendingRows).slice(0, filters.limit + 1);
};
