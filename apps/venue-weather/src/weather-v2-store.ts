// Run with bun.
import type { UpsertParams, WeatherRow } from "./types";

const R2_LIVE_PREFIX_V2 = "venue-weather-live/v2";
const R2_SNAPSHOT_PREFIX_V2 = "venue-weather-snapshots/v2";
const JSON_CONTENT_TYPE = "application/json";
const SCHEMA_VERSION = 2;

interface CompleteWeatherV2Metrics {
  dewPoint: number;
  relativeHumidity: number;
  shortwaveRadiation: number;
  wetBulbTemperature: number;
}

export interface WeatherV2Row {
  dew_point: number;
  keibajo_code: string;
  precipitation: number | null;
  race_date: string;
  relative_humidity: number;
  shortwave_radiation: number;
  temperature: number | null;
  weather_code: number | null;
  weather_hour: number;
  wet_bulb_temperature: number;
  wind_gusts: number | null;
  wind_speed: number | null;
}

interface WeatherV2CatalogEvent extends WeatherV2Row {
  fetched_at: string;
  latitude: number;
  longitude: number;
  venue_name: string;
  weather_data_type: UpsertParams["weatherType"];
}

interface StoredWeatherV2Payload {
  fetchedAt: string;
  keibajoCode: string;
  raceDate: string;
  rows: WeatherV2Row[];
  schemaVersion: 2;
  weatherDataType: UpsertParams["weatherType"];
}

const sanitizePathSegment = (value: string): string => value.replace(/[^A-Za-z0-9_:-]/g, "_");

export const buildLiveWeatherV2R2Key = (
  raceDate: string,
  keibajoCode: string,
  weatherType: UpsertParams["weatherType"],
): string =>
  `${R2_LIVE_PREFIX_V2}/race_date=${sanitizePathSegment(raceDate)}/keibajo_code=${sanitizePathSegment(keibajoCode)}/${weatherType}.json`;

export const buildSnapshotWeatherV2R2Key = (
  raceDate: string,
  keibajoCode: string,
  weatherType: UpsertParams["weatherType"],
  fetchedAt: string,
): string =>
  `${R2_SNAPSHOT_PREFIX_V2}/race_date=${sanitizePathSegment(raceDate)}/keibajo_code=${sanitizePathSegment(keibajoCode)}/${weatherType}/${sanitizePathSegment(fetchedAt)}.json`;

const hasV2Metrics = (row: WeatherRow): row is WeatherRow & CompleteWeatherV2Metrics =>
  typeof row.dewPoint === "number" &&
  typeof row.relativeHumidity === "number" &&
  typeof row.shortwaveRadiation === "number" &&
  typeof row.wetBulbTemperature === "number";

const buildPayload = (
  params: Omit<UpsertParams, "archive" | "catalogStream">,
): StoredWeatherV2Payload | null => {
  if (params.rows.length === 0 || !params.rows.every(hasV2Metrics)) return null;
  return {
    fetchedAt: params.fetchedAt,
    keibajoCode: params.keibajoCode,
    raceDate: params.raceDate,
    rows: params.rows.map((row) => ({
      dew_point: row.dewPoint,
      keibajo_code: params.keibajoCode,
      precipitation: row.precipitation,
      race_date: params.raceDate,
      relative_humidity: row.relativeHumidity,
      shortwave_radiation: row.shortwaveRadiation,
      temperature: row.temperature,
      weather_code: row.weatherCode,
      weather_hour: row.hour,
      wet_bulb_temperature: row.wetBulbTemperature,
      wind_gusts: row.windGusts,
      wind_speed: row.windSpeed,
    })),
    schemaVersion: SCHEMA_VERSION,
    weatherDataType: params.weatherType,
  };
};

const parseStoredPayload = async (object: R2ObjectBody): Promise<StoredWeatherV2Payload | null> => {
  const parsed = (await object.json()) as Partial<StoredWeatherV2Payload>;
  if (
    parsed.schemaVersion !== SCHEMA_VERSION ||
    typeof parsed.raceDate !== "string" ||
    !Array.isArray(parsed.rows)
  ) {
    return null;
  }
  return {
    fetchedAt: parsed.fetchedAt ?? "",
    keibajoCode: parsed.keibajoCode ?? "",
    raceDate: parsed.raceDate,
    rows: parsed.rows,
    schemaVersion: SCHEMA_VERSION,
    weatherDataType: parsed.weatherDataType ?? "forecast",
  };
};

const putJson = async (archive: R2Bucket, key: string, value: unknown): Promise<void> => {
  await archive.put(key, JSON.stringify(value), {
    httpMetadata: { contentType: JSON_CONTENT_TYPE },
  });
};

const toCatalogEvents = (
  payload: StoredWeatherV2Payload,
  venue: UpsertParams["venue"],
): WeatherV2CatalogEvent[] =>
  payload.rows.map((row) => ({
    ...row,
    fetched_at: payload.fetchedAt,
    latitude: venue.lat,
    longitude: venue.lon,
    venue_name: venue.name,
    weather_data_type: payload.weatherDataType,
  }));

export const putVenueWeatherV2RuntimeObjects = async (
  params: Omit<UpsertParams, "catalogStream">,
): Promise<number> => {
  const payload = buildPayload(params);
  if (payload === null) return 0;
  await Promise.all([
    putJson(
      params.archive,
      buildLiveWeatherV2R2Key(params.raceDate, params.keibajoCode, params.weatherType),
      payload,
    ),
    putJson(
      params.archive,
      buildSnapshotWeatherV2R2Key(
        params.raceDate,
        params.keibajoCode,
        params.weatherType,
        params.fetchedAt,
      ),
      payload,
    ),
  ]);
  return payload.rows.length;
};

const buildDatePrefix = (raceDate: string): string =>
  `${R2_LIVE_PREFIX_V2}/race_date=${sanitizePathSegment(raceDate)}/`;

const listLiveKeys = async (
  archive: R2Bucket,
  raceDate: string,
  cursor?: string,
): Promise<string[]> => {
  const result = await archive.list({ cursor, prefix: buildDatePrefix(raceDate) });
  const keys = result.objects.map((object) => object.key);
  return result.truncated
    ? [...keys, ...(await listLiveKeys(archive, raceDate, result.cursor))]
    : keys;
};

const keyBase = (key: string): string =>
  key.endsWith("/actual.json")
    ? key.slice(0, -"/actual.json".length)
    : key.slice(0, -"/forecast.json".length);

const preferActualKeys = (keys: string[]): string[] => {
  const actualBases = new Set(keys.filter((key) => key.endsWith("/actual.json")).map(keyBase));
  return keys.filter((key) => !key.endsWith("/forecast.json") || !actualBases.has(keyBase(key)));
};

export const readWeatherV2ByDate = async (
  archive: R2Bucket,
  raceDate: string,
): Promise<WeatherV2Row[]> => {
  const payloads = await Promise.all(
    preferActualKeys(await listLiveKeys(archive, raceDate)).map(async (key) => {
      const object = await archive.get(key).catch(() => null);
      return object === null ? null : parseStoredPayload(object);
    }),
  );
  return payloads
    .filter((payload): payload is StoredWeatherV2Payload => payload?.raceDate === raceDate)
    .flatMap((payload) => payload.rows)
    .sort((left, right) =>
      left.keibajo_code === right.keibajo_code
        ? left.weather_hour - right.weather_hour
        : left.keibajo_code.localeCompare(right.keibajo_code),
    );
};

export const sendVenueWeatherV2CatalogEvents = async (
  params: Omit<UpsertParams, "archive" | "catalogStream"> & {
    catalogStream: UpsertParams["catalogStream"];
  },
): Promise<number> => {
  const payload = buildPayload(params);
  if (payload === null || params.catalogStream === undefined) return 0;
  const events = toCatalogEvents(payload, params.venue);
  await params.catalogStream.send(events);
  return events.length;
};
