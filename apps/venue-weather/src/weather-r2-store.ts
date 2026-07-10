// Run with bun.
import type { UpsertParams, VenueCoord, WeatherCacheRow, WeatherRow, WeatherType } from "./types";

const R2_LIVE_PREFIX = "venue-weather-live/v1";
const R2_SNAPSHOT_PREFIX = "venue-weather-snapshots/v1";
const R2_CATALOG_STAGING_PREFIX = "venue-weather-catalog-staging/v1";
const JSON_CONTENT_TYPE = "application/json";
const NDJSON_CONTENT_TYPE = "application/x-ndjson";

interface StoredWeatherPayload {
  fetchedAt: string;
  keibajoCode: string;
  raceDate: string;
  rows: WeatherCacheRow[];
  schemaVersion: 1;
  venue: VenueCoord;
  weatherDataType: WeatherType;
}

interface WeatherCatalogEvent {
  fetched_at: string;
  keibajo_code: string;
  latitude: number;
  longitude: number;
  precipitation: number | null;
  race_date: string;
  temperature: number | null;
  venue_name: string;
  weather_code: number | null;
  weather_data_type: WeatherType;
  weather_hour: number;
  wind_gusts: number | null;
  wind_speed: number | null;
}

export interface BackfillWeatherPayload {
  fetchedAt: string;
  keibajoCode: string;
  raceDate: string;
  rows: WeatherRow[];
  venue: VenueCoord;
  weatherType: WeatherType;
}

interface CatalogBatchEvent {
  fetched_at: string;
  keibajo_code: string;
  latitude: number;
  longitude: number;
  precipitation: number | null;
  race_date: string;
  temperature: number | null;
  venue_name: string;
  weather_code: number | null;
  weather_data_type: WeatherType;
  weather_hour: number;
  wind_gusts: number | null;
  wind_speed: number | null;
}

const sanitizePathSegment = (value: string): string => value.replace(/[^A-Za-z0-9_:-]/g, "_");

const buildDatePrefix = (raceDate: string): string =>
  `${R2_LIVE_PREFIX}/race_date=${sanitizePathSegment(raceDate)}/`;

export const buildLiveWeatherR2Key = (
  raceDate: string,
  keibajoCode: string,
  weatherType: WeatherType,
): string =>
  `${buildDatePrefix(raceDate)}keibajo_code=${sanitizePathSegment(keibajoCode)}/${weatherType}.json`;

export const buildSnapshotWeatherR2Key = (
  raceDate: string,
  keibajoCode: string,
  weatherType: WeatherType,
  fetchedAt: string,
): string =>
  `${R2_SNAPSHOT_PREFIX}/race_date=${sanitizePathSegment(raceDate)}/keibajo_code=${sanitizePathSegment(keibajoCode)}/${weatherType}/${sanitizePathSegment(fetchedAt)}.json`;

export const buildCatalogStagingWeatherR2Key = (
  raceDate: string,
  keibajoCode: string,
  weatherType: WeatherType,
  fetchedAt: string,
): string =>
  `${R2_CATALOG_STAGING_PREFIX}/race_date=${sanitizePathSegment(raceDate)}/keibajo_code=${sanitizePathSegment(keibajoCode)}/${weatherType}/${sanitizePathSegment(fetchedAt)}.ndjson`;

export const buildCatalogBatchStagingWeatherR2Key = (fetchedAt: string): string =>
  `${R2_CATALOG_STAGING_PREFIX}/backfill-batches/${sanitizePathSegment(fetchedAt)}.ndjson`;

const toCacheRows = (
  keibajoCode: string,
  raceDate: string,
  rows: WeatherRow[],
): WeatherCacheRow[] =>
  rows.map((row) => ({
    keibajo_code: keibajoCode,
    precipitation: row.precipitation,
    race_date: raceDate,
    temperature: row.temperature,
    weather_hour: row.hour,
    weather_type: row.weatherCode,
    wind_gusts: row.windGusts,
    wind_speed: row.windSpeed,
  }));

const sortRows = (rows: WeatherCacheRow[]): WeatherCacheRow[] =>
  [...rows].sort((a, b) => {
    const venueCompare = a.keibajo_code.localeCompare(b.keibajo_code);
    if (venueCompare !== 0) return venueCompare;
    return a.weather_hour - b.weather_hour;
  });

const buildPayload = ({
  fetchedAt,
  keibajoCode,
  raceDate,
  rows,
  venue,
  weatherType,
}: Omit<UpsertParams, "archive" | "catalogStream">): StoredWeatherPayload => ({
  fetchedAt,
  keibajoCode,
  raceDate,
  rows: sortRows(toCacheRows(keibajoCode, raceDate, rows)),
  schemaVersion: 1,
  venue,
  weatherDataType: weatherType,
});

const parseStoredPayload = async (object: R2ObjectBody): Promise<StoredWeatherPayload | null> => {
  const parsed = (await object.json()) as Partial<StoredWeatherPayload>;
  if (
    parsed.schemaVersion !== 1 ||
    typeof parsed.raceDate !== "string" ||
    typeof parsed.keibajoCode !== "string" ||
    !Array.isArray(parsed.rows)
  ) {
    return null;
  }
  return {
    fetchedAt: parsed.fetchedAt ?? "",
    keibajoCode: parsed.keibajoCode,
    raceDate: parsed.raceDate,
    rows: parsed.rows,
    schemaVersion: 1,
    venue: parsed.venue ?? { lat: 0, lon: 0, name: "" },
    weatherDataType: parsed.weatherDataType ?? "forecast",
  };
};

const putJson = async (archive: R2Bucket, key: string, value: unknown): Promise<void> => {
  await archive.put(key, JSON.stringify(value), {
    httpMetadata: { contentType: JSON_CONTENT_TYPE },
  });
};

const toCatalogEvents = (payload: StoredWeatherPayload): WeatherCatalogEvent[] =>
  payload.rows.map((row) => ({
    fetched_at: payload.fetchedAt,
    keibajo_code: payload.keibajoCode,
    latitude: payload.venue.lat,
    longitude: payload.venue.lon,
    precipitation: row.precipitation,
    race_date: payload.raceDate,
    temperature: row.temperature,
    venue_name: payload.venue.name,
    weather_code: row.weather_type,
    weather_data_type: payload.weatherDataType,
    weather_hour: row.weather_hour,
    wind_gusts: row.wind_gusts,
    wind_speed: row.wind_speed,
  }));

const payloadToCatalogEvents = (payload: BackfillWeatherPayload): CatalogBatchEvent[] =>
  payload.rows.map((row) => ({
    fetched_at: payload.fetchedAt,
    keibajo_code: payload.keibajoCode,
    latitude: payload.venue.lat,
    longitude: payload.venue.lon,
    precipitation: row.precipitation,
    race_date: payload.raceDate,
    temperature: row.temperature,
    venue_name: payload.venue.name,
    weather_code: row.weatherCode,
    weather_data_type: payload.weatherType,
    weather_hour: row.hour,
    wind_gusts: row.windGusts,
    wind_speed: row.windSpeed,
  }));

const putCatalogStagingEvents = async ({
  archive,
  catalogStream,
  fetchedAt,
  keibajoCode,
  payload,
  raceDate,
  weatherType,
}: {
  archive: R2Bucket;
  catalogStream?: UpsertParams["catalogStream"];
  fetchedAt: string;
  keibajoCode: string;
  payload: StoredWeatherPayload;
  raceDate: string;
  weatherType: WeatherType;
}): Promise<void> => {
  const events = toCatalogEvents(payload);
  if (events.length === 0) {
    return;
  }
  await catalogStream?.send(events).catch(() => undefined);
  const body = `${events.map((event) => JSON.stringify(event)).join("\n")}\n`;
  await archive.put(
    buildCatalogStagingWeatherR2Key(raceDate, keibajoCode, weatherType, fetchedAt),
    body,
    {
      httpMetadata: { contentType: NDJSON_CONTENT_TYPE },
    },
  );
};

export const upsertVenueWeather = async (params: UpsertParams): Promise<number> => {
  if (params.rows.length === 0) return 0;
  const payload = buildPayload(params);
  await putVenueWeatherRuntimeObjects(params);
  await putCatalogStagingEvents({
    archive: params.archive,
    catalogStream: params.catalogStream,
    fetchedAt: params.fetchedAt,
    keibajoCode: params.keibajoCode,
    payload,
    raceDate: params.raceDate,
    weatherType: params.weatherType,
  });
  return params.rows.length;
};

export const putVenueWeatherRuntimeObjects = async (
  params: Omit<UpsertParams, "catalogStream">,
): Promise<number> => {
  if (params.rows.length === 0) return 0;
  const payload = buildPayload(params);
  await putJson(
    params.archive,
    buildLiveWeatherR2Key(params.raceDate, params.keibajoCode, params.weatherType),
    payload,
  );
  await putJson(
    params.archive,
    buildSnapshotWeatherR2Key(
      params.raceDate,
      params.keibajoCode,
      params.weatherType,
      params.fetchedAt,
    ),
    payload,
  );
  return params.rows.length;
};

export const backfillVenueWeatherPayloads = async (
  archive: R2Bucket,
  catalogStream: UpsertParams["catalogStream"],
  payloads: BackfillWeatherPayload[],
): Promise<number> =>
  payloads.reduce(
    async (previous, payload) =>
      (await previous) +
      (await upsertVenueWeather({
        archive,
        catalogStream,
        fetchedAt: payload.fetchedAt,
        keibajoCode: payload.keibajoCode,
        raceDate: payload.raceDate,
        rows: payload.rows,
        venue: payload.venue,
        weatherType: payload.weatherType,
      })),
    Promise.resolve(0),
  );

export const backfillVenueWeatherRuntimeOnly = async (
  archive: R2Bucket,
  payloads: BackfillWeatherPayload[],
): Promise<number> =>
  payloads.reduce(
    async (previous, payload) =>
      (await previous) +
      (await putVenueWeatherRuntimeObjects({
        archive,
        fetchedAt: payload.fetchedAt,
        keibajoCode: payload.keibajoCode,
        raceDate: payload.raceDate,
        rows: payload.rows,
        venue: payload.venue,
        weatherType: payload.weatherType,
      })),
    Promise.resolve(0),
  );

export const backfillVenueWeatherCatalogOnly = async (
  archive: R2Bucket,
  catalogStream: UpsertParams["catalogStream"],
  payloads: BackfillWeatherPayload[],
  fetchedAt: string,
): Promise<number> => {
  const events = payloads.flatMap(payloadToCatalogEvents);
  if (events.length === 0) {
    return 0;
  }
  await catalogStream?.send(events).catch(() => undefined);
  const body = `${events.map((event) => JSON.stringify(event)).join("\n")}\n`;
  await archive.put(buildCatalogBatchStagingWeatherR2Key(fetchedAt), body, {
    httpMetadata: { contentType: NDJSON_CONTENT_TYPE },
  });
  return events.length;
};

const listLiveWeatherKeys = async (
  archive: R2Bucket,
  raceDate: string,
  cursor?: string,
): Promise<string[]> => {
  const result = await archive.list({ cursor, prefix: buildDatePrefix(raceDate) });
  const keys = result.objects.map((object) => object.key);
  if (!result.truncated) {
    return keys;
  }
  return [...keys, ...(await listLiveWeatherKeys(archive, raceDate, result.cursor))];
};

export const readWeatherByDate = async (
  archive: R2Bucket,
  raceDate: string,
): Promise<WeatherCacheRow[]> => {
  const keys = await listLiveWeatherKeys(archive, raceDate);
  const payloads = await Promise.all(
    keys.map(async (key) => {
      const object = await archive.get(key).catch(() => null);
      return object ? parseStoredPayload(object) : null;
    }),
  );
  return sortRows(
    payloads
      .filter((payload): payload is StoredWeatherPayload => payload?.raceDate === raceDate)
      .flatMap((payload) => payload.rows),
  );
};
