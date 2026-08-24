import { expect, it, vi } from "vitest";
import {
  backfillVenueWeatherPayloads,
  backfillVenueWeatherCatalogOnly,
  backfillVenueWeatherRuntimeOnly,
  buildCatalogStagingWeatherR2Key,
  buildLiveWeatherR2Key,
  buildSnapshotWeatherR2Key,
  preferActualLiveWeatherKeys,
  putVenueWeatherRuntimeObjects,
  readWeatherByDate,
  upsertVenueWeather,
} from "./weather-r2-store";

const TOKYO_VENUE = { lat: 35.6622, lon: 139.4856, name: "東京" };
const WEATHER_ROW = {
  date: "2026-06-22",
  hour: 10,
  precipitation: 0,
  temperature: 20.5,
  weatherCode: 3,
  windGusts: 8.1,
  windSpeed: 5.2,
};

it("builds deterministic R2 keys", () => {
  expect(buildLiveWeatherR2Key("2026-06-22", "05", "forecast")).toBe(
    "venue-weather-live/v1/race_date=2026-06-22/keibajo_code=05/forecast.json",
  );
  expect(buildSnapshotWeatherR2Key("2026-06-22", "05", "actual", "2026-06-22T10:00:00Z")).toBe(
    "venue-weather-snapshots/v1/race_date=2026-06-22/keibajo_code=05/actual/2026-06-22T10:00:00Z.json",
  );
  expect(
    buildCatalogStagingWeatherR2Key("2026-06-22", "05", "actual", "2026-06-22T10:00:00Z"),
  ).toBe(
    "venue-weather-catalog-staging/v1/race_date=2026-06-22/keibajo_code=05/actual/2026-06-22T10:00:00Z.ndjson",
  );
});

it("upsertVenueWeather returns 0 and skips R2 writes for empty rows", async () => {
  const put = vi.fn();
  const archive = { put } as unknown as R2Bucket;

  const result = await upsertVenueWeather({
    archive,
    fetchedAt: "2026-06-22T10:00:00.000Z",
    keibajoCode: "05",
    raceDate: "2026-06-22",
    rows: [],
    venue: TOKYO_VENUE,
    weatherType: "forecast",
  });

  expect(result).toBe(0);
  expect(put).not.toHaveBeenCalled();
});

it("upsertVenueWeather writes live, snapshot, catalog staging, and pipeline records", async () => {
  const put = vi.fn().mockResolvedValue(undefined);
  const send = vi.fn().mockResolvedValue(undefined);
  const archive = { put } as unknown as R2Bucket;

  const result = await upsertVenueWeather({
    archive,
    catalogStream: { send },
    fetchedAt: "2026-06-22T10:00:00.000Z",
    keibajoCode: "05",
    raceDate: "2026-06-22",
    rows: [WEATHER_ROW],
    venue: TOKYO_VENUE,
    weatherType: "actual",
  });

  expect(result).toBe(1);
  expect(put).toHaveBeenCalledTimes(3);
  expect(send).toHaveBeenCalledWith([
    {
      fetched_at: "2026-06-22T10:00:00.000Z",
      keibajo_code: "05",
      latitude: 35.6622,
      longitude: 139.4856,
      precipitation: 0,
      race_date: "2026-06-22",
      temperature: 20.5,
      venue_name: "東京",
      weather_code: 3,
      weather_data_type: "actual",
      weather_hour: 10,
      wind_gusts: 8.1,
      wind_speed: 5.2,
    },
  ]);
});

it("upsertVenueWeather still writes catalog staging when pipeline send fails", async () => {
  const put = vi.fn().mockResolvedValue(undefined);
  const send = vi.fn().mockRejectedValue(new Error("stream down"));

  await upsertVenueWeather({
    archive: { put } as unknown as R2Bucket,
    catalogStream: { send },
    fetchedAt: "2026-06-22T10:00:00.000Z",
    keibajoCode: "05",
    raceDate: "2026-06-22",
    rows: [WEATHER_ROW],
    venue: TOKYO_VENUE,
    weatherType: "forecast",
  });

  expect(put).toHaveBeenCalledTimes(3);
});

it("putVenueWeatherRuntimeObjects writes live and snapshot without catalog staging", async () => {
  const put = vi.fn().mockResolvedValue(undefined);

  const result = await putVenueWeatherRuntimeObjects({
    archive: { put } as unknown as R2Bucket,
    fetchedAt: "2026-06-22T10:00:00.000Z",
    keibajoCode: "05",
    raceDate: "2026-06-22",
    rows: [WEATHER_ROW],
    venue: TOKYO_VENUE,
    weatherType: "actual",
  });

  expect(result).toBe(1);
  expect(put).toHaveBeenCalledTimes(2);
  expect(put).toHaveBeenCalledWith(
    "venue-weather-live/v1/race_date=2026-06-22/keibajo_code=05/actual.json",
    expect.any(String),
    expect.any(Object),
  );
  expect(put).toHaveBeenCalledWith(
    "venue-weather-snapshots/v1/race_date=2026-06-22/keibajo_code=05/actual/2026-06-22T10:00:00_000Z.json",
    expect.any(String),
    expect.any(Object),
  );
});

it("backfillVenueWeatherPayloads sums rows from all payloads", async () => {
  const put = vi.fn().mockResolvedValue(undefined);
  const archive = { put } as unknown as R2Bucket;

  const result = await backfillVenueWeatherPayloads(archive, undefined, [
    {
      fetchedAt: "2026-06-22T10:00:00.000Z",
      keibajoCode: "05",
      raceDate: "2026-06-22",
      rows: [WEATHER_ROW],
      venue: TOKYO_VENUE,
      weatherType: "actual",
    },
    {
      fetchedAt: "2026-06-22T10:00:00.000Z",
      keibajoCode: "06",
      raceDate: "2026-06-22",
      rows: [
        { ...WEATHER_ROW, hour: 11 },
        { ...WEATHER_ROW, hour: 12 },
      ],
      venue: { lat: 35.725, lon: 139.485, name: "中山" },
      weatherType: "actual",
    },
  ]);

  expect(result).toBe(3);
});

it("backfillVenueWeatherRuntimeOnly sums rows without a catalog stream", async () => {
  const put = vi.fn().mockResolvedValue(undefined);

  const result = await backfillVenueWeatherRuntimeOnly({ put } as unknown as R2Bucket, [
    {
      fetchedAt: "2026-06-22T10:00:00.000Z",
      keibajoCode: "05",
      raceDate: "2026-06-22",
      rows: [WEATHER_ROW, { ...WEATHER_ROW, hour: 11 }],
      venue: TOKYO_VENUE,
      weatherType: "actual",
    },
  ]);

  expect(result).toBe(2);
  expect(put).toHaveBeenCalledTimes(2);
});

it("backfillVenueWeatherCatalogOnly writes one staging object and sends all rows", async () => {
  const put = vi.fn().mockResolvedValue(undefined);
  const send = vi.fn().mockResolvedValue(undefined);

  const result = await backfillVenueWeatherCatalogOnly(
    { put } as unknown as R2Bucket,
    { send },
    [
      {
        fetchedAt: "2026-06-22T10:00:00.000Z",
        keibajoCode: "05",
        raceDate: "2026-06-22",
        rows: [WEATHER_ROW, { ...WEATHER_ROW, hour: 11 }],
        venue: TOKYO_VENUE,
        weatherType: "actual",
      },
    ],
    "2026-06-22T10:00:00.000Z",
  );

  expect(result).toBe(2);
  expect(send).toHaveBeenCalledWith([
    expect.objectContaining({ keibajo_code: "05", weather_hour: 10 }),
    expect.objectContaining({ keibajo_code: "05", weather_hour: 11 }),
  ]);
  expect(put).toHaveBeenCalledTimes(1);
});

it("backfillVenueWeatherCatalogOnly skips empty batches", async () => {
  const put = vi.fn().mockResolvedValue(undefined);

  const result = await backfillVenueWeatherCatalogOnly(
    { put } as unknown as R2Bucket,
    undefined,
    [],
    "2026-06-22T10:00:00.000Z",
  );

  expect(result).toBe(0);
  expect(put).not.toHaveBeenCalled();
});

it("readWeatherByDate reads paginated R2 objects and sorts rows", async () => {
  const list = vi
    .fn()
    .mockResolvedValueOnce({
      cursor: "next",
      objects: [{ key: "b" }],
      truncated: true,
    })
    .mockResolvedValueOnce({
      objects: [{ key: "a" }],
      truncated: false,
    });
  const get = vi.fn().mockImplementation(async (key: string) => ({
    json: vi.fn().mockResolvedValue({
      keibajoCode: key,
      raceDate: "2026-06-22",
      rows: [
        {
          keibajo_code: key === "a" ? "01" : "05",
          precipitation: null,
          race_date: "2026-06-22",
          temperature: 20,
          weather_hour: key === "a" ? 11 : 10,
          weather_type: 1,
          wind_gusts: null,
          wind_speed: 3,
        },
      ],
      schemaVersion: 1,
    }),
  }));

  const result = await readWeatherByDate({ get, list } as unknown as R2Bucket, "2026-06-22");

  expect(result.map((row) => row.keibajo_code)).toStrictEqual(["01", "05"]);
  expect(list).toHaveBeenLastCalledWith({
    cursor: "next",
    prefix: "venue-weather-live/v1/race_date=2026-06-22/",
  });
});

it("readWeatherByDate skips invalid, missing, and mismatched payloads", async () => {
  const list = vi.fn().mockResolvedValue({
    objects: [{ key: "invalid" }, { key: "missing" }, { key: "other-date" }, { key: "valid" }],
    truncated: false,
  });
  const get = vi.fn().mockImplementation(async (key: string) => {
    if (key === "missing") return null;
    if (key === "invalid") {
      return { json: vi.fn().mockResolvedValue({ schemaVersion: 0 }) };
    }
    return {
      json: vi.fn().mockResolvedValue({
        keibajoCode: "05",
        raceDate: key === "other-date" ? "2026-06-21" : "2026-06-22",
        rows: [
          {
            keibajo_code: "05",
            precipitation: 0,
            race_date: "2026-06-22",
            temperature: 20,
            weather_hour: 10,
            weather_type: 1,
            wind_gusts: 5,
            wind_speed: 3,
          },
        ],
        schemaVersion: 1,
      }),
    };
  });

  const result = await readWeatherByDate({ get, list } as unknown as R2Bucket, "2026-06-22");

  expect(result).toHaveLength(1);
});

it("prefers actual per venue while retaining forecast for venues without actual", () => {
  const keys = preferActualLiveWeatherKeys([
    "venue-weather-live/v1/race_date=2026-06-22/keibajo_code=01/forecast.json",
    "venue-weather-live/v1/race_date=2026-06-22/keibajo_code=01/actual.json",
    "venue-weather-live/v1/race_date=2026-06-22/keibajo_code=05/forecast.json",
  ]);

  expect(keys).toStrictEqual([
    "venue-weather-live/v1/race_date=2026-06-22/keibajo_code=01/actual.json",
    "venue-weather-live/v1/race_date=2026-06-22/keibajo_code=05/forecast.json",
  ]);
});

it("readWeatherByDate does not duplicate forecast after actual arrives", async () => {
  const forecastKey = "venue-weather-live/v1/race_date=2026-06-22/keibajo_code=05/forecast.json";
  const actualKey = "venue-weather-live/v1/race_date=2026-06-22/keibajo_code=05/actual.json";
  const list = vi.fn().mockResolvedValue({
    objects: [{ key: forecastKey }, { key: actualKey }],
    truncated: false,
  });
  const get = vi.fn().mockResolvedValue({
    json: vi.fn().mockResolvedValue({
      keibajoCode: "05",
      raceDate: "2026-06-22",
      rows: [{ ...WEATHER_ROW, keibajo_code: "05", race_date: "2026-06-22" }],
      schemaVersion: 1,
      weatherDataType: "actual",
    }),
  });

  const result = await readWeatherByDate({ get, list } as unknown as R2Bucket, "2026-06-22");

  expect(result).toHaveLength(1);
  expect(get).toHaveBeenCalledTimes(1);
  expect(get).toHaveBeenCalledWith(actualKey);
});
