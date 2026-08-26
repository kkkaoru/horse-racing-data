import { afterEach, expect, it, vi } from "vitest";
import { handleWeatherFetch } from "./weather-handler";
import type { Env } from "./types";

const buildEnv = (overrides: Partial<Env> = {}): Env =>
  ({
    WEATHER_ARCHIVE: {
      get: vi.fn().mockResolvedValue(null),
      list: vi.fn().mockResolvedValue({ objects: [], truncated: false }),
    },
    WEATHER_JOBS: {},
    WEATHER_KV: {
      delete: vi.fn().mockResolvedValue(undefined),
      get: vi.fn().mockResolvedValue(null),
      put: vi.fn().mockResolvedValue(undefined),
    },
    ...overrides,
  }) as unknown as Env;

afterEach(() => {
  vi.unstubAllGlobals();
});

it("handleWeatherFetch responds ok for the ping path", async () => {
  const res = await handleWeatherFetch(new Request("https://x/ping"), buildEnv());

  expect(res.status).toBe(200);
  expect(await res.text()).toBe("ok");
});

it("handleWeatherFetch returns 400 when race_date is missing", async () => {
  const res = await handleWeatherFetch(new Request("https://x/weather"), buildEnv());

  expect(res.status).toBe(400);
});

it("handleWeatherFetch returns 400 when race_date is not eight digits", async () => {
  const res = await handleWeatherFetch(new Request("https://x/weather?race_date=2026"), buildEnv());

  expect(res.status).toBe(400);
});

it("handleWeatherFetch returns cached rows from KV", async () => {
  const env = buildEnv({
    WEATHER_KV: {
      get: vi.fn().mockResolvedValue(
        JSON.stringify([
          {
            keibajo_code: "05",
            precipitation: 0,
            race_date: "2026-06-22",
            temperature: 20.5,
            weather_hour: 10,
            weather_type: 3,
            wind_gusts: 8.1,
            wind_speed: 5.2,
          },
        ]),
      ),
    } as unknown as KVNamespace,
  });

  const res = await handleWeatherFetch(new Request("https://x/weather?race_date=20260622"), env);

  expect(res.status).toBe(200);
  expect(await res.json()).toStrictEqual({
    rows: [
      {
        keibajo_code: "05",
        precipitation: 0,
        race_date: "2026-06-22",
        temperature: 20.5,
        weather_code: 3,
        weather_hour: 10,
        weather_type: 3,
        wind_gusts: 8.1,
        wind_speed: 5.2,
      },
    ],
    source: "kv",
  });
});

it("handleWeatherFetch merges a complete v2 row set into cached v1 rows", async () => {
  const env = buildEnv({
    WEATHER_ARCHIVE: {
      get: vi.fn().mockResolvedValue({
        json: vi.fn().mockResolvedValue({
          fetchedAt: "2026-08-25T01:30:00.000Z",
          keibajoCode: "05",
          raceDate: "2026-08-25",
          rows: [
            {
              dew_point: 22,
              keibajo_code: "05",
              precipitation: 0,
              race_date: "2026-08-25",
              relative_humidity: 80,
              shortwave_radiation: 100,
              temperature: 25,
              weather_code: 1,
              weather_hour: 10,
              wet_bulb_temperature: 23,
              wind_gusts: 8,
              wind_speed: 4,
            },
          ],
          schemaVersion: 2,
          weatherDataType: "forecast",
        }),
      }),
      list: vi.fn().mockResolvedValue({
        objects: [
          {
            key: "venue-weather-live/v2/race_date=2026-08-25/keibajo_code=05/forecast.json",
          },
        ],
        truncated: false,
      }),
    } as unknown as R2Bucket,
    WEATHER_KV: {
      get: vi.fn().mockResolvedValue(
        JSON.stringify([
          {
            keibajo_code: "05",
            precipitation: 0,
            race_date: "2026-08-25",
            temperature: 25,
            weather_hour: 10,
            weather_type: 1,
            wind_gusts: 8,
            wind_speed: 4,
          },
        ]),
      ),
    } as unknown as KVNamespace,
  });

  const res = await handleWeatherFetch(new Request("https://x/weather?race_date=20260825"), env);

  expect(await res.json()).toStrictEqual({
    rows: [
      {
        dew_point: 22,
        keibajo_code: "05",
        precipitation: 0,
        race_date: "2026-08-25",
        relative_humidity: 80,
        shortwave_radiation: 100,
        temperature: 25,
        weather_code: 1,
        weather_hour: 10,
        weather_type: 1,
        wet_bulb_temperature: 23,
        wind_gusts: 8,
        wind_speed: 4,
      },
    ],
    source: "kv",
  });
});

it("handleWeatherFetch reads from R2 and populates KV on cache miss", async () => {
  const put = vi.fn().mockResolvedValue(undefined);
  const archive = {
    get: vi.fn().mockResolvedValue({
      json: vi.fn().mockResolvedValue({
        fetchedAt: "2026-06-22T10:00:00.000Z",
        keibajoCode: "05",
        raceDate: "2026-06-22",
        rows: [
          {
            keibajo_code: "05",
            precipitation: 0,
            race_date: "2026-06-22",
            temperature: 20.5,
            weather_hour: 10,
            weather_type: 3,
            wind_gusts: 8.1,
            wind_speed: 5.2,
          },
        ],
        schemaVersion: 1,
        venue: { lat: 35.6622, lon: 139.4856, name: "東京" },
        weatherDataType: "forecast",
      }),
    }),
    list: vi.fn().mockResolvedValue({
      objects: [
        { key: "venue-weather-live/v1/race_date=2026-06-22/keibajo_code=05/forecast.json" },
      ],
      truncated: false,
    }),
  } as unknown as R2Bucket;
  const env = buildEnv({
    WEATHER_ARCHIVE: archive,
    WEATHER_KV: {
      get: vi.fn().mockResolvedValue(null),
      put,
    } as unknown as KVNamespace,
  });

  const res = await handleWeatherFetch(new Request("https://x/weather?race_date=20260622"), env);

  expect(await res.json()).toStrictEqual({
    rows: [
      {
        keibajo_code: "05",
        precipitation: 0,
        race_date: "2026-06-22",
        temperature: 20.5,
        weather_code: 3,
        weather_hour: 10,
        weather_type: 3,
        wind_gusts: 8.1,
        wind_speed: 5.2,
      },
    ],
    source: "r2",
  });
  expect(put).toHaveBeenCalledWith("weather:2026-06-22", expect.any(String), {
    expirationTtl: 3600,
  });
});

it("handleWeatherFetch returns empty rows without populating KV when R2 is empty", async () => {
  const put = vi.fn().mockResolvedValue(undefined);
  const env = buildEnv({
    WEATHER_KV: {
      get: vi.fn().mockResolvedValue(null),
      put,
    } as unknown as KVNamespace,
  });

  const res = await handleWeatherFetch(new Request("https://x/weather?race_date=20260622"), env);

  expect(await res.json()).toStrictEqual({ rows: [], source: "r2" });
  expect(put).not.toHaveBeenCalled();
});

it("handleWeatherFetch ignores stale Cache API responses and reads the invalidatable KV", async () => {
  const match = vi.fn().mockResolvedValue(Response.json({ rows: [], source: "cache-api" }));
  vi.stubGlobal("caches", { default: { match, put: vi.fn().mockResolvedValue(undefined) } });
  const env = buildEnv();
  Object.defineProperty(env.WEATHER_KV, "get", {
    value: vi.fn().mockResolvedValue(
      JSON.stringify([
        {
          keibajo_code: "05",
          precipitation: 0,
          race_date: "2026-06-22",
          temperature: 20.5,
          weather_hour: 10,
          weather_type: 3,
          wind_gusts: 8.1,
          wind_speed: 5.2,
        },
      ]),
    ),
  });

  const res = await handleWeatherFetch(new Request("https://x/weather?race_date=20260622"), env);

  expect(await res.json()).toStrictEqual({
    rows: [
      {
        keibajo_code: "05",
        precipitation: 0,
        race_date: "2026-06-22",
        temperature: 20.5,
        weather_code: 3,
        weather_hour: 10,
        weather_type: 3,
        wind_gusts: 8.1,
        wind_speed: 5.2,
      },
    ],
    source: "kv",
  });
  expect(match).not.toHaveBeenCalled();
});

it("handleWeatherFetch rejects v2 backfill without its isolated token", async () => {
  const res = await handleWeatherFetch(
    new Request("https://x/api/internal/backfill-r2-catalog-v2", {
      body: "[]",
      method: "POST",
    }),
    buildEnv(),
  );

  expect(res.status).toBe(401);
});

it("handleWeatherFetch rejects empty v2 backfill batches", async () => {
  const res = await handleWeatherFetch(
    new Request("https://x/api/internal/backfill-r2-catalog-v2", {
      body: "[]",
      headers: { "x-venue-weather-v2-backfill-token": "v2-secret" },
      method: "POST",
    }),
    buildEnv({ VENUE_WEATHER_V2_BACKFILL_TOKEN: "v2-secret" }),
  );

  expect(res.status).toBe(400);
});

it("handleWeatherFetch fails closed when the v2 stream binding is absent", async () => {
  const res = await handleWeatherFetch(
    new Request("https://x/api/internal/backfill-r2-catalog-v2", {
      body: JSON.stringify([{ race_date: "2025-01-01" }]),
      headers: { "x-venue-weather-v2-backfill-token": "v2-secret" },
      method: "POST",
    }),
    buildEnv({ VENUE_WEATHER_V2_BACKFILL_TOKEN: "v2-secret" }),
  );

  expect(res.status).toBe(503);
});

it("handleWeatherFetch sends authorized v2 backfill events", async () => {
  const send = vi.fn().mockResolvedValue(undefined);
  const res = await handleWeatherFetch(
    new Request("https://x/api/internal/backfill-r2-catalog-v2", {
      body: JSON.stringify([{ race_date: "2025-01-01" }]),
      headers: { "x-venue-weather-v2-backfill-token": "v2-secret" },
      method: "POST",
    }),
    buildEnv({
      VENUE_WEATHER_V2_BACKFILL_TOKEN: "v2-secret",
      WEATHER_CATALOG_STREAM_V2: { send },
    }),
  );

  expect(await res.json()).toStrictEqual({ rows: 1 });
  expect(send).toHaveBeenCalledWith([{ race_date: "2025-01-01" }]);
});

it("handleWeatherFetch rejects backfill without the internal token", async () => {
  const res = await handleWeatherFetch(
    new Request("https://x/api/internal/backfill-r2-catalog", { method: "POST" }),
    buildEnv(),
  );

  expect(res.status).toBe(401);
});

it("handleWeatherFetch accepts authorized backfill payloads", async () => {
  const archive = {
    put: vi.fn().mockResolvedValue(undefined),
  } as unknown as R2Bucket;
  const env = buildEnv({
    VENUE_WEATHER_INTERNAL_TOKEN: "secret",
    WEATHER_ARCHIVE: archive,
    WEATHER_CATALOG_STREAM: { send: vi.fn().mockResolvedValue(undefined) },
  });
  const request = new Request("https://x/api/internal/backfill-r2-catalog", {
    body: JSON.stringify({
      payloads: [
        {
          fetchedAt: "2026-06-22T10:00:00.000Z",
          keibajoCode: "05",
          raceDate: "2026-06-22",
          rows: [
            {
              date: "2026-06-22",
              hour: 10,
              precipitation: 0,
              temperature: 20,
              weatherCode: 1,
              windGusts: 5,
              windSpeed: 3,
            },
          ],
          venue: { lat: 35.6622, lon: 139.4856, name: "東京" },
          weatherType: "actual",
        },
      ],
    }),
    headers: { "x-venue-weather-internal-token": "secret" },
    method: "POST",
  });

  const res = await handleWeatherFetch(request, env);

  expect(await res.json()).toStrictEqual({ payloads: 1, rows: 1 });
});

it("handleWeatherFetch supports catalog-only backfill payloads", async () => {
  const put = vi.fn().mockResolvedValue(undefined);
  const send = vi.fn().mockResolvedValue(undefined);
  const env = buildEnv({
    VENUE_WEATHER_INTERNAL_TOKEN: "secret",
    WEATHER_ARCHIVE: { put } as unknown as R2Bucket,
    WEATHER_CATALOG_STREAM: { send },
  });
  const request = new Request("https://x/api/internal/backfill-r2-catalog", {
    body: JSON.stringify({
      catalogOnly: true,
      fetchedAt: "2026-06-22T10:00:00.000Z",
      payloads: [
        {
          fetchedAt: "2026-06-22T10:00:00.000Z",
          keibajoCode: "05",
          raceDate: "2026-06-22",
          rows: [
            {
              date: "2026-06-22",
              hour: 10,
              precipitation: 0,
              temperature: 20,
              weatherCode: 1,
              windGusts: 5,
              windSpeed: 3,
            },
          ],
          venue: { lat: 35.6622, lon: 139.4856, name: "東京" },
          weatherType: "actual",
        },
      ],
    }),
    headers: { "x-venue-weather-internal-token": "secret" },
    method: "POST",
  });

  const res = await handleWeatherFetch(request, env);

  expect(await res.json()).toStrictEqual({ payloads: 1, rows: 1 });
  expect(send).toHaveBeenCalledTimes(1);
  expect(put).toHaveBeenCalledTimes(1);
});

it("handleWeatherFetch supports runtime-only backfill payloads", async () => {
  const put = vi.fn().mockResolvedValue(undefined);
  const send = vi.fn().mockResolvedValue(undefined);
  const env = buildEnv({
    VENUE_WEATHER_INTERNAL_TOKEN: "secret",
    WEATHER_ARCHIVE: { put } as unknown as R2Bucket,
    WEATHER_CATALOG_STREAM: { send },
  });
  const request = new Request("https://x/api/internal/backfill-r2-catalog", {
    body: JSON.stringify({
      payloads: [
        {
          fetchedAt: "2026-06-22T10:00:00.000Z",
          keibajoCode: "05",
          raceDate: "2026-06-22",
          rows: [
            {
              date: "2026-06-22",
              hour: 10,
              precipitation: 0,
              temperature: 20,
              weatherCode: 1,
              windGusts: 5,
              windSpeed: 3,
            },
          ],
          venue: { lat: 35.6622, lon: 139.4856, name: "東京" },
          weatherType: "actual",
        },
      ],
      runtimeOnly: true,
    }),
    headers: { "x-venue-weather-internal-token": "secret" },
    method: "POST",
  });

  const res = await handleWeatherFetch(request, env);

  expect(await res.json()).toStrictEqual({ payloads: 1, rows: 1 });
  expect(send).not.toHaveBeenCalled();
  expect(put).toHaveBeenCalledTimes(2);
});

it("handleWeatherFetch rejects conflicting backfill modes", async () => {
  const request = new Request("https://x/api/internal/backfill-r2-catalog", {
    body: JSON.stringify({
      catalogOnly: true,
      payloads: [],
      runtimeOnly: true,
    }),
    headers: { "x-venue-weather-internal-token": "secret" },
    method: "POST",
  });

  const res = await handleWeatherFetch(
    request,
    buildEnv({ VENUE_WEATHER_INTERNAL_TOKEN: "secret" }),
  );

  expect(res.status).toBe(400);
  expect(await res.text()).toBe("invalid backfill mode");
});

it("handleWeatherFetch returns the default body for an unknown path", async () => {
  const res = await handleWeatherFetch(new Request("https://x/foo"), buildEnv());

  expect(await res.text()).toBe("venue-weather");
});
