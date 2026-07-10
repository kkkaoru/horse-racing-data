import { afterEach, beforeEach, expect, it, vi } from "vitest";
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

beforeEach(() => {
  vi.stubGlobal("caches", undefined);
});

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
        weather_hour: 10,
        weather_type: 3,
        wind_gusts: 8.1,
        wind_speed: 5.2,
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

it("handleWeatherFetch uses Cache API response when present", async () => {
  const match = vi.fn().mockResolvedValue(Response.json({ rows: [], source: "cache-api" }));
  const put = vi.fn().mockResolvedValue(undefined);
  vi.stubGlobal("caches", { default: { match, put } });

  const res = await handleWeatherFetch(
    new Request("https://x/weather?race_date=20260622"),
    buildEnv(),
  );

  expect(await res.json()).toStrictEqual({ rows: [], source: "cache-api" });
  expect(put).not.toHaveBeenCalled();
});

it("handleWeatherFetch stores non-empty R2 responses in Cache API", async () => {
  const match = vi.fn().mockResolvedValue(undefined);
  const cachePut = vi.fn().mockResolvedValue(undefined);
  vi.stubGlobal("caches", { default: { match, put: cachePut } });
  const env = buildEnv({
    WEATHER_ARCHIVE: {
      get: vi.fn().mockResolvedValue({
        json: vi.fn().mockResolvedValue({
          keibajoCode: "05",
          raceDate: "2026-06-22",
          rows: [
            {
              keibajo_code: "05",
              precipitation: null,
              race_date: "2026-06-22",
              temperature: 20,
              weather_hour: 10,
              weather_type: 1,
              wind_gusts: null,
              wind_speed: 3,
            },
          ],
          schemaVersion: 1,
        }),
      }),
      list: vi.fn().mockResolvedValue({ objects: [{ key: "k" }], truncated: false }),
    } as unknown as R2Bucket,
  });

  await handleWeatherFetch(new Request("https://x/weather?race_date=20260622"), env);

  expect(cachePut).toHaveBeenCalledTimes(1);
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
