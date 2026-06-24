import { expect, it, vi } from "vitest";
import { handleWeatherFetch } from "./weather-handler";
import type { Env } from "./types";

it("handleWeatherFetch responds ok for the ping path", async () => {
  const env = {
    WEATHER_KV: {},
    WEATHER_DB: {},
  } as unknown as Env;

  const res = await handleWeatherFetch(new Request("https://x/ping"), env);

  expect(res.status).toBe(200);
  expect(await res.text()).toBe("ok");
});

it("handleWeatherFetch returns 400 when race_date is missing", async () => {
  const env = {
    WEATHER_KV: {},
    WEATHER_DB: {},
  } as unknown as Env;

  const res = await handleWeatherFetch(new Request("https://x/weather"), env);

  expect(res.status).toBe(400);
});

it("handleWeatherFetch returns 400 when race_date is not eight digits", async () => {
  const env = {
    WEATHER_KV: {},
    WEATHER_DB: {},
  } as unknown as Env;

  const res = await handleWeatherFetch(new Request("https://x/weather?race_date=2026"), env);

  expect(res.status).toBe(400);
});

it("handleWeatherFetch returns cached rows from KV", async () => {
  const env = {
    WEATHER_KV: {
      get: vi.fn().mockResolvedValue(
        JSON.stringify([
          {
            keibajo_code: "05",
            race_date: "2026-06-22",
            weather_hour: 10,
            temperature: 20.5,
            precipitation: 0,
            wind_speed: 5.2,
            wind_gusts: 8.1,
            weather_type: 3,
          },
        ]),
      ),
    },
    WEATHER_DB: {},
  } as unknown as Env;

  const res = await handleWeatherFetch(new Request("https://x/weather?race_date=20260622"), env);

  expect(res.status).toBe(200);
  expect(await res.json()).toStrictEqual({
    rows: [
      {
        keibajo_code: "05",
        race_date: "2026-06-22",
        weather_hour: 10,
        temperature: 20.5,
        precipitation: 0,
        wind_speed: 5.2,
        wind_gusts: 8.1,
        weather_type: 3,
      },
    ],
    source: "kv",
  });
});

it("handleWeatherFetch reads from D1 and populates KV on cache miss", async () => {
  const put = vi.fn().mockResolvedValue(undefined);
  const all = vi.fn().mockResolvedValue({
    results: [
      {
        keibajo_code: "05",
        race_date: "2026-06-22",
        weather_hour: 10,
        temperature: 20.5,
        precipitation: 0,
        wind_speed: 5.2,
        wind_gusts: 8.1,
        weather_code: 3,
      },
    ],
  });
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const env = {
    WEATHER_KV: {
      get: vi.fn().mockResolvedValue(null),
      put,
    },
    WEATHER_DB: { prepare },
  } as unknown as Env;

  const res = await handleWeatherFetch(new Request("https://x/weather?race_date=20260622"), env);

  expect(await res.json()).toStrictEqual({
    rows: [
      {
        keibajo_code: "05",
        race_date: "2026-06-22",
        weather_hour: 10,
        temperature: 20.5,
        precipitation: 0,
        wind_speed: 5.2,
        wind_gusts: 8.1,
        weather_type: 3,
      },
    ],
    source: "d1",
  });
  expect(bind).toHaveBeenCalledWith("2026-06-22");
  expect(put).toHaveBeenCalledTimes(1);
  expect(put).toHaveBeenCalledWith("weather:2026-06-22", expect.any(String), {
    expirationTtl: 3600,
  });
});

it("handleWeatherFetch returns empty rows without populating KV when D1 is empty", async () => {
  const put = vi.fn().mockResolvedValue(undefined);
  const all = vi.fn().mockResolvedValue({ results: [] });
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const env = {
    WEATHER_KV: {
      get: vi.fn().mockResolvedValue(null),
      put,
    },
    WEATHER_DB: { prepare },
  } as unknown as Env;

  const res = await handleWeatherFetch(new Request("https://x/weather?race_date=20260622"), env);

  expect(await res.json()).toStrictEqual({ rows: [], source: "d1" });
  expect(put).not.toHaveBeenCalled();
});

it("handleWeatherFetch returns the default body for an unknown path", async () => {
  const env = {
    WEATHER_KV: {},
    WEATHER_DB: {},
  } as unknown as Env;

  const res = await handleWeatherFetch(new Request("https://x/foo"), env);

  expect(await res.text()).toBe("venue-weather");
});
