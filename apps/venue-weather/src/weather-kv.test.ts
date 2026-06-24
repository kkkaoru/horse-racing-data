import { expect, it, vi } from "vitest";
import {
  buildWeatherKey,
  deleteWeatherFromKv,
  getWeatherFromKv,
  putWeatherToKv,
} from "./weather-kv";

it("buildWeatherKey returns the prefixed key", () => {
  expect(buildWeatherKey("20260622")).toBe("weather:20260622");
});

it("getWeatherFromKv returns parsed rows on hit", async () => {
  const get = vi.fn().mockResolvedValue(
    JSON.stringify([
      {
        keibajo_code: "05",
        race_date: "20260622",
        weather_hour: 10,
        temperature: 20.5,
        precipitation: 0,
        wind_speed: 5.2,
        wind_gusts: 8.1,
        weather_type: 3,
      },
    ]),
  );
  const kv = { get } as unknown as KVNamespace;

  const result = await getWeatherFromKv(kv, "20260622");

  expect(result).toStrictEqual([
    {
      keibajo_code: "05",
      race_date: "20260622",
      weather_hour: 10,
      temperature: 20.5,
      precipitation: 0,
      wind_speed: 5.2,
      wind_gusts: 8.1,
      weather_type: 3,
    },
  ]);
  expect(get).toHaveBeenCalledWith("weather:20260622");
});

it("getWeatherFromKv returns null on miss", async () => {
  const kv = {
    get: vi.fn().mockResolvedValue(null),
  } as unknown as KVNamespace;

  const result = await getWeatherFromKv(kv, "20260622");

  expect(result).toBe(null);
});

it("putWeatherToKv writes the serialized rows with ttl", async () => {
  const put = vi.fn().mockResolvedValue(undefined);
  const kv = { put } as unknown as KVNamespace;

  await putWeatherToKv({
    kv,
    raceDate: "20260622",
    rows: [
      {
        keibajo_code: "05",
        race_date: "20260622",
        weather_hour: 10,
        temperature: 20.5,
        precipitation: 0,
        wind_speed: 5.2,
        wind_gusts: 8.1,
        weather_type: 3,
      },
    ],
    ttlSeconds: 3600,
  });

  expect(put).toHaveBeenCalledWith(
    "weather:20260622",
    '[{"keibajo_code":"05","race_date":"20260622","weather_hour":10,"temperature":20.5,"precipitation":0,"wind_speed":5.2,"wind_gusts":8.1,"weather_type":3}]',
    { expirationTtl: 3600 },
  );
});

it("deleteWeatherFromKv deletes the key", async () => {
  const deleteFn = vi.fn().mockResolvedValue(undefined);
  const kv = { delete: deleteFn } as unknown as KVNamespace;

  await deleteWeatherFromKv(kv, "20260622");

  expect(deleteFn).toHaveBeenCalledWith("weather:20260622");
});
