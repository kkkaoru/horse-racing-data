import { expect, it, vi } from "vitest";
import { readWeatherByDate } from "./weather-d1-reader";

it("readWeatherByDate maps weather_code to weather_type", async () => {
  const all = vi.fn().mockResolvedValue({
    results: [
      {
        keibajo_code: "05",
        race_date: "20260622",
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
  const db = { prepare } as unknown as D1Database;

  const result = await readWeatherByDate(db, "20260622");

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
  expect(bind).toHaveBeenCalledWith("20260622");
});

it("readWeatherByDate returns an empty array when no results", async () => {
  const all = vi.fn().mockResolvedValue({ results: [] });
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;

  const result = await readWeatherByDate(db, "20260622");

  expect(result).toStrictEqual([]);
});
