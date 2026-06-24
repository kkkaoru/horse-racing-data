// Run with bun.
import type { WeatherCacheRow } from "./types";

interface RawWeatherRow {
  keibajo_code: string;
  race_date: string;
  weather_hour: number;
  temperature: number | null;
  precipitation: number | null;
  wind_speed: number | null;
  wind_gusts: number | null;
  weather_code: number | null;
}

const READ_SQL =
  "SELECT keibajo_code, race_date, weather_hour, temperature, precipitation, wind_speed, wind_gusts, weather_code FROM venue_weather WHERE race_date = ? ORDER BY keibajo_code, weather_hour";

const mapRow = (row: RawWeatherRow): WeatherCacheRow => ({
  keibajo_code: row.keibajo_code,
  race_date: row.race_date,
  weather_hour: row.weather_hour,
  temperature: row.temperature,
  precipitation: row.precipitation,
  wind_speed: row.wind_speed,
  wind_gusts: row.wind_gusts,
  weather_type: row.weather_code,
});

export const readWeatherByDate = async (
  db: D1Database,
  raceDate: string,
): Promise<WeatherCacheRow[]> => {
  const result = await db.prepare(READ_SQL).bind(raceDate).all<RawWeatherRow>();
  return result.results.map(mapRow);
};
