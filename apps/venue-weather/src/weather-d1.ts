// Run with bun.
import type { UpsertParams } from "./types";

const UPSERT_SQL = `
INSERT OR REPLACE INTO venue_weather (
  keibajo_code, race_date, weather_hour, weather_type,
  venue_name, latitude, longitude,
  weather_code, temperature, precipitation, wind_speed, wind_gusts,
  fetched_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`;

export const upsertVenueWeather = async ({
  db,
  keibajoCode,
  raceDate,
  weatherType,
  venue,
  rows,
  fetchedAt,
}: UpsertParams): Promise<number> => {
  if (rows.length === 0) return 0;
  const statements = rows.map((row) =>
    db
      .prepare(UPSERT_SQL)
      .bind(
        keibajoCode,
        raceDate,
        row.hour,
        weatherType,
        venue.name,
        venue.lat,
        venue.lon,
        row.weatherCode,
        row.temperature,
        row.precipitation,
        row.windSpeed,
        row.windGusts,
        fetchedAt,
      ),
  );
  await db.batch(statements);
  return rows.length;
};
