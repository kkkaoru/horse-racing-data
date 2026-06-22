// Run with bun.
export type WeatherType = "forecast" | "actual";

export interface WeatherJob {
  type: WeatherType;
  keibajoCode: string;
  raceDate: string;
}

export interface WeatherRow {
  date: string;
  hour: number;
  weatherCode: number | null;
  temperature: number | null;
  precipitation: number | null;
  windSpeed: number | null;
  windGusts: number | null;
}

export interface VenueCoord {
  name: string;
  lat: number;
  lon: number;
}

export interface UpsertParams {
  db: D1Database;
  keibajoCode: string;
  raceDate: string;
  weatherType: WeatherType;
  venue: VenueCoord;
  rows: WeatherRow[];
  fetchedAt: string;
}

export interface Env {
  WEATHER_DB: D1Database;
  WEATHER_JOBS: Queue<WeatherJob>;
}
