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
  relativeHumidity?: number | null;
  dewPoint?: number | null;
  wetBulbTemperature?: number | null;
  shortwaveRadiation?: number | null;
}

export interface VenueCoord {
  name: string;
  lat: number;
  lon: number;
}

export interface WeatherCacheRow {
  keibajo_code: string;
  race_date: string;
  weather_hour: number;
  temperature: number | null;
  precipitation: number | null;
  wind_speed: number | null;
  wind_gusts: number | null;
  weather_type: number | null;
}

export interface UpsertParams {
  archive: R2Bucket;
  catalogStream?: PipelineBinding;
  keibajoCode: string;
  raceDate: string;
  weatherType: WeatherType;
  venue: VenueCoord;
  rows: WeatherRow[];
  fetchedAt: string;
}

export interface PipelineBinding {
  send(records: unknown[]): Promise<void>;
}

export interface Env {
  WEATHER_ARCHIVE: R2Bucket;
  WEATHER_CATALOG_STREAM?: PipelineBinding;
  WEATHER_CATALOG_STREAM_V2?: PipelineBinding;
  WEATHER_JOBS: Queue<WeatherJob>;
  WEATHER_KV: KVNamespace;
  VENUE_WEATHER_INTERNAL_TOKEN?: string;
  VENUE_WEATHER_V2_BACKFILL_TOKEN?: string;
}
