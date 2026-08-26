// Run with bun.
import { fetchVenueWeather } from "./weather-api";
import { upsertVenueWeather } from "./weather-r2-store";
import {
  putVenueWeatherV2RuntimeObjects,
  sendVenueWeatherV2CatalogEvents,
} from "./weather-v2-store";
import { deleteWeatherFromKv } from "./weather-kv";
import { VENUE_COORDS } from "./venue-coords";
import type { Env, WeatherJob } from "./types";

export const processWeatherJob = async (job: WeatherJob, env: Env): Promise<void> => {
  const venue = VENUE_COORDS[job.keibajoCode];
  if (!venue) {
    console.error(`Unknown keibajo_code: ${job.keibajoCode}`);
    return;
  }
  const rows = await fetchVenueWeather({
    venue,
    raceDate: job.raceDate,
    weatherType: job.type,
  });
  const fetchedAt = new Date().toISOString();
  await upsertVenueWeather({
    archive: env.WEATHER_ARCHIVE,
    catalogStream: env.WEATHER_CATALOG_STREAM,
    keibajoCode: job.keibajoCode,
    raceDate: job.raceDate,
    weatherType: job.type,
    venue,
    rows,
    fetchedAt,
  });
  await Promise.all([
    putVenueWeatherV2RuntimeObjects({
      archive: env.WEATHER_ARCHIVE,
      keibajoCode: job.keibajoCode,
      raceDate: job.raceDate,
      weatherType: job.type,
      venue,
      rows,
      fetchedAt,
    }),
    sendVenueWeatherV2CatalogEvents({
      catalogStream: env.WEATHER_CATALOG_STREAM_V2,
      keibajoCode: job.keibajoCode,
      raceDate: job.raceDate,
      weatherType: job.type,
      venue,
      rows,
      fetchedAt,
    }),
  ]);
  // Invalidate KV cache so the next read reflects the fresh R2 data.
  await deleteWeatherFromKv(env.WEATHER_KV, job.raceDate);
};

export const handleWeatherBatch = async (
  batch: MessageBatch<WeatherJob>,
  env: Env,
): Promise<void> => {
  await Promise.all(
    batch.messages.map(async (msg) => {
      await processWeatherJob(msg.body, env);
      msg.ack();
    }),
  );
};
