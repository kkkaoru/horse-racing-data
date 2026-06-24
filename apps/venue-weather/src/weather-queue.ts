// Run with bun.
import { fetchVenueWeather } from "./weather-api";
import { upsertVenueWeather } from "./weather-d1";
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
    db: env.WEATHER_DB,
    keibajoCode: job.keibajoCode,
    raceDate: job.raceDate,
    weatherType: job.type,
    venue,
    rows,
    fetchedAt,
  });
  // Invalidate KV cache so the next read reflects the fresh D1 data
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
