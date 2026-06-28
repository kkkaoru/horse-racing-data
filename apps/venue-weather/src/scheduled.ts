// Run with bun.
import { VENUE_COORDS } from "./venue-coords";
import type { Env, WeatherJob, WeatherType } from "./types";

// JST 01:30 (16:30 UTC). Forecast must populate D1 BEFORE the finish-position launchd cron fires at JST 03:00 (scripts/launchd/com.kkk4oru.finish-position-predict.plist).
const FORECAST_CRON = "30 16 * * *";

export const getTodayJst = (): string => {
  const now = new Date();
  // Offset by JST (+9h)
  const jst = new Date(now.getTime() + 9 * 60 * 60 * 1000);
  return jst.toISOString().slice(0, 10);
};

export const buildWeatherJobs = (raceDate: string, weatherType: WeatherType): WeatherJob[] =>
  Object.keys(VENUE_COORDS)
    .sort()
    .map((keibajoCode) => ({
      type: weatherType,
      keibajoCode,
      raceDate,
    }));

export const dispatchWeatherJobs = async (
  env: Env,
  raceDate: string,
  weatherType: WeatherType,
): Promise<void> => {
  const jobs = buildWeatherJobs(raceDate, weatherType);
  await env.WEATHER_JOBS.sendBatch(jobs.map((body) => ({ body })));
};

export const handleScheduled = async (event: ScheduledController, env: Env): Promise<void> => {
  const raceDate = getTodayJst();
  const weatherType: WeatherType = event.cron === FORECAST_CRON ? "forecast" : "actual";
  await dispatchWeatherJobs(env, raceDate, weatherType);
};
