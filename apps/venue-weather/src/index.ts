// Run with bun.
import { handleWeatherBatch } from "./weather-queue";
import { handleScheduled } from "./scheduled";
import type { Env, WeatherJob } from "./types";

export default {
  fetch: (_request: Request, _env: Env): Response => new Response("venue-weather"),
  async queue(batch: MessageBatch<WeatherJob>, env: Env): Promise<void> {
    await handleWeatherBatch(batch, env);
  },
  async scheduled(event: ScheduledController, env: Env): Promise<void> {
    await handleScheduled(event, env);
  },
} satisfies ExportedHandler<Env, WeatherJob>;
