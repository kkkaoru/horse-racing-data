// Run with bun.
import { handleWeatherBatch } from "./weather-queue";
import { handleScheduled } from "./scheduled";
import { handleWeatherFetch } from "./weather-handler";
import type { Env, WeatherJob } from "./types";

export default {
  fetch: (request: Request, env: Env): Promise<Response> => handleWeatherFetch(request, env),
  async queue(batch: MessageBatch<WeatherJob>, env: Env): Promise<void> {
    await handleWeatherBatch(batch, env);
  },
  async scheduled(event: ScheduledController, env: Env): Promise<void> {
    await handleScheduled(event, env);
  },
} satisfies ExportedHandler<Env, WeatherJob>;
