// Run with bun. Routes Worker-native rescore HTTP reads through service bindings.

import type { Env } from "../types";

const REALTIME_HOT_HOST = "sync-realtime-data-hot.kkk4oru.com";
const REALTIME_HOST = "sync-realtime-data.kkk4oru.com";

const requestHostname = (input: RequestInfo | URL): string =>
  new URL(input instanceof Request ? input.url : input.toString()).hostname;

export const resolveRescoreRealtimeFetch =
  (env: Env, fallbackFetch: typeof fetch): typeof fetch =>
  (input, init) => {
    const hostname = requestHostname(input);
    if (hostname === REALTIME_HOT_HOST && env.REALTIME_HOT !== undefined)
      return env.REALTIME_HOT.fetch(input, init);
    if (hostname === REALTIME_HOST && env.REALTIME_SERVICE !== undefined)
      return env.REALTIME_SERVICE.fetch(input, init);
    return fallbackFetch(input, init);
  };
