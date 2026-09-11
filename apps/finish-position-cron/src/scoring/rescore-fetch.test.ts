// Run with bun. Tests service-binding routing for rescore realtime reads.

import { expect, test, vi } from "vitest";

import type { Env } from "../types";
import { resolveRescoreRealtimeFetch } from "./rescore-fetch";

const response = (): Response => Response.json({ ok: true });

test("routes hot odds and horse weights to their in-process service bindings", async () => {
  const hotFetch = vi.fn(async () => response());
  const realtimeFetch = vi.fn(async () => response());
  const fallbackFetch = vi.fn(async () => response());
  const routedFetch = resolveRescoreRealtimeFetch(
    {
      REALTIME_HOT: { fetch: hotFetch },
      REALTIME_SERVICE: { fetch: realtimeFetch },
    } as unknown as Env,
    fallbackFetch,
  );

  await routedFetch("https://sync-realtime-data-hot.kkk4oru.com/api/odds/race");
  await routedFetch(new Request("https://sync-realtime-data.kkk4oru.com/api/horse-weight/race"));

  expect(hotFetch).toHaveBeenCalledTimes(1);
  expect(realtimeFetch).toHaveBeenCalledTimes(1);
  expect(fallbackFetch).not.toHaveBeenCalled();
});

test("falls back for unbound and unrelated origins", async () => {
  const fallbackFetch = vi.fn(async () => response());
  const routedFetch = resolveRescoreRealtimeFetch({} as unknown as Env, fallbackFetch);

  await routedFetch(new URL("https://sync-realtime-data-hot.kkk4oru.com/api/odds/race"));
  await routedFetch("https://example.com/data");

  expect(fallbackFetch).toHaveBeenCalledTimes(2);
});
