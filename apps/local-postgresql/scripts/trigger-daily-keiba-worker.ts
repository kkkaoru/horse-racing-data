#!/usr/bin/env bun

import {
  type FallbackProvider,
  triggerWorkerFallback,
  waitForWorkerFallback,
} from "../src/replica-push/worker-fallback";

const providerValue = Bun.argv[Bun.argv.indexOf("--provider") + 1];
if (providerValue !== "jv" && providerValue !== "nv")
  throw new Error("--provider must be jv or nv");
const provider: FallbackProvider = providerValue;
const result = await triggerWorkerFallback(
  provider,
  Bun.env.DAILY_KEIBA_SYNC_BASE_URL,
  Bun.env.DAILY_KEIBA_SYNC_ADMIN_TOKEN,
);
console.log(
  JSON.stringify({ provider: result.provider, runDate: result.runDate, runId: result.runId }),
);
await waitForWorkerFallback(
  result,
  Bun.env.DAILY_KEIBA_SYNC_BASE_URL ?? "",
  Bun.env.DAILY_KEIBA_SYNC_ADMIN_TOKEN ?? "",
);
console.log(
  JSON.stringify({ provider: result.provider, runId: result.runId, status: "succeeded" }),
);
