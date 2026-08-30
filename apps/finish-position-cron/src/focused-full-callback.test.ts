// Run with bun. Tests for scoped detached-Container completion callbacks.

import { expect, test, vi } from "vitest";
import {
  buildFocusedFullCompletionCallbackUrl,
  handleFocusedFullCompletionCallback,
} from "./focused-full-callback";
import type { ValidatedFocusedFullWatchPayload } from "./focused-full-watch";
import type { Env } from "./types";

const WATCH: ValidatedFocusedFullWatchPayload = {
  body: {
    category: "jra",
    daysAhead: 0,
    keibajoCode: "05",
    mode: "full",
    raceBango: "09",
    runDate: "2026-08-24",
    runDateIso: "2026-08-24",
    runYmd: "20260824",
    skipDedup: true,
  },
  doName: "race-chain-predict-jra-1",
  role: "race-chain",
  watchId: "watch-123",
  workKey: "focused-full:20260824:jra:05:09",
};

const ENV = {
  FOCUSED_FULL_CALLBACK_URL:
    "https://finish-position-cron.example.workers.dev/api/internal/focused-full-completion-callback",
  TRIGGER_TOKEN: "callback-signing-secret",
} as Env;

test("builds a signed callback and immediately enqueues an authoritative status tick", async () => {
  const callbackUrl = await buildFocusedFullCompletionCallbackUrl(ENV, WATCH);
  if (callbackUrl === undefined) throw new Error("callback URL was not built");
  const send = vi.fn(async () => undefined);

  const response = await handleFocusedFullCompletionCallback(
    new Request(callbackUrl, { method: "POST" }),
    ENV,
    { now: () => 123_000, send },
  );

  expect(response.status).toBe(202);
  expect(send).toHaveBeenCalledWith(
    ENV,
    expect.objectContaining({
      ...WATCH,
      deadlineAtMs: 1_983_000,
      type: "focused-full-watch-tick",
    }),
  );
});

test("rejects a tampered callback without queue delivery", async () => {
  const callbackUrl = await buildFocusedFullCompletionCallbackUrl(ENV, WATCH);
  if (callbackUrl === undefined) throw new Error("callback URL was not built");
  const url = new URL(callbackUrl);
  url.searchParams.set("payload", `${url.searchParams.get("payload")}x`);
  const send = vi.fn(async () => undefined);

  const response = await handleFocusedFullCompletionCallback(
    new Request(url, { method: "POST" }),
    ENV,
    { now: () => 123_000, send },
  );

  expect(response.status).toBe(401);
  expect(send).not.toHaveBeenCalled();
});

test("disables callbacks when no public callback URL is configured", async () => {
  await expect(
    buildFocusedFullCompletionCallbackUrl({ TRIGGER_TOKEN: "callback-signing-secret" }, WATCH),
  ).resolves.toBeUndefined();
});
