// run with: bun run test
import { expect, it, vi } from "vitest";

import {
  HandlerTimeoutError,
  QUEUE_HANDLER_TIMEOUT_MS,
  withHandlerTimeout,
} from "./handler-timeout";

it("resolves with the task value when the task settles before the deadline", async () => {
  const result = await withHandlerTimeout({
    label: "fast",
    ms: 1_000,
    task: Promise.resolve(42),
  });
  expect(result).toBe(42);
});

it("rejects with HandlerTimeoutError carrying the label when the deadline fires", async () => {
  vi.useFakeTimers();
  const slow = new Promise<number>(() => {});
  const pending = withHandlerTimeout({ label: "slow-scrape", ms: 100, task: slow });
  vi.advanceTimersByTime(100);
  await expect(pending).rejects.toBeInstanceOf(HandlerTimeoutError);
  vi.useRealTimers();
});

it("preserves the original Error when the task rejects with an Error before the deadline", async () => {
  await expect(
    withHandlerTimeout({
      label: "boom",
      ms: 1_000,
      task: Promise.reject(new Error("upstream failed")),
    }),
  ).rejects.toThrow("upstream failed");
});

it("wraps a non-Error rejection in an Error so the catch sees a real Error instance", async () => {
  await expect(
    withHandlerTimeout({
      label: "string-throw",
      ms: 1_000,
      task: Promise.reject("raw-string-error"),
    }),
  ).rejects.toThrow("raw-string-error");
});

it("clears the pending timer once the task resolves so unrelated timers do not fire", async () => {
  vi.useFakeTimers();
  const cleared: number[] = [];
  const originalClear = globalThis.clearTimeout;
  globalThis.clearTimeout = ((handle: ReturnType<typeof setTimeout>) => {
    cleared.push(Number(handle));
    return originalClear(handle);
  }) as typeof clearTimeout;
  await withHandlerTimeout({ label: "fast", ms: 1_000, task: Promise.resolve("done") });
  globalThis.clearTimeout = originalClear;
  vi.useRealTimers();
  expect(cleared.length).toBe(1);
});

it("exposes a queue handler budget that is comfortably below the 30s runtime cancel", () => {
  expect(QUEUE_HANDLER_TIMEOUT_MS).toBe(24_950);
});

it("constructs HandlerTimeoutError with both name and label populated", () => {
  const error = new HandlerTimeoutError("fetch-results");
  expect(error.name).toBe("HandlerTimeoutError");
  expect(error.label).toBe("fetch-results");
  expect(error.message).toBe("handler timeout: fetch-results");
});
