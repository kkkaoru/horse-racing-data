import { expect, it, vi } from "vitest";

import { retry } from "./retry";

const noSleep = (): Promise<void> => Promise.resolve();

it("returns the resolved value when load succeeds on first attempt", async () => {
  const load = vi.fn<() => Promise<string>>().mockResolvedValue("ok");
  await expect(retry(load, { sleep: noSleep })).resolves.toBe("ok");
  expect(load).toHaveBeenCalledTimes(1);
});

it("retries until load succeeds within attempt limit", async () => {
  const load = vi
    .fn<() => Promise<string>>()
    .mockRejectedValueOnce(new Error("first"))
    .mockRejectedValueOnce(new Error("second"))
    .mockResolvedValueOnce("ok");
  await expect(retry(load, { attempts: 3, sleep: noSleep })).resolves.toBe("ok");
  expect(load).toHaveBeenCalledTimes(3);
});

it("throws the last error when all attempts fail", async () => {
  const load = vi.fn<() => Promise<string>>().mockRejectedValue(new Error("boom"));
  await expect(retry(load, { attempts: 2, sleep: noSleep })).rejects.toThrow("boom");
  expect(load).toHaveBeenCalledTimes(2);
});

it("does not retry when shouldRetry returns false", async () => {
  const load = vi.fn<() => Promise<string>>().mockRejectedValue(new Error("fatal"));
  const shouldRetry = vi.fn<(error: unknown, attempt: number) => boolean>().mockReturnValue(false);
  await expect(retry(load, { attempts: 5, shouldRetry, sleep: noSleep })).rejects.toThrow("fatal");
  expect(load).toHaveBeenCalledTimes(1);
  expect(shouldRetry).toHaveBeenCalledTimes(1);
});

it("invokes sleep with exponentially growing delays capped by maxDelayMs", async () => {
  const sleep = vi.fn<(ms: number) => Promise<void>>().mockResolvedValue(undefined);
  const load = vi.fn<() => Promise<string>>().mockRejectedValue(new Error("boom"));
  await expect(
    retry(load, { attempts: 4, baseDelayMs: 100, maxDelayMs: 300, sleep }),
  ).rejects.toThrow("boom");
  expect(sleep.mock.calls.map((call) => call[0])).toStrictEqual([100, 200, 300]);
});

it("uses default attempt count when omitted", async () => {
  const load = vi.fn<() => Promise<string>>().mockRejectedValue(new Error("boom"));
  await expect(retry(load, { sleep: noSleep })).rejects.toThrow("boom");
  expect(load).toHaveBeenCalledTimes(3);
});

it("passes the current attempt index to shouldRetry", async () => {
  const load = vi.fn<() => Promise<string>>().mockRejectedValue(new Error("boom"));
  const shouldRetry = vi.fn<(error: unknown, attempt: number) => boolean>().mockReturnValue(true);
  await expect(retry(load, { attempts: 3, shouldRetry, sleep: noSleep })).rejects.toThrow("boom");
  expect(shouldRetry.mock.calls.map((call) => call[1])).toStrictEqual([0, 1]);
});
