import { expect, it, vi } from "vitest";

import { isRetryableDbError, withDbRetry } from "./db-retry";

type Loader = () => Promise<string>;

const noSleep = (): Promise<void> => Promise.resolve();

interface ErrorWithCode extends Error {
  code: string;
}

const buildError = (code: string, message = "boom"): ErrorWithCode =>
  Object.assign(new Error(message), { code });

it("retries on ECONNRESET until success", async () => {
  const load = vi
    .fn<Loader>()
    .mockRejectedValueOnce(buildError("ECONNRESET"))
    .mockResolvedValueOnce("ok");
  await expect(withDbRetry(load, { sleep: noSleep })).resolves.toBe("ok");
  expect(load).toHaveBeenCalledTimes(2);
});

it("retries on SQLSTATE 08006 connection failure", async () => {
  const load = vi
    .fn<Loader>()
    .mockRejectedValueOnce(buildError("08006"))
    .mockResolvedValueOnce("ok");
  await expect(withDbRetry(load, { sleep: noSleep })).resolves.toBe("ok");
  expect(load).toHaveBeenCalledTimes(2);
});

it("retries on serialization failures", async () => {
  const load = vi
    .fn<Loader>()
    .mockRejectedValueOnce(buildError("40001"))
    .mockResolvedValueOnce("ok");
  await expect(withDbRetry(load, { sleep: noSleep })).resolves.toBe("ok");
  expect(load).toHaveBeenCalledTimes(2);
});

it("retries when the error message mentions a terminated connection", async () => {
  const load = vi
    .fn<Loader>()
    .mockRejectedValueOnce(new Error("Connection terminated unexpectedly"))
    .mockResolvedValueOnce("ok");
  await expect(withDbRetry(load, { sleep: noSleep })).resolves.toBe("ok");
  expect(load).toHaveBeenCalledTimes(2);
});

it("does not retry on non-transient errors", async () => {
  const load = vi.fn<Loader>().mockRejectedValue(buildError("23505", "duplicate key"));
  await expect(withDbRetry(load, { sleep: noSleep })).rejects.toThrow("duplicate key");
  expect(load).toHaveBeenCalledTimes(1);
});

it("isRetryableDbError flags ETIMEDOUT", () => {
  expect(isRetryableDbError(buildError("ETIMEDOUT"))).toBe(true);
});

it("isRetryableDbError ignores plain errors", () => {
  expect(isRetryableDbError(new Error("oops"))).toBe(false);
});
