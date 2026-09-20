// Runs with bun through Vitest; no queue I/O.
import { expect, it, vi } from "vitest";
import { buildReadFailureAlert, notifyReadFailure } from "./read-failure-alert";

it("builds a warning alert with the sanitized failure fields and a JST timestamp", () => {
  const alert = buildReadFailureAlert({
    event: "race_runners_read_failed",
    errorName: "R2SqlQueryError",
    status: 503,
    nowMs: Date.UTC(2026, 8, 20, 12, 0, 0),
  });
  expect(alert).toStrictEqual({
    checkName: "catalog-read-failure",
    severity: "warning",
    title: "Catalog read failed: race_runners_read_failed",
    description:
      "A trusted Catalog binding read failed after its bounded retry and answered 503: race_runners_read_failed.",
    fields: [
      { name: "event", value: "race_runners_read_failed" },
      { name: "errorName", value: "R2SqlQueryError" },
      { name: "status", value: "503" },
    ],
    timestampJst: "2026-09-20T21:00:00.000+09:00",
  });
});

it("omits absent detail fields", () => {
  const alert = buildReadFailureAlert({ event: "race_day_list_read_failed" });
  expect(alert.fields).toStrictEqual([{ name: "event", value: "race_day_list_read_failed" }]);
  expect(alert.timestampJst).toMatch(/\+09:00$/u);
});

it("sends through the queue and stays silent without one", async () => {
  const send = vi.fn<(message: unknown) => Promise<void>>().mockResolvedValue(undefined);
  await notifyReadFailure({ send }, { event: "race_day_list_read_failed" });
  expect(send).toHaveBeenCalledTimes(1);
  expect(send.mock.calls[0]?.[0]).toMatchObject({
    checkName: "catalog-read-failure",
    fields: [{ name: "event", value: "race_day_list_read_failed" }],
  });
  await expect(notifyReadFailure(undefined, { event: "x" })).resolves.toBeUndefined();
});

it("never throws when the queue rejects", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const send = vi
    .fn<(message: unknown) => Promise<void>>()
    .mockRejectedValue(new Error("queue unavailable"));
  await expect(
    notifyReadFailure({ send }, { event: "race_day_list_read_failed" }),
  ).resolves.toBeUndefined();
  expect(log).toHaveBeenCalledWith('{"event":"catalog_read_failure_alert_failed"}');
  log.mockRestore();
});
