import { describe, expect, test } from "vitest";
import {
  acquisitionBody,
  defaultAcquisitionWindow,
  CRON_JV_DAILY,
  CRON_JV_MONITOR,
  CRON_NV_DAILY,
  CRON_NV_MONITOR,
  jstDate,
  scheduledAction,
} from "./schedule";

describe("daily schedule", () => {
  test("maps all four UTC crons to their JST actions", () => {
    expect(scheduledAction(CRON_NV_DAILY)).toEqual({ action: "run", provider: "nv" });
    expect(scheduledAction(CRON_NV_MONITOR)).toEqual({ action: "monitor", provider: "nv" });
    expect(scheduledAction(CRON_JV_DAILY)).toEqual({ action: "run", provider: "jv" });
    expect(scheduledAction(CRON_JV_MONITOR)).toEqual({ action: "monitor", provider: "jv" });
    expect(scheduledAction("* * * * *")).toBeUndefined();
  });

  test("derives the JST date across a UTC day boundary", () => {
    expect(jstDate(Date.parse("2026-09-02T16:00:00Z"))).toBe("20260903");
    expect(jstDate(Date.parse("2026-09-03T11:00:00Z"))).toBe("20260903");
  });

  test("builds provider-specific overlapping acquisition queries", () => {
    expect(defaultAcquisitionWindow("nv", "20260903", 2)).toEqual({
      fromTime: "20260901000000",
      toTime: null,
    });
    expect(defaultAcquisitionWindow("jv", "20260301", 2)).toEqual({
      fromTime: "20260227000000",
      toTime: "20260301235959",
    });
    expect(acquisitionBody("nv", "20260903123456", null)).toEqual({
      dataSpec: "RACE",
      fromTime: "20260903123456",
      option: 1,
    });
    expect(acquisitionBody("jv", "20260903123456", "20260904120000")).toEqual({
      dataSpec: "RACE",
      from: "20260903123456",
      to: "20260904120000",
    });
    expect(() => acquisitionBody("jv", "20260903123456", null)).toThrow("end time");
  });
});
