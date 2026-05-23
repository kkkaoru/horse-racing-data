import { describe, expect, test } from "vitest";

import {
  formatRunningStyleDateProgressLine,
  parseRunningStyleDateCliArgs,
} from "./run-running-style-date";
import {
  isRunningStyleDateProgressRowComplete,
  resolveRunningStyleDateYmd,
  summarizeRunningStyleDateProgress,
  type RunningStyleDateProgressRow,
} from "../src/running-style-date-progress";

describe("resolveRunningStyleDateYmd", () => {
  test("accepts YYYYMMDD as-is", () => {
    expect(resolveRunningStyleDateYmd("20260524")).toBe("20260524");
  });

  test("builds YYYYMMDD from MM-DD using the current JST year", () => {
    expect(resolveRunningStyleDateYmd("05-24", undefined, new Date("2026-05-20T00:00:00+09:00"))).toBe(
      "20260524",
    );
  });

  test("builds YYYYMMDD from MM-DD with an explicit year", () => {
    expect(resolveRunningStyleDateYmd("5-4", 2025)).toBe("20250504");
  });
});

describe("parseRunningStyleDateCliArgs", () => {
  test("requires --date", () => {
    expect(() => parseRunningStyleDateCliArgs([])).toThrow(/Usage/);
  });

  test("parses date and timing flags", () => {
    expect(
      parseRunningStyleDateCliArgs(
        [
          "--date",
          "05-24",
          "--year",
          "2026",
          "--poll-ms",
          "1000",
          "--delay-ms",
          "250",
          "--register-model",
          "jra:tmp/jra.json",
          "--remote-models",
          "--schedule-only",
        ],
        new Date("2026-05-20T00:00:00+09:00"),
      ),
    ).toEqual({
      dateYmd: "20260524",
      delayMs: 250,
      ensureModels: true,
      maxRounds: 120,
      pollMs: 1000,
      registerModels: [
        {
          inputPath: "tmp/jra.json",
          remote: true,
          source: "jra",
        },
      ],
      remoteModels: true,
      scheduleOnly: true,
      syncModels: true,
    });
  });
});

describe("running style date progress", () => {
  const completeRow: RunningStyleDateProgressRow = {
    cacheReady: true,
    d1Count: 12,
    displayReady: true,
    expectedHorses: 12,
    featuresReady: true,
    inferenceStatus: "completed",
    parquetReady: true,
    raceKey: "nar:20260524:44:12",
    source: "nar",
  };

  test("marks a fully ready race as complete", () => {
    expect(isRunningStyleDateProgressRowComplete(completeRow)).toBe(true);
  });

  test("summarizes incomplete races", () => {
    const summary = summarizeRunningStyleDateProgress([
      completeRow,
      {
        ...completeRow,
        cacheReady: false,
        d1Count: 0,
        inferenceStatus: "pending",
        parquetReady: false,
        raceKey: "jra:20260524:05:01",
        source: "jra",
      },
    ]);
    expect(summary.scanned).toBe(2);
    expect(summary.d1Ready).toBe(1);
    expect(summary.incomplete).toBe(1);
  });

  test("formats progress lines", () => {
    const line = formatRunningStyleDateProgressLine(
      summarizeRunningStyleDateProgress([completeRow]),
      "20260524",
      3,
    );
    expect(line).toContain("date=20260524");
    expect(line).toContain("round=3");
    expect(line).toContain("incomplete=0");
  });
});
