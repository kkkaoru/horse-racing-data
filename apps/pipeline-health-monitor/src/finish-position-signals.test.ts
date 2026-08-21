import { expect, it } from "vitest";

import {
  buildCanarySignal,
  buildEndpointFailureSignal,
  buildEndpointRecoverySignal,
  buildReadinessSignals,
} from "./finish-position-signals";
import type { PredictionReadinessResponse } from "./types";

const readiness = (deadline: "T-120" | "T-60" = "T-60"): PredictionReadinessResponse => ({
  checkedAt: "2026-08-15T00:00:00Z",
  races: [
    {
      complete: false,
      deadline,
      expectedCount: 10,
      keibajoCode: "05",
      minutesToPost: deadline === "T-120" ? 120 : 60,
      missingCount: 2,
      newestPredictionAt: null,
      oldestPredictionAt: null,
      predictionCount: 8,
      raceBango: "01",
      raceKey: "jra:05:01",
      raceStartAtJst: "2026-08-15T10:00:00+09:00",
      source: "jra",
    },
  ],
  runYmd: "20260815",
});

it("builds warning at T-120 and critical at T-60", () => {
  const warning = buildReadinessSignals(readiness("T-120"))[0];
  expect(warning?.severity).toBe("warning");
  expect(warning?.key).toBe("finish-position-readiness:20260815:jra:05:01");
  expect(warning?.fields[0]).toStrictEqual({ name: "Run date", value: "20260815" });
  expect(warning?.title).toBe("finish-position predictions incomplete 20260815 jra:05:01");
  const critical = buildReadinessSignals(readiness("T-60"))[0];
  expect(critical?.severity).toBe("critical");
  expect(critical?.fields).toContainEqual({ name: "Coverage", value: "8/10" });
});

it("describes missing entry rows separately", () => {
  const response = readiness();
  const race = response.races[0];
  if (race) {
    race.expectedCount = 0;
    race.predictionCount = 0;
    race.missingCount = 0;
  }
  expect(buildReadinessSignals(response)[0]?.description).toContain("entry rows");
});

it("marks an overdue or absent canary unhealthy and a consumed canary healthy", () => {
  const now = new Date("2026-08-15T00:20:00Z");
  const absent = buildCanarySignal({ canaries: [], checkedAt: now.toISOString() }, now);
  expect(absent.ok).toBe(false);
  expect(absent.description).toContain("No delivery canary");
  const overdue = buildCanarySignal(
    {
      canaries: [
        {
          consumedAt: null,
          deliveryLagMs: null,
          enqueuedAt: "2026-08-15T00:00:00Z",
          id: "oldest",
        },
      ],
      checkedAt: now.toISOString(),
    },
    now,
  );
  expect(overdue.ok).toBe(false);
  expect(overdue.fields[0]?.value).toBe("oldest");
  const healthy = buildCanarySignal(
    {
      canaries: [
        {
          consumedAt: "2026-08-15T00:16:00Z",
          deliveryLagMs: 60_000,
          enqueuedAt: "2026-08-15T00:15:00Z",
          id: "consumed",
        },
      ],
      checkedAt: now.toISOString(),
    },
    now,
  );
  expect(healthy.ok).toBe(true);
  const staleHeartbeat = buildCanarySignal(
    {
      canaries: [
        {
          consumedAt: "2026-08-15T00:01:00Z",
          deliveryLagMs: 60_000,
          enqueuedAt: "2026-08-15T00:00:00Z",
          id: "stale-consumed",
        },
      ],
      checkedAt: now.toISOString(),
    },
    now,
  );
  expect(staleHeartbeat.ok).toBe(false);
  expect(staleHeartbeat.description).toContain("No new delivery canary");
});

it("builds endpoint failure and recovery signals with the same key", () => {
  const failure = buildEndpointFailureSignal("readiness", new Error("timeout"));
  const recovery = buildEndpointRecoverySignal("readiness");
  expect(failure.ok).toBe(false);
  expect(failure.description).toContain("timeout");
  expect(recovery.ok).toBe(true);
  expect(recovery.key).toBe(failure.key);
});
