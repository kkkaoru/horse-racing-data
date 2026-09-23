// Run with bun (vitest).
import { expect, it, vi } from "vitest";

import {
  buildRaceTrendEnqueueMarkerKey,
  filterUnmarkedRaceTrendCandidates,
  markRaceTrendCandidatesEnqueued,
  type RaceTrendEnqueueCandidate,
} from "./race-trend-enqueue-marker";

const candidate: RaceTrendEnqueueCandidate = {
  delaySeconds: 0,
  message: {
    cacheGeneration: "3",
    day: "23",
    keibajoCode: "30",
    kind: "race-trend",
    month: "09",
    options: {
      frameEndYmd: "20260922",
      frameStartYmd: "20250923",
      includeRealtimeResults: true,
      jockeyEndYmd: "20260922",
      jockeyStartYmd: "20260823",
      source: "nar",
    },
    raceNumber: "05",
    source: "nar",
    year: "2026",
  },
};

it("builds a marker key from race, generation and variant options", () => {
  expect(buildRaceTrendEnqueueMarkerKey(candidate.message)).toBe(
    'race-trend-warm-enqueued:v1:nar:20260923:30:05:3:{"frameEndYmd":"20260922","frameStartYmd":"20250923","includeRealtimeResults":true,"jockeyEndYmd":"20260922","jockeyStartYmd":"20260823","source":"nar"}',
  );
});

it("keeps every candidate when no KV binding is available", async () => {
  expect(
    await filterUnmarkedRaceTrendCandidates({ candidates: [candidate], kv: undefined }),
  ).toStrictEqual([candidate]);
  await markRaceTrendCandidatesEnqueued({ candidates: [candidate], kv: undefined });
});

it("drops candidates whose marker exists and marks enqueued ones with a TTL", async () => {
  const store = new Map<string, string>();
  const put = vi.fn<
    (key: string, value: string, options: { expirationTtl: number }) => Promise<void>
  >(async (key, value) => {
    store.set(key, value);
  });
  const kv = { get: async (key: string) => store.get(key) ?? null, put };
  const other: RaceTrendEnqueueCandidate = {
    delaySeconds: 30,
    message: { ...candidate.message, raceNumber: "06" },
  };
  expect(
    await filterUnmarkedRaceTrendCandidates({ candidates: [candidate, other], kv }),
  ).toStrictEqual([candidate, other]);
  await markRaceTrendCandidatesEnqueued({ candidates: [candidate], kv });
  expect(put.mock.calls[0]?.[2]).toStrictEqual({ expirationTtl: 900 });
  expect(
    await filterUnmarkedRaceTrendCandidates({ candidates: [candidate, other], kv }),
  ).toStrictEqual([other]);
});
