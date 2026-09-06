// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import {
  WIN_RATE_HEATMAP_CACHE_FALLBACK_NAMESPACE,
  WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS,
  WIN_RATE_HEATMAP_CACHE_NAMESPACE,
  WIN_RATE_HEATMAP_CACHE_TTL_SECONDS,
  WIN_RATE_HEATMAP_CACHE_URL_BASE,
  buildWinRateHeatmapCacheFallbackKeys,
  buildWinRateHeatmapCacheKey,
  buildWinRateHeatmapFragmentCacheKey,
  buildWinRateHeatmapRunnerSignature,
  createWinRateHeatmapCacheRequest,
  expandWinRateHeatmapCacheReadKeys,
  isWinRateHeatmapCacheFragment,
  isWinRateHeatmapCacheManifest,
  isWinRateHeatmapSectionPayload,
  serializeWinRateHeatmapCacheQuery,
} from "./win-rate-heatmap-cache";

const GENERATION = "123e4567-e89b-12d3-a456-426614174000";

it("uses the column-fragment heatmap cache namespace v18", () => {
  expect(WIN_RATE_HEATMAP_CACHE_NAMESPACE).toBe("pc-keiba-viewer:win-rate-heatmap:v18");
  expect(WIN_RATE_HEATMAP_CACHE_FALLBACK_NAMESPACE).toBe("pc-keiba-viewer:win-rate-heatmap:v17");
  expect(WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS).toHaveLength(14);
});

it("uses a 36 hour heatmap cache TTL", () => {
  expect(WIN_RATE_HEATMAP_CACHE_TTL_SECONDS).toBe(129600);
});

it("builds per-race manifest and generation-scoped column keys", () => {
  const key = buildWinRateHeatmapCacheKey({
    day: "21",
    keibajoCode: "05",
    month: "8",
    query: "",
    raceNumber: "1",
    year: "2026",
  });
  expect(key).toBe(`${WIN_RATE_HEATMAP_CACHE_NAMESPACE}:2026:08:21:05:01:default`);
  expect(buildWinRateHeatmapFragmentCacheKey(key, GENERATION, "jockey")).toBe(
    `${key}:fragment:${GENERATION}:jockey`,
  );
});

it("keeps the previous namespace for busts but never reads it", () => {
  expect(
    buildWinRateHeatmapCacheFallbackKeys({
      day: "29",
      keibajoCode: "04",
      month: "08",
      query: "",
      raceNumber: "08",
      year: "2026",
    }),
  ).toStrictEqual(["pc-keiba-viewer:win-rate-heatmap:v17:2026:08:29:04:08:default"]);
  expect(
    expandWinRateHeatmapCacheReadKeys(
      "pc-keiba-viewer:win-rate-heatmap:v18:2026:08:29:04:08:default",
    ),
  ).toStrictEqual(["pc-keiba-viewer:win-rate-heatmap:v18:2026:08:29:04:08:default"]);
});

it("keeps explicit query fingerprints and stable query ordering", () => {
  expect(
    buildWinRateHeatmapCacheKey({
      day: "21",
      keibajoCode: "50",
      month: "08",
      query: "statsVenue=1",
      raceNumber: "12",
      year: "2026",
    }),
  ).toBe(`${WIN_RATE_HEATMAP_CACHE_NAMESPACE}:2026:08:21:50:12:statsVenue=1`);
  expect(serializeWinRateHeatmapCacheQuery(new URLSearchParams())).toBe("default");
  expect(serializeWinRateHeatmapCacheQuery(new URLSearchParams("b=2&a=1"))).toBe("a=1&b=2");
  expect(serializeWinRateHeatmapCacheQuery(new URLSearchParams("a=2&a=1"))).toBe("a=1&a=2");
});

it("builds a Cache API URL", () => {
  expect(createWinRateHeatmapCacheRequest("heatmap-key").url).toBe(
    `${WIN_RATE_HEATMAP_CACHE_URL_BASE}heatmap-key`,
  );
});

it("builds stable runner signatures from every heatmap input dimension", () => {
  const first = {
    bamei: "馬A",
    bataiju: "480",
    chokyoshimeiRyakusho: "調教師",
    damSireName: "母父",
    futanJuryo: "560",
    kettoTorokuBango: "2023100001",
    kishumeiRyakusho: "騎手",
    sireName: "父",
    sireSireName: "父父",
    umaban: "1",
    wakuban: "1",
  };
  const second = { bamei: "馬B", sourceHorseId: "nar-b", umaban: 2, wakuban: 2 };
  const signature = buildWinRateHeatmapRunnerSignature([second, first]);
  expect(signature).toBe(buildWinRateHeatmapRunnerSignature([first, second]));
  expect(buildWinRateHeatmapRunnerSignature([{ bamei: "馬A" }])).toBe(null);
  expect(buildWinRateHeatmapRunnerSignature([{ umaban: "1" }])).toBe(null);
  expect(buildWinRateHeatmapRunnerSignature([{ bamei: "", umaban: "1" }])).toBe(null);
});

it("validates a complete manifest and rejects duplicate or malformed fragments", () => {
  const manifest = {
    fragmentKinds: [...WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS],
    generation: GENERATION,
    runnerSignature: "runner-signature",
    type: "win-rate-heatmap-manifest",
  };
  expect(isWinRateHeatmapCacheManifest(manifest)).toBe(true);
  expect(isWinRateHeatmapCacheManifest({ ...manifest, generation: "bad" })).toBe(false);
  expect(isWinRateHeatmapCacheManifest({ ...manifest, runnerSignature: "" })).toBe(false);
  expect(
    isWinRateHeatmapCacheManifest({
      ...manifest,
      fragmentKinds: manifest.fragmentKinds.map((kind) => (kind === "trainer" ? "jockey" : kind)),
    }),
  ).toBe(false);
  expect(isWinRateHeatmapCacheManifest(null)).toBe(false);
});

it("treats an explicitly available empty or zero-valued fragment as present", () => {
  expect(
    isWinRateHeatmapCacheFragment({
      generation: GENERATION,
      kind: "jockey",
      rows: [],
      type: "win-rate-heatmap-fragment",
    }),
  ).toBe(true);
  expect(
    isWinRateHeatmapCacheFragment({
      generation: GENERATION,
      kind: "jockey",
      rows: [{ starts: 0, winRate: 0 }],
      type: "win-rate-heatmap-fragment",
    }),
  ).toBe(true);
  expect(
    isWinRateHeatmapCacheFragment({
      generation: GENERATION,
      kind: "unknown",
      rows: [],
      type: "win-rate-heatmap-fragment",
    }),
  ).toBe(false);
  expect(isWinRateHeatmapCacheFragment("fragment")).toBe(false);
});

it("accepts complete section payload arrays and rejects missing arrays", () => {
  const payload = {
    bloodlineRows: [],
    carriedWeightClassStats: [],
    frameStats: [],
    horseResults: [],
    runners: [],
    similarRows: [],
    type: "win-rate-heatmap",
    weightClassStats: [],
  };
  expect(isWinRateHeatmapSectionPayload(payload)).toBe(true);
  expect(isWinRateHeatmapSectionPayload({ ...payload, weightClassStats: undefined })).toBe(false);
  expect(isWinRateHeatmapSectionPayload(null)).toBe(false);
  expect(isWinRateHeatmapSectionPayload({ type: "condition" })).toBe(false);
});
