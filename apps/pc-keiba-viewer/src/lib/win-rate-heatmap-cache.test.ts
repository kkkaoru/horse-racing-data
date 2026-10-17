// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import qualifiedFixture from "./__fixtures__/arc-qualified-stats.json";
import { requireArcQualifiedStats } from "./arc-qualified-stats";
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

it("uses v24 to invalidate falsely bounded carried-weight presentations", () => {
  expect(WIN_RATE_HEATMAP_CACHE_NAMESPACE).toBe("pc-keiba-viewer:win-rate-heatmap:v24");
  expect(WIN_RATE_HEATMAP_CACHE_FALLBACK_NAMESPACE).toBe("pc-keiba-viewer:win-rate-heatmap:v19");
  expect(WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS).toHaveLength(21);
});

it("uses a 36 hour heatmap cache TTL", () => {
  expect(WIN_RATE_HEATMAP_CACHE_TTL_SECONDS).toBe(216000);
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
  ).toStrictEqual(["pc-keiba-viewer:win-rate-heatmap:v19:2026:08:29:04:08:default"]);
  expect(
    expandWinRateHeatmapCacheReadKeys(
      "pc-keiba-viewer:win-rate-heatmap:v24:2026:08:29:04:08:default",
    ),
  ).toStrictEqual(["pc-keiba-viewer:win-rate-heatmap:v24:2026:08:29:04:08:default"]);
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
  expect(serializeWinRateHeatmapCacheQuery(new URLSearchParams("source=jra"))).toBe("default");
  expect(serializeWinRateHeatmapCacheQuery(new URLSearchParams("source=nar&a=1"))).toBe("a=1");
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
  const second = {
    bamei: "馬B",
    identitySource: "nar",
    sourceHorseId: "nar-b",
    umaban: 2,
    wakuban: 2,
  };
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
  expect(isWinRateHeatmapCacheManifest({ ...manifest, hasPresentation: true })).toBe(true);
  expect(isWinRateHeatmapCacheManifest({ ...manifest, hasPresentation: "yes" })).toBe(false);
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
  expect(
    isWinRateHeatmapSectionPayload({
      ...payload,
      arcQualifiedStats: requireArcQualifiedStats(qualifiedFixture),
      arcCohort: "arc-20261004-jra-A8-05-v2",
    }),
  ).toBe(true);
  expect(isWinRateHeatmapSectionPayload({ ...payload, arcCohort: "old" })).toBe(false);
  expect(isWinRateHeatmapSectionPayload({ ...payload, presentation: null })).toBe(false);
  expect(
    isWinRateHeatmapSectionPayload({
      ...payload,
      presentation: { version: 1, rows: [], combinedBloodlineRows: [], displays: {} },
    }),
  ).toBe(true);
  expect(isWinRateHeatmapSectionPayload({ ...payload, weightClassStats: undefined })).toBe(false);
  expect(isWinRateHeatmapSectionPayload(null)).toBe(false);
  expect(isWinRateHeatmapSectionPayload({ type: "condition" })).toBe(false);
});

it.each([undefined, 1, 2, "1", null])("validates the producer data revision %s", (revision) => {
  expect(
    isWinRateHeatmapCacheManifest({
      type: "win-rate-heatmap-manifest",
      fragmentKinds: WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS,
      generation: GENERATION,
      runnerSignature: "[]",
      producerDataRevision: revision,
    }),
  ).toBe(revision === undefined || revision === 1);
  expect(
    isWinRateHeatmapSectionPayload({
      type: "win-rate-heatmap",
      runners: [],
      bloodlineRows: [],
      carriedWeightClassStats: [],
      frameStats: [],
      horseResults: [],
      similarRows: [],
      weightClassStats: [],
      producerDataRevision: revision,
    }),
  ).toBe(revision === undefined || revision === 1);
});

it("changes only overseas heatmap keys and disallows legacy fallback", () => {
  const input = {
    year: "2026",
    month: "10",
    day: "04",
    keibajoCode: "A8",
    raceNumber: "05",
    query: "",
  };
  expect(buildWinRateHeatmapCacheKey(input)).toBe(
    "pc-keiba-viewer:win-rate-heatmap:v24:overseas-readers-v3-arc-qualified-v2:2026:10:04:A8:05:default",
  );
  expect(buildWinRateHeatmapCacheFallbackKeys(input)).toStrictEqual([]);
});

it("keeps domestic 77 keys free of overseas mapping revision namespaces", () => {
  expect(
    buildWinRateHeatmapCacheKey({
      year: "2026",
      month: "10",
      day: "04",
      keibajoCode: "77",
      raceNumber: "05",
      query: "",
    }),
  ).toBe("pc-keiba-viewer:win-rate-heatmap:v24:2026:10:04:77:05:default");
});

it("preserves domestic registration bytes and leading nonzero IDs", () => {
  expect(buildWinRateHeatmapRunnerSignature([{ umaban: "1", kettoTorokuBango: "000123" }])).toBe(
    '["01","000123","","","","","","","",""]',
  );
});
it.each([
  { sourceHorseId: "123" },
  { identitySource: "foreign" },
  { identitySource: "foreign", sourceHorseId: "0000" },
  { identitySource: "", sourceHorseId: "123" },
])("rejects incomplete foreign identity %j", (identity) => {
  expect(
    buildWinRateHeatmapRunnerSignature([
      { ...identity, umaban: "1", kettoTorokuBango: "0000000000", bamei: "Same name" },
    ]),
  ).toBeNull();
});
it.each([
  { sourceHorseId: "456" },
  { identitySource: "other" },
  { ownerNameFull: "New owner" },
  { banushimei: "New display" },
  { sourceOwnerId: "owner2" },
])("invalidates changed foreign profile %j with zero and native registration", (change) => {
  const foreign = {
    umaban: "1",
    kettoTorokuBango: "0000000000",
    identitySource: "foreign",
    sourceHorseId: "000123",
  };
  expect(buildWinRateHeatmapRunnerSignature([foreign])).not.toBeNull();
  expect(buildWinRateHeatmapRunnerSignature([{ ...foreign, ...change }])).not.toBe(
    buildWinRateHeatmapRunnerSignature([foreign]),
  );
  const native = { ...foreign, kettoTorokuBango: "2023100001" };
  expect(buildWinRateHeatmapRunnerSignature([{ ...native, ...change }])).not.toBe(
    buildWinRateHeatmapRunnerSignature([native]),
  );
});

it("uses mapped horse provider independently of name identity source", () => {
  const runner = {
    umaban: "1",
    sourceHorseId: "000a029d42",
    sourceHorseProvider: "netkeiba",
    identitySource: "jra-van",
  };
  expect(buildWinRateHeatmapRunnerSignature([runner])).not.toBeNull();
  expect(buildWinRateHeatmapRunnerSignature([runner])).toBe(
    buildWinRateHeatmapRunnerSignature([{ ...runner, identitySource: "jra" }]),
  );
  expect(buildWinRateHeatmapRunnerSignature([runner])).not.toBe(
    buildWinRateHeatmapRunnerSignature([{ ...runner, sourceHorseProvider: "other" }]),
  );
});

it("rejects old Arc payloads and manifests even with a current marker", () => {
  expect(
    isWinRateHeatmapSectionPayload({
      type: "win-rate-heatmap",
      arcCohort: "arc-20261004-jra-A8-05-v2",
      bloodlineRows: [],
      carriedWeightClassStats: [],
      frameStats: [],
      horseResults: [],
      runners: [],
      similarRows: [],
      weightClassStats: [],
    }),
  ).toBe(false);
  expect(
    isWinRateHeatmapCacheManifest({
      type: "win-rate-heatmap-manifest",
      arcCohort: "arc-20261004-jra-A8-05-v2",
      generation: "1234567890abcdef",
      runnerSignature: "x",
      fragmentKinds: WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS,
    }),
  ).toBe(false);
});

it("versions foreign runner signatures without changing domestic signatures", () => {
  expect(
    buildWinRateHeatmapRunnerSignature([
      { umaban: "1", sourceHorseProvider: "netkeiba", sourceHorseId: "000a029d42" },
    ]),
  ).toMatch(/arc-qualified-v2/u);
  expect(
    buildWinRateHeatmapRunnerSignature([{ umaban: "1", kettoTorokuBango: "2023100001" }]),
  ).not.toMatch(/arc-qualified-v2/u);
});
