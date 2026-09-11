// Run with bun. Tests for race-scoped fallback when category-wide freshness changes elsewhere.

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";

const { foundationReadinessMock, runningStyleReadinessMock } = vi.hoisted(() => ({
  foundationReadinessMock: vi.fn(async () => ({ ready: true, reason: "ready" })),
  runningStyleReadinessMock: vi.fn<
    () => Promise<Array<{ race: Record<string, never>; reason: string | null }>>
  >(async () => [{ race: {}, reason: null }]),
}));

vi.mock("./day-base-race-materializer", () => ({
  buildDayBaseRaceFoundationKey: (
    category: string,
    runYmd: string,
    venueCode: string,
    raceNumber: string,
  ): string => `foundation/${category}/${runYmd}/${venueCode}/${raceNumber}`,
  getDayBaseRaceFoundationReadiness: foundationReadinessMock,
}));

vi.mock("./running-style-readiness", () => ({
  getRunningStyleRaceReadiness: runningStyleReadinessMock,
}));

import { getRaceScopedDayBaseReadiness } from "./race-scoped-day-base-readiness";
import { fetchCatalogRaceSourceSnapshots } from "./race-source-snapshot";

const ENTRY_SET_HASH = "fa32fa947945769fbed1777051008a62da266529d53ed7e3a54102d3bab850c7";
const catalogFetchMock = vi.fn<(request: Request) => Promise<Response>>();
const foundationGetMock = vi.fn<() => Promise<R2ObjectBody | null>>();
const runningStyleAllMock = vi.fn<() => Promise<{ results: Record<string, unknown>[] }>>();
const realtimeBindMock = vi.fn(() => ({ all: runningStyleAllMock }));
const realtimePrepareMock = vi.fn(() => ({ bind: realtimeBindMock }));

const featureRow = (ketto: string, umaban: number, pNige: number) => ({
  ketto_toroku_bango: ketto,
  rs_p_nige: pNige,
  rs_p_oikomi: 0.1,
  rs_p_sashi: 0.2,
  rs_p_senkou: 0.3,
  rs_predicted_class: 0,
  umaban,
});

const catalogRow = (ketto: string, umaban: number, category: "ban-ei" | "jra" | "nar" = "nar") => ({
  babajotai_code_dirt: "1",
  babajotai_code_shiba: null,
  bamei: `Horse ${String(umaban)}`,
  banushimei: "Owner",
  barei: 4,
  chokyoshimei_ryakusho: "Trainer",
  futan_juryo: 56,
  grade_code: null,
  hasso_jikoku: "1605",
  juryo_shubetsu_code: "1",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0910",
  keibajo_code: category === "ban-ei" ? "83" : "50",
  ketto_toroku_bango: ketto,
  kishumei_ryakusho: "Jockey",
  kyori: 1400,
  kyoso_joken_code: "010",
  kyoso_shubetsu_code: "1",
  race_bango: "11",
  race_date: "20260910",
  race_name: "Race",
  seibetsu_code: "1",
  shusso_tosu: 2,
  source: category === "jra" ? "jra" : "nar",
  track_code: "24",
  umaban,
  wakuban: String(umaban),
});

const databaseRow = (ketto: string, umaban: number, pNige: number) => ({
  ketto_toroku_bango: ketto,
  p_nige: pNige,
  p_oikomi: 0.1,
  p_sashi: 0.2,
  p_senkou: 0.3,
  predicted_class: 0,
  umaban,
});

const foundationObject = (
  rows: unknown[] = [featureRow("horse-b", 2, 0.4), featureRow("horse-a", 1, 0.4)],
  metadata: Record<string, string> = {
    "entry-set-hash": ENTRY_SET_HASH,
    "row-count": "2",
  },
): R2ObjectBody =>
  ({ customMetadata: metadata, json: vi.fn(async () => ({ rows })) }) as unknown as R2ObjectBody;

const makeEnv = (): Env =>
  ({
    FEATURES_CACHE: { get: foundationGetMock },
    PC_KEIBA_R2_CATALOG: { fetch: catalogFetchMock },
    REALTIME_DB: { prepare: realtimePrepareMock },
  }) as unknown as Env;

const params = (env: Env, category: "ban-ei" | "jra" | "nar" = "nar") => ({
  category,
  env,
  keibajoCode: category === "ban-ei" ? "83" : "50",
  raceBango: "11",
  runYmd: "20260910",
});

beforeEach(() => {
  foundationReadinessMock.mockReset();
  foundationReadinessMock.mockResolvedValue({ ready: true, reason: "ready" });
  runningStyleReadinessMock.mockReset();
  runningStyleReadinessMock.mockResolvedValue([{ race: {}, reason: null }]);
  foundationGetMock.mockReset();
  foundationGetMock.mockResolvedValue(foundationObject());
  catalogFetchMock.mockReset();
  catalogFetchMock.mockImplementation(async () =>
    Response.json({
      rows: [catalogRow("horse-b", 2), catalogRow("horse-a", 1)],
    }),
  );
  runningStyleAllMock.mockReset();
  runningStyleAllMock.mockResolvedValue({
    results: [databaseRow("horse-a", 1, 0.4), databaseRow("horse-b", 2, 0.4)],
  });
  realtimeBindMock.mockClear();
  realtimePrepareMock.mockClear();
});

test("accepts a race only when its attested RS feature values are unchanged", async () => {
  const env = makeEnv();
  await expect(getRaceScopedDayBaseReadiness(params(env))).resolves.toStrictEqual({
    ready: true,
    reason: "ready",
  });
  expect(foundationReadinessMock).toHaveBeenCalledWith({
    category: "nar",
    env,
    raceNumber: "11",
    runYmd: "20260910",
    venueCode: "50",
  });
  expect(foundationGetMock).toHaveBeenCalledWith("foundation/nar/20260910/50/11");
  expect(catalogFetchMock.mock.calls[0]?.[0]?.url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/race-features?date=20260910&source=nar&keibajoCode=50&raceBango=11",
  );
  expect(realtimeBindMock).toHaveBeenCalledWith("nar:20260910:50:11");
});

test("rejects a target race update even when its timestamp is below another race's category max", async () => {
  runningStyleAllMock.mockResolvedValueOnce({
    results: [databaseRow("horse-a", 1, 0.4), databaseRow("horse-b", 2, 0.41)],
  });
  await expect(getRaceScopedDayBaseReadiness(params(makeEnv()))).resolves.toStrictEqual({
    ready: false,
    reason: "race-running-style-features-mismatch",
  });
});

test("returns the per-race attestation failure without live probes", async () => {
  foundationReadinessMock.mockResolvedValueOnce({ ready: false, reason: "foundation-miss" });
  await expect(getRaceScopedDayBaseReadiness(params(makeEnv()))).resolves.toStrictEqual({
    ready: false,
    reason: "foundation-miss",
  });
  expect(catalogFetchMock).not.toHaveBeenCalled();
});

test("fails closed when the Catalog binding or response is unavailable", async () => {
  const missingCatalog = { ...makeEnv(), PC_KEIBA_R2_CATALOG: undefined };
  await expect(getRaceScopedDayBaseReadiness(params(missingCatalog))).rejects.toThrow(
    "PC_KEIBA_R2_CATALOG binding is unavailable",
  );
  catalogFetchMock.mockResolvedValueOnce(new Response("unavailable", { status: 503 }));
  await expect(getRaceScopedDayBaseReadiness(params(makeEnv()))).rejects.toThrow(
    "Catalog race source snapshot failed with HTTP 503",
  );
  catalogFetchMock.mockResolvedValueOnce(Response.json({ rows: "invalid" }));
  await expect(getRaceScopedDayBaseReadiness(params(makeEnv()))).rejects.toThrow(
    "Catalog race source snapshot returned invalid rows",
  );
});

test("fails closed on empty or malformed Catalog entries", async () => {
  for (const rows of [[], [null], [{ ketto_toroku_bango: "", umaban: 0 }]]) {
    catalogFetchMock.mockResolvedValueOnce(Response.json({ rows }));
    await expect(getRaceScopedDayBaseReadiness(params(makeEnv()))).rejects.toThrow("catalog-row");
  }
});

test("rejects a disappeared foundation and invalid metadata", async () => {
  foundationGetMock.mockResolvedValueOnce(null);
  await expect(getRaceScopedDayBaseReadiness(params(makeEnv()))).resolves.toStrictEqual({
    ready: false,
    reason: "race-foundation-disappeared",
  });
  for (const metadata of [
    { "entry-set-hash": "", "row-count": "2" },
    { "entry-set-hash": "abc", "row-count": "2" },
    { "entry-set-hash": ENTRY_SET_HASH, "row-count": "0" },
  ]) {
    foundationGetMock.mockResolvedValueOnce(foundationObject([], metadata));
    await expect(getRaceScopedDayBaseReadiness(params(makeEnv()))).resolves.toStrictEqual({
      ready: false,
      reason: "race-foundation-metadata-invalid",
    });
  }
});

test("rejects a changed entrant count or identity set", async () => {
  foundationGetMock.mockResolvedValueOnce(
    foundationObject([], { "entry-set-hash": ENTRY_SET_HASH, "row-count": "3" }),
  );
  await expect(getRaceScopedDayBaseReadiness(params(makeEnv()))).resolves.toStrictEqual({
    ready: false,
    reason: "race-entry-set-mismatch",
  });
  foundationGetMock.mockResolvedValueOnce(
    foundationObject([], { "entry-set-hash": "a".repeat(64), "row-count": "2" }),
  );
  await expect(getRaceScopedDayBaseReadiness(params(makeEnv()))).resolves.toStrictEqual({
    ready: false,
    reason: "race-entry-set-mismatch",
  });
});

test("accepts a matching stable source hash for source-watermark fallback", async () => {
  const snapshots = await fetchCatalogRaceSourceSnapshots({
    catalog: {
      fetch: vi.fn(async () =>
        Response.json({ rows: [catalogRow("horse-b", 2), catalogRow("horse-a", 1)] }),
      ),
    },
    category: "nar",
    raceBango: "11",
    runYmd: "20260910",
    venueCode: "50",
  });
  const stableSourceHash = snapshots.get("nar:2026:0910:50:11")?.stableSourceHash;
  if (stableSourceHash === undefined) throw new Error("expected stable source hash");
  foundationGetMock.mockResolvedValueOnce(
    foundationObject(undefined, {
      "catalog-source-hash": stableSourceHash,
      "entry-set-hash": ENTRY_SET_HASH,
      "row-count": "2",
    }),
  );

  await expect(
    getRaceScopedDayBaseReadiness({ ...params(makeEnv()), requireStableSourceHash: true }),
  ).resolves.toStrictEqual({ ready: true, reason: "ready" });
});

test("requires and compares a stable source hash for source-watermark fallback", async () => {
  await expect(
    getRaceScopedDayBaseReadiness({ ...params(makeEnv()), requireStableSourceHash: true }),
  ).resolves.toStrictEqual({
    ready: false,
    reason: "race-catalog-source-hash-missing",
  });
  foundationGetMock.mockResolvedValueOnce(
    foundationObject(undefined, {
      "catalog-source-hash": "stale-source-hash",
      "entry-set-hash": ENTRY_SET_HASH,
      "row-count": "2",
    }),
  );
  await expect(getRaceScopedDayBaseReadiness(params(makeEnv()))).resolves.toStrictEqual({
    ready: false,
    reason: "race-catalog-source-mismatch",
  });
});

test("requires the target race running-style generation to remain complete", async () => {
  runningStyleReadinessMock.mockResolvedValueOnce([
    { race: {}, reason: "prediction-count-1-of-2" },
  ]);
  await expect(getRaceScopedDayBaseReadiness(params(makeEnv()))).resolves.toStrictEqual({
    ready: false,
    reason: "race-running-style-prediction-count-1-of-2",
  });
  runningStyleReadinessMock.mockResolvedValueOnce([]);
  await expect(getRaceScopedDayBaseReadiness(params(makeEnv()))).resolves.toStrictEqual({
    ready: false,
    reason: "race-running-style-state-missing",
  });
});

test("rejects malformed or differently sized RS feature rows", async () => {
  foundationGetMock.mockResolvedValueOnce(foundationObject([{ rs_p_nige: null }]));
  await expect(getRaceScopedDayBaseReadiness(params(makeEnv()))).resolves.toStrictEqual({
    ready: false,
    reason: "race-running-style-features-invalid",
  });
  runningStyleAllMock.mockResolvedValueOnce({ results: [databaseRow("horse-a", 1, 0.4)] });
  await expect(getRaceScopedDayBaseReadiness(params(makeEnv()))).resolves.toStrictEqual({
    ready: false,
    reason: "race-rs-row-count-1-of-2",
  });
  runningStyleAllMock.mockResolvedValueOnce({ results: [] });
  await expect(getRaceScopedDayBaseReadiness(params(makeEnv()))).resolves.toStrictEqual({
    ready: false,
    reason: "race-running-style-features-invalid",
  });
});

test("does not require running-style features for Ban-ei", async () => {
  foundationGetMock.mockResolvedValueOnce(foundationObject([]));
  catalogFetchMock.mockResolvedValueOnce(
    Response.json({
      rows: [catalogRow("horse-b", 2, "ban-ei"), catalogRow("horse-a", 1, "ban-ei")],
    }),
  );
  await expect(getRaceScopedDayBaseReadiness(params(makeEnv(), "ban-ei"))).resolves.toStrictEqual({
    ready: true,
    reason: "ready",
  });
  expect(runningStyleReadinessMock).not.toHaveBeenCalled();
  expect(runningStyleAllMock).not.toHaveBeenCalled();
});
