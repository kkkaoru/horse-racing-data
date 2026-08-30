// Run with bun. Tests for the fail-closed discovery barrier before day-base generation.

import { beforeEach, expect, test, vi } from "vitest";

const { getRunningStyleRaceReadinessMock } = vi.hoisted(() => ({
  getRunningStyleRaceReadinessMock: vi.fn(
    async (params: {
      races: readonly { category: string; keibajoCode: string; raceBango: string }[];
    }) => params.races.map((race) => ({ race, reason: null as string | null })),
  ),
}));

vi.mock("./running-style-readiness", () => ({
  getRunningStyleRaceReadiness: getRunningStyleRaceReadinessMock,
}));

import { getDayBaseDiscoveryReadiness } from "./day-base-discovery-readiness";
import type { Env } from "./types";

beforeEach(() => {
  getRunningStyleRaceReadinessMock.mockClear();
});

const raceRows = [
  { keibajo_code: "01", race_bango: "01", source: "jra" },
  { keibajo_code: "04", race_bango: "01", source: "jra" },
  { keibajo_code: "35", race_bango: "01", source: "nar" },
  { keibajo_code: "55", race_bango: "01", source: "nar" },
  { keibajo_code: "83", race_bango: "01", source: "nar" },
];

const makeEnv = (
  options: {
    catalogResponse?: Response;
    discoveredCount?: number | null;
  } = {},
): {
  env: Env;
  bind: ReturnType<typeof vi.fn>;
  fetch: ReturnType<typeof vi.fn>;
  prepare: ReturnType<typeof vi.fn>;
} => {
  const fetch = vi.fn(async () => options.catalogResponse ?? Response.json({ rows: raceRows }));
  const first = vi.fn(async () =>
    options.discoveredCount === null
      ? null
      : { race_count: options.discoveredCount === undefined ? 2 : options.discoveredCount },
  );
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  return {
    env: {
      PC_KEIBA_R2_CATALOG: { fetch },
      REALTIME_DB: { prepare },
    } as unknown as Env,
    bind,
    fetch,
    prepare,
  };
};

test("accepts JRA only after D1 contains every authoritative Catalog race", async () => {
  const { env, bind, fetch, prepare } = makeEnv({ discoveredCount: 2 });

  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env, runYmd: "20260830" }),
  ).resolves.toStrictEqual({ ready: true, reason: "ready" });

  expect(fetch).toHaveBeenCalledWith(
    new Request("https://pc-keiba-r2-catalog.internal/v1/race-keys?date=20260830"),
  );
  expect(prepare).toHaveBeenCalledWith(expect.stringContaining("source = 'jra'"));
  expect(bind).toHaveBeenCalledWith("2026", "0830");
});

test("rejects a partial JRA discovery before a Container can start", async () => {
  const { env } = makeEnv({ discoveredCount: 1 });

  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env, runYmd: "20260830" }),
  ).resolves.toStrictEqual({ ready: false, reason: "discovery-race-count-1-of-2" });
  expect(getRunningStyleRaceReadinessMock).not.toHaveBeenCalled();
});

test("rejects complete discovery until running-style inference covers every race", async () => {
  const { env } = makeEnv({ discoveredCount: 2 });
  getRunningStyleRaceReadinessMock.mockResolvedValueOnce([
    {
      race: { category: "jra", keibajoCode: "01", raceBango: "01" },
      reason: null,
    },
    {
      race: { category: "jra", keibajoCode: "04", raceBango: "01" },
      reason: "state-missing",
    },
  ]);

  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env, runYmd: "20260830" }),
  ).resolves.toStrictEqual({ ready: false, reason: "running-style-race-count-1-of-2" });
});

test("separates ordinary NAR races from ban-ei venue 83", async () => {
  const nar = makeEnv({ discoveredCount: 2 });
  await expect(
    getDayBaseDiscoveryReadiness({ category: "nar", env: nar.env, runYmd: "20260830" }),
  ).resolves.toStrictEqual({ ready: true, reason: "ready" });
  expect(nar.prepare).toHaveBeenCalledWith(expect.stringContaining("<> '83'"));

  const banEi = makeEnv({ discoveredCount: 1 });
  await expect(
    getDayBaseDiscoveryReadiness({ category: "ban-ei", env: banEi.env, runYmd: "20260830" }),
  ).resolves.toStrictEqual({ ready: true, reason: "ready" });
  expect(banEi.prepare).toHaveBeenCalledWith(expect.stringContaining("= '83'"));
});

test("does not query D1 when the Catalog has no races for the category", async () => {
  const { env, prepare } = makeEnv({ catalogResponse: Response.json({ rows: [] }) });

  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env, runYmd: "20260830" }),
  ).resolves.toStrictEqual({ ready: false, reason: "catalog-races-empty" });
  expect(prepare).not.toHaveBeenCalled();
});

test("fails closed when the Catalog binding or response is unavailable", async () => {
  const missingBinding = makeEnv().env;
  missingBinding.PC_KEIBA_R2_CATALOG = undefined;
  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env: missingBinding, runYmd: "20260830" }),
  ).rejects.toThrow("PC_KEIBA_R2_CATALOG binding is unavailable");

  const failedResponse = makeEnv({ catalogResponse: new Response("down", { status: 503 }) });
  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env: failedResponse.env, runYmd: "20260830" }),
  ).rejects.toThrow("Catalog discovery readiness failed with HTTP 503");
});

test("fails closed for malformed Catalog payloads and race keys", async () => {
  const malformedPayload = makeEnv({ catalogResponse: Response.json({ rows: "invalid" }) });
  await expect(
    getDayBaseDiscoveryReadiness({
      category: "jra",
      env: malformedPayload.env,
      runYmd: "20260830",
    }),
  ).rejects.toThrow("Catalog discovery readiness returned invalid rows");

  const malformedRow = makeEnv({ catalogResponse: Response.json({ rows: [{ source: "jra" }] }) });
  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env: malformedRow.env, runYmd: "20260830" }),
  ).rejects.toThrow("Catalog race-keys returned an invalid row");

  const nonRecordRow = makeEnv({ catalogResponse: Response.json({ rows: [null] }) });
  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env: nonRecordRow.env, runYmd: "20260830" }),
  ).rejects.toThrow("Catalog race-keys returned an invalid row");
});

test("fails closed for missing and invalid D1 counts", async () => {
  const missing = makeEnv({ discoveredCount: null });
  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env: missing.env, runYmd: "20260830" }),
  ).rejects.toThrow("D1 discovery readiness returned an invalid race count");

  const negative = makeEnv({ discoveredCount: -1 });
  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env: negative.env, runYmd: "20260830" }),
  ).rejects.toThrow("D1 discovery readiness returned an invalid race count");
});
