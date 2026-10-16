// Run with bun. Tests for the fail-closed discovery barrier before day-base generation.

import { expect, test, vi } from "vitest";

import { getDayBaseDiscoveryReadiness } from "./day-base-discovery-readiness";
import type { Env } from "./types";

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
    entryResponse?: Response;
  } = {},
): {
  env: Env;
  bind: ReturnType<typeof vi.fn>;
  fetch: ReturnType<typeof vi.fn>;
  prepare: ReturnType<typeof vi.fn>;
} => {
  const fetch = vi.fn(async (request: Request) =>
    new URL(request.url).pathname === "/v1/race-keys"
      ? (options.catalogResponse ?? Response.json({ rows: raceRows }))
      : (options.entryResponse ??
        Response.json({
          date: "20260830",
          source: new URL(request.url).searchParams.get("source"),
          entries: raceRows
            .filter((row) =>
              new URL(request.url).searchParams.get("source") === "jra"
                ? row.source === "jra"
                : new URL(request.url).searchParams.get("source") === "ban-ei"
                  ? row.keibajo_code === "83"
                  : row.source === "nar" && row.keibajo_code !== "83",
            )
            .map((row) => ({
              source: new URL(request.url).searchParams.get("source"),
              keibajoCode: row.keibajo_code,
              raceBango: row.race_bango,
              kettoTorokuBango: "0000000001",
              umaban: 1,
            })),
        })),
  );
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
      FINISH_POSITION_ATTESTATION_TOKEN: "test-attestation-token",
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
    expect.objectContaining({
      method: "GET",
      url: "https://pc-keiba-r2-catalog.internal/v1/race-keys?date=20260830",
    }),
  );
  expect(prepare).toHaveBeenCalledWith(expect.stringContaining("source = 'jra'"));
  expect(bind).toHaveBeenCalledWith("2026", "0830");
  expect(fetch).toHaveBeenCalledWith(
    expect.objectContaining({
      url: "https://pc-keiba-r2-catalog.internal/v1/internal/fresh-race-entries-bulk?date=20260830&source=jra",
    }),
  );
});

test("does not wait for overseas sales races in the domestic JRA foundation", async () => {
  const { env, prepare } = makeEnv({
    catalogResponse: Response.json({
      rows: [...raceRows, { source: "jra", keibajo_code: "A8", race_bango: "05" }],
    }),
    discoveredCount: 2,
  });

  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env, runYmd: "20260830" }),
  ).resolves.toStrictEqual({ ready: true, reason: "ready" });
  expect(prepare).toHaveBeenCalledWith(
    expect.stringContaining("source = 'jra' and keibajo_code not glob '[A-Z]*'"),
  );
});

test("does not start a domestic JRA foundation for an overseas-only catalog", async () => {
  const { env } = makeEnv({
    catalogResponse: Response.json({
      rows: [{ source: "jra", keibajo_code: "A8", race_bango: "05" }],
    }),
    discoveredCount: 1,
  });

  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env, runYmd: "20260830" }),
  ).resolves.toStrictEqual({ ready: false, reason: "catalog-races-empty" });
});

test("rejects a partial JRA discovery before a Container can start", async () => {
  const { env } = makeEnv({ discoveredCount: 1 });

  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env, runYmd: "20260830" }),
  ).resolves.toStrictEqual({ ready: false, reason: "discovery-race-count-1-of-2" });
});

test("defers a Container when Catalog has races but no runners yet", async () => {
  const { env } = makeEnv({ entryResponse: new Response("empty", { status: 502 }) });

  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env, runYmd: "20260830" }),
  ).rejects.toThrow("Catalog bulk readiness entries failed with HTTP 502");
});

test("defers a Container when one race has no active runners yet", async () => {
  const { env } = makeEnv({
    entryResponse: Response.json({
      date: "20260830",
      source: "jra",
      entries: [
        {
          source: "jra",
          keibajoCode: "01",
          raceBango: "01",
          kettoTorokuBango: "0000000001",
          umaban: 1,
        },
      ],
    }),
  });

  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env, runYmd: "20260830" }),
  ).resolves.toStrictEqual({ ready: false, reason: "catalog-race-entries-incomplete" });
});

test("accepts complete discovery without circularly requiring running-style inference", async () => {
  const { env } = makeEnv({ discoveredCount: 2 });

  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env, runYmd: "20260830" }),
  ).resolves.toStrictEqual({ ready: true, reason: "ready" });
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

test("avoids Catalog queries when D1 discovery has no races", async () => {
  const { env, fetch, prepare } = makeEnv({ discoveredCount: 0 });

  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env, runYmd: "20260830" }),
  ).resolves.toStrictEqual({ ready: false, reason: "discovery-races-empty" });
  expect(prepare).toHaveBeenCalledOnce();
  expect(fetch).not.toHaveBeenCalled();
});

test("fails closed when Catalog has no races but D1 discovery has races", async () => {
  const { env, prepare } = makeEnv({ catalogResponse: Response.json({ rows: [] }) });

  await expect(
    getDayBaseDiscoveryReadiness({ category: "jra", env, runYmd: "20260830" }),
  ).resolves.toStrictEqual({ ready: false, reason: "catalog-races-empty" });
  expect(prepare).toHaveBeenCalledOnce();
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
