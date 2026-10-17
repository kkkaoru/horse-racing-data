// Run with bun (bunx vitest).
import { expect, it, vi } from "vitest";
import type { Env, WorkerDependencies } from "./types";
import { handleRequest } from "./worker";

interface Harness {
  env: Env;
  dependencies: WorkerDependencies;
}
const url: string =
  "https://catalog.test/v1/heatmap-partnership-stats?year=2026&month=09&day=13&keibajoCode=06&raceNumber=01&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=1";
const harness = (revision: string): Harness => {
  const body: string = JSON.stringify({
    version: 1,
    raw: {
      nvd_se: { dataPrefix: "raw/", snapshotId: revision, partitions: {} },
      jvd_se: { dataPrefix: "raw/", snapshotId: revision, partitions: {} },
    },
    history: { dataPrefix: "history/", snapshotId: revision, partitions: {} },
  });
  return {
    env: {
      R2_SQL_ACCOUNT_ID: "account",
      R2_SQL_BUCKET_NAME: "bucket",
      R2_SQL_NAMESPACE: "ns",
      R2_SQL_TOKEN: "test",
      CATALOG_KV: {
        get: vi.fn(async () => null),
        put: vi.fn(async () => undefined),
        delete: vi.fn(async () => undefined),
      },
      CATALOG_OBJECTS: {
        get: vi.fn(async () => ({ body: new Blob([body]).stream(), size: body.length })),
      },
    },
    dependencies: {
      cache: {
        match: vi.fn(async () => undefined),
        put: vi.fn(async () => undefined),
        delete: vi.fn(async () => true),
      },
      fetchImpl: vi.fn(async (_input, init) => {
        const sql = String(init?.body);
        return Response.json({
          success: true,
          result: {
            rows: sql.includes("AS horse_id")
              ? [
                  {
                    surface: "芝",
                    umaban: "01",
                    horse_id: "2023100001",
                    jockey_id: "j1",
                    trainer_id: "t1",
                    horse_name: "Horse",
                    jockey_name: "Jockey",
                    trainer_name: "Trainer",
                  },
                ]
              : [],
          },
        });
      }),
    },
  };
};

it("warms shared cohorts and exposes all three independent column rows", async () => {
  const test = harness("warm-endpoint");
  const response = await handleRequest(new Request(`${url}&warm=1`), test.env, test.dependencies);
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    partnershipRows: [
      {
        kind: "horseJockey",
        umaban: 1,
        name: "Horse × Jockey",
        starts: 0,
        wins: 0,
        places: 0,
        shows: 0,
      },
      { kind: "jockeyVenue", umaban: 1, name: "Jockey", starts: 0, wins: 0, places: 0, shows: 0 },
      {
        kind: "jockeyTrainerVenue",
        umaban: 1,
        name: "Jockey × Trainer",
        starts: 0,
        wins: 0,
        places: 0,
        shows: 0,
      },
    ],
  });
  // One entries query plus one counts query per kind; no target-race scans.
  expect(test.dependencies.fetchImpl).toHaveBeenCalledTimes(6);
  expect((await handleRequest(new Request(url), test.env, test.dependencies)).status).toBe(200);
  expect(test.dependencies.fetchImpl).toHaveBeenCalledTimes(7);
});

it("warms overseas partnership cohorts through URL parsing and all SQL scopes", async () => {
  const test = harness("overseas-warm-endpoint");
  const overseasUrl = new URL(url);
  overseasUrl.searchParams.set("keibajoCode", "A8");
  overseasUrl.searchParams.set("warm", "1");
  const response = await handleRequest(new Request(overseasUrl), test.env, test.dependencies);
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    partnershipRows: [
      {
        kind: "horseJockey",
        umaban: 1,
        name: "Horse × Jockey",
        starts: 0,
        wins: 0,
        places: 0,
        shows: 0,
      },
      { kind: "jockeyVenue", umaban: 1, name: "Jockey", starts: 0, wins: 0, places: 0, shows: 0 },
      {
        kind: "jockeyTrainerVenue",
        umaban: 1,
        name: "Jockey × Trainer",
        starts: 0,
        wins: 0,
        places: 0,
        shows: 0,
      },
    ],
  });
  expect(test.dependencies.fetchImpl).toHaveBeenCalledTimes(8);
  expect(test.dependencies.fetchImpl).toHaveBeenCalledWith(
    expect.anything(),
    expect.objectContaining({ body: expect.stringMatching("keibajo_code = 'A8'") }),
  );
  overseasUrl.searchParams.delete("warm");
  expect((await handleRequest(new Request(overseasUrl), test.env, test.dependencies)).status).toBe(
    200,
  );
  expect(test.dependencies.fetchImpl).toHaveBeenCalledTimes(11);
});

it("returns a cache-miss status without running history aggregation on reads", async () => {
  const test = harness("cold-endpoint");
  const response = await handleRequest(new Request(url), test.env, test.dependencies);
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "partnership_not_warmed" });
  expect(test.dependencies.fetchImpl).toHaveBeenCalledTimes(1);
});

it("requires a source snapshot for cache validity", async () => {
  const test = harness("");
  expect(
    (
      await handleRequest(
        new Request(url.replace("source=jra", "source=nar")),
        test.env,
        test.dependencies,
      )
    ).status,
  ).toBe(503);
  expect(
    (
      await handleRequest(
        new Request(url),
        { ...test.env, CATALOG_OBJECTS: undefined },
        test.dependencies,
      )
    ).status,
  ).toBe(503);
  expect(test.dependencies.fetchImpl).not.toHaveBeenCalled();
});

it.each(["jvd_se", "nvd_se"])("invalidates warmed cohorts when %s changes", async (table) => {
  const test = harness(`both-sources-${table}`);
  expect(
    (await handleRequest(new Request(`${url}&warm=1`), test.env, test.dependencies)).status,
  ).toBe(200);
  const revision = `both-sources-${table}`;
  const manifest = JSON.stringify({
    version: 1,
    raw: {
      jvd_se: { dataPrefix: "raw/", snapshotId: revision, partitions: {} },
      nvd_se: { dataPrefix: "raw/", snapshotId: revision, partitions: {} },
      [table]: { dataPrefix: "raw/", snapshotId: `${revision}-changed`, partitions: {} },
    },
    history: { dataPrefix: "history/", snapshotId: revision, partitions: {} },
  });
  test.env.CATALOG_OBJECTS = {
    get: vi.fn(async () => ({ body: new Blob([manifest]).stream(), size: manifest.length })),
  };
  expect((await handleRequest(new Request(url), test.env, test.dependencies)).status).toBe(503);
  expect(test.dependencies.cache.match).toHaveBeenCalledWith(
    expect.objectContaining({ url: expect.stringMatching(/partnership-unknown-id-v1/) }),
  );
});

it.each(["jvd_se", "nvd_se"])("fails closed without required %s metadata", async (table) => {
  const test = harness("missing-required");
  const body = JSON.stringify({
    version: 1,
    raw: {
      [table]: { dataPrefix: "raw/", snapshotId: "present", partitions: {} },
    },
    history: { dataPrefix: "history/", snapshotId: "history", partitions: {} },
  });
  test.env.CATALOG_OBJECTS = {
    get: vi.fn(async () => ({ body: new Blob([body]).stream(), size: body.length })),
  };
  const response = await handleRequest(new Request(url), test.env, test.dependencies);
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "partnership_snapshot_unavailable" });
  expect(test.dependencies.fetchImpl).not.toHaveBeenCalled();
});

it("fences foreign cohorts with authoritative current content at unchanged snapshots", async () => {
  const test = harness("foreign-content");
  const originalFetch = test.dependencies.fetchImpl;
  const profile = {
    race_source: "jra",
    source: "netkeiba",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0913",
    keibajo_code: "A8",
    race_bango: "05",
    umaban: "01",
    source_horse_id: "000a029d42",
    source_jockey_id: "05504",
    source_trainer_id: "05701",
    source_owner_id: "a002e7",
    sire_source_id: "",
    sire_name: "",
    sire_sire_source_id: "",
    sire_sire_name: "",
    dam_source_id: "",
    dam_name: "",
    dam_sire_source_id: "",
    dam_sire_name: "",
  };
  test.dependencies.fetchImpl = async (input, init) =>
    String(init?.body).includes("SELECT m.race_source")
      ? Response.json({ success: true, result: { rows: [profile] } })
      : originalFetch(input, init);
  const foreignUrl = url.replace("keibajoCode=06", "keibajoCode=A8");
  expect(
    (await handleRequest(new Request(`${foreignUrl}&warm=1`), test.env, test.dependencies)).status,
  ).toBe(200);
  expect((await handleRequest(new Request(foreignUrl), test.env, test.dependencies)).status).toBe(
    200,
  );
  profile.source_owner_id = "a002e8";
  expect((await handleRequest(new Request(foreignUrl), test.env, test.dependencies)).status).toBe(
    503,
  );
});
