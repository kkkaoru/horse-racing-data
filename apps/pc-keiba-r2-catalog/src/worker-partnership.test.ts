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
    raw: { jvd_se: { dataPrefix: "raw/", snapshotId: revision, partitions: {} } },
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
  expect(test.dependencies.fetchImpl).toHaveBeenCalledTimes(11);
  expect((await handleRequest(new Request(url), test.env, test.dependencies)).status).toBe(200);
  expect(test.dependencies.fetchImpl).toHaveBeenCalledTimes(12);
});

it("returns a cache-miss status without running history aggregation on reads", async () => {
  const test = harness("cold-endpoint");
  const response = await handleRequest(new Request(url), test.env, test.dependencies);
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "partnership_not_warmed" });
  expect(test.dependencies.fetchImpl).toHaveBeenCalledTimes(1);
});

it("requires a source snapshot for cache validity", async () => {
  const test = harness("missing-source");
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
