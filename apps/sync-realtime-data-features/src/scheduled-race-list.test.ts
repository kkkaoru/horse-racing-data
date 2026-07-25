// Run with: bun run --filter sync-realtime-data-features test
import { expect, it, vi } from "vitest";

import {
  listTodayRaceKeysFromCatalog,
  listTodayRaceKeysWithKvCache,
  listTomorrowRaceKeysFromCatalog,
  listTomorrowRaceKeysWithKvCache,
  type ScheduledRaceListEnv,
  toRaceJobKeyFromTodayRaceKey,
} from "./scheduled-race-list";
import type { CatalogServiceBinding, Env } from "./types";

interface BuildKvEnvArgs {
  fetch: CatalogServiceBinding["fetch"];
  jraCached: string | null;
  narCached: string | null;
}

const buildEnv = (fetch: CatalogServiceBinding["fetch"]): Pick<Env, "PC_KEIBA_R2_CATALOG"> => ({
  PC_KEIBA_R2_CATALOG: { fetch },
});

const buildKvEnv = (
  args: BuildKvEnvArgs,
): { env: ScheduledRaceListEnv; kv: ScheduledRaceListEnv["FEATURES_KV"] } => {
  const get = vi.fn(async (key: string): Promise<string | null> => {
    if (
      key === "race-keys:catalog-v1:jra:20260529" ||
      key === "race-keys:catalog-v1:jra:20260530"
    ) {
      return args.jraCached;
    }
    if (
      key === "race-keys:catalog-v1:nar:20260529" ||
      key === "race-keys:catalog-v1:nar:20260530"
    ) {
      return args.narCached;
    }
    return null;
  });
  const put = vi.fn(
    async (_key: string, _value: string, _options: { expirationTtl: number }): Promise<void> =>
      undefined,
  );
  const kv = { get, put };
  return {
    env: {
      FEATURES_KV: kv,
      PC_KEIBA_R2_CATALOG: { fetch: args.fetch },
    },
    kv,
  };
};

it("listTodayRaceKeysFromCatalog requests the date and maps JRA, NAR, and Ban-ei rows", async () => {
  const requests: Request[] = [];
  const fetch = vi.fn(async (request: Request): Promise<Response> => {
    requests.push(request);
    return Response.json({
      rows: [
        {
          kaisaiNen: "2026",
          kaisaiTsukihi: "0529",
          keibajoCode: "05",
          raceBango: "01",
          raceKey: "jra:2026:0529:05:01",
          source: "jra",
        },
        {
          kaisaiNen: "2026",
          kaisaiTsukihi: "0529",
          keibajoCode: "30",
          raceBango: "08",
          raceKey: "nar:2026:0529:30:08",
          source: "nar",
        },
        {
          kaisaiNen: "2026",
          kaisaiTsukihi: "0529",
          keibajoCode: "83",
          raceBango: "11",
          raceKey: "nar:2026:0529:83:11",
          source: "nar",
        },
      ],
    });
  });
  const rows = await listTodayRaceKeysFromCatalog(buildEnv(fetch), "20260529");
  expect(requests[0]?.url).toBe("https://pc-keiba-r2-catalog/v1/race-keys?date=20260529");
  expect(rows).toStrictEqual([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "05",
      raceBango: "01",
      raceKey: "jra:2026:0529:05:01",
      source: "jra",
    },
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      source: "nar",
    },
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "83",
      raceBango: "11",
      raceKey: "nar:2026:0529:83:11",
      source: "nar",
    },
  ]);
});

it("listTodayRaceKeysFromCatalog uses an injected catalog binding", async () => {
  const envFetch = vi.fn();
  const contextFetch = vi.fn(async (): Promise<Response> => Response.json({ rows: [] }));
  const rows = await listTodayRaceKeysFromCatalog(buildEnv(envFetch), "20260529", {
    catalog: { fetch: contextFetch },
  });
  expect(envFetch).not.toHaveBeenCalled();
  expect(contextFetch).toHaveBeenCalledTimes(1);
  expect(rows).toStrictEqual([]);
});

it("listTodayRaceKeysFromCatalog skips malformed rows", async () => {
  const fetch = vi.fn(
    async (): Promise<Response> =>
      Response.json({
        rows: [
          null,
          [],
          {
            kaisaiNen: "2026",
            kaisaiTsukihi: "0529",
            keibajoCode: "05",
            raceBango: "01",
            raceKey: "unknown:2026:0529:05:01",
            source: "unknown",
          },
          {
            kaisaiNen: null,
            kaisaiTsukihi: "0529",
            keibajoCode: "05",
            raceBango: "01",
            raceKey: "jra:2026:0529:05:01",
            source: "jra",
          },
          {
            kaisaiNen: "2026",
            kaisaiTsukihi: 529,
            keibajoCode: "05",
            raceBango: "01",
            raceKey: "jra:2026:0529:05:01",
            source: "jra",
          },
          {
            kaisaiNen: "2026",
            kaisaiTsukihi: "0529",
            keibajoCode: null,
            raceBango: "01",
            raceKey: "jra:2026:0529:05:01",
            source: "jra",
          },
          {
            kaisaiNen: "2026",
            kaisaiTsukihi: "0529",
            keibajoCode: "05",
            raceBango: null,
            raceKey: "jra:2026:0529:05:01",
            source: "jra",
          },
          {
            kaisaiNen: "2026",
            kaisaiTsukihi: "0529",
            keibajoCode: "05",
            raceBango: "01",
            source: "jra",
          },
        ],
      }),
  );
  const rows = await listTodayRaceKeysFromCatalog(buildEnv(fetch), "20260529");
  expect(rows).toStrictEqual([]);
});

it("listTomorrowRaceKeysFromCatalog requests tomorrow in JST", async () => {
  const requests: Request[] = [];
  const fetch = vi.fn(async (request: Request): Promise<Response> => {
    requests.push(request);
    return Response.json({
      rows: [
        {
          kaisaiNen: "2026",
          kaisaiTsukihi: "0530",
          keibajoCode: "05",
          raceBango: "11",
          raceKey: "jra:2026:0530:05:11",
          source: "jra",
        },
      ],
    });
  });
  const rows = await listTomorrowRaceKeysFromCatalog(
    buildEnv(fetch),
    new Date("2026-05-29T03:00:00Z"),
  );
  expect(requests[0]?.url).toBe("https://pc-keiba-r2-catalog/v1/race-keys?date=20260530");
  expect(rows).toStrictEqual([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0530",
      keibajoCode: "05",
      raceBango: "11",
      raceKey: "jra:2026:0530:05:11",
      source: "jra",
    },
  ]);
});

it("listTodayRaceKeysWithKvCache returns cached entries without calling catalog", async () => {
  const fetch = vi.fn();
  const { env, kv } = buildKvEnv({
    fetch,
    jraCached:
      '[{"kaisaiNen":"2026","kaisaiTsukihi":"0529","keibajoCode":"05","raceBango":"01","raceKey":"jra:2026:0529:05:01","source":"jra"}]',
    narCached: "[]",
  });
  const rows = await listTodayRaceKeysWithKvCache({ context: {}, env, yyyymmdd: "20260529" });
  expect(fetch).not.toHaveBeenCalled();
  expect(kv.put).not.toHaveBeenCalled();
  expect(rows).toStrictEqual([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "05",
      raceBango: "01",
      raceKey: "jra:2026:0529:05:01",
      source: "jra",
    },
  ]);
});

it("listTodayRaceKeysWithKvCache fetches catalog and populates both source caches on miss", async () => {
  const fetch = vi.fn(
    async (): Promise<Response> =>
      Response.json({
        rows: [
          {
            kaisaiNen: "2026",
            kaisaiTsukihi: "0529",
            keibajoCode: "05",
            raceBango: "01",
            raceKey: "jra:2026:0529:05:01",
            source: "jra",
          },
          {
            kaisaiNen: "2026",
            kaisaiTsukihi: "0529",
            keibajoCode: "30",
            raceBango: "08",
            raceKey: "nar:2026:0529:30:08",
            source: "nar",
          },
        ],
      }),
  );
  const { env, kv } = buildKvEnv({ fetch, jraCached: null, narCached: null });
  const rows = await listTodayRaceKeysWithKvCache({ context: {}, env, yyyymmdd: "20260529" });
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(kv.put).toHaveBeenCalledWith(
    "race-keys:catalog-v1:jra:20260529",
    '[{"kaisaiNen":"2026","kaisaiTsukihi":"0529","keibajoCode":"05","raceBango":"01","raceKey":"jra:2026:0529:05:01","source":"jra"}]',
    { expirationTtl: 1800 },
  );
  expect(kv.put).toHaveBeenCalledWith(
    "race-keys:catalog-v1:nar:20260529",
    '[{"kaisaiNen":"2026","kaisaiTsukihi":"0529","keibajoCode":"30","raceBango":"08","raceKey":"nar:2026:0529:30:08","source":"nar"}]',
    { expirationTtl: 1800 },
  );
  expect(rows).toHaveLength(2);
});

it("listTodayRaceKeysWithKvCache refreshes catalog when only one source is cached", async () => {
  const fetch = vi.fn(async (): Promise<Response> => Response.json({ rows: [] }));
  const { env, kv } = buildKvEnv({ fetch, jraCached: "[]", narCached: null });
  const rows = await listTodayRaceKeysWithKvCache({ context: {}, env, yyyymmdd: "20260529" });
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(kv.put).toHaveBeenCalledTimes(2);
  expect(rows).toStrictEqual([]);
});

it("listTodayRaceKeysWithKvCache returns empty and does not cache catalog failures", async () => {
  const fetch = vi.fn(async (): Promise<Response> => new Response(null, { status: 503 }));
  const { env, kv } = buildKvEnv({ fetch, jraCached: null, narCached: null });
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const rows = await listTodayRaceKeysWithKvCache({ context: {}, env, yyyymmdd: "20260529" });
  expect(rows).toStrictEqual([]);
  expect(kv.put).not.toHaveBeenCalled();
  expect(consoleSpy).toHaveBeenCalledWith(
    "[features] listTodayRaceKeysWithKvCache catalog failure",
    new Error("PC_KEIBA_R2_CATALOG /v1/race-keys failed with HTTP 503"),
  );
  consoleSpy.mockRestore();
});

it("listTomorrowRaceKeysWithKvCache resolves tomorrow before reading KV", async () => {
  const fetch = vi.fn();
  const { env, kv } = buildKvEnv({ fetch, jraCached: "[]", narCached: "[]" });
  const rows = await listTomorrowRaceKeysWithKvCache({
    context: {},
    env,
    now: new Date("2026-05-29T03:00:00Z"),
  });
  expect(kv.get).toHaveBeenCalledWith("race-keys:catalog-v1:jra:20260530");
  expect(kv.get).toHaveBeenCalledWith("race-keys:catalog-v1:nar:20260530");
  expect(rows).toStrictEqual([]);
});

it("toRaceJobKeyFromTodayRaceKey maps fields", () => {
  expect(
    toRaceJobKeyFromTodayRaceKey({
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      source: "nar",
    }),
  ).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0529:30:08",
    source: "nar",
  });
});
