// Run with bun. Fail-closed tests for fresh per-race rescore attestations.

import { describe, expect, test, vi } from "vitest";

import { addRescoreAttestationToUrl, createRescoreAttestation } from "./rescore-attestation";

const ENTRY_SET_HASH = "254ae2424e5b63c0120bc978da10131edb8ca8c56e59268b7859f091a65a9c27";
const FEATURE_KEY = "feat-cache/catalog-v1/jra/20260823/01/03/features.parquet";
const FOUNDATION_KEY = "feat-daybase-race/catalog-v1/jra/20260823/01/03/foundation.json";
const SOURCE_KEY = "feat-daybase/catalog-v1/jra/20260823/features.parquet";

interface HeadObject {
  customMetadata?: Record<string, string>;
  etag: string;
  version?: string;
}

interface TestOptions {
  catalogResponse?: Response;
  feature?: HeadObject | null;
  foundation?: HeadObject | null;
  source?: HeadObject | null;
  token?: string;
  withCatalog?: boolean;
}

const freshBody = (overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  date: "20260823",
  entries: [
    { kettoTorokuBango: "2019100002", umaban: 2 },
    { kettoTorokuBango: "2019100001", umaban: 1 },
  ],
  keibajoCode: "01",
  raceBango: "03",
  source: "jra",
  ...overrides,
});

const makeFixture = (options: TestOptions = {}) => {
  const feature =
    options.feature === undefined
      ? { etag: "feature-etag", version: "feature-version" }
      : options.feature;
  const foundation =
    options.foundation === undefined
      ? {
          customMetadata: {
            "entry-set-hash": ENTRY_SET_HASH,
            "row-count": "2",
            "source-etag": "source-etag",
          },
          etag: "foundation-etag",
          version: "foundation-version",
        }
      : options.foundation;
  const source =
    options.source === undefined
      ? { etag: "source-etag", version: "source-version" }
      : options.source;
  const head = vi.fn(async (key: string): Promise<HeadObject | null> => {
    if (key === FEATURE_KEY) return feature;
    if (key === FOUNDATION_KEY) return foundation;
    if (key === SOURCE_KEY) return source;
    throw new Error(`unexpected key: ${key}`);
  });
  const fetch = vi.fn(async (request: Request): Promise<Response> => {
    if (request.signal.aborted) throw request.signal.reason;
    return (
      options.catalogResponse ??
      Response.json(freshBody(), { headers: { "Cache-Control": "no-store" } })
    );
  });
  const env = {
    FEATURES_CACHE: { head },
    FINISH_POSITION_ATTESTATION_TOKEN: options.token === undefined ? "secret" : options.token,
    PC_KEIBA_R2_CATALOG: options.withCatalog === false ? undefined : { fetch },
  };
  return { env, fetch, head };
};

const create = (options: TestOptions = {}) => {
  const fixture = makeFixture(options);
  return {
    ...fixture,
    result: createRescoreAttestation({
      category: "jra",
      env: fixture.env,
      keibajoCode: "1",
      raceBango: "3",
      runYmd: "20260823",
    }),
  };
};

test("creates fresh evidence from exact race, foundation, and source identities", async () => {
  const before = Date.now();
  const { fetch, head, result } = create();
  await expect(result).resolves.toStrictEqual({
    attestationIssuedAtMs: expect.any(Number),
    entryCount: 2,
    entrySetHash: ENTRY_SET_HASH,
    featureCacheEtag: "feature-etag",
    featureCacheVersion: "feature-version",
  });
  const attestation = await result;
  expect(attestation.attestationIssuedAtMs).toBeGreaterThanOrEqual(before);
  expect(head).toHaveBeenCalledTimes(3);
  expect(head).toHaveBeenCalledWith(FEATURE_KEY);
  expect(head).toHaveBeenCalledWith(FOUNDATION_KEY);
  expect(head).toHaveBeenCalledWith(SOURCE_KEY);

  const request = fetch.mock.calls[0]?.[0];
  expect(request).toBeInstanceOf(Request);
  if (!(request instanceof Request)) throw new Error("expected catalog Request");
  const url = new URL(request.url);
  expect(url.pathname).toBe("/v1/internal/fresh-race-entries");
  expect(Object.fromEntries(url.searchParams)).toStrictEqual({
    date: "20260823",
    keibajoCode: "01",
    raceBango: "03",
    source: "jra",
  });
  expect(request.headers.get("Authorization")).toBe("Bearer secret");
  expect(request.headers.get("Cache-Control")).toBe("no-store");
  expect(request.headers.get("Pragma")).toBe("no-cache");
  expect(request.signal).toBeInstanceOf(AbortSignal);
});

test("adds only compact evidence fields while preserving race scope", () => {
  const result = addRescoreAttestationToUrl(
    "http://do/predict?category=jra&runDate=20260823&keibajoCode=01&raceBango=03&mode=rescore",
    {
      attestationIssuedAtMs: 1_777_000_000_000,
      entryCount: 2,
      entrySetHash: ENTRY_SET_HASH,
      featureCacheEtag: "feature-etag",
      featureCacheVersion: "feature-version",
    },
  );
  expect(Object.fromEntries(new URL(result).searchParams)).toStrictEqual({
    attestationIssuedAtMs: "1777000000000",
    category: "jra",
    entryCount: "2",
    entrySetHash: ENTRY_SET_HASH,
    featureCacheEtag: "feature-etag",
    featureCacheVersion: "feature-version",
    keibajoCode: "01",
    mode: "rescore",
    raceBango: "03",
    runDate: "20260823",
  });
});

describe("fails closed before Container dispatch", () => {
  test.each([
    ["missing feature cache", { feature: null }, "missing per-race feature cache"],
    ["missing foundation", { foundation: null }, "missing per-race foundation"],
    ["missing day-base", { source: null }, "missing source day-base cache"],
    [
      "entry hash mismatch",
      {
        foundation: {
          customMetadata: {
            "entry-set-hash": "0".repeat(64),
            "row-count": "2",
            "source-etag": "source-etag",
          },
          etag: "foundation-etag",
        },
      },
      "foundation entry-set hash mismatch",
    ],
    [
      "entry count mismatch",
      {
        foundation: {
          customMetadata: {
            "entry-set-hash": ENTRY_SET_HASH,
            "row-count": "3",
            "source-etag": "source-etag",
          },
          etag: "foundation-etag",
        },
      },
      "foundation entry count mismatch",
    ],
    [
      "source mismatch",
      {
        foundation: {
          customMetadata: {
            "entry-set-hash": ENTRY_SET_HASH,
            "row-count": "2",
            "source-etag": "stale-etag",
          },
          etag: "foundation-etag",
        },
      },
      "foundation source etag mismatch",
    ],
    ["missing source etag", { source: { etag: "" } }, "missing source day-base etag"],
    ["missing feature etag", { feature: { etag: "", version: "v" } }, "missing feature cache etag"],
    ["missing feature version", { feature: { etag: "e" } }, "missing feature cache version"],
  ])("rejects %s", async (_name, options, expected) => {
    await expect(create(options).result).rejects.toThrow(expected);
  });

  test("rejects a missing catalog binding or bearer secret", async () => {
    await expect(create({ withCatalog: false }).result).rejects.toThrow(
      "PC_KEIBA_R2_CATALOG binding is required",
    );
    await expect(create({ token: " " }).result).rejects.toThrow(
      "FINISH_POSITION_ATTESTATION_TOKEN is required",
    );
  });

  test("rejects non-success and malformed catalog responses", async () => {
    await expect(
      create({ catalogResponse: new Response("unavailable", { status: 503 }) }).result,
    ).rejects.toThrow("fresh race entries failed with HTTP 503");
    await expect(
      create({ catalogResponse: Response.json(freshBody({ entries: [] })) }).result,
    ).rejects.toThrow("empty or invalid fresh race entries response");
    await expect(
      create({ catalogResponse: Response.json(freshBody({ raceBango: "04" })) }).result,
    ).rejects.toThrow("fresh race entries scope mismatch");
  });

  test("propagates the bounded catalog request timeout", async () => {
    const timeout = vi
      .spyOn(AbortSignal, "timeout")
      .mockReturnValue(AbortSignal.abort(new DOMException("catalog timed out", "TimeoutError")));
    await expect(create().result).rejects.toThrow("catalog timed out");
    expect(timeout).toHaveBeenCalledWith(15_000);
    timeout.mockRestore();
  });

  test("rejects invalid and duplicate entries", async () => {
    await expect(
      create({ catalogResponse: Response.json(freshBody({ entries: [null] })) }).result,
    ).rejects.toThrow("invalid attestation entry");
    await expect(
      create({
        catalogResponse: Response.json(
          freshBody({ entries: [{ kettoTorokuBango: "", umaban: 1 }] }),
        ),
      }).result,
    ).rejects.toThrow("invalid attestation kettoTorokuBango");
    await expect(
      create({
        catalogResponse: Response.json(
          freshBody({ entries: [{ kettoTorokuBango: "2019100001", umaban: 0 }] }),
        ),
      }).result,
    ).rejects.toThrow("invalid attestation umaban");
    await expect(
      create({
        catalogResponse: Response.json(
          freshBody({
            entries: [
              { kettoTorokuBango: "2019100001", umaban: 1 },
              { kettoTorokuBango: "2019100001", umaban: 1 },
            ],
          }),
        ),
      }).result,
    ).rejects.toThrow("duplicate fresh race entry");
  });

  test("rejects invalid scope before any external request", async () => {
    const fixture = makeFixture();
    await expect(
      createRescoreAttestation({
        category: "jra",
        env: fixture.env,
        keibajoCode: "venue",
        raceBango: "3",
        runYmd: "20260823",
      }),
    ).rejects.toThrow("invalid attestation keibajoCode");
    expect(fixture.fetch).not.toHaveBeenCalled();
    expect(fixture.head).not.toHaveBeenCalled();
  });

  test("rejects malformed URL evidence", () => {
    const base = {
      attestationIssuedAtMs: 1,
      entryCount: 2,
      entrySetHash: ENTRY_SET_HASH,
      featureCacheEtag: "etag",
      featureCacheVersion: "version",
    };
    expect(() =>
      addRescoreAttestationToUrl("http://do/predict", { ...base, entryCount: 0 }),
    ).toThrow("invalid rescore attestation");
    expect(() =>
      addRescoreAttestationToUrl("http://do/predict", { ...base, featureCacheVersion: " " }),
    ).toThrow("missing feature cache version");
  });
});
