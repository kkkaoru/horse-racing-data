// Run with bun. Contract, attestation, resource-bound, and default-off tests
// for the distinct Worker market-signal foundation producer.

import { expect, test, vi } from "vitest";

import {
  buildMarketSignalFoundationKey,
  marketSignalFoundationEnabled,
  materializeMarketSignalFoundation,
  type MarketSignalFoundationBucket,
  type MarketSignalFoundationObject,
  type MarketSignalFoundationObjectBody,
  type MarketSignalFoundationPutOptions,
  type MaterializeMarketSignalFoundationInput,
} from "./race-chain-market-signal-foundation";

const SOURCE_KEY: string = "feat-daybase/catalog-v1/jra/20260824/features.parquet";
const MANIFEST_KEY: string = "feat-daybase-race/catalog-v1/jra/20260824/manifest.json";
const FOUNDATION_KEY: string = "feat-daybase-race/catalog-v1/jra/20260824/05/11/foundation.json";
const ENTRY_SET_HASH: string = "443d6c5b96d28ebe21e458b248a615f248793bdbe26caf9328495352101b54a1";
const INPUT_FEATURE_HASH: string =
  "599adbf6355b9f2681a7a5d4116c3c42e0ea92ab6954bd4a3206065065ed2247";
const OUTPUT_FEATURE_HASH: string =
  "ec38a1343165bf4655bc26be25e4f52ae8799a815cf5151e24b59f59ba921606";
const ODDS_SNAPSHOT_HASH: string =
  "a0d4e20b8ffce95464b1636583cac8f7b0b0e60e630d7f1b6fa0f364fc9af18d";
const encoder: TextEncoder = new TextEncoder();

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const sha256Hex = async (value: string): Promise<string> => {
  const digest = await crypto.subtle.digest("SHA-256", encoder.encode(value));
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
};

interface CapturedPut {
  body: Uint8Array;
  key: string;
  options: MarketSignalFoundationPutOptions;
}

interface BucketFixture {
  bucket: MarketSignalFoundationBucket;
  get: ReturnType<typeof vi.fn>;
  head: ReturnType<typeof vi.fn>;
  puts: CapturedPut[];
}

interface FixtureOverrides {
  foundationBytes?: Uint8Array;
  foundationValue?: unknown;
  get?: (key: string) => Promise<MarketSignalFoundationObjectBody | null>;
  head?: (key: string) => Promise<MarketSignalFoundationObject | null>;
  manifestBytes?: Uint8Array;
  manifestValue?: unknown;
  put?: (
    key: string,
    value: Uint8Array,
    options: MarketSignalFoundationPutOptions,
  ) => Promise<unknown>;
}

const sourceIdentity = (): Record<string, unknown> => ({
  etag: "source-etag",
  key: SOURCE_KEY,
  version: "source-version",
});

const featureSchema = (): Array<Record<string, unknown>> =>
  [
    "race_id",
    "ketto_toroku_bango",
    "umaban",
    "career_win_rate",
    "odds_score",
    "popularity_score",
    "tansho_odds",
    "tansho_ninkijun",
  ].map((name) => ({ name, physicalType: "DOUBLE" }));

const foundationRows = (): Array<Record<string, unknown>> => [
  {
    career_win_rate: 0.4,
    ketto_toroku_bango: "2020000001",
    odds_score: 0.8,
    popularity_score: 0.8,
    race_id: "jra:2026:0824:05:11",
    tansho_ninkijun: 9,
    tansho_odds: 99,
    umaban: 1,
  },
  {
    career_win_rate: 0.3,
    ketto_toroku_bango: "2020000002",
    odds_score: 0.7,
    popularity_score: 0.7,
    race_id: "jra:2026:0824:05:11",
    tansho_ninkijun: 8,
    tansho_odds: 88,
    umaban: 2,
  },
];

const manifestValue = (): Record<string, unknown> => ({
  contract: {
    contractVersion: "day-base-race-foundation-v1",
    featureHash: INPUT_FEATURE_HASH,
    featureSchema: featureSchema(),
    generationId: "base-generation",
    raceCount: 1,
    rowCount: 2,
    schemaVersion: "1",
  },
  races: [
    {
      entrySetHash: ENTRY_SET_HASH,
      key: FOUNDATION_KEY,
      raceId: "jra:2026:0824:05:11",
      rowCount: 2,
    },
  ],
  source: sourceIdentity(),
});

const foundationValue = (): Record<string, unknown> => ({
  contract: {
    contractVersion: "day-base-race-foundation-v1",
    entrySetHash: ENTRY_SET_HASH,
    featureHash: INPUT_FEATURE_HASH,
    generationId: "base-generation",
    rowCount: 2,
    schemaVersion: "1",
  },
  raceId: "jra:2026:0824:05:11",
  rows: foundationRows(),
  source: sourceIdentity(),
});

const manifestFromToken = (token: string): Record<string, unknown> => {
  const value = manifestValue();
  if (!isRecord(value.contract)) throw new Error("invalid test manifest contract");
  if (token === "manifest-contract-version") {
    return { ...value, contract: { ...value.contract, contractVersion: "old" } };
  }
  if (token === "empty-feature-hash") {
    return { ...value, contract: { ...value.contract, featureHash: "" } };
  }
  if (token === "empty-feature-schema") {
    return { ...value, contract: { ...value.contract, featureSchema: [] } };
  }
  if (token === "non-array-feature-schema") {
    return { ...value, contract: { ...value.contract, featureSchema: null } };
  }
  if (token === "invalid-feature-field") {
    return { ...value, contract: { ...value.contract, featureSchema: [null] } };
  }
  const schema = featureSchema();
  return { ...value, contract: { ...value.contract, featureSchema: [...schema, schema[0]] } };
};

const objectBody = (
  value: unknown,
  etag: string,
  version: string,
  bytesOverride?: Uint8Array,
): MarketSignalFoundationObjectBody => {
  const bytes: Uint8Array = bytesOverride ?? encoder.encode(JSON.stringify(value));
  const copy: Uint8Array<ArrayBuffer> = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return {
    arrayBuffer: async () => copy.buffer,
    etag,
    size: bytes.byteLength,
    version,
  };
};

const makeBucket = (overrides: FixtureOverrides = {}): BucketFixture => {
  const puts: CapturedPut[] = [];
  const get = vi.fn(
    overrides.get ??
      (async (key: string) =>
        key === MANIFEST_KEY
          ? objectBody(
              overrides.manifestValue ?? manifestValue(),
              "manifest-etag",
              "manifest-version",
              overrides.manifestBytes,
            )
          : objectBody(
              overrides.foundationValue ?? foundationValue(),
              "foundation-etag",
              "foundation-version",
              overrides.foundationBytes,
            )),
  );
  const head = vi.fn(
    overrides.head ??
      (async (key: string) => {
        if (key === SOURCE_KEY)
          return { etag: "source-etag", size: 1024, version: "source-version" };
        const latest: CapturedPut | undefined = puts.find((put) => put.key === key);
        return latest === undefined
          ? null
          : {
              customMetadata: latest.options.customMetadata,
              etag: "artifact-etag",
              size: latest.body.byteLength,
              version: "artifact-version",
            };
      }),
  );
  const put = vi.fn(
    overrides.put ??
      (async (key: string, value: Uint8Array, options: MarketSignalFoundationPutOptions) => {
        puts.push({ body: value, key, options });
        return {};
      }),
  );
  return { bucket: { get, head, put }, get, head, puts };
};

const enabledInput = (
  bucket: MarketSignalFoundationBucket,
): MaterializeMarketSignalFoundationInput => ({
  category: "jra",
  env: { FEATURES_CACHE: bucket, WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED: "1" },
  keibajoCode: "5",
  liveOddsByHorseNumber: new Map([
    [1, { tanshoNinkijun: 1, tanshoOdds: 2 }],
    [2, { tanshoNinkijun: 2, tanshoOdds: 4 }],
  ]),
  raceBango: "11",
  runYmd: "20260824",
});

test("market-signal foundation flag is default-off and only accepts the explicit enabled value", () => {
  expect(marketSignalFoundationEnabled(undefined)).toBe(false);
  expect(marketSignalFoundationEnabled("0")).toBe(false);
  expect(marketSignalFoundationEnabled("1")).toBe(true);
});

test("buildMarketSignalFoundationKey uses a namespace distinct from day-base and final caches", () => {
  expect(buildMarketSignalFoundationKey("jra", "20260824", "5", "1")).toBe(
    "feat-racechain-market-signal/catalog-v1/jra/20260824/05/01/foundation.json",
  );
});

test("materializeMarketSignalFoundation skips without reading R2 when the flag is disabled", async () => {
  const fixture = makeBucket();
  const input = enabledInput(fixture.bucket);
  input.env.WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED = "0";

  await expect(materializeMarketSignalFoundation(input)).resolves.toStrictEqual({
    reason: "disabled",
    status: "skipped",
  });
  expect(fixture.get).not.toHaveBeenCalled();
  expect(fixture.head).not.toHaveBeenCalled();
});

test("materializeMarketSignalFoundation writes an attested distinct artifact with telemetry", async () => {
  const fixture = makeBucket();
  const ticks: number[] = [0, 10, 20, 30];
  const result = await materializeMarketSignalFoundation(enabledInput(fixture.bucket), {
    now: () => ticks.shift() ?? 30,
  });

  expect(result).toStrictEqual({
    artifactEtag: "artifact-etag",
    artifactVersion: "artifact-version",
    baseGenerationId: "base-generation",
    cacheHit: false,
    computeMs: 10,
    key: "feat-racechain-market-signal/catalog-v1/jra/20260824/05/11/foundation.json",
    oddsSnapshotHash: ODDS_SNAPSHOT_HASH,
    rowCount: 2,
    status: "materialized",
    totalMs: 30,
  });
  expect(fixture.puts).toHaveLength(1);
  expect(fixture.puts[0]?.key).toBe(
    "feat-racechain-market-signal/catalog-v1/jra/20260824/05/11/foundation.json",
  );
  expect(fixture.puts[0]?.options.customMetadata).toStrictEqual({
    "base-generation-id": "base-generation",
    "contract-version": "race-chain-market-signal-foundation-v1",
    "entry-set-hash": ENTRY_SET_HASH,
    "odds-snapshot-hash": ODDS_SNAPSHOT_HASH,
    "output-feature-hash": OUTPUT_FEATURE_HASH,
    "race-id": "jra:2026:0824:05:11",
    "row-count": "2",
    "schema-version": "1",
    "source-etag": "source-etag",
    "source-version": "source-version",
    "worker-compute-ms": "10",
  });
  const body: unknown = JSON.parse(new TextDecoder().decode(fixture.puts[0]?.body));
  expect(body).toMatchObject({
    base: {
      foundationEtag: "foundation-etag",
      foundationKey: FOUNDATION_KEY,
      foundationVersion: "foundation-version",
      manifestEtag: "manifest-etag",
      manifestKey: MANIFEST_KEY,
      manifestVersion: "manifest-version",
    },
    contract: {
      baseGenerationId: "base-generation",
      contractVersion: "race-chain-market-signal-foundation-v1",
      entrySetHash: ENTRY_SET_HASH,
      inputFeatureHash: INPUT_FEATURE_HASH,
      oddsSnapshotHash: ODDS_SNAPSHOT_HASH,
      outputFeatureHash: OUTPUT_FEATURE_HASH,
      raceId: "jra:2026:0824:05:11",
      rowCount: 2,
      schemaVersion: "1",
    },
    source: sourceIdentity(),
    telemetry: { totalMs: 30, workerComputeMs: 10 },
  });
});

test("materializeMarketSignalFoundation canonicalizes absent R2 versions to empty strings", async () => {
  const manifest = manifestValue();
  const foundation = foundationValue();
  manifest.source = { ...sourceIdentity(), version: "" };
  foundation.source = { ...sourceIdentity(), version: "" };
  let artifactHeadCount = 0;
  const fixture = makeBucket({
    get: async (key) => {
      const value = key === MANIFEST_KEY ? manifest : foundation;
      const body = objectBody(
        value,
        key === MANIFEST_KEY ? "manifest-etag" : "foundation-etag",
        "",
      );
      return { arrayBuffer: body.arrayBuffer, etag: body.etag, size: body.size };
    },
    head: async (key) => {
      if (key === SOURCE_KEY) return { etag: "source-etag", size: 1024 };
      artifactHeadCount += 1;
      return artifactHeadCount === 1
        ? null
        : {
            customMetadata: {
              "base-generation-id": "base-generation",
              "contract-version": "race-chain-market-signal-foundation-v1",
              "entry-set-hash": ENTRY_SET_HASH,
              "odds-snapshot-hash": ODDS_SNAPSHOT_HASH,
              "output-feature-hash": OUTPUT_FEATURE_HASH,
              "race-id": "jra:2026:0824:05:11",
              "row-count": "2",
              "schema-version": "1",
              "source-etag": "source-etag",
              "source-version": "",
              "worker-compute-ms": "0",
            },
            etag: "artifact-etag",
            size: 1024,
          };
    },
  });

  await expect(
    materializeMarketSignalFoundation(enabledInput(fixture.bucket)),
  ).resolves.toMatchObject({
    status: "materialized",
  });
  const body: unknown = JSON.parse(new TextDecoder().decode(fixture.puts[0]?.body));
  expect(body).toMatchObject({
    base: { foundationVersion: "", manifestVersion: "" },
    source: { version: "" },
  });
});

test("materializeMarketSignalFoundation reuses only an exact current-snapshot artifact", async () => {
  const fixture = makeBucket();
  await materializeMarketSignalFoundation(enabledInput(fixture.bucket));

  await expect(
    materializeMarketSignalFoundation(enabledInput(fixture.bucket)),
  ).resolves.toMatchObject({
    artifactEtag: "artifact-etag",
    artifactVersion: "artifact-version",
    baseGenerationId: "base-generation",
    cacheHit: true,
    oddsSnapshotHash: ODDS_SNAPSHOT_HASH,
    status: "materialized",
  });
  expect(fixture.puts).toHaveLength(1);
});

test("materializeMarketSignalFoundation withholds attestation when post-write HEAD is stale", async () => {
  const fixture = makeBucket({
    head: async (key) =>
      key === SOURCE_KEY
        ? { etag: "source-etag", size: 1024, version: "source-version" }
        : { customMetadata: {}, etag: "stale", size: 1, version: "stale" },
  });

  await expect(
    materializeMarketSignalFoundation(enabledInput(fixture.bucket)),
  ).resolves.toStrictEqual({ reason: "artifact-attestation-unavailable", status: "fallback" });
  expect(fixture.puts).toHaveLength(1);
});

test("materializeMarketSignalFoundation rejects unsupported categories", async () => {
  const fixture = makeBucket();
  const input = enabledInput(fixture.bucket);

  await expect(
    materializeMarketSignalFoundation({ ...input, category: "nar" }),
  ).resolves.toStrictEqual({ reason: "unsupported-category", status: "fallback" });
});

test.each([
  ["2026-08-24", "5", "11"],
  ["20260824", "abc", "11"],
  ["20260824", "5", "abc"],
])(
  "materializeMarketSignalFoundation rejects malformed race identity",
  async (runYmd, venue, race) => {
    const fixture = makeBucket();
    const input = enabledInput(fixture.bucket);

    await expect(
      materializeMarketSignalFoundation({
        ...input,
        keibajoCode: venue,
        raceBango: race,
        runYmd,
      }),
    ).resolves.toStrictEqual({ reason: "invalid-race-identity", status: "fallback" });
  },
);

test("materializeMarketSignalFoundation requires a non-empty bounded live board", async () => {
  const fixture = makeBucket();
  const input = enabledInput(fixture.bucket);

  await expect(
    materializeMarketSignalFoundation({ ...input, liveOddsByHorseNumber: new Map() }),
  ).resolves.toStrictEqual({ reason: "invalid-live-board", status: "fallback" });
  await expect(
    materializeMarketSignalFoundation({
      ...input,
      liveOddsByHorseNumber: new Map(
        Array.from({ length: 33 }, (_, index) => [
          index + 1,
          { tanshoNinkijun: index + 1, tanshoOdds: index + 2 },
        ]),
      ),
    }),
  ).resolves.toStrictEqual({ reason: "invalid-live-board", status: "fallback" });
});

test.each([
  [null, "source-unavailable"],
  [{ etag: "source-etag", size: 0, version: "source-version" }, "source-unavailable"],
  [
    { etag: "source-etag", size: 17 * 1024 * 1024, version: "source-version" },
    "source-unavailable",
  ],
])("materializeMarketSignalFoundation rejects an unavailable source", async (source, reason) => {
  const fixture = makeBucket({ head: async () => source });

  await expect(
    materializeMarketSignalFoundation(enabledInput(fixture.bucket)),
  ).resolves.toStrictEqual({
    reason,
    status: "fallback",
  });
});

test("materializeMarketSignalFoundation rejects missing foundation objects", async () => {
  const missingManifest = makeBucket({
    get: async (key) => (key === MANIFEST_KEY ? null : objectBody(foundationValue(), "f", "v")),
  });
  await expect(
    materializeMarketSignalFoundation(enabledInput(missingManifest.bucket)),
  ).resolves.toStrictEqual({ reason: "foundation-unavailable", status: "fallback" });

  const missingRace = makeBucket({
    get: async (key) => (key === FOUNDATION_KEY ? null : objectBody(manifestValue(), "m", "v")),
  });
  await expect(
    materializeMarketSignalFoundation(enabledInput(missingRace.bucket)),
  ).resolves.toStrictEqual({ reason: "foundation-unavailable", status: "fallback" });
});

test.each([
  [encoder.encode("{"), undefined],
  [new Uint8Array(0), undefined],
  [new Uint8Array(2 * 1024 * 1024 + 1), undefined],
])("materializeMarketSignalFoundation rejects malformed or unbounded JSON", async (bytes) => {
  const fixture = makeBucket({ manifestBytes: bytes });

  await expect(
    materializeMarketSignalFoundation(enabledInput(fixture.bucket)),
  ).resolves.toStrictEqual({
    reason: "foundation-unavailable",
    status: "fallback",
  });
});

test("materializeMarketSignalFoundation rejects a truncated R2 body", async () => {
  const fixture = makeBucket({
    get: async (key) => {
      const object = objectBody(
        key === MANIFEST_KEY ? manifestValue() : foundationValue(),
        "etag",
        "version",
      );
      return key === MANIFEST_KEY ? { ...object, size: object.size + 1 } : object;
    },
  });

  await expect(
    materializeMarketSignalFoundation(enabledInput(fixture.bucket)),
  ).resolves.toStrictEqual({
    reason: "foundation-unavailable",
    status: "fallback",
  });
});

test.each([
  [{ ...manifestValue(), source: { ...sourceIdentity(), etag: "stale" } }, foundationValue()],
  [manifestValue(), { ...foundationValue(), source: { ...sourceIdentity(), version: "stale" } }],
  [{ ...manifestValue(), contract: null }, foundationValue()],
  [manifestValue(), { ...foundationValue(), contract: null }],
  [42, foundationValue()],
  [manifestValue(), 42],
  ["manifest-contract-version", foundationValue()],
  [manifestValue(), { ...foundationValue(), raceId: "jra:2026:0824:05:12" }],
  [{ ...manifestValue(), races: [] }, foundationValue()],
  [{ ...manifestValue(), races: null }, foundationValue()],
  [
    {
      ...manifestValue(),
      races: [{ entrySetHash: ENTRY_SET_HASH, key: FOUNDATION_KEY, raceId: "wrong", rowCount: 2 }],
    },
    foundationValue(),
  ],
  [
    {
      ...manifestValue(),
      races: [
        { entrySetHash: "", key: FOUNDATION_KEY, raceId: "jra:2026:0824:05:11", rowCount: 2 },
      ],
    },
    foundationValue(),
  ],
  [
    {
      ...manifestValue(),
      races: [
        {
          entrySetHash: ENTRY_SET_HASH,
          key: FOUNDATION_KEY,
          raceId: "jra:2026:0824:05:11",
          rowCount: 0,
        },
      ],
    },
    foundationValue(),
  ],
  [manifestValue(), { ...foundationValue(), rows: [] }],
  [manifestValue(), { ...foundationValue(), rows: [null] }],
  [
    manifestValue(),
    {
      ...foundationValue(),
      rows: foundationRows().map((row, index) =>
        index === 0 ? { ...row, ketto_toroku_bango: "" } : row,
      ),
    },
  ],
  [
    manifestValue(),
    {
      ...foundationValue(),
      rows: foundationRows().map((row, index) => (index === 0 ? { ...row, umaban: true } : row)),
    },
  ],
  [
    manifestValue(),
    {
      ...foundationValue(),
      rows: foundationRows().map((row, index) => (index === 0 ? { ...row, umaban: "x" } : row)),
    },
  ],
  [
    manifestValue(),
    {
      ...foundationValue(),
      rows: foundationRows().map((row, index) =>
        index === 0 ? { ...row, umaban: "999999999999999999999" } : row,
      ),
    },
  ],
  [
    manifestValue(),
    {
      ...foundationValue(),
      rows: foundationRows().map((row) => ({ ...row, ketto_toroku_bango: "same", umaban: 1 })),
    },
  ],
  [
    manifestValue(),
    {
      ...foundationValue(),
      rows: foundationRows().map((row, index) => ({ ...row, umaban: index + 2 })),
    },
  ],
  [manifestValue(), "foundation-generation"],
  [
    {
      ...manifestValue(),
      source: null,
    },
    foundationValue(),
  ],
  [
    manifestValue(),
    {
      ...foundationValue(),
      source: { etag: "source-etag", key: SOURCE_KEY },
    },
  ],
  ["empty-feature-hash", foundationValue()],
  ["empty-feature-schema", foundationValue()],
  ["non-array-feature-schema", foundationValue()],
  ["invalid-feature-field", foundationValue()],
  ["duplicate-feature-name", foundationValue()],
])(
  "materializeMarketSignalFoundation fails closed on an attestation mismatch %#",
  async (manifestInput, foundationInput) => {
    const manifest =
      typeof manifestInput === "string" ? manifestFromToken(manifestInput) : manifestInput;
    const foundation =
      foundationInput === "foundation-generation"
        ? (() => {
            const value = foundationValue();
            if (!isRecord(value.contract)) throw new Error("invalid test foundation contract");
            return { ...value, contract: { ...value.contract, generationId: "stale" } };
          })()
        : foundationInput;
    const fixture = makeBucket({ foundationValue: foundation, manifestValue: manifest });

    await expect(
      materializeMarketSignalFoundation(enabledInput(fixture.bucket)),
    ).resolves.toStrictEqual({
      reason: "foundation-attestation-mismatch",
      status: "fallback",
    });
    expect(fixture.puts).toHaveLength(0);
  },
);

test("materializeMarketSignalFoundation propagates market parity failures without writing", async () => {
  const fixture = makeBucket();
  const input = enabledInput(fixture.bucket);

  await expect(
    materializeMarketSignalFoundation({
      ...input,
      liveOddsByHorseNumber: new Map([
        [1, { tanshoNinkijun: 1, tanshoOdds: 0 }],
        [2, { tanshoNinkijun: 2, tanshoOdds: 4 }],
      ]),
    }),
  ).resolves.toStrictEqual({ reason: "market-signal-invalid-live-odds", status: "fallback" });
  expect(fixture.puts).toHaveLength(0);
});

test("materializeMarketSignalFoundation enforces the Worker compute budget", async () => {
  const fixture = makeBucket();
  const ticks: number[] = [0, 10, 261];

  await expect(
    materializeMarketSignalFoundation(enabledInput(fixture.bucket), {
      now: () => ticks.shift() ?? 261,
    }),
  ).resolves.toStrictEqual({ reason: "compute-limit", status: "fallback" });
  expect(fixture.puts).toHaveLength(0);
});

test("materializeMarketSignalFoundation rejects a negative compute duration", async () => {
  const fixture = makeBucket();
  const ticks: number[] = [10, 10, 5];

  await expect(
    materializeMarketSignalFoundation(enabledInput(fixture.bucket), {
      now: () => ticks.shift() ?? 5,
    }),
  ).resolves.toStrictEqual({ reason: "compute-limit", status: "fallback" });
  expect(fixture.puts).toHaveLength(0);
});

test("materializeMarketSignalFoundation rejects a base schema containing output columns", async () => {
  const manifest = manifestValue();
  const foundation = foundationValue();
  const manifestContract = manifest.contract;
  const foundationContract = foundation.contract;
  if (!isRecord(manifestContract) || !isRecord(foundationContract)) {
    throw new Error("invalid test manifest contract");
  }
  const schema = [...featureSchema(), { name: "form_market_edge", physicalType: "DOUBLE" }];
  const hash = await sha256Hex(schema.map((field) => field.name).join("\n"));
  manifestContract.featureHash = hash;
  manifestContract.featureSchema = schema;
  foundationContract.featureHash = hash;
  foundation.rows = foundationRows().map((row) => ({ ...row, form_market_edge: null }));
  const fixture = makeBucket({ foundationValue: foundation, manifestValue: manifest });

  await expect(
    materializeMarketSignalFoundation(enabledInput(fixture.bucket)),
  ).resolves.toStrictEqual({
    reason: "output-feature-limit",
    status: "fallback",
  });
  expect(fixture.puts).toHaveLength(0);
});

test("materializeMarketSignalFoundation enforces the enriched output size bound", async () => {
  const manifest = manifestValue();
  const foundation = foundationValue();
  const manifestContract = manifest.contract;
  const foundationContract = foundation.contract;
  if (!isRecord(manifestContract) || !isRecord(foundationContract)) {
    throw new Error("invalid test foundation contract");
  }
  const schema = [...featureSchema(), { name: "payload", physicalType: "BYTE_ARRAY" }];
  const hash = await sha256Hex(schema.map((field) => field.name).join("\n"));
  manifestContract.featureHash = hash;
  manifestContract.featureSchema = schema;
  foundationContract.featureHash = hash;
  foundation.rows = foundationRows().map((row, index) => ({
    ...row,
    payload: index === 0 ? "x".repeat(2_095_000) : "",
  }));
  const fixture = makeBucket({ foundationValue: foundation, manifestValue: manifest });

  await expect(
    materializeMarketSignalFoundation(enabledInput(fixture.bucket)),
  ).resolves.toStrictEqual({ reason: "output-size-limit", status: "fallback" });
  expect(fixture.puts).toHaveLength(0);
});

test("materializeMarketSignalFoundation surfaces an R2 write failure", async () => {
  const fixture = makeBucket({
    put: async () => Promise.reject(new Error("r2-write-failed")),
  });

  await expect(materializeMarketSignalFoundation(enabledInput(fixture.bucket))).rejects.toThrow(
    "r2-write-failed",
  );
});
