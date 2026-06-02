// Run with bun. `bun run --filter pc-keiba-viewer test`
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const { getCloudflareContextMock } = vi.hoisted(() => ({
  getCloudflareContextMock: vi.fn<() => Promise<unknown>>(),
}));

vi.mock("@opennextjs/cloudflare", () => ({
  getCloudflareContext: getCloudflareContextMock,
}));

import { GET, POST } from "./route";

const INTERNAL_TOKEN = "test-internal-token";
const AUTH_HEADER = "x-pc-keiba-internal-token";
const ENDPOINT_URL = "https://example.com/api/admin/paddock-backfill-user-id";
const KV_TTL_SECONDS = 30 * 24 * 60 * 60;

interface BackfillResponseBody {
  commit: boolean;
  counts: {
    entriesUpdated: number;
    entriesUpdatedFor602: number;
    entriesUpdatedForBefore: number;
    keysScanned: number;
    keysUpdated: number;
  };
  ok: boolean;
}

interface ErrorResponseBody {
  error: string;
}

interface KvListKey {
  name: string;
}

interface KvListResult {
  cursor?: string;
  keys: KvListKey[];
  list_complete: boolean;
}

type KvGetFn = (key: string, options: { type: "json" }) => Promise<unknown>;
type KvListFn = (options?: {
  cursor?: string;
  limit?: number;
  prefix?: string;
}) => Promise<KvListResult>;
type KvPutFn = (key: string, value: string, options?: { expirationTtl?: number }) => Promise<void>;

interface KvMock {
  get: ReturnType<typeof vi.fn<KvGetFn>>;
  list: ReturnType<typeof vi.fn<KvListFn>>;
  put: ReturnType<typeof vi.fn<KvPutFn>>;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isBackfillResponseBody = (value: unknown): value is BackfillResponseBody => {
  if (!isRecord(value)) return false;
  if (typeof value.commit !== "boolean" || value.ok !== true) return false;
  if (!isRecord(value.counts)) return false;
  return (
    typeof value.counts.entriesUpdated === "number" &&
    typeof value.counts.entriesUpdatedFor602 === "number" &&
    typeof value.counts.entriesUpdatedForBefore === "number" &&
    typeof value.counts.keysScanned === "number" &&
    typeof value.counts.keysUpdated === "number"
  );
};

const isErrorResponseBody = (value: unknown): value is ErrorResponseBody => {
  if (!isRecord(value)) return false;
  return typeof value.error === "string";
};

const readJsonAsBackfill = async (response: Response): Promise<BackfillResponseBody> => {
  const body: unknown = await response.json();
  if (!isBackfillResponseBody(body)) {
    throw new Error("response body is not a BackfillResponseBody");
  }
  return body;
};

const readJsonAsError = async (response: Response): Promise<ErrorResponseBody> => {
  const body: unknown = await response.json();
  if (!isErrorResponseBody(body)) {
    throw new Error("response body is not an ErrorResponseBody");
  }
  return body;
};

const buildAuthedGet = (): Request =>
  new Request(ENDPOINT_URL, {
    headers: { [AUTH_HEADER]: INTERNAL_TOKEN },
    method: "GET",
  });

const buildUnauthedGet = (): Request => new Request(ENDPOINT_URL, { method: "GET" });

const buildAuthedPost = (body: unknown): Request =>
  new Request(ENDPOINT_URL, {
    body: JSON.stringify(body),
    headers: { "content-type": "application/json", [AUTH_HEADER]: INTERNAL_TOKEN },
    method: "POST",
  });

const buildUnauthedPost = (body: unknown): Request =>
  new Request(ENDPOINT_URL, {
    body: JSON.stringify(body),
    headers: { "content-type": "application/json" },
    method: "POST",
  });

const buildPostWithRawBody = (rawBody: string): Request =>
  new Request(ENDPOINT_URL, {
    body: rawBody,
    headers: { "content-type": "application/json", [AUTH_HEADER]: INTERNAL_TOKEN },
    method: "POST",
  });

const buildKvMock = (): KvMock => ({
  get: vi.fn<KvGetFn>(),
  list: vi.fn<KvListFn>(),
  put: vi.fn<KvPutFn>(),
});

interface PutCall {
  key: string;
  options: { expirationTtl?: number } | undefined;
  value: string;
}

const getFirstPutCall = (kv: KvMock): PutCall => {
  const call = kv.put.mock.calls[0];
  if (!call) {
    throw new Error("kv.put was not called");
  }
  return { key: call[0], options: call[2], value: call[1] };
};

const stubCloudflareEnv = (kv: KvMock | null): void => {
  getCloudflareContextMock.mockResolvedValue({
    ctx: null,
    env: kv ? { PADDOCK_STATE_KV: kv } : {},
  });
};

const buildScoreEntry = (overrides: {
  at: string;
  id: string;
  userId?: string;
}): {
  at: string;
  category: "attention";
  delta: 1;
  horseName: string;
  horseNumber: string;
  id: string;
  scores: {
    attention: number;
    kaeshi: number;
    officialRank: null;
    paddock: number;
    preference: number;
    total: number;
  };
  type: "score";
  userId?: string;
} => ({
  at: overrides.at,
  category: "attention",
  delta: 1,
  horseName: "Test",
  horseNumber: "1",
  id: overrides.id,
  scores: {
    attention: 1,
    kaeshi: 0,
    officialRank: null,
    paddock: 0,
    preference: 0,
    total: 0.5,
  },
  type: "score",
  ...(overrides.userId === undefined ? {} : { userId: overrides.userId }),
});

const buildOfficialRankEntry = (overrides: {
  at: string;
  id: string;
}): {
  at: string;
  horseName: string;
  horseNumber: string;
  id: string;
  officialRank: 1;
  scores: {
    attention: number;
    kaeshi: number;
    officialRank: 1 | null;
    paddock: number;
    preference: number;
    total: number;
  };
  type: "official-rank";
} => ({
  at: overrides.at,
  horseName: "Rank",
  horseNumber: "2",
  id: overrides.id,
  officialRank: 1,
  scores: {
    attention: 0,
    kaeshi: 0,
    officialRank: 1,
    paddock: 0,
    preference: 0,
    total: 0,
  },
  type: "official-rank",
});

beforeEach(() => {
  getCloudflareContextMock.mockReset();
  vi.stubEnv("PC_KEIBA_INTERNAL_TOKEN", INTERNAL_TOKEN);
});

afterEach(() => {
  vi.unstubAllEnvs();
});

it("GET returns 403 when auth header is missing", async () => {
  const response = await GET(buildUnauthedGet());
  expect(response.status).toBe(403);
  const body = await readJsonAsError(response);
  expect(body).toStrictEqual({ error: "forbidden" });
});

it("GET returns 403 when auth header is incorrect", async () => {
  const request = new Request(ENDPOINT_URL, {
    headers: { [AUTH_HEADER]: "wrong-token" },
    method: "GET",
  });
  const response = await GET(request);
  expect(response.status).toBe(403);
  const body = await readJsonAsError(response);
  expect(body).toStrictEqual({ error: "forbidden" });
});

it("GET returns 403 when PC_KEIBA_INTERNAL_TOKEN env is unset even with header present", async () => {
  vi.unstubAllEnvs();
  delete process.env.PC_KEIBA_INTERNAL_TOKEN;
  const response = await GET(buildAuthedGet());
  expect(response.status).toBe(403);
  const body = await readJsonAsError(response);
  expect(body).toStrictEqual({ error: "forbidden" });
});

it("GET returns 503 when PADDOCK_STATE_KV binding is missing", async () => {
  stubCloudflareEnv(null);
  const response = await GET(buildAuthedGet());
  expect(response.status).toBe(503);
  const body = await readJsonAsError(response);
  expect(body).toStrictEqual({ error: "kv_unavailable" });
});

it("GET dry-run returns counts for entries on/before cutoff without writing to KV", async () => {
  const kv = buildKvMock();
  kv.list.mockResolvedValue({
    cursor: undefined,
    keys: [{ name: "paddock:20260602:05:09" }],
    list_complete: true,
  } satisfies KvListResult);
  kv.get.mockResolvedValue({
    history: [
      buildScoreEntry({ at: "2026-06-02T10:00:00Z", id: "a" }),
      buildScoreEntry({ at: "2026-06-02T11:00:00Z", id: "b" }),
      buildScoreEntry({ at: "2026-06-01T10:00:00Z", id: "c" }),
      buildScoreEntry({ at: "2026-05-30T10:00:00Z", id: "d" }),
      buildScoreEntry({ at: "2026-06-03T10:00:00Z", id: "e" }),
    ],
    horses: {},
    raceKey: "20260602:05:09",
    updatedAt: "2026-06-03T00:00:00Z",
  });
  stubCloudflareEnv(kv);
  const response = await GET(buildAuthedGet());
  expect(response.status).toBe(200);
  const body = await readJsonAsBackfill(response);
  expect(body).toStrictEqual({
    commit: false,
    counts: {
      entriesUpdated: 4,
      entriesUpdatedFor602: 2,
      entriesUpdatedForBefore: 2,
      keysScanned: 1,
      keysUpdated: 1,
    },
    ok: true,
  });
  expect(kv.put).not.toHaveBeenCalled();
});

it("GET dry-run paginates KV.list across multiple pages", async () => {
  const kv = buildKvMock();
  kv.list
    .mockResolvedValueOnce({
      cursor: "cursor-1",
      keys: [{ name: "paddock:20260602:05:09" }],
      list_complete: false,
    } satisfies KvListResult)
    .mockResolvedValueOnce({
      cursor: undefined,
      keys: [{ name: "paddock:20260602:05:10" }],
      list_complete: true,
    } satisfies KvListResult);
  kv.get.mockResolvedValue({
    history: [],
    horses: {},
    raceKey: "20260602:05:09",
    updatedAt: "2026-06-03T00:00:00Z",
  });
  stubCloudflareEnv(kv);
  const response = await GET(buildAuthedGet());
  expect(response.status).toBe(200);
  const body = await readJsonAsBackfill(response);
  expect(body).toStrictEqual({
    commit: false,
    counts: {
      entriesUpdated: 0,
      entriesUpdatedFor602: 0,
      entriesUpdatedForBefore: 0,
      keysScanned: 2,
      keysUpdated: 0,
    },
    ok: true,
  });
  expect(kv.list).toHaveBeenCalledTimes(2);
});

it("POST returns 403 when auth header is missing", async () => {
  const response = await POST(buildUnauthedPost({ legacyIdFor602: "x", legacyIdForBefore: "y" }));
  expect(response.status).toBe(403);
  const body = await readJsonAsError(response);
  expect(body).toStrictEqual({ error: "forbidden" });
});

it("POST returns 400 when body is not JSON", async () => {
  const response = await POST(buildPostWithRawBody("{not json"));
  expect(response.status).toBe(400);
  const body = await readJsonAsError(response);
  expect(body).toStrictEqual({ error: "invalid_body" });
});

it("POST returns 400 when legacyIdFor602 is missing", async () => {
  const response = await POST(buildAuthedPost({ legacyIdForBefore: "y" }));
  expect(response.status).toBe(400);
  const body = await readJsonAsError(response);
  expect(body).toStrictEqual({ error: "invalid_body" });
});

it("POST returns 400 when legacyIdForBefore is empty string", async () => {
  const response = await POST(buildAuthedPost({ legacyIdFor602: "x", legacyIdForBefore: "" }));
  expect(response.status).toBe(400);
  const body = await readJsonAsError(response);
  expect(body).toStrictEqual({ error: "invalid_body" });
});

it("POST returns 400 when body is a JSON array, not an object", async () => {
  const response = await POST(buildAuthedPost([]));
  expect(response.status).toBe(400);
  const body = await readJsonAsError(response);
  expect(body).toStrictEqual({ error: "invalid_body" });
});

it("POST returns 503 when PADDOCK_STATE_KV binding is missing", async () => {
  stubCloudflareEnv(null);
  const response = await POST(
    buildAuthedPost({ legacyIdFor602: "L602", legacyIdForBefore: "LBEFORE" }),
  );
  expect(response.status).toBe(503);
  const body = await readJsonAsError(response);
  expect(body).toStrictEqual({ error: "kv_unavailable" });
});

it("POST backfills score entries by date bucket and skips entries after cutoff", async () => {
  const kv = buildKvMock();
  kv.list.mockResolvedValue({
    cursor: undefined,
    keys: [{ name: "paddock:20260602:05:09" }],
    list_complete: true,
  } satisfies KvListResult);
  kv.get.mockResolvedValue({
    history: [
      buildScoreEntry({ at: "2026-06-02T10:00:00Z", id: "x1" }),
      buildScoreEntry({ at: "2026-06-01T10:00:00Z", id: "x2" }),
      buildScoreEntry({ at: "2026-06-03T10:00:00Z", id: "x3" }),
    ],
    horses: {},
    raceKey: "20260602:05:09",
    updatedAt: "2026-06-03T00:00:00Z",
  });
  kv.put.mockResolvedValue(undefined);
  stubCloudflareEnv(kv);
  const response = await POST(
    buildAuthedPost({ legacyIdFor602: "L602", legacyIdForBefore: "LBEFORE" }),
  );
  expect(response.status).toBe(200);
  const body = await readJsonAsBackfill(response);
  expect(body).toStrictEqual({
    commit: true,
    counts: {
      entriesUpdated: 2,
      entriesUpdatedFor602: 1,
      entriesUpdatedForBefore: 1,
      keysScanned: 1,
      keysUpdated: 1,
    },
    ok: true,
  });
  expect(kv.put).toHaveBeenCalledTimes(1);
  const putCall = getFirstPutCall(kv);
  expect(putCall.key).toBe("paddock:20260602:05:09");
  expect(putCall.options).toStrictEqual({ expirationTtl: KV_TTL_SECONDS });
  const persisted: unknown = JSON.parse(putCall.value);
  if (!isRecord(persisted) || !Array.isArray(persisted.history)) {
    throw new Error("persisted state shape unexpected");
  }
  const ids = persisted.history.map((entry) =>
    isRecord(entry) && typeof entry.userId === "string" ? entry.userId : null,
  );
  expect(ids).toStrictEqual(["L602", "LBEFORE", null]);
});

it("POST leaves official-rank entries untouched and does not assign userId to them", async () => {
  const kv = buildKvMock();
  kv.list.mockResolvedValue({
    cursor: undefined,
    keys: [{ name: "paddock:20260601:42:11" }],
    list_complete: true,
  } satisfies KvListResult);
  kv.get.mockResolvedValue({
    history: [
      buildOfficialRankEntry({ at: "2026-06-01T09:00:00Z", id: "or1" }),
      buildScoreEntry({ at: "2026-06-01T10:00:00Z", id: "s1" }),
    ],
    horses: {},
    raceKey: "20260601:42:11",
    updatedAt: "2026-06-02T00:00:00Z",
  });
  kv.put.mockResolvedValue(undefined);
  stubCloudflareEnv(kv);
  const response = await POST(
    buildAuthedPost({ legacyIdFor602: "L602", legacyIdForBefore: "LBEFORE" }),
  );
  expect(response.status).toBe(200);
  const body = await readJsonAsBackfill(response);
  expect(body).toStrictEqual({
    commit: true,
    counts: {
      entriesUpdated: 1,
      entriesUpdatedFor602: 0,
      entriesUpdatedForBefore: 1,
      keysScanned: 1,
      keysUpdated: 1,
    },
    ok: true,
  });
  const putCall = getFirstPutCall(kv);
  const persisted: unknown = JSON.parse(putCall.value);
  if (!isRecord(persisted) || !Array.isArray(persisted.history)) {
    throw new Error("persisted state shape unexpected");
  }
  const officialRank = persisted.history[0];
  if (!isRecord(officialRank)) {
    throw new Error("official-rank entry missing");
  }
  expect(officialRank.type).toBe("official-rank");
  expect(officialRank.userId).toBe(undefined);
});

it("POST skips entries that already have a userId set", async () => {
  const kv = buildKvMock();
  kv.list.mockResolvedValue({
    cursor: undefined,
    keys: [{ name: "paddock:20260601:42:11" }],
    list_complete: true,
  } satisfies KvListResult);
  kv.get.mockResolvedValue({
    history: [
      buildScoreEntry({ at: "2026-06-01T10:00:00Z", id: "existing", userId: "USER_EXISTING" }),
    ],
    horses: {},
    raceKey: "20260601:42:11",
    updatedAt: "2026-06-02T00:00:00Z",
  });
  kv.put.mockResolvedValue(undefined);
  stubCloudflareEnv(kv);
  const response = await POST(
    buildAuthedPost({ legacyIdFor602: "L602", legacyIdForBefore: "LBEFORE" }),
  );
  expect(response.status).toBe(200);
  const body = await readJsonAsBackfill(response);
  expect(body).toStrictEqual({
    commit: true,
    counts: {
      entriesUpdated: 0,
      entriesUpdatedFor602: 0,
      entriesUpdatedForBefore: 0,
      keysScanned: 1,
      keysUpdated: 0,
    },
    ok: true,
  });
  expect(kv.put).not.toHaveBeenCalled();
});

it("POST bumps updatedAt on the persisted blob so the DO picks up the backfill", async () => {
  const kv = buildKvMock();
  kv.list.mockResolvedValue({
    cursor: undefined,
    keys: [{ name: "paddock:20260601:42:11" }],
    list_complete: true,
  } satisfies KvListResult);
  kv.get.mockResolvedValue({
    history: [buildScoreEntry({ at: "2026-06-01T10:00:00Z", id: "s1" })],
    horses: {},
    raceKey: "20260601:42:11",
    updatedAt: "2026-06-02T00:00:00Z",
  });
  kv.put.mockResolvedValue(undefined);
  stubCloudflareEnv(kv);
  await POST(buildAuthedPost({ legacyIdFor602: "L602", legacyIdForBefore: "LBEFORE" }));
  const putCall = getFirstPutCall(kv);
  const persisted: unknown = JSON.parse(putCall.value);
  if (!isRecord(persisted) || typeof persisted.updatedAt !== "string") {
    throw new Error("persisted state shape unexpected");
  }
  expect(persisted.updatedAt > "2026-06-02T00:00:00Z").toBe(true);
});

it("POST ignores KV entries whose payload is not a valid PaddockState", async () => {
  const kv = buildKvMock();
  kv.list.mockResolvedValue({
    cursor: undefined,
    keys: [{ name: "paddock:bogus" }],
    list_complete: true,
  } satisfies KvListResult);
  kv.get.mockResolvedValue({ wrong: "shape" });
  stubCloudflareEnv(kv);
  const response = await POST(
    buildAuthedPost({ legacyIdFor602: "L602", legacyIdForBefore: "LBEFORE" }),
  );
  expect(response.status).toBe(200);
  const body = await readJsonAsBackfill(response);
  expect(body).toStrictEqual({
    commit: true,
    counts: {
      entriesUpdated: 0,
      entriesUpdatedFor602: 0,
      entriesUpdatedForBefore: 0,
      keysScanned: 1,
      keysUpdated: 0,
    },
    ok: true,
  });
  expect(kv.put).not.toHaveBeenCalled();
});
