// Run with bun: `bun run --filter pc-keiba-viewer test`
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const mocks = vi.hoisted(() => ({
  bustPredictionCacheApiForRaceMock: vi.fn<(...args: never[]) => unknown>(),
}));

vi.mock("../../../../lib/prediction-kv-cache.server", () => ({
  bustPredictionCacheApiForRace: mocks.bustPredictionCacheApiForRaceMock,
}));

const { bustPredictionCacheApiForRaceMock } = mocks;

import { POST } from "./route";

const INTERNAL_TOKEN = "test-internal-token";
const AUTH_HEADER = "x-pc-keiba-internal-token";
const ENDPOINT_URL = "https://example.com/api/internal/prediction-cache-bust";

interface SuccessBody {
  busted: number;
  ok: true;
}

interface ErrorBody {
  error: string;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isSuccessBody = (value: unknown): value is SuccessBody => {
  if (!isRecord(value)) return false;
  return typeof value.busted === "number" && value.ok === true;
};

const isErrorBody = (value: unknown): value is ErrorBody => {
  if (!isRecord(value)) return false;
  return typeof value.error === "string";
};

const readSuccess = async (response: Response): Promise<SuccessBody> => {
  const body: unknown = await response.json();
  if (!isSuccessBody(body)) throw new Error("body is not a SuccessBody");
  return body;
};

const readError = async (response: Response): Promise<ErrorBody> => {
  const body: unknown = await response.json();
  if (!isErrorBody(body)) throw new Error("body is not an ErrorBody");
  return body;
};

const buildAuthedRequest = (body: unknown): Request =>
  new Request(ENDPOINT_URL, {
    body: JSON.stringify(body),
    headers: {
      "content-type": "application/json",
      [AUTH_HEADER]: INTERNAL_TOKEN,
    },
    method: "POST",
  });

const buildUnauthedRequest = (body: unknown): Request =>
  new Request(ENDPOINT_URL, {
    body: JSON.stringify(body),
    headers: { "content-type": "application/json" },
    method: "POST",
  });

const buildRequestWithRawBody = (rawBody: string): Request =>
  new Request(ENDPOINT_URL, {
    body: rawBody,
    headers: {
      "content-type": "application/json",
      [AUTH_HEADER]: INTERNAL_TOKEN,
    },
    method: "POST",
  });

beforeEach(() => {
  bustPredictionCacheApiForRaceMock.mockReset();
  vi.stubEnv("PC_KEIBA_INTERNAL_TOKEN", INTERNAL_TOKEN);
  bustPredictionCacheApiForRaceMock.mockResolvedValue({ busted: 2 });
});

afterEach(() => {
  vi.unstubAllEnvs();
});

it("POST returns 403 when auth header is missing", async () => {
  const response = await POST(
    buildUnauthedRequest({
      keibajoCode: "05",
      mmdd: "0809",
      raceBango: "11",
      source: "jra",
      year: "2026",
    }),
  );
  expect(response.status).toBe(403);
  const body = await readError(response);
  expect(body).toStrictEqual({ error: "forbidden" });
  expect(bustPredictionCacheApiForRaceMock).not.toHaveBeenCalled();
});

it("POST returns 403 when auth header value does not match", async () => {
  const request = new Request(ENDPOINT_URL, {
    body: JSON.stringify({
      keibajoCode: "05",
      mmdd: "0809",
      raceBango: "11",
      source: "jra",
      year: "2026",
    }),
    headers: {
      "content-type": "application/json",
      [AUTH_HEADER]: "wrong-token",
    },
    method: "POST",
  });
  const response = await POST(request);
  expect(response.status).toBe(403);
  const body = await readError(response);
  expect(body).toStrictEqual({ error: "forbidden" });
  expect(bustPredictionCacheApiForRaceMock).not.toHaveBeenCalled();
});

it("POST returns 403 when PC_KEIBA_INTERNAL_TOKEN env is unset even with header present", async () => {
  vi.unstubAllEnvs();
  delete process.env.PC_KEIBA_INTERNAL_TOKEN;
  const response = await POST(
    buildAuthedRequest({
      keibajoCode: "05",
      mmdd: "0809",
      raceBango: "11",
      source: "jra",
      year: "2026",
    }),
  );
  expect(response.status).toBe(403);
  const body = await readError(response);
  expect(body).toStrictEqual({ error: "forbidden" });
});

it("POST returns 400 when body is not valid JSON", async () => {
  const response = await POST(buildRequestWithRawBody("{not json"));
  expect(response.status).toBe(400);
  const body = await readError(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST returns 400 when body is JSON null", async () => {
  const response = await POST(buildAuthedRequest(null));
  expect(response.status).toBe(400);
  const body = await readError(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST returns 400 when body source is not jra/nar", async () => {
  const response = await POST(
    buildAuthedRequest({
      keibajoCode: "05",
      mmdd: "0809",
      raceBango: "11",
      source: "overseas",
      year: "2026",
    }),
  );
  expect(response.status).toBe(400);
  const body = await readError(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST returns 400 when mmdd is missing", async () => {
  const response = await POST(
    buildAuthedRequest({
      keibajoCode: "05",
      raceBango: "11",
      source: "jra",
      year: "2026",
    }),
  );
  expect(response.status).toBe(400);
  const body = await readError(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST returns 200 with busted for a valid JRA body", async () => {
  const response = await POST(
    buildAuthedRequest({
      keibajoCode: "05",
      mmdd: "0809",
      raceBango: "11",
      source: "jra",
      year: "2026",
    }),
  );
  expect(response.status).toBe(200);
  const body = await readSuccess(response);
  expect(body).toStrictEqual({ busted: 2, ok: true });
  expect(bustPredictionCacheApiForRaceMock).toHaveBeenCalledWith({
    keibajoCode: "05",
    mmdd: "0809",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
});

it("POST returns 200 with busted for a valid NAR body", async () => {
  bustPredictionCacheApiForRaceMock.mockResolvedValue({ busted: 2 });
  const response = await POST(
    buildAuthedRequest({
      keibajoCode: "50",
      mmdd: "0809",
      raceBango: "07",
      source: "nar",
      year: "2026",
    }),
  );
  expect(response.status).toBe(200);
  const body = await readSuccess(response);
  expect(body).toStrictEqual({ busted: 2, ok: true });
});
