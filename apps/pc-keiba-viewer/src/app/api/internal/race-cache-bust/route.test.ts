// Run with bun: `bun run --filter pc-keiba-viewer test`
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const mocks = vi.hoisted(() => ({
  bustRaceCachesForRaceMock: vi.fn<(...args: never[]) => unknown>(),
}));

vi.mock("../../../../lib/race-cache-bust.server", () => ({
  bustRaceCachesForRace: mocks.bustRaceCachesForRaceMock,
}));

const { bustRaceCachesForRaceMock } = mocks;

import { POST } from "./route";

const INTERNAL_TOKEN = "test-internal-token";
const AUTH_HEADER = "x-pc-keiba-internal-token";
const ENDPOINT_URL = "https://example.com/api/internal/race-cache-bust";

interface SuccessBody {
  busted: number;
  generation: number;
  ok: boolean;
}

interface ErrorBody {
  error: string;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isSuccessBody = (value: unknown): value is SuccessBody => {
  if (!isRecord(value)) return false;
  return (
    typeof value.busted === "number" && typeof value.generation === "number" && value.ok === true
  );
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
  bustRaceCachesForRaceMock.mockReset();
  vi.stubEnv("PC_KEIBA_INTERNAL_TOKEN", INTERNAL_TOKEN);
  bustRaceCachesForRaceMock.mockResolvedValue({ busted: 0, generation: 1 });
});

afterEach(() => {
  vi.unstubAllEnvs();
});

it("POST returns 403 when auth header is missing", async () => {
  const response = await POST(
    buildUnauthedRequest({
      keibajoCode: "05",
      mmdd: "0628",
      raceBango: "11",
      source: "jra",
      year: "2026",
    }),
  );
  expect(response.status).toBe(403);
  const body = await readError(response);
  expect(body).toStrictEqual({ error: "forbidden" });
  expect(bustRaceCachesForRaceMock).not.toHaveBeenCalled();
});

it("POST returns 403 when auth header value does not match", async () => {
  const request = new Request(ENDPOINT_URL, {
    body: JSON.stringify({
      keibajoCode: "05",
      mmdd: "0628",
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
});

it("POST returns 403 when PC_KEIBA_INTERNAL_TOKEN env is unset even with header present", async () => {
  vi.unstubAllEnvs();
  delete process.env.PC_KEIBA_INTERNAL_TOKEN;
  const response = await POST(
    buildAuthedRequest({
      keibajoCode: "05",
      mmdd: "0628",
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

it("POST returns 400 when body source is missing", async () => {
  const response = await POST(
    buildAuthedRequest({
      keibajoCode: "05",
      mmdd: "0628",
      raceBango: "11",
      year: "2026",
    }),
  );
  expect(response.status).toBe(400);
  const body = await readError(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST returns 400 when keibajoCode is malformed", async () => {
  const response = await POST(
    buildAuthedRequest({
      keibajoCode: "5",
      mmdd: "0628",
      raceBango: "11",
      source: "jra",
      year: "2026",
    }),
  );
  expect(response.status).toBe(400);
  const body = await readError(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST returns 400 when mmdd is malformed", async () => {
  const response = await POST(
    buildAuthedRequest({
      keibajoCode: "05",
      mmdd: "6-28",
      raceBango: "11",
      source: "jra",
      year: "2026",
    }),
  );
  expect(response.status).toBe(400);
  const body = await readError(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST returns 200 with busted and generation for a valid JRA body", async () => {
  bustRaceCachesForRaceMock.mockResolvedValue({ busted: 20, generation: 7 });
  const response = await POST(
    buildAuthedRequest({
      keibajoCode: "05",
      mmdd: "0628",
      raceBango: "11",
      source: "jra",
      year: "2026",
    }),
  );
  expect(response.status).toBe(200);
  const body = await readSuccess(response);
  expect(body).toStrictEqual({ busted: 20, generation: 7, ok: true });
  expect(bustRaceCachesForRaceMock).toHaveBeenCalledWith({
    keibajoCode: "05",
    mmdd: "0628",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
});

it("POST returns 200 with busted and generation for a valid NAR body", async () => {
  bustRaceCachesForRaceMock.mockResolvedValue({ busted: 8, generation: 2 });
  const response = await POST(
    buildAuthedRequest({
      keibajoCode: "50",
      mmdd: "0529",
      raceBango: "07",
      source: "nar",
      year: "2026",
    }),
  );
  expect(response.status).toBe(200);
  const body = await readSuccess(response);
  expect(body).toStrictEqual({ busted: 8, generation: 2, ok: true });
});
