// Run with bun. Tests for authoritative Container role environment merging.

import { expect, test, vi } from "vitest";

vi.mock("@cloudflare/containers", () => ({
  Container: class {},
}));

import {
  buildLegacyPredictContainerEnvVars,
  buildRaceChainPredictContainerEnvVars,
  FinishPositionPredictContainer,
} from "./container-class";
import { WATCH_REQUEST_HEADER, WATCH_RESPONSE_HEADER } from "./focused-full-watch";
import type { FocusedFullWatchBody, ValidatedFocusedFullWatchPayload } from "./focused-full-watch";
import { PREDICT_DO_INTERNAL_PURGE_PATH } from "./predict-do-state-purge";

interface RuntimeContainerHarness {
  getTcpPort: ReturnType<typeof vi.fn>;
  running: boolean;
}

interface ContainerHarness {
  container: FinishPositionPredictContainer;
  containerFetchMock: ReturnType<typeof vi.fn>;
  destroyMock: ReturnType<typeof vi.fn>;
  featureCachePutMock: ReturnType<typeof vi.fn>;
  queueSendMock: ReturnType<typeof vi.fn>;
  runtimeContainer: RuntimeContainerHarness;
  scheduleMock: ReturnType<typeof vi.fn>;
  startAndWaitForPortsMock: ReturnType<typeof vi.fn>;
  storageDeleteMock: ReturnType<typeof vi.fn>;
  storageGetMock: ReturnType<typeof vi.fn>;
  storageListMock: ReturnType<typeof vi.fn>;
  storagePutMock: ReturnType<typeof vi.fn>;
}

const WATCH_BODY: FocusedFullWatchBody = {
  category: "jra",
  daysAhead: 0,
  keibajoCode: "05",
  mode: "full",
  raceBango: "09",
  runDate: "2026-08-24",
  runDateIso: "2026-08-24",
  runYmd: "20260824",
  skipDedup: true,
};

const WATCH_PAYLOAD: ValidatedFocusedFullWatchPayload = {
  body: WATCH_BODY,
  doName: "predict-jra-race-1",
  role: "race-chain",
  watchId: "focused-full:20260824:jra:05:09:source-message-1",
  workKey: "focused-full:20260824:jra:05:09",
};

const FOCUSED_PREDICT_URL =
  "http://do/predict?mode=full&category=jra&runDate=20260824&keibajoCode=05&raceBango=09";

const makeContainerHarness = (
  responseBody: string,
  watchEnabled: string,
  queuePresent: boolean,
): ContainerHarness => {
  const containerFetchMock = vi.fn(
    async () => new Response(responseBody, { headers: { "Content-Type": "application/x-ndjson" } }),
  );
  const scheduleMock = vi.fn(async () => ({ taskId: "scheduled-watch" }));
  const startAndWaitForPortsMock = vi.fn(async () => undefined);
  const destroyMock = vi.fn(async () => undefined);
  const featureCachePutMock = vi.fn(async () => undefined);
  const storageDeleteMock = vi.fn(async () => true);
  const storageGetMock = vi.fn(async () => undefined);
  const storageListMock = vi.fn(async () => new Map());
  const storagePutMock = vi.fn(async () => undefined);
  const queueSendMock = vi.fn(async () => undefined);
  const runtimeContainer = {
    getTcpPort: vi.fn(() => ({ fetch: containerFetchMock })),
    running: true,
  };
  const container = Reflect.construct(FinishPositionPredictContainer, []);
  if (!(container instanceof FinishPositionPredictContainer)) {
    throw new Error("Failed to construct test Container");
  }
  Object.defineProperties(container, {
    containerFetch: { value: containerFetchMock },
    ctx: {
      value: {
        container: runtimeContainer,
        storage: {
          delete: storageDeleteMock,
          get: storageGetMock,
          list: storageListMock,
          put: storagePutMock,
        },
        waitUntil: vi.fn(),
      },
    },
    destroy: { value: destroyMock },
    env: {
      value: {
        FEATURES_CACHE: { put: featureCachePutMock },
        FOCUSED_FULL_COMPLETION_QUEUE: queuePresent ? { send: queueSendMock } : undefined,
        FOCUSED_FULL_WATCH_ENABLED: watchEnabled,
        NEON_DATABASE_URL: "postgres://output/db",
        PREDICT_DAYS_AHEAD: "0",
        PREDICT_RUN_COORDINATOR: {
          get: vi.fn(() => ({ fetch: vi.fn(async () => Response.json({ ok: true })) })),
          idFromName: vi.fn(() => ({ name: "predict-run-coordinator" })),
        },
        TRIGGER_TOKEN: "secret-token",
      },
    },
    renewActivityTimeout: { value: vi.fn() },
    schedule: { value: scheduleMock },
    startAndWaitForPorts: { value: startAndWaitForPortsMock },
  });
  return {
    container,
    containerFetchMock,
    destroyMock,
    featureCachePutMock,
    queueSendMock,
    runtimeContainer,
    scheduleMock,
    startAndWaitForPortsMock,
    storageDeleteMock,
    storageGetMock,
    storageListMock,
    storagePutMock,
  };
};

test("returns missing prewarm status without starting a stopped Container", async () => {
  const harness = makeContainerHarness("", "0", false);
  harness.runtimeContainer.running = false;
  Object.defineProperty(harness.container, "getState", {
    value: vi.fn(async () => ({ lastChange: 1, status: "running" })),
  });

  const response = await harness.container.fetch(
    new Request("http://do/prewarm-day-base-status?category=nar&runDate=20260826"),
  );

  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toStrictEqual({
    error: null,
    finishedAtMs: null,
    flightKey: "nar:20260826",
    generation: 0,
    startedAtMs: null,
    status: "missing",
  });
  expect(harness.runtimeContainer.getTcpPort).not.toHaveBeenCalled();
  expect(harness.startAndWaitForPortsMock).not.toHaveBeenCalled();
  expect(harness.containerFetchMock).not.toHaveBeenCalled();
});

test("forwards prewarm status only to an already-running Container process", async () => {
  const harness = makeContainerHarness(
    '{"flightKey":"jra:20260826","generation":3,"status":"running"}\n',
    "0",
    false,
  );
  const request = new Request("http://do/prewarm-day-base-status?category=jra&runDate=20260826");

  const response = await harness.container.fetch(request);

  expect(response.status).toBe(200);
  expect(await response.text()).toBe(
    '{"flightKey":"jra:20260826","generation":3,"status":"running"}\n',
  );
  expect(harness.runtimeContainer.getTcpPort).toHaveBeenCalledWith(8080);
  const forwardedRequest = harness.containerFetchMock.mock.calls[0]?.[0];
  expect(forwardedRequest).toBeInstanceOf(Request);
  expect(forwardedRequest?.url).toBe(request.url);
  expect(forwardedRequest?.method).toBe("GET");
  expect(harness.startAndWaitForPortsMock).not.toHaveBeenCalled();
});

test("reports a read-only status forwarding failure without destroying the live Container", async () => {
  const harness = makeContainerHarness("", "0", false);
  harness.containerFetchMock.mockRejectedValueOnce(new Error("status port unavailable"));

  const response = await harness.container.fetch(
    new Request("http://do/prewarm-day-base-status?category=ban-ei&runDate=20260826"),
  );

  expect(response.status).toBe(503);
  await expect(response.json()).resolves.toStrictEqual({
    error: "Error: status port unavailable",
    status: "unavailable",
  });
  expect(harness.startAndWaitForPortsMock).not.toHaveBeenCalled();
  expect(harness.destroyMock).not.toHaveBeenCalled();
});

test("bounds a read-only status request without starting or destroying the Container", async () => {
  vi.useFakeTimers();
  const harness = makeContainerHarness("", "0", false);
  harness.containerFetchMock.mockImplementationOnce(() => new Promise(() => undefined));

  const responsePromise = harness.container.fetch(
    new Request("http://do/prewarm-day-base-status?category=jra&runDate=20260826"),
  );
  await vi.advanceTimersByTimeAsync(5_000);
  const response = await responsePromise;

  expect(response.status).toBe(503);
  await expect(response.json()).resolves.toStrictEqual({
    error: "Error: Container delivery readiness exceeded 5000ms",
    status: "unavailable",
  });
  expect(harness.startAndWaitForPortsMock).not.toHaveBeenCalled();
  expect(harness.destroyMock).not.toHaveBeenCalled();
  vi.useRealTimers();
});

test("uses a private admin path for stale Durable Object state purge", () => {
  expect(PREDICT_DO_INTERNAL_PURGE_PATH).toBe("/__admin/purge-unused-state");
});

test("buildLegacyPredictContainerEnvVars fixes the legacy role after inherited variables", () => {
  const envVars = buildLegacyPredictContainerEnvVars({
    env: {
      NEON_DATABASE_URL: "postgres://legacy-output/db",
      PIPELINE_TOTAL_TIMEOUT_SECONDS: "1800",
      PREDICT_DAYS_AHEAD: "1",
    },
    inheritedEnvVars: {
      CALLER_VALUE: "preserved",
      PREDICT_CONTAINER_ROLE: "race-chain",
    },
  });

  expect(envVars.PREDICT_CONTAINER_ROLE).toBe("legacy");
  expect(envVars.CALLER_VALUE).toBe("preserved");
  expect(envVars.NEON_DATABASE_URL).toBe("postgres://legacy-output/db");
  expect(envVars.PIPELINE_TOTAL_TIMEOUT_SECONDS).toBe("1800");
});

test("buildRaceChainPredictContainerEnvVars fixes the role after inherited variables", () => {
  const envVars = buildRaceChainPredictContainerEnvVars({
    env: {
      DAY_BASE_SPLIT_ENABLED: "jra,nar,ban-ei",
      NEON_DATABASE_URL: "postgres://race-output/db",
      PIPELINE_TOTAL_TIMEOUT_SECONDS: "1800",
      PREDICT_DAYS_AHEAD: "0",
      SOURCE_DATABASE_URL: "r2-catalog://pc-keiba",
    },
    inheritedEnvVars: {
      PREDICT_CONTAINER_ROLE: "legacy",
    },
  });

  expect(envVars.PREDICT_CONTAINER_ROLE).toBe("race-chain");
  expect(envVars.DAY_BASE_SPLIT_ENABLED).toBe("jra,nar,ban-ei");
  expect(envVars.SOURCE_DATABASE_URL).toBe("r2-catalog://pc-keiba");
});

test("buildRaceChainPredictContainerEnvVars keeps production defaults fail closed", () => {
  const envVars = buildRaceChainPredictContainerEnvVars({
    env: {
      NEON_DATABASE_URL: "postgres://output/db",
      PIPELINE_TOTAL_TIMEOUT_SECONDS: "1800",
      PREDICT_DAYS_AHEAD: "0",
    },
    inheritedEnvVars: {
      MODELS_DIR: "/caller-models",
      PREDICT_SERVE_MODE: "cli",
    },
  });

  expect(envVars.MODELS_DIR).toBe("/models");
  expect(envVars.PREDICT_SERVE_MODE).toBe("http");
  expect(envVars.PIPELINE_TOTAL_TIMEOUT_SECONDS).toBe("1800");
  expect(envVars.PYTHONUNBUFFERED).toBe("1");
  expect(envVars.SOURCE_DATABASE_URL).toBe("");
});

test("keeps the legacy accepted response unchanged while the watch gate is off", async () => {
  const harness = makeContainerHarness('{"type":"result","status":"accepted"}\n', "0", false);

  const response = await harness.container.fetch(new Request(FOCUSED_PREDICT_URL));

  expect(response.status).toBe(200);
  expect(response.headers.get(WATCH_RESPONSE_HEADER)).toBe(null);
  expect(await response.text()).toBe('{"type":"result","status":"accepted"}\n');
  expect(harness.scheduleMock).not.toHaveBeenCalled();
  expect(harness.startAndWaitForPortsMock).toHaveBeenCalledWith([8080], {
    abort: expect.any(AbortSignal),
    instanceGetTimeoutMS: 20_000,
    portReadyTimeoutMS: 60_000,
  });
});

test("requires the day-base R2 commit before the prewarm response stream succeeds", async () => {
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const consoleLog = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const resultLine = `${JSON.stringify({
    type: "result",
    category: "nar",
    racesPredicted: 1,
    parquetBase64: "bWFpbg==",
    parquetKey: "feat-daybase/catalog-v1/nar/20260826/features.parquet",
  })}\n`;
  const harness = makeContainerHarness(resultLine, "0", false);
  harness.featureCachePutMock.mockRejectedValueOnce(new Error("commit unavailable"));

  const response = await harness.container.fetch(
    new Request("http://do/prewarm-day-base?category=nar&runDate=20260826"),
  );

  await expect(response.text()).rejects.toThrow("commit unavailable");
  expect(harness.featureCachePutMock).toHaveBeenCalledTimes(1);
  expect(consoleError).toHaveBeenCalledWith(
    expect.stringMatching(/^\[daybase-r2-commit\] failed key=/),
  );
  consoleLog.mockRestore();
  consoleError.mockRestore();
});

test("destroys a replacement that cannot become port-ready instead of holding the slot", async () => {
  const error = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const harness = makeContainerHarness("", "0", false);
  harness.startAndWaitForPortsMock.mockRejectedValueOnce(new Error("port 8080 not ready"));

  const response = await harness.container.fetch(new Request(FOCUSED_PREDICT_URL));

  expect(response.status).toBe(502);
  expect(harness.containerFetchMock).not.toHaveBeenCalled();
  expect(harness.destroyMock).toHaveBeenCalledOnce();
  expect(error).toHaveBeenCalledWith(
    expect.stringContaining("fetch failed path=/predict category=jra"),
  );
  log.mockRestore();
  error.mockRestore();
});

test("hard-stops an SDK readiness call that ignores its abort signal", async () => {
  vi.useFakeTimers();
  const error = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const harness = makeContainerHarness("", "0", false);
  harness.startAndWaitForPortsMock.mockImplementationOnce(() => new Promise(() => undefined));

  const responsePromise = harness.container.fetch(new Request(FOCUSED_PREDICT_URL));
  await vi.advanceTimersByTimeAsync(65_000);
  const response = await responsePromise;

  expect(response.status).toBe(502);
  expect(await response.json()).toStrictEqual({
    detail: "Error: Container delivery readiness exceeded 65000ms",
    error: "Container start failed",
  });
  expect(harness.containerFetchMock).not.toHaveBeenCalled();
  expect(harness.destroyMock).toHaveBeenCalledOnce();
  error.mockRestore();
  vi.useRealTimers();
});

test("returns after the cleanup deadline when destroy also hangs", async () => {
  vi.useFakeTimers();
  const error = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const harness = makeContainerHarness("", "0", false);
  harness.startAndWaitForPortsMock.mockRejectedValueOnce(new Error("port 8080 not ready"));
  harness.destroyMock.mockImplementationOnce(() => new Promise(() => undefined));

  const responsePromise = harness.container.fetch(new Request(FOCUSED_PREDICT_URL));
  await vi.advanceTimersByTimeAsync(15_000);
  const response = await responsePromise;

  expect(response.status).toBe(502);
  expect(error).toHaveBeenCalledWith(
    expect.stringContaining("failed-delivery destroy failed path=/predict category=jra"),
  );
  error.mockRestore();
  vi.useRealTimers();
});

test("hard-stops a container HTTP fetch that never returns headers", async () => {
  vi.useFakeTimers();
  const error = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const harness = makeContainerHarness("", "0", false);
  harness.containerFetchMock.mockImplementationOnce(() => new Promise(() => undefined));

  const responsePromise = harness.container.fetch(new Request(FOCUSED_PREDICT_URL));
  await vi.advanceTimersByTimeAsync(65_000);
  const response = await responsePromise;

  expect(response.status).toBe(502);
  expect(harness.startAndWaitForPortsMock).toHaveBeenCalledOnce();
  expect(harness.destroyMock).toHaveBeenCalledOnce();
  expect(error).toHaveBeenCalledWith(
    expect.stringContaining("fetch failed path=/predict category=jra"),
  );
  error.mockRestore();
  vi.useRealTimers();
});

test("enqueues the first durable watch tick and returns its ID for a gated accepted response", async () => {
  const harness = makeContainerHarness('{"type":"result","status":"accepted"}\n', "1", true);
  const request = new Request(FOCUSED_PREDICT_URL, {
    headers: { [WATCH_REQUEST_HEADER]: JSON.stringify(WATCH_PAYLOAD) },
  });

  const response = await harness.container.fetch(request);
  const watchId = response.headers.get(WATCH_RESPONSE_HEADER);

  expect(response.status).toBe(200);
  expect(watchId).toBe("focused-full:20260824:jra:05:09:source-message-1");
  expect(harness.queueSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      ...WATCH_PAYLOAD,
      type: "focused-full-watch-tick",
      watchId,
    }),
    { delaySeconds: 30 },
  );
  expect(harness.scheduleMock).not.toHaveBeenCalled();
});

test("reuses the source generation watch ID when an accepted response is delivered twice", async () => {
  const harness = makeContainerHarness('{"type":"result","status":"accepted"}\n', "1", true);
  const makeRequest = (): Request =>
    new Request(FOCUSED_PREDICT_URL, {
      headers: { [WATCH_REQUEST_HEADER]: JSON.stringify(WATCH_PAYLOAD) },
    });

  const first = await harness.container.fetch(makeRequest());
  const second = await harness.container.fetch(makeRequest());

  expect(first.headers.get(WATCH_RESPONSE_HEADER)).toBe(WATCH_PAYLOAD.watchId);
  expect(second.headers.get(WATCH_RESPONSE_HEADER)).toBe(WATCH_PAYLOAD.watchId);
  expect(harness.queueSendMock).toHaveBeenCalledTimes(2);
  expect(harness.queueSendMock.mock.calls[0]?.[0]).toEqual(
    expect.objectContaining({ watchId: WATCH_PAYLOAD.watchId }),
  );
  expect(harness.queueSendMock.mock.calls[1]?.[0]).toEqual(
    expect.objectContaining({ watchId: WATCH_PAYLOAD.watchId }),
  );
});

test("does not register a watch for a non-accepted focused response", async () => {
  const harness = makeContainerHarness('{"type":"result","status":"success"}\n', "1", true);
  const request = new Request(FOCUSED_PREDICT_URL, {
    headers: { [WATCH_REQUEST_HEADER]: JSON.stringify(WATCH_PAYLOAD) },
  });

  const response = await harness.container.fetch(request);

  expect(response.headers.get(WATCH_RESPONSE_HEADER)).toBe(null);
  expect(harness.queueSendMock).not.toHaveBeenCalled();
  expect(harness.scheduleMock).not.toHaveBeenCalled();
});

test("does not return an accepted watch header when the initial durable tick send fails", async () => {
  const error = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const harness = makeContainerHarness('{"type":"result","status":"accepted"}\n', "1", true);
  harness.queueSendMock.mockRejectedValueOnce(new Error("completion queue unavailable"));

  const response = await harness.container.fetch(
    new Request(FOCUSED_PREDICT_URL, {
      headers: { [WATCH_REQUEST_HEADER]: JSON.stringify(WATCH_PAYLOAD) },
    }),
  );

  expect(response.status).toBe(502);
  expect(response.headers.get(WATCH_RESPONSE_HEADER)).toBe(null);
  expect(harness.scheduleMock).not.toHaveBeenCalled();
  error.mockRestore();
});

test("rejects invalid gated metadata but rolls back to legacy polling without the Queue binding", async () => {
  const invalid = makeContainerHarness('{"type":"result","status":"accepted"}\n', "1", true);
  const missingQueue = makeContainerHarness('{"type":"result","status":"accepted"}\n', "1", false);

  const invalidResponse = await invalid.container.fetch(new Request(FOCUSED_PREDICT_URL));
  const missingQueueResponse = await missingQueue.container.fetch(new Request(FOCUSED_PREDICT_URL));

  expect(invalidResponse.status).toBe(400);
  expect(missingQueueResponse.status).toBe(200);
  expect(missingQueueResponse.headers.get(WATCH_RESPONSE_HEADER)).toBe(null);
  expect(invalid.containerFetchMock).not.toHaveBeenCalled();
  expect(missingQueue.containerFetchMock).toHaveBeenCalledTimes(1);
  expect(missingQueue.scheduleMock).not.toHaveBeenCalled();
});

test("purges without touching SDK storage or alarms", async () => {
  const harness = makeContainerHarness("", "0", false);
  const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  const response = await harness.container.fetch(
    new Request(`http://do${PREDICT_DO_INTERNAL_PURGE_PATH}`, {
      headers: { authorization: "Bearer secret-token" },
    }),
  );

  expect(response.status).toBe(200);
  expect(harness.destroyMock).toHaveBeenCalledOnce();
  expect(harness.storageListMock).not.toHaveBeenCalled();
  expect(harness.storageDeleteMock).not.toHaveBeenCalled();
  expect(harness.containerFetchMock).not.toHaveBeenCalled();
  expect(log).toHaveBeenCalledTimes(2);
  expect(warn).not.toHaveBeenCalled();
  log.mockRestore();
  warn.mockRestore();
});

test("logs authorized stop as info while retaining warnings for unauthorized requests", async () => {
  const authorized = makeContainerHarness("", "0", false);
  const unauthorized = makeContainerHarness("", "0", false);
  const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  const accepted = await authorized.container.fetch(
    new Request("http://do/__admin/stop-container", {
      headers: { authorization: "Bearer secret-token" },
    }),
  );
  const rejected = await unauthorized.container.fetch(
    new Request("http://do/__admin/stop-container"),
  );

  expect(accepted.status).toBe(200);
  expect(rejected.status).toBe(401);
  expect(authorized.destroyMock).toHaveBeenCalledOnce();
  expect(unauthorized.destroyMock).not.toHaveBeenCalled();
  expect(log).toHaveBeenCalledTimes(2);
  expect(warn).toHaveBeenCalledOnce();
  log.mockRestore();
  warn.mockRestore();
});
