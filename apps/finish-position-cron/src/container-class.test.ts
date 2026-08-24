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

interface ContainerHarness {
  container: FinishPositionPredictContainer;
  containerFetchMock: ReturnType<typeof vi.fn>;
  destroyMock: ReturnType<typeof vi.fn>;
  queueSendMock: ReturnType<typeof vi.fn>;
  scheduleMock: ReturnType<typeof vi.fn>;
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
  const destroyMock = vi.fn(async () => undefined);
  const storageDeleteMock = vi.fn(async () => true);
  const storageGetMock = vi.fn(async () => undefined);
  const storageListMock = vi.fn(async () => new Map());
  const storagePutMock = vi.fn(async () => undefined);
  const queueSendMock = vi.fn(async () => undefined);
  const container = Reflect.construct(FinishPositionPredictContainer, []);
  if (!(container instanceof FinishPositionPredictContainer)) {
    throw new Error("Failed to construct test Container");
  }
  Object.defineProperties(container, {
    containerFetch: { value: containerFetchMock },
    ctx: {
      value: {
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
  });
  return {
    container,
    containerFetchMock,
    destroyMock,
    queueSendMock,
    scheduleMock,
    storageDeleteMock,
    storageGetMock,
    storageListMock,
    storagePutMock,
  };
};

test("uses a private admin path for stale Durable Object state purge", () => {
  expect(PREDICT_DO_INTERNAL_PURGE_PATH).toBe("/__admin/purge-unused-state");
});

test("buildLegacyPredictContainerEnvVars fixes the legacy role after inherited variables", () => {
  const envVars = buildLegacyPredictContainerEnvVars({
    env: {
      NEON_DATABASE_URL: "postgres://legacy-output/db",
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
});

test("buildRaceChainPredictContainerEnvVars fixes the role after inherited variables", () => {
  const envVars = buildRaceChainPredictContainerEnvVars({
    env: {
      DAY_BASE_SPLIT_ENABLED: "jra,nar,ban-ei",
      NEON_DATABASE_URL: "postgres://race-output/db",
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
      PREDICT_DAYS_AHEAD: "0",
    },
    inheritedEnvVars: {
      MODELS_DIR: "/caller-models",
      PREDICT_SERVE_MODE: "cli",
    },
  });

  expect(envVars.MODELS_DIR).toBe("/models");
  expect(envVars.PREDICT_SERVE_MODE).toBe("http");
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
