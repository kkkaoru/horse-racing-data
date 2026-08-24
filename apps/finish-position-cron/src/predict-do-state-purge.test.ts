// Run with bun. Tests for bounded stale predict Durable Object cleanup.

import { expect, test, vi } from "vitest";
import {
  handlePredictDoStatePurge,
  MAX_PREDICT_DO_PURGE_BATCH_SIZE,
  purgePredictDoStorage,
} from "./predict-do-state-purge";

const STALE_ID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SECOND_STALE_ID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const THIRD_STALE_ID = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const PROTECTED_ID = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const UPPERCASE_ID = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

test("destroys the Container and deletes only application-owned state", async () => {
  const callOrder: string[] = [];
  await purgePredictDoStorage({
    deleteApplicationState: vi.fn(async () => {
      callOrder.push("delete-application-state");
    }),
    destroy: vi.fn(async () => {
      callOrder.push("destroy");
    }),
  });
  expect(callOrder).toStrictEqual(["destroy", "delete-application-state"]);
});

test("propagates an application-state cleanup failure after destroying the Container", async () => {
  const destroy = vi.fn(async () => undefined);
  await expect(
    purgePredictDoStorage({
      deleteApplicationState: vi.fn(async () => {
        throw new Error("application state cleanup failed");
      }),
      destroy,
    }),
  ).rejects.toThrow("application state cleanup failed");
  expect(destroy).toHaveBeenCalledOnce();
});

const makeRequest = (token: string | null, body: unknown): Request =>
  new Request("https://cron.example/api/admin/purge-unused-predict-do-state", {
    body: JSON.stringify(body),
    headers: token === null ? {} : { authorization: `Bearer ${token}` },
    method: "POST",
  });

test("rejects an unauthenticated purge before resolving protected IDs", async () => {
  const resolveIdFromName = vi.fn(() => PROTECTED_ID);
  const purgeId = vi.fn(async () => Response.json({ ok: true }));
  const response = await handlePredictDoStatePurge(
    makeRequest(null, { dryRun: true, ids: [STALE_ID] }),
    {
      purgeId,
      resolveIdFromName,
      triggerToken: "secret-token",
    },
  );
  expect(response.status).toBe(401);
  expect(resolveIdFromName).not.toHaveBeenCalled();
  expect(purgeId).not.toHaveBeenCalled();
});

test("rejects malformed, duplicate, uppercase, empty, and oversized ID batches", async () => {
  const resolveIdFromName = vi.fn(() => PROTECTED_ID);
  const purgeId = vi.fn(async () => Response.json({ ok: true }));
  const malformedResponse = await handlePredictDoStatePurge(
    makeRequest("secret-token", { dryRun: true, ids: ["not-an-id"] }),
    { purgeId, resolveIdFromName, triggerToken: "secret-token" },
  );
  const duplicateResponse = await handlePredictDoStatePurge(
    makeRequest("secret-token", { dryRun: true, ids: [STALE_ID, STALE_ID] }),
    { purgeId, resolveIdFromName, triggerToken: "secret-token" },
  );
  const uppercaseResponse = await handlePredictDoStatePurge(
    makeRequest("secret-token", { dryRun: true, ids: [UPPERCASE_ID] }),
    { purgeId, resolveIdFromName, triggerToken: "secret-token" },
  );
  const emptyResponse = await handlePredictDoStatePurge(
    makeRequest("secret-token", { dryRun: true, ids: [] }),
    { purgeId, resolveIdFromName, triggerToken: "secret-token" },
  );
  const oversizedResponse = await handlePredictDoStatePurge(
    makeRequest("secret-token", {
      dryRun: true,
      ids: Array.from({ length: MAX_PREDICT_DO_PURGE_BATCH_SIZE + 1 }, (_, index) =>
        index.toString(16).padStart(64, "0"),
      ),
    }),
    { purgeId, resolveIdFromName, triggerToken: "secret-token" },
  );
  expect(malformedResponse.status).toBe(400);
  expect(duplicateResponse.status).toBe(400);
  expect(uppercaseResponse.status).toBe(400);
  expect(emptyResponse.status).toBe(400);
  expect(oversizedResponse.status).toBe(400);
  expect(purgeId).not.toHaveBeenCalled();
});

test("requires dryRun to be an explicit boolean", async () => {
  const response = await handlePredictDoStatePurge(
    makeRequest("secret-token", { ids: [STALE_ID] }),
    {
      purgeId: vi.fn(async () => Response.json({ ok: true })),
      resolveIdFromName: vi.fn(() => PROTECTED_ID),
      triggerToken: "secret-token",
    },
  );
  expect(response.status).toBe(400);
});

test("fails the whole batch closed when a current routing ID is present", async () => {
  const purgeId = vi.fn(async () => Response.json({ ok: true }));
  const response = await handlePredictDoStatePurge(
    makeRequest("secret-token", { dryRun: false, ids: [STALE_ID, PROTECTED_ID] }),
    {
      purgeId,
      resolveIdFromName: vi.fn(() => PROTECTED_ID),
      triggerToken: "secret-token",
    },
  );
  expect(response.status).toBe(409);
  expect(await response.json()).toStrictEqual({
    dryRun: false,
    error: "current predict Durable Object IDs are protected",
    ok: false,
    protectedIds: [PROTECTED_ID],
    requestedCount: 2,
  });
  expect(purgeId).not.toHaveBeenCalled();
});

test("dry-run reports eligible IDs without invoking any Durable Object", async () => {
  const purgeId = vi.fn(async () => Response.json({ ok: true }));
  const response = await handlePredictDoStatePurge(
    makeRequest("secret-token", { dryRun: true, ids: [STALE_ID, SECOND_STALE_ID] }),
    {
      purgeId,
      resolveIdFromName: vi.fn(() => PROTECTED_ID),
      triggerToken: "secret-token",
    },
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    dryRun: true,
    ok: true,
    requestedCount: 2,
    results: [
      { id: STALE_ID, status: "eligible" },
      { id: SECOND_STALE_ID, status: "eligible" },
    ],
  });
  expect(purgeId).not.toHaveBeenCalled();
});

test("purges every eligible ID and returns explicit success counts", async () => {
  const purgeId = vi.fn(async () => Response.json({ ok: true }));
  const response = await handlePredictDoStatePurge(
    makeRequest("secret-token", { dryRun: false, ids: [STALE_ID, SECOND_STALE_ID] }),
    {
      purgeId,
      resolveIdFromName: vi.fn(() => PROTECTED_ID),
      triggerToken: "secret-token",
    },
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    dryRun: false,
    failedCount: 0,
    ok: true,
    purgedCount: 2,
    requestedCount: 2,
    results: [
      { id: STALE_ID, status: "purged" },
      { id: SECOND_STALE_ID, status: "purged" },
    ],
  });
  expect(purgeId).toHaveBeenCalledTimes(2);
  expect(purgeId).toHaveBeenNthCalledWith(1, STALE_ID);
  expect(purgeId).toHaveBeenNthCalledWith(2, SECOND_STALE_ID);
});

test("limits stale Durable Object purges to two concurrent operations", async () => {
  const firstPurge = Promise.withResolvers<Response>();
  const secondPurge = Promise.withResolvers<Response>();
  const purgeId = vi
    .fn<(id: string) => Promise<Response>>()
    .mockImplementationOnce(() => firstPurge.promise)
    .mockImplementationOnce(() => secondPurge.promise)
    .mockResolvedValueOnce(Response.json({ ok: true }));
  const responsePromise = handlePredictDoStatePurge(
    makeRequest("secret-token", {
      dryRun: false,
      ids: [STALE_ID, SECOND_STALE_ID, THIRD_STALE_ID],
    }),
    {
      purgeId,
      resolveIdFromName: vi.fn(() => PROTECTED_ID),
      triggerToken: "secret-token",
    },
  );
  await vi.waitFor(() => expect(purgeId).toHaveBeenCalledTimes(2));
  expect(purgeId).not.toHaveBeenCalledWith(THIRD_STALE_ID);
  firstPurge.resolve(Response.json({ ok: true }));
  secondPurge.resolve(Response.json({ ok: true }));
  expect((await responsePromise).status).toBe(200);
  expect(purgeId).toHaveBeenCalledTimes(3);
  expect(purgeId).toHaveBeenNthCalledWith(3, THIRD_STALE_ID);
});

test("returns multi-status with per-ID failures for HTTP and thrown errors", async () => {
  const purgeId = vi
    .fn<(id: string) => Promise<Response>>()
    .mockResolvedValueOnce(new Response(null, { status: 503 }))
    .mockRejectedValueOnce(new Error("binding unavailable"));
  const response = await handlePredictDoStatePurge(
    makeRequest("secret-token", { dryRun: false, ids: [STALE_ID, SECOND_STALE_ID] }),
    {
      purgeId,
      resolveIdFromName: vi.fn(() => PROTECTED_ID),
      triggerToken: "secret-token",
    },
  );
  expect(response.status).toBe(207);
  expect(await response.json()).toStrictEqual({
    dryRun: false,
    failedCount: 2,
    ok: false,
    purgedCount: 0,
    requestedCount: 2,
    results: [
      { error: "Durable Object purge returned HTTP 503", id: STALE_ID, status: "failed" },
      { error: "Error: binding unavailable", id: SECOND_STALE_ID, status: "failed" },
    ],
  });
});
