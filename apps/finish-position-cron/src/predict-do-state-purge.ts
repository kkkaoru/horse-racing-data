// Run with bun. Authenticated, bounded cleanup for stale Container Durable
// Object storage. The caller supplies namespace operations so validation and
// result reporting remain unit-testable outside the Workers runtime.

import { listAllowedPredictDoNames } from "./predict-do-shard";
import { isAuthorized } from "./trigger";

interface PredictDoStatePurgeDependencies {
  purgeId(id: string): Promise<Response>;
  resolveIdFromName(name: string): string;
  triggerToken: string;
}

interface PredictDoStatePurgeRequest {
  dryRun: boolean;
  ids: string[];
}

interface PredictDoStatePurgeResult {
  id: string;
  status: "eligible" | "failed" | "purged";
  error?: string;
}

export interface PredictDoStoragePurgeOperations {
  deleteApplicationState(): Promise<void>;
  destroy(): Promise<void>;
}

const AUTHORIZATION_HEADER = "authorization";
const DO_ID_PATTERN = /^[0-9a-f]{64}$/u;
export const PREDICT_DO_INTERNAL_PURGE_PATH = "/__admin/purge-unused-state";
export const MAX_PREDICT_DO_PURGE_BATCH_SIZE = 25;
export const PREDICT_DO_PURGE_CONCURRENCY: number = 2;
const HTTP_BAD_REQUEST = 400;
const HTTP_CONFLICT = 409;
const HTTP_MULTI_STATUS = 207;
const HTTP_OK = 200;
const HTTP_UNAUTHORIZED = 401;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const parsePurgeIds = (value: unknown): string[] | null => {
  if (!Array.isArray(value)) return null;
  if (value.length === 0 || value.length > MAX_PREDICT_DO_PURGE_BATCH_SIZE) return null;
  if (!value.every((id) => typeof id === "string" && DO_ID_PATTERN.test(id))) return null;
  const ids = value.filter((id): id is string => typeof id === "string");
  return new Set(ids).size === ids.length ? ids : null;
};

const parsePurgeRequest = (value: unknown): PredictDoStatePurgeRequest | null => {
  if (!isRecord(value) || typeof value.dryRun !== "boolean") return null;
  const ids = parsePurgeIds(value.ids);
  return ids === null ? null : { dryRun: value.dryRun, ids };
};

const buildProtectedIdSet = (
  resolveIdFromName: PredictDoStatePurgeDependencies["resolveIdFromName"],
): ReadonlySet<string> => new Set(listAllowedPredictDoNames().map(resolveIdFromName));

const purgeOne = async (
  id: string,
  purgeId: PredictDoStatePurgeDependencies["purgeId"],
): Promise<PredictDoStatePurgeResult> => {
  try {
    const response = await purgeId(id);
    return response.ok
      ? { id, status: "purged" }
      : { error: `Durable Object purge returned HTTP ${response.status}`, id, status: "failed" };
  } catch (error) {
    return { error: String(error), id, status: "failed" };
  }
};

const purgeInBoundedBatches = (
  ids: string[],
  purgeId: PredictDoStatePurgeDependencies["purgeId"],
): Promise<PredictDoStatePurgeResult[]> =>
  Array.from({ length: Math.ceil(ids.length / PREDICT_DO_PURGE_CONCURRENCY) }, (_, index) =>
    ids.slice(index * PREDICT_DO_PURGE_CONCURRENCY, (index + 1) * PREDICT_DO_PURGE_CONCURRENCY),
  ).reduce<Promise<PredictDoStatePurgeResult[]>>(
    async (completed, batch) => [
      ...(await completed),
      ...(await Promise.all(batch.map((id) => purgeOne(id, purgeId)))),
    ],
    Promise.resolve([]),
  );

export const purgePredictDoStorage = async (
  operations: PredictDoStoragePurgeOperations,
): Promise<void> => {
  await operations.destroy();
  // Container owns its alarm and schedule storage. Purge only keys explicitly
  // owned by this application; deleting all DO storage corrupts SDK lifecycle
  // state and is unsupported by the Container API.
  await operations.deleteApplicationState();
};

export const handlePredictDoStatePurge = async (
  request: Request,
  dependencies: PredictDoStatePurgeDependencies,
): Promise<Response> => {
  if (!isAuthorized(request.headers.get(AUTHORIZATION_HEADER), dependencies.triggerToken)) {
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }

  const parsed = parsePurgeRequest(await request.json());
  if (parsed === null) {
    return Response.json(
      {
        error: `invalid request: ids must contain 1-${MAX_PREDICT_DO_PURGE_BATCH_SIZE} unique lowercase 64-hex Durable Object IDs and dryRun must be boolean`,
        ok: false,
      },
      { status: HTTP_BAD_REQUEST },
    );
  }

  const protectedIds = buildProtectedIdSet((name) => dependencies.resolveIdFromName(name));
  const requestedProtectedIds = parsed.ids.filter((id) => protectedIds.has(id));
  if (requestedProtectedIds.length > 0) {
    return Response.json(
      {
        dryRun: parsed.dryRun,
        error: "current predict Durable Object IDs are protected",
        ok: false,
        protectedIds: requestedProtectedIds,
        requestedCount: parsed.ids.length,
      },
      { status: HTTP_CONFLICT },
    );
  }

  if (parsed.dryRun) {
    return Response.json({
      dryRun: true,
      ok: true,
      requestedCount: parsed.ids.length,
      results: parsed.ids.map((id) => ({ id, status: "eligible" })),
    });
  }

  const results = await purgeInBoundedBatches(parsed.ids, (id) => dependencies.purgeId(id));
  const failedCount = results.filter((result) => result.status === "failed").length;
  return Response.json(
    {
      dryRun: false,
      failedCount,
      ok: failedCount === 0,
      purgedCount: results.length - failedCount,
      requestedCount: parsed.ids.length,
      results,
    },
    { status: failedCount === 0 ? HTTP_OK : HTTP_MULTI_STATUS },
  );
};
