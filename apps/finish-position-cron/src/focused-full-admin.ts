// Run with bun. Operator routes that inspect focused-full lanes and clear verified-stale commit barriers.
import { isAuthorized } from "./trigger";
import type { Env } from "./types";

const ADMIN_FOCUSED_FULL_DIAGNOSTICS_PATH = "/api/admin/focused-full-diagnostics";
const ADMIN_CLEAR_FOCUSED_FULL_COMMITS_PATH = "/api/admin/clear-focused-full-commits";
const DO_DIAGNOSTICS_URL = "http://do/__admin/focused-full-diagnostics";
const DO_CLEAR_COMMITS_URL = "http://do/__admin/clear-focused-full-commits";
const COORDINATOR_DO_NAME = "predict-run-coordinator";
const MAX_CLEAR_WORK_KEYS = 100;
const FOCUSED_FULL_WORK_KEY_PATTERN: RegExp =
  /^focused-full:\d{8}:(?:jra|nar|ban-ei):\d{2}:\d{2}$/u;
const HTTP_BAD_REQUEST = 400;
const HTTP_UNAUTHORIZED = 401;

type AdminEnv = Pick<Env, "PREDICT_RUN_COORDINATOR" | "TRIGGER_TOKEN">;

const coordinatorStub = (env: AdminEnv): DurableObjectStub =>
  env.PREDICT_RUN_COORDINATOR.get(env.PREDICT_RUN_COORDINATOR.idFromName(COORDINATOR_DO_NAME));

const unauthorized = (): Response =>
  Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });

export const parseClearWorkKeys = (body: unknown): string[] | null => {
  if (typeof body !== "object" || body === null || !("workKeys" in body)) return null;
  const { workKeys } = body;
  if (!Array.isArray(workKeys) || workKeys.length === 0 || workKeys.length > MAX_CLEAR_WORK_KEYS)
    return null;
  return workKeys.every(
    (key): key is string => typeof key === "string" && FOCUSED_FULL_WORK_KEY_PATTERN.test(key),
  )
    ? workKeys
    : null;
};

const handleDiagnostics = async (request: Request, env: AdminEnv): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) return unauthorized();
  return coordinatorStub(env).fetch(new Request(DO_DIAGNOSTICS_URL, { method: "GET" }));
};

const handleClearCommits = async (request: Request, env: AdminEnv): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) return unauthorized();
  const workKeys = parseClearWorkKeys(await request.json().catch(() => null));
  if (workKeys === null)
    return Response.json({ error: "invalid workKeys", ok: false }, { status: HTTP_BAD_REQUEST });
  console.warn(`[predict-worker] admin-clear-focused-full-commits workKeys=${workKeys.join(",")}`);
  return coordinatorStub(env).fetch(
    new Request(DO_CLEAR_COMMITS_URL, { body: JSON.stringify({ workKeys }), method: "POST" }),
  );
};

const ROUTES = new Map<string, (request: Request, env: AdminEnv) => Promise<Response>>([
  [`GET:${ADMIN_FOCUSED_FULL_DIAGNOSTICS_PATH}`, handleDiagnostics],
  [`POST:${ADMIN_CLEAR_FOCUSED_FULL_COMMITS_PATH}`, handleClearCommits],
]);

// Returns null when the request is not one of these operator routes.
export const routeFocusedFullAdmin = (
  request: Request,
  pathname: string,
  env: AdminEnv,
): Promise<Response> | null => ROUTES.get(`${request.method}:${pathname}`)?.(request, env) ?? null;
