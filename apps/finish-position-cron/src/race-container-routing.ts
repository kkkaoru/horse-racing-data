// Run with bun. Fail-closed routing for optional resource-specific Containers.

import type { Env, PredictCategory, PredictionContainerRole } from "./types";

export type { PredictionContainerRole } from "./types";

interface ResolveRaceContainerRouteParams {
  category: PredictCategory;
  env: Env;
  forceLegacy?: boolean;
  focusedFull: boolean;
  keibajoCode?: string;
  raceBango?: string;
  runYmd: string;
}

interface ResolveRescoreContainerRouteParams {
  attempts: number;
  category: PredictCategory;
  env: Env;
  forceLegacy?: boolean;
  keibajoCode: string;
  raceBango: string;
  runYmd: string;
}

interface PredictionContainerRoute<Role extends PredictionContainerRole> {
  namespace: Env["FINISH_POSITION_PREDICT_CONTAINER"];
  role: Role;
}

const RACE_CHAIN_ENABLED_VALUE: string = "1";
const CATEGORY_SEPARATOR: string = ",";
const FIRST_QUEUE_ATTEMPT: number = 1;
const RACE_KEY_PAD_WIDTH: number = 2;
const CONTAINER_ROLE_PREFIXES: Record<PredictionContainerRole, string> = {
  legacy: "",
  "race-chain": "race-chain-",
  rescore: "rescore-",
};
const CONTAINER_BINDING_KEYS: Record<
  PredictionContainerRole,
  | "FINISH_POSITION_PREDICT_CONTAINER"
  | "FINISH_POSITION_RACE_CHAIN_CONTAINER"
  | "FINISH_POSITION_RESCORE_CONTAINER"
> = {
  legacy: "FINISH_POSITION_PREDICT_CONTAINER",
  "race-chain": "FINISH_POSITION_RACE_CHAIN_CONTAINER",
  rescore: "FINISH_POSITION_RESCORE_CONTAINER",
};
const CONTAINER_ROLE_LABELS: Record<PredictionContainerRole, string> = {
  legacy: "Legacy",
  "race-chain": "Race-chain",
  rescore: "Rescore",
};

const legacyRoute = (env: Env): PredictionContainerRoute<"legacy"> => ({
  namespace: env.FINISH_POSITION_PREDICT_CONTAINER,
  role: "legacy",
});

const enabledCategories = (env: Env): ReadonlySet<string> =>
  new Set(
    (env.RACE_CHAIN_CONTAINER_CATEGORIES ?? "")
      .split(CATEGORY_SEPARATOR)
      .map((category) => category.trim())
      .filter((category) => category.length > 0),
  );

export const qualifyPredictionContainerDoName = (
  doName: string,
  role: PredictionContainerRole,
): string => `${CONTAINER_ROLE_PREFIXES[role]}${doName}`;

export const resolveRaceContainerRoute = async (
  params: ResolveRaceContainerRouteParams,
): Promise<PredictionContainerRoute<"legacy" | "race-chain">> => {
  const { category, env, forceLegacy, focusedFull, keibajoCode, raceBango } = params;
  if (
    forceLegacy === true ||
    !focusedFull ||
    env.RACE_CHAIN_CONTAINER_ENABLED !== RACE_CHAIN_ENABLED_VALUE
  )
    return legacyRoute(env);
  if (!enabledCategories(env).has(category)) return legacyRoute(env);
  if (env.FINISH_POSITION_RACE_CHAIN_CONTAINER === undefined) return legacyRoute(env);
  if (keibajoCode === undefined || raceBango === undefined) return legacyRoute(env);
  // Exact foundation freshness is a Queue precondition, enforced before any
  // coordinator or Container slot claim. Routing must remain a pure binding
  // choice: repeating R2 HEAD here could turn a transient second probe into a
  // legacy fallback that bypasses the fail-closed HIT gate.
  return { namespace: env.FINISH_POSITION_RACE_CHAIN_CONTAINER, role: "race-chain" };
};

// Selecting a small namespace does not authorize execution: the consumer must
// still attest the cache and weight generation before starting the Container.
// Retry attempts deliberately return to legacy without copying Queue messages
// or resetting their retry budget.
export const resolveRescoreContainerRoute = (
  params: ResolveRescoreContainerRouteParams,
): PredictionContainerRoute<"legacy" | "rescore"> => {
  const { attempts, category, env, forceLegacy, keibajoCode, raceBango, runYmd } = params;
  if (
    attempts !== FIRST_QUEUE_ATTEMPT ||
    forceLegacy === true ||
    env.RESCORE_CONTAINER_ENABLED !== RACE_CHAIN_ENABLED_VALUE ||
    env.FINISH_POSITION_RESCORE_CONTAINER === undefined
  )
    return legacyRoute(env);
  const raceKey = `${category}:${runYmd}:${keibajoCode.trim().padStart(RACE_KEY_PAD_WIDTH, "0")}:${raceBango.trim().padStart(RACE_KEY_PAD_WIDTH, "0")}`;
  const allowed = new Set(
    (env.RESCORE_CONTAINER_RACES ?? "").split(CATEGORY_SEPARATOR).map((key) => key.trim()),
  );
  return allowed.has(raceKey)
    ? { namespace: env.FINISH_POSITION_RESCORE_CONTAINER, role: "rescore" }
    : legacyRoute(env);
};

export const isPredictionContainerRole = (value: unknown): value is PredictionContainerRole =>
  typeof value === "string" && Object.hasOwn(CONTAINER_ROLE_PREFIXES, value);

export const resolveContainerNamespaceForRole = (
  env: Env,
  role: PredictionContainerRole | undefined,
): Env["FINISH_POSITION_PREDICT_CONTAINER"] => {
  const effectiveRole: PredictionContainerRole = role ?? "legacy";
  const namespace = env[CONTAINER_BINDING_KEYS[effectiveRole]];
  if (namespace === undefined)
    throw new Error(`${CONTAINER_ROLE_LABELS[effectiveRole]} container binding is unavailable`);
  return namespace;
};
