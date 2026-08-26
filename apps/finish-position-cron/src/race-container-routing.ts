// Run with bun. Fail-closed routing for the optional race-chain Container.

import type { Env, PredictCategory } from "./types";

export type PredictionContainerRole = "legacy" | "race-chain";

interface ResolveRaceContainerRouteParams {
  category: PredictCategory;
  env: Env;
  forceLegacy?: boolean;
  focusedFull: boolean;
  keibajoCode?: string;
  raceBango?: string;
  runYmd: string;
}

interface PredictionContainerRoute {
  namespace: Env["FINISH_POSITION_PREDICT_CONTAINER"];
  role: PredictionContainerRole;
}

const RACE_CHAIN_ENABLED_VALUE: string = "1";
const CATEGORY_SEPARATOR: string = ",";
const RACE_CHAIN_DO_NAME_PREFIX: string = "race-chain-";

const legacyRoute = (env: Env): PredictionContainerRoute => ({
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
): string => (role === "race-chain" ? `${RACE_CHAIN_DO_NAME_PREFIX}${doName}` : doName);

export const resolveRaceContainerRoute = async (
  params: ResolveRaceContainerRouteParams,
): Promise<PredictionContainerRoute> => {
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

export const resolveContainerNamespaceForRole = (
  env: Env,
  role: PredictionContainerRole | undefined,
): Env["FINISH_POSITION_PREDICT_CONTAINER"] => {
  if (role !== "race-chain") return env.FINISH_POSITION_PREDICT_CONTAINER;
  if (env.FINISH_POSITION_RACE_CHAIN_CONTAINER === undefined)
    throw new Error("Race-chain container binding is unavailable");
  return env.FINISH_POSITION_RACE_CHAIN_CONTAINER;
};
