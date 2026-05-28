// Run with bun. Fires the pc-keiba-viewer day-level trend cache bust endpoint
// when a race finishes. The viewer drops the merged race-trend cache plus the
// d1-daily / d1-snapshot caches for the source × YMD, so the next request from
// any open trend page rebuilds the payload immediately.

import { formatError } from "./format-error";
import type { Env } from "./types";

const VIEWER_INTERNAL_BUST_PATH = "/api/internal/trend-cache-bust";
const AUTH_HEADER = "x-pc-keiba-internal-token";
const DEFAULT_VIEWER_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";

export type TrendBustSource = "jra" | "nar";

export interface TrendBustRequest {
  source: TrendBustSource;
  targetYmd: string;
}

export interface TrendBustOutcome {
  message?: string;
  status: "error" | "ok" | "skipped";
}

const isYyyymmdd = (value: string): boolean => /^\d{8}$/u.test(value);

const resolveViewerOrigin = (env: Env): string => {
  const configured = env.RUNNING_STYLE_CACHE_ORIGIN?.trim();
  return configured && configured.length > 0 ? configured : DEFAULT_VIEWER_ORIGIN;
};

const buildBustRequestInit = (env: Env, body: TrendBustRequest): RequestInit | null => {
  const token = env.PC_KEIBA_VIEWER_INTERNAL_TOKEN?.trim();
  if (!token) return null;
  return {
    body: JSON.stringify(body),
    headers: {
      [AUTH_HEADER]: token,
      "content-type": "application/json",
    },
    method: "POST",
  };
};

export const requestTrendCacheBust = async (
  env: Env,
  body: TrendBustRequest,
): Promise<TrendBustOutcome> => {
  if (!isYyyymmdd(body.targetYmd)) {
    return { message: `invalid targetYmd: ${body.targetYmd}`, status: "error" };
  }
  const init = buildBustRequestInit(env, body);
  if (!init) {
    return { message: "PC_KEIBA_VIEWER_INTERNAL_TOKEN not configured", status: "skipped" };
  }
  const url = `${resolveViewerOrigin(env)}${VIEWER_INTERNAL_BUST_PATH}`;
  try {
    const response = await fetch(url, init);
    if (!response.ok) {
      return { message: `HTTP ${response.status}`, status: "error" };
    }
    return { status: "ok" };
  } catch (error) {
    return {
      message: formatError(error),
      status: "error",
    };
  }
};

export interface RaceFinishContext {
  kaisaiNen: string;
  kaisaiTsukihi: string;
  source: TrendBustSource;
}

export const buildTrendBustFromRaceContext = (context: RaceFinishContext): TrendBustRequest => ({
  source: context.source,
  targetYmd: `${context.kaisaiNen}${context.kaisaiTsukihi}`,
});
