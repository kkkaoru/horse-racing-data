// Run with bun. Persisted rotating cursor for the past14 rebuild candidate
// window. Without this, the past14 rebuild queue always restarted at index 0
// on every scheduled tick, so whichever candidates sorted first perpetually
// starved out the rest of the rolling 14-day window whenever the reserved
// per-tick quota was smaller than the full candidate count. Persisting the
// cursor lets every candidate eventually get a turn.

import type { Env } from "../types";

const CURSOR_KV_KEY = "features:past14-cursor:v1";

const parseCursor = (raw: string | null): number => {
  if (raw === null) {
    return 0;
  }
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
};

export const readPast14Cursor = async (env: Env): Promise<number> => {
  try {
    return parseCursor(await env.FEATURES_KV.get(CURSOR_KV_KEY));
  } catch {
    return 0;
  }
};

export const writePast14Cursor = async (env: Env, cursor: number): Promise<void> => {
  try {
    await env.FEATURES_KV.put(CURSOR_KV_KEY, String(cursor));
  } catch {
    // best-effort; a failed cursor write just resets rotation progress next tick
  }
};
