// Run with bun. Status polling may keep real work alive, never idle or expired work.
import {
  FOCUSED_FULL_WATCH_PROGRESS_STALE_MS,
  FOCUSED_FULL_WATCH_TIMEOUT_MS,
} from "./focused-full-watch";

interface StatusActivityParams {
  nowMs: number;
  payload: unknown;
  raceKey: string;
}
const isTimestamp = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);

export const shouldRenewFocusedFullActivity = ({
  nowMs,
  payload,
  raceKey,
}: StatusActivityParams): boolean => {
  if (
    typeof payload !== "object" ||
    payload === null ||
    !("status" in payload) ||
    !("raceKey" in payload) ||
    !("startedAtMs" in payload) ||
    !("lastProgressAtMs" in payload)
  )
    return false;
  return (
    payload.status === "running" &&
    payload.raceKey === raceKey &&
    isTimestamp(payload.startedAtMs) &&
    isTimestamp(payload.lastProgressAtMs) &&
    payload.startedAtMs <= nowMs &&
    payload.lastProgressAtMs <= nowMs &&
    payload.lastProgressAtMs >= payload.startedAtMs &&
    nowMs - payload.startedAtMs < FOCUSED_FULL_WATCH_TIMEOUT_MS &&
    nowMs - payload.lastProgressAtMs <= FOCUSED_FULL_WATCH_PROGRESS_STALE_MS
  );
};
