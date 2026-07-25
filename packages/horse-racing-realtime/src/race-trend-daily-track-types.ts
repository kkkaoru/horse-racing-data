// Run with bun.
// Shared types for the daily-track race-trend Durable Object.
// Consumers: sync-realtime-data (DO implementation) and pc-keiba-viewer (client).

export type RaceTrendDailyTrackSource = "jra" | "nar";

export type RaceTrendRunningStyle = "nige" | "senkou" | "sashi" | "oikomi";

export interface RaceTrendStarterRow extends Record<string, unknown> {
  source: RaceTrendDailyTrackSource;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  raceName: string | null;
  hassoJikoku: string | null;
  runnerCount: string | null;
  wakuban: string | null;
  umaban: string | null;
  bamei: string | null;
  jockeyName: string | null;
  // Optional so callers that predate trainer support continue to compile.
  // When absent the aggregator treats trainer as unknown and the trainer
  // filter degrades to a no-op for that row.
  chokyoshiName?: string | null;
  // Optional so callers that predate the void-result status continue to
  // compile. Set to the timestamp the upstream result-fetch circuit breaker
  // confirmed this race will never produce a result (upstream cancellation
  // or an unrecoverable publish failure -- see EMPTY_RESULT_VOID_LOG_STATUS /
  // EMPTY_RESULT_GIVEUP_LOG_STATUS in sync-realtime-data's worker.ts),
  // undefined/null otherwise. Distinguishes "this race has no result because
  // it never will" from "no result yet" so the viewer can render an explicit
  // status instead of a silent gap (2026-07-24 Oi 5R/6R incident).
  resultVoidAt?: string | null;
  tanshoOdds: string | null;
  tanshoPopularity: string | null;
  finishPosition: number;
  sohaTime: string | null;
  corner1: string | null;
  corner2: string | null;
  corner3: string | null;
  corner4: string | null;
  bataiju: string | null;
  zogenFugo: string | null;
  zogenSa: string | null;
}

export interface RaceTrendRunningStyleCache {
  raceKey: string;
  horseNumber: string;
  predictedLabel: RaceTrendRunningStyle;
}

export interface RaceTrendDailyTrackRow {
  raceBango: string;
  raceKey: string;
  isComplete: boolean;
  finishedAt: string | null;
  fetchedAt: string;
  starterRows: RaceTrendStarterRow[];
  runningStyles: RaceTrendRunningStyleCache[];
}

export interface RaceTrendDailyTrackState {
  source: RaceTrendDailyTrackSource;
  targetYmd: string;
  keibajoCode: string;
  races: Record<string, RaceTrendDailyTrackRow>;
  updatedAt: string;
}

export interface RaceTrendDailyTrackQuery {
  source: RaceTrendDailyTrackSource;
  targetYmd: string;
  keibajoCode: string;
  beforeRaceBango: string;
}

export interface RaceTrendDailyTrackResponse {
  races: RaceTrendDailyTrackRow[];
}
