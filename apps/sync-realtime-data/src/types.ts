import type {
  RealtimeHorseOddsTrend,
  RealtimeHorseWeight,
  RealtimeOddsData,
  RealtimeOddsHistoryPoint,
  RealtimeOddsType,
  RealtimeRacePayload as SharedRealtimeRacePayload,
  RealtimeRaceSource,
} from "horse-racing-realtime/types";

export type Job =
  | {
      type: "discover-urls";
      date: string;
    }
  | {
      type: "plan-realtime-fetches";
      date: string;
    }
  | {
      type: "fetch-odds";
      raceKey: string;
    }
  | {
      type: "fetch-weights";
      raceKey: string;
    };

export type HorseOddsTrend = RealtimeHorseOddsTrend;
export type HorseWeight = RealtimeHorseWeight;
export type NarRaceSource = RealtimeRaceSource;
export type OddsData = RealtimeOddsData;
export type OddsHistoryPoint = RealtimeOddsHistoryPoint;
export type OddsType = RealtimeOddsType;
export type RealtimeRacePayload = SharedRealtimeRacePayload;

export interface HyperdriveBinding {
  connectionString: string;
}

export interface Env {
  DATABASE_TARGET?: string;
  DATABASE_URL_NEON?: string;
  HYPERDRIVE?: HyperdriveBinding;
  ODDS_CACHE: DurableObjectNamespace;
  ODDS_DO_TTL_SECONDS?: string;
  REALTIME_ADMIN_TOKEN?: string;
  REALTIME_API_CACHE_SECONDS?: string;
  REALTIME_DB: D1Database;
  REALTIME_JOBS: Queue<Job>;
}
