import type {
  RealtimeHorseOddsTrend,
  RealtimeRaceEntry,
  RealtimeHorseWeight,
  RealtimeOddsData,
  RealtimeOddsHistoryPoint,
  RealtimeOddsType,
  RealtimeRacePayload as SharedRealtimeRacePayload,
  RealtimeRaceResult,
  RealtimeRaceSource,
  RealtimeTrackCondition,
} from "horse-racing-realtime/types";
import type { BrowserWorker } from "@cloudflare/playwright";

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
    }
  | {
      type: "fetch-results";
      raceKey: string;
    }
  | {
      type: "fetch-jra-track-condition";
      date: string;
      keibajoCode: string;
    };

export type HorseOddsTrend = RealtimeHorseOddsTrend;
export type RaceEntry = RealtimeRaceEntry;
export type RaceResult = RealtimeRaceResult;
export type HorseWeight = RealtimeHorseWeight;
export type NarRaceSource = RealtimeRaceSource;
export type OddsData = RealtimeOddsData;
export type OddsHistoryPoint = RealtimeOddsHistoryPoint;
export type OddsType = RealtimeOddsType;
export type RealtimeRacePayload = SharedRealtimeRacePayload;
export type TrackCondition = RealtimeTrackCondition;

export interface HyperdriveBinding {
  connectionString: string;
}

export interface Env {
  DATABASE_TARGET?: string;
  DATABASE_URL_NEON?: string;
  HYPERDRIVE?: HyperdriveBinding;
  JRA_BROWSER?: BrowserWorker;
  ODDS_CACHE: DurableObjectNamespace;
  ODDS_DO_TTL_SECONDS?: string;
  REALTIME_ADMIN_TOKEN?: string;
  REALTIME_API_CACHE_SECONDS?: string;
  REALTIME_DB: D1Database;
  REALTIME_JOBS: Queue<Job>;
  REALTIME_TEST_NOW?: string;
  TRACK_CONDITION_CACHE: DurableObjectNamespace;
  TRACK_CONDITION_DO_TTL_SECONDS?: string;
}
