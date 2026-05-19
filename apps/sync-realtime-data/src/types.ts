import type {
  RealtimeHorseOddsTrend,
  RealtimeOddsTrend,
  RealtimeOddsTrendPoint,
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
      selfSchedule?: boolean;
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
    }
  | {
      type: "discover-premium-races";
      date: string;
    }
  | {
      type: "discover-premium-race-links";
      date: string;
    }
  | {
      type: "plan-premium-race-data-fetches";
      date: string;
    }
  | {
      type: "fetch-premium-race-data";
      raceKey: string;
    }
  | {
      type: "fetch-premium-paddock";
      raceKey: string;
    }
  | {
      type: "plan-running-style-predictions";
      date: string;
    }
  | {
      type: "generate-running-style-predictions";
      raceKey: string;
      source: "jra" | "nar";
      kaisaiNen: string;
      kaisaiTsukihi: string;
      keibajoCode: string;
      raceBango: string;
      predictedAt: string;
    };

export type RunningStylePredictionJob = Extract<
  Job,
  { type: "generate-running-style-predictions" }
>;

export type HorseOddsTrend = RealtimeHorseOddsTrend;
export type OddsTrend = RealtimeOddsTrend;
export type OddsTrendPoint = RealtimeOddsTrendPoint;
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
  PREMIUM_PADDOCK_CACHE: DurableObjectNamespace;
  PREMIUM_PADDOCK_DO_TTL_SECONDS?: string;
  PREMIUM_PADDOCK_DISCORD_BOT_NAME?: string;
  PREMIUM_PADDOCK_DISCORD_WEBHOOK_URL?: string;
  PREMIUM_RACE_QUEUE_DELAY_SECONDS?: string;
  PREMIUM_RACE_JOBS?: Queue<Job>;
  PREMIUM_RACE_COMMENT_LABEL_EVALUATION?: string;
  PREMIUM_RACE_COMMENT_LABEL_FRAME?: string;
  PREMIUM_RACE_COMMENT_LABEL_HORSE_NAME?: string;
  PREMIUM_RACE_COMMENT_LABEL_HORSE_NUMBER?: string;
  PREMIUM_RACE_COMMENT_LABEL_TEXT?: string;
  PREMIUM_RACE_COMMENT_PATH_TEMPLATE?: string;
  PREMIUM_RACE_COMMENT_ROW_CLASS?: string;
  PREMIUM_RACE_COOKIE?: string;
  PREMIUM_RACE_ENTRY_LINK_PATTERN?: string;
  PREMIUM_RACE_ORIGIN?: string;
  PREMIUM_RACE_PADDOCK_GROUP_FAVORITE_LABEL?: string;
  PREMIUM_RACE_PADDOCK_GROUP_VALUE_LABEL?: string;
  PREMIUM_RACE_PADDOCK_LABEL_COMMENT?: string;
  PREMIUM_RACE_PADDOCK_LABEL_EVALUATION?: string;
  PREMIUM_RACE_PADDOCK_LABEL_FRAME?: string;
  PREMIUM_RACE_PADDOCK_LABEL_HORSE_NAME?: string;
  PREMIUM_RACE_PADDOCK_LABEL_HORSE_NUMBER?: string;
  PREMIUM_RACE_PADDOCK_PATH_TEMPLATE?: string;
  PREMIUM_RACE_PADDOCK_PENDING_TEXT?: string;
  PREMIUM_RACE_PADDOCK_ROW_CLASS?: string;
  PREMIUM_RACE_PADDOCK_TABLE_CLASS?: string;
  PREMIUM_RACE_PADDOCK_UNAVAILABLE_TEXT?: string;
  PREMIUM_RACE_PROXY_BEARER?: string;
  PREMIUM_RACE_PROXY_URL?: string;
  PREMIUM_RACE_PROXY_USER_ID?: string;
  PREMIUM_RACE_RESPONSE_CHARSET?: string;
  PREMIUM_RACE_SOURCE_ID_QUERY_KEY?: string;
  PREMIUM_RACE_TOP_PATH_TEMPLATE?: string;
  PREMIUM_RACE_WORK_COMMENT_CLASS?: string;
  PREMIUM_RACE_WORK_DATE_CLASS?: string;
  PREMIUM_RACE_WORK_GRADE_CLASS?: string;
  PREMIUM_RACE_WORK_HORSE_NAME_CLASS?: string;
  PREMIUM_RACE_WORK_HORSE_NUMBER_CLASS?: string;
  PREMIUM_RACE_WORK_PATH_TEMPLATE?: string;
  PREMIUM_RACE_WORK_RIDER_CLASS?: string;
  PREMIUM_RACE_WORK_ROW_CLASS?: string;
  PREMIUM_RACE_WORK_TEXT_CLASS?: string;
  REALTIME_ADMIN_TOKEN?: string;
  REALTIME_API_CACHE_SECONDS?: string;
  REALTIME_DB: D1Database;
  REALTIME_JOBS: Queue<Job>;
  REALTIME_TEST_NOW?: string;
  RUNNING_STYLE_JOBS?: Queue<Job>;
  RUNNING_STYLE_D1_WRITE_ENABLED?: string;
  RUNNING_STYLE_MODELS: R2Bucket;
  TRACK_CONDITION_CACHE: DurableObjectNamespace;
  TRACK_CONDITION_DO_TTL_SECONDS?: string;
}
