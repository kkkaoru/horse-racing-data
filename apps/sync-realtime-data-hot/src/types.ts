import type {
  RealtimeHorseOddsTrend,
  RealtimeHorseWeight,
  RealtimeOddsData,
  RealtimeOddsHistoryPoint,
  RealtimeOddsTrend,
  RealtimeOddsTrendPoint,
  RealtimeOddsType,
  RealtimeRaceEntry,
  RealtimeRaceResult,
  RealtimeRaceSource,
} from "horse-racing-realtime/types";
import type { BrowserWorker } from "@cloudflare/playwright";
import type {
  D1Database,
  DurableObjectNamespace,
  KVNamespace,
  Queue,
  R2Bucket,
} from "@cloudflare/workers-types";

export type Job =
  | {
      type: "plan-odds-fetches";
      date: string;
      selfSchedule?: boolean;
    }
  | {
      type: "fetch-odds";
      raceKey: string;
    }
  | {
      type: "archive-odds-to-r2";
      date: string;
    };

export type HorseOddsTrend = RealtimeHorseOddsTrend;
export type HorseWeight = RealtimeHorseWeight;
export type NarRaceSource = RealtimeRaceSource;
export type OddsData = RealtimeOddsData;
export type OddsHistoryPoint = RealtimeOddsHistoryPoint;
export type OddsTrend = RealtimeOddsTrend;
export type OddsTrendPoint = RealtimeOddsTrendPoint;
export type OddsType = RealtimeOddsType;
export type RaceEntry = RealtimeRaceEntry;
export type RaceResult = RealtimeRaceResult;

export type OddsSource = "jra" | "nar";

export interface HyperdriveBinding {
  connectionString: string;
}

export interface LocalRaceRow {
  hasso_jikoku: string | null;
  kaisai_kai?: string | null;
  kaisai_nichime?: string | null;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  kyosomei_hondai: string | null;
  race_bango: string;
}

export interface OddsFetchStateRow {
  raceKey: string;
  source: OddsSource;
  raceStartAtJst: string;
  debaUrl: string;
  oddsLinksJson: string;
  lastOddsFetchAt: string | null;
  lastOddsQueuedAt: string | null;
  oddsFetchLockUntil: string | null;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  updatedAt: string;
}

export interface OddsFetchStateUpsertInput {
  raceKey: string;
  source: OddsSource;
  raceStartAtJst: string;
  debaUrl: string;
  oddsLinksJson: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

export interface RaceListEntry {
  raceKey: string;
  source: OddsSource;
  raceStartAtJst: string;
  lastOddsFetchAt: string | null;
}

export interface Env {
  REALTIME_HOT_DB: D1Database;
  REALTIME_HOT_JOBS: Queue<Job>;
  ODDS_HOT_KV: KVNamespace;
  ODDS_ARCHIVE: R2Bucket;
  ODDS_CACHE: DurableObjectNamespace;
  JRA_BROWSER?: BrowserWorker;
  PC_KEIBA_VIEWER?: { fetch: typeof fetch };
  ODDS_DO_TTL_SECONDS?: string;
  ODDS_LATEST_KV_TTL_SECONDS?: string;
  ODDS_RACE_LIST_KV_TTL_SECONDS?: string;
  ODDS_EDGE_CACHE_TTL_SECONDS?: string;
  ODDS_D1_RESULT_CACHE_TTL_SECONDS?: string;
  ODDS_R2_ARCHIVE_RETENTION_DAYS?: string;
  ODDS_STALE_MIRROR_SECONDS?: string;
  PC_KEIBA_VIEWER_INTERNAL_TOKEN?: string;
  REALTIME_HOT_TEST_NOW?: string;
  HYPERDRIVE?: HyperdriveBinding;
}
