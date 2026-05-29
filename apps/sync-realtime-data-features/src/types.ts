// Run with bun.

export interface DailyRaceEntryRow {
  source: "jra" | "nar";
  race_date: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
  wakuban: string | null;
  umaban: number | null;
  bamei: string | null;
  race_name: string | null;
  hasso_jikoku: string | null;
  track_code: string | null;
  grade_code: string | null;
  kyoso_shubetsu_code: string | null;
  juryo_shubetsu_code: string | null;
  kyoso_joken_code: string | null;
  babajotai_code_shiba: string | null;
  babajotai_code_dirt: string | null;
  kyori: number | null;
  shusso_tosu: number | null;
  seibetsu_code: string | null;
  barei: number | null;
  futan_juryo: number | null;
  kishumei_ryakusho: string | null;
  chokyoshimei_ryakusho: string | null;
  banushimei: string | null;
  finish_position: number | null;
  finish_norm: number | null;
  tansho_ninkijun: number | null;
  tansho_odds: number | null;
  soha_time: number | null;
  time_sa: number | null;
  kohan_3f: number | null;
  corner1_norm: number | null;
  corner2_norm: number | null;
  corner3_norm: number | null;
  corner4_norm: number | null;
  corner_1: number | null;
  corner_2: number | null;
  corner_3: number | null;
  corner_4: number | null;
  bataiju: number | null;
  zogen_fugo: string | null;
  zogen_sa: number | null;
}

export interface RaceJobKey {
  raceKey: string;
  source: "jra" | "nar";
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

export type Job =
  | {
      type: "build-race-features";
      raceKey: string;
      source: "jra" | "nar";
      kaisaiNen: string;
      kaisaiTsukihi: string;
      keibajoCode: string;
      raceBango: string;
    }
  | {
      type: "predict-running-style";
      raceKey: string;
      source: "jra" | "nar";
      kaisaiNen: string;
      kaisaiTsukihi: string;
      keibajoCode: string;
      raceBango: string;
      predictedAt: string;
    }
  | {
      type: "predict-finish-position";
      raceKey: string;
      source: "jra" | "nar";
      kaisaiNen: string;
      kaisaiTsukihi: string;
      keibajoCode: string;
      raceBango: string;
      predictedAt: string;
    }
  | {
      type: "archive-features-to-r2";
      date: string;
    };

export interface HyperdriveBinding {
  connectionString: string;
}

export interface FinishPositionPredictionEntry {
  horse_number: number;
  predicted_position: number;
  probability: number;
}

export interface RunningStyleRow {
  raceKey: string;
  horseNumber: number;
  kettoTorokuBango: string;
  bamei: string | null;
  category: string;
  kaisaiNen: string;
  modelVersion: string;
  pNige: number;
  pSenkou: number;
  pSashi: number;
  pOikomi: number;
  predictedLabel: string;
  predictedAt: string;
}

export interface RunningStyleInferenceStateRow {
  raceKey: string;
  source: "jra" | "nar";
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  status: string;
  featuresR2Key: string | null;
  modelVersion: string | null;
  expectedHorseCount: number | null;
  writtenHorseCount: number | null;
  attemptedAt: string | null;
  completedAt: string | null;
  errorMessage: string | null;
}

export interface FinishPositionInferenceStateRow {
  raceKey: string;
  source: "jra" | "nar";
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  status: string;
  predictionsR2Key: string | null;
  modelVersion: string | null;
  attemptedAt: string | null;
  completedAt: string | null;
  errorMessage: string | null;
}

export interface FinishPositionPredictionsRow {
  raceKey: string;
  source: "jra" | "nar";
  predictionsJson: string;
  predictedAt: string;
  predictorVersion: string;
}

export interface Env {
  REALTIME_FEATURES_DB: D1Database;
  REALTIME_FEATURES_JOBS: Queue<Job>;
  FEATURES_KV: KVNamespace;
  FEATURES_ARCHIVE: R2Bucket;
  MODELS: R2Bucket;
  REALTIME_OLD?: { fetch: typeof fetch };
  HYPERDRIVE?: HyperdriveBinding;
  FEATURES_BUILD_LOCK_TTL_SECONDS?: string;
  FEATURES_RACE_LIST_KV_TTL_SECONDS?: string;
  FEATURES_ENQUEUE_LOCK_TTL_SECONDS?: string;
  FEATURES_EDGE_CACHE_TTL_SECONDS?: string;
  FEATURES_R2_LIST_CACHE_TTL_SECONDS?: string;
  FEATURES_PARQUET_BYTES_CACHE_TTL_SECONDS?: string;
  FEATURES_LATEST_KV_TTL_SECONDS?: string;
  PC_KEIBA_VIEWER_INTERNAL_TOKEN?: string;
}
