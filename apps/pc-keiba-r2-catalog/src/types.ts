export type CatalogSource = "jra" | "nar";
export type RunningStyleSourceScope = "ban-ei" | "jra" | "nar";
export type SourceScope = "all" | "ban-ei" | "jra" | "nar";

export interface DailyRaceEntryRow {
  source: CatalogSource;
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

export interface CatalogRaceKeyRow {
  source: CatalogSource;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
}

export interface KvStore {
  delete(key: string): Promise<void>;
  get(key: string): Promise<string | null>;
  put(key: string, value: string, options: { expirationTtl: number }): Promise<void>;
}

export interface CacheStore {
  delete(request: Request): Promise<boolean>;
  match(request: Request): Promise<Response | undefined>;
  put(request: Request, response: Response): Promise<void>;
}

export type Fetcher = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

export interface R2SqlCatalogConfig {
  R2_SQL_ACCOUNT_ID: string;
  R2_SQL_BUCKET_NAME: string;
  R2_SQL_NAMESPACE: string;
  R2_SQL_TOKEN: string;
}

export interface Env extends R2SqlCatalogConfig {
  ADMIN_TOKEN?: string;
  CACHE_TTL_SECONDS?: string;
  CATALOG_KV: KvStore;
  KV_TTL_SECONDS?: string;
}

export interface WorkerDependencies {
  cache: CacheStore;
  fetchImpl: Fetcher;
}

export interface RaceFeatureFilters {
  date: string;
  keibajoCode?: string;
  raceBango?: string;
  source: SourceScope;
}

export interface RunningStyleFeatureFilters {
  date: string;
  keibajoCode: string;
  raceBango: string;
  source: RunningStyleSourceScope;
}
