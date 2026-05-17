// Run with bun. Loaders for v1.5 running-style artifacts living in R2:
// the compact LightGBM model JSON exported by the Mac trainer, and the
// per-horse + per-race feature JSONL written by the Mac feature batch
// builder. Cold-start fetch is the bottleneck so we cache the latest
// payload in module-scoped state keyed by R2 etag.

import type { CompactLightGBMModel } from "./running-style-lightgbm-tree";

export interface RaceHorseFeatureRow {
  raceKey: string;
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  category: string;
  kettoTorokuBango: string;
  umaban: number;
  bamei: string | null;
  perHorseFeatures: Record<string, number | null>;
  peerInputs: {
    pastNigeRate: number | null;
    pastSenkouRate: number | null;
    pastSashiRate: number | null;
    pastOikomiRate: number | null;
    pastCorner1NormAvg5: number | null;
    speedIndexAvg5: number | null;
    speedIndexBest5: number | null;
    pastFirst3fAvg5: number | null;
    kohan3fAvg5: number | null;
    careerWinRate: number | null;
  };
}

interface CachedModel {
  etag: string;
  payload: CompactLightGBMModel;
}

interface CachedFeatures {
  etag: string;
  rows: ReadonlyArray<RaceHorseFeatureRow>;
}

const MODEL_CACHE = new Map<string, CachedModel>();
const FEATURES_CACHE = new Map<string, CachedFeatures>();

const decodeJson = async <T>(body: ReadableStream<Uint8Array> | null): Promise<T> => {
  if (body === null) throw new Error("R2 object body missing");
  const text = await new Response(body).text();
  return JSON.parse(text) as T;
};

const decodeJsonl = async <T>(body: ReadableStream<Uint8Array> | null): Promise<T[]> => {
  if (body === null) throw new Error("R2 object body missing");
  const text = await new Response(body).text();
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as T);
};

export const loadLightGBMModelFromR2 = async (
  bucket: R2Bucket,
  key: string,
): Promise<CompactLightGBMModel> => {
  const head = await bucket.head(key);
  if (head === null) throw new Error(`R2 object not found: ${key}`);
  const etag = head.etag;
  const cached = MODEL_CACHE.get(key);
  if (cached !== undefined && cached.etag === etag) return cached.payload;
  const object = await bucket.get(key);
  if (object === null) throw new Error(`R2 object not found: ${key}`);
  const payload = await decodeJson<CompactLightGBMModel>(object.body);
  MODEL_CACHE.set(key, { etag, payload });
  return payload;
};

export const loadFeaturesFromR2 = async (
  bucket: R2Bucket,
  key: string,
): Promise<ReadonlyArray<RaceHorseFeatureRow>> => {
  const head = await bucket.head(key);
  if (head === null) throw new Error(`R2 object not found: ${key}`);
  const etag = head.etag;
  const cached = FEATURES_CACHE.get(key);
  if (cached !== undefined && cached.etag === etag) return cached.rows;
  const object = await bucket.get(key);
  if (object === null) throw new Error(`R2 object not found: ${key}`);
  const rows = await decodeJsonl<RaceHorseFeatureRow>(object.body);
  FEATURES_CACHE.set(key, { etag, rows });
  return rows;
};

export const clearRunningStyleCaches = (): void => {
  MODEL_CACHE.clear();
  FEATURES_CACHE.clear();
};
