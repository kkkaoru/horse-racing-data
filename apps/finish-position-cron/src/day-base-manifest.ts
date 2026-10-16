// Run with bun. A published day-base points only to an immutable, owner-scoped R2 object.

import { buildDayBaseObjectKey } from "./day-base-object-key";
import type { DaybaseWatermark } from "./ndjson-stream";
import type { Env, PredictCategory } from "./types";

interface ManifestScope {
  category: PredictCategory;
  env: Pick<Env, "FINISH_POSITION_CRON_DB">;
  runYmd: string;
}

interface SourceScope<T extends { etag: string; customMetadata?: Record<string, string> }> {
  category: PredictCategory;
  env: {
    DAY_BASE_MANIFEST_REQUIRED?: string;
    FEATURES_CACHE: { head(key: string): Promise<T | null> };
    FINISH_POSITION_CRON_DB: D1Database;
  };
  runYmd: string;
}

export interface DayBaseSource<T> {
  key: string;
  object: T;
}

interface OwnerScope extends ManifestScope {
  generationId: string;
}

interface PublishScope extends OwnerScope {
  etag: string;
  watermark: DaybaseWatermark;
}

interface ManifestRow {
  generation_id: string | null;
  object_etag: string | null;
  object_key: string | null;
  state: "missing" | "pending" | "published";
  watermark_json: string | null;
}

export type DayBaseManifest =
  | { state: "missing" }
  | { state: "pending" }
  | {
      etag: string;
      generationId: string;
      key: string;
      state: "published";
      watermark: DaybaseWatermark;
    };

const parseWatermark = (serialized: string): DaybaseWatermark | null => {
  try {
    const value: unknown = JSON.parse(serialized);
    if (typeof value !== "object" || value === null || Array.isArray(value)) return null;
    if (!("maxDataSakuseiNengappi" in value) || !("rowCount" in value)) return null;
    if (!("rsPredictedAtMax" in value) || !("rsRowCount" in value)) return null;
    if (
      typeof value.maxDataSakuseiNengappi !== "string" ||
      value.maxDataSakuseiNengappi.length === 0 ||
      typeof value.rowCount !== "number" ||
      !Number.isSafeInteger(value.rowCount) ||
      typeof value.rsPredictedAtMax !== "string" ||
      value.rsPredictedAtMax.length === 0 ||
      typeof value.rsRowCount !== "number" ||
      !Number.isSafeInteger(value.rsRowCount) ||
      ("rsContentHash" in value &&
        (typeof value.rsContentHash !== "string" || value.rsContentHash.length === 0))
    )
      return null;
    return {
      maxDataSakuseiNengappi: value.maxDataSakuseiNengappi,
      rowCount: value.rowCount,
      ...("rsContentHash" in value && typeof value.rsContentHash === "string"
        ? { rsContentHash: value.rsContentHash }
        : {}),
      rsPredictedAtMax: value.rsPredictedAtMax,
      rsRowCount: value.rsRowCount,
    };
  } catch {
    return null;
  }
};

const STAGE_SQL = `insert into finish_position_day_base_manifests
  (category, run_ymd, repair_generation_id, status, updated_at)
select ?1, ?2, ?3, 'pending', ?4
  from finish_position_day_base_repair_requests
 where category = ?1 and run_ymd = ?2 and repair_generation_id = ?3
on conflict(category, run_ymd) do update
  set status = case when finish_position_day_base_manifests.repair_generation_id = excluded.repair_generation_id
      then finish_position_day_base_manifests.status else 'pending' end,
      object_key = case when finish_position_day_base_manifests.repair_generation_id = excluded.repair_generation_id
      then finish_position_day_base_manifests.object_key else null end,
      object_etag = case when finish_position_day_base_manifests.repair_generation_id = excluded.repair_generation_id
      then finish_position_day_base_manifests.object_etag else null end,
      watermark_json = case when finish_position_day_base_manifests.repair_generation_id = excluded.repair_generation_id
      then finish_position_day_base_manifests.watermark_json else null end,
      repair_generation_id = excluded.repair_generation_id,
      updated_at = excluded.updated_at`;

const PUBLISH_SQL = `update finish_position_day_base_manifests
   set status = 'published', object_key = ?4, object_etag = ?5,
       watermark_json = ?6, updated_at = ?7
 where category = ?1 and run_ymd = ?2 and repair_generation_id = ?3
   and (status = 'pending' or (status = 'published' and object_key = ?4 and object_etag = ?5 and watermark_json = ?6))
   and exists (
     select 1 from finish_position_day_base_repair_requests
      where category = ?1 and run_ymd = ?2 and repair_generation_id = ?3
   )`;

const RESOLVE_SQL = `select
 case when r.category is not null and
   (m.category is null or r.repair_generation_id is null or r.repair_generation_id <> m.repair_generation_id)
   then 'pending' else coalesce(m.status, 'missing') end as state,
 m.repair_generation_id as generation_id, m.object_key, m.object_etag, m.watermark_json
 from (select 1) as one
 left join finish_position_day_base_manifests as m on m.category = ?1 and m.run_ymd = ?2
 left join finish_position_day_base_repair_requests as r on r.category = ?1 and r.run_ymd = ?2`;

export const stageDayBaseManifest = async (params: OwnerScope): Promise<boolean> => {
  const result = await params.env.FINISH_POSITION_CRON_DB.prepare(STAGE_SQL)
    .bind(params.category, params.runYmd, params.generationId, new Date().toISOString())
    .run();
  return result.meta.changes === 1;
};

export const publishDayBaseManifest = async (params: PublishScope): Promise<boolean> => {
  const key = buildDayBaseObjectKey(params);
  const result = await params.env.FINISH_POSITION_CRON_DB.prepare(PUBLISH_SQL)
    .bind(
      params.category,
      params.runYmd,
      params.generationId,
      key,
      params.etag,
      JSON.stringify(params.watermark),
      new Date().toISOString(),
    )
    .run();
  return result.meta.changes === 1;
};

export const getDayBaseManifest = async (params: ManifestScope): Promise<DayBaseManifest> => {
  const row = await params.env.FINISH_POSITION_CRON_DB.prepare(RESOLVE_SQL)
    .bind(params.category, params.runYmd)
    .first<ManifestRow>();
  if (row === null || row.state === "missing") return { state: "missing" };
  if (
    row.state !== "published" ||
    row.generation_id === null ||
    row.object_key === null ||
    row.object_etag === null ||
    row.watermark_json === null
  )
    return { state: "pending" };
  if (row.object_key !== buildDayBaseObjectKey({ ...params, generationId: row.generation_id }))
    return { state: "pending" };
  const watermark = parseWatermark(row.watermark_json);
  if (watermark === null || row.object_etag.length === 0) return { state: "pending" };
  return {
    etag: row.object_etag,
    generationId: row.generation_id,
    key: row.object_key,
    state: "published",
    watermark,
  };
};

export const getDayBaseSource = async <
  T extends { etag: string; customMetadata?: Record<string, string> },
>(
  params: SourceScope<T>,
): Promise<DayBaseSource<T> | null> => {
  const manifest = await getDayBaseManifest(params);
  if (manifest.state === "pending") return null;
  if (manifest.state === "missing") {
    if (params.env.DAY_BASE_MANIFEST_REQUIRED === "1") return null;
    const key = buildDayBaseObjectKey(params);
    const object = await params.env.FEATURES_CACHE.head(key);
    return object === null ? null : { key, object };
  }
  const object = await params.env.FEATURES_CACHE.head(manifest.key);
  const metadata = object?.customMetadata;
  if (
    object === null ||
    object.etag !== manifest.etag ||
    metadata?.["max-data-sakusei-nengappi"] !== manifest.watermark.maxDataSakuseiNengappi ||
    metadata?.["row-count"] !== String(manifest.watermark.rowCount) ||
    (manifest.watermark.rsContentHash !== undefined &&
      metadata?.["rs-content-hash"] !== manifest.watermark.rsContentHash) ||
    metadata?.["rs-predicted-at-max"] !== manifest.watermark.rsPredictedAtMax ||
    metadata?.["rs-row-count"] !== String(manifest.watermark.rsRowCount)
  )
    return null;
  return { key: manifest.key, object };
};
