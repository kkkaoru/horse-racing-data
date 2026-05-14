import { createWriteStream } from "node:fs";
import { mkdir } from "node:fs/promises";
import { dirname, isAbsolute, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { Pool } from "pg";

import { getConnectionString, loadEnv } from "./compare-corner-predictions";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const APP_DIR = resolve(SCRIPT_DIR, "../..");

type Options = {
  fromDate: string;
  includeVectorNeighbors: boolean;
  output: string;
  sourceScope: "jra" | "nar";
  target: "local" | "neon";
  toDate: string;
};

type DatasetRow = {
  source: string;
  race_date: string;
  race_id: string;
  horse_key: string;
  keibajo_code: string;
  race_bango: string;
  track_code: string | null;
  grade_code: string | null;
  kyoso_shubetsu_code: string | null;
  juryo_shubetsu_code: string | null;
  kyoso_joken_code: string | null;
  babajotai_code_shiba: string | null;
  babajotai_code_dirt: string | null;
  kyori: number | null;
  shusso_tosu: number | null;
  umaban: number | null;
  seibetsu_code: string | null;
  barei: number | null;
  futan_juryo: string | null;
  finish_position: number | null;
  finish_norm: string | null;
  tansho_ninkijun: number | null;
  tansho_odds: string | null;
  soha_time: number | null;
  time_sa: string | null;
  kohan_3f: string | null;
  horse_corner1_avg: string | null;
  horse_corner2_avg: string | null;
  horse_corner3_avg: string | null;
  horse_corner4_avg: string | null;
  horse_corner1_recent_avg: string | null;
  horse_corner2_recent_avg: string | null;
  horse_corner3_recent_avg: string | null;
  horse_corner4_recent_avg: string | null;
  horse_corner1_last: string | null;
  horse_corner2_last: string | null;
  horse_corner3_last: string | null;
  horse_corner4_last: string | null;
  horse_start_count: string | null;
  horse_finish_norm_avg: string | null;
  horse_finish_norm_recent_avg: string | null;
  horse_finish_norm_last: string | null;
  horse_popularity_norm_avg: string | null;
  horse_popularity_norm_recent_avg: string | null;
  horse_popularity_norm_last: string | null;
  horse_odds_avg: string | null;
  horse_odds_recent_avg: string | null;
  horse_odds_last: string | null;
  horse_time_sa_avg: string | null;
  horse_time_sa_recent_avg: string | null;
  horse_time_sa_last: string | null;
  horse_kohan_3f_avg: string | null;
  horse_kohan_3f_recent_avg: string | null;
  horse_kohan_3f_last: string | null;
  horse_days_since_last_start: string | null;
  jockey_corner1_avg: string | null;
  jockey_corner2_avg: string | null;
  jockey_corner3_avg: string | null;
  jockey_corner4_avg: string | null;
  jockey_start_count: string | null;
  trainer_corner1_avg: string | null;
  trainer_corner2_avg: string | null;
  trainer_corner3_avg: string | null;
  trainer_corner4_avg: string | null;
  trainer_start_count: string | null;
  owner_corner1_avg: string | null;
  owner_corner2_avg: string | null;
  owner_corner3_avg: string | null;
  owner_corner4_avg: string | null;
  owner_start_count: string | null;
  course_number_corner1_avg: string | null;
  course_number_corner2_avg: string | null;
  course_number_corner3_avg: string | null;
  course_number_corner4_avg: string | null;
  course_number_start_count: string | null;
  venue_course_number_corner1_avg: string | null;
  venue_course_number_corner2_avg: string | null;
  venue_course_number_corner3_avg: string | null;
  venue_course_number_corner4_avg: string | null;
  venue_course_number_start_count: string | null;
  vector_neighbor10_count: string | null;
  vector_neighbor10_similarity_avg: string | null;
  vector_neighbor10_corner1_avg: string | null;
  vector_neighbor10_corner2_avg: string | null;
  vector_neighbor10_corner3_avg: string | null;
  vector_neighbor10_corner4_avg: string | null;
  vector_neighbor30_count: string | null;
  vector_neighbor30_similarity_avg: string | null;
  vector_neighbor30_corner1_avg: string | null;
  vector_neighbor30_corner2_avg: string | null;
  vector_neighbor30_corner3_avg: string | null;
  vector_neighbor30_corner4_avg: string | null;
  vector_neighbor50_count: string | null;
  vector_neighbor50_similarity_avg: string | null;
  vector_neighbor50_corner1_avg: string | null;
  vector_neighbor50_corner2_avg: string | null;
  vector_neighbor50_corner3_avg: string | null;
  vector_neighbor50_corner4_avg: string | null;
  corner1_norm: string | null;
  corner2_norm: string | null;
  corner3_norm: string | null;
  corner4_norm: string | null;
};

const normalizeDate = (value: string): string => value.replaceAll("-", "");

const parseArgs = (args: string[]): Options => {
  const options: Options = {
    fromDate: "20210101",
    includeVectorNeighbors: false,
    output: "apps/pc-keiba-viewer/tmp/corner-lightgbm-jra.csv",
    sourceScope: "jra",
    target: "local",
    toDate: "20261231",
  };
  for (let index = 0; index < args.length; index += 1) {
    const name = args[index];
    const value = args[index + 1];
    if (name === "--help" || name === "-h") {
      console.log(`Usage:
  bun run src/scripts/export-corner-lightgbm-dataset.ts [options]

Options:
  --from-date YYYY-MM-DD
  --to-date YYYY-MM-DD
  --target local|neon
  --source-scope jra|nar
  --with-vector-neighbors
  --output path/to/dataset.csv
`);
      process.exit(0);
    }
    if (name === "--with-vector-neighbors") {
      options.includeVectorNeighbors = true;
      continue;
    }
    if (!value) {
      throw new Error(`${name} requires a value.`);
    }
    if (name === "--from-date") {
      options.fromDate = normalizeDate(value);
    } else if (name === "--to-date") {
      options.toDate = normalizeDate(value);
    } else if (name === "--target") {
      if (value !== "local" && value !== "neon") {
        throw new Error("--target must be local or neon.");
      }
      options.target = value;
    } else if (name === "--source-scope") {
      if (value !== "jra" && value !== "nar") {
        throw new Error("--source-scope must be jra or nar.");
      }
      options.sourceScope = value;
    } else if (name === "--output") {
      options.output = value;
    } else {
      throw new Error(`Unknown argument: ${name}`);
    }
    index += 1;
  }
  return options;
};

const csvValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return "";
  }
  const text =
    typeof value === "string" || typeof value === "number" || typeof value === "boolean"
      ? String(value)
      : JSON.stringify(value);
  if (!/[",\n\r]/u.test(text)) {
    return text;
  }
  return `"${text.replaceAll('"', '""')}"`;
};

const writeRows = async (path: string, rows: DatasetRow[]): Promise<void> => {
  await mkdir(dirname(path), { recursive: true });
  const stream = createWriteStream(path, "utf8");
  const columns: Array<keyof DatasetRow> = [
    "source",
    "race_date",
    "race_id",
    "horse_key",
    "keibajo_code",
    "race_bango",
    "track_code",
    "grade_code",
    "kyoso_shubetsu_code",
    "juryo_shubetsu_code",
    "kyoso_joken_code",
    "babajotai_code_shiba",
    "babajotai_code_dirt",
    "kyori",
    "shusso_tosu",
    "umaban",
    "seibetsu_code",
    "barei",
    "futan_juryo",
    "finish_position",
    "finish_norm",
    "tansho_ninkijun",
    "tansho_odds",
    "soha_time",
    "time_sa",
    "kohan_3f",
    "horse_corner1_avg",
    "horse_corner2_avg",
    "horse_corner3_avg",
    "horse_corner4_avg",
    "horse_corner1_recent_avg",
    "horse_corner2_recent_avg",
    "horse_corner3_recent_avg",
    "horse_corner4_recent_avg",
    "horse_corner1_last",
    "horse_corner2_last",
    "horse_corner3_last",
    "horse_corner4_last",
    "horse_start_count",
    "horse_finish_norm_avg",
    "horse_finish_norm_recent_avg",
    "horse_finish_norm_last",
    "horse_popularity_norm_avg",
    "horse_popularity_norm_recent_avg",
    "horse_popularity_norm_last",
    "horse_odds_avg",
    "horse_odds_recent_avg",
    "horse_odds_last",
    "horse_time_sa_avg",
    "horse_time_sa_recent_avg",
    "horse_time_sa_last",
    "horse_kohan_3f_avg",
    "horse_kohan_3f_recent_avg",
    "horse_kohan_3f_last",
    "horse_days_since_last_start",
    "jockey_corner1_avg",
    "jockey_corner2_avg",
    "jockey_corner3_avg",
    "jockey_corner4_avg",
    "jockey_start_count",
    "trainer_corner1_avg",
    "trainer_corner2_avg",
    "trainer_corner3_avg",
    "trainer_corner4_avg",
    "trainer_start_count",
    "owner_corner1_avg",
    "owner_corner2_avg",
    "owner_corner3_avg",
    "owner_corner4_avg",
    "owner_start_count",
    "course_number_corner1_avg",
    "course_number_corner2_avg",
    "course_number_corner3_avg",
    "course_number_corner4_avg",
    "course_number_start_count",
    "venue_course_number_corner1_avg",
    "venue_course_number_corner2_avg",
    "venue_course_number_corner3_avg",
    "venue_course_number_corner4_avg",
    "venue_course_number_start_count",
    "vector_neighbor10_count",
    "vector_neighbor10_similarity_avg",
    "vector_neighbor10_corner1_avg",
    "vector_neighbor10_corner2_avg",
    "vector_neighbor10_corner3_avg",
    "vector_neighbor10_corner4_avg",
    "vector_neighbor30_count",
    "vector_neighbor30_similarity_avg",
    "vector_neighbor30_corner1_avg",
    "vector_neighbor30_corner2_avg",
    "vector_neighbor30_corner3_avg",
    "vector_neighbor30_corner4_avg",
    "vector_neighbor50_count",
    "vector_neighbor50_similarity_avg",
    "vector_neighbor50_corner1_avg",
    "vector_neighbor50_corner2_avg",
    "vector_neighbor50_corner3_avg",
    "vector_neighbor50_corner4_avg",
    "corner1_norm",
    "corner2_norm",
    "corner3_norm",
    "corner4_norm",
  ];
  stream.write(`${columns.join(",")}\n`);
  for (const row of rows) {
    stream.write(`${columns.map((column) => csvValue(row[column])).join(",")}\n`);
  }
  await new Promise<void>((resolveStream, reject) => {
    stream.on("error", reject);
    stream.end(resolveStream);
  });
};

const main = async () => {
  await loadEnv();
  const options = parseArgs(process.argv.slice(2));
  const pool = new Pool({ connectionString: getConnectionString(options.target), max: 4 });
  try {
    const vectorNeighborSelect = options.includeVectorNeighbors
      ? `
          coalesce(vector_neighbors.vector_neighbor10_count, 0) vector_neighbor10_count,
          coalesce(vector_neighbors.vector_neighbor10_similarity_avg, 0) vector_neighbor10_similarity_avg,
          coalesce(vector_neighbors.vector_neighbor10_corner1_avg, 0) vector_neighbor10_corner1_avg,
          coalesce(vector_neighbors.vector_neighbor10_corner2_avg, 0) vector_neighbor10_corner2_avg,
          coalesce(vector_neighbors.vector_neighbor10_corner3_avg, 0) vector_neighbor10_corner3_avg,
          coalesce(vector_neighbors.vector_neighbor10_corner4_avg, 0) vector_neighbor10_corner4_avg,
          coalesce(vector_neighbors.vector_neighbor30_count, 0) vector_neighbor30_count,
          coalesce(vector_neighbors.vector_neighbor30_similarity_avg, 0) vector_neighbor30_similarity_avg,
          coalesce(vector_neighbors.vector_neighbor30_corner1_avg, 0) vector_neighbor30_corner1_avg,
          coalesce(vector_neighbors.vector_neighbor30_corner2_avg, 0) vector_neighbor30_corner2_avg,
          coalesce(vector_neighbors.vector_neighbor30_corner3_avg, 0) vector_neighbor30_corner3_avg,
          coalesce(vector_neighbors.vector_neighbor30_corner4_avg, 0) vector_neighbor30_corner4_avg,
          coalesce(vector_neighbors.vector_neighbor50_count, 0) vector_neighbor50_count,
          coalesce(vector_neighbors.vector_neighbor50_similarity_avg, 0) vector_neighbor50_similarity_avg,
          coalesce(vector_neighbors.vector_neighbor50_corner1_avg, 0) vector_neighbor50_corner1_avg,
          coalesce(vector_neighbors.vector_neighbor50_corner2_avg, 0) vector_neighbor50_corner2_avg,
          coalesce(vector_neighbors.vector_neighbor50_corner3_avg, 0) vector_neighbor50_corner3_avg,
          coalesce(vector_neighbors.vector_neighbor50_corner4_avg, 0) vector_neighbor50_corner4_avg,
        `
      : `
          0::numeric vector_neighbor10_count,
          0::numeric vector_neighbor10_similarity_avg,
          0::numeric vector_neighbor10_corner1_avg,
          0::numeric vector_neighbor10_corner2_avg,
          0::numeric vector_neighbor10_corner3_avg,
          0::numeric vector_neighbor10_corner4_avg,
          0::numeric vector_neighbor30_count,
          0::numeric vector_neighbor30_similarity_avg,
          0::numeric vector_neighbor30_corner1_avg,
          0::numeric vector_neighbor30_corner2_avg,
          0::numeric vector_neighbor30_corner3_avg,
          0::numeric vector_neighbor30_corner4_avg,
          0::numeric vector_neighbor50_count,
          0::numeric vector_neighbor50_similarity_avg,
          0::numeric vector_neighbor50_corner1_avg,
          0::numeric vector_neighbor50_corner2_avg,
          0::numeric vector_neighbor50_corner3_avg,
          0::numeric vector_neighbor50_corner4_avg,
        `;
    const vectorNeighborJoin = options.includeVectorNeighbors
      ? `
        left join lateral (
          with nearest as (
            select
              candidate.corner1_norm,
              candidate.corner2_norm,
              candidate.corner3_norm,
              candidate.corner4_norm,
              1 / (1 + (candidate.feature_vector <-> fr.feature_vector)) similarity,
              row_number() over (order by candidate.feature_vector <-> fr.feature_vector) neighbor_rank
            from (
              select
                corner1_norm,
                corner2_norm,
                corner3_norm,
                corner4_norm,
                feature_vector
              from race_entry_corner_features candidate_pool
              where
                candidate_pool.source = fr.source
                and candidate_pool.race_date < fr.race_date
                and candidate_pool.race_date >= to_char(to_date(fr.race_date, 'YYYYMMDD') - interval '3 years', 'YYYYMMDD')
                and candidate_pool.keibajo_code = fr.keibajo_code
                and left(coalesce(candidate_pool.track_code, ''), 1) = left(coalesce(fr.track_code, ''), 1)
                and candidate_pool.kyori between fr.kyori - 400 and fr.kyori + 400
                and candidate_pool.corner4_norm is not null
              order by candidate_pool.race_date desc
              limit 1800
            ) candidate
            order by candidate.feature_vector <-> fr.feature_vector
            limit 50
          )
          select
            count(*) filter (where neighbor_rank <= 10)::numeric vector_neighbor10_count,
            avg(similarity) filter (where neighbor_rank <= 10) vector_neighbor10_similarity_avg,
            sum(corner1_norm * similarity) filter (where neighbor_rank <= 10) / nullif(sum(similarity) filter (where neighbor_rank <= 10), 0) vector_neighbor10_corner1_avg,
            sum(corner2_norm * similarity) filter (where neighbor_rank <= 10) / nullif(sum(similarity) filter (where neighbor_rank <= 10), 0) vector_neighbor10_corner2_avg,
            sum(corner3_norm * similarity) filter (where neighbor_rank <= 10) / nullif(sum(similarity) filter (where neighbor_rank <= 10), 0) vector_neighbor10_corner3_avg,
            sum(corner4_norm * similarity) filter (where neighbor_rank <= 10) / nullif(sum(similarity) filter (where neighbor_rank <= 10), 0) vector_neighbor10_corner4_avg,
            count(*) filter (where neighbor_rank <= 30)::numeric vector_neighbor30_count,
            avg(similarity) filter (where neighbor_rank <= 30) vector_neighbor30_similarity_avg,
            sum(corner1_norm * similarity) filter (where neighbor_rank <= 30) / nullif(sum(similarity) filter (where neighbor_rank <= 30), 0) vector_neighbor30_corner1_avg,
            sum(corner2_norm * similarity) filter (where neighbor_rank <= 30) / nullif(sum(similarity) filter (where neighbor_rank <= 30), 0) vector_neighbor30_corner2_avg,
            sum(corner3_norm * similarity) filter (where neighbor_rank <= 30) / nullif(sum(similarity) filter (where neighbor_rank <= 30), 0) vector_neighbor30_corner3_avg,
            sum(corner4_norm * similarity) filter (where neighbor_rank <= 30) / nullif(sum(similarity) filter (where neighbor_rank <= 30), 0) vector_neighbor30_corner4_avg,
            count(*) filter (where neighbor_rank <= 50)::numeric vector_neighbor50_count,
            avg(similarity) filter (where neighbor_rank <= 50) vector_neighbor50_similarity_avg,
            sum(corner1_norm * similarity) filter (where neighbor_rank <= 50) / nullif(sum(similarity) filter (where neighbor_rank <= 50), 0) vector_neighbor50_corner1_avg,
            sum(corner2_norm * similarity) filter (where neighbor_rank <= 50) / nullif(sum(similarity) filter (where neighbor_rank <= 50), 0) vector_neighbor50_corner2_avg,
            sum(corner3_norm * similarity) filter (where neighbor_rank <= 50) / nullif(sum(similarity) filter (where neighbor_rank <= 50), 0) vector_neighbor50_corner3_avg,
            sum(corner4_norm * similarity) filter (where neighbor_rank <= 50) / nullif(sum(similarity) filter (where neighbor_rank <= 50), 0) vector_neighbor50_corner4_avg
          from nearest
        ) vector_neighbors on true
      `
      : "";
    const result = await pool.query<DatasetRow>(
      `
        with featured_rows as (
          select
            *,
            avg(corner1_norm) over horse_history horse_corner1_avg,
            avg(corner2_norm) over horse_history horse_corner2_avg,
            avg(corner3_norm) over horse_history horse_corner3_avg,
            avg(corner4_norm) over horse_history horse_corner4_avg,
            avg(corner1_norm) over horse_recent_history horse_corner1_recent_avg,
            avg(corner2_norm) over horse_recent_history horse_corner2_recent_avg,
            avg(corner3_norm) over horse_recent_history horse_corner3_recent_avg,
            avg(corner4_norm) over horse_recent_history horse_corner4_recent_avg,
            avg(corner1_norm) over horse_last_history horse_corner1_last,
            avg(corner2_norm) over horse_last_history horse_corner2_last,
            avg(corner3_norm) over horse_last_history horse_corner3_last,
            avg(corner4_norm) over horse_last_history horse_corner4_last,
            count(*) over horse_history horse_start_count,
            avg(finish_norm) over horse_history horse_finish_norm_avg,
            avg(finish_norm) over horse_recent_history horse_finish_norm_recent_avg,
            avg(finish_norm) over horse_last_history horse_finish_norm_last,
            avg(tansho_ninkijun::numeric / nullif(shusso_tosu, 0)) over horse_history horse_popularity_norm_avg,
            avg(tansho_ninkijun::numeric / nullif(shusso_tosu, 0)) over horse_recent_history horse_popularity_norm_recent_avg,
            avg(tansho_ninkijun::numeric / nullif(shusso_tosu, 0)) over horse_last_history horse_popularity_norm_last,
            avg(tansho_odds) over horse_history horse_odds_avg,
            avg(tansho_odds) over horse_recent_history horse_odds_recent_avg,
            avg(tansho_odds) over horse_last_history horse_odds_last,
            avg(time_sa) over horse_history horse_time_sa_avg,
            avg(time_sa) over horse_recent_history horse_time_sa_recent_avg,
            avg(time_sa) over horse_last_history horse_time_sa_last,
            avg(kohan_3f) over horse_history horse_kohan_3f_avg,
            avg(kohan_3f) over horse_recent_history horse_kohan_3f_recent_avg,
            avg(kohan_3f) over horse_last_history horse_kohan_3f_last,
            to_date(race_date, 'YYYYMMDD') - to_date(max(race_date) over horse_history, 'YYYYMMDD') horse_days_since_last_start,
            avg(case when btrim(coalesce(kishumei_ryakusho, '')) <> '' then corner1_norm else null end) over jockey_history jockey_corner1_avg,
            avg(case when btrim(coalesce(kishumei_ryakusho, '')) <> '' then corner2_norm else null end) over jockey_history jockey_corner2_avg,
            avg(case when btrim(coalesce(kishumei_ryakusho, '')) <> '' then corner3_norm else null end) over jockey_history jockey_corner3_avg,
            avg(case when btrim(coalesce(kishumei_ryakusho, '')) <> '' then corner4_norm else null end) over jockey_history jockey_corner4_avg,
            count(case when btrim(coalesce(kishumei_ryakusho, '')) <> '' then 1 else null end) over jockey_history jockey_start_count,
            avg(case when btrim(coalesce(chokyoshimei_ryakusho, '')) <> '' then corner1_norm else null end) over trainer_history trainer_corner1_avg,
            avg(case when btrim(coalesce(chokyoshimei_ryakusho, '')) <> '' then corner2_norm else null end) over trainer_history trainer_corner2_avg,
            avg(case when btrim(coalesce(chokyoshimei_ryakusho, '')) <> '' then corner3_norm else null end) over trainer_history trainer_corner3_avg,
            avg(case when btrim(coalesce(chokyoshimei_ryakusho, '')) <> '' then corner4_norm else null end) over trainer_history trainer_corner4_avg,
            count(case when btrim(coalesce(chokyoshimei_ryakusho, '')) <> '' then 1 else null end) over trainer_history trainer_start_count,
            avg(case when btrim(coalesce(banushimei, '')) <> '' then corner1_norm else null end) over owner_history owner_corner1_avg,
            avg(case when btrim(coalesce(banushimei, '')) <> '' then corner2_norm else null end) over owner_history owner_corner2_avg,
            avg(case when btrim(coalesce(banushimei, '')) <> '' then corner3_norm else null end) over owner_history owner_corner3_avg,
            avg(case when btrim(coalesce(banushimei, '')) <> '' then corner4_norm else null end) over owner_history owner_corner4_avg,
            count(case when btrim(coalesce(banushimei, '')) <> '' then 1 else null end) over owner_history owner_start_count,
            avg(corner1_norm) over course_number_history course_number_corner1_avg,
            avg(corner2_norm) over course_number_history course_number_corner2_avg,
            avg(corner3_norm) over course_number_history course_number_corner3_avg,
            avg(corner4_norm) over course_number_history course_number_corner4_avg,
            count(*) over course_number_history course_number_start_count,
            avg(corner1_norm) over venue_course_number_history venue_course_number_corner1_avg,
            avg(corner2_norm) over venue_course_number_history venue_course_number_corner2_avg,
            avg(corner3_norm) over venue_course_number_history venue_course_number_corner3_avg,
            avg(corner4_norm) over venue_course_number_history venue_course_number_corner4_avg,
            count(*) over venue_course_number_history venue_course_number_start_count
          from race_entry_corner_features
          where source = $1
          window
            horse_history as (
              partition by source, ketto_toroku_bango
              order by race_date, keibajo_code, race_bango
              rows between unbounded preceding and 1 preceding
            ),
            horse_recent_history as (
              partition by source, ketto_toroku_bango
              order by race_date, keibajo_code, race_bango
              rows between 5 preceding and 1 preceding
            ),
            horse_last_history as (
              partition by source, ketto_toroku_bango
              order by race_date, keibajo_code, race_bango
              rows between 1 preceding and 1 preceding
            ),
            jockey_history as (
              partition by source, kishumei_ryakusho
              order by race_date, keibajo_code, race_bango
              rows between unbounded preceding and 1 preceding
            ),
            trainer_history as (
              partition by source, chokyoshimei_ryakusho
              order by race_date, keibajo_code, race_bango
              rows between unbounded preceding and 1 preceding
            ),
            owner_history as (
              partition by source, banushimei
              order by race_date, keibajo_code, race_bango
              rows between unbounded preceding and 1 preceding
            ),
            course_number_history as (
              partition by source, track_code, kyori, shusso_tosu, umaban
              order by race_date, keibajo_code, race_bango
              rows between unbounded preceding and 1 preceding
            ),
            venue_course_number_history as (
              partition by source, keibajo_code, track_code, kyori, shusso_tosu, umaban
              order by race_date, keibajo_code, race_bango
              rows between unbounded preceding and 1 preceding
            )
        )
        select
          source,
          race_date,
          source || ':' || kaisai_nen || kaisai_tsukihi || ':' || keibajo_code || ':' || race_bango race_id,
          ketto_toroku_bango horse_key,
          keibajo_code,
          race_bango,
          track_code,
          grade_code,
          kyoso_shubetsu_code,
          juryo_shubetsu_code,
          kyoso_joken_code,
          babajotai_code_shiba,
          babajotai_code_dirt,
          kyori,
          shusso_tosu,
          umaban,
          seibetsu_code,
          barei,
          futan_juryo,
          finish_position,
          finish_norm,
          tansho_ninkijun,
          tansho_odds,
          soha_time,
          time_sa,
          kohan_3f,
          horse_corner1_avg,
          horse_corner2_avg,
          horse_corner3_avg,
          horse_corner4_avg,
          horse_corner1_recent_avg,
          horse_corner2_recent_avg,
          horse_corner3_recent_avg,
          horse_corner4_recent_avg,
          horse_corner1_last,
          horse_corner2_last,
          horse_corner3_last,
          horse_corner4_last,
          horse_start_count,
          horse_finish_norm_avg,
          horse_finish_norm_recent_avg,
          horse_finish_norm_last,
          horse_popularity_norm_avg,
          horse_popularity_norm_recent_avg,
          horse_popularity_norm_last,
          horse_odds_avg,
          horse_odds_recent_avg,
          horse_odds_last,
          horse_time_sa_avg,
          horse_time_sa_recent_avg,
          horse_time_sa_last,
          horse_kohan_3f_avg,
          horse_kohan_3f_recent_avg,
          horse_kohan_3f_last,
          horse_days_since_last_start,
          jockey_corner1_avg,
          jockey_corner2_avg,
          jockey_corner3_avg,
          jockey_corner4_avg,
          jockey_start_count,
          trainer_corner1_avg,
          trainer_corner2_avg,
          trainer_corner3_avg,
          trainer_corner4_avg,
          trainer_start_count,
          owner_corner1_avg,
          owner_corner2_avg,
          owner_corner3_avg,
          owner_corner4_avg,
          owner_start_count,
          course_number_corner1_avg,
          course_number_corner2_avg,
          course_number_corner3_avg,
          course_number_corner4_avg,
          course_number_start_count,
          venue_course_number_corner1_avg,
          venue_course_number_corner2_avg,
          venue_course_number_corner3_avg,
          venue_course_number_corner4_avg,
          venue_course_number_start_count,
          ${vectorNeighborSelect}
          corner1_norm,
          corner2_norm,
          corner3_norm,
          corner4_norm
        from featured_rows fr
        ${vectorNeighborJoin}
        where
          race_date between $2 and $3
          and corner4_norm is not null
        order by race_date, keibajo_code, race_bango, umaban
      `,
      [options.sourceScope, options.fromDate, options.toDate],
    );
    const output = isAbsolute(options.output) ? options.output : resolve(APP_DIR, options.output);
    await writeRows(output, result.rows);
    console.log(`rows=${result.rowCount ?? result.rows.length}`);
    console.log(`output=${output}`);
  } finally {
    await pool.end();
  }
};

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
