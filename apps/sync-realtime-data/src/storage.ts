import { BABA_CODE_TO_LOCAL_KEIBAJO, buildRaceKey, type KeibaGoRaceLink } from "./keiba-go";
import { buildRealtimeRaceKey } from "./race-key";
import { formatRaceStartJst, toJstIsoString } from "./time";
import type {
  HorseOddsTrend,
  HorseWeight,
  NarRaceSource,
  OddsData,
  OddsHistoryPoint,
  OddsTrend,
  OddsTrendPoint,
  OddsType,
  RaceEntry,
  RaceResult,
  RealtimeRacePayload,
  TrackCondition,
} from "./types";

export interface HotOddsPayload {
  fetchedAt: string;
  history: OddsHistoryPoint[];
  historyByType: Partial<Record<OddsType, OddsTrendPoint[]>>;
  latest: Partial<Record<OddsType, OddsData[]>>;
}
import type {
  PremiumDataTopHorse,
  PremiumPaddockBulletin,
  PremiumRaceLink,
  PremiumStableComment,
  PremiumTrainingReview,
} from "./premium-race";

const D1_BATCH_SIZE = 100;
// Dedupe identical fetch_logs INSERTs across a short window to keep D1 write
// pressure flat during retry storms (e.g. plan-realtime-fetches looping on the
// same error). 60s aligns with queue retry backoff and lets retention sweeps
// still see one row per distinct outcome.
const LOG_DEDUPE_KV_PREFIX = "log-dedupe:";
const LOG_DEDUPE_TTL_SECONDS = 60;
const LOG_DEDUPE_HASH_OFFSET = 2166136261;
const LOG_DEDUPE_HASH_PRIME = 16777619;
const LOG_DEDUPE_HASH_MASK = 0xffffffff;
const LOG_DEDUPE_NULL_TOKEN = "null";
const LOG_DEDUPE_HEX_RADIX = 16;
// Content-hash dedup for premium stable_comments + training_reviews rewrites.
// SHA-1 truncated to 16 hex chars is collision-resistant enough for a per-race
// "did the scraped HTML change" check while keeping the hash row small.
const PREMIUM_PADDOCK_CONTENT_HASH_ALGORITHM = "SHA-1";
const PREMIUM_PADDOCK_CONTENT_HASH_HEX_LENGTH = 16;
const PREMIUM_PADDOCK_CONTENT_HASH_BYTE_RADIX = 16;
const PREMIUM_PADDOCK_CONTENT_HASH_BYTE_PAD_LENGTH = 2;

const parsePremiumDataTopReasons = (value: string): string[] => {
  try {
    const parsed: unknown = JSON.parse(value);
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed.filter((item): item is string => typeof item === "string" && item.length > 0);
  } catch {
    return [];
  }
};

const normalizeStoredJockeyName = (value: string | null | undefined): string | null => {
  if (!value) {
    return null;
  }
  const normalized = value
    .replace(/[△▲☆★◇◆□■▽▼]/gu, "")
    .replace(/[\s\p{Separator}\u200B-\u200D\uFEFF]+/gu, "");
  return normalized === "" ? null : normalized;
};

interface RaceSourceRow {
  baba_code: string;
  deba_url: string;
  empty_result_attempts: number;
  kaisai_kai: string | null;
  kaisai_nichime: string | null;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  last_odds_fetch_at: string | null;
  last_odds_queued_at: string | null;
  last_result_fetch_at: string | null;
  last_result_queued_at: string | null;
  last_weight_fetch_at: string | null;
  odds_fetch_lock_until: string | null;
  odds_links_json: string;
  race_bango: string;
  race_key: string;
  race_name: string | null;
  race_start_at_jst: string;
  result_complete_at: string | null;
  result_expected_horse_count: number | null;
  result_fetch_lock_until: string | null;
  result_saved_horse_count: number | null;
  source: "jra" | "nar";
}

interface EmptyResultAttemptsRow {
  count: number;
}

interface WeightSnapshotRow {
  change_amount: number | null;
  change_sign: string | null;
  fetched_at: string;
  horse_name: string | null;
  horse_number: string;
  weight: number | null;
}

interface RaceEntrySnapshotRow {
  fetched_at: string;
  horse_name: string | null;
  horse_number: string;
  jockey_name: string | null;
  status: string | null;
}

interface RaceResultSnapshotRow {
  fetched_at: string;
  finish_position: string;
  horse_name: string | null;
  horse_number: string;
  time: string | null;
}

interface SameDayVenueJockeyWinRow {
  jockey_name: string;
  latest_race_bango: string;
  win_count: number;
}

interface JraVenueScheduleRow {
  first_race_start_at_jst: string;
  keibajo_code: string;
  last_fetch_at: string | null;
  last_queued_at: string | null;
  last_race_start_at_jst: string;
}

interface TrackConditionSnapshotRow {
  dirt_condition: string | null;
  dirt_measurement_date: string | null;
  dirt_moisture_final_bend: string | null;
  dirt_moisture_final_furlong: string | null;
  dirt_moisture_measured_at: string | null;
  fetched_at: string;
  source_updated_at: string | null;
  turf_condition: string | null;
  turf_course_layout: string | null;
  turf_cushion_measured_at: string | null;
  turf_cushion_value: string | null;
  turf_going: string | null;
  turf_height_japanese_zoysia_grass: string | null;
  turf_height_perennial_ryegrass: string | null;
  turf_measurement_date: string | null;
  turf_moisture_final_bend: string | null;
  turf_moisture_final_furlong: string | null;
  turf_moisture_measured_at: string | null;
  weather: string | null;
}

interface PremiumRaceLinkRow {
  entry_url: string;
  race_key: string;
  source_race_id: string;
}

interface PremiumTrainingReviewRow {
  comment_text: string | null;
  evaluation_grade: string | null;
  evaluation_text: string | null;
  fetched_at: string;
  horse_name: string | null;
  horse_number: string;
  rider_name: string | null;
  training_date: string;
}

interface PremiumStableCommentRow {
  comment_text: string;
  evaluation_grade: number | null;
  evaluation_text: string | null;
  fetched_at: string;
  frame_number: string | null;
  horse_name: string | null;
  horse_number: string;
}

interface PremiumDataTopHorseRow {
  fetched_at: string;
  horse_name: string | null;
  horse_number: string;
  rank: number;
  reasons_json: string;
}

interface PremiumPaddockBulletinRow {
  comment_text: string | null;
  evaluation_text: string | null;
  fetched_at: string;
  frame_number: string | null;
  group_key: "favorite" | "value";
  horse_name: string | null;
  horse_number: string;
}

interface PremiumPaddockFetchStateRow {
  last_fetch_at: string | null;
  last_queued_at: string | null;
  retry_after: string | null;
  status: string;
}

interface PremiumPaddockNotificationStateRow {
  last_payload_fetched_at: string | null;
  last_notified_at: string | null;
  last_send_attempt_at: string | null;
  message: string | null;
  payload_signature: string | null;
  skip_reason: string | null;
  status: string;
}

interface PremiumRaceDataFetchStateRow {
  last_fetch_at: string | null;
  last_queued_at: string | null;
  message: string | null;
  retry_after: string | null;
  status: string;
}

export interface PremiumRacePayload {
  dataTopHorses: (PremiumDataTopHorse & { fetchedAt: string })[];
  paddockBulletins: (PremiumPaddockBulletin & { fetchedAt: string })[];
  stableComments: (PremiumStableComment & { fetchedAt: string })[];
  trainingReviews: (PremiumTrainingReview & { fetchedAt: string })[];
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

export interface SchedulableRaceSource extends NarRaceSource {
  lastOddsQueuedAt: string | null;
  lastResultFetchAt: string | null;
  lastResultQueuedAt: string | null;
  oddsFetchLockUntil: string | null;
  resultCompleteAt: string | null;
  resultFetchLockUntil: string | null;
}

export interface JraVenueTrackConditionSchedule {
  firstRaceStartAtJst: string;
  keibajoCode: string;
  lastFetchAt: string | null;
  lastQueuedAt: string | null;
  lastRaceStartAtJst: string;
}

export interface PremiumRaceDataFetchCandidate {
  raceKey: string;
}

export interface PremiumPaddockContentHashInput {
  stableComments?: readonly PremiumStableComment[];
  trainingReviews?: readonly PremiumTrainingReview[];
}

interface PremiumPaddockContentHashRow {
  content_hash: string;
}

const parseOddsLinks = (value: string): Partial<Record<OddsType, string>> => {
  try {
    const parsed = JSON.parse(value);
    return typeof parsed === "object" && parsed !== null ? parsed : {};
  } catch {
    return {};
  }
};

const toRaceSource = (row: RaceSourceRow): NarRaceSource => ({
  babaCode: row.baba_code,
  debaUrl: row.deba_url,
  kaisaiKai: row.kaisai_kai,
  kaisaiNichime: row.kaisai_nichime,
  kaisaiNen: row.kaisai_nen,
  kaisaiTsukihi: row.kaisai_tsukihi,
  keibajoCode: row.keibajo_code,
  lastOddsFetchAt: row.last_odds_fetch_at,
  lastWeightFetchAt: row.last_weight_fetch_at,
  oddsLinks: parseOddsLinks(row.odds_links_json),
  raceBango: row.race_bango,
  raceKey: row.race_key,
  raceName: row.race_name,
  raceStartAtJst: row.race_start_at_jst,
  source: row.source,
});

const toSchedulableRaceSource = (row: RaceSourceRow): SchedulableRaceSource => ({
  ...toRaceSource(row),
  lastOddsQueuedAt: row.last_odds_queued_at,
  lastResultFetchAt: row.last_result_fetch_at,
  lastResultQueuedAt: row.last_result_queued_at,
  oddsFetchLockUntil: row.odds_fetch_lock_until,
  resultCompleteAt: row.result_complete_at,
  resultFetchLockUntil: row.result_fetch_lock_until,
});

const runD1Batches = async (db: D1Database, statements: D1PreparedStatement[]): Promise<void> => {
  for (let index = 0; index < statements.length; index += D1_BATCH_SIZE) {
    await db.batch(statements.slice(index, index + D1_BATCH_SIZE));
  }
};

export const upsertNarRaceSource = async (
  db: D1Database,
  link: KeibaGoRaceLink,
  race: LocalRaceRow,
  oddsLinks: Partial<Record<OddsType, string>>,
): Promise<void> => {
  const keibajoCode = BABA_CODE_TO_LOCAL_KEIBAJO[link.babaCode];
  if (!keibajoCode || !race.hasso_jikoku) {
    return;
  }
  const raceBango = race.race_bango.padStart(2, "0");
  const raceKey = buildRaceKey(race.kaisai_nen, race.kaisai_tsukihi, keibajoCode, raceBango);
  const now = toJstIsoString();
  await db
    .prepare(
      `
        insert into realtime_race_sources (
          race_key, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          baba_code, kaisai_kai, kaisai_nichime, race_start_at_jst, race_name, deba_url, odds_links_json,
          discovered_at, updated_at
        )
        values (?, 'nar', ?, ?, ?, ?, ?, null, null, ?, ?, ?, ?, ?, ?)
        on conflict(race_key) do update set
          baba_code = excluded.baba_code,
          race_start_at_jst = excluded.race_start_at_jst,
          race_name = excluded.race_name,
          deba_url = excluded.deba_url,
          odds_links_json = excluded.odds_links_json,
          updated_at = excluded.updated_at
      `,
    )
    .bind(
      raceKey,
      race.kaisai_nen,
      race.kaisai_tsukihi,
      keibajoCode,
      raceBango,
      link.babaCode,
      formatRaceStartJst(race.kaisai_nen, race.kaisai_tsukihi, race.hasso_jikoku),
      race.kyosomei_hondai,
      link.url,
      JSON.stringify(oddsLinks),
      now,
      now,
    )
    .run();
};

export const upsertJraRaceSource = async (
  db: D1Database,
  race: LocalRaceRow,
  entryUrl: string | null,
): Promise<void> => {
  if (!race.hasso_jikoku || !entryUrl) {
    return;
  }
  const raceBango = race.race_bango.padStart(2, "0");
  const raceKey = buildRealtimeRaceKey(
    "jra",
    race.kaisai_nen,
    race.kaisai_tsukihi,
    race.keibajo_code,
    raceBango,
  );
  const now = toJstIsoString();
  await db
    .prepare(
      `
        insert into realtime_race_sources (
          race_key, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          baba_code, kaisai_kai, kaisai_nichime, race_start_at_jst, race_name, deba_url, odds_links_json,
          discovered_at, updated_at
        )
        values (?, 'jra', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, ?)
        on conflict(race_key) do update set
          source = excluded.source,
          baba_code = excluded.baba_code,
          kaisai_kai = excluded.kaisai_kai,
          kaisai_nichime = excluded.kaisai_nichime,
          race_start_at_jst = excluded.race_start_at_jst,
          race_name = excluded.race_name,
          deba_url = excluded.deba_url,
          updated_at = excluded.updated_at
      `,
    )
    .bind(
      raceKey,
      race.kaisai_nen,
      race.kaisai_tsukihi,
      race.keibajo_code,
      raceBango,
      race.keibajo_code,
      race.kaisai_kai ?? null,
      race.kaisai_nichime ?? null,
      formatRaceStartJst(race.kaisai_nen, race.kaisai_tsukihi, race.hasso_jikoku),
      race.kyosomei_hondai,
      entryUrl,
      now,
      now,
    )
    .run();
};

export const listRaceSourceKeibajoCodesByDate = async (
  db: D1Database,
  targetDate: string,
): Promise<string[]> => {
  const result = await db
    .prepare(
      `
        select distinct keibajo_code
        from realtime_race_sources
        where kaisai_nen = ?
          and kaisai_tsukihi = ?
        order by keibajo_code
      `,
    )
    .bind(targetDate.slice(0, 4), targetDate.slice(4, 8))
    .all<{ keibajo_code: string }>();
  return result.results.map((row) => row.keibajo_code);
};

export const getRaceSource = async (
  db: D1Database,
  raceKey: string,
): Promise<NarRaceSource | null> => {
  const row = await db
    .prepare("select * from realtime_race_sources where race_key = ?")
    .bind(raceKey)
    .first<RaceSourceRow>();
  return row ? toRaceSource(row) : null;
};

export const listSchedulableRaceSourcesByDate = async (
  db: D1Database,
  targetDate: string,
): Promise<SchedulableRaceSource[]> => {
  const result = await db
    .prepare(
      `
        select *
        from realtime_race_sources
        where kaisai_nen = ?
          and kaisai_tsukihi = ?
        order by race_start_at_jst asc
      `,
    )
    .bind(targetDate.slice(0, 4), targetDate.slice(4, 8))
    .all<RaceSourceRow>();
  return result.results.map(toSchedulableRaceSource);
};

export const getVenueLastRaceStartAtJst = async (
  db: D1Database,
  race: Pick<NarRaceSource, "kaisaiNen" | "kaisaiTsukihi" | "keibajoCode" | "source">,
): Promise<string | null> => {
  const row = await db
    .prepare(
      `
        select max(race_start_at_jst) race_start_at_jst
        from realtime_race_sources
        where source = ?
          and kaisai_nen = ?
          and kaisai_tsukihi = ?
          and keibajo_code = ?
      `,
    )
    .bind(race.source, race.kaisaiNen, race.kaisaiTsukihi, race.keibajoCode)
    .first<{ race_start_at_jst: string | null }>();
  return row?.race_start_at_jst ?? null;
};

export const countRaceSourcesByDate = async (
  db: D1Database,
  targetDate: string,
): Promise<number> => {
  const row = await db
    .prepare(
      `
        select count(*) count
        from realtime_race_sources
        where kaisai_nen = ?
          and kaisai_tsukihi = ?
      `,
    )
    .bind(targetDate.slice(0, 4), targetDate.slice(4, 8))
    .first<{ count: number }>();
  return Number(row?.count ?? 0);
};

export const countJraRaceSourcesMissingRaceDateFieldsByDate = async (
  db: D1Database,
  targetDate: string,
): Promise<number> => {
  const row = await db
    .prepare(
      `
        select count(*) count
        from realtime_race_sources
        where source = 'jra'
          and kaisai_nen = ?
          and kaisai_tsukihi = ?
          and (kaisai_kai is null or kaisai_nichime is null)
      `,
    )
    .bind(targetDate.slice(0, 4), targetDate.slice(4, 8))
    .first<{ count: number }>();
  return Number(row?.count ?? 0);
};

export const listJraVenueTrackConditionSchedulesByDate = async (
  db: D1Database,
  targetDate: string,
): Promise<JraVenueTrackConditionSchedule[]> => {
  const result = await db
    .prepare(
      `
        select
          races.keibajo_code,
          min(races.race_start_at_jst) first_race_start_at_jst,
          max(races.race_start_at_jst) last_race_start_at_jst,
          state.last_fetch_at,
          state.last_queued_at
        from realtime_race_sources races
        left join jra_track_condition_fetch_state state
          on state.kaisai_nen = races.kaisai_nen
          and state.kaisai_tsukihi = races.kaisai_tsukihi
          and state.keibajo_code = races.keibajo_code
        where races.source = 'jra'
          and races.kaisai_nen = ?
          and races.kaisai_tsukihi = ?
        group by races.keibajo_code
        order by races.keibajo_code
      `,
    )
    .bind(targetDate.slice(0, 4), targetDate.slice(4, 8))
    .all<JraVenueScheduleRow>();
  return result.results.map((row) => ({
    firstRaceStartAtJst: row.first_race_start_at_jst,
    keibajoCode: row.keibajo_code,
    lastFetchAt: row.last_fetch_at,
    lastQueuedAt: row.last_queued_at,
    lastRaceStartAtJst: row.last_race_start_at_jst,
  }));
};

export const markTrackConditionQueued = async (
  db: D1Database,
  jobs: { date: string; keibajoCode: string }[],
  queuedAt: string,
): Promise<void> => {
  if (jobs.length === 0) {
    return;
  }
  await runD1Batches(
    db,
    jobs.map((job) =>
      db
        .prepare(
          `
            insert into jra_track_condition_fetch_state (
              kaisai_nen, kaisai_tsukihi, keibajo_code, last_queued_at, updated_at
            )
            values (?, ?, ?, ?, ?)
            on conflict(kaisai_nen, kaisai_tsukihi, keibajo_code) do update set
              last_queued_at = excluded.last_queued_at,
              updated_at = excluded.updated_at
          `,
        )
        .bind(job.date.slice(0, 4), job.date.slice(4, 8), job.keibajoCode, queuedAt, queuedAt),
    ),
  );
};

export const claimTrackConditionFetch = async (
  db: D1Database,
  params: {
    date: string;
    keibajoCode: string;
    lockUntil: string;
    now: string;
  },
): Promise<boolean> => {
  await db
    .prepare(
      `
        insert into jra_track_condition_fetch_state (
          kaisai_nen, kaisai_tsukihi, keibajo_code, updated_at
        )
        values (?, ?, ?, ?)
        on conflict(kaisai_nen, kaisai_tsukihi, keibajo_code) do nothing
      `,
    )
    .bind(params.date.slice(0, 4), params.date.slice(4, 8), params.keibajoCode, params.now)
    .run();
  const result = await db
    .prepare(
      `
        update jra_track_condition_fetch_state
        set fetch_lock_until = ?, updated_at = ?
        where kaisai_nen = ?
          and kaisai_tsukihi = ?
          and keibajo_code = ?
          and (fetch_lock_until is null or fetch_lock_until <= ?)
      `,
    )
    .bind(
      params.lockUntil,
      params.now,
      params.date.slice(0, 4),
      params.date.slice(4, 8),
      params.keibajoCode,
      params.now,
    )
    .run();
  return result.meta.changes > 0;
};

export const failTrackConditionFetch = async (
  db: D1Database,
  params: { date: string; keibajoCode: string; now?: string },
): Promise<void> => {
  const now = params.now ?? toJstIsoString();
  await db
    .prepare(
      `
        update jra_track_condition_fetch_state
        set last_queued_at = null,
            fetch_lock_until = null,
            updated_at = ?
        where kaisai_nen = ?
          and kaisai_tsukihi = ?
          and keibajo_code = ?
      `,
    )
    .bind(now, params.date.slice(0, 4), params.date.slice(4, 8), params.keibajoCode)
    .run();
};

export const completeTrackConditionFetch = async (
  db: D1Database,
  params: { date: string; fetchedAt: string; keibajoCode: string },
): Promise<void> => {
  await db
    .prepare(
      `
        update jra_track_condition_fetch_state
        set last_fetch_at = ?,
            last_queued_at = null,
            fetch_lock_until = null,
            updated_at = ?
        where kaisai_nen = ?
          and kaisai_tsukihi = ?
          and keibajo_code = ?
      `,
    )
    .bind(
      params.fetchedAt,
      toJstIsoString(),
      params.date.slice(0, 4),
      params.date.slice(4, 8),
      params.keibajoCode,
    )
    .run();
};

export const updateLastFetch = async (
  db: D1Database,
  raceKey: string,
  column: "last_odds_fetch_at" | "last_result_fetch_at" | "last_weight_fetch_at",
  fetchedAt: string,
): Promise<void> => {
  await db
    .prepare(`update realtime_race_sources set ${column} = ?, updated_at = ? where race_key = ?`)
    .bind(fetchedAt, toJstIsoString(), raceKey)
    .run();
};

export const markResultFetchQueued = async (
  db: D1Database,
  raceKeys: string[],
  queuedAt: string,
): Promise<void> => {
  if (raceKeys.length === 0) {
    return;
  }
  await runD1Batches(
    db,
    raceKeys.map((raceKey) =>
      db
        .prepare(
          `
            update realtime_race_sources
            set last_result_queued_at = ?, updated_at = ?
            where race_key = ?
              and result_complete_at is null
              and (last_result_queued_at is null or last_result_queued_at <= ?)
          `,
        )
        .bind(queuedAt, queuedAt, raceKey, queuedAt),
    ),
  );
};

export const claimResultFetch = async (
  db: D1Database,
  raceKey: string,
  lockUntil: string,
  now = toJstIsoString(),
): Promise<boolean> => {
  const result = await db
    .prepare(
      `
        update realtime_race_sources
        set result_fetch_lock_until = ?, updated_at = ?
        where race_key = ?
          and result_complete_at is null
          and (result_fetch_lock_until is null or result_fetch_lock_until <= ?)
      `,
    )
    .bind(lockUntil, now, raceKey, now)
    .run();
  return result.meta.changes > 0;
};

export const completeResultFetch = async (
  db: D1Database,
  raceKey: string,
  fetchedAt: string,
  completion: {
    expectedHorseCount: number;
    isComplete: boolean;
    savedHorseCount: number;
  },
): Promise<void> => {
  const now = toJstIsoString();
  await db
    .prepare(
      `
        update realtime_race_sources
        set last_result_fetch_at = ?,
            last_result_queued_at = null,
            result_fetch_lock_until = null,
            result_complete_at = case when ? then ? else result_complete_at end,
            result_expected_horse_count = ?,
            result_saved_horse_count = ?,
            updated_at = ?
        where race_key = ?
      `,
    )
    .bind(
      fetchedAt,
      completion.isComplete ? 1 : 0,
      fetchedAt,
      completion.expectedHorseCount,
      completion.savedHorseCount,
      now,
      raceKey,
    )
    .run();
};

export const failResultFetch = async (db: D1Database, raceKey: string): Promise<void> => {
  const now = toJstIsoString();
  await db
    .prepare(
      `
        update realtime_race_sources
        set last_result_queued_at = null,
            result_fetch_lock_until = null,
            updated_at = ?
        where race_key = ?
          and result_complete_at is null
      `,
    )
    .bind(now, raceKey)
    .run();
};

// Records a partial NAR result fetch. Unlike completeResultFetch this keeps
// result_complete_at NULL and sets a SHORT result_fetch_lock_until so the next
// result-poll cron tick can re-claim and re-fetch once the upstream finishes
// publishing the remaining rows. Clears last_result_queued_at so the planner
// can re-enqueue.
export const recordPartialResultFetch = async (
  db: D1Database,
  raceKey: string,
  fetchedAt: string,
  retryLockUntil: string,
  counts: {
    expectedHorseCount: number;
    savedHorseCount: number;
  },
): Promise<void> => {
  const now = toJstIsoString();
  await db
    .prepare(
      `
        update realtime_race_sources
        set last_result_fetch_at = ?,
            last_result_queued_at = null,
            result_fetch_lock_until = ?,
            result_expected_horse_count = ?,
            result_saved_horse_count = ?,
            updated_at = ?
        where race_key = ?
          and result_complete_at is null
      `,
    )
    .bind(
      fetchedAt,
      retryLockUntil,
      counts.expectedHorseCount,
      counts.savedHorseCount,
      now,
      raceKey,
    )
    .run();
};

// Empty-result circuit breaker (2026-06-28). Increments the per-race counter
// of consecutive empty result-fetch attempts and returns the new count. The
// caller compares it against RESULT_FETCH_EMPTY_GIVEUP_COUNT (worker.ts) and
// either keeps retrying (re-throw the empty-result error → failResultFetch
// clears the lock and the planner re-enqueues on the next cron tick) or
// marks the race as force-completed via markEmptyResultGiveUp.
export const incrementEmptyResultAttempts = async (
  db: D1Database,
  raceKey: string,
): Promise<number> => {
  const row = await db
    .prepare(
      `
        update realtime_race_sources
        set empty_result_attempts = empty_result_attempts + 1,
            updated_at = ?
        where race_key = ?
        returning empty_result_attempts as count
      `,
    )
    .bind(toJstIsoString(), raceKey)
    .first<EmptyResultAttemptsRow>();
  return row ? Number(row.count) : 0;
};

// Empty-result give-up (2026-06-28). Force-completes a race after the
// circuit-breaker counter trips so the planner stops re-enqueueing it.
// Mirrors completeResultFetch with isComplete=true but keeps the counts at
// zero because no result rows ever landed for this race. Clears the lock so
// follow-up SELECTs treat the row as terminal.
export const markEmptyResultGiveUp = async (
  db: D1Database,
  raceKey: string,
  completedAt: string,
): Promise<void> => {
  const now = toJstIsoString();
  await db
    .prepare(
      `
        update realtime_race_sources
        set result_complete_at = ?,
            last_result_fetch_at = ?,
            last_result_queued_at = null,
            result_fetch_lock_until = null,
            updated_at = ?
        where race_key = ?
      `,
    )
    .bind(completedAt, completedAt, now, raceKey)
    .run();
};

// Empty-result counter reset (2026-06-28). Called when a result fetch lands
// any non-empty result rows so a transient empty followed by a real publish
// does not trip the breaker. Guarded with `empty_result_attempts > 0` so a
// race that has never had an empty attempt is a no-op write.
export const resetEmptyResultAttempts = async (db: D1Database, raceKey: string): Promise<void> => {
  await db
    .prepare(
      `
        update realtime_race_sources
        set empty_result_attempts = 0,
            updated_at = ?
        where race_key = ?
          and empty_result_attempts > 0
      `,
    )
    .bind(toJstIsoString(), raceKey)
    .run();
};

// Write-on-change guards (2026-06-26). The weight watchdog (every minute) and
// the result poll (every 2 min) re-scrape the same race dozens of times before
// the upstream data finalizes, and each scrape used to DELETE + re-INSERT the
// identical rows. DELETEs and INSERTs both count as D1 writes, so for a single
// race this could re-write the same snapshot 30-50 times. These signature
// helpers serialize the meaningful columns (everything except the always-
// changing fetched_at) so the caller can read the current stored rows, compare,
// and skip the DELETE + INSERT entirely when nothing changed. The signature is
// order-independent: rows are sorted before joining so a re-scrape that returns
// the same horses in a different order is still treated as unchanged.
const SNAPSHOT_FIELD_SEPARATOR_CHAR_CODE = 31;
const SNAPSHOT_ROW_SEPARATOR_CHAR_CODE = 30;
const SNAPSHOT_FIELD_SEPARATOR = String.fromCharCode(SNAPSHOT_FIELD_SEPARATOR_CHAR_CODE);
const SNAPSHOT_ROW_SEPARATOR = String.fromCharCode(SNAPSHOT_ROW_SEPARATOR_CHAR_CODE);
const SNAPSHOT_NULL_TOKEN = String.fromCharCode(0);

const snapshotNullable = (value: string | number | null): string =>
  value === null ? SNAPSHOT_NULL_TOKEN : String(value);

const joinSnapshotRows = (rows: ReadonlyArray<string>): string =>
  [...rows].sort().join(SNAPSHOT_ROW_SEPARATOR);

const weightSnapshotSignature = (horses: ReadonlyArray<HorseWeight>): string =>
  joinSnapshotRows(
    horses.map((horse) =>
      [
        horse.horseNumber,
        snapshotNullable(horse.horseName),
        snapshotNullable(horse.weight),
        snapshotNullable(horse.changeSign),
        snapshotNullable(horse.changeAmount),
      ].join(SNAPSHOT_FIELD_SEPARATOR),
    ),
  );

const entrySnapshotSignature = (horses: ReadonlyArray<Omit<RaceEntry, "fetchedAt">>): string =>
  joinSnapshotRows(
    horses.map((horse) =>
      [
        horse.horseNumber,
        snapshotNullable(horse.horseName),
        snapshotNullable(normalizeStoredJockeyName(horse.jockeyName)),
        snapshotNullable(horse.status),
      ].join(SNAPSHOT_FIELD_SEPARATOR),
    ),
  );

const resultSnapshotSignature = (horses: ReadonlyArray<Omit<RaceResult, "fetchedAt">>): string =>
  joinSnapshotRows(
    horses.map((horse) =>
      [
        horse.horseNumber,
        snapshotNullable(horse.horseName),
        horse.finishPosition,
        snapshotNullable(horse.time),
      ].join(SNAPSHOT_FIELD_SEPARATOR),
    ),
  );

export const insertHorseWeightSnapshot = async (
  db: D1Database,
  raceKey: string,
  fetchedAt: string,
  weights: HorseWeight[],
): Promise<void> => {
  if (weights.length === 0) {
    return;
  }
  const stored = await getLatestHorseWeights(db, raceKey);
  if (
    stored !== null &&
    weightSnapshotSignature(stored.horses) === weightSnapshotSignature(weights)
  ) {
    return;
  }
  await db.prepare("delete from horse_weight_snapshots where race_key = ?").bind(raceKey).run();
  await runD1Batches(
    db,
    weights.map((weight) =>
      db
        .prepare(
          `
            insert into horse_weight_snapshots (
              race_key, fetched_at, horse_number, horse_name,
              weight, change_sign, change_amount
            )
            values (?, ?, ?, ?, ?, ?, ?)
          `,
        )
        .bind(
          raceKey,
          fetchedAt,
          weight.horseNumber,
          weight.horseName,
          weight.weight,
          weight.changeSign,
          weight.changeAmount,
        ),
    ),
  );
};

export const insertRaceEntrySnapshot = async (
  db: D1Database,
  raceKey: string,
  fetchedAt: string,
  entries: Omit<RaceEntry, "fetchedAt">[],
): Promise<number> => {
  // 2026-06-07: same shape as the W1 weight-snapshot fix (storage.ts:821) but
  // for race_entry_snapshots. The DELETE must NOT run when the upstream
  // returned an empty parse, otherwise a transient blank wipes the saved
  // entries for that race_key. Empty entries downstream collapse
  // expectedHorseCount to 0 inside fetchAndStoreResults, which makes
  // resolveResultFetchOutcome return "complete" with savedHorseCount=0 and
  // permanently freezes race_result_snapshots at 0 rows (observed for
  // jra:2026:0607:05:04 / 東京 4R).
  if (entries.length === 0) {
    return 0;
  }
  const stored = await getLatestRaceEntries(db, raceKey);
  if (
    stored !== null &&
    entrySnapshotSignature(stored.horses) === entrySnapshotSignature(entries)
  ) {
    return entries.length;
  }
  await db.prepare("delete from race_entry_snapshots where race_key = ?").bind(raceKey).run();
  await runD1Batches(
    db,
    entries.map((entry) =>
      db
        .prepare(
          `
            insert into race_entry_snapshots (
              race_key, fetched_at, horse_number, horse_name, jockey_name, status
            )
            values (?, ?, ?, ?, ?, ?)
          `,
        )
        .bind(
          raceKey,
          fetchedAt,
          entry.horseNumber,
          entry.horseName,
          normalizeStoredJockeyName(entry.jockeyName),
          entry.status,
        ),
    ),
  );
  return entries.length;
};

export const insertRaceResultSnapshot = async (
  db: D1Database,
  raceKey: string,
  fetchedAt: string,
  results: Omit<RaceResult, "fetchedAt">[],
): Promise<number> => {
  if (results.length === 0) {
    return 0;
  }
  const stored = await getLatestRaceResults(db, raceKey);
  if (
    stored !== null &&
    resultSnapshotSignature(stored.horses) === resultSnapshotSignature(results)
  ) {
    return results.length;
  }
  await db.prepare("delete from race_result_snapshots where race_key = ?").bind(raceKey).run();
  await runD1Batches(
    db,
    results.map((result) =>
      db
        .prepare(
          `
            insert into race_result_snapshots (
              race_key, fetched_at, horse_number, horse_name,
              finish_position, time
            )
            values (?, ?, ?, ?, ?, ?)
          `,
        )
        .bind(
          raceKey,
          fetchedAt,
          result.horseNumber,
          result.horseName,
          result.finishPosition,
          result.time,
        ),
    ),
  );
  return results.length;
};

const fnv1aHex = (value: string): string => {
  const hash = Array.from(value).reduce(
    (acc, char) => ((acc ^ char.charCodeAt(0)) * LOG_DEDUPE_HASH_PRIME) & LOG_DEDUPE_HASH_MASK,
    LOG_DEDUPE_HASH_OFFSET,
  );
  return (hash >>> 0).toString(LOG_DEDUPE_HEX_RADIX);
};

const buildLogDedupeKey = (
  jobType: string,
  status: string,
  raceKey: string | null,
  message: string | null,
): string => {
  const messageHash = message === null ? LOG_DEDUPE_NULL_TOKEN : fnv1aHex(message);
  const raceToken = raceKey ?? LOG_DEDUPE_NULL_TOKEN;
  return `${LOG_DEDUPE_KV_PREFIX}${jobType}:${status}:${raceToken}:${messageHash}`;
};

const shouldSkipFetchLog = async (
  kv: KVNamespace | undefined,
  jobType: string,
  status: string,
  raceKey: string | null,
  message: string | null,
): Promise<boolean> => {
  if (!kv) return false;
  const key = buildLogDedupeKey(jobType, status, raceKey, message);
  const seen = await kv.get(key).catch(() => null);
  if (seen) return true;
  await kv.put(key, "1", { expirationTtl: LOG_DEDUPE_TTL_SECONDS }).catch(() => undefined);
  return false;
};

export const logFetch = async (
  db: D1Database,
  jobType: string,
  status: string,
  raceKey: string | null,
  message: string | null,
  kv?: KVNamespace,
): Promise<void> => {
  if (await shouldSkipFetchLog(kv, jobType, status, raceKey, message)) return;
  await db
    .prepare(
      "insert into fetch_logs (race_key, job_type, status, message, created_at) values (?, ?, ?, ?, ?)",
    )
    .bind(raceKey, jobType, status, message, toJstIsoString())
    .run();
};

export interface ExportOddsChunkOptions {
  sinceId: number;
  batchSize: number;
  afterFetchedAt?: string;
}

export interface ExportedOddsRow {
  id: number;
  race_key: string;
  fetched_at: string;
  odds_type: string;
  combination: string;
  odds: number | null;
  min_odds: number | null;
  max_odds: number | null;
  average_odds: number | null;
  rank: number | null;
}

const EXPORT_ODDS_BASE_QUERY =
  "select id, race_key, fetched_at, odds_type, combination, odds, min_odds, max_odds, average_odds, rank from odds_snapshots where id > ?";

export const listOddsSnapshotsForExport = async (
  db: D1Database,
  options: ExportOddsChunkOptions,
): Promise<ExportedOddsRow[]> => {
  const query = options.afterFetchedAt
    ? `${EXPORT_ODDS_BASE_QUERY} and fetched_at >= ? order by id asc limit ?`
    : `${EXPORT_ODDS_BASE_QUERY} order by id asc limit ?`;
  const stmt = db.prepare(query);
  const bound = options.afterFetchedAt
    ? stmt.bind(options.sinceId, options.afterFetchedAt, options.batchSize)
    : stmt.bind(options.sinceId, options.batchSize);
  const result = await bound.all<ExportedOddsRow>();
  return result.results;
};

export interface ListRaceSourcesForSeedOptions {
  sinceId: number;
  batchSize: number;
}

export interface ExportedRaceSourceRow {
  race_key: string;
  source: "jra" | "nar";
  race_start_at_jst: string;
  deba_url: string;
  odds_links_json: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  rowid: number;
}

// Used by the PR3 cutover seed script. Reads upcoming race sources (today
// through +3 days) so the new sync-realtime-data-hot Worker can pre-populate
// its odds_fetch_state before /api/internal/odds-fetch-state forwarding takes
// over for newly discovered races.
const LIST_RACE_SOURCES_FOR_SEED_QUERY =
  "select rowid, race_key, source, race_start_at_jst, deba_url, odds_links_json, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango from realtime_race_sources where rowid > ? and race_start_at_jst between datetime('now', '-1 day') and datetime('now', '+3 days') order by rowid asc limit ?";

export const listRaceSourcesForSeed = async (
  db: D1Database,
  options: ListRaceSourcesForSeedOptions,
): Promise<ExportedRaceSourceRow[]> => {
  const result = await db
    .prepare(LIST_RACE_SOURCES_FOR_SEED_QUERY)
    .bind(options.sinceId, options.batchSize)
    .all<ExportedRaceSourceRow>();
  return result.results;
};

export interface ListRaceKeysByDateOptions {
  kaisaiNen: string;
  kaisaiTsukihi: string;
}

export interface RaceKeyRow {
  race_key: string;
}

const LIST_RACE_KEYS_BY_DATE_QUERY =
  "select distinct race_key from realtime_race_sources where kaisai_nen = ? and kaisai_tsukihi = ? order by race_key asc";

// Phase B-1 helper. Returns the per-day distinct race_key list so the
// new sync-realtime-data-features worker's seed script can re-compute
// per-race Parquet features without selecting daily_race_entries.
// daily_race_entries SELECT is forbidden by Phase 0 rule 3.
export const listRaceKeysByDateFromHyperdrive = async (
  db: D1Database,
  options: ListRaceKeysByDateOptions,
): Promise<RaceKeyRow[]> => {
  const result = await db
    .prepare(LIST_RACE_KEYS_BY_DATE_QUERY)
    .bind(options.kaisaiNen, options.kaisaiTsukihi)
    .all<RaceKeyRow>();
  return result.results;
};

export interface DeleteOddsChunkOptions {
  sinceId: number;
  batchSize: number;
  upperBoundId: number;
}

export interface DeleteOddsChunkResult {
  next_since_id: number;
  deleted: number;
  done: boolean;
}

const SELECT_TARGET_IDS_QUERY =
  "select id from odds_snapshots where id > ? and id <= ? order by id asc limit ?";

// Phase F final-step helper. Reads the next slice of ids and deletes them in a
// single bounded statement so the delete query stays compatible with D1's
// per-query memory budget. The caller is expected to throttle invocation
// (night window + sleep) so the live polling workload is not impacted.
export const deleteOddsSnapshotsChunk = async (
  db: D1Database,
  options: DeleteOddsChunkOptions,
): Promise<DeleteOddsChunkResult> => {
  const targets = await db
    .prepare(SELECT_TARGET_IDS_QUERY)
    .bind(options.sinceId, options.upperBoundId, options.batchSize)
    .all<{ id: number }>();
  const ids = targets.results.map((row) => row.id);
  if (ids.length === 0) {
    return { deleted: 0, done: true, next_since_id: options.sinceId };
  }
  const placeholders = ids.map(() => "?").join(", ");
  const result = await db
    .prepare(`delete from odds_snapshots where id in (${placeholders})`)
    .bind(...ids)
    .run();
  const deleted = result.meta.rows_written ?? ids.length;
  return {
    deleted,
    done: ids.length < options.batchSize,
    next_since_id: ids.at(-1)!,
  };
};

export interface DeleteRowidChunkOptions {
  sinceRowid: number;
  chunkSize: number;
}

export interface DeleteRowidChunkResult {
  deletedRowCount: number;
  nextSinceRowid: number;
}

const SELECT_DAILY_RACE_ENTRIES_TARGET_ROWIDS_QUERY =
  "select rowid from daily_race_entries where rowid > ? order by rowid asc limit ?";

// Phase F final-step helper for daily_race_entries. Same SELECT + DELETE IN
// pattern as deleteOddsSnapshotsChunk so the delete statement stays bounded
// and D1 can plan it within the per-query memory budget. The caller (night-
// window CLI) throttles invocation so live polling is unaffected.
export const deleteDailyRaceEntriesChunk = async (
  db: D1Database,
  options: DeleteRowidChunkOptions,
): Promise<DeleteRowidChunkResult> => {
  const targets = await db
    .prepare(SELECT_DAILY_RACE_ENTRIES_TARGET_ROWIDS_QUERY)
    .bind(options.sinceRowid, options.chunkSize)
    .all<{ rowid: number }>();
  const rowids = targets.results.map((row) => row.rowid);
  if (rowids.length === 0) {
    return { deletedRowCount: 0, nextSinceRowid: options.sinceRowid };
  }
  const placeholders = rowids.map(() => "?").join(", ");
  const result = await db
    .prepare(`delete from daily_race_entries where rowid in (${placeholders})`)
    .bind(...rowids)
    .run();
  const deletedRowCount = result.meta.rows_written ?? rowids.length;
  return {
    deletedRowCount,
    nextSinceRowid: rowids.at(-1)!,
  };
};

const SELECT_RACE_RUNNING_STYLES_TARGET_ROWIDS_QUERY =
  "select rowid from race_running_styles where rowid > ? order by rowid asc limit ?";

// Same shape as deleteDailyRaceEntriesChunk but targets the race_running_styles
// table. Phase F cleanup pairs with the matching CLI under scripts/.
export const deleteRaceRunningStylesChunk = async (
  db: D1Database,
  options: DeleteRowidChunkOptions,
): Promise<DeleteRowidChunkResult> => {
  const targets = await db
    .prepare(SELECT_RACE_RUNNING_STYLES_TARGET_ROWIDS_QUERY)
    .bind(options.sinceRowid, options.chunkSize)
    .all<{ rowid: number }>();
  const rowids = targets.results.map((row) => row.rowid);
  if (rowids.length === 0) {
    return { deletedRowCount: 0, nextSinceRowid: options.sinceRowid };
  }
  const placeholders = rowids.map(() => "?").join(", ");
  const result = await db
    .prepare(`delete from race_running_styles where rowid in (${placeholders})`)
    .bind(...rowids)
    .run();
  const deletedRowCount = result.meta.rows_written ?? rowids.length;
  return {
    deletedRowCount,
    nextSinceRowid: rowids.at(-1)!,
  };
};

interface D1RetentionResult {
  fetchLogsDeleted: number;
}

const FETCH_LOGS_RETENTION_DAYS = 30;

const formatIsoCutoff = (now: Date, daysAgo: number): string => {
  const cutoff = new Date(now.getTime() - daysAgo * 24 * 60 * 60 * 1000);
  return toJstIsoString(cutoff);
};

// Trim row backlog so CREATE INDEX and analytic queries stay within D1's
// per-query memory budget. odds_snapshots retention is owned by the new
// sync-realtime-data-hot worker (Phase F handles its archive/retention).
export const runD1Retention = async (
  db: D1Database,
  now = new Date(),
): Promise<D1RetentionResult> => {
  const logsCutoff = formatIsoCutoff(now, FETCH_LOGS_RETENTION_DAYS);
  const logsResult = await db
    .prepare("delete from fetch_logs where created_at < ?")
    .bind(logsCutoff)
    .run()
    .catch((): { meta: { rows_written?: number } } => ({ meta: {} }));
  return {
    fetchLogsDeleted: logsResult.meta.rows_written ?? 0,
  };
};

export const upsertPremiumRaceLink = async (
  db: D1Database,
  raceKey: string,
  link: PremiumRaceLink,
): Promise<void> => {
  const now = toJstIsoString();
  await db
    .prepare(
      `
        insert into premium_race_links (
          race_key, source_race_id, entry_url, discovered_at, updated_at
        )
        values (?, ?, ?, ?, ?)
        on conflict(race_key) do update set
          source_race_id = excluded.source_race_id,
          entry_url = excluded.entry_url,
          updated_at = excluded.updated_at
      `,
    )
    .bind(raceKey, link.sourceRaceId, link.entryUrl, now, now)
    .run();
};

export const getPremiumRaceLink = async (
  db: D1Database,
  raceKey: string,
): Promise<PremiumRaceLink | null> => {
  const row = await db
    .prepare(
      "select race_key, source_race_id, entry_url from premium_race_links where race_key = ?",
    )
    .bind(raceKey)
    .first<PremiumRaceLinkRow>();
  return row ? { entryUrl: row.entry_url, sourceRaceId: row.source_race_id } : null;
};

const compareStableCommentsForHash = (
  left: PremiumStableComment,
  right: PremiumStableComment,
): number => left.horseNumber.localeCompare(right.horseNumber);

const compareTrainingReviewsForHash = (
  left: PremiumTrainingReview,
  right: PremiumTrainingReview,
): number =>
  `${left.horseNumber}:${left.trainingDate}`.localeCompare(
    `${right.horseNumber}:${right.trainingDate}`,
  );

const toPremiumPaddockContentHashHexByte = (byte: number): string =>
  byte
    .toString(PREMIUM_PADDOCK_CONTENT_HASH_BYTE_RADIX)
    .padStart(PREMIUM_PADDOCK_CONTENT_HASH_BYTE_PAD_LENGTH, "0");

export const computePremiumPaddockContentHash = async (
  input: PremiumPaddockContentHashInput,
): Promise<string> => {
  const sortedStableComments = (input.stableComments ?? []).toSorted(compareStableCommentsForHash);
  const sortedTrainingReviews = (input.trainingReviews ?? []).toSorted(
    compareTrainingReviewsForHash,
  );
  const payload = JSON.stringify({
    stableComments: sortedStableComments,
    trainingReviews: sortedTrainingReviews,
  });
  const digest = await crypto.subtle.digest(
    PREMIUM_PADDOCK_CONTENT_HASH_ALGORITHM,
    new TextEncoder().encode(payload),
  );
  return Array.from(new Uint8Array(digest), toPremiumPaddockContentHashHexByte)
    .join("")
    .slice(0, PREMIUM_PADDOCK_CONTENT_HASH_HEX_LENGTH);
};

const getPremiumPaddockContentHash = async (
  db: D1Database,
  raceKey: string,
): Promise<string | null> => {
  const row = await db
    .prepare("select content_hash from premium_paddock_content_hashes where race_key = ?")
    .bind(raceKey)
    .first<PremiumPaddockContentHashRow>();
  return row ? row.content_hash : null;
};

export const replacePremiumRaceData = async (
  db: D1Database,
  params: {
    dataTopHorses?: PremiumDataTopHorse[];
    fetchedAt: string;
    link: PremiumRaceLink;
    paddockBulletins?: PremiumPaddockBulletin[];
    raceKey: string;
    stableComments?: PremiumStableComment[];
    trainingReviews?: PremiumTrainingReview[];
  },
): Promise<void> => {
  const now = toJstIsoString();
  // Dedup the stable_comments + training_reviews rewrites when the scraped
  // content hasn't changed since the last fetch. paddockBulletins and
  // dataTopHorses are still rewritten unconditionally because they live in
  // separate tables and the paddock job doesn't pass review/comment fields.
  const hasReviewOrComment =
    params.trainingReviews !== undefined || params.stableComments !== undefined;
  const newContentHash = hasReviewOrComment
    ? await computePremiumPaddockContentHash({
        stableComments: params.stableComments,
        trainingReviews: params.trainingReviews,
      })
    : null;
  const previousContentHash = hasReviewOrComment
    ? await getPremiumPaddockContentHash(db, params.raceKey)
    : null;
  const skipReviewAndCommentWrites =
    newContentHash !== null &&
    previousContentHash !== null &&
    previousContentHash === newContentHash;
  const trainingReviewsForInsert: PremiumTrainingReview[] = skipReviewAndCommentWrites
    ? []
    : (params.trainingReviews ?? []);
  const stableCommentsForInsert: PremiumStableComment[] = skipReviewAndCommentWrites
    ? []
    : (params.stableComments ?? []);
  const statements: D1PreparedStatement[] = [
    ...(params.trainingReviews && !skipReviewAndCommentWrites
      ? [db.prepare("delete from premium_training_reviews where race_key = ?").bind(params.raceKey)]
      : []),
    ...(params.stableComments && !skipReviewAndCommentWrites
      ? [db.prepare("delete from premium_stable_comments where race_key = ?").bind(params.raceKey)]
      : []),
    ...(params.dataTopHorses
      ? [db.prepare("delete from premium_data_top_horses where race_key = ?").bind(params.raceKey)]
      : []),
    ...(params.paddockBulletins
      ? [
          db
            .prepare("delete from premium_paddock_bulletins where race_key = ?")
            .bind(params.raceKey),
        ]
      : []),
    ...trainingReviewsForInsert.map((row) =>
      db
        .prepare(
          `
            insert into premium_training_reviews (
              race_key, source_race_id, fetched_at, horse_number, horse_name,
              training_date, evaluation_text, evaluation_grade, comment_text, rider_name, created_at
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(race_key, horse_number, training_date) do update set
              source_race_id = excluded.source_race_id,
              fetched_at = excluded.fetched_at,
              horse_name = excluded.horse_name,
              evaluation_text = excluded.evaluation_text,
              evaluation_grade = excluded.evaluation_grade,
              comment_text = excluded.comment_text,
              rider_name = excluded.rider_name,
              created_at = excluded.created_at
          `,
        )
        .bind(
          params.raceKey,
          params.link.sourceRaceId,
          params.fetchedAt,
          row.horseNumber,
          row.horseName,
          row.trainingDate,
          row.evaluationText,
          row.evaluationGrade,
          row.commentText,
          row.riderName,
          now,
        ),
    ),
    ...stableCommentsForInsert.map((row) =>
      db
        .prepare(
          `
            insert into premium_stable_comments (
              race_key, source_race_id, fetched_at, frame_number, horse_number,
              horse_name, comment_text, evaluation_text, evaluation_grade, created_at
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(race_key, horse_number) do update set
              source_race_id = excluded.source_race_id,
              fetched_at = excluded.fetched_at,
              frame_number = excluded.frame_number,
              horse_name = excluded.horse_name,
              comment_text = excluded.comment_text,
              evaluation_text = excluded.evaluation_text,
              evaluation_grade = excluded.evaluation_grade,
              created_at = excluded.created_at
          `,
        )
        .bind(
          params.raceKey,
          params.link.sourceRaceId,
          params.fetchedAt,
          row.frameNumber,
          row.horseNumber,
          row.horseName,
          row.commentText,
          row.evaluationText,
          row.evaluationGrade,
          now,
        ),
    ),
    ...(params.dataTopHorses ?? []).map((row) =>
      db
        .prepare(
          `
            insert into premium_data_top_horses (
              race_key, source_race_id, fetched_at, rank, horse_number,
              horse_name, reasons_json, created_at
            )
            values (?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(race_key, rank) do update set
              source_race_id = excluded.source_race_id,
              fetched_at = excluded.fetched_at,
              horse_number = excluded.horse_number,
              horse_name = excluded.horse_name,
              reasons_json = excluded.reasons_json,
              created_at = excluded.created_at
          `,
        )
        .bind(
          params.raceKey,
          params.link.sourceRaceId,
          params.fetchedAt,
          row.rank,
          row.horseNumber,
          row.horseName,
          JSON.stringify(row.reasons),
          now,
        ),
    ),
    ...(params.paddockBulletins ?? []).map((row) =>
      db
        .prepare(
          `
            insert into premium_paddock_bulletins (
              race_key, source_race_id, fetched_at, group_key, frame_number,
              horse_number, horse_name, evaluation_text, comment_text, created_at
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(race_key, group_key, horse_number) do update set
              source_race_id = excluded.source_race_id,
              fetched_at = excluded.fetched_at,
              frame_number = excluded.frame_number,
              horse_name = excluded.horse_name,
              evaluation_text = excluded.evaluation_text,
              comment_text = excluded.comment_text,
              created_at = excluded.created_at
          `,
        )
        .bind(
          params.raceKey,
          params.link.sourceRaceId,
          params.fetchedAt,
          row.groupKey,
          row.frameNumber,
          row.horseNumber,
          row.horseName,
          row.evaluationText,
          row.commentText,
          now,
        ),
    ),
    ...(newContentHash !== null
      ? [
          db
            .prepare(
              `
                insert into premium_paddock_content_hashes (race_key, content_hash, updated_at)
                values (?, ?, ?)
                on conflict(race_key) do update set
                  content_hash = excluded.content_hash,
                  updated_at = excluded.updated_at
              `,
            )
            .bind(params.raceKey, newContentHash, now),
        ]
      : []),
  ];
  await runD1Batches(db, statements);
};

export const getPremiumRacePayload = async (
  db: D1Database,
  raceKey: string,
): Promise<PremiumRacePayload> => {
  const [trainingRows, commentRows, paddockRows, dataTopRows] = await Promise.all([
    db
      .prepare(
        `
          select *
          from premium_training_reviews
          where race_key = ?
          order by cast(horse_number as integer), training_date desc
        `,
      )
      .bind(raceKey)
      .all<PremiumTrainingReviewRow>(),
    db
      .prepare(
        `
          select *
          from premium_stable_comments
          where race_key = ?
          order by coalesce(evaluation_grade, 99), cast(horse_number as integer)
        `,
      )
      .bind(raceKey)
      .all<PremiumStableCommentRow>(),
    db
      .prepare(
        `
          select *
          from premium_paddock_bulletins
          where race_key = ?
          order by group_key, cast(horse_number as integer)
        `,
      )
      .bind(raceKey)
      .all<PremiumPaddockBulletinRow>(),
    db
      .prepare(
        `
          select *
          from premium_data_top_horses
          where race_key = ?
          order by rank
        `,
      )
      .bind(raceKey)
      .all<PremiumDataTopHorseRow>(),
  ]);
  return {
    dataTopHorses: dataTopRows.results.map((row) => ({
      fetchedAt: row.fetched_at,
      horseName: row.horse_name,
      horseNumber: row.horse_number,
      rank: row.rank,
      reasons: parsePremiumDataTopReasons(row.reasons_json),
    })),
    paddockBulletins: paddockRows.results.map((row) => ({
      commentText: row.comment_text,
      evaluationText: row.evaluation_text,
      fetchedAt: row.fetched_at,
      frameNumber: row.frame_number,
      groupKey: row.group_key,
      horseName: row.horse_name,
      horseNumber: row.horse_number,
    })),
    stableComments: commentRows.results.map((row) => ({
      commentText: row.comment_text,
      evaluationGrade: row.evaluation_grade,
      evaluationText: row.evaluation_text,
      fetchedAt: row.fetched_at,
      frameNumber: row.frame_number,
      horseName: row.horse_name,
      horseNumber: row.horse_number,
    })),
    trainingReviews: trainingRows.results.map((row) => ({
      commentText: row.comment_text,
      evaluationGrade: row.evaluation_grade,
      evaluationText: row.evaluation_text,
      fetchedAt: row.fetched_at,
      horseName: row.horse_name,
      horseNumber: row.horse_number,
      riderName: row.rider_name,
      trainingDate: row.training_date,
    })),
  };
};

export const listPremiumRaceDataFetchCandidatesByDate = async (
  db: D1Database,
  targetDate: string,
  now: string,
): Promise<PremiumRaceDataFetchCandidate[]> => {
  const rows = await db
    .prepare(
      `
        select rs.race_key
        from realtime_race_sources rs
        inner join premium_race_links link on link.race_key = rs.race_key
        left join premium_race_data_fetch_state state on state.race_key = rs.race_key
        where rs.source in ('jra', 'nar')
          and rs.keibajo_code != '83'
          and rs.kaisai_nen = ?
          and rs.kaisai_tsukihi = ?
          and (
            state.race_key is null
            or state.status in ('idle', 'failed')
            or (state.status = 'pending' and (state.retry_after is null or state.retry_after <= ?))
            or (
              state.status = 'queued'
              and state.last_queued_at is not null
              and datetime(state.last_queued_at) <= datetime(?, '-15 minutes')
            )
            or (
              state.status in ('ok', 'empty', 'auth_required')
              and (state.retry_after is null or state.retry_after <= ?)
              and (
                not exists (
                  select 1
                  from premium_data_top_horses data_top
                  where data_top.race_key = rs.race_key
                )
                or (
                  rs.race_start_at_jst is not null
                  and datetime(rs.race_start_at_jst) > datetime(?, '-15 minutes')
                  and (
                    state.last_fetch_at is null
                    or datetime(state.last_fetch_at) < datetime(rs.race_start_at_jst, '-30 minutes')
                  )
                )
              )
              and datetime(coalesce(state.last_fetch_at, '1970-01-01T00:00:00+09:00'))
                <= datetime(?, '-5 minutes')
            )
          )
          and (
            state.last_queued_at is null
            or datetime(state.last_queued_at) <= datetime(?, '-5 minutes')
          )
        order by
          case when rs.race_start_at_jst is null then 1 else 0 end,
          abs(julianday(rs.race_start_at_jst) - julianday(?)) asc,
          rs.keibajo_code,
          rs.race_bango
      `,
    )
    .bind(targetDate.slice(0, 4), targetDate.slice(4, 8), now, now, now, now, now, now, now)
    .all<{ race_key: string }>();
  return rows.results.map((row) => ({ raceKey: row.race_key }));
};

export const markPremiumRaceDataQueued = async (
  db: D1Database,
  raceKeys: string[],
  queuedAt: string,
): Promise<void> => {
  if (raceKeys.length === 0) {
    return;
  }
  await runD1Batches(
    db,
    raceKeys.map((raceKey) =>
      db
        .prepare(
          `
            insert into premium_race_data_fetch_state (
              race_key, status, message, last_queued_at, updated_at
            )
            values (?, 'queued', null, ?, ?)
            on conflict(race_key) do update set
              status = 'queued',
              message = null,
              last_queued_at = excluded.last_queued_at,
              updated_at = excluded.updated_at
          `,
        )
        .bind(raceKey, queuedAt, queuedAt),
    ),
  );
};

export const getPremiumRaceDataFetchState = async (
  db: D1Database,
  raceKey: string,
): Promise<{
  lastFetchAt: string | null;
  lastQueuedAt: string | null;
  message: string | null;
  retryAfter: string | null;
  status: string;
} | null> => {
  const row = await db
    .prepare(
      `
        select status, last_queued_at, last_fetch_at, retry_after, message
        from premium_race_data_fetch_state
        where race_key = ?
      `,
    )
    .bind(raceKey)
    .first<PremiumRaceDataFetchStateRow>();
  return row
    ? {
        lastFetchAt: row.last_fetch_at,
        lastQueuedAt: row.last_queued_at,
        message: row.message ?? null,
        retryAfter: row.retry_after,
        status: row.status,
      }
    : null;
};

export const updatePremiumRaceDataFetchState = async (
  db: D1Database,
  params: {
    fetchedAt?: string | null;
    message?: string | null;
    raceKey: string;
    retryAfter?: string | null;
    status: string;
  },
): Promise<void> => {
  const now = toJstIsoString();
  await db
    .prepare(
      `
        insert into premium_race_data_fetch_state (
          race_key, status, message, last_fetch_at, retry_after, updated_at
        )
        values (?, ?, ?, ?, ?, ?)
        on conflict(race_key) do update set
          status = excluded.status,
          message = excluded.message,
          last_fetch_at = coalesce(excluded.last_fetch_at, premium_race_data_fetch_state.last_fetch_at),
          retry_after = excluded.retry_after,
          updated_at = excluded.updated_at
      `,
    )
    .bind(
      params.raceKey,
      params.status,
      params.message ?? null,
      params.fetchedAt ?? null,
      params.retryAfter ?? null,
      now,
    )
    .run();
};

export const markPremiumPaddockQueued = async (
  db: D1Database,
  raceKeys: string[],
  queuedAt: string,
): Promise<void> => {
  if (raceKeys.length === 0) {
    return;
  }
  await runD1Batches(
    db,
    raceKeys.map((raceKey) =>
      db
        .prepare(
          `
            insert into premium_paddock_fetch_state (
              race_key, status, last_queued_at, updated_at
            )
            values (?, 'queued', ?, ?)
            on conflict(race_key) do update set
              status = 'queued',
              last_queued_at = excluded.last_queued_at,
              updated_at = excluded.updated_at
          `,
        )
        .bind(raceKey, queuedAt, queuedAt),
    ),
  );
};

export const getPremiumPaddockFetchState = async (
  db: D1Database,
  raceKey: string,
): Promise<{
  lastFetchAt: string | null;
  lastQueuedAt: string | null;
  retryAfter: string | null;
  status: string;
} | null> => {
  const row = await db
    .prepare(
      `
        select status, last_queued_at, last_fetch_at, retry_after
        from premium_paddock_fetch_state
        where race_key = ?
      `,
    )
    .bind(raceKey)
    .first<PremiumPaddockFetchStateRow>();
  return row
    ? {
        lastFetchAt: row.last_fetch_at,
        lastQueuedAt: row.last_queued_at,
        retryAfter: row.retry_after,
        status: row.status,
      }
    : null;
};

export const updatePremiumPaddockFetchState = async (
  db: D1Database,
  params: {
    fetchedAt?: string | null;
    message?: string | null;
    raceKey: string;
    retryAfter?: string | null;
    status: string;
  },
): Promise<void> => {
  const now = toJstIsoString();
  await db
    .prepare(
      `
        insert into premium_paddock_fetch_state (
          race_key, status, message, last_fetch_at, retry_after, updated_at
        )
        values (?, ?, ?, ?, ?, ?)
        on conflict(race_key) do update set
          status = excluded.status,
          message = excluded.message,
          last_fetch_at = excluded.last_fetch_at,
          retry_after = excluded.retry_after,
          last_queued_at = null,
          fetch_lock_until = null,
          updated_at = excluded.updated_at
      `,
    )
    .bind(
      params.raceKey,
      params.status,
      params.message ?? null,
      params.fetchedAt ?? null,
      params.retryAfter ?? null,
      now,
    )
    .run();
};

export const getPremiumPaddockNotificationState = async (
  db: D1Database,
  raceKey: string,
): Promise<{
  lastPayloadFetchedAt: string | null;
  lastNotifiedAt: string | null;
  lastSendAttemptAt: string | null;
  message: string | null;
  payloadSignature: string | null;
  skipReason: string | null;
  status: string;
} | null> => {
  const row = await db
    .prepare(
      `
        select
          status,
          payload_signature,
          last_payload_fetched_at,
          last_send_attempt_at,
          last_notified_at,
          skip_reason,
          message
        from premium_paddock_notification_state
        where race_key = ?
      `,
    )
    .bind(raceKey)
    .first<PremiumPaddockNotificationStateRow>();
  return row
    ? {
        lastPayloadFetchedAt: row.last_payload_fetched_at,
        lastNotifiedAt: row.last_notified_at,
        lastSendAttemptAt: row.last_send_attempt_at,
        message: row.message,
        payloadSignature: row.payload_signature,
        skipReason: row.skip_reason,
        status: row.status,
      }
    : null;
};

export const updatePremiumPaddockNotificationState = async (
  db: D1Database,
  params: {
    message?: string | null;
    payloadFetchedAt?: string | null;
    notifiedAt?: string | null;
    payloadSignature: string;
    raceKey: string;
    sendAttemptAt?: string | null;
    skipReason?: string | null;
    status: string;
  },
): Promise<void> => {
  const now = toJstIsoString();
  await db
    .prepare(
      `
        insert into premium_paddock_notification_state (
          race_key,
          status,
          payload_signature,
          last_payload_fetched_at,
          last_send_attempt_at,
          last_notified_at,
          skip_reason,
          message,
          updated_at
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict(race_key) do update set
          status = excluded.status,
          payload_signature = excluded.payload_signature,
          last_payload_fetched_at = coalesce(
            excluded.last_payload_fetched_at,
            premium_paddock_notification_state.last_payload_fetched_at
          ),
          last_send_attempt_at = coalesce(
            excluded.last_send_attempt_at,
            premium_paddock_notification_state.last_send_attempt_at
          ),
          last_notified_at = coalesce(
            excluded.last_notified_at,
            premium_paddock_notification_state.last_notified_at
          ),
          skip_reason = excluded.skip_reason,
          message = excluded.message,
          updated_at = excluded.updated_at
      `,
    )
    .bind(
      params.raceKey,
      params.status,
      params.payloadSignature,
      params.payloadFetchedAt ?? null,
      params.sendAttemptAt ?? null,
      params.notifiedAt ?? null,
      params.skipReason ?? null,
      params.message ?? null,
      now,
    )
    .run();
};

export const claimPremiumPaddockNotificationSend = async (
  db: D1Database,
  params: {
    lockBefore: string;
    payloadFetchedAt: string;
    payloadSignature: string;
    raceKey: string;
    sendAttemptAt: string;
  },
): Promise<boolean> => {
  const now = toJstIsoString();
  const inserted = await db
    .prepare(
      `
        insert into premium_paddock_notification_state (
          race_key,
          status,
          payload_signature,
          last_payload_fetched_at,
          last_send_attempt_at,
          last_notified_at,
          skip_reason,
          message,
          updated_at
        )
        values (?, 'sending', ?, ?, ?, null, null, null, ?)
        on conflict(race_key) do nothing
      `,
    )
    .bind(
      params.raceKey,
      params.payloadSignature,
      params.payloadFetchedAt,
      params.sendAttemptAt,
      now,
    )
    .run();
  if (inserted.meta.changes > 0) {
    return true;
  }

  const result = await db
    .prepare(
      `
        update premium_paddock_notification_state
        set status = 'sending',
          payload_signature = ?,
          last_payload_fetched_at = ?,
          last_send_attempt_at = ?,
          skip_reason = null,
          message = null,
          updated_at = ?
        where race_key = ?
          and last_notified_at is null
          and (
            last_send_attempt_at is null
            or last_send_attempt_at < ?
            or status not in ('sending', 'ok')
          )
      `,
    )
    .bind(
      params.payloadSignature,
      params.payloadFetchedAt,
      params.sendAttemptAt,
      now,
      params.raceKey,
      params.lockBefore,
    )
    .run();
  return result.meta.changes > 0;
};

export const recordPremiumPaddockNotificationEvent = async (
  db: D1Database,
  params: {
    fetchedAt: string;
    message?: string | null;
    payloadSignature: string;
    raceKey: string;
    sentAt?: string | null;
    skipReason?: string | null;
    status: string;
  },
): Promise<void> => {
  const now = toJstIsoString();
  await db
    .prepare(
      `
        insert into premium_paddock_notification_events (
          race_key,
          fetched_at,
          payload_signature,
          status,
          skip_reason,
          message,
          sent_at,
          created_at,
          updated_at
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict(race_key, fetched_at) do update set
          payload_signature = excluded.payload_signature,
          status = excluded.status,
          skip_reason = excluded.skip_reason,
          message = excluded.message,
          sent_at = coalesce(excluded.sent_at, premium_paddock_notification_events.sent_at),
          updated_at = excluded.updated_at
      `,
    )
    .bind(
      params.raceKey,
      params.fetchedAt,
      params.payloadSignature,
      params.status,
      params.skipReason ?? null,
      params.message ?? null,
      params.sentAt ?? null,
      now,
      now,
    )
    .run();
};

export const toHorseTrends = (history: OddsHistoryPoint[]): HorseOddsTrend[] => {
  const byHorse = new Map<string, OddsHistoryPoint[]>();
  for (const point of history) {
    byHorse.set(point.horseNumber, [...(byHorse.get(point.horseNumber) ?? []), point]);
  }
  return Array.from(byHorse.entries()).map(([horseNumber, points]) => ({ horseNumber, points }));
};

export const toOddsTrendsByType = (
  historyByType: Partial<Record<OddsType, OddsTrendPoint[]>>,
): Partial<Record<OddsType, OddsTrend[]>> => {
  const result: Partial<Record<OddsType, OddsTrend[]>> = {};
  for (const [oddsType, history] of Object.entries(historyByType) as [
    OddsType,
    OddsTrendPoint[],
  ][]) {
    const byCombination = new Map<string, OddsTrendPoint[]>();
    for (const point of history) {
      byCombination.set(point.combination, [
        ...(byCombination.get(point.combination) ?? []),
        point,
      ]);
    }
    result[oddsType] = Array.from(byCombination.entries()).map(([combination, points]) => ({
      combination,
      points,
    }));
  }
  return result;
};

const LATEST_HORSE_WEIGHTS_SQL = `
  select *
  from horse_weight_snapshots
  where race_key = ?
    and fetched_at = (
      select max(fetched_at) from horse_weight_snapshots where race_key = ?
    )
  order by cast(horse_number as integer) asc
`;

const LATEST_RACE_ENTRIES_SQL = `
  select *
  from race_entry_snapshots
  where race_key = ?
    and fetched_at = (
      select max(fetched_at) from race_entry_snapshots where race_key = ?
    )
  order by cast(horse_number as integer) asc
`;

const LATEST_RACE_RESULTS_SQL = `
  select *
  from race_result_snapshots
  where race_key = ?
    and fetched_at = (
      select max(fetched_at) from race_result_snapshots where race_key = ?
    )
  order by
    case when finish_position glob '[0-9]*' then cast(finish_position as integer) else 999 end asc,
    cast(horse_number as integer) asc
`;

const mapHorseWeights = (
  rows: ReadonlyArray<WeightSnapshotRow>,
): { fetchedAt: string; horses: HorseWeight[] } | null => {
  const firstFetchedAt = rows[0]?.fetched_at;
  if (!firstFetchedAt) return null;
  return {
    fetchedAt: firstFetchedAt,
    horses: rows.map((row) => ({
      changeAmount: row.change_amount,
      changeSign: row.change_sign,
      horseName: row.horse_name,
      horseNumber: row.horse_number,
      weight: row.weight,
    })),
  };
};

const mapRaceEntries = (
  rows: ReadonlyArray<RaceEntrySnapshotRow>,
): { fetchedAt: string; horses: RaceEntry[] } | null => {
  const firstFetchedAt = rows[0]?.fetched_at;
  if (!firstFetchedAt) return null;
  return {
    fetchedAt: firstFetchedAt,
    horses: rows.map((row) => ({
      fetchedAt: row.fetched_at,
      horseName: row.horse_name,
      horseNumber: row.horse_number,
      jockeyName: row.jockey_name,
      status: row.status,
    })),
  };
};

const mapRaceResults = (
  rows: ReadonlyArray<RaceResultSnapshotRow>,
): { fetchedAt: string; horses: RaceResult[] } | null => {
  const firstFetchedAt = rows[0]?.fetched_at;
  if (!firstFetchedAt) return null;
  return {
    fetchedAt: firstFetchedAt,
    horses: rows.map((row) => ({
      fetchedAt: row.fetched_at,
      finishPosition: row.finish_position,
      horseName: row.horse_name,
      horseNumber: row.horse_number,
      time: row.time,
    })),
  };
};

export const getLatestHorseWeights = async (
  db: D1Database,
  raceKey: string,
): Promise<{ fetchedAt: string; horses: HorseWeight[] } | null> => {
  const result = await db
    .prepare(LATEST_HORSE_WEIGHTS_SQL)
    .bind(raceKey, raceKey)
    .all<WeightSnapshotRow>();
  return mapHorseWeights(result.results);
};

export const getLatestRaceEntries = async (
  db: D1Database,
  raceKey: string,
): Promise<{ fetchedAt: string; horses: RaceEntry[] } | null> => {
  const result = await db
    .prepare(LATEST_RACE_ENTRIES_SQL)
    .bind(raceKey, raceKey)
    .all<RaceEntrySnapshotRow>();
  return mapRaceEntries(result.results);
};

export const getLatestRaceResults = async (
  db: D1Database,
  raceKey: string,
): Promise<{ fetchedAt: string; horses: RaceResult[] } | null> => {
  const result = await db
    .prepare(LATEST_RACE_RESULTS_SQL)
    .bind(raceKey, raceKey)
    .all<RaceResultSnapshotRow>();
  return mapRaceResults(result.results);
};

const toTrackCondition = (row: TrackConditionSnapshotRow): TrackCondition => ({
  dirt: {
    condition: row.dirt_condition,
    measurementDate: row.dirt_measurement_date,
    moisture: {
      finalBend: row.dirt_moisture_final_bend,
      finalFurlong: row.dirt_moisture_final_furlong,
      measuredAt: row.dirt_moisture_measured_at,
    },
  },
  fetchedAt: row.fetched_at,
  sourceUpdatedAt: row.source_updated_at,
  turf: {
    condition: row.turf_condition,
    courseLayout: row.turf_course_layout,
    cushionMeasuredAt: row.turf_cushion_measured_at,
    cushionValue: row.turf_cushion_value,
    going: row.turf_going,
    height: {
      japaneseZoysiaGrass: row.turf_height_japanese_zoysia_grass,
      perennialRyegrass: row.turf_height_perennial_ryegrass,
    },
    measurementDate: row.turf_measurement_date,
    moisture: {
      finalBend: row.turf_moisture_final_bend,
      finalFurlong: row.turf_moisture_final_furlong,
      measuredAt: row.turf_moisture_measured_at,
    },
  },
  weather: row.weather,
});

export const getLatestTrackConditionForRace = async (
  db: D1Database,
  raceKey: string,
): Promise<TrackCondition | null> => {
  const row = await db
    .prepare(
      `
        select track.*
        from jra_track_condition_snapshots track
        join realtime_race_sources races
          on races.race_key = track.race_key
        where track.race_key = ?
          and track.fetched_at <= races.race_start_at_jst
        order by track.fetched_at desc, track.id desc
        limit 1
      `,
    )
    .bind(raceKey)
    .first<TrackConditionSnapshotRow>();
  return row ? toTrackCondition(row) : null;
};

export const insertJraTrackConditionSnapshot = async (
  db: D1Database,
  params: {
    condition: TrackCondition;
    date: string;
    fetchedAt: string;
    keibajoCode: string;
  },
): Promise<{ raceKey: string; raceStartAtJst: string }[]> => {
  const races = await db
    .prepare(
      `
        select race_key, race_start_at_jst
        from realtime_race_sources
        where source = 'jra'
          and kaisai_nen = ?
          and kaisai_tsukihi = ?
          and keibajo_code = ?
        order by race_start_at_jst asc
      `,
    )
    .bind(params.date.slice(0, 4), params.date.slice(4, 8), params.keibajoCode)
    .all<{ race_key: string; race_start_at_jst: string }>();
  if (races.results.length === 0) {
    return [];
  }

  await runD1Batches(
    db,
    races.results.map((race) =>
      db
        .prepare(
          `
            insert into jra_track_condition_snapshots (
              race_key, kaisai_nen, kaisai_tsukihi, keibajo_code, fetched_at,
              source_updated_at, weather, turf_condition, turf_measurement_date,
              turf_cushion_value, turf_cushion_measured_at, turf_moisture_measured_at,
              turf_moisture_final_furlong, turf_moisture_final_bend,
              turf_height_japanese_zoysia_grass, turf_height_perennial_ryegrass,
              turf_course_layout, turf_going, dirt_condition, dirt_measurement_date,
              dirt_moisture_measured_at, dirt_moisture_final_furlong, dirt_moisture_final_bend
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `,
        )
        .bind(
          race.race_key,
          params.date.slice(0, 4),
          params.date.slice(4, 8),
          params.keibajoCode,
          params.fetchedAt,
          params.condition.sourceUpdatedAt,
          params.condition.weather,
          params.condition.turf.condition,
          params.condition.turf.measurementDate,
          params.condition.turf.cushionValue,
          params.condition.turf.cushionMeasuredAt,
          params.condition.turf.moisture.measuredAt,
          params.condition.turf.moisture.finalFurlong,
          params.condition.turf.moisture.finalBend,
          params.condition.turf.height.japaneseZoysiaGrass,
          params.condition.turf.height.perennialRyegrass,
          params.condition.turf.courseLayout,
          params.condition.turf.going,
          params.condition.dirt.condition,
          params.condition.dirt.measurementDate,
          params.condition.dirt.moisture.measuredAt,
          params.condition.dirt.moisture.finalFurlong,
          params.condition.dirt.moisture.finalBend,
        ),
    ),
  );

  return races.results.map((race) => ({
    raceKey: race.race_key,
    raceStartAtJst: race.race_start_at_jst,
  }));
};

export const getSameDayVenueJockeyWins = async (
  db: D1Database,
  params: {
    kaisaiNen: string;
    kaisaiTsukihi: string;
    keibajoCode: string;
    beforeRaceBango: string;
  },
): Promise<
  {
    jockeyName: string;
    latestRaceNumber: string;
    winCount: number;
  }[]
> => {
  const result = await db
    .prepare(
      `
        with target_races as (
          select race_key, race_bango
          from realtime_race_sources
          where kaisai_nen = ?
            and kaisai_tsukihi = ?
            and keibajo_code = ?
            and cast(race_bango as integer) < cast(? as integer)
        ),
        latest_results as (
          select race_key, max(fetched_at) fetched_at
          from race_result_snapshots
          where race_key in (select race_key from target_races)
          group by race_key
        ),
        latest_entries as (
          select race_key, max(fetched_at) fetched_at
          from race_entry_snapshots
          where race_key in (select race_key from target_races)
          group by race_key
        ),
        winners as (
          select
            target_races.race_bango,
            entries.jockey_name
          from target_races
          join latest_results
            on latest_results.race_key = target_races.race_key
          join race_result_snapshots results
            on results.race_key = latest_results.race_key
            and results.fetched_at = latest_results.fetched_at
            and results.finish_position in ('1', '01')
          join latest_entries
            on latest_entries.race_key = target_races.race_key
          join race_entry_snapshots entries
            on entries.race_key = latest_entries.race_key
            and entries.fetched_at = latest_entries.fetched_at
            and entries.horse_number = results.horse_number
          where entries.jockey_name is not null
            and trim(entries.jockey_name) <> ''
        )
        select
          jockey_name,
          count(*) win_count,
          max(race_bango) latest_race_bango
        from winners
        group by jockey_name
        order by win_count desc, latest_race_bango desc, jockey_name asc
      `,
    )
    .bind(params.kaisaiNen, params.kaisaiTsukihi, params.keibajoCode, params.beforeRaceBango)
    .all<SameDayVenueJockeyWinRow>();
  return result.results.map((row) => ({
    jockeyName: row.jockey_name,
    latestRaceNumber: row.latest_race_bango,
    winCount: row.win_count,
  }));
};

export const buildRealtimePayload = async (
  db: D1Database,
  raceKey: string,
  source: NarRaceSource | null,
  odds: HotOddsPayload | null,
  trackCondition: TrackCondition | null = null,
): Promise<RealtimeRacePayload> => {
  // Single db.batch RPC for race-entry / horse-weight / result snapshots so the
  // /realtime polling endpoint pays one D1 round-trip per request instead of
  // three. The odds payload (history, historyByType, latest) is fetched from
  // the new sync-realtime-data-hot worker before this call, so this no longer
  // touches the odds_snapshots table.
  const [entriesResult, weightsResult, resultsResult] = await db.batch<
    RaceEntrySnapshotRow | WeightSnapshotRow | RaceResultSnapshotRow
  >([
    db.prepare(LATEST_RACE_ENTRIES_SQL).bind(raceKey, raceKey),
    db.prepare(LATEST_HORSE_WEIGHTS_SQL).bind(raceKey, raceKey),
    db.prepare(LATEST_RACE_RESULTS_SQL).bind(raceKey, raceKey),
  ]);
  const raceEntries = mapRaceEntries((entriesResult?.results ?? []) as RaceEntrySnapshotRow[]);
  const horseWeights = mapHorseWeights((weightsResult?.results ?? []) as WeightSnapshotRow[]);
  const raceResults = mapRaceResults((resultsResult?.results ?? []) as RaceResultSnapshotRow[]);
  return {
    raceEntries,
    horseWeights,
    odds: odds
      ? {
          fetchedAt: odds.fetchedAt,
          history: odds.history,
          historyByType: odds.historyByType,
          horseTrends: toHorseTrends(odds.history),
          latest: odds.latest,
          trendsByType: toOddsTrendsByType(odds.historyByType),
        }
      : null,
    raceResults,
    raceKey,
    source,
    trackCondition,
  };
};
