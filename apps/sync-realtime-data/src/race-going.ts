// run with: bun run test (vitest)
// Pre-race going (馬場状態) side record for finish-position serving. Pre-race
// JV RA records never carry the going, so the race-day 出馬表 page is the only
// source until the results RA lands after racing. Win odds / popularity are
// never read here.
import { formatError } from "./format-error";
import { fetchRacePage } from "./keiba-go";
import { getRaceSource, logFetch } from "./storage";
import { toJstIsoString } from "./time";

export type GoingSurface = "dirt" | "turf";

export interface RaceGoing {
  going: string;
  surface: GoingSurface;
}

export interface RaceGoingObservation extends RaceGoing {
  observedAt: string;
}

export interface RaceGoingPayload {
  goings: RaceGoingObservation[];
  raceKey: string;
}

interface UpsertRaceGoingsParams {
  db: D1Database;
  goings: RaceGoing[];
  observedAt: string;
  raceKey: string;
}

interface ReadRaceGoingParams {
  db: D1Database;
  now: Date;
  raceKey: string;
}

interface RaceGoingRow {
  going: string;
  observed_at: string;
  surface: GoingSurface;
}

// Only the four JV going grades are kept: ban-ei moisture ("2.2") and blanks
// are dropped so a going is never invented.
const GOING_GRADES: ReadonlySet<string> = new Set(["良", "稍重", "重", "不良"]);
const SURFACE_BY_LABEL: Record<string, GoingSurface> = { ダート: "dirt", 芝: "turf" };
// jra.go.jp 出馬表 header: <span class="cap">芝</span><span class="txt">稍重</span>.
// A 芝→ダート race lists both surfaces, exactly like its results RA record.
const JRA_GOING_PATTERN: RegExp =
  /<span class="cap">(芝|ダート)<\/span><span class="txt">([^<]*)<\/span>/gu;
// keiba.go.jp DebaTable header: <ul class="dataArea"><li>ダート　1400ｍ（右）　天候：曇　馬場：不良
const NAR_GOING_PATTERN: RegExp = /<ul class="dataArea">\s*<li>(芝|ダート)[^<]*?馬場：([^\s<]+)/u;
const RACE_GOING_PATH_PATTERN: RegExp = /^\/api\/race-going\/(.+)$/u;
const RACE_KEY_PATTERN: RegExp = /^(jra|nar):\d{4}:\d{4}:[0-9A-Z]{2}:\d{2}$/u;
const UPSERT_RACE_GOING_SQL: string = `
  insert into race_going_snapshots (race_key, surface, going, observed_at)
  values (?, ?, ?, ?)
  on conflict (race_key, surface) do update set
    going = excluded.going,
    observed_at = excluded.observed_at
`;
const LIST_RACE_GOINGS_SQL: string = `
  select surface, going, observed_at
  from race_going_snapshots
  where race_key = ?
  order by surface
`;

const toRaceGoing = (label: string, going: string): RaceGoing => ({
  going,
  surface: SURFACE_BY_LABEL[label]!,
});

export const parseJraRaceGoing = (html: string): RaceGoing[] =>
  [...html.matchAll(JRA_GOING_PATTERN)]
    .map((match) => toRaceGoing(match[1]!, match[2]!.trim()))
    .filter((going) => GOING_GRADES.has(going.going));

export const parseNarRaceGoing = (html: string): RaceGoing[] => {
  const match = html.match(NAR_GOING_PATTERN);
  return match && GOING_GRADES.has(match[2]!) ? [toRaceGoing(match[1]!, match[2]!)] : [];
};

export const raceGoingRaceKeyFromRequest = (url: URL): string | null => {
  const match = url.pathname.match(RACE_GOING_PATH_PATTERN);
  const decoded = match ? decodeURIComponent(match[1]!) : "";
  return RACE_KEY_PATTERN.test(decoded) ? decoded : null;
};

export const upsertRaceGoings = async (params: UpsertRaceGoingsParams): Promise<void> => {
  if (params.goings.length === 0) return;
  await params.db.batch(
    params.goings.map((going) =>
      params.db
        .prepare(UPSERT_RACE_GOING_SQL)
        .bind(params.raceKey, going.surface, going.going, params.observedAt),
    ),
  );
};

const toObservation = (row: RaceGoingRow): RaceGoingObservation => ({
  going: row.going,
  observedAt: row.observed_at,
  surface: row.surface,
});

export const listRaceGoings = async (
  db: D1Database,
  raceKey: string,
): Promise<RaceGoingObservation[]> =>
  (await db.prepare(LIST_RACE_GOINGS_SQL).bind(raceKey).all<RaceGoingRow>()).results.map(
    toObservation,
  );

const refreshRaceGoing = async (
  params: ReadRaceGoingParams & { debaUrl: string; source: "jra" | "nar" },
): Promise<void> => {
  try {
    const html = await fetchRacePage(params.debaUrl);
    await upsertRaceGoings({
      db: params.db,
      goings: params.source === "jra" ? parseJraRaceGoing(html) : parseNarRaceGoing(html),
      observedAt: toJstIsoString(params.now),
      raceKey: params.raceKey,
    });
  } catch (error) {
    await logFetch(params.db, "race-going", "error", params.raceKey, formatError(error));
  }
};

// Before post time the race page is re-read so the record tracks same-day
// going changes; after post time only the last pre-race observation is served.
// A failed page fetch keeps the stored observation (or none) -- never a guess.
export const readRaceGoing = async (
  params: ReadRaceGoingParams,
): Promise<RaceGoingPayload | null> => {
  const race = await getRaceSource(params.db, params.raceKey);
  if (!race) return null;
  if (params.now.getTime() < new Date(race.raceStartAtJst).getTime()) {
    await refreshRaceGoing({ ...params, debaUrl: race.debaUrl, source: race.source });
  }
  return { goings: await listRaceGoings(params.db, params.raceKey), raceKey: params.raceKey };
};
