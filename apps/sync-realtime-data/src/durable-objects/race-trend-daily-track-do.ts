// run with: bun run test
// Durable Object that holds the current-day per-track race-trend snapshot
// for one (source, targetYmd, keibajoCode) tuple. Pushes from
// `fetchAndStoreResults` deliver partial / complete result rows on each fetch
// cycle, an alarm-driven re-sync rebuilds the same state from D1 snapshots
// so a missed push (eg. transient DO error) is eventually reconciled, and a
// GET /races endpoint returns the rows whose raceBango is strictly less than
// a query parameter so the viewer can render sibling results for one race.
import type { DurableObjectState } from "@cloudflare/workers-types";
import type {
  RaceTrendDailyTrackResponse,
  RaceTrendDailyTrackRow,
  RaceTrendDailyTrackSource,
  RaceTrendDailyTrackState,
} from "horse-racing-realtime/race-trend-daily-track-types";
import type { Env } from "../types";
import { mergeJsonHeaders } from "../http";

interface RaceTrendStorageLike {
  get: (key: string) => Promise<RaceTrendDailyTrackState | undefined>;
  put: (key: string, value: RaceTrendDailyTrackState) => Promise<void>;
  setAlarm: (at: number) => Promise<void>;
}

interface RaceTrendStateLike {
  blockConcurrencyWhile: (callback: () => Promise<void>) => Promise<void>;
  storage: RaceTrendStorageLike;
}

interface CreateForTestParams {
  env: Env;
  state: RaceTrendStateLike;
}

interface AlarmContext {
  env: Env;
  now: Date;
}

interface ParsedDoId {
  keibajoCode: string;
  source: RaceTrendDailyTrackSource;
  targetYmd: string;
}

interface RawSnapshotRow extends Record<string, unknown> {
  raceKey: string;
  raceBango: string;
  source: RaceTrendDailyTrackSource;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceName: string | null;
  hassoJikoku: string | null;
  umaban: string;
  horseName: string | null;
  jockeyName: string | null;
  finishPosition: string;
  sohaTime: string | null;
  weight: number | null;
  changeSign: string | null;
  changeAmount: number | null;
  fetchedAt: string;
  expectedHorseCount: number | null;
  savedHorseCount: number | null;
  resultCompleteAt: string | null;
}

interface RawRunningStyleRow extends Record<string, unknown> {
  raceKey: string;
  horseNumber: number;
  predictedLabel: string;
}

const STORAGE_KEY = "snapshot";
const PUSH_URL = "https://race-trend-daily-track-do/push";
const RACES_URL = "https://race-trend-daily-track-do/races";

// JST polling window: 09:00 (inclusive) - 23:00 (inclusive). Outside this
// window, races have not started / have all completed for the day, so a
// 30-minute alarm interval is more than enough to catch a stray late push.
const ALARM_WINDOW_START_HOUR = 9;
const ALARM_WINDOW_END_HOUR = 23;
const ALARM_IN_WINDOW_MS = 60_000;
const ALARM_OUT_OF_WINDOW_MS = 30 * 60_000;

const RUNNING_STYLE_LABELS = new Set(["nige", "senkou", "sashi", "oikomi"]);

// Mirrors the viewer's `getRaceTrendTodayStarterRows` SQL, but scoped to a
// single (source, kaisai_nen, kaisai_tsukihi, keibajo_code) tuple so the DO
// owns one venue-day of results. Also pulls the per-race completion meta
// (expected / saved horse count, result_complete_at) so the DO can flag
// each raceBango row as fully complete vs. partial.
const SNAPSHOT_SELECT_SQL = `
  with latest_result as (
    select race_key, horse_number, finish_position, time
    from race_result_snapshots r1
    where fetched_at = (
      select max(fetched_at) from race_result_snapshots r2
      where r2.race_key = r1.race_key and r2.horse_number = r1.horse_number
    )
  ),
  latest_entry as (
    select race_key, horse_number, horse_name, jockey_name
    from race_entry_snapshots e1
    where fetched_at = (
      select max(fetched_at) from race_entry_snapshots e2
      where e2.race_key = e1.race_key and e2.horse_number = e1.horse_number
    )
  ),
  latest_weight as (
    select race_key, horse_number, weight, change_sign, change_amount
    from horse_weight_snapshots w1
    where fetched_at = (
      select max(fetched_at) from horse_weight_snapshots w2
      where w2.race_key = w1.race_key and w2.horse_number = w1.horse_number
    )
  ),
  latest_fetch_at as (
    select race_key, max(fetched_at) as fetched_at
    from race_result_snapshots
    group by race_key
  )
  select
    s.source as source,
    r.race_key as raceKey,
    s.kaisai_nen as kaisaiNen,
    s.kaisai_tsukihi as kaisaiTsukihi,
    s.keibajo_code as keibajoCode,
    s.race_bango as raceBango,
    s.race_name as raceName,
    s.race_start_at_jst as hassoJikoku,
    r.horse_number as umaban,
    e.horse_name as horseName,
    e.jockey_name as jockeyName,
    r.finish_position as finishPosition,
    r.time as sohaTime,
    w.weight as weight,
    w.change_sign as changeSign,
    w.change_amount as changeAmount,
    f.fetched_at as fetchedAt,
    s.result_expected_horse_count as expectedHorseCount,
    s.result_saved_horse_count as savedHorseCount,
    s.result_complete_at as resultCompleteAt
  from latest_result r
  join realtime_race_sources s on s.race_key = r.race_key
  left join latest_entry e on e.race_key = r.race_key and e.horse_number = r.horse_number
  left join latest_weight w on w.race_key = r.race_key and w.horse_number = r.horse_number
  left join latest_fetch_at f on f.race_key = r.race_key
  where s.source = ?
    and s.kaisai_nen = ?
    and s.kaisai_tsukihi = ?
    and s.keibajo_code = ?
  order by s.race_bango asc, cast(nullif(r.horse_number, '') as integer) asc
`;

const RUNNING_STYLE_SELECT_SQL = `
  select race_key as raceKey, horse_number as horseNumber, predicted_label as predictedLabel
  from race_running_styles
  where race_key in (
    select race_key from realtime_race_sources
    where source = ?
      and kaisai_nen = ?
      and kaisai_tsukihi = ?
      and keibajo_code = ?
  )
`;

const json = (body: unknown, init?: ResponseInit): Response =>
  new Response(JSON.stringify(body), {
    headers: mergeJsonHeaders(init),
    status: init?.status ?? 200,
  });

const notFoundResponse = (): Response => json({ error: "not found" }, { status: 404 });

const badRequestResponse = (message: string): Response => json({ error: message }, { status: 400 });

const buildDoIdName = (parsed: ParsedDoId): string =>
  `${parsed.source}:${parsed.targetYmd}:${parsed.keibajoCode}`;

// raceKey shape: `${source}:${year}:${monthDay}:${keibajoCode}:${raceBango}`.
// The DO derives its own (source, targetYmd, keibajoCode) tuple from the
// pushed row's raceKey so push bodies stay flat (no context wrapper) and
// alarm reconciliation has the tuple available even on a cold hydration.
export const parseDoContextFromRaceKey = (raceKey: string): ParsedDoId | null => {
  const segments = raceKey.split(":");
  if (segments.length !== 5) return null;
  const [source, year, monthDay, keibajoCode] = segments;
  if (source !== "jra" && source !== "nar") return null;
  if (!year || !monthDay || !keibajoCode) return null;
  return { keibajoCode, source, targetYmd: `${year}${monthDay}` };
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isRaceTrendSource = (value: unknown): value is RaceTrendDailyTrackSource =>
  value === "jra" || value === "nar";

const isRaceTrendDailyTrackRow = (value: unknown): value is RaceTrendDailyTrackRow => {
  if (!isRecord(value)) return false;
  return (
    typeof value.raceBango === "string" &&
    typeof value.raceKey === "string" &&
    typeof value.isComplete === "boolean" &&
    typeof value.fetchedAt === "string" &&
    Array.isArray(value.starterRows) &&
    Array.isArray(value.runningStyles)
  );
};

const formatHassoJikoku = (raceStartAtJst: string | null): string | null => {
  if (typeof raceStartAtJst !== "string" || raceStartAtJst.length < 16) return null;
  return `${raceStartAtJst.slice(11, 13)}${raceStartAtJst.slice(14, 16)}`;
};

const toFinishPosition = (raw: string): number => {
  const cleaned = raw.replace(/\s+/gu, "");
  const parsed = Number.parseInt(cleaned, 10);
  return Number.isFinite(parsed) ? parsed : 0;
};

const isFinishPositionRanked = (raw: string): boolean => toFinishPosition(raw) > 0;

const toBataiju = (weight: number | null): string | null =>
  weight === null ? null : String(weight);

const toZogenSa = (change: number | null): string | null =>
  change === null ? null : String(change);

const isRunningStyleLabel = (value: string): value is "nige" | "senkou" | "sashi" | "oikomi" =>
  RUNNING_STYLE_LABELS.has(value);

const isRawRunningStyleRow = (value: unknown): value is RawRunningStyleRow => {
  if (!isRecord(value)) return false;
  return (
    typeof value.raceKey === "string" &&
    typeof value.horseNumber === "number" &&
    typeof value.predictedLabel === "string"
  );
};

const isRawSnapshotRow = (value: unknown): value is RawSnapshotRow => {
  if (!isRecord(value)) return false;
  return (
    isRaceTrendSource(value.source) &&
    typeof value.raceKey === "string" &&
    typeof value.raceBango === "string" &&
    typeof value.kaisaiNen === "string" &&
    typeof value.kaisaiTsukihi === "string" &&
    typeof value.keibajoCode === "string" &&
    typeof value.umaban === "string" &&
    typeof value.finishPosition === "string" &&
    typeof value.fetchedAt === "string"
  );
};

const toStarterRow = (raw: RawSnapshotRow) => ({
  bamei: raw.horseName,
  bataiju: toBataiju(raw.weight),
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  finishPosition: toFinishPosition(raw.finishPosition),
  hassoJikoku: formatHassoJikoku(raw.hassoJikoku),
  jockeyName: raw.jockeyName,
  kaisaiNen: raw.kaisaiNen,
  kaisaiTsukihi: raw.kaisaiTsukihi,
  keibajoCode: raw.keibajoCode,
  raceBango: raw.raceBango,
  raceName: raw.raceName,
  runnerCount: null,
  sohaTime: raw.sohaTime,
  source: raw.source,
  tanshoOdds: null,
  tanshoPopularity: null,
  umaban: raw.umaban,
  wakuban: null,
  zogenFugo: raw.changeSign,
  zogenSa: toZogenSa(raw.changeAmount),
});

interface BuildRowGroupInput {
  raceBango: string;
  raceKey: string;
  rows: RawSnapshotRow[];
  runningStyles: RawRunningStyleRow[];
}

const isRowComplete = (rows: RawSnapshotRow[]): boolean => {
  const first = rows[0];
  if (!first || first.resultCompleteAt === null) return false;
  const expected = first.expectedHorseCount;
  const saved = first.savedHorseCount;
  if (expected === null || saved === null) return false;
  const rankedCount = rows.filter((row) => isFinishPositionRanked(row.finishPosition)).length;
  return expected > 0 && saved >= expected && rankedCount >= expected;
};

const buildRowFromGroup = (input: BuildRowGroupInput): RaceTrendDailyTrackRow => {
  const first = input.rows[0]!;
  return {
    fetchedAt: first.fetchedAt,
    finishedAt: first.resultCompleteAt,
    isComplete: isRowComplete(input.rows),
    raceBango: input.raceBango,
    raceKey: input.raceKey,
    runningStyles: input.runningStyles
      .filter(
        (style) => style.raceKey === input.raceKey && isRunningStyleLabel(style.predictedLabel),
      )
      .map((style) => ({
        horseNumber: String(style.horseNumber),
        predictedLabel: isRunningStyleLabel(style.predictedLabel) ? style.predictedLabel : "sashi",
        raceKey: style.raceKey,
      })),
    starterRows: input.rows.map(toStarterRow),
  };
};

const groupRowsByRaceBango = (
  raws: RawSnapshotRow[],
): Map<string, { raceKey: string; rows: RawSnapshotRow[] }> => {
  const map = new Map<string, { raceKey: string; rows: RawSnapshotRow[] }>();
  raws.forEach((raw) => {
    const existing = map.get(raw.raceBango) ?? { raceKey: raw.raceKey, rows: [] };
    existing.rows.push(raw);
    map.set(raw.raceBango, existing);
  });
  return map;
};

const buildRowsFromSnapshotResults = (
  raws: RawSnapshotRow[],
  runningStyles: RawRunningStyleRow[],
): RaceTrendDailyTrackRow[] => {
  const grouped = groupRowsByRaceBango(raws);
  return Array.from(grouped.entries(), ([raceBango, group]) =>
    buildRowFromGroup({ raceBango, raceKey: group.raceKey, rows: group.rows, runningStyles }),
  );
};

interface QuerySnapshotsArgs {
  env: Env;
  parsed: ParsedDoId;
}

const querySnapshotRows = async ({
  env,
  parsed,
}: QuerySnapshotsArgs): Promise<RawSnapshotRow[]> => {
  const kaisaiNen = parsed.targetYmd.slice(0, 4);
  const kaisaiTsukihi = parsed.targetYmd.slice(4, 8);
  const result = await env.REALTIME_DB.prepare(SNAPSHOT_SELECT_SQL)
    .bind(parsed.source, kaisaiNen, kaisaiTsukihi, parsed.keibajoCode)
    .all<Record<string, unknown>>();
  return result.results.filter((row): row is RawSnapshotRow => isRawSnapshotRow(row));
};

const queryRunningStyleRows = async ({
  env,
  parsed,
}: QuerySnapshotsArgs): Promise<RawRunningStyleRow[]> => {
  const kaisaiNen = parsed.targetYmd.slice(0, 4);
  const kaisaiTsukihi = parsed.targetYmd.slice(4, 8);
  const result = await env.REALTIME_DB.prepare(RUNNING_STYLE_SELECT_SQL)
    .bind(parsed.source, kaisaiNen, kaisaiTsukihi, parsed.keibajoCode)
    .all<Record<string, unknown>>();
  return result.results.filter((row): row is RawRunningStyleRow => isRawRunningStyleRow(row));
};

const isInPollingWindow = (now: Date): boolean => {
  const hour = Number(
    new Intl.DateTimeFormat("ja-JP-u-ca-gregory", {
      hour: "2-digit",
      hour12: false,
      timeZone: "Asia/Tokyo",
    })
      .formatToParts(now)
      .find((part) => part.type === "hour")?.value ?? "",
  );
  if (!Number.isFinite(hour)) return false;
  return hour >= ALARM_WINDOW_START_HOUR && hour <= ALARM_WINDOW_END_HOUR;
};

export const computeNextAlarmDelayMs = (now: Date): number =>
  isInPollingWindow(now) ? ALARM_IN_WINDOW_MS : ALARM_OUT_OF_WINDOW_MS;

interface MergeArgs {
  existing: RaceTrendDailyTrackState | null;
  incoming: RaceTrendDailyTrackRow;
  parsed: ParsedDoId;
  updatedAt: string;
}

const mergeIncomingRow = ({
  existing,
  incoming,
  parsed,
  updatedAt,
}: MergeArgs): RaceTrendDailyTrackState => {
  const baseRaces = existing ? { ...existing.races } : {};
  const currentRow = baseRaces[incoming.raceBango];
  const shouldOverwrite =
    !currentRow ||
    new Date(incoming.fetchedAt).getTime() >= new Date(currentRow.fetchedAt).getTime();
  const nextRaces = shouldOverwrite ? { ...baseRaces, [incoming.raceBango]: incoming } : baseRaces;
  return {
    keibajoCode: parsed.keibajoCode,
    races: nextRaces,
    source: parsed.source,
    targetYmd: parsed.targetYmd,
    updatedAt,
  };
};

const buildAlarmReplacementState = (
  rows: RaceTrendDailyTrackRow[],
  existing: RaceTrendDailyTrackState | null,
  parsed: ParsedDoId,
  updatedAt: string,
): RaceTrendDailyTrackState =>
  rows.reduce<RaceTrendDailyTrackState>(
    (acc, row) => mergeIncomingRow({ existing: acc, incoming: row, parsed, updatedAt }),
    existing ?? {
      keibajoCode: parsed.keibajoCode,
      races: {},
      source: parsed.source,
      targetYmd: parsed.targetYmd,
      updatedAt,
    },
  );

const compareRaceBango = (a: string, b: string): number =>
  Number(a) - Number(b) === 0 ? a.localeCompare(b) : Number(a) - Number(b);

const sortRowsByRaceBango = (rows: RaceTrendDailyTrackRow[]): RaceTrendDailyTrackRow[] =>
  rows.toSorted((a, b) => compareRaceBango(a.raceBango, b.raceBango));

interface SelectRacesArgs {
  beforeRaceBango: string | null;
  state: RaceTrendDailyTrackState | null;
}

const selectRaces = ({ beforeRaceBango, state }: SelectRacesArgs): RaceTrendDailyTrackRow[] => {
  if (!state) return [];
  const all = Object.values(state.races);
  const filtered =
    beforeRaceBango === null
      ? all
      : all.filter((row) => Number(row.raceBango) < Number(beforeRaceBango));
  return sortRowsByRaceBango(filtered);
};

export class RaceTrendDailyTrackDO {
  private state: RaceTrendDailyTrackState | null = null;
  private readonly env: Env;
  private readonly doState: RaceTrendStateLike;
  // The DurableObjectId.name is unavailable on the raw `state.id` handle in
  // Cloudflare's runtime when the DO was created via `idFromName`. Callers
  // pass the id name implicitly by routing requests through the right stub,
  // so the DO learns its own (source, targetYmd, keibajoCode) from the first
  // `/push` body or from a previously-persisted snapshot.
  private parsed: ParsedDoId | null = null;

  constructor(state: DurableObjectState, env: Env) {
    this.env = env;
    const doStateLike: RaceTrendStateLike = {
      blockConcurrencyWhile: (callback) => state.blockConcurrencyWhile(callback),
      storage: {
        get: (key) => state.storage.get<RaceTrendDailyTrackState>(key),
        put: (key, value) => state.storage.put<RaceTrendDailyTrackState>(key, value),
        setAlarm: (at) => state.storage.setAlarm(at),
      },
    };
    this.doState = doStateLike;
    void doStateLike.blockConcurrencyWhile(async () => {
      const stored = await doStateLike.storage.get(STORAGE_KEY);
      if (stored !== undefined) {
        this.state = stored;
        this.parsed = {
          keibajoCode: stored.keibajoCode,
          source: stored.source,
          targetYmd: stored.targetYmd,
        };
      }
    });
  }

  // Test factory: mirrors the real constructor hydration path against a
  // typed fake state and env so we can exercise /push / /races / alarm
  // without forging a full DurableObjectState. Awaits hydration so callers
  // can immediately assert on the loaded snapshot.
  static async createForTest(params: CreateForTestParams): Promise<RaceTrendDailyTrackDO> {
    const instance: RaceTrendDailyTrackDO = Object.create(RaceTrendDailyTrackDO.prototype);
    Reflect.set(instance, "env", params.env);
    Reflect.set(instance, "doState", params.state);
    Reflect.set(instance, "state", null);
    Reflect.set(instance, "parsed", null);
    await params.state.blockConcurrencyWhile(async () => {
      const stored = await params.state.storage.get(STORAGE_KEY);
      if (stored !== undefined) {
        Reflect.set(instance, "state", stored);
        Reflect.set(instance, "parsed", {
          keibajoCode: stored.keibajoCode,
          source: stored.source,
          targetYmd: stored.targetYmd,
        });
      }
    });
    return instance;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "POST" && url.pathname === "/push") return this.handlePush(request);
    if (request.method === "GET" && url.pathname === "/races") return this.handleGet(url);
    if (request.method === "POST" && url.pathname === "/sync") return this.handleSync();
    return notFoundResponse();
  }

  async alarm(): Promise<void> {
    await this.runAlarmTick({ env: this.env, now: new Date() });
  }

  // Exposed for tests so the alarm body can be invoked with an injected
  // clock without monkey-patching Date.
  async runAlarmTick(context: AlarmContext): Promise<void> {
    if (this.parsed) {
      await this.refreshFromD1({ env: context.env, parsed: this.parsed });
    }
    await this.doState.storage.setAlarm(
      context.now.getTime() + computeNextAlarmDelayMs(context.now),
    );
  }

  private async handlePush(request: Request): Promise<Response> {
    const body: unknown = await request.json();
    if (!isRaceTrendDailyTrackRow(body)) return badRequestResponse("invalid body");
    const context = parseDoContextFromRaceKey(body.raceKey);
    if (!context) return badRequestResponse("invalid raceKey");
    const updatedAt = new Date().toISOString();
    const next = mergeIncomingRow({
      existing: this.state,
      incoming: body,
      parsed: context,
      updatedAt,
    });
    this.state = next;
    this.parsed = context;
    await this.doState.storage.put(STORAGE_KEY, next);
    return json({ ok: true });
  }

  private handleGet(url: URL): Response {
    const beforeRaceBango = url.searchParams.get("beforeRaceBango");
    const rows = selectRaces({ beforeRaceBango, state: this.state });
    const payload: RaceTrendDailyTrackResponse = { races: rows };
    return json(payload, {
      headers: {
        "X-Race-Trend-DO": rows.length === 0 ? "miss" : "hit",
      },
    });
  }

  private async handleSync(): Promise<Response> {
    if (!this.parsed) return badRequestResponse("DO id not yet learned");
    await this.refreshFromD1({ env: this.env, parsed: this.parsed });
    return json({ ok: true });
  }

  private async refreshFromD1(args: QuerySnapshotsArgs): Promise<void> {
    const [snapshotRows, runningStyleRows] = await Promise.all([
      querySnapshotRows(args),
      queryRunningStyleRows(args),
    ]);
    const rows = buildRowsFromSnapshotResults(snapshotRows, runningStyleRows);
    const updatedAt = new Date().toISOString();
    const merged = buildAlarmReplacementState(rows, this.state, args.parsed, updatedAt);
    this.state = merged;
    await this.doState.storage.put(STORAGE_KEY, merged);
  }
}

interface PushArgs {
  row: RaceTrendDailyTrackRow;
  stub: { fetch: (input: string, init?: RequestInit) => Promise<Response> };
}

export const pushRaceTrendDailyTrackRowToStub = async (args: PushArgs): Promise<Response> =>
  args.stub.fetch(PUSH_URL, {
    body: JSON.stringify(args.row),
    method: "POST",
  });

interface FetchRacesArgs {
  beforeRaceBango: string;
  stub: { fetch: (input: string, init?: RequestInit) => Promise<Response> };
}

export const fetchRaceTrendDailyTrackRacesFromStub = async (
  args: FetchRacesArgs,
): Promise<Response> => {
  const url = `${RACES_URL}?beforeRaceBango=${encodeURIComponent(args.beforeRaceBango)}`;
  return args.stub.fetch(url, { method: "GET" });
};

interface BuildDoNameArgs {
  keibajoCode: string;
  source: RaceTrendDailyTrackSource;
  targetYmd: string;
}

export const buildRaceTrendDailyTrackDoIdName = (args: BuildDoNameArgs): string =>
  buildDoIdName(args);

// State helpers re-exported for tests so the merge logic can be unit-tested
// without going through the DO HTTP layer.
export const __testables = {
  buildRowsFromSnapshotResults,
  computeNextAlarmDelayMs,
  isRawSnapshotRow,
  mergeIncomingRow,
  selectRaces,
};

export const RACE_TREND_DAILY_TRACK_STORAGE_KEY = STORAGE_KEY;
export const RACE_TREND_DAILY_TRACK_PUSH_URL = PUSH_URL;
export const RACE_TREND_DAILY_TRACK_RACES_URL = RACES_URL;
