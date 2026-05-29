// Run with bun.
import type { DurableObjectState } from "@cloudflare/workers-types";
import { jsonResponse } from "./http";
import { toHorseTrends, toOddsTrendsByType } from "./storage";
import type {
  Env,
  HorseOddsTrend,
  OddsData,
  OddsHistoryPoint,
  OddsTrend,
  OddsTrendPoint,
  OddsType,
} from "./types";

const DEFAULT_TTL_SECONDS = 7200;
const MIN_TTL_SECONDS = 60;
const ALARM_BUFFER_MS = 60_000;
const MS_PER_SECOND = 1000;
const MAX_POINTS_PER_RACE = 60;

interface StoredOddsState {
  expiresAt: number;
  fetchedAt: string | null;
  historyByType: Partial<Record<OddsType, OddsTrendPoint[]>>;
  latest: Partial<Record<OddsType, OddsData[]>>;
}

interface PutOddsBody {
  fetchedAt: string;
  latest: Partial<Record<OddsType, OddsData[]>>;
}

export interface OddsCachePayload {
  fetchedAt: string | null;
  history: HorseOddsTrend[];
  historyByType: Partial<Record<OddsType, OddsTrend[]>>;
  latest: Partial<Record<OddsType, OddsData[]>>;
}

const resolveTtlSeconds = (env: Env): number => {
  const raw = env.ODDS_DO_TTL_SECONDS;
  if (!raw) {
    return DEFAULT_TTL_SECONDS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0
    ? Math.max(parsed, MIN_TTL_SECONDS)
    : DEFAULT_TTL_SECONDS;
};

const createEmptyState = (): Omit<StoredOddsState, "expiresAt"> => ({
  fetchedAt: null,
  historyByType: {},
  latest: {},
});

const oddsDataToTrendPoint = (fetchedAt: string, row: OddsData): OddsTrendPoint => ({
  combination: row.combination,
  fetchedAt,
  odds: row.odds ?? null,
  rank: row.rank ?? null,
});

const appendPointsForType = (
  existing: OddsTrendPoint[],
  next: OddsTrendPoint[],
): OddsTrendPoint[] => {
  const merged = [...existing, ...next];
  return merged.length > MAX_POINTS_PER_RACE
    ? merged.slice(merged.length - MAX_POINTS_PER_RACE)
    : merged;
};

const mergeHistory = (
  current: Partial<Record<OddsType, OddsTrendPoint[]>>,
  fetchedAt: string,
  latest: Partial<Record<OddsType, OddsData[]>>,
): Partial<Record<OddsType, OddsTrendPoint[]>> => {
  const next: Partial<Record<OddsType, OddsTrendPoint[]>> = { ...current };
  const entries = Object.entries(latest) as [OddsType, OddsData[] | undefined][];
  entries.forEach(([oddsType, rows]) => {
    if (!rows || rows.length === 0) {
      return;
    }
    const points = rows.map((row) => oddsDataToTrendPoint(fetchedAt, row));
    next[oddsType] = appendPointsForType(current[oddsType] ?? [], points);
  });
  return next;
};

const tanshoPointsToHistory = (points: OddsTrendPoint[]): OddsHistoryPoint[] =>
  points.map((point) => ({
    fetchedAt: point.fetchedAt,
    horseNumber: point.combination,
    odds: point.odds,
    popularity: point.rank,
  }));

const buildPayload = (state: Omit<StoredOddsState, "expiresAt">): OddsCachePayload => ({
  fetchedAt: state.fetchedAt,
  history: toHorseTrends(tanshoPointsToHistory(state.historyByType.tansho ?? [])),
  historyByType: toOddsTrendsByType(state.historyByType),
  latest: state.latest,
});

export class OddsCacheHot {
  constructor(
    private readonly state: DurableObjectState,
    private readonly env: Env,
  ) {}

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const raceKey = decodeURIComponent(url.pathname.split("/").filter(Boolean).at(-1) ?? "");
    if (!raceKey) {
      return jsonResponse({ error: "raceKey is required" }, { status: 400 });
    }
    if (request.method === "PUT") {
      return this.handlePut(raceKey, request);
    }
    if (request.method === "GET") {
      return this.handleGet(raceKey);
    }
    return jsonResponse({ error: "method not allowed" }, { status: 405 });
  }

  private async handlePut(raceKey: string, request: Request): Promise<Response> {
    const body = (await request.json()) as PutOddsBody;
    const stored = (await this.state.storage.get<StoredOddsState>(raceKey)) ?? null;
    const base = stored ? { ...stored } : { ...createEmptyState(), expiresAt: 0 };
    if (stored && stored.fetchedAt === body.fetchedAt) {
      return jsonResponse({ ok: true, skipped: true });
    }
    const ttlSeconds = resolveTtlSeconds(this.env);
    const nextState: StoredOddsState = {
      expiresAt: Date.now() + ttlSeconds * MS_PER_SECOND,
      fetchedAt: body.fetchedAt,
      historyByType: mergeHistory(base.historyByType, body.fetchedAt, body.latest),
      latest: body.latest,
    };
    await this.state.storage.put(raceKey, nextState);
    await this.state.storage.setAlarm(nextState.expiresAt + ALARM_BUFFER_MS);
    return jsonResponse({ ok: true });
  }

  private async handleGet(raceKey: string): Promise<Response> {
    const stored = await this.state.storage.get<StoredOddsState>(raceKey);
    if (!stored || stored.expiresAt <= Date.now()) {
      return jsonResponse(null, { status: 404 });
    }
    return jsonResponse(buildPayload(stored));
  }

  async alarm(): Promise<void> {
    const entries = await this.state.storage.list<StoredOddsState>();
    const now = Date.now();
    const survivingExpiresAt: number[] = [];
    for (const [key, payload] of entries) {
      if (payload.expiresAt <= now) {
        await this.state.storage.delete(key);
        continue;
      }
      survivingExpiresAt.push(payload.expiresAt);
    }
    if (survivingExpiresAt.length === 0) {
      return;
    }
    const nextAlarm = Math.min(...survivingExpiresAt);
    await this.state.storage.setAlarm(nextAlarm + ALARM_BUFFER_MS);
  }
}

export const getOddsCacheId = (env: Env, raceKey: string): DurableObjectId =>
  env.ODDS_CACHE.idFromName(raceKey);

export const readCachedOdds = async (
  env: Env,
  raceKey: string,
): Promise<OddsCachePayload | null> => {
  const stub = env.ODDS_CACHE.get(getOddsCacheId(env, raceKey));
  const response = await stub.fetch(`https://odds-cache/races/${encodeURIComponent(raceKey)}`);
  if (!response.ok) {
    return null;
  }
  return response.json();
};

export const writeCachedOdds = async (
  env: Env,
  raceKey: string,
  payload: PutOddsBody,
): Promise<void> => {
  const stub = env.ODDS_CACHE.get(getOddsCacheId(env, raceKey));
  await stub.fetch(`https://odds-cache/races/${encodeURIComponent(raceKey)}`, {
    body: JSON.stringify(payload),
    method: "PUT",
  });
};
