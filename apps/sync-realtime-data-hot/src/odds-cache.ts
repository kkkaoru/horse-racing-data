import type { DurableObjectState } from "@cloudflare/workers-types";
import { jsonResponse } from "./http";
import type { Env, OddsData, OddsHistoryPoint, OddsTrendPoint, OddsType } from "./types";

const DEFAULT_TTL_SECONDS = 7200;
const MIN_TTL_SECONDS = 60;
const ALARM_BUFFER_MS = 60_000;
const MS_PER_SECOND = 1000;

interface CachedOddsPayload {
  expiresAt: number;
  fetchedAt: string;
  history: OddsHistoryPoint[];
  historyByType?: Partial<Record<OddsType, OddsTrendPoint[]>>;
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
      const body = (await request.json()) as Omit<CachedOddsPayload, "expiresAt">;
      const ttlSeconds = resolveTtlSeconds(this.env);
      const payload: CachedOddsPayload = {
        ...body,
        expiresAt: Date.now() + ttlSeconds * MS_PER_SECOND,
      };
      await this.state.storage.put(raceKey, payload);
      await this.state.storage.setAlarm(payload.expiresAt + ALARM_BUFFER_MS);
      return jsonResponse({ ok: true });
    }
    if (request.method === "GET") {
      const payload = await this.state.storage.get<CachedOddsPayload>(raceKey);
      if (!payload || payload.expiresAt <= Date.now()) {
        return jsonResponse(null, { status: 404 });
      }
      return jsonResponse(payload);
    }
    return jsonResponse({ error: "method not allowed" }, { status: 405 });
  }

  async alarm(): Promise<void> {
    const entries = await this.state.storage.list<CachedOddsPayload>();
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
): Promise<Omit<CachedOddsPayload, "expiresAt"> | null> => {
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
  payload: Omit<CachedOddsPayload, "expiresAt">,
): Promise<void> => {
  const stub = env.ODDS_CACHE.get(getOddsCacheId(env, raceKey));
  await stub.fetch(`https://odds-cache/races/${encodeURIComponent(raceKey)}`, {
    body: JSON.stringify(payload),
    method: "PUT",
  });
};
