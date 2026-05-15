import type { DurableObjectState } from "@cloudflare/workers-types";
import { mergeJsonHeaders } from "./http";
import type { Env, OddsData, OddsHistoryPoint, OddsTrendPoint, OddsType } from "./types";

interface CachedOddsPayload {
  expiresAt: number;
  fetchedAt: string;
  history: OddsHistoryPoint[];
  historyByType?: Partial<Record<OddsType, OddsTrendPoint[]>>;
  latest: Partial<Record<OddsType, OddsData[]>>;
}

const json = (body: unknown, init?: ResponseInit): Response =>
  new Response(JSON.stringify(body), {
    headers: mergeJsonHeaders(init),
    status: init?.status ?? 200,
  });

export class OddsCache {
  constructor(
    private readonly state: DurableObjectState,
    private readonly env: Env,
  ) {}

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const raceKey = decodeURIComponent(url.pathname.split("/").filter(Boolean).at(-1) ?? "");
    if (!raceKey) {
      return json({ error: "raceKey is required" }, { status: 400 });
    }

    if (request.method === "PUT") {
      const body = (await request.json()) as Omit<CachedOddsPayload, "expiresAt">;
      const ttlSeconds = Number(this.env.ODDS_DO_TTL_SECONDS ?? "7200");
      const payload: CachedOddsPayload = {
        ...body,
        expiresAt: Date.now() + Math.max(ttlSeconds, 60) * 1000,
      };
      await this.state.storage.put(raceKey, payload);
      await this.state.storage.setAlarm(payload.expiresAt + 60_000);
      return json({ ok: true });
    }

    if (request.method === "GET") {
      const payload = await this.state.storage.get<CachedOddsPayload>(raceKey);
      if (!payload || payload.expiresAt <= Date.now()) {
        return json(null, { status: 404 });
      }
      return json(payload);
    }

    return json({ error: "method not allowed" }, { status: 405 });
  }

  async alarm(): Promise<void> {
    const entries = await this.state.storage.list<CachedOddsPayload>();
    const now = Date.now();
    let nextAlarm: number | null = null;
    for (const [key, payload] of entries) {
      if (payload.expiresAt <= now) {
        await this.state.storage.delete(key);
        continue;
      }
      nextAlarm = nextAlarm === null ? payload.expiresAt : Math.min(nextAlarm, payload.expiresAt);
    }
    if (nextAlarm !== null) {
      await this.state.storage.setAlarm(nextAlarm + 60_000);
    }
  }
}

export const getOddsCacheId = (env: Env, raceKey: string): DurableObjectId =>
  env.ODDS_CACHE.idFromName(raceKey);

export const readCachedOdds = async (
  env: Env,
  raceKey: string,
): Promise<{
  fetchedAt: string;
  history: OddsHistoryPoint[];
  historyByType?: Partial<Record<OddsType, OddsTrendPoint[]>>;
  latest: Partial<Record<OddsType, OddsData[]>>;
} | null> => {
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
  payload: {
    fetchedAt: string;
    history: OddsHistoryPoint[];
    historyByType?: Partial<Record<OddsType, OddsTrendPoint[]>>;
    latest: Partial<Record<OddsType, OddsData[]>>;
  },
): Promise<void> => {
  const stub = env.ODDS_CACHE.get(getOddsCacheId(env, raceKey));
  await stub.fetch(`https://odds-cache/races/${encodeURIComponent(raceKey)}`, {
    body: JSON.stringify(payload),
    method: "PUT",
  });
};
