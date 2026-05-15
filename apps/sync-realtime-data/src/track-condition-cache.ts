import type { DurableObjectState } from "@cloudflare/workers-types";
import { mergeJsonHeaders } from "./http";
import type { Env, TrackCondition } from "./types";

interface CachedTrackConditionPayload extends TrackCondition {
  expiresAt: number;
}

const json = (body: unknown, init?: ResponseInit): Response =>
  new Response(JSON.stringify(body), {
    headers: mergeJsonHeaders(init),
    status: init?.status ?? 200,
  });

export class TrackConditionCache {
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
      const body = (await request.json()) as TrackCondition;
      const ttlSeconds = Number(this.env.TRACK_CONDITION_DO_TTL_SECONDS ?? "86400");
      const payload: CachedTrackConditionPayload = {
        ...body,
        expiresAt: Date.now() + Math.max(ttlSeconds, 3600) * 1000,
      };
      await this.state.storage.put(raceKey, payload);
      await this.state.storage.setAlarm(payload.expiresAt + 60_000);
      return json({ ok: true });
    }

    if (request.method === "GET") {
      const payload = await this.state.storage.get<CachedTrackConditionPayload>(raceKey);
      if (!payload || payload.expiresAt <= Date.now()) {
        return json(null, { status: 404 });
      }
      const { expiresAt: _expiresAt, ...trackCondition } = payload;
      return json(trackCondition);
    }

    return json({ error: "method not allowed" }, { status: 405 });
  }

  async alarm(): Promise<void> {
    const entries = await this.state.storage.list<CachedTrackConditionPayload>();
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

export const getTrackConditionCacheId = (env: Env, raceKey: string): DurableObjectId =>
  env.TRACK_CONDITION_CACHE.idFromName(raceKey);

export const readCachedTrackCondition = async (
  env: Env,
  raceKey: string,
): Promise<TrackCondition | null> => {
  const stub = env.TRACK_CONDITION_CACHE.get(getTrackConditionCacheId(env, raceKey));
  const response = await stub.fetch(
    `https://track-condition-cache/races/${encodeURIComponent(raceKey)}`,
  );
  if (!response.ok) {
    return null;
  }
  return response.json();
};

export const writeCachedTrackCondition = async (
  env: Env,
  raceKey: string,
  payload: TrackCondition,
): Promise<void> => {
  const stub = env.TRACK_CONDITION_CACHE.get(getTrackConditionCacheId(env, raceKey));
  await stub.fetch(`https://track-condition-cache/races/${encodeURIComponent(raceKey)}`, {
    body: JSON.stringify(payload),
    method: "PUT",
  });
};
