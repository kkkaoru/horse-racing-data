import type { Env } from "./types";

const DEFAULT_TTL_SECONDS = 20 * 60;

export class PremiumPaddockCache {
  constructor(
    private readonly state: DurableObjectState,
    private readonly env: Env,
  ) {}

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "GET") {
      const payload = await this.state.storage.get<unknown>("payload");
      const cachedAt = await this.state.storage.get<number>("cachedAt");
      const ttlSeconds = Number(this.env.PREMIUM_PADDOCK_DO_TTL_SECONDS ?? DEFAULT_TTL_SECONDS);
      if (
        payload === undefined ||
        cachedAt === undefined ||
        Date.now() - cachedAt > ttlSeconds * 1000
      ) {
        return new Response(null, { status: 404 });
      }
      return Response.json(payload);
    }
    if (request.method === "PUT") {
      const payload = await request.json();
      await this.state.storage.put("payload", payload);
      await this.state.storage.put("cachedAt", Date.now());
      return Response.json({ ok: true });
    }
    if (url.pathname === "/clear" && request.method === "POST") {
      await this.state.storage.deleteAll();
      return Response.json({ ok: true });
    }
    return new Response(null, { status: 405 });
  }
}

const getStub = (env: Env, raceKey: string): DurableObjectStub => {
  const id = env.PREMIUM_PADDOCK_CACHE.idFromName(raceKey);
  return env.PREMIUM_PADDOCK_CACHE.get(id);
};

export const readCachedPremiumPaddock = async (env: Env, raceKey: string): Promise<unknown> => {
  const response = await getStub(env, raceKey).fetch("https://cache.local/");
  if (!response.ok) {
    return null;
  }
  return response.json();
};

export const writeCachedPremiumPaddock = async (
  env: Env,
  raceKey: string,
  payload: unknown,
): Promise<void> => {
  await getStub(env, raceKey).fetch("https://cache.local/", {
    body: JSON.stringify(payload),
    method: "PUT",
  });
};

export const clearCachedPremiumPaddock = async (env: Env, raceKey: string): Promise<void> => {
  await getStub(env, raceKey).fetch("https://cache.local/clear", { method: "POST" });
};
