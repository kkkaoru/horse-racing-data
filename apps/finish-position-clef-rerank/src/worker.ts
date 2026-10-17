// Run checks with bun run check; deployed as a Cloudflare Worker behind the viewer's Access host.
import { parseBudget, suggestRaceBets, type ServiceFetcher } from "./bets-route";
import {
  buildClefInput,
  fuseRanks,
  latestBase,
  parseBasePredictions,
  parseBundle,
  parseClefAnswers,
  toBaseRanks,
  type RerankBundle,
  type RerankResult,
} from "./fuse";

export interface KvStore {
  get: (key: string) => Promise<string | null>;
  put: (key: string, value: string, options: { expirationTtl: number }) => Promise<void>;
}

export interface AiBinding {
  run: (model: string, input: Record<string, unknown>) => Promise<unknown>;
}

export interface Env {
  AI: AiBinding;
  DETAIL_SECTION_CACHE_KV: KvStore;
  VIEWER: ServiceFetcher;
}

export interface RaceAddress {
  ymd: string;
  keibajoCode: string;
  raceBango: string;
}

export interface ScheduledEvent {
  scheduledTime: number;
}

interface Outcome {
  status: number;
  body: unknown;
}

interface LoadedRace {
  bundle: RerankBundle;
  base: NonNullable<ReturnType<typeof parseBasePredictions>>;
}

const CLEF_MODEL: string = "@cf/cloudflare/clef-flash";
const ROUTE_PATTERN: RegExp = /^\/api\/clef-rerank\/(\d{8})\/([0-9A-Z]{2})\/(\d{2})$/u;
const PROBE_PATH: string = "/api/clef-rerank/probe";
const BET_ROUTE_PATTERN: RegExp = /^\/api\/clef-rerank\/bets\/(\d{8})\/([0-9A-Z]{2})\/(\d{2})$/u;
const INDEX_ROUTE_PATTERN: RegExp = /^\/api\/clef-rerank\/(\d{8})$/u;
const RESULT_TTL_SECONDS: number = 259_200;
const CRON_CONCURRENCY: number = 4;
const JST_OFFSET_MS: number = 32_400_000;
const RESPONSE_HEADERS: Record<string, string> = { "Cache-Control": "no-store" };
const NOT_FOUND: Outcome = { status: 404, body: { error: "clef rerank is not available" } };

const raceSuffix = ({ ymd, keibajoCode, raceBango }: RaceAddress): string =>
  `${ymd}:${keibajoCode}:${raceBango}`;

export const bundleKey = (race: RaceAddress): string => `clefrr:v1:bundle:${raceSuffix(race)}`;
export const resultKey = (race: RaceAddress): string => `clefrr:v1:result:${raceSuffix(race)}`;
export const indexKey = (ymd: string): string => `clefrr:v1:index:${ymd}`;
const baseKey = (race: RaceAddress): string => `pred:fp:v1:${raceSuffix(race)}`;

const parseJson = (text: string | null): unknown => {
  try {
    return text === null ? null : JSON.parse(text);
  } catch {
    return null;
  }
};

const loadRace = async (env: Env, race: RaceAddress): Promise<LoadedRace | null> => {
  const [bundleText, baseText] = await Promise.all([
    env.DETAIL_SECTION_CACHE_KV.get(bundleKey(race)),
    env.DETAIL_SECTION_CACHE_KV.get(baseKey(race)),
  ]);
  const bundle = parseBundle(parseJson(bundleText));
  const base = parseBasePredictions(parseJson(baseText));
  return bundle === null ||
    base === null ||
    !bundle.baseModels.includes(latestBase(base).modelVersion)
    ? null
    : { bundle, base };
};

const isFresh = (cached: unknown, loaded: LoadedRace): cached is RerankResult =>
  typeof cached === "object" &&
  cached !== null &&
  "baseGeneratedAt" in cached &&
  "modelVersion" in cached &&
  cached.baseGeneratedAt === latestBase(loaded.base).predictionGeneratedAt &&
  cached.modelVersion === loaded.bundle.modelVersion;

const scoreRace = async (env: Env, loaded: LoadedRace): Promise<RerankResult | null> => {
  const baseRanks = toBaseRanks(loaded.base);
  const input = buildClefInput(loaded.bundle, baseRanks);
  const answers = parseClefAnswers(
    await env.AI.run(CLEF_MODEL, input),
    baseRanks.map((row) => row.horseNumber),
  );
  return answers === null
    ? null
    : {
        modelVersion: loaded.bundle.modelVersion,
        label: loaded.bundle.label,
        raceKey: loaded.bundle.raceKey,
        baseModelVersion: latestBase(loaded.base).modelVersion,
        baseGeneratedAt: latestBase(loaded.base).predictionGeneratedAt,
        generatedAt: new Date().toISOString(),
        predictions: fuseRanks({ bundle: loaded.bundle, baseRanks, answers }),
      };
};

export const rerankRace = async (env: Env, race: RaceAddress): Promise<Outcome> => {
  const loaded = await loadRace(env, race);
  if (loaded === null) return NOT_FOUND;
  const cached = parseJson(await env.DETAIL_SECTION_CACHE_KV.get(resultKey(race)));
  if (isFresh(cached, loaded)) return { status: 200, body: cached };
  const result = await scoreRace(env, loaded);
  if (result === null) return { status: 502, body: { error: "clef-flash answer was malformed" } };
  await env.DETAIL_SECTION_CACHE_KV.put(resultKey(race), JSON.stringify(result), {
    expirationTtl: RESULT_TTL_SECONDS,
  });
  return { status: 200, body: result };
};

const safeRerank = async (env: Env, race: RaceAddress): Promise<Outcome> => {
  try {
    return await rerankRace(env, race);
  } catch (error) {
    console.error("clef rerank failed", raceSuffix(race), String(error));
    return { status: 503, body: { error: "clef rerank failed" } };
  }
};

const readIndex = async (env: Env, ymd: string): Promise<RaceAddress[]> => {
  const value = parseJson(await env.DETAIL_SECTION_CACHE_KV.get(indexKey(ymd)));
  return Array.isArray(value)
    ? value
        .filter(
          (item): item is string => typeof item === "string" && /^[0-9A-Z]{2}:\d{2}$/u.test(item),
        )
        .map((item) => ({ ymd, keibajoCode: item.slice(0, 2), raceBango: item.slice(3) }))
    : [];
};

const chunk = <T>(items: readonly T[], size: number): T[][] =>
  Array.from({ length: Math.ceil(items.length / size) }, (_, index) =>
    items.slice(index * size, index * size + size),
  );

export const warmDay = async (env: Env, ymd: string): Promise<number[]> => {
  const races = await readIndex(env, ymd);
  const statuses: number[] = [];
  await chunk(races, CRON_CONCURRENCY).reduce(async (previous, group) => {
    await previous;
    const outcomes = await Promise.all(group.map((race) => safeRerank(env, race)));
    statuses.push(...outcomes.map((outcome) => outcome.status));
  }, Promise.resolve());
  return statuses;
};

export const jstYmd = (nowMs: number): string =>
  new Date(nowMs + JST_OFFSET_MS).toISOString().slice(0, 10).replaceAll("-", "");

// Access-protected parity probe: forwards one System One request to hosted clef-flash.
const probe = async (env: Env, request: Request): Promise<Outcome> => {
  const body = parseJson(await request.text());
  return typeof body === "object" && body !== null && !Array.isArray(body)
    ? { status: 200, body: await env.AI.run(CLEF_MODEL, Object.fromEntries(Object.entries(body))) }
    : { status: 400, body: { error: "probe body must be a JSON object" } };
};

const safeBets = async (env: Env, url: URL, match: RegExpExecArray): Promise<Outcome> => {
  const budget = parseBudget(url.searchParams.get("budget"));
  if (budget === null)
    return { status: 400, body: { error: "budget must be a multiple of 100 from 100 to 1000000" } };
  try {
    return await suggestRaceBets(env, {
      ymd: String(match[1]),
      keibajoCode: String(match[2]),
      raceBango: String(match[3]),
      budget,
    });
  } catch (error) {
    console.error("clef bets failed", url.pathname, String(error));
    return { status: 503, body: { error: "clef bet suggestion failed" } };
  }
};

const route = async (env: Env, url: URL): Promise<Outcome> => {
  const pathname = url.pathname;
  const bets = BET_ROUTE_PATTERN.exec(pathname);
  if (bets !== null) return safeBets(env, url, bets);
  const race = ROUTE_PATTERN.exec(pathname);
  if (race !== null)
    return safeRerank(env, {
      ymd: String(race[1]),
      keibajoCode: String(race[2]),
      raceBango: String(race[3]),
    });
  const day = INDEX_ROUTE_PATTERN.exec(pathname);
  return day === null ? NOT_FOUND : { status: 200, body: await readIndex(env, String(day[1])) };
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const outcome =
      request.method === "POST" && url.pathname === PROBE_PATH
        ? await probe(env, request)
        : await route(env, url);
    return Response.json(outcome.body, { status: outcome.status, headers: RESPONSE_HEADERS });
  },
  async scheduled(controller: ScheduledEvent, env: Env): Promise<void> {
    const statuses = await warmDay(env, jstYmd(controller.scheduledTime));
    console.log("clef rerank warm", JSON.stringify(statuses));
  },
};
