// Run with bun (bunx vitest). GET /api/clef-rerank/bets/{ymd}/{kj}/{rb}?budget= handler: gathers the
// viewer's race data over a service binding, asks clef-flash once per odds snapshot, then splits
// the requested budget into tickets.
import {
  buildBetRequest,
  parseBetAnswers,
  suggestBets,
  type BetAnswers,
  type BetSuggestion,
  type RunnerProfile,
} from "./bets";
import { parseBasePredictions, toBaseRanks } from "./fuse";

export interface ServiceFetcher {
  fetch: (request: Request) => Promise<Response>;
}

export interface BetEnv {
  AI: { run: (model: string, input: Record<string, unknown>) => Promise<unknown> };
  DETAIL_SECTION_CACHE_KV: {
    get: (key: string) => Promise<string | null>;
    put: (key: string, value: string, options: { expirationTtl: number }) => Promise<void>;
  };
  VIEWER: ServiceFetcher;
}

export interface BetRaceAddress {
  ymd: string;
  keibajoCode: string;
  raceBango: string;
  budget: number;
}

export interface BetOutcome {
  status: number;
  body: unknown;
}

export interface BetRunner extends RunnerProfile {
  clefWin: number | null;
  clefTop3: number;
}

export interface BetResponse {
  raceKey: string;
  oddsFetchedAt: string;
  generatedAt: string;
  runners: BetRunner[];
  suggestion: BetSuggestion;
}

const CLEF_MODEL: string = "@cf/cloudflare/clef-flash";
const VIEWER_ORIGIN: string = "https://pc-keiba-viewer.kkk4oru.com";
const ANSWER_TTL_SECONDS: number = 259_200;
const DEFAULT_BUDGET: number = 2000;
const MIN_BUDGET: number = 100;
const MAX_BUDGET: number = 1_000_000;
const BUDGET_STEP: number = 100;
const MAX_JRA_VENUE: number = 10;
const SOURCE_PATHS: readonly string[] = [
  "sections/win-rate-heatmap",
  "realtime",
  "recent-results",
  "sections/training",
  "trends",
];

const raceSuffix = ({ ymd, keibajoCode, raceBango }: BetRaceAddress): string =>
  `${ymd}:${keibajoCode}:${raceBango}`;

const parseJson = (text: string | null): unknown => {
  try {
    return text === null ? null : JSON.parse(text);
  } catch {
    return null;
  }
};

// Budget must be a whole number of 100-yen units; an absent value means the UI default.
export const parseBudget = (value: string | null): number | null => {
  const budget = value === null ? DEFAULT_BUDGET : Number(value);
  return Number.isInteger(budget) &&
    budget >= MIN_BUDGET &&
    budget <= MAX_BUDGET &&
    budget % BUDGET_STEP === 0
    ? budget
    : null;
};

const sourceOf = (keibajoCode: string): string =>
  Number(keibajoCode) >= 1 && Number(keibajoCode) <= MAX_JRA_VENUE ? "jra" : "nar";

export const viewerPath = (race: BetRaceAddress, suffix: string): string =>
  `/api/races/${race.ymd.slice(0, 4)}/${race.ymd.slice(4, 6)}/${race.ymd.slice(6, 8)}/${race.keibajoCode}/${race.raceBango}/${suffix}?source=${sourceOf(race.keibajoCode)}`;

const fetchViewer = async (env: BetEnv, path: string): Promise<unknown> => {
  const response = await env.VIEWER.fetch(new Request(`${VIEWER_ORIGIN}${path}`));
  return response.ok ? response.json() : null;
};

const isRankRow = (value: unknown): value is { horseNumber: number; rank: number } =>
  typeof value === "object" &&
  value !== null &&
  "horseNumber" in value &&
  "rank" in value &&
  typeof value.horseNumber === "number" &&
  typeof value.rank === "number";

// Prefer the approved clef rerank order; otherwise use the served model's order.
const loadRanks = async (env: BetEnv, race: BetRaceAddress): Promise<Map<number, number>> => {
  const [rerankText, baseText] = await Promise.all([
    env.DETAIL_SECTION_CACHE_KV.get(`clefrr:v1:result:${raceSuffix(race)}`),
    env.DETAIL_SECTION_CACHE_KV.get(`pred:fp:v1:${raceSuffix(race)}`),
  ]);
  const rerank = parseJson(rerankText);
  const rows =
    typeof rerank === "object" &&
    rerank !== null &&
    "predictions" in rerank &&
    Array.isArray(rerank.predictions)
      ? rerank.predictions.filter(isRankRow)
      : toBaseRanks(parseBasePredictions(parseJson(baseText)) ?? []);
  return new Map(rows.map((row) => [row.horseNumber, row.rank]));
};

const answerKey = (race: BetRaceAddress, oddsFetchedAt: string): string =>
  `clefbet:v1:answers:${raceSuffix(race)}:${oddsFetchedAt}`;

const loadAnswers = async (
  env: BetEnv,
  key: string,
  request: Record<string, unknown>,
  horses: readonly number[],
): Promise<BetAnswers | null> => {
  const cached = parseBetAnswers(parseJson(await env.DETAIL_SECTION_CACHE_KV.get(key)), horses);
  if (cached !== null) return cached;
  const raw = await env.AI.run(CLEF_MODEL, request);
  const answers = parseBetAnswers(raw, horses);
  if (answers !== null)
    await env.DETAIL_SECTION_CACHE_KV.put(key, JSON.stringify(raw), {
      expirationTtl: ANSWER_TTL_SECONDS,
    });
  return answers;
};

export const suggestRaceBets = async (env: BetEnv, race: BetRaceAddress): Promise<BetOutcome> => {
  const [[heatmap, realtime, recent, training, trends], ranks] = await Promise.all([
    Promise.all(SOURCE_PATHS.map((suffix) => fetchViewer(env, viewerPath(race, suffix)))),
    loadRanks(env, race),
  ]);
  const built = buildBetRequest({ heatmap, realtime, recent, training, trends, ranks });
  if (built.runners.length === 0 || built.odds.tansho === undefined)
    return { status: 404, body: { error: "runners or odds are not available yet" } };
  const horses = built.runners.map((runner) => runner.horseNumber);
  const answers = await loadAnswers(
    env,
    answerKey(race, built.oddsFetchedAt),
    built.request,
    horses,
  );
  if (answers === null) return { status: 502, body: { error: "clef-flash answer was malformed" } };
  const suggestion = suggestBets({ race: built, answers, budget: race.budget });
  return suggestion === null
    ? { status: 404, body: { error: "no ticket has published odds" } }
    : {
        status: 200,
        body: {
          raceKey: raceSuffix(race),
          oddsFetchedAt: built.oddsFetchedAt,
          generatedAt: new Date().toISOString(),
          runners: built.runners.map((runner) => ({
            ...runner,
            clefWin: answers.winner[String(runner.horseNumber)] ?? null,
            clefTop3: Number(answers.top3[String(runner.horseNumber)]),
          })),
          suggestion,
        } satisfies BetResponse,
      };
};
