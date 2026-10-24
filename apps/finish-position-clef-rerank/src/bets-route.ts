// Run with bun (bunx vitest). GET /api/clef-rerank/bets/{ymd}/{kj}/{rb}?budget= handler: gathers the
// viewer's race data over a service binding, asks clef-flash once per odds snapshot, then splits
// the requested budget into tickets.
import {
  buildBetRequest,
  parseBetAnswers,
  readOdds,
  suggestPlans,
  type BetAnswers,
  type BetPlanSet,
  type BetSuggestion,
  type BetType,
  type RunnerProfile,
  type SuggestInput,
} from "./bets";
import { latestBase, parseBasePredictions, toBaseRanks } from "./fuse";

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
  // Bet types the viewer selected; null means every type with odds.
  types: ReadonlySet<BetType> | null;
  // Profit in yen every winning ticket must clear over the budget; null means no target.
  target: number | null;
  // Every selected bet type with odds must appear in the suggestion.
  requireAll: boolean;
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
  baseGeneratedAt: string;
  oddsFetchedAt: string;
  generatedAt: string;
  runners: BetRunner[];
  // First plan only, kept for viewers deployed before multi-type plans.
  suggestion: BetSuggestion;
  plans: BetPlanSet;
  target: number | null;
}

const CLEF_MODEL: string = "@cf/cloudflare/clef-flash";
const VIEWER_ORIGIN: string = "https://pc-keiba-viewer.kkk4oru.com";
const ANSWER_TTL_SECONDS: number = 259_200;
const DEFAULT_BUDGET: number = 2000;
// Two bet types are always suggested, so the smallest budget is two 100-yen tickets.
const MIN_BUDGET: number = 200;
const MAX_BUDGET: number = 1_000_000;
const DEFAULT_TARGET: number = 10_000;
const MAX_TARGET: number = 10_000_000;
const BUDGET_STEP: number = 100;
const MAX_JRA_VENUE: number = 10;
const SIGNATURE_FAVORITES: number = 6;
const SOURCE_PATHS: readonly string[] = [
  "sections/win-rate-heatmap",
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

// Target profit is whole yen; an absent value means the UI default.
export const parseTarget = (value: string | null): number | null => {
  const target = value === null ? DEFAULT_TARGET : Number(value);
  return Number.isInteger(target) && target >= 0 && target <= MAX_TARGET ? target : null;
};

const sourceOf = (keibajoCode: string): string =>
  Number(keibajoCode) >= 1 && Number(keibajoCode) <= MAX_JRA_VENUE ? "jra" : "nar";

export const viewerPath = (race: BetRaceAddress, suffix: string): string =>
  `/api/races/${race.ymd.slice(0, 4)}/${race.ymd.slice(4, 6)}/${race.ymd.slice(6, 8)}/${race.keibajoCode}/${race.raceBango}/${suffix}?source=${sourceOf(race.keibajoCode)}`;

export const fetchViewer = async (env: BetEnv, path: string): Promise<unknown> => {
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

interface RaceRanks {
  ranks: Map<number, number>;
  baseGeneratedAt: string;
}

// Prefer the approved clef rerank order; otherwise use the served model's order.
const loadRanks = async (env: BetEnv, race: BetRaceAddress): Promise<RaceRanks> => {
  const [rerankText, baseText] = await Promise.all([
    env.DETAIL_SECTION_CACHE_KV.get(`clefrr:v1:result:${raceSuffix(race)}`),
    env.DETAIL_SECTION_CACHE_KV.get(`pred:fp:v1:${raceSuffix(race)}`),
  ]);
  const rerank = parseJson(rerankText);
  const base = parseBasePredictions(parseJson(baseText)) ?? [];
  const rows =
    typeof rerank === "object" &&
    rerank !== null &&
    "predictions" in rerank &&
    Array.isArray(rerank.predictions)
      ? rerank.predictions.filter(isRankRow)
      : toBaseRanks(base);
  return {
    ranks: new Map(rows.map((row) => [row.horseNumber, row.rank])),
    baseGeneratedAt: base.length === 0 ? "" : latestBase(base).predictionGeneratedAt,
  };
};

const compareOddsAsc = (a: { odds: number | null }, b: { odds: number | null }): number =>
  (a.odds ?? Number.POSITIVE_INFINITY) - (b.odds ?? Number.POSITIVE_INFINITY);

// clef-flash re-runs only when the prediction is regenerated or the favourites' order moves;
// smaller odds moves only re-price the tickets.
export const oddsSignature = (realtime: unknown): string =>
  (readOdds(realtime).book.tansho ?? [])
    .toSorted(compareOddsAsc)
    .slice(0, SIGNATURE_FAVORITES)
    .map((entry) => entry.combination)
    .join("-");

const answerKey = (race: BetRaceAddress, baseGeneratedAt: string, signature: string): string =>
  `clefbet:v2:answers:${raceSuffix(race)}:${baseGeneratedAt}:${signature}`;

// The all-types key carries no type list, so the cron warm serves the default view.
const resultKey = (race: BetRaceAddress): string =>
  `clefbet:v3:result:${raceSuffix(race)}:${String(race.budget)}:t${String(race.target ?? "-")}${race.types === null ? "" : `:${[...race.types].toSorted().join(",")}`}${race.requireAll ? ":all" : ""}`;

// Results are written only by suggestRaceBets, so matching cache keys are trusted as BetResponse.
const isBetResponse = (value: unknown): value is BetResponse =>
  typeof value === "object" &&
  value !== null &&
  "oddsFetchedAt" in value &&
  "baseGeneratedAt" in value &&
  "suggestion" in value &&
  "runners" in value;

type UnfitReason = "target-unreachable" | "required-types-unfit";

const UNFIT_BODIES: Record<UnfitReason, { error: string; reason: UnfitReason }> = {
  "target-unreachable": {
    error: "no ticket reaches the target profit within the budget",
    reason: "target-unreachable",
  },
  "required-types-unfit": {
    error: "the selected bet types cannot all fit the budget and target",
    reason: "required-types-unfit",
  },
};

const isUnfitReason = (value: unknown): value is UnfitReason =>
  value === "target-unreachable" || value === "required-types-unfit";

// Odds exist, but the budget, target or required bet types leave no suggestion.
export const unfitReasonOf = (value: unknown): UnfitReason | null =>
  typeof value === "object" && value !== null && "reason" in value && isUnfitReason(value.reason)
    ? value.reason
    : null;

const matchesSnapshot = (
  value: object,
  fresh: Pick<BetResponse, "oddsFetchedAt" | "baseGeneratedAt">,
): boolean =>
  "oddsFetchedAt" in value &&
  value.oddsFetchedAt === fresh.oddsFetchedAt &&
  "baseGeneratedAt" in value &&
  value.baseGeneratedAt === fresh.baseGeneratedAt;

// An unfit budget or target is cached like a suggestion, so repeat views of the same odds snapshot
// skip the viewer's sections too.
const readCachedResult = async (
  env: BetEnv,
  race: BetRaceAddress,
  fresh: Pick<BetResponse, "oddsFetchedAt" | "baseGeneratedAt">,
): Promise<BetOutcome | null> => {
  const cached = parseJson(await env.DETAIL_SECTION_CACHE_KV.get(resultKey(race)));
  if (typeof cached !== "object" || cached === null || !matchesSnapshot(cached, fresh)) return null;
  const reason = unfitReasonOf(cached);
  if (reason !== null) return { status: 404, body: UNFIT_BODIES[reason] };
  return isBetResponse(cached) ? { status: 200, body: cached } : null;
};

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

// Explains a missing suggestion: dropping the required-all rule or the target would have left one.
const diagnoseUnfit = (input: SuggestInput): UnfitReason | null => {
  const { target, ...untargeted } = input;
  if (input.requireAll === true && suggestPlans({ ...input, requireAll: false }) !== null)
    return "required-types-unfit";
  if (target !== undefined && suggestPlans(untargeted) !== null) return "target-unreachable";
  return input.requireAll === true && suggestPlans({ ...untargeted, requireAll: false }) !== null
    ? "required-types-unfit"
    : null;
};

// Every generated suggestion is cached per odds snapshot and budget, so repeat views and the
// cron warm reuse it without touching clef-flash or the viewer's heavier sections.
export const suggestRaceBets = async (env: BetEnv, race: BetRaceAddress): Promise<BetOutcome> => {
  const [realtime, { ranks, baseGeneratedAt }] = await Promise.all([
    fetchViewer(env, viewerPath(race, "realtime")),
    loadRanks(env, race),
  ]);
  const oddsFetchedAt = readOdds(realtime).fetchedAt;
  const cached = await readCachedResult(env, race, { oddsFetchedAt, baseGeneratedAt });
  if (cached !== null) return cached;
  const [heatmap, recent, training, trends] = await Promise.all(
    SOURCE_PATHS.map((suffix) => fetchViewer(env, viewerPath(race, suffix))),
  );
  const built = buildBetRequest({ heatmap, realtime, recent, training, trends, ranks });
  if (built.runners.length === 0 || built.odds.tansho === undefined)
    return { status: 404, body: { error: "runners or odds are not available yet" } };
  const horses = built.runners.map((runner) => runner.horseNumber);
  const answers = await loadAnswers(
    env,
    answerKey(race, baseGeneratedAt, oddsSignature(realtime)),
    built.request,
    horses,
  );
  if (answers === null) return { status: 502, body: { error: "clef-flash answer was malformed" } };
  const selection = {
    race: built,
    answers,
    budget: race.budget,
    requireAll: race.requireAll,
    ...(race.types === null ? {} : { types: race.types }),
  };
  const targeted = { ...selection, ...(race.target === null ? {} : { target: race.target }) };
  const plans = suggestPlans(targeted);
  const reason = plans === null ? diagnoseUnfit(targeted) : null;
  if (reason !== null) {
    await env.DETAIL_SECTION_CACHE_KV.put(
      resultKey(race),
      JSON.stringify({
        ...UNFIT_BODIES[reason],
        oddsFetchedAt: built.oddsFetchedAt,
        baseGeneratedAt,
      }),
      { expirationTtl: ANSWER_TTL_SECONDS },
    );
    return { status: 404, body: UNFIT_BODIES[reason] };
  }
  if (plans === null) return { status: 404, body: { error: "no ticket has published odds" } };
  const body: BetResponse = {
    raceKey: raceSuffix(race),
    baseGeneratedAt,
    oddsFetchedAt: built.oddsFetchedAt,
    generatedAt: new Date().toISOString(),
    runners: built.runners.map((runner) => ({
      ...runner,
      clefWin: answers.winner[String(runner.horseNumber)] ?? null,
      clefTop3: Number(answers.top3[String(runner.horseNumber)]),
    })),
    suggestion: plans.plans[0],
    plans,
    target: race.target,
  };
  await env.DETAIL_SECTION_CACHE_KV.put(resultKey(race), JSON.stringify(body), {
    expirationTtl: ANSWER_TTL_SECONDS,
  });
  return { status: 200, body };
};

const warmedKey = (
  race: Omit<BetRaceAddress, "budget" | "types" | "target" | "requireAll">,
): string => `clefbet:v3:warmed:${race.ymd}:${race.keibajoCode}:${race.raceBango}`;

// Cron path: regenerate the default-budget suggestion only for races whose served prediction
// was (re)generated since the last warm, including the post-horse-weight rescore.
export const warmRaceBets = async (
  env: BetEnv,
  race: Omit<BetRaceAddress, "budget" | "types" | "target" | "requireAll">,
): Promise<number> => {
  const target = {
    ...race,
    budget: DEFAULT_BUDGET,
    types: null,
    target: DEFAULT_TARGET,
    requireAll: false,
  };
  const [{ baseGeneratedAt }, warmed] = await Promise.all([
    loadRanks(env, target),
    env.DETAIL_SECTION_CACHE_KV.get(warmedKey(race)),
  ]);
  if (baseGeneratedAt === "" || warmed === baseGeneratedAt) return 304;
  const outcome = await suggestRaceBets(env, target);
  if (outcome.status === 200 || unfitReasonOf(outcome.body) !== null)
    await env.DETAIL_SECTION_CACHE_KV.put(warmedKey(race), baseGeneratedAt, {
      expirationTtl: ANSWER_TTL_SECONDS,
    });
  return outcome.status;
};
