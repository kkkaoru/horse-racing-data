// Run via bun (CLI scripts/push-neon-sync.ts).
// Warms the pc-keiba-viewer KV cache for the next JST day after the
// post-sync analytics indexes step. This covers the case where the
// scheduled 21:00 JST viewer cron has already fired before the operator
// pushes today's data, leaving tomorrow's races un-warmed.

export type CacheWarmEndpoint = "race-detail-sections" | "race-detail-ssr" | "race-trends";

export interface CacheWarmCredentials {
  accessClientId: string;
  accessClientSecret: string;
}

export interface ViewerCacheWarmEnvironment {
  origin: string;
  skipWarm: boolean;
  credentials: CacheWarmCredentials | null;
}

export interface ResolveViewerCacheWarmEnvironmentInput {
  pushEnv: Record<string, string | undefined>;
  viewerEnv: Record<string, string | undefined>;
}

export interface ComputeTomorrowJstDateInput {
  now: Date;
}

export interface BuildCacheWarmRequestInput {
  origin: string;
  endpoint: CacheWarmEndpoint;
  isoDate: string;
  credentials: CacheWarmCredentials;
}

export interface CacheWarmRequest {
  url: string;
  init: RequestInit;
}

export interface CacheWarmEndpointResult {
  endpoint: CacheWarmEndpoint;
  outcome: "success" | "failure";
  summary: string;
}

export interface FireCacheWarmEndpointInput {
  request: CacheWarmRequest;
  endpoint: CacheWarmEndpoint;
  fetchImpl: typeof fetch;
}

export interface WarmViewerCachesInput {
  pushEnv: Record<string, string | undefined>;
  viewerEnv: Record<string, string | undefined>;
  now: Date;
  fetchImpl: typeof fetch;
  log: (message: string) => void;
}

const DEFAULT_VIEWER_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";
const SKIP_WARM_ENV_KEY = "REPLICA_PUSH_SKIP_VIEWER_WARM";
const VIEWER_ORIGIN_ENV_KEY = "REPLICA_PUSH_VIEWER_ORIGIN";
const ACCESS_CLIENT_ID_ENV_KEY = "PC_KEIBA_ACCESS_CLIENT_ID";
const ACCESS_CLIENT_SECRET_ENV_KEY = "PC_KEIBA_ACCESS_CLIENT_SECRET";
const SKIP_WARM_TRUE_VALUE = "1";
const JST_OFFSET_MS = 9 * 60 * 60 * 1000;
const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const CACHE_WARM_HEADER_KEY = "X-PC-Keiba-Cache-Warm";
const CACHE_WARM_HEADER_VALUE = "scheduled";
const ACCESS_CLIENT_ID_HEADER_KEY = "CF-Access-Client-Id";
const ACCESS_CLIENT_SECRET_HEADER_KEY = "CF-Access-Client-Secret";
const CONTENT_TYPE_HEADER_KEY = "Content-Type";
const CONTENT_TYPE_JSON = "application/json";
const ENDPOINT_LIST: readonly CacheWarmEndpoint[] = [
  "race-detail-sections",
  "race-detail-ssr",
  "race-trends",
];
const ISO_DATE_PAD_LENGTH = 2;
const ISO_DATE_YEAR_PAD_LENGTH = 4;

export function computeTomorrowJstDate(input: ComputeTomorrowJstDateInput): string {
  const jstMs = input.now.getTime() + JST_OFFSET_MS + ONE_DAY_MS;
  const jstDate = new Date(jstMs);
  const year = jstDate.getUTCFullYear().toString().padStart(ISO_DATE_YEAR_PAD_LENGTH, "0");
  const month = (jstDate.getUTCMonth() + 1).toString().padStart(ISO_DATE_PAD_LENGTH, "0");
  const day = jstDate.getUTCDate().toString().padStart(ISO_DATE_PAD_LENGTH, "0");
  return `${year}-${month}-${day}`;
}

function resolveCredentials(
  viewerEnv: Record<string, string | undefined>,
): CacheWarmCredentials | null {
  const accessClientId = viewerEnv[ACCESS_CLIENT_ID_ENV_KEY];
  const accessClientSecret = viewerEnv[ACCESS_CLIENT_SECRET_ENV_KEY];
  if (
    accessClientId === undefined ||
    accessClientId === "" ||
    accessClientSecret === undefined ||
    accessClientSecret === ""
  ) {
    return null;
  }
  return { accessClientId, accessClientSecret };
}

export function resolveViewerCacheWarmEnvironment(
  input: ResolveViewerCacheWarmEnvironmentInput,
): ViewerCacheWarmEnvironment {
  const origin = input.pushEnv[VIEWER_ORIGIN_ENV_KEY] ?? DEFAULT_VIEWER_ORIGIN;
  const skipWarm = input.pushEnv[SKIP_WARM_ENV_KEY] === SKIP_WARM_TRUE_VALUE;
  const credentials = resolveCredentials(input.viewerEnv);
  return { origin, skipWarm, credentials };
}

export function buildCacheWarmRequest(input: BuildCacheWarmRequestInput): CacheWarmRequest {
  const url = `${input.origin}/api/cache-warm/${input.endpoint}?date=${input.isoDate}`;
  const headers: Record<string, string> = {
    [CACHE_WARM_HEADER_KEY]: CACHE_WARM_HEADER_VALUE,
    [ACCESS_CLIENT_ID_HEADER_KEY]: input.credentials.accessClientId,
    [ACCESS_CLIENT_SECRET_HEADER_KEY]: input.credentials.accessClientSecret,
    [CONTENT_TYPE_HEADER_KEY]: CONTENT_TYPE_JSON,
  };
  return { url, init: { method: "POST", headers } };
}

function summarizeSuccessBody(body: unknown): string {
  if (body === null || typeof body !== "object") {
    return JSON.stringify(body);
  }
  const record = body as Record<string, unknown>;
  const enqueued = record.enqueued;
  const raceCount = record.raceCount;
  const warmed = record.warmed;
  const dueRaceCount = record.dueRaceCount;
  const segments: string[] = [];
  if (typeof enqueued === "number") segments.push(`enqueued=${enqueued}`);
  if (typeof warmed === "number") segments.push(`warmed=${warmed}`);
  if (typeof dueRaceCount === "number") segments.push(`dueRaceCount=${dueRaceCount}`);
  if (typeof raceCount === "number") segments.push(`raceCount=${raceCount}`);
  return segments.length === 0 ? JSON.stringify(record) : segments.join(" ");
}

export async function fireCacheWarmEndpoint(
  input: FireCacheWarmEndpointInput,
): Promise<CacheWarmEndpointResult> {
  const response = await input.fetchImpl(input.request.url, input.request.init);
  if (!response.ok) {
    return {
      endpoint: input.endpoint,
      outcome: "failure",
      summary: `HTTP ${response.status}`,
    };
  }
  const body = (await response.json()) as unknown;
  return {
    endpoint: input.endpoint,
    outcome: "success",
    summary: summarizeSuccessBody(body),
  };
}

interface LogSkipMessageInput {
  log: (message: string) => void;
  skipWarm: boolean;
  hasCredentials: boolean;
}

function logSkipReason(input: LogSkipMessageInput): boolean {
  if (input.skipWarm) {
    input.log(`Viewer cache warm skipped (${SKIP_WARM_ENV_KEY}=${SKIP_WARM_TRUE_VALUE})`);
    return true;
  }
  if (!input.hasCredentials) {
    input.log("⚠ viewer warm skipped: credentials missing");
    return true;
  }
  return false;
}

function logEndpointResult(log: (message: string) => void, result: CacheWarmEndpointResult): void {
  const icon = result.outcome === "success" ? "✓" : "⚠";
  const verb = result.outcome === "success" ? "" : " failed";
  log(`${icon} ${result.endpoint}${verb}: ${result.summary}`);
}

function describeFailureReason(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason);
}

interface AwaitSettledResultsInput {
  settled: PromiseSettledResult<CacheWarmEndpointResult>[];
  endpoints: readonly CacheWarmEndpoint[];
  log: (message: string) => void;
}

function handleSettledResults(input: AwaitSettledResultsInput): void {
  input.settled.forEach((settled, index) => {
    if (settled.status === "fulfilled") {
      logEndpointResult(input.log, settled.value);
      return;
    }
    const endpoint = input.endpoints[index]!;
    input.log(`⚠ ${endpoint} failed: ${describeFailureReason(settled.reason)}`);
  });
}

export async function warmViewerCachesForTomorrowJst(input: WarmViewerCachesInput): Promise<void> {
  const environment = resolveViewerCacheWarmEnvironment({
    pushEnv: input.pushEnv,
    viewerEnv: input.viewerEnv,
  });
  if (
    logSkipReason({
      log: input.log,
      skipWarm: environment.skipWarm,
      hasCredentials: environment.credentials !== null,
    })
  ) {
    return;
  }
  const credentials = environment.credentials!;
  const isoDate = computeTomorrowJstDate({ now: input.now });
  input.log(`Warming viewer KV cache for tomorrow JST (${isoDate})`);
  const requests = ENDPOINT_LIST.map((endpoint) => ({
    endpoint,
    request: buildCacheWarmRequest({
      origin: environment.origin,
      endpoint,
      isoDate,
      credentials,
    }),
  }));
  const settled = await Promise.allSettled(
    requests.map(({ endpoint, request }) =>
      fireCacheWarmEndpoint({ endpoint, request, fetchImpl: input.fetchImpl }),
    ),
  );
  handleSettledResults({ settled, endpoints: ENDPOINT_LIST, log: input.log });
}
