// Run via Bun from scripts/run-pc-keiba-update-and-sync.ts.

export type RealtimeDiscoveryJobType = "discover-urls" | "plan-premium-race-data-fetches";

export interface RealtimeDiscoveryOptions {
  baseUrl: string;
  dates: RealtimeDiscoveryDates;
  fetcher: typeof fetch;
  log: (message: string) => void;
  pollIntervalMilliseconds: number;
  pollTimeoutMilliseconds: number;
  retryDelay: (milliseconds: number) => Promise<void>;
  token: string;
}

export interface RealtimeDiscoveryDates {
  base: string;
  next: string;
}

interface FetchAttemptSuccess {
  kind: "success";
  response: Response;
}

interface FetchAttemptFailure {
  error: unknown;
  kind: "failure";
}

type FetchAttempt = FetchAttemptFailure | FetchAttemptSuccess;

interface RealtimeRequest {
  date: string;
  init: RequestInit;
  input: string;
  label: string;
}

interface HttpFailureLog {
  attempt: number;
  request: RealtimeRequest;
  responseBody: string;
  status: number;
}

interface DiscoveryStatus {
  complete: boolean;
  d1JraRaceCount: number;
  date: string;
  neonJraRaceCount: number;
}

const JOB_PATH = "/api/jobs";
const DISCOVERY_STATUS_PATH = "/api/internal/discovery-status";
const MAX_ATTEMPTS = 3;
const RETRYABLE_REQUEST_TIMEOUT = 408;
const RETRYABLE_TOO_MANY_REQUESTS = 429;
const SERVER_ERROR_STATUS = 500;
const MAX_LOGGED_RESPONSE_BODY_LENGTH = 256;
const SENSITIVE_JSON_FIELD_PATTERN =
  /("(?:api[-_]?key|authorization|cookie|password|secret|token)"\s*:\s*)"(?:\\.|[^"\\])*"/giu;
const BEARER_TOKEN_PATTERN = /\bBearer\s+[a-z0-9._~+/-]+=*/giu;

const validateDate = (date: string): string => {
  if (/^\d{8}$/u.test(date)) return date;
  throw new Error(`SYNC_REALTIME_DATA_DATE must use YYYYMMDD format: ${date}`);
};

export const getJstDate = (now: Date): string =>
  now.toLocaleDateString("sv-SE", { timeZone: "Asia/Tokyo" }).replaceAll("-", "");

export const resolveRealtimeDiscoveryDate = (
  configuredDate: string | undefined,
  now: Date,
): string =>
  validateDate(
    configuredDate === undefined || configuredDate === "" ? getJstDate(now) : configuredDate,
  );

export const addDaysToRealtimeDiscoveryDate = (date: string, days: number): string => {
  const parsed = validateDate(date);
  const instant = new Date(
    `${parsed.slice(0, 4)}-${parsed.slice(4, 6)}-${parsed.slice(6, 8)}T00:00:00+09:00`,
  );
  instant.setUTCDate(instant.getUTCDate() + days);
  return getJstDate(instant);
};

export const resolveRealtimeDiscoveryDates = (
  configuredDate: string | undefined,
  now: Date,
): RealtimeDiscoveryDates => {
  const base = resolveRealtimeDiscoveryDate(configuredDate, now);
  return { base, next: addDaysToRealtimeDiscoveryDate(base, 1) };
};

const unquoteEnvValue = (value: string): string => {
  const quoted =
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"));
  return quoted ? value.slice(1, -1) : value;
};

export const readEnvValue = (contents: string, name: string): string | undefined => {
  const prefix = `${name}=`;
  const line = contents
    .split(/\r?\n/u)
    .map((candidate) => candidate.trim())
    .find((candidate) => candidate.startsWith(prefix));
  return line === undefined ? undefined : unquoteEnvValue(line.slice(prefix.length).trim());
};

export const resolveRealtimeAdminToken = (
  explicitToken: string | undefined,
  devVarsContents: string,
): string => {
  const token =
    explicitToken === undefined || explicitToken === ""
      ? readEnvValue(devVarsContents, "REALTIME_ADMIN_TOKEN")
      : explicitToken;
  if (token !== undefined && token !== "") return token;
  throw new Error(
    "REALTIME_ADMIN_TOKEN is required; set it in the environment or apps/sync-realtime-data/.dev.vars",
  );
};

const shouldRetryStatus = (status: number): boolean =>
  status === RETRYABLE_REQUEST_TIMEOUT ||
  status === RETRYABLE_TOO_MANY_REQUESTS ||
  status >= SERVER_ERROR_STATUS;

const retryDelayMilliseconds = (attempt: number): number => (attempt === 1 ? 250 : 1_000);

const executeFetch = async (
  options: RealtimeDiscoveryOptions,
  input: string,
  init: RequestInit,
): Promise<FetchAttempt> =>
  options.fetcher(input, init).then(
    (response): FetchAttempt => ({ kind: "success", response }),
    (error: unknown): FetchAttempt => ({ error, kind: "failure" }),
  );

const sanitizeResponseBody = (responseBody: string): string => {
  const normalized = responseBody
    .replace(SENSITIVE_JSON_FIELD_PATTERN, '$1"[REDACTED]"')
    .replace(BEARER_TOKEN_PATTERN, "Bearer [REDACTED]")
    .replace(/\s+/gu, " ")
    .trim();
  return normalized.length <= MAX_LOGGED_RESPONSE_BODY_LENGTH
    ? normalized
    : `${normalized.slice(0, MAX_LOGGED_RESPONSE_BODY_LENGTH)}…`;
};

const responseBodyDetail = (responseBody: string): string =>
  responseBody === "" ? "no response body" : responseBody;

const logHttpFailure = (options: RealtimeDiscoveryOptions, failure: HttpFailureLog): void => {
  const body = responseBodyDetail(sanitizeResponseBody(failure.responseBody));
  options.log(
    `Realtime request non-2xx url=${failure.request.input} date=${failure.request.date} attempt=${failure.attempt}/${MAX_ATTEMPTS} status=${failure.status} body=${JSON.stringify(body)}`,
  );
};

const requestWithRetry = async (
  options: RealtimeDiscoveryOptions,
  request: RealtimeRequest,
  attempt: number,
): Promise<Response> => {
  const result = await executeFetch(options, request.input, request.init);
  if (result.kind === "failure") {
    if (attempt >= MAX_ATTEMPTS) {
      const detail = result.error instanceof Error ? result.error.message : String(result.error);
      throw new Error(
        `Realtime request ${request.label} failed after ${attempt} attempts: ${detail}`,
        { cause: result.error },
      );
    }
    options.log(`Realtime request ${request.label} failed; retrying attempt ${attempt + 1}...`);
    await options.retryDelay(retryDelayMilliseconds(attempt));
    return requestWithRetry(options, request, attempt + 1);
  }

  if (result.response.ok) return result.response;
  const responseBody = (await result.response.text()).trim();
  logHttpFailure(options, {
    attempt,
    request,
    responseBody,
    status: result.response.status,
  });
  if (shouldRetryStatus(result.response.status) && attempt < MAX_ATTEMPTS) {
    options.log(
      `Realtime request ${request.label} returned HTTP ${result.response.status}; retrying attempt ${attempt + 1}...`,
    );
    await options.retryDelay(retryDelayMilliseconds(attempt));
    return requestWithRetry(options, request, attempt + 1);
  }
  const detail = responseBodyDetail(sanitizeResponseBody(responseBody));
  const cause = new Error(`HTTP ${result.response.status}: ${detail}`);
  throw new Error(
    `Realtime request ${request.label} failed with HTTP ${result.response.status}: ${detail}`,
    { cause },
  );
};

const requestHeaders = (token: string): Record<string, string> => ({
  authorization: `Bearer ${token}`,
  "content-type": "application/json",
});

const enqueueJob = async (
  options: RealtimeDiscoveryOptions,
  date: string,
  jobType: RealtimeDiscoveryJobType,
): Promise<void> => {
  await requestWithRetry(
    options,
    {
      date,
      init: {
        body: JSON.stringify({ date, type: jobType }),
        headers: requestHeaders(options.token),
        method: "POST",
      },
      input: `${options.baseUrl.replace(/\/+$/u, "")}${JOB_PATH}`,
      label: `enqueue ${jobType} ${date}`,
    },
    1,
  );
};

const parseDiscoveryStatus = (value: unknown, expectedDate: string): DiscoveryStatus => {
  if (typeof value !== "object" || value === null) {
    throw new Error(`Invalid discovery status response for ${expectedDate}`);
  }
  const candidate = value satisfies object;
  if (
    !("complete" in candidate) ||
    typeof candidate.complete !== "boolean" ||
    !("date" in candidate) ||
    candidate.date !== expectedDate ||
    !("d1JraRaceCount" in candidate) ||
    !Number.isInteger(candidate.d1JraRaceCount) ||
    Number(candidate.d1JraRaceCount) < 0 ||
    !("neonJraRaceCount" in candidate) ||
    !Number.isInteger(candidate.neonJraRaceCount) ||
    Number(candidate.neonJraRaceCount) < 0
  ) {
    throw new Error(`Invalid discovery status response for ${expectedDate}`);
  }
  return {
    complete: candidate.complete,
    d1JraRaceCount: Number(candidate.d1JraRaceCount),
    date: candidate.date,
    neonJraRaceCount: Number(candidate.neonJraRaceCount),
  };
};

const parseRetryAfterMilliseconds = (value: string | null, fallback: number): number => {
  if (value === null) return fallback;
  const seconds = Number(value);
  return Number.isFinite(seconds) && seconds > 0 ? seconds * 1_000 : fallback;
};

const pollDiscovery = async (
  options: RealtimeDiscoveryOptions,
  date: string,
  elapsedMilliseconds: number,
): Promise<void> => {
  const response = await requestWithRetry(
    options,
    {
      date,
      init: { headers: requestHeaders(options.token), method: "GET" },
      input: `${options.baseUrl.replace(/\/+$/u, "")}${DISCOVERY_STATUS_PATH}?date=${date}`,
      label: `discovery status ${date}`,
    },
    1,
  );
  const body: unknown = await response.json();
  const status = parseDiscoveryStatus(body, date);
  if (status.complete) {
    options.log(
      `Discovery completed for ${date}: D1 JRA ${status.d1JraRaceCount}/${status.neonJraRaceCount}.`,
    );
    return;
  }
  if (elapsedMilliseconds >= options.pollTimeoutMilliseconds) {
    throw new Error(
      `Timed out waiting for discovery ${date}: D1 JRA ${status.d1JraRaceCount}/${status.neonJraRaceCount}`,
    );
  }
  const requestedDelay = parseRetryAfterMilliseconds(
    response.headers.get("retry-after"),
    options.pollIntervalMilliseconds,
  );
  const remaining = options.pollTimeoutMilliseconds - elapsedMilliseconds;
  const delay = Math.min(requestedDelay, remaining);
  options.log(
    `Discovery pending for ${date}: D1 JRA ${status.d1JraRaceCount}/${status.neonJraRaceCount}; polling again in ${delay}ms.`,
  );
  await options.retryDelay(delay);
  await pollDiscovery(options, date, elapsedMilliseconds + delay);
};

const discoverThenPlan = async (options: RealtimeDiscoveryOptions, date: string): Promise<void> => {
  await enqueueJob(options, date, "discover-urls");
  await pollDiscovery(options, date, 0);
  await enqueueJob(options, date, "plan-premium-race-data-fetches");
};

export const triggerRealtimeDiscoveryAfterReplica = async (
  options: RealtimeDiscoveryOptions,
): Promise<void> => {
  validateDate(options.dates.base);
  validateDate(options.dates.next);
  if (options.token === "") {
    throw new Error("REALTIME_ADMIN_TOKEN must not be empty");
  }
  if (options.pollIntervalMilliseconds <= 0 || options.pollTimeoutMilliseconds <= 0) {
    throw new Error("Discovery poll interval and timeout must be positive");
  }
  await discoverThenPlan(options, options.dates.base);
  await discoverThenPlan(options, options.dates.next);
};
