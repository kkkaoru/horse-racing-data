const DEFAULT_HOT_API_BASE_URL = "https://sync-realtime-data-hot.kkk4oru.com";
const DEFAULT_D1_CUTOFF = "2026-07-08T18:52:00+09:00";
const DEFAULT_D1_DATABASE_NAME = "sync-realtime-data-hot-v2";
const DEFAULT_WAREHOUSE = "78109ec18c7c85b194b19fb32e3bb149_pc-keiba-odds-archive";

export const REQUIRED_ODDS_TYPES = [
  "tansho",
  "fukusho",
  "wakuren",
  "umaren",
  "umatan",
  "wide",
  "3renpuku",
  "3rentan",
] as const;

export interface CommandResult {
  code: number;
  stdout: string;
  stderr: string;
}

export type CommandRunner = (
  args: string[],
  env?: Record<string, string>,
) => Promise<CommandResult>;

export interface OddsR2CutoverConfig {
  commandImpl: CommandRunner;
  d1DatabaseName: string;
  fetchImpl: typeof fetch;
  hotApiBaseUrl: string;
  raceKeys: string[];
  r2SqlAuthToken?: string;
  sinceFetchedAt: string;
  warehouse: string;
}

export interface CheckResult {
  detail: string;
  name: string;
  ok: boolean;
}

export interface VerifyOddsR2CutoverResult {
  checks: CheckResult[];
  ok: boolean;
}

interface OddsPayload {
  fetchedAt?: unknown;
  latest?: unknown;
  raceKey?: unknown;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const buildCheck = (name: string, ok: boolean, detail: string): CheckResult => ({
  detail,
  name,
  ok,
});

export const buildDefaultConfig = (
  commandImpl: CommandRunner,
  fetchImpl: typeof fetch,
  env: Record<string, string | undefined>,
): OddsR2CutoverConfig => ({
  commandImpl,
  d1DatabaseName: env.ODDS_D1_DATABASE_NAME ?? DEFAULT_D1_DATABASE_NAME,
  fetchImpl,
  hotApiBaseUrl: env.ODDS_HOT_API_BASE_URL ?? DEFAULT_HOT_API_BASE_URL,
  raceKeys: (env.ODDS_R2_VERIFY_RACE_KEYS ?? "")
    .split(",")
    .map((raceKey) => raceKey.trim())
    .filter(Boolean),
  r2SqlAuthToken: env.WRANGLER_R2_SQL_AUTH_TOKEN ?? env.R2_API_TOKEN,
  sinceFetchedAt: env.ODDS_R2_VERIFY_D1_CUTOFF ?? DEFAULT_D1_CUTOFF,
  warehouse: env.ODDS_R2_SQL_WAREHOUSE ?? DEFAULT_WAREHOUSE,
});

export const buildHotOddsUrl = (baseUrl: string, raceKey: string): string => {
  const url = new URL(`/api/odds/${encodeURIComponent(raceKey)}`, baseUrl);
  url.searchParams.set("fresh", "1");
  return url.toString();
};

export const summarizeLatestCounts = (payload: OddsPayload): Record<string, number> => {
  if (!isRecord(payload.latest)) {
    return {};
  }
  return Object.fromEntries(
    Object.entries(payload.latest).map(([oddsType, rows]) => [
      oddsType,
      Array.isArray(rows) ? rows.length : 0,
    ]),
  );
};

export const validateHotOddsPayload = (
  expectedRaceKey: string,
  payload: OddsPayload,
): CheckResult[] => {
  const counts = summarizeLatestCounts(payload);
  const missingTypes = REQUIRED_ODDS_TYPES.filter((oddsType) => (counts[oddsType] ?? 0) === 0);
  return [
    buildCheck(
      `hot-api raceKey ${expectedRaceKey}`,
      payload.raceKey === expectedRaceKey,
      `actual=${String(payload.raceKey)}`,
    ),
    buildCheck(
      `hot-api fetchedAt ${expectedRaceKey}`,
      typeof payload.fetchedAt === "string" && payload.fetchedAt.length > 0,
      `fetchedAt=${String(payload.fetchedAt)}`,
    ),
    buildCheck(
      `hot-api odds types ${expectedRaceKey}`,
      missingTypes.length === 0,
      missingTypes.length === 0 ? JSON.stringify(counts) : `missing=${missingTypes.join(",")}`,
    ),
  ];
};

export const verifyHotApiRace = async (
  config: OddsR2CutoverConfig,
  raceKey: string,
): Promise<CheckResult[]> => {
  const response = await config.fetchImpl(buildHotOddsUrl(config.hotApiBaseUrl, raceKey));
  if (!response.ok) {
    return [buildCheck(`hot-api fetch ${raceKey}`, false, `status=${String(response.status)}`)];
  }
  const payload = (await response.json()) as OddsPayload;
  return [
    buildCheck(`hot-api fetch ${raceKey}`, true, `status=${String(response.status)}`),
    ...validateHotOddsPayload(raceKey, payload),
  ];
};

const parseJsonArrayFromWranglerOutput = (output: string): unknown[] | null => {
  const start = output.indexOf("[");
  const end = output.lastIndexOf("]");
  if (start === -1 || end === -1 || end < start) {
    return null;
  }
  try {
    const parsed = JSON.parse(output.slice(start, end + 1));
    return Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
};

const extractD1Count = (output: string): number | null => {
  const parsed = parseJsonArrayFromWranglerOutput(output);
  const first = parsed?.[0];
  if (!isRecord(first) || !Array.isArray(first.results)) {
    return null;
  }
  const result = first.results[0];
  return isRecord(result) && typeof result.rows === "number" ? result.rows : null;
};

const D1_ODDS_SNAPSHOTS_TABLE_SQL =
  "select name from sqlite_master where type = 'table' and name = 'odds_snapshots'";

const d1ExecuteArgs = (databaseName: string, sql: string): string[] => [
  "bunx",
  "wrangler",
  "d1",
  "execute",
  databaseName,
  "--remote",
  "--command",
  sql,
];

const hasD1OddsSnapshotsTable = (output: string): boolean | null => {
  const parsed = parseJsonArrayFromWranglerOutput(output);
  const first = parsed?.[0];
  if (!isRecord(first) || !Array.isArray(first.results)) {
    return null;
  }
  return first.results.some((row) => isRecord(row) && row.name === "odds_snapshots");
};

export const verifyD1OddsWritesStopped = async (
  config: OddsR2CutoverConfig,
): Promise<CheckResult> => {
  const tableResult = await config.commandImpl(
    d1ExecuteArgs(config.d1DatabaseName, D1_ODDS_SNAPSHOTS_TABLE_SQL),
  );
  if (tableResult.code !== 0) {
    return buildCheck(
      "d1 odds_snapshots stopped",
      false,
      tableResult.stderr.trim() || "table check failed",
    );
  }
  const tableExists = hasD1OddsSnapshotsTable(tableResult.stdout);
  if (tableExists === false) {
    return buildCheck("d1 odds_snapshots stopped", true, "table dropped");
  }
  if (tableExists === null) {
    return buildCheck("d1 odds_snapshots stopped", false, "table check unavailable");
  }
  const sql = `select count(*) as rows from odds_snapshots where fetched_at >= '${config.sinceFetchedAt.replaceAll("'", "''")}'`;
  const result = await config.commandImpl(d1ExecuteArgs(config.d1DatabaseName, sql));
  const rows = result.code === 0 ? extractD1Count(result.stdout) : null;
  return buildCheck(
    "d1 odds_snapshots stopped",
    rows === 0,
    rows === null ? result.stderr.trim() || "count unavailable" : `rows=${String(rows)}`,
  );
};

export const verifyCloudflareResource = async (
  commandImpl: CommandRunner,
  args: string[],
  expectedText: string,
  name: string,
): Promise<CheckResult> => {
  const result = await commandImpl(args);
  const output = `${result.stdout}\n${result.stderr}`;
  return buildCheck(
    name,
    result.code === 0 && output.includes(expectedText),
    result.code === 0 ? `expected=${expectedText}` : output.trim(),
  );
};

export const verifyR2SqlNamespace = async (config: OddsR2CutoverConfig): Promise<CheckResult> => {
  if (!config.r2SqlAuthToken) {
    return buildCheck("r2-sql namespace odds", false, "missing WRANGLER_R2_SQL_AUTH_TOKEN");
  }
  const result = await config.commandImpl(
    ["bunx", "wrangler", "r2", "sql", "query", config.warehouse, "SHOW NAMESPACES"],
    { WRANGLER_R2_SQL_AUTH_TOKEN: config.r2SqlAuthToken },
  );
  const output = `${result.stdout}\n${result.stderr}`;
  const hasError = output.includes("ERROR") || output.includes("Unauthorized");
  const ok = result.code === 0 && !hasError && output.includes("odds");
  return buildCheck("r2-sql namespace odds", ok, ok ? "queried SHOW NAMESPACES" : output.trim());
};

export const verifyOddsR2Cutover = async (
  config: OddsR2CutoverConfig,
): Promise<VerifyOddsR2CutoverResult> => {
  const checks: CheckResult[] = [];
  if (config.raceKeys.length === 0) {
    checks.push(buildCheck("hot-api race keys", false, "missing ODDS_R2_VERIFY_RACE_KEYS"));
  }
  for (const raceKey of config.raceKeys) {
    checks.push(...(await verifyHotApiRace(config, raceKey)));
  }
  checks.push(await verifyD1OddsWritesStopped(config));
  checks.push(
    await verifyCloudflareResource(
      config.commandImpl,
      ["bunx", "wrangler", "r2", "bucket", "catalog", "get", "pc-keiba-odds-archive"],
      "Status:       active",
      "r2 catalog active",
    ),
  );
  checks.push(
    await verifyCloudflareResource(
      config.commandImpl,
      ["bunx", "wrangler", "pipelines", "streams", "list"],
      "odds_snapshots_hot_stream",
      "pipeline stream exists",
    ),
  );
  checks.push(
    await verifyCloudflareResource(
      config.commandImpl,
      ["bunx", "wrangler", "pipelines", "sinks", "list"],
      "odds_snapshots_hot_sink",
      "pipeline sink exists",
    ),
  );
  checks.push(
    await verifyCloudflareResource(
      config.commandImpl,
      ["bunx", "wrangler", "pipelines", "list"],
      "odds_snapshots_hot_pipeline",
      "pipeline exists",
    ),
  );
  checks.push(await verifyR2SqlNamespace(config));
  return { checks, ok: checks.every((check) => check.ok) };
};
