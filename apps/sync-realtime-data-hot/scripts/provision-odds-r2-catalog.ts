import { type CheckResult, type CommandRunner } from "./verify-odds-r2-cutover";

const DEFAULT_BUCKET = "pc-keiba-odds-archive";
const DEFAULT_NAMESPACE = "odds";
const DEFAULT_PIPELINE_NAME = "odds_snapshots_hot_pipeline";
const DEFAULT_PIPELINE_SQL_FILE = "apps/sync-realtime-data-hot/pipelines/odds-catalog-pipeline.sql";
const DEFAULT_ROLL_INTERVAL_SECONDS = "60";
const DEFAULT_SINK_NAME = "odds_snapshots_hot_sink";
const DEFAULT_STREAM_NAME = "odds_snapshots_hot_stream";
const DEFAULT_TABLE = "snapshots_hot";

export interface ProvisionOddsR2CatalogConfig {
  bucket: string;
  catalogToken?: string;
  commandImpl: CommandRunner;
  namespace: string;
  pipelineName: string;
  pipelineSqlFile: string;
  rollIntervalSeconds: string;
  sinkName: string;
  streamName: string;
  table: string;
}

export interface ProvisionOddsR2CatalogResult {
  checks: CheckResult[];
  ok: boolean;
}

const buildCheck = (name: string, ok: boolean, detail: string): CheckResult => ({
  detail,
  name,
  ok,
});

export const buildDefaultProvisionConfig = (
  commandImpl: CommandRunner,
  env: Record<string, string | undefined>,
): ProvisionOddsR2CatalogConfig => ({
  bucket: env.ODDS_R2_CATALOG_BUCKET ?? DEFAULT_BUCKET,
  catalogToken: env.ODDS_R2_CATALOG_TOKEN ?? env.WRANGLER_R2_SQL_AUTH_TOKEN ?? env.R2_API_TOKEN,
  commandImpl,
  namespace: env.ODDS_R2_CATALOG_NAMESPACE ?? DEFAULT_NAMESPACE,
  pipelineName: env.ODDS_R2_CATALOG_PIPELINE_NAME ?? DEFAULT_PIPELINE_NAME,
  pipelineSqlFile: env.ODDS_R2_CATALOG_PIPELINE_SQL_FILE ?? DEFAULT_PIPELINE_SQL_FILE,
  rollIntervalSeconds: env.ODDS_R2_CATALOG_ROLL_INTERVAL_SECONDS ?? DEFAULT_ROLL_INTERVAL_SECONDS,
  sinkName: env.ODDS_R2_CATALOG_SINK_NAME ?? DEFAULT_SINK_NAME,
  streamName: env.ODDS_R2_CATALOG_STREAM_NAME ?? DEFAULT_STREAM_NAME,
  table: env.ODDS_R2_CATALOG_TABLE ?? DEFAULT_TABLE,
});

const commandOutput = (result: Awaited<ReturnType<CommandRunner>>): string =>
  `${result.stdout}\n${result.stderr}`.trim();

const hasCommandError = (output: string): boolean =>
  output.includes("ERROR") ||
  output.includes("Unauthorized") ||
  output.includes("not authorized") ||
  output.includes("code: 1012");

export const commandIncludes = async (
  commandImpl: CommandRunner,
  args: string[],
  expectedText: string,
): Promise<boolean> => {
  const result = await commandImpl(args);
  const output = commandOutput(result);
  return result.code === 0 && !hasCommandError(output) && output.includes(expectedText);
};

export const ensureCatalogActive = async (
  config: ProvisionOddsR2CatalogConfig,
): Promise<CheckResult> => {
  const result = await config.commandImpl([
    "bunx",
    "wrangler",
    "r2",
    "bucket",
    "catalog",
    "get",
    config.bucket,
  ]);
  const output = commandOutput(result);
  const ok =
    result.code === 0 && !hasCommandError(output) && output.includes("Status:       active");
  return buildCheck("r2 catalog active", ok, ok ? `bucket=${config.bucket}` : output);
};

export const ensureStreamExists = async (
  config: ProvisionOddsR2CatalogConfig,
): Promise<CheckResult> => {
  const exists = await commandIncludes(
    config.commandImpl,
    ["bunx", "wrangler", "pipelines", "streams", "list"],
    config.streamName,
  );
  return buildCheck(
    "pipeline stream exists",
    exists,
    exists ? `stream=${config.streamName}` : `missing stream=${config.streamName}`,
  );
};

export const ensureSinkExists = async (
  config: ProvisionOddsR2CatalogConfig,
): Promise<CheckResult> => {
  const exists = await commandIncludes(
    config.commandImpl,
    ["bunx", "wrangler", "pipelines", "sinks", "list"],
    config.sinkName,
  );
  if (exists) {
    return buildCheck("pipeline sink exists", true, `sink=${config.sinkName}`);
  }
  if (!config.catalogToken) {
    return buildCheck("pipeline sink exists", false, "missing ODDS_R2_CATALOG_TOKEN");
  }
  const result = await config.commandImpl([
    "bunx",
    "wrangler",
    "pipelines",
    "sinks",
    "create",
    config.sinkName,
    "--type",
    "r2-data-catalog",
    "--bucket",
    config.bucket,
    "--namespace",
    config.namespace,
    "--table",
    config.table,
    "--catalog-token",
    config.catalogToken,
    "--roll-interval",
    config.rollIntervalSeconds,
  ]);
  const output = commandOutput(result);
  const ok = result.code === 0 && !hasCommandError(output);
  return buildCheck("pipeline sink exists", ok, ok ? `created sink=${config.sinkName}` : output);
};

export const ensurePipelineExists = async (
  config: ProvisionOddsR2CatalogConfig,
): Promise<CheckResult> => {
  const exists = await commandIncludes(
    config.commandImpl,
    ["bunx", "wrangler", "pipelines", "list"],
    config.pipelineName,
  );
  if (exists) {
    return buildCheck("pipeline exists", true, `pipeline=${config.pipelineName}`);
  }
  const result = await config.commandImpl([
    "bunx",
    "wrangler",
    "pipelines",
    "create",
    config.pipelineName,
    "--sql-file",
    config.pipelineSqlFile,
  ]);
  const output = commandOutput(result);
  const ok = result.code === 0 && !hasCommandError(output);
  return buildCheck("pipeline exists", ok, ok ? `created pipeline=${config.pipelineName}` : output);
};

export const provisionOddsR2Catalog = async (
  config: ProvisionOddsR2CatalogConfig,
): Promise<ProvisionOddsR2CatalogResult> => {
  const checks: CheckResult[] = [
    await ensureCatalogActive(config),
    await ensureStreamExists(config),
    await ensureSinkExists(config),
  ];
  const sinkOk = checks[checks.length - 1]?.ok === true;
  if (sinkOk) {
    checks.push(await ensurePipelineExists(config));
  } else {
    checks.push(buildCheck("pipeline exists", false, "skipped because sink is unavailable"));
  }
  return { checks, ok: checks.every((check) => check.ok) };
};
