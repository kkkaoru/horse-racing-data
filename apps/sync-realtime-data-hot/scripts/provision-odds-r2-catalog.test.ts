// Run with bun.
import { expect, it, vi } from "vitest";

import {
  buildDefaultProvisionConfig,
  commandIncludes,
  ensureCatalogActive,
  ensurePipelineExists,
  ensureSinkExists,
  ensureStreamExists,
  provisionOddsR2Catalog,
  type ProvisionOddsR2CatalogConfig,
} from "./provision-odds-r2-catalog";
import type { CommandResult } from "./verify-odds-r2-cutover";

const buildCommandResult = (overrides: Partial<CommandResult> = {}): CommandResult => ({
  code: 0,
  stderr: "",
  stdout: "",
  ...overrides,
});

const buildConfig = (
  overrides: Partial<ProvisionOddsR2CatalogConfig> = {},
): ProvisionOddsR2CatalogConfig => ({
  bucket: "pc-keiba-odds-archive",
  catalogToken: "token",
  commandImpl: vi.fn(async () => buildCommandResult()),
  namespace: "odds",
  pipelineName: "odds_snapshots_hot_pipeline",
  pipelineSqlFile: "apps/sync-realtime-data-hot/pipelines/odds-catalog-pipeline.sql",
  rollIntervalSeconds: "60",
  sinkName: "odds_snapshots_hot_sink",
  streamName: "odds_snapshots_hot_stream",
  table: "snapshots_hot",
  ...overrides,
});

it("buildDefaultProvisionConfig reads env overrides", () => {
  const config = buildDefaultProvisionConfig(vi.fn(), {
    ODDS_R2_CATALOG_BUCKET: "bucket2",
    ODDS_R2_CATALOG_NAMESPACE: "namespace2",
    ODDS_R2_CATALOG_PIPELINE_NAME: "pipeline2",
    ODDS_R2_CATALOG_PIPELINE_SQL_FILE: "pipeline.sql",
    ODDS_R2_CATALOG_ROLL_INTERVAL_SECONDS: "120",
    ODDS_R2_CATALOG_SINK_NAME: "sink2",
    ODDS_R2_CATALOG_STREAM_NAME: "stream2",
    ODDS_R2_CATALOG_TABLE: "table2",
    ODDS_R2_CATALOG_TOKEN: "catalog-token",
  });
  expect(config.bucket).toBe("bucket2");
  expect(config.catalogToken).toBe("catalog-token");
  expect(config.namespace).toBe("namespace2");
  expect(config.pipelineName).toBe("pipeline2");
  expect(config.pipelineSqlFile).toBe("pipeline.sql");
  expect(config.rollIntervalSeconds).toBe("120");
  expect(config.sinkName).toBe("sink2");
  expect(config.streamName).toBe("stream2");
  expect(config.table).toBe("table2");
});

it("buildDefaultProvisionConfig falls back to WRANGLER_R2_SQL_AUTH_TOKEN then defaults", () => {
  const config = buildDefaultProvisionConfig(vi.fn(), { WRANGLER_R2_SQL_AUTH_TOKEN: "sql-token" });
  expect(config.catalogToken).toBe("sql-token");
  expect(config.bucket).toBe("pc-keiba-odds-archive");
  expect(config.namespace).toBe("odds");
  expect(config.pipelineSqlFile.endsWith("/pipelines/odds-catalog-pipeline.sql")).toBe(true);
});

it("buildDefaultProvisionConfig falls back to R2_API_TOKEN when catalog token is absent", () => {
  const config = buildDefaultProvisionConfig(vi.fn(), { R2_API_TOKEN: "r2-token" });
  expect(config.catalogToken).toBe("r2-token");
});

it("commandIncludes returns true only when command succeeds and output contains expected text", async () => {
  const commandImpl = vi.fn(async () => buildCommandResult({ stdout: "needle" }));
  await expect(commandIncludes(commandImpl, ["cmd"], "needle")).resolves.toBe(true);
});

it("commandIncludes returns false on command error output", async () => {
  const commandImpl = vi.fn(async () => buildCommandResult({ stdout: "ERROR needle" }));
  await expect(commandIncludes(commandImpl, ["cmd"], "needle")).resolves.toBe(false);
});

it("ensureCatalogActive passes when catalog is active", async () => {
  const commandImpl = vi.fn(async () => buildCommandResult({ stdout: "Status:       active" }));
  const check = await ensureCatalogActive(buildConfig({ commandImpl }));
  expect(check).toStrictEqual({
    detail: "bucket=pc-keiba-odds-archive",
    name: "r2 catalog active",
    ok: true,
  });
});

it("ensureCatalogActive fails when command output is not active", async () => {
  const commandImpl = vi.fn(async () => buildCommandResult({ stdout: "Status:       disabled" }));
  const check = await ensureCatalogActive(buildConfig({ commandImpl }));
  expect(check.ok).toBe(false);
});

it("ensureStreamExists passes when stream is listed", async () => {
  const commandImpl = vi.fn(async () =>
    buildCommandResult({ stdout: "odds_snapshots_hot_stream" }),
  );
  const check = await ensureStreamExists(buildConfig({ commandImpl }));
  expect(check).toStrictEqual({
    detail: "stream=odds_snapshots_hot_stream",
    name: "pipeline stream exists",
    ok: true,
  });
});

it("ensureStreamExists fails when stream is missing", async () => {
  const commandImpl = vi.fn(async () => buildCommandResult({ stdout: "other_stream" }));
  const check = await ensureStreamExists(buildConfig({ commandImpl }));
  expect(check).toStrictEqual({
    detail: "missing stream=odds_snapshots_hot_stream",
    name: "pipeline stream exists",
    ok: false,
  });
});

it("ensureSinkExists skips creation when sink already exists", async () => {
  const commandImpl = vi.fn(async () => buildCommandResult({ stdout: "odds_snapshots_hot_sink" }));
  const check = await ensureSinkExists(buildConfig({ commandImpl }));
  expect(check).toStrictEqual({
    detail: "sink=odds_snapshots_hot_sink",
    name: "pipeline sink exists",
    ok: true,
  });
  expect(commandImpl).toHaveBeenCalledTimes(1);
});

it("ensureSinkExists fails without catalog token when sink is missing", async () => {
  const commandImpl = vi.fn(async () => buildCommandResult({ stdout: "other_sink" }));
  const check = await ensureSinkExists(buildConfig({ catalogToken: undefined, commandImpl }));
  expect(check).toStrictEqual({
    detail: "missing ODDS_R2_CATALOG_TOKEN",
    name: "pipeline sink exists",
    ok: false,
  });
});

it("ensureSinkExists creates missing sink with catalog settings", async () => {
  const commandImpl = vi
    .fn()
    .mockResolvedValueOnce(buildCommandResult({ stdout: "other_sink" }))
    .mockResolvedValueOnce(buildCommandResult({ stdout: "created" }));
  const check = await ensureSinkExists(buildConfig({ commandImpl }));
  expect(check).toStrictEqual({
    detail: "created sink=odds_snapshots_hot_sink",
    name: "pipeline sink exists",
    ok: true,
  });
  expect(commandImpl).toHaveBeenLastCalledWith([
    "bunx",
    "wrangler",
    "pipelines",
    "sinks",
    "create",
    "odds_snapshots_hot_sink",
    "--type",
    "r2-data-catalog",
    "--bucket",
    "pc-keiba-odds-archive",
    "--namespace",
    "odds",
    "--table",
    "snapshots_hot",
    "--catalog-token",
    "token",
    "--roll-interval",
    "60",
  ]);
});

it("ensureSinkExists surfaces unauthorized creation output", async () => {
  const commandImpl = vi
    .fn()
    .mockResolvedValueOnce(buildCommandResult({ stdout: "other_sink" }))
    .mockResolvedValueOnce(buildCommandResult({ code: 1, stderr: "code: 1012" }));
  const check = await ensureSinkExists(buildConfig({ commandImpl }));
  expect(check).toStrictEqual({
    detail: "code: 1012",
    name: "pipeline sink exists",
    ok: false,
  });
});

it("ensurePipelineExists skips creation when pipeline already exists", async () => {
  const commandImpl = vi.fn(async () =>
    buildCommandResult({ stdout: "odds_snapshots_hot_pipeline" }),
  );
  const check = await ensurePipelineExists(buildConfig({ commandImpl }));
  expect(check).toStrictEqual({
    detail: "pipeline=odds_snapshots_hot_pipeline",
    name: "pipeline exists",
    ok: true,
  });
  expect(commandImpl).toHaveBeenCalledTimes(1);
});

it("ensurePipelineExists creates missing pipeline from sql file", async () => {
  const commandImpl = vi
    .fn()
    .mockResolvedValueOnce(buildCommandResult({ stdout: "other_pipeline" }))
    .mockResolvedValueOnce(buildCommandResult({ stdout: "created" }));
  const check = await ensurePipelineExists(buildConfig({ commandImpl }));
  expect(check).toStrictEqual({
    detail: "created pipeline=odds_snapshots_hot_pipeline",
    name: "pipeline exists",
    ok: true,
  });
  expect(commandImpl).toHaveBeenLastCalledWith([
    "bunx",
    "wrangler",
    "pipelines",
    "create",
    "odds_snapshots_hot_pipeline",
    "--sql-file",
    "apps/sync-realtime-data-hot/pipelines/odds-catalog-pipeline.sql",
  ]);
});

it("provisionOddsR2Catalog creates pipeline only when sink is available", async () => {
  const commandImpl = vi.fn(async (args: string[]) => {
    const joined = args.join(" ");
    if (joined.includes("catalog get"))
      return buildCommandResult({ stdout: "Status:       active" });
    if (joined.includes("streams list"))
      return buildCommandResult({ stdout: "odds_snapshots_hot_stream" });
    if (joined.includes("sinks list")) return buildCommandResult({ stdout: "other_sink" });
    if (joined.includes("sinks create")) return buildCommandResult({ stdout: "created sink" });
    if (joined.includes("pipelines list")) return buildCommandResult({ stdout: "other_pipeline" });
    if (joined.includes("pipelines create"))
      return buildCommandResult({ stdout: "created pipeline" });
    return buildCommandResult();
  });
  const result = await provisionOddsR2Catalog(buildConfig({ commandImpl }));
  expect(result.ok).toBe(true);
  expect(result.checks.map((check) => check.ok)).toStrictEqual([true, true, true, true]);
});

it("provisionOddsR2Catalog skips pipeline creation when sink creation fails", async () => {
  const commandImpl = vi.fn(async (args: string[]) => {
    const joined = args.join(" ");
    if (joined.includes("catalog get"))
      return buildCommandResult({ stdout: "Status:       active" });
    if (joined.includes("streams list"))
      return buildCommandResult({ stdout: "odds_snapshots_hot_stream" });
    if (joined.includes("sinks list")) return buildCommandResult({ stdout: "other_sink" });
    if (joined.includes("sinks create"))
      return buildCommandResult({ code: 1, stderr: "Unauthorized" });
    return buildCommandResult();
  });
  const result = await provisionOddsR2Catalog(buildConfig({ commandImpl }));
  expect(result.ok).toBe(false);
  expect(result.checks.at(-1)).toStrictEqual({
    detail: "skipped because sink is unavailable",
    name: "pipeline exists",
    ok: false,
  });
});
