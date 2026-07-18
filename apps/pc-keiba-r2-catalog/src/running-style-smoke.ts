import { executeR2Sql } from "./r2-sql";
import { buildRunningStyleExplainQuery, buildRunningStyleFeaturesQuery } from "./running-style-sql";
import type {
  Fetcher,
  R2SqlCatalogConfig,
  RunningStyleFeatureFilters,
  RunningStyleSourceScope,
} from "./types";

const parseSource = (value: string): RunningStyleSourceScope => {
  if (value === "jra" || value === "nar" || value === "ban-ei") return value;
  throw new Error("source must be jra, nar, or ban-ei");
};

const parseCode = (value: string | undefined, name: string): string => {
  if (value === undefined || !/^\d{1,2}$/u.test(value)) {
    throw new Error(`${name} must contain one or two digits`);
  }
  return value.padStart(2, "0");
};

export const parseRunningStyleSmokeArgs = (args: string[]): RunningStyleFeatureFilters => {
  const [date, source, keibajoCode, raceBango] = args;
  if (date === undefined || source === undefined) {
    throw new Error("usage: smoke:running-style YYYYMMDD jra|nar|ban-ei keibajoCode raceBango");
  }
  return {
    date,
    keibajoCode: parseCode(keibajoCode, "keibajoCode"),
    raceBango: parseCode(raceBango, "raceBango"),
    source: parseSource(source),
  };
};

export interface RunningStyleSmokeResult {
  explainRows: number;
  queryRows: number;
}

export const runRunningStyleSmoke = async (
  config: R2SqlCatalogConfig,
  filters: RunningStyleFeatureFilters,
  fetchImpl: Fetcher,
  includeOrderBy: boolean,
): Promise<RunningStyleSmokeResult> => {
  const explain = await executeR2Sql(
    config,
    buildRunningStyleExplainQuery(config, filters, includeOrderBy),
    fetchImpl,
  );
  const rows = await executeR2Sql(
    config,
    buildRunningStyleFeaturesQuery(config, filters, includeOrderBy),
    fetchImpl,
  );
  return { explainRows: explain.length, queryRows: rows.length };
};
