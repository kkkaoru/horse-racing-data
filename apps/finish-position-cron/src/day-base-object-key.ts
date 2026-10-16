// Run with bun. Stable legacy and immutable generation-scoped day-base keys.
import type { PredictCategory } from "./types";

const R2_DAY_BASE_PREFIX = "feat-daybase";
const R2_DAY_BASE_GENERATION = "catalog-v1";
const R2_DAY_BASE_FILE = "features.parquet";
const GENERATION_ID_PATTERN = /^[A-Za-z0-9_-]{1,128}$/u;

interface BuildDayBaseObjectKeyParams {
  category: PredictCategory;
  generationId?: string;
  runYmd: string;
}

export const buildDayBaseObjectKey = (params: BuildDayBaseObjectKeyParams): string => {
  if (params.generationId !== undefined && !GENERATION_ID_PATTERN.test(params.generationId))
    throw new Error("invalid day-base generation id");
  const prefix = `${R2_DAY_BASE_PREFIX}/${R2_DAY_BASE_GENERATION}/${params.category}/${params.runYmd}`;
  return params.generationId === undefined
    ? `${prefix}/${R2_DAY_BASE_FILE}`
    : `${prefix}/generations/${params.generationId}/${R2_DAY_BASE_FILE}`;
};
