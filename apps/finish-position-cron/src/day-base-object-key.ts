import type { PredictCategory } from "./types";

const R2_DAY_BASE_PREFIX = "feat-daybase";
const R2_DAY_BASE_GENERATION = "catalog-v1";
const R2_DAY_BASE_FILE = "features.parquet";

interface BuildDayBaseObjectKeyParams {
  category: PredictCategory;
  runYmd: string;
}

export const buildDayBaseObjectKey = (params: BuildDayBaseObjectKeyParams): string =>
  `${R2_DAY_BASE_PREFIX}/${R2_DAY_BASE_GENERATION}/${params.category}/${params.runYmd}/${R2_DAY_BASE_FILE}`;
