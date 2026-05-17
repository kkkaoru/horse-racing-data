// Run with bun. Cron entrypoint that pulls the day's per-horse feature
// JSONL + the active LightGBM JSON from R2, runs JS-side inference, and
// upserts predictions into D1. Gated by RUNNING_STYLE_D1_WRITE_ENABLED so
// the schedule can stay armed in dev/preview without touching the live
// race_running_styles table.

import { runRunningStyleInference } from "./running-style-inference";
import type { Env } from "./types";

export const RUNNING_STYLE_INFERENCE_CRON = "*/10 * * * *";

const SOURCES = ["nar", "jra"] as const;
const ENABLED_FLAG = "1";
const DATE_PAD_WIDTH = 2;

type Source = (typeof SOURCES)[number];

interface SourceSummary {
  source: Source;
  status: "wrote" | "skipped" | "failed";
  raceCount?: number;
  horseCount?: number;
  writtenCount?: number;
  message?: string;
}

const padDatePart = (value: number): string => String(value).padStart(DATE_PAD_WIDTH, "0");

const formatYYYYMMDDInJst = (now: Date): string => {
  const utcMillis = now.getTime();
  const jstOffsetMinutes = 9 * 60;
  const jst = new Date(utcMillis + jstOffsetMinutes * 60 * 1000);
  return `${jst.getUTCFullYear()}${padDatePart(jst.getUTCMonth() + 1)}${padDatePart(jst.getUTCDate())}`;
};

const buildModelKey = (source: Source): string => `running-style/models/${source}/latest.json`;
const buildFeaturesKey = (source: Source, date: string): string =>
  `running-style/features/${source}/${date}.jsonl`;

const isInferenceEnabled = (env: Env): boolean => env.RUNNING_STYLE_D1_WRITE_ENABLED === ENABLED_FLAG;

const runForSource = async (env: Env, source: Source, predictedAt: string, date: string): Promise<SourceSummary> => {
  try {
    const summary = await runRunningStyleInference(
      env.RUNNING_STYLE_MODELS,
      env.REALTIME_DB,
      {
        featuresKey: buildFeaturesKey(source, date),
        modelKey: buildModelKey(source),
        predictedAt,
      },
    );
    return {
      horseCount: summary.horseCount,
      raceCount: summary.raceCount,
      source,
      status: "wrote",
      writtenCount: summary.writtenCount,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return { message, source, status: "failed" };
  }
};

export const runRunningStyleCronTick = async (env: Env, now: Date): Promise<SourceSummary[]> => {
  if (!isInferenceEnabled(env)) {
    return SOURCES.map((source): SourceSummary => ({ source, status: "skipped" }));
  }
  const predictedAt = now.toISOString();
  const date = formatYYYYMMDDInJst(now);
  return Promise.all(SOURCES.map((source) => runForSource(env, source, predictedAt, date)));
};
