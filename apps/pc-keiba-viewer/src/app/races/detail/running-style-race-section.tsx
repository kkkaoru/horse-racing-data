// Run with: imported by race-detail-page.tsx (Next.js async server component)

import { getRunningStyleMetricsForActiveModel } from "../../../db/corner-running-style-queries";
import type { RaceSource } from "../../../lib/codes";
import { getRaceRunningStylesWithCache } from "../../../lib/running-style-cache.server";
import {
  getRunningStyleBucketSectionData,
  type RunningStyleBucketSectionData,
} from "./detail-section-data";
import { RunningStyleSection, type RunnerDisplayInfo } from "./running-style-section";

interface RunningStyleRaceSectionProps {
  source: RaceSource;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  category: string;
  runnersByUmaban: Record<number, RunnerDisplayInfo>;
  searchParams: Record<string, string | string[] | undefined>;
  day: string;
  month: string;
  raceNumber: string;
  year: string;
}

const EMPTY_BUCKET_SECTION_DATA: RunningStyleBucketSectionData = {
  bucketEvaluation: null,
  bucketGradeCode: null,
  bucketRace: null,
  bucketScope: null,
  bucketSource: null,
  dimensionFlags: null,
};

export const RunningStyleRaceSection = async ({
  source,
  kaisaiNen,
  kaisaiTsukihi,
  keibajoCode,
  raceBango,
  category,
  runnersByUmaban,
  searchParams,
  day,
  month,
  raceNumber,
  year,
}: RunningStyleRaceSectionProps) => {
  const metricsCategory = category === "ban-ei" ? "nar" : category;
  const [rows, metrics, bucketData] = await Promise.all([
    getRaceRunningStylesWithCache({
      kaisaiNen,
      kaisaiTsukihi,
      keibajoCode,
      raceBango,
      source,
    }).catch(() => []),
    getRunningStyleMetricsForActiveModel(metricsCategory).catch(() => null),
    getRunningStyleBucketSectionData({
      day,
      keibajoCode,
      month,
      query: searchParams,
      raceNumber,
      raceSource: source,
      year,
    }).catch(() => EMPTY_BUCKET_SECTION_DATA),
  ]);
  return (
    <RunningStyleSection
      bucketEvaluation={bucketData.bucketEvaluation}
      bucketGradeCode={bucketData.bucketGradeCode}
      bucketRace={bucketData.bucketRace}
      bucketScope={bucketData.bucketScope}
      bucketSource={bucketData.bucketSource}
      dimensionFlags={bucketData.dimensionFlags}
      modelMacroF1={metrics?.macroF1 ?? null}
      modelVersion={metrics?.modelVersion ?? null}
      rows={rows}
      runnersByUmaban={runnersByUmaban}
    />
  );
};
