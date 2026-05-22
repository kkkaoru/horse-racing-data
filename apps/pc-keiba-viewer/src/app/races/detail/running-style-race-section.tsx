// Run with: imported by race-detail-page.tsx (Next.js async server component)

import { getRunningStyleMetricsForActiveModel } from "../../../db/corner-running-style-queries";
import { getRaceRunningStylesWithCache } from "../../../lib/running-style-cache.server";
import { RunningStyleSection, type RunnerDisplayInfo } from "./running-style-section";

interface RunningStyleRaceSectionProps {
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  category: string;
  runnersByUmaban: Record<number, RunnerDisplayInfo>;
}

export const RunningStyleRaceSection = async ({
  source,
  kaisaiNen,
  kaisaiTsukihi,
  keibajoCode,
  raceBango,
  category,
  runnersByUmaban,
}: RunningStyleRaceSectionProps) => {
  const metricsCategory = category === "ban-ei" ? "nar" : category;
  const [rows, metrics] = await Promise.all([
    getRaceRunningStylesWithCache({
      kaisaiNen,
      kaisaiTsukihi,
      keibajoCode,
      raceBango,
      source,
    }).catch(() => []),
    getRunningStyleMetricsForActiveModel(metricsCategory).catch(() => null),
  ]);
  return (
    <RunningStyleSection
      modelMacroF1={metrics?.macroF1 ?? null}
      modelVersion={metrics?.modelVersion ?? null}
      rows={rows}
      runnersByUmaban={runnersByUmaban}
    />
  );
};
