// Run with: imported by race-detail-page.tsx (Next.js async server component)

import {
  buildRaceKey,
  getRaceRunningStylesFromD1,
  getRunningStyleMetricsForActiveModel,
} from "../../../db/corner-running-style-queries";
import { RunningStyleSection } from "./running-style-section";

interface RunningStyleRaceSectionProps {
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  category: string;
}

export const RunningStyleRaceSection = async ({
  source,
  kaisaiNen,
  kaisaiTsukihi,
  keibajoCode,
  raceBango,
  category,
}: RunningStyleRaceSectionProps) => {
  if (category === "ban-ei") return null;
  const raceKey = buildRaceKey({
    kaisaiNen,
    kaisaiTsukihi,
    keibajoCode,
    raceBango,
    source,
  });
  const [rows, metrics] = await Promise.all([
    getRaceRunningStylesFromD1(raceKey).catch(() => []),
    getRunningStyleMetricsForActiveModel(category).catch(() => null),
  ]);
  return (
    <RunningStyleSection
      modelMacroF1={metrics?.macroF1 ?? null}
      modelVersion={metrics?.modelVersion ?? null}
      rows={rows}
    />
  );
};
