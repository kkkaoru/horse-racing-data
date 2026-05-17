// Run with: imported by race-detail-page.tsx (Next.js async server component)

import {
  getActiveCornerPositionModel,
  getCornerPositionMetricsForActiveModel,
  getRaceCornerPositionPredictions,
} from "../../../db/corner-running-style-queries";
import { CornerPositionSection } from "./corner-position-section";

interface CornerPositionRaceSectionProps {
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  category: string;
  isStraightCourse: boolean;
  bameiByUmaban: Record<number, string | null>;
}

const buildEmptyProps = (
  isStraightCourse: boolean,
  bameiByUmaban: Record<number, string | null>,
) => ({
  bameiByUmaban,
  isStraightCourse,
  meanMae: null,
  modelVersion: null,
  rows: [],
});

export const CornerPositionRaceSection = async ({
  source,
  kaisaiNen,
  kaisaiTsukihi,
  keibajoCode,
  raceBango,
  category,
  isStraightCourse,
  bameiByUmaban,
}: CornerPositionRaceSectionProps) => {
  if (category === "ban-ei") return null;
  if (isStraightCourse) {
    return <CornerPositionSection {...buildEmptyProps(true, bameiByUmaban)} />;
  }
  const active = await getActiveCornerPositionModel(category).catch(() => null);
  if (active === null) {
    return <CornerPositionSection {...buildEmptyProps(false, bameiByUmaban)} />;
  }
  const [rows, metrics] = await Promise.all([
    getRaceCornerPositionPredictions(
      { kaisaiNen, kaisaiTsukihi, keibajoCode, raceBango, source },
      active.modelVersion,
    ).catch(() => []),
    getCornerPositionMetricsForActiveModel(category).catch(() => null),
  ]);
  return (
    <CornerPositionSection
      bameiByUmaban={bameiByUmaban}
      isStraightCourse={false}
      meanMae={metrics?.meanMae ?? null}
      modelVersion={active.modelVersion}
      rows={rows}
    />
  );
};
