import { formatRaceTimeTenths as formatCalculatedRaceTime } from "../../../lib/race-time";
import type { RaceTimeStats } from "../../../lib/race-types";

interface RaceTimeStatsMetric {
  label: string;
  value: string;
}

export const formatRaceTimeTenths = (value: number | null): string =>
  formatCalculatedRaceTime(value);

export const formatRaceTimeDecimalTenths = (value: number | null): string =>
  value === null ? "-" : (value / 10).toFixed(1);

const buildRaceTimeStatsMetrics = (stats: RaceTimeStats): RaceTimeStatsMetric[] => [
  { label: "最速レースタイム", value: formatRaceTimeTenths(stats.fastestRaceTime) },
  { label: "最速上がり3F", value: formatRaceTimeDecimalTenths(stats.fastestKohan3f) },
  { label: "平均レースタイム", value: formatRaceTimeTenths(stats.averageRaceTime) },
  { label: "平均上がり3F", value: formatRaceTimeDecimalTenths(stats.averageKohan3f) },
  { label: "中央値レースタイム", value: formatRaceTimeTenths(stats.medianRaceTime) },
  { label: "中央値上がり3F", value: formatRaceTimeDecimalTenths(stats.medianKohan3f) },
];

export function RaceTimeStatsMetrics({ stats }: { stats: RaceTimeStats }) {
  return (
    <div aria-label="タイム傾向" className="race-time-stats-metrics">
      {buildRaceTimeStatsMetrics(stats).map((metric) => (
        <div key={metric.label}>
          <span>{metric.label}</span>
          <strong>{metric.value}</strong>
        </div>
      ))}
    </div>
  );
}
