import type { RaceTimeStats } from "../../../lib/race-types";

interface RaceTimeStatsMetric {
  label: string;
  value: string;
}

export const formatRaceTimeTenths = (value: number | null): string => {
  if (value === null) {
    return "-";
  }
  const tenths = Math.round(value);
  const minutes = Math.floor(tenths / 600);
  const seconds = Math.floor((tenths % 600) / 10);
  const remainder = tenths % 10;
  return minutes > 0
    ? `${minutes}:${String(seconds).padStart(2, "0")}.${remainder}`
    : `${seconds}.${remainder}`;
};

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
