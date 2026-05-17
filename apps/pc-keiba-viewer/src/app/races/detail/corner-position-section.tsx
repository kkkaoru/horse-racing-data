// Run with: rendered by the race detail page (Next.js server component)

import type { RaceCornerPositionRow } from "../../../db/corner-running-style-queries";

const STRAIGHT_COURSE_BANNER = "直線コース（コーナーなし、参考値）";

interface CornerPositionSectionProps {
  rows: RaceCornerPositionRow[];
  meanMae: number | null;
  modelVersion: string | null;
  isStraightCourse: boolean;
  bameiByUmaban: Record<number, string | null>;
}

const formatRank = (value: number | null, total: number): string => {
  if (value === null) return "-";
  if (total <= 1) return "-";
  const denominator = total - 1;
  const rank = value * denominator + 1;
  return rank.toFixed(1);
};

const formatPercent = (value: number | null): string => {
  if (value === null) return "-";
  const percent = value * 100;
  return `${percent.toFixed(0)}%`;
};

const horseNumberDisplay = (umaban: number): string => `${umaban}番`;

interface CornerPositionTableProps {
  rows: RaceCornerPositionRow[];
  bameiByUmaban: Record<number, string | null>;
}

const CornerPositionTable = ({ rows, bameiByUmaban }: CornerPositionTableProps) => {
  const sortedRows = rows.toSorted((a, b) => a.umaban - b.umaban);
  const total = sortedRows.length;
  return (
    <table className="corner-position-table">
      <thead>
        <tr>
          <th scope="col">馬番</th>
          <th scope="col">馬名</th>
          <th scope="col">1コーナー</th>
          <th scope="col">3コーナー</th>
          <th scope="col">4コーナー</th>
          <th scope="col">1コーナー位置</th>
        </tr>
      </thead>
      <tbody>
        {sortedRows.map((row) => (
          <tr key={`${row.source}-${row.kaisaiNen}-${row.kaisaiTsukihi}-${row.keibajoCode}-${row.raceBango}-${row.kettoTorokuBango}`}>
            <td>{horseNumberDisplay(row.umaban)}</td>
            <td>{bameiByUmaban[row.umaban] ?? "馬名不明"}</td>
            <td>{formatRank(row.corner1Pred, total)}</td>
            <td>{formatRank(row.corner3Pred, total)}</td>
            <td>{formatRank(row.corner4Pred, total)}</td>
            <td>{formatPercent(row.corner1Pred)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
};

interface MetricsBadgeProps {
  meanMae: number | null;
  modelVersion: string | null;
}

const MetricsBadge = ({ meanMae, modelVersion }: MetricsBadgeProps) => {
  if (modelVersion === null) return null;
  return (
    <p className="corner-position-metrics">
      モデル: {modelVersion}
      {meanMae !== null ? `（平均 MAE: ${meanMae.toFixed(3)}）` : null}
    </p>
  );
};

export const CornerPositionSection = ({
  rows,
  meanMae,
  modelVersion,
  isStraightCourse,
  bameiByUmaban,
}: CornerPositionSectionProps) => {
  if (isStraightCourse) {
    return (
      <section className="corner-position-section" aria-label="コーナー通過順予測">
        <h2>コーナー通過順予測</h2>
        <p className="corner-position-banner">{STRAIGHT_COURSE_BANNER}</p>
      </section>
    );
  }

  if (rows.length === 0) {
    return (
      <section className="corner-position-section" aria-label="コーナー通過順予測">
        <h2>コーナー通過順予測</h2>
        <p className="corner-position-empty">このレースのコーナー予測データはまだありません。</p>
      </section>
    );
  }

  return (
    <section className="corner-position-section" aria-label="コーナー通過順予測">
      <h2>コーナー通過順予測</h2>
      <CornerPositionTable rows={rows} bameiByUmaban={bameiByUmaban} />
      <MetricsBadge meanMae={meanMae} modelVersion={modelVersion} />
    </section>
  );
};

export { formatPercent, formatRank, STRAIGHT_COURSE_BANNER };
