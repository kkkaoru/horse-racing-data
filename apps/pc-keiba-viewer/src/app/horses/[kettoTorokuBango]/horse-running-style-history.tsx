// Run with: rendered by the horse detail page (Next.js server component)

import type {
  RaceRunningStyleRow,
  RunningStyleLabel,
} from "../../../db/corner-running-style-parsers";

const STYLE_DISPLAY: Record<RunningStyleLabel, string> = {
  nige: "逃げ",
  senkou: "先行",
  sashi: "差し",
  oikomi: "追い込み",
};

const STYLE_CHIP_CLASS: Record<RunningStyleLabel, string> = {
  nige: "running-style-chip nige",
  senkou: "running-style-chip senkou",
  sashi: "running-style-chip sashi",
  oikomi: "running-style-chip oikomi",
};

const DATE_SLICE_END = 10;

interface HorseRunningStyleHistoryProps {
  rows: RaceRunningStyleRow[];
}

const formatRaceLabel = (row: RaceRunningStyleRow): string => {
  const date = row.predictedAt.slice(0, DATE_SLICE_END);
  return `${date} ${row.raceKey}`;
};

const chipText = (row: RaceRunningStyleRow): string => {
  const label = STYLE_DISPLAY[row.predictedLabel];
  return `${formatRaceLabel(row)} ${label}`;
};

export const HorseRunningStyleHistory = ({ rows }: HorseRunningStyleHistoryProps) => {
  if (rows.length === 0) {
    return (
      <section className="horse-running-style-history" aria-label="脚質履歴">
        <h2>脚質履歴</h2>
        <p className="horse-running-style-empty">この馬の脚質予測履歴はまだありません。</p>
      </section>
    );
  }

  return (
    <section className="horse-running-style-history" aria-label="脚質履歴">
      <h2>脚質履歴</h2>
      <ol className="horse-running-style-chip-list">
        {rows.map((row) => (
          <li
            key={`${row.raceKey}-${row.horseNumber}`}
            className={STYLE_CHIP_CLASS[row.predictedLabel]}
          >
            {chipText(row)}
          </li>
        ))}
      </ol>
    </section>
  );
};

export { chipText, formatRaceLabel, STYLE_DISPLAY };
