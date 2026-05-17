// Run with: rendered by the race detail page (Next.js client component)

"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useMemo } from "react";

import type {
  RaceRunningStyleRow,
  RunningStyleLabel,
} from "../../../db/corner-running-style-parsers";

const STYLE_TAB_VALUES = ["all", "nige", "senkou", "sashi", "oikomi"] as const;
type StyleTab = (typeof STYLE_TAB_VALUES)[number];

const STYLE_TAB_LABELS: Record<StyleTab, string> = {
  all: "全体",
  nige: "逃げ",
  senkou: "先行",
  sashi: "差し",
  oikomi: "追い込み",
};

const RUNNING_STYLE_DISPLAY: Record<RunningStyleLabel, string> = {
  nige: "逃げ",
  senkou: "先行",
  sashi: "差し",
  oikomi: "追い込み",
};

const TOP_HIGHLIGHT_COUNT = 3;
const PERCENT_DECIMALS = 0;
const SEARCH_PARAM_KEY = "style";

interface RunningStyleSectionProps {
  rows: RaceRunningStyleRow[];
  modelMacroF1: number | null;
  modelVersion: string | null;
}

const formatPercent = (value: number): string => `${(value * 100).toFixed(PERCENT_DECIMALS)}%`;

const probabilityForTab = (row: RaceRunningStyleRow, tab: Exclude<StyleTab, "all">): number => {
  if (tab === "nige") return row.p_nige;
  if (tab === "senkou") return row.p_senkou;
  if (tab === "sashi") return row.p_sashi;
  return row.p_oikomi;
};

const isStyleTab = (value: string): value is StyleTab => {
  for (const candidate of STYLE_TAB_VALUES) {
    if (candidate === value) return true;
  }
  return false;
};

const resolveCurrentTab = (raw: string | null): StyleTab => {
  if (raw === null) return "all";
  return isStyleTab(raw) ? raw : "all";
};

const sortRowsByTab = (rows: RaceRunningStyleRow[], tab: StyleTab): RaceRunningStyleRow[] => {
  if (tab === "all") {
    return rows.toSorted((a, b) => a.horseNumber - b.horseNumber);
  }
  return rows.toSorted((a, b) => probabilityForTab(b, tab) - probabilityForTab(a, tab));
};

const horseNumberDisplay = (horseNumber: number): string => `${horseNumber}番`;

const explicitChipText = (row: RaceRunningStyleRow, tab: Exclude<StyleTab, "all">): string => {
  const name = row.bamei ?? "馬名不明";
  const styleName = STYLE_TAB_LABELS[tab];
  return `${horseNumberDisplay(row.horseNumber)} ${name} ${styleName} ${formatPercent(probabilityForTab(row, tab))}`;
};

interface AllRowsTableProps {
  rows: RaceRunningStyleRow[];
}

const AllRowsTable = ({ rows }: AllRowsTableProps) => (
  <table className="running-style-table">
    <thead>
      <tr>
        <th scope="col">馬番</th>
        <th scope="col">馬名</th>
        <th scope="col">逃げ</th>
        <th scope="col">先行</th>
        <th scope="col">差し</th>
        <th scope="col">追い込み</th>
        <th scope="col">予測ラベル</th>
      </tr>
    </thead>
    <tbody>
      {rows.map((row) => (
        <tr key={`${row.raceKey}-${row.horseNumber}`}>
          <td>{horseNumberDisplay(row.horseNumber)}</td>
          <td>{row.bamei ?? "馬名不明"}</td>
          <td>{formatPercent(row.p_nige)}</td>
          <td>{formatPercent(row.p_senkou)}</td>
          <td>{formatPercent(row.p_sashi)}</td>
          <td>{formatPercent(row.p_oikomi)}</td>
          <td>{RUNNING_STYLE_DISPLAY[row.predictedLabel]}</td>
        </tr>
      ))}
    </tbody>
  </table>
);

interface FocusedListProps {
  rows: RaceRunningStyleRow[];
  tab: Exclude<StyleTab, "all">;
}

const FocusedList = ({ rows, tab }: FocusedListProps) => (
  <ol className="running-style-focus-list">
    {rows.map((row, index) => (
      <li
        key={`${row.raceKey}-${row.horseNumber}`}
        className={
          index < TOP_HIGHLIGHT_COUNT ? "running-style-focus-chip top" : "running-style-focus-chip"
        }
        aria-current={index < TOP_HIGHLIGHT_COUNT ? "true" : undefined}
      >
        {explicitChipText(row, tab)}
      </li>
    ))}
  </ol>
);

interface TabButtonsProps {
  currentTab: StyleTab;
  onSelect: (tab: StyleTab) => void;
}

const TabButtons = ({ currentTab, onSelect }: TabButtonsProps) => (
  <div className="running-style-tabs" role="tablist" aria-label="脚質フォーカス">
    {STYLE_TAB_VALUES.map((tab) => (
      <button
        type="button"
        key={tab}
        role="tab"
        aria-selected={currentTab === tab}
        className={currentTab === tab ? "running-style-tab active" : "running-style-tab"}
        onClick={() => {
          onSelect(tab);
        }}
      >
        {STYLE_TAB_LABELS[tab]}
      </button>
    ))}
  </div>
);

const MetricsBadge = ({
  modelMacroF1,
  modelVersion,
}: {
  modelMacroF1: number | null;
  modelVersion: string | null;
}) => {
  if (modelVersion === null) return null;
  return (
    <p className="running-style-metrics">
      モデル: {modelVersion}
      {modelMacroF1 !== null ? `（macro-F1: ${modelMacroF1.toFixed(3)}）` : null}
    </p>
  );
};

export const RunningStyleSection = ({
  rows,
  modelMacroF1,
  modelVersion,
}: RunningStyleSectionProps) => {
  const router = useRouter();
  const searchParams = useSearchParams();
  const currentTab = resolveCurrentTab(searchParams?.get(SEARCH_PARAM_KEY) ?? null);
  const visibleRows = useMemo(() => sortRowsByTab(rows, currentTab), [rows, currentTab]);

  const handleSelect = (tab: StyleTab): void => {
    const params = new URLSearchParams(searchParams?.toString() ?? "");
    if (tab === "all") {
      params.delete(SEARCH_PARAM_KEY);
    } else {
      params.set(SEARCH_PARAM_KEY, tab);
    }
    const query = params.toString();
    router.replace(query === "" ? "?" : `?${query}`, { scroll: false });
  };

  if (rows.length === 0) {
    return (
      <section className="running-style-section" aria-label="脚質予測">
        <h2>脚質予測</h2>
        <p className="running-style-empty">このレースの脚質予測データはまだありません。</p>
      </section>
    );
  }

  return (
    <section className="running-style-section" aria-label="脚質予測">
      <h2>脚質予測</h2>
      <TabButtons currentTab={currentTab} onSelect={handleSelect} />
      {currentTab === "all" ? (
        <AllRowsTable rows={visibleRows} />
      ) : (
        <FocusedList rows={visibleRows} tab={currentTab} />
      )}
      <MetricsBadge modelMacroF1={modelMacroF1} modelVersion={modelVersion} />
    </section>
  );
};

export { STYLE_TAB_VALUES };
export type { StyleTab };
