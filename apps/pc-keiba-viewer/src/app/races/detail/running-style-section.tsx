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
const MISSING_NAME_PLACEHOLDER = "馬名不明";
const MISSING_JOCKEY_PLACEHOLDER = "騎手不明";

interface RunnerDisplayInfo {
  bamei: string | null;
  jockey: string | null;
}

interface RunningStyleSectionProps {
  rows: RaceRunningStyleRow[];
  modelMacroF1: number | null;
  modelVersion: string | null;
  runnersByUmaban: Record<number, RunnerDisplayInfo>;
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

const resolveBamei = (
  row: RaceRunningStyleRow,
  runners: Record<number, RunnerDisplayInfo>,
): string => row.bamei ?? runners[row.horseNumber]?.bamei ?? MISSING_NAME_PLACEHOLDER;

const resolveJockey = (
  row: RaceRunningStyleRow,
  runners: Record<number, RunnerDisplayInfo>,
): string => runners[row.horseNumber]?.jockey ?? MISSING_JOCKEY_PLACEHOLDER;

const explicitChipText = (
  row: RaceRunningStyleRow,
  tab: Exclude<StyleTab, "all">,
  runners: Record<number, RunnerDisplayInfo>,
): string => {
  const styleName = STYLE_TAB_LABELS[tab];
  return `${horseNumberDisplay(row.horseNumber)} ${resolveBamei(row, runners)} ${styleName} ${formatPercent(probabilityForTab(row, tab))}`;
};

interface AllRowsTableProps {
  rows: RaceRunningStyleRow[];
  runnersByUmaban: Record<number, RunnerDisplayInfo>;
}

const AllRowsTable = ({ rows, runnersByUmaban }: AllRowsTableProps) => (
  <div className="runner-table-wrap">
    <table className="runner-table">
      <thead>
        <tr>
          <th scope="col">馬番</th>
          <th scope="col">馬名</th>
          <th scope="col">騎手名</th>
          <th scope="col">脚質</th>
          <th scope="col">逃げ</th>
          <th scope="col">先行</th>
          <th scope="col">差し</th>
          <th scope="col">追い込み</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={`${row.raceKey}-${row.horseNumber}`}>
            <td>{horseNumberDisplay(row.horseNumber)}</td>
            <td>{resolveBamei(row, runnersByUmaban)}</td>
            <td>{resolveJockey(row, runnersByUmaban)}</td>
            <td>{RUNNING_STYLE_DISPLAY[row.predictedLabel]}</td>
            <td>{formatPercent(row.p_nige)}</td>
            <td>{formatPercent(row.p_senkou)}</td>
            <td>{formatPercent(row.p_sashi)}</td>
            <td>{formatPercent(row.p_oikomi)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  </div>
);

interface FocusedListProps {
  rows: RaceRunningStyleRow[];
  tab: Exclude<StyleTab, "all">;
  runnersByUmaban: Record<number, RunnerDisplayInfo>;
}

const FocusedList = ({ rows, tab, runnersByUmaban }: FocusedListProps) => (
  <ol className="running-style-focus-list">
    {rows.map((row, index) => (
      <li
        key={`${row.raceKey}-${row.horseNumber}`}
        className={
          index < TOP_HIGHLIGHT_COUNT ? "running-style-focus-chip top" : "running-style-focus-chip"
        }
        aria-current={index < TOP_HIGHLIGHT_COUNT ? "true" : undefined}
      >
        {explicitChipText(row, tab, runnersByUmaban)}
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

interface MetricsBadgeProps {
  modelMacroF1: number | null;
  modelVersion: string | null;
}

const MetricsBadge = ({ modelMacroF1, modelVersion }: MetricsBadgeProps) => {
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
  runnersByUmaban,
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
        <AllRowsTable rows={visibleRows} runnersByUmaban={runnersByUmaban} />
      ) : (
        <FocusedList rows={visibleRows} tab={currentTab} runnersByUmaban={runnersByUmaban} />
      )}
      <MetricsBadge modelMacroF1={modelMacroF1} modelVersion={modelVersion} />
    </section>
  );
};

export { STYLE_TAB_VALUES };
export type { StyleTab, RunnerDisplayInfo };
