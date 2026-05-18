// Run with: rendered by the race detail page (Next.js client component)

"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useMemo } from "react";

import type {
  RaceRunningStyleRow,
  RunningStyleLabel,
} from "../../../db/corner-running-style-parsers";

const STYLE_TAB_VALUES = ["nige", "senkou", "sashi", "oikomi"] as const;
type StyleTab = (typeof STYLE_TAB_VALUES)[number];

const STYLE_TAB_LABELS: Record<StyleTab, string> = {
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

const DEFAULT_TAB: StyleTab = "nige";
const PERCENT_DECIMALS = 2;
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

const probabilityForTab = (row: RaceRunningStyleRow, tab: StyleTab): number => {
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
  if (raw === null) return DEFAULT_TAB;
  return isStyleTab(raw) ? raw : DEFAULT_TAB;
};

const sortRowsByTab = (rows: RaceRunningStyleRow[], tab: StyleTab): RaceRunningStyleRow[] =>
  rows.toSorted((a, b) => probabilityForTab(b, tab) - probabilityForTab(a, tab));

const resolveBamei = (
  row: RaceRunningStyleRow,
  runners: Record<number, RunnerDisplayInfo>,
): string => row.bamei ?? runners[row.horseNumber]?.bamei ?? MISSING_NAME_PLACEHOLDER;

const resolveJockey = (
  row: RaceRunningStyleRow,
  runners: Record<number, RunnerDisplayInfo>,
): string => runners[row.horseNumber]?.jockey ?? MISSING_JOCKEY_PLACEHOLDER;

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
            <td>{row.horseNumber}</td>
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

interface TabButtonsProps {
  currentTab: StyleTab;
  onSelect: (tab: StyleTab) => void;
}

const TabButtons = ({ currentTab, onSelect }: TabButtonsProps) => (
  <div className="running-style-tabs" role="tablist" aria-label="脚質ソート">
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
    if (tab === DEFAULT_TAB) {
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
      <AllRowsTable rows={visibleRows} runnersByUmaban={runnersByUmaban} />
      <MetricsBadge modelMacroF1={modelMacroF1} modelVersion={modelVersion} />
    </section>
  );
};

export { STYLE_TAB_VALUES };
export type { StyleTab, RunnerDisplayInfo };
