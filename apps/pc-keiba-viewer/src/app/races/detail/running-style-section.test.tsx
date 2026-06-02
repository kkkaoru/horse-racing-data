// Run with: bunx vitest run src/app/races/detail/running-style-section.test.tsx

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, test, vi } from "vitest";

import type { RaceRunningStyleRow } from "../../../db/corner-running-style-parsers";
import type {
  RaceRowForRunningStyleBucketFilter,
  RunningStyleBucketMetrics,
  RunningStyleBucketScope,
  RunningStyleDimensionFlags,
} from "../../../lib/running-style-prediction-dimensions";

const replaceMock = vi.fn<(href: string, options?: { scroll?: boolean }) => void>();

vi.mock("next/navigation", () => ({
  useRouter: () => ({ replace: replaceMock }),
  useSearchParams: () => new URLSearchParams(),
}));

vi.mock("./realtime-client", () => ({
  useRealtimeRaceSelector: <T,>(selector: (state: { payload: null }) => T): T =>
    selector({ payload: null }),
}));

import { RunningStyleSection } from "./running-style-section";

const buildRow = (overrides: Partial<RaceRunningStyleRow>): RaceRunningStyleRow => ({
  bamei: "テストホース",
  category: "jra",
  horseNumber: 1,
  kaisaiNen: "2025",
  kettoTorokuBango: "2020100001",
  modelVersion: "jra-rs-v1.0",
  p_nige: 0.05,
  p_oikomi: 0.05,
  p_sashi: 0.4,
  p_senkou: 0.5,
  predictedAt: "2025-05-17T01:00:00Z",
  predictedLabel: "senkou",
  raceKey: "jra:2025:0517:05:11",
  ...overrides,
});

const buildBucketRace = (
  overrides: Partial<RaceRowForRunningStyleBucketFilter>,
): RaceRowForRunningStyleBucketFilter => ({
  source: "jra",
  keibajoCode: "05",
  kyori: 2000,
  kyosoShubetsuCode: "13",
  kyosoJokenCode: "010",
  kyosoJokenMeisho: null,
  trackCode: "10",
  gradeCode: null,
  kyosomeiHondai: null,
  ...overrides,
});

const buildFlags = (
  overrides: Partial<RunningStyleDimensionFlags>,
): RunningStyleDimensionFlags => ({
  keibajo: true,
  distance: true,
  kyosoShubetsu: true,
  kyosoJoken: true,
  condition: false,
  track: true,
  grade: false,
  raceName: false,
  ...overrides,
});

const buildEvaluation = (
  overrides: Partial<RunningStyleBucketMetrics>,
): RunningStyleBucketMetrics => ({
  raceCount: 120,
  predictionCount: 1500,
  accuracy: 0.653,
  accuracyCI: { lower: 0.621, upper: 0.685 },
  macroF1: 0.412,
  weightedF1: 0.638,
  qwk: 0.715,
  top2Accuracy: 0.842,
  overallLogLoss: 0.875,
  perClass: {
    nige: { precision: 0.6, recall: 0.55, f1: 0.574, support: 150 },
    senkou: { precision: 0.7, recall: 0.65, f1: 0.674, support: 600 },
    sashi: { precision: 0.65, recall: 0.7, f1: 0.674, support: 500 },
    oikomi: { precision: 0.4, recall: 0.3, f1: 0.343, support: 250 },
  },
  perClassLogLoss: { nige: 0.9, senkou: 0.7, sashi: 0.85, oikomi: 1.2 },
  confusionMatrix: [
    [80, 50, 15, 5],
    [70, 400, 100, 30],
    [30, 80, 350, 40],
    [10, 25, 90, 125],
  ],
  smallSampleWarning: false,
  ...overrides,
});

const buildScope = (overrides: Partial<RunningStyleBucketScope>): RunningStyleBucketScope => ({
  flags: {
    keibajo: true,
    distance: true,
    kyosoShubetsu: true,
    kyosoJoken: true,
    condition: false,
    track: true,
    grade: false,
    raceName: false,
  },
  level: "exact",
  ...overrides,
});

afterEach(() => {
  cleanup();
  replaceMock.mockReset();
});

describe("RunningStyleSection - empty state", () => {
  test("renders an empty placeholder when no rows are passed", () => {
    render(
      <RunningStyleSection
        rows={[]}
        modelMacroF1={null}
        modelVersion={null}
        runnersByUmaban={{}}
      />,
    );
    expect(screen.getByText("このレースの脚質予測データはまだありません。")).toBeTruthy();
  });
});

describe("RunningStyleSection - default tab", () => {
  test("renders all eight column headers including 脚質", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={0.42}
        modelVersion="jra-rs-v1.0"
        runnersByUmaban={{}}
      />,
    );
    expect(screen.getByText("馬番")).toBeTruthy();
    expect(screen.getByText("馬名")).toBeTruthy();
    expect(screen.getByText("騎手名")).toBeTruthy();
    expect(screen.getByText("脚質")).toBeTruthy();
  });

  test("renders horse_number as a bare integer without the 番 suffix", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({ horseNumber: 7, p_nige: 0.5 })]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
      />,
    );
    const cells = screen.getAllByRole("cell");
    expect(cells[0]?.textContent).toBe("7");
  });

  test("orders runners by p_nige descending when no tab query is present", () => {
    render(
      <RunningStyleSection
        rows={[
          buildRow({ horseNumber: 3, bamei: "馬C", p_nige: 0.2 }),
          buildRow({ horseNumber: 1, bamei: "馬A", p_nige: 0.8 }),
          buildRow({ horseNumber: 2, bamei: "馬B", p_nige: 0.5 }),
        ]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
      />,
    );
    const rows = screen.getAllByRole("row");
    expect(rows[1]?.textContent).toContain("馬A");
    expect(rows[2]?.textContent).toContain("馬B");
    expect(rows[3]?.textContent).toContain("馬C");
  });

  test("formats probability cells with two decimal places", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({ horseNumber: 1, p_nige: 0.1234 })]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
      />,
    );
    expect(screen.getByText("12.34%")).toBeTruthy();
  });

  test("renders the model version and macro-F1 in the metrics badge", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={0.42}
        modelVersion="jra-rs-v1.0"
        runnersByUmaban={{}}
      />,
    );
    expect(screen.getByText(/モデル: jra-rs-v1\.0/u)).toBeTruthy();
    expect(screen.getByText(/macro-F1: 0\.420/u)).toBeTruthy();
  });

  test("omits the metrics badge when modelVersion is null", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion={null}
        runnersByUmaban={{}}
      />,
    );
    expect(screen.queryByText(/モデル:/u)).toBe(null);
  });
});

describe("RunningStyleSection - tab interactions", () => {
  test("clicking 先行 sets the style query parameter", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
      />,
    );
    fireEvent.click(screen.getByRole("tab", { name: "先行" }));
    expect(replaceMock).toHaveBeenCalledTimes(1);
    const [target] = replaceMock.mock.calls[0] ?? [];
    expect(target).toBe("?style=senkou");
  });

  test("clicking 逃げ (the default tab) removes the style query parameter", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
      />,
    );
    fireEvent.click(screen.getByRole("tab", { name: "逃げ" }));
    expect(replaceMock).toHaveBeenCalledTimes(1);
    const [target] = replaceMock.mock.calls[0] ?? [];
    expect(target).toBe("?");
  });

  test("does not render a 全体 tab anymore", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
      />,
    );
    expect(screen.queryByRole("tab", { name: "全体" })).toBe(null);
  });
});

describe("RunningStyleSection - race-level nige rank label", () => {
  test("shows 逃げ for the single predicted=nige row when only one horse is nige", () => {
    render(
      <RunningStyleSection
        rows={[
          buildRow({
            horseNumber: 1,
            bamei: "テスト逃げ馬",
            predictedLabel: "nige",
            p_nige: 0.55,
            p_senkou: 0.25,
            p_sashi: 0.1,
            p_oikomi: 0.1,
          }),
          buildRow({
            horseNumber: 2,
            bamei: "テスト先行馬",
            predictedLabel: "senkou",
            p_nige: 0.2,
            p_senkou: 0.5,
            p_sashi: 0.2,
            p_oikomi: 0.1,
          }),
        ]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
      />,
    );
    const labelCells = screen.getAllByRole("cell").filter((cell) => cell.textContent === "逃げ");
    expect(labelCells.length).toBe(1);
  });

  test("shows 先行?(p_nige%) for predicted=nige rows ranked 2nd or lower in p_nige", () => {
    render(
      <RunningStyleSection
        rows={[
          buildRow({
            horseNumber: 1,
            bamei: "1番馬",
            predictedLabel: "nige",
            p_nige: 0.7,
            p_senkou: 0.2,
            p_sashi: 0.05,
            p_oikomi: 0.05,
          }),
          buildRow({
            horseNumber: 2,
            bamei: "2番馬",
            predictedLabel: "nige",
            p_nige: 0.55,
            p_senkou: 0.3,
            p_sashi: 0.1,
            p_oikomi: 0.05,
          }),
          buildRow({
            horseNumber: 3,
            bamei: "3番馬",
            predictedLabel: "nige",
            p_nige: 0.45,
            p_senkou: 0.35,
            p_sashi: 0.15,
            p_oikomi: 0.05,
          }),
        ]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
      />,
    );
    expect(screen.getByText("先行?(55%)")).toBeTruthy();
    expect(screen.getByText("先行?(45%)")).toBeTruthy();
  });

  test("renders the rank-1 nige row with the plain 逃げ label even when other nige rows are demoted", () => {
    render(
      <RunningStyleSection
        rows={[
          buildRow({
            horseNumber: 1,
            bamei: "1番馬",
            predictedLabel: "nige",
            p_nige: 0.7,
            p_senkou: 0.2,
            p_sashi: 0.05,
            p_oikomi: 0.05,
          }),
          buildRow({
            horseNumber: 2,
            bamei: "2番馬",
            predictedLabel: "nige",
            p_nige: 0.55,
            p_senkou: 0.3,
            p_sashi: 0.1,
            p_oikomi: 0.05,
          }),
        ]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
      />,
    );
    const labelCells = screen.getAllByRole("cell").filter((cell) => cell.textContent === "逃げ");
    expect(labelCells.length).toBe(1);
  });

  test("renders no 逃げ label when no row has predicted=nige", () => {
    render(
      <RunningStyleSection
        rows={[
          buildRow({
            horseNumber: 1,
            bamei: "先行馬A",
            predictedLabel: "senkou",
            p_nige: 0.3,
            p_senkou: 0.5,
            p_sashi: 0.1,
            p_oikomi: 0.1,
          }),
          buildRow({
            horseNumber: 2,
            bamei: "差し馬B",
            predictedLabel: "sashi",
            p_nige: 0.1,
            p_senkou: 0.2,
            p_sashi: 0.6,
            p_oikomi: 0.1,
          }),
        ]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
      />,
    );
    const labelCells = screen.getAllByRole("cell").filter((cell) => cell.textContent === "逃げ");
    expect(labelCells.length).toBe(0);
  });

  test("breaks p_nige ties by horse_number ascending so the smaller umaban becomes rank-1", () => {
    render(
      <RunningStyleSection
        rows={[
          buildRow({
            horseNumber: 5,
            bamei: "5番馬",
            predictedLabel: "nige",
            p_nige: 0.5,
            p_senkou: 0.3,
            p_sashi: 0.1,
            p_oikomi: 0.1,
          }),
          buildRow({
            horseNumber: 3,
            bamei: "3番馬",
            predictedLabel: "nige",
            p_nige: 0.5,
            p_senkou: 0.3,
            p_sashi: 0.1,
            p_oikomi: 0.1,
          }),
        ]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
      />,
    );
    expect(screen.getByText("先行?(50%)")).toBeTruthy();
    const labelCells = screen.getAllByRole("cell").filter((cell) => cell.textContent === "逃げ");
    expect(labelCells.length).toBe(1);
  });

  test("does not demote non-nige predictedLabel rows even when their p_nige is the rank-1 value", () => {
    render(
      <RunningStyleSection
        rows={[
          buildRow({
            horseNumber: 1,
            bamei: "高p_nige先行馬",
            predictedLabel: "senkou",
            p_nige: 0.6,
            p_senkou: 0.3,
            p_sashi: 0.05,
            p_oikomi: 0.05,
          }),
          buildRow({
            horseNumber: 2,
            bamei: "低p_nige逃げ馬",
            predictedLabel: "nige",
            p_nige: 0.2,
            p_senkou: 0.5,
            p_sashi: 0.2,
            p_oikomi: 0.1,
          }),
        ]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
      />,
    );
    expect(screen.getByText("先行?(20%)")).toBeTruthy();
    const labelCells = screen.getAllByRole("cell").filter((cell) => cell.textContent === "先行");
    expect(labelCells.length).toBe(1);
  });
});

describe("RunningStyleBucketEvaluationPanel - rendering", () => {
  test("renders accuracy + Wilson CI + race/prediction counts", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({})}
        bucketScope={buildScope({})}
        dimensionFlags={buildFlags({})}
        bucketRace={buildBucketRace({})}
        bucketSource="jra"
        bucketGradeCode={null}
      />,
    );
    expect(screen.getByText("65.3% ±3.2%")).toBeTruthy();
    expect(screen.getByText(/120レース.*1,500予測/u)).toBeTruthy();
  });

  test("renders QWK with three decimal places", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({ qwk: 0.7123 })}
        bucketScope={buildScope({})}
        dimensionFlags={buildFlags({})}
        bucketRace={buildBucketRace({})}
        bucketSource="jra"
        bucketGradeCode={null}
      />,
    );
    expect(screen.getByText("0.712")).toBeTruthy();
  });

  test("renders sixteen confusion matrix cells when bucketEvaluation is given", () => {
    const { container } = render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({})}
        bucketScope={buildScope({})}
        dimensionFlags={buildFlags({})}
        bucketRace={buildBucketRace({})}
        bucketSource="jra"
        bucketGradeCode={null}
      />,
    );
    const cells = container.querySelectorAll(".running-style-bucket-heatmap-cell");
    expect(cells.length).toBe(16);
  });

  test("shows small-sample badge when smallSampleWarning is true", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({
          predictionCount: 15,
          smallSampleWarning: true,
        })}
        bucketScope={buildScope({})}
        dimensionFlags={buildFlags({})}
        bucketRace={buildBucketRace({})}
        bucketSource="jra"
        bucketGradeCode={null}
      />,
    );
    expect(screen.getByText("(n=15, small sample)")).toBeTruthy();
  });

  test("renders 'n too small' for classes with support below five", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({
          perClass: {
            nige: { precision: null, recall: null, f1: null, support: 2 },
            senkou: { precision: 0.7, recall: 0.65, f1: 0.674, support: 600 },
            sashi: { precision: 0.65, recall: 0.7, f1: 0.674, support: 500 },
            oikomi: { precision: 0.4, recall: 0.3, f1: 0.343, support: 250 },
          },
        })}
        bucketScope={buildScope({})}
        dimensionFlags={buildFlags({})}
        bucketRace={buildBucketRace({})}
        bucketSource="jra"
        bucketGradeCode={null}
      />,
    );
    expect(screen.getAllByText("n too small").length).toBe(3);
  });

  test("does not render bucket panel when bucketEvaluation is null and flags absent", () => {
    const { container } = render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={null}
      />,
    );
    expect(container.querySelector(".running-style-bucket-evaluation-panel")).toBe(null);
    expect(container.querySelector(".running-style-bucket-toggles")).toBe(null);
  });

  test("does not render the bucket panel when bucketScope is null even with metrics", () => {
    const { container } = render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({})}
        bucketScope={null}
        dimensionFlags={buildFlags({})}
        bucketRace={buildBucketRace({})}
        bucketSource="jra"
        bucketGradeCode={null}
      />,
    );
    expect(container.querySelector(".running-style-bucket-evaluation-panel")).toBe(null);
  });

  test("renders the exact scope label joined from active dimensions without a fallback notice", () => {
    const { container } = render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({})}
        bucketScope={buildScope({ level: "exact" })}
        dimensionFlags={buildFlags({})}
        bucketRace={buildBucketRace({})}
        bucketSource="jra"
        bucketGradeCode={null}
      />,
    );
    expect(screen.getByText("東京 / 2000m / 3歳以上 / 2勝クラス / 芝直線 の脚質精度")).toBeTruthy();
    expect(container.querySelector(".running-style-bucket-scope-notice")).toBe(null);
  });

  test("renders the keibajo fallback label and a fallback notice when scope level is keibajo", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({})}
        bucketScope={buildScope({
          flags: {
            keibajo: true,
            distance: false,
            kyosoShubetsu: false,
            kyosoJoken: false,
            condition: false,
            track: false,
            grade: false,
            raceName: false,
          },
          level: "keibajo",
        })}
        dimensionFlags={buildFlags({})}
        bucketRace={buildBucketRace({ keibajoCode: "30" })}
        bucketSource="nar"
        bucketGradeCode={null}
      />,
    );
    expect(screen.getByText("門別（全レース） の脚質精度")).toBeTruthy();
    expect(
      screen.getByText("該当条件のデータが無いため門別（全レース）で集計しています"),
    ).toBeTruthy();
  });

  test("renders the NAR category fallback label when scope level is category", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({})}
        bucketScope={buildScope({
          flags: {
            keibajo: false,
            distance: false,
            kyosoShubetsu: false,
            kyosoJoken: false,
            condition: false,
            track: false,
            grade: false,
            raceName: false,
          },
          level: "category",
        })}
        dimensionFlags={buildFlags({})}
        bucketRace={buildBucketRace({ source: "nar", keibajoCode: "30" })}
        bucketSource="nar"
        bucketGradeCode={null}
      />,
    );
    expect(screen.getByText("NAR 全体 の脚質精度")).toBeTruthy();
    expect(screen.getByText("該当条件のデータが無いためNAR 全体で集計しています")).toBeTruthy();
  });

  test("renders the eight headline metric cards including per-class recall", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({})}
        bucketScope={buildScope({})}
        dimensionFlags={buildFlags({})}
        bucketRace={buildBucketRace({})}
        bucketSource="jra"
        bucketGradeCode={null}
      />,
    );
    expect(screen.getByText("正解率")).toBeTruthy();
    expect(screen.getByText("Top2正解率")).toBeTruthy();
    expect(screen.getByText("逃げ的中率")).toBeTruthy();
    expect(screen.getByText("追込的中率")).toBeTruthy();
  });

  test("renders the accuracy headline card formatted with two decimals", () => {
    const { container } = render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({ accuracy: 0.6531 })}
        bucketScope={buildScope({})}
        dimensionFlags={buildFlags({})}
        bucketRace={buildBucketRace({})}
        bucketSource="jra"
        bucketGradeCode={null}
      />,
    );
    const cards = container.querySelectorAll(".running-style-bucket-metric-card");
    expect(cards.length).toBe(8);
    expect(screen.getByText("65.31%")).toBeTruthy();
  });
});

describe("RunningStyleDimensionToggles - JRA branch", () => {
  test("renders kyosoJoken + track checkboxes for JRA and hides condition", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({})}
        bucketScope={buildScope({})}
        dimensionFlags={buildFlags({})}
        bucketRace={buildBucketRace({})}
        bucketSource="jra"
        bucketGradeCode={null}
      />,
    );
    expect(screen.getByLabelText("東京")).toBeTruthy();
    expect(screen.getByLabelText("2000m")).toBeTruthy();
    expect(screen.getByLabelText("2勝クラス")).toBeTruthy();
  });

  test("hides grade and raceName checkboxes when gradeCode is null", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({})}
        bucketScope={buildScope({})}
        dimensionFlags={buildFlags({})}
        bucketRace={buildBucketRace({ gradeCode: null })}
        bucketSource="jra"
        bucketGradeCode={null}
      />,
    );
    expect(screen.queryByLabelText("G1")).toBe(null);
    expect(screen.queryByLabelText("G2")).toBe(null);
    expect(screen.queryByLabelText("G3")).toBe(null);
  });

  test("shows grade checkbox but hides raceName when gradeCode is C (G3)", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({})}
        bucketScope={buildScope({})}
        dimensionFlags={buildFlags({ grade: true })}
        bucketRace={buildBucketRace({ gradeCode: "C", kyosomeiHondai: "重賞テスト" })}
        bucketSource="jra"
        bucketGradeCode="C"
      />,
    );
    expect(screen.getByLabelText("G3")).toBeTruthy();
    expect(screen.queryByLabelText("重賞テスト")).toBe(null);
  });

  test("shows both grade and raceName checkboxes when gradeCode is A (G1)", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({})}
        bucketScope={buildScope({})}
        dimensionFlags={buildFlags({ grade: true, raceName: true })}
        bucketRace={buildBucketRace({ gradeCode: "A", kyosomeiHondai: "ジャパンカップ" })}
        bucketSource="jra"
        bucketGradeCode="A"
      />,
    );
    expect(screen.getByLabelText("G1")).toBeTruthy();
    expect(screen.getByLabelText("ジャパンカップ")).toBeTruthy();
  });
});

describe("RunningStyleDimensionToggles - NAR branch", () => {
  test("hides kyosoJoken + track and shows condition checkbox for NAR", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({})}
        bucketScope={buildScope({})}
        dimensionFlags={buildFlags({ kyosoJoken: false, track: false, condition: true })}
        bucketRace={buildBucketRace({
          source: "nar",
          keibajoCode: "30",
          kyosoJokenMeisho: "C2",
        })}
        bucketSource="nar"
        bucketGradeCode={null}
      />,
    );
    expect(screen.getByLabelText("C2")).toBeTruthy();
    expect(screen.queryByLabelText("2勝クラス")).toBe(null);
  });

  test("renders fallback condition label '条件' when kyosoJokenMeisho is blank for NAR", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({})}
        bucketScope={buildScope({})}
        dimensionFlags={buildFlags({ kyosoJoken: false, track: false, condition: true })}
        bucketRace={buildBucketRace({ source: "nar", kyosoJokenMeisho: "  " })}
        bucketSource="nar"
        bucketGradeCode={null}
      />,
    );
    expect(screen.getByLabelText("条件")).toBeTruthy();
  });
});

describe("RunningStyleDimensionToggles - click behavior", () => {
  test("clicking a checked checkbox writes runningStyleKeibajo=0 via router.replace", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({})}
        bucketScope={buildScope({})}
        dimensionFlags={buildFlags({})}
        bucketRace={buildBucketRace({})}
        bucketSource="jra"
        bucketGradeCode={null}
      />,
    );
    fireEvent.click(screen.getByLabelText("東京"));
    expect(replaceMock).toHaveBeenCalledTimes(1);
    const [target] = replaceMock.mock.calls[0] ?? [];
    expect(target).toBe("?runningStyleKeibajo=0");
  });

  test("clicking an unchecked checkbox writes runningStyleCondition=1 via router.replace", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({})]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
        bucketEvaluation={buildEvaluation({})}
        bucketScope={buildScope({})}
        dimensionFlags={buildFlags({
          kyosoJoken: false,
          track: false,
          condition: false,
        })}
        bucketRace={buildBucketRace({
          source: "nar",
          keibajoCode: "30",
          kyosoJokenMeisho: "C1",
        })}
        bucketSource="nar"
        bucketGradeCode={null}
      />,
    );
    fireEvent.click(screen.getByLabelText("C1"));
    expect(replaceMock).toHaveBeenCalledTimes(1);
    const [target] = replaceMock.mock.calls[0] ?? [];
    expect(target).toBe("?runningStyleCondition=1");
  });
});
