// Run with: bunx vitest run src/app/races/detail/running-style-section.test.tsx

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, test, vi } from "vitest";

import type { RaceRunningStyleRow } from "../../../db/corner-running-style-parsers";

const replaceMock = vi.fn<(href: string, options?: { scroll?: boolean }) => void>();

vi.mock("next/navigation", () => ({
  useRouter: () => ({ replace: replaceMock }),
  useSearchParams: () => new URLSearchParams(),
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
  raceKey: "jra:20250517:05:11",
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
