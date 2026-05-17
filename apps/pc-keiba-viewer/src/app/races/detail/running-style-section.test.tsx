// Run with: bun run test src/app/races/detail/running-style-section.test.tsx

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
    render(<RunningStyleSection rows={[]} modelMacroF1={null} modelVersion={null} />);
    expect(screen.getByText("このレースの脚質予測データはまだありません。")).toBeTruthy();
  });
});

describe("RunningStyleSection - default tab", () => {
  test("renders the full probabilities table when no tab is selected", () => {
    render(
      <RunningStyleSection
        rows={[
          buildRow({
            horseNumber: 3,
            bamei: "馬A",
            p_nige: 0.6,
            p_senkou: 0.2,
            p_sashi: 0.1,
            p_oikomi: 0.1,
            predictedLabel: "nige",
          }),
          buildRow({
            horseNumber: 1,
            bamei: "馬B",
            p_nige: 0.1,
            p_senkou: 0.7,
            p_sashi: 0.1,
            p_oikomi: 0.1,
            predictedLabel: "senkou",
          }),
        ]}
        modelMacroF1={0.42}
        modelVersion="jra-rs-v1.0"
      />,
    );
    expect(screen.getByText("馬番")).toBeTruthy();
    expect(screen.getByText("予測ラベル")).toBeTruthy();
    expect(screen.getByText("1番")).toBeTruthy();
    expect(screen.getByText("3番")).toBeTruthy();
  });

  test("orders runners by horse number in the default view", () => {
    render(
      <RunningStyleSection
        rows={[
          buildRow({ horseNumber: 3, bamei: "馬C" }),
          buildRow({ horseNumber: 1, bamei: "馬A" }),
          buildRow({ horseNumber: 2, bamei: "馬B" }),
        ]}
        modelMacroF1={null}
        modelVersion="v1"
      />,
    );
    const rows = screen.getAllByRole("row");
    // first row is header, remaining 3 are body
    expect(rows[1].textContent).toContain("馬A");
    expect(rows[2].textContent).toContain("馬B");
    expect(rows[3].textContent).toContain("馬C");
  });

  test("renders the model version and macro-F1 in the metrics badge", () => {
    render(
      <RunningStyleSection rows={[buildRow({})]} modelMacroF1={0.42} modelVersion="jra-rs-v1.0" />,
    );
    expect(screen.getByText(/モデル: jra-rs-v1\.0/u)).toBeTruthy();
    expect(screen.getByText(/macro-F1: 0\.420/u)).toBeTruthy();
  });

  test("omits the metrics badge when modelVersion is null", () => {
    render(<RunningStyleSection rows={[buildRow({})]} modelMacroF1={null} modelVersion={null} />);
    expect(screen.queryByText(/モデル:/u)).toBe(null);
  });
});

describe("RunningStyleSection - tab interactions", () => {
  test("clicking a focus tab triggers router.replace with the style query parameter", () => {
    render(<RunningStyleSection rows={[buildRow({})]} modelMacroF1={null} modelVersion="v1" />);
    const nigeTab = screen.getByRole("tab", { name: "逃げ" });
    fireEvent.click(nigeTab);
    expect(replaceMock).toHaveBeenCalledTimes(1);
    const [target] = replaceMock.mock.calls[0];
    expect(target).toBe("?style=nige");
  });

  test("clicking 全体 removes the style query parameter", () => {
    render(<RunningStyleSection rows={[buildRow({})]} modelMacroF1={null} modelVersion="v1" />);
    fireEvent.click(screen.getByRole("tab", { name: "全体" }));
    expect(replaceMock).toHaveBeenCalledTimes(1);
    const [target] = replaceMock.mock.calls[0];
    expect(target).toBe("?");
  });
});
