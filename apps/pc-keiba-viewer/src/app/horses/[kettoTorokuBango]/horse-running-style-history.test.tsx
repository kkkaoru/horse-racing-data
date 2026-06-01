// Run with: bun run test src/app/horses/[kettoTorokuBango]/horse-running-style-history.test.tsx

import { cleanup, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, test } from "vitest";

import type { RaceRunningStyleRow } from "../../../db/corner-running-style-parsers";
import {
  chipText,
  formatRaceLabel,
  HorseRunningStyleHistory,
  STYLE_DISPLAY,
} from "./horse-running-style-history";

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

afterEach(cleanup);

describe("STYLE_DISPLAY", () => {
  test("maps every running-style label to its kanji label", () => {
    expect(STYLE_DISPLAY).toStrictEqual({
      nige: "逃げ",
      oikomi: "追い込み",
      sashi: "差し",
      senkou: "先行",
    });
  });
});

describe("formatRaceLabel", () => {
  test("emits ISO date prefix and race key", () => {
    const row = buildRow({
      predictedAt: "2025-05-17T01:00:00Z",
      raceKey: "jra:2025:0517:05:11",
    });
    expect(formatRaceLabel(row)).toBe("2025-05-17 jra:2025:0517:05:11");
  });
});

describe("chipText", () => {
  test("combines race label with predicted style", () => {
    const row = buildRow({ predictedLabel: "nige" });
    expect(chipText(row)).toBe("2025-05-17 jra:2025:0517:05:11 逃げ");
  });
});

describe("HorseRunningStyleHistory - empty state", () => {
  test("renders an empty placeholder when no rows are provided", () => {
    render(<HorseRunningStyleHistory rows={[]} />);
    expect(screen.getByText("この馬の脚質予測履歴はまだありません。")).toBeTruthy();
  });
});

describe("HorseRunningStyleHistory - chip list", () => {
  test("renders one chip per row preserving order", () => {
    render(
      <HorseRunningStyleHistory
        rows={[
          buildRow({ raceKey: "jra:2025:0517:05:11", predictedLabel: "nige" }),
          buildRow({
            raceKey: "jra:2025:0503:05:09",
            predictedAt: "2025-05-03T01:00:00Z",
            predictedLabel: "sashi",
          }),
        ]}
      />,
    );
    const items = screen.getAllByRole("listitem");
    expect(items).toHaveLength(2);
    const [first, second] = items;
    if (first === undefined || second === undefined) throw new Error("missing items");
    expect(first.textContent).toBe("2025-05-17 jra:2025:0517:05:11 逃げ");
    expect(second.textContent).toBe("2025-05-03 jra:2025:0503:05:09 差し");
  });

  test("applies a style-specific css class to each chip", () => {
    render(
      <HorseRunningStyleHistory
        rows={[
          buildRow({ predictedLabel: "nige", raceKey: "r1" }),
          buildRow({ predictedLabel: "oikomi", raceKey: "r2" }),
        ]}
      />,
    );
    const items = screen.getAllByRole("listitem");
    const [first, second] = items;
    if (first === undefined || second === undefined) throw new Error("missing items");
    expect(first.className).toContain("nige");
    expect(second.className).toContain("oikomi");
  });
});
