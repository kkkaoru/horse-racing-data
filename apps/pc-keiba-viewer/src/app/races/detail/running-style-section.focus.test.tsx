// Run with: bun run test src/app/races/detail/running-style-section.focus.test.tsx

import { cleanup, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, test, vi } from "vitest";

import type { RaceRunningStyleRow } from "../../../db/corner-running-style-parsers";

vi.mock("next/navigation", () => ({
  useRouter: () => ({ replace: vi.fn<(href: string) => void>() }),
  useSearchParams: () => new URLSearchParams("style=nige"),
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

afterEach(cleanup);

describe("RunningStyleSection - nige focus tab", () => {
  test("sorts rows by p_nige descending and emits explicit chip text", () => {
    render(
      <RunningStyleSection
        rows={[
          buildRow({ horseNumber: 3, bamei: "馬C", p_nige: 0.2 }),
          buildRow({ horseNumber: 1, bamei: "ロードカナロア", p_nige: 0.8 }),
          buildRow({ horseNumber: 2, bamei: "馬B", p_nige: 0.5 }),
        ]}
        modelMacroF1={0.42}
        modelVersion="jra-rs-v1.0"
      />,
    );
    const items = screen.getAllByRole("listitem");
    expect(items).toHaveLength(3);
    expect(items[0]?.textContent).toBe("1番 ロードカナロア 逃げ 80%");
    expect(items[1]?.textContent).toBe("2番 馬B 逃げ 50%");
    expect(items[2]?.textContent).toBe("3番 馬C 逃げ 20%");
  });

  test("flags top-3 entries with aria-current for visual highlight", () => {
    render(
      <RunningStyleSection
        rows={[
          buildRow({ horseNumber: 1, bamei: "馬A", p_nige: 0.9 }),
          buildRow({ horseNumber: 2, bamei: "馬B", p_nige: 0.7 }),
          buildRow({ horseNumber: 3, bamei: "馬C", p_nige: 0.5 }),
          buildRow({ horseNumber: 4, bamei: "馬D", p_nige: 0.1 }),
        ]}
        modelMacroF1={null}
        modelVersion="v1"
      />,
    );
    const items = screen.getAllByRole("listitem");
    expect(items[0]?.getAttribute("aria-current")).toBe("true");
    expect(items[1]?.getAttribute("aria-current")).toBe("true");
    expect(items[2]?.getAttribute("aria-current")).toBe("true");
    expect(items[3]?.getAttribute("aria-current")).toBe(null);
  });

  test("renders 馬名不明 when bamei is missing", () => {
    render(
      <RunningStyleSection
        rows={[buildRow({ horseNumber: 5, bamei: null, p_nige: 0.6 })]}
        modelMacroF1={null}
        modelVersion="v1"
      />,
    );
    expect(screen.getByText("5番 馬名不明 逃げ 60%")).toBeTruthy();
  });
});
