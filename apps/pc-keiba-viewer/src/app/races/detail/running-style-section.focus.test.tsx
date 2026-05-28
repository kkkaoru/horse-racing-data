// Run with: bunx vitest run src/app/races/detail/running-style-section.focus.test.tsx

import { cleanup, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, test, vi } from "vitest";

import type { RaceRunningStyleRow } from "../../../db/corner-running-style-parsers";

const replaceMock = vi.fn<(href: string, options?: { scroll?: boolean }) => void>();

const searchParamsRef = { current: new URLSearchParams() };

vi.mock("next/navigation", () => ({
  useRouter: () => ({ replace: replaceMock }),
  useSearchParams: () => searchParamsRef.current,
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
  raceKey: "jra:20250517:05:11",
  ...overrides,
});

afterEach(() => {
  cleanup();
  replaceMock.mockReset();
  searchParamsRef.current = new URLSearchParams();
});

describe("RunningStyleSection - per-tab sort", () => {
  test("sorts table rows by p_senkou descending when ?style=senkou is in the URL", () => {
    searchParamsRef.current = new URLSearchParams("style=senkou");
    render(
      <RunningStyleSection
        rows={[
          buildRow({ horseNumber: 1, bamei: "馬A", p_senkou: 0.2 }),
          buildRow({ horseNumber: 2, bamei: "馬B", p_senkou: 0.8 }),
          buildRow({ horseNumber: 3, bamei: "馬C", p_senkou: 0.5 }),
        ]}
        modelMacroF1={0.42}
        modelVersion="jra-rs-v1.0"
        runnersByUmaban={{}}
      />,
    );
    const rows = screen.getAllByRole("row");
    expect(rows[1]?.textContent).toContain("馬B");
    expect(rows[2]?.textContent).toContain("馬C");
    expect(rows[3]?.textContent).toContain("馬A");
  });

  test("sorts table rows by p_oikomi descending when ?style=oikomi is in the URL", () => {
    searchParamsRef.current = new URLSearchParams("style=oikomi");
    render(
      <RunningStyleSection
        rows={[
          buildRow({ horseNumber: 1, bamei: "追A", p_oikomi: 0.1 }),
          buildRow({ horseNumber: 2, bamei: "追B", p_oikomi: 0.7 }),
          buildRow({ horseNumber: 3, bamei: "追C", p_oikomi: 0.3 }),
        ]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
      />,
    );
    const rows = screen.getAllByRole("row");
    expect(rows[1]?.textContent).toContain("追B");
    expect(rows[2]?.textContent).toContain("追C");
    expect(rows[3]?.textContent).toContain("追A");
  });

  test("renders 馬名不明 fallback when bamei is missing in both D1 row and runner map", () => {
    searchParamsRef.current = new URLSearchParams("style=nige");
    render(
      <RunningStyleSection
        rows={[buildRow({ horseNumber: 5, bamei: null, p_nige: 0.6 })]}
        modelMacroF1={null}
        modelVersion="v1"
        runnersByUmaban={{}}
      />,
    );
    expect(screen.getByText("馬名不明")).toBeTruthy();
  });
});
