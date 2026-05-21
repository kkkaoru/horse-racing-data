import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { TopRaceSummary } from "../lib/race-types";
import { HomeRealtime } from "./home-realtime";

vi.mock("next/link", () => ({
  default: ({ children, href }: { children: ReactNode; href: string }) => (
    <a href={href}>{children}</a>
  ),
}));

const now = new Date("2026-05-17T03:00:00.000Z").getTime();

const race = (overrides: Partial<TopRaceSummary>): TopRaceSummary => ({
  source: "nar",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0517",
  keibajoCode: "42",
  raceBango: "01",
  kyosomeiHondai: "テストレース",
  kyosomeiFukudai: null,
  gradeCode: null,
  kyosoShubetsuCode: null,
  kyosoKigoCode: null,
  juryoShubetsuCode: null,
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyori: "1200",
  trackCode: "24",
  hassoJikoku: "1230",
  shussoTosu: "12",
  raceStartAt: "2026-05-17T03:30:00.000Z",
  ...overrides,
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("HomeRealtime", () => {
  it("shows race window load errors distinctly from empty states", () => {
    render(
      <HomeRealtime initialFinished={[]} initialLoadFailed initialNow={now} initialUpcoming={[]} />,
    );

    expect(screen.getByText("次のレースを読み込めませんでした。")).toBeTruthy();
    expect(screen.getByText("発走済みレースを読み込めませんでした。")).toBeTruthy();
  });

  it("shows empty states when race windows load successfully with no rows", () => {
    render(
      <HomeRealtime
        initialFinished={[]}
        initialLoadFailed={false}
        initialNow={now}
        initialUpcoming={[]}
      />,
    );

    expect(screen.getByText("次のレースは見つかりませんでした。")).toBeTruthy();
    expect(screen.getByText("発走済みのレースは見つかりませんでした。")).toBeTruthy();
  });

  it("shows the scheduled task list from upcoming races", () => {
    render(
      <HomeRealtime
        initialFinished={[race({ raceBango: "02", raceStartAt: "2026-05-17T02:30:00.000Z" })]}
        initialLoadFailed={false}
        initialNow={now}
        initialUpcoming={[race({})]}
      />,
    );

    expect(screen.getByText("30:00")).toBeTruthy();
    expect(screen.getByText("浦和 / 1R / 12:30 / ダート / 1200m")).toBeTruthy();
    expect(screen.getByRole("heading", { name: "スケジュール一覧" })).toBeTruthy();
    expect(screen.getByText("オッズ更新 / 浦和 / 1R / 12:30 / ダート / 1200m")).toBeTruthy();
  });

  it("keeps started races out of the next-race list", () => {
    render(
      <HomeRealtime
        initialFinished={[]}
        initialLoadFailed={false}
        initialNow={now}
        initialUpcoming={[
          race({ raceBango: "01", raceStartAt: "2026-05-17T02:59:59.000Z" }),
          race({ raceBango: "02", raceStartAt: "2026-05-17T03:10:00.000Z", hassoJikoku: "1210" }),
        ]}
      />,
    );

    const nextRaceSection = screen.getByRole("heading", { name: "次のレース" }).closest("section");
    if (!nextRaceSection) {
      throw new Error("next race section not found");
    }
    expect(within(nextRaceSection).queryByText("浦和 / 1R / 12:30 / ダート / 1200m")).toBeNull();
    expect(within(nextRaceSection).getByText("浦和 / 2R / 12:10 / ダート / 1200m")).toBeTruthy();
  });

  it("paginates only the scheduled task list", () => {
    render(
      <HomeRealtime
        initialFinished={[]}
        initialLoadFailed={false}
        initialNow={now}
        initialUpcoming={Array.from({ length: 7 }, (_, index) =>
          race({
            hassoJikoku: `${12 + Math.floor(index / 2)}${index % 2 === 0 ? "30" : "45"}`,
            raceBango: String(index + 1).padStart(2, "0"),
            raceStartAt: new Date(now + (index + 1) * 20 * 60_000).toISOString(),
          }),
        )}
      />,
    );

    expect(screen.getByText("1 / 2ページ（7件）")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "次へ" }));
    expect(screen.getByText("2 / 2ページ（7件）")).toBeTruthy();
    expect(screen.getByText(/オッズ更新 \/ 浦和 \/ 7R/u)).toBeTruthy();
  });
});
