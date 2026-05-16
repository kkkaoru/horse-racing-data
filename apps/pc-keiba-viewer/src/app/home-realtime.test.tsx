import { cleanup, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { TopRaceSummary } from "../lib/race-types";
import { HomeRealtime } from "./home-realtime";

vi.mock("next/link", () => ({
  default: ({ children, href }: { children: ReactNode; href: string }) => (
    <a href={href}>{children}</a>
  ),
}));

vi.mock("./races/detail/realtime-client", () => ({
  isRealtimeRacePayload: (value: unknown) => typeof value === "object" && value !== null,
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

  it("keeps the odds update list skeleton visible while the first odds refresh is pending", () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(() => new Promise(() => {}));

    const { container } = render(
      <HomeRealtime
        initialFinished={[race({ raceBango: "02", raceStartAt: "2026-05-17T02:30:00.000Z" })]}
        initialLoadFailed={false}
        initialNow={now}
        initialUpcoming={[race({})]}
      />,
    );

    expect(screen.getByText("30:00")).toBeTruthy();
    expect(screen.getByText("浦和 / 1R / 12:30 / ダート / 1200m")).toBeTruthy();
    expect(container.querySelector('[aria-busy="true"]')).toBeTruthy();
  });

  it("shows an odds update error when all realtime refreshes fail", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(null, { status: 503 }));

    render(
      <HomeRealtime
        initialFinished={[]}
        initialLoadFailed={false}
        initialNow={now}
        initialUpcoming={[race({})]}
      />,
    );

    await waitFor(() => {
      expect(screen.getByText("オッズ更新予定を読み込めませんでした。")).toBeTruthy();
    });
  });
});
