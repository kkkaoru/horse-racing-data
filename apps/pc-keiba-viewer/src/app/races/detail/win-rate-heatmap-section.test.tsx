// bun で実行する (bunx vitest)
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import { afterEach, expect, it, vi } from "vitest";

import type {
  HorseWeightSnapshot,
  UseHorseWeightStreamParams,
} from "../../../lib/horse-weight-stream-client";
import { useHorseWeightStream } from "../../../lib/horse-weight-stream-client";
import type {
  BloodlineStatsRow,
  FrameStatsRow,
  HorseRaceResult,
  Runner,
  SimilarRaceStatsRow,
} from "../../../lib/race-types";
import { useRealtimeRacePayload } from "./realtime-client";
import { WinRateHeatmapSection } from "./win-rate-heatmap-section";

vi.mock("../../../lib/horse-weight-stream-client", () => ({
  useHorseWeightStream: vi.fn<(params: UseHorseWeightStreamParams) => HorseWeightSnapshot | null>(
    () => null,
  ),
}));

vi.mock("./realtime-client", () => ({
  useRealtimeRacePayload: vi.fn<
    () => { error: string | null; payload: RealtimeRacePayload | null }
  >(() => ({ error: null, payload: null })),
}));

interface MockMediaQueryEvent {
  matches: boolean;
}

interface MockMediaQueryListController {
  matches: boolean;
  fire: (matches: boolean) => void;
}

const installMatchMediaMock = (initialMatches: boolean): MockMediaQueryListController => {
  const listeners = new Set<(event: MockMediaQueryEvent) => void>();
  const controller: MockMediaQueryListController = {
    fire: (matches: boolean) => {
      controller.matches = matches;
      listeners.forEach((listener) => {
        listener({ matches });
      });
    },
    matches: initialMatches,
  };
  const mediaQueryList = {
    addEventListener: (_: string, listener: (event: MockMediaQueryEvent) => void) => {
      listeners.add(listener);
    },
    addListener: (listener: (event: MockMediaQueryEvent) => void) => {
      listeners.add(listener);
    },
    get matches() {
      return controller.matches;
    },
    removeEventListener: (_: string, listener: (event: MockMediaQueryEvent) => void) => {
      listeners.delete(listener);
    },
    removeListener: (listener: (event: MockMediaQueryEvent) => void) => {
      listeners.delete(listener);
    },
  };
  vi.stubGlobal(
    "matchMedia",
    vi.fn(() => mediaQueryList),
  );
  return controller;
};

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.mocked(useHorseWeightStream).mockReturnValue(null);
  vi.mocked(useRealtimeRacePayload).mockReturnValue({ error: null, payload: null });
});

const heatmapRealtimeRequest = {
  apiBaseUrl: "https://realtime.test",
  day: "21",
  keibajoCode: "05",
  month: "08",
  raceNumber: "11",
  source: "jra",
  year: "2026",
};

const liveWeightSnapshot: HorseWeightSnapshot = {
  fetchedAt: "2026-08-21T10:00:00+09:00",
  horses: [
    {
      changeAmount: 2,
      changeSign: "+",
      horseName: "Alpha",
      horseNumber: "1",
      weight: 485,
    },
  ],
};

const runner: Runner = {
  banushimei: "Owner A",
  barei: "4",
  bamei: "Alpha",
  bataiju: null,
  chokyoshimeiRyakusho: "Trainer A",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  damSireName: null,
  futanJuryo: "570",
  kakuteiChakujun: "00",
  kettoTorokuBango: "2020100001",
  kishumeiRyakusho: "Jockey A",
  kohan3f: null,
  seibetsuCode: "1",
  sireName: null,
  sireSireName: null,
  sohaTime: null,
  tanshoNinkijun: "00",
  tanshoOdds: "0000",
  timeSa: null,
  umaban: "01",
  wakuban: "1",
  zogenFugo: null,
  zogenSa: null,
};

const similarJockey: SimilarRaceStatsRow = {
  category: "jockey",
  currentHorseNumbers: "1",
  details: [],
  horseCount: 40,
  name: "Jockey A",
  quinellaCount: 20,
  quinellaRate: 25,
  showCount: 30,
  showRate: 37.5,
  starts: 80,
  winCount: 16,
  winRate: 20,
};

const similarTrainer: SimilarRaceStatsRow = {
  category: "trainer",
  currentHorseNumbers: "1",
  details: [],
  horseCount: 12,
  name: "Trainer A",
  quinellaCount: 8,
  quinellaRate: 16,
  showCount: 10,
  showRate: 20,
  starts: 50,
  winCount: 5,
  winRate: 10,
};

const bloodlineSire: BloodlineStatsRow = {
  category: "sire",
  currentHorseNumbers: "1",
  details: [],
  horseCount: 30,
  name: "Sire Alpha",
  quinellaCount: 40,
  quinellaRate: 20,
  showCount: 60,
  showRate: 30,
  starts: 200,
  winCount: 24,
  winRate: 12,
};

const frameOne: FrameStatsRow = {
  averageFinish: 3.2,
  averagePopularity: 4.1,
  count: 40,
  details: [],
  frameNumber: "1",
  medianFinish: 2.5,
  medianPopularity: 3,
  quinellaCount: 12,
  quinellaRate: 30,
  runnerCount: 16,
  score: 0.8,
  showCount: 18,
  showRate: 45,
  winCount: 6,
  winRate: 15,
};

const horsePastWin: HorseRaceResult = {
  babajotaiCodeDirt: null,
  babajotaiCodeShiba: null,
  bamei: "Alpha",
  banushimei: "Owner A",
  barei: "3",
  bataiju: "480",
  chokyoshimeiRyakusho: "Trainer A",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  currentBarei: "4",
  currentJockey: "Jockey A",
  currentSeibetsuCode: "1",
  currentUmaban: "01",
  futanJuryo: "570",
  gradeCode: null,
  hassoJikoku: "1200",
  juryoShubetsuCode: null,
  kaisaiNen: "2025",
  kaisaiTsukihi: "0112",
  kakuteiChakujun: "01",
  keibajoCode: "05",
  kettoTorokuBango: "2020100001",
  kishumeiRyakusho: "Jockey A",
  kohan3f: null,
  kyori: "1600",
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosomeiFukudai: null,
  kyosomeiHondai: "Past A",
  kyosomeiKakkonai: null,
  kyosoShubetsuCode: null,
  raceBango: "01",
  seibetsuCode: "1",
  sohaTime: null,
  tanshoNinkijun: "1",
  tanshoOdds: "12",
  tenkoCode: null,
  timeSa: null,
  trackCode: "10",
  umaban: "03",
  wakuban: "3",
  zogenFugo: null,
  zogenSa: null,
};

it("shows an empty state when there are no runners", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[]}
      similarRows={[]}
    />,
  );
  expect(screen.getByText("勝率ヒートマップを表示する出走馬がありません。")).toBeDefined();
  expect(document.querySelector(".win-rate-heatmap-color-scale-slot")).toBeNull();
});

it("renders a heatmap of win rates by default without a horse-name column", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[bloodlineSire]}
      frameStats={[frameOne]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[similarJockey, similarTrainer]}
    />,
  );
  expect(screen.queryByText("馬名")).toBeNull();
  expect(screen.getByText("番")).toBeDefined();
  expect(screen.getByText("枠")).toBeDefined();
  expect(screen.queryByText("馬体重")).toBeNull();
  expect(screen.getByText("斤量")).toBeDefined();
  expect(screen.getByText("馬")).toBeDefined();
  expect(screen.getByText("騎手")).toBeDefined();
  expect(screen.getByText("調教師")).toBeDefined();
  expect(screen.getByText("父")).toBeDefined();
  expect(screen.getByText("母父")).toBeDefined();
  expect(screen.getByText("父父")).toBeDefined();
  expect(screen.getAllByText("勝").length).toBe(8);
  expect(screen.queryByText("連")).toBeNull();
  expect(screen.queryByText("複")).toBeNull();
  expect(screen.getByText("20.0%")).toBeDefined();
  expect(screen.getByText("10.0%")).toBeDefined();
  expect(screen.getByText("12.0%")).toBeDefined();
  expect(screen.getByText("15.0%")).toBeDefined();
  expect(screen.getByRole("checkbox", { name: "レース数" })).toBeDefined();
  expect(screen.getByRole("checkbox", { name: "レース数" })).toHaveProperty("checked", false);
  expect(screen.queryByText("(80)")).toBeNull();
  expect(screen.queryByText("(40)")).toBeNull();
  expect(screen.queryByText("25.0%")).toBeNull();
  expect(screen.queryByText("37.5%")).toBeNull();
  expect(screen.queryByText("16.0%")).toBeNull();
  expect(screen.queryByText("30.0%")).toBeNull();
  expect(screen.queryByText("45.0%")).toBeNull();
  expect(screen.getByTitle("Alpha")).toBeDefined();
  expect(screen.getByText("Alpha")).toBeDefined();
  expect(screen.getByText("Jockey A")).toBeDefined();
  expect(screen.getByText("Trainer A")).toBeDefined();
  expect(
    document.querySelector(".win-rate-heatmap-tooltip .frame-number-badge.frame-1"),
  ).toBeDefined();
  expect(screen.queryByTitle("枠の勝率: 枠1 15.0%（40走）")).toBeNull();
  expect(
    screen.queryByText(
      "番だけで馬を識別します。枠は同条件レース分析の枠番成績です。勝・連・複は色の濃さのヒートマップです。赤が勝率、橙が連対率、青が複勝率で、濃いほど高いです。",
    ),
  ).toBeNull();
  const tableWrap = document.querySelector(".win-rate-heatmap-table-wrap");
  const viewToggle = document.querySelector(".win-rate-heatmap-view-toggle");
  expect(viewToggle instanceof HTMLFieldSetElement).toBe(true);
  expect(tableWrap instanceof HTMLDivElement).toBe(true);
  expect(tableWrap?.contains(viewToggle)).toBe(false);
  expect(tableWrap?.querySelector(".win-rate-heatmap-table") instanceof HTMLTableElement).toBe(
    true,
  );
  expect(document.querySelector(".running-style-bucket-controls")).toBeNull();
  expect(screen.getByRole("radio", { name: /^勝率$/ })).toHaveProperty("checked", true);
  expect(screen.getByRole("radio", { name: /^連対率$/ })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: /^複勝率$/ })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: "勝率+連対率+複勝率" })).toHaveProperty(
    "checked",
    false,
  );
  expect(
    screen.getByRole("figure", { name: "勝率の色は0%から40%以上まで濃くなります" }),
  ).toBeDefined();
  expect(screen.getByText("0%")).toBeDefined();
  expect(screen.getByText("10%")).toBeDefined();
  expect(screen.getByText("20%")).toBeDefined();
  expect(screen.getByText("30%")).toBeDefined();
  expect(screen.getByText("40%以上")).toBeDefined();
  expect(viewToggle?.nextElementSibling?.className).toBe("win-rate-heatmap-color-scale-slot");
  expect(
    document.querySelector(".win-rate-heatmap-color-scale-slot")?.nextElementSibling?.className,
  ).toBe("stats-table-wrap win-rate-heatmap-table-wrap");
  const winRateScaleBar = document.querySelector(".win-rate-heatmap-color-scale-bar");
  if (!(winRateScaleBar instanceof HTMLDivElement)) {
    throw new Error("expected win-rate color scale bar");
  }
  expect(winRateScaleBar.style.backgroundImage).toBe(
    "linear-gradient(to right, hsl(8, 22%, 96%) 0%, hsl(8, 40%, 79%) 25%, hsl(8, 59%, 62%) 50%, hsl(8, 77%, 45%) 75%, hsl(8, 95%, 28%) 100%)",
  );
  expect(document.querySelectorAll(".win-rate-heatmap-color-scale-bar").length).toBe(1);
  expect(document.querySelector(".win-rate-heatmap-color-scale")?.className).toBe(
    "win-rate-heatmap-color-scale",
  );
});

it("shows quinella-rate swatches when the quinella-rate radio is selected", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[bloodlineSire]}
      frameStats={[frameOne]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[similarJockey, similarTrainer]}
    />,
  );
  fireEvent.click(screen.getByRole("radio", { name: /^連対率$/ }));
  expect(screen.getByRole("radio", { name: /^連対率$/ })).toHaveProperty("checked", true);
  expect(screen.getByRole("radio", { name: /^勝率$/ })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: /^複勝率$/ })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: "勝率+連対率+複勝率" })).toHaveProperty(
    "checked",
    false,
  );
  expect(screen.getAllByText("連").length).toBe(8);
  expect(screen.queryByText("勝")).toBeNull();
  expect(screen.queryByText("複")).toBeNull();
  expect(screen.getByText("30.0%")).toBeDefined();
  expect(screen.queryByText("15.0%")).toBeNull();
  expect(screen.queryByText("45.0%")).toBeNull();
  expect(
    screen.getByRole("figure", { name: "連対率の色は0%から40%以上まで濃くなります" }),
  ).toBeDefined();
  const quinellaScaleBar = document.querySelector(".win-rate-heatmap-color-scale-bar");
  if (!(quinellaScaleBar instanceof HTMLDivElement)) {
    throw new Error("expected quinella color scale bar");
  }
  expect(quinellaScaleBar.style.backgroundImage).toBe(
    "linear-gradient(to right, hsl(36, 22%, 96%) 0%, hsl(36, 40%, 79%) 25%, hsl(36, 59%, 62%) 50%, hsl(36, 77%, 45%) 75%, hsl(36, 95%, 28%) 100%)",
  );
});

it("shows show-rate swatches when the show-rate radio is selected", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[bloodlineSire]}
      frameStats={[frameOne]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[similarJockey, similarTrainer]}
    />,
  );
  fireEvent.click(screen.getByRole("radio", { name: /^複勝率$/ }));
  expect(screen.getByRole("radio", { name: /^複勝率$/ })).toHaveProperty("checked", true);
  expect(screen.getByRole("radio", { name: /^勝率$/ })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: /^連対率$/ })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: "勝率+連対率+複勝率" })).toHaveProperty(
    "checked",
    false,
  );
  expect(screen.getAllByText("複").length).toBe(8);
  expect(screen.queryByText("勝")).toBeNull();
  expect(screen.queryByText("連")).toBeNull();
  expect(screen.getByText("45.0%")).toBeDefined();
  expect(screen.queryByText("15.0%")).toBeNull();
  expect(
    screen.getByRole("figure", { name: "複勝率の色は0%から40%以上まで濃くなります" }),
  ).toBeDefined();
  const showScaleBar = document.querySelector(".win-rate-heatmap-color-scale-bar");
  if (!(showScaleBar instanceof HTMLDivElement)) {
    throw new Error("expected show color scale bar");
  }
  expect(showScaleBar.style.backgroundImage).toBe(
    "linear-gradient(to right, hsl(196, 22%, 96%) 0%, hsl(196, 40%, 79%) 25%, hsl(196, 59%, 62%) 50%, hsl(196, 77%, 45%) 75%, hsl(196, 95%, 28%) 100%)",
  );
});

it("shows win, quinella, and show swatches when the combined radio is selected", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[bloodlineSire]}
      frameStats={[frameOne]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[similarJockey, similarTrainer]}
    />,
  );
  fireEvent.click(screen.getByRole("radio", { name: "勝率+連対率+複勝率" }));
  expect(screen.getByRole("radio", { name: "勝率+連対率+複勝率" })).toHaveProperty("checked", true);
  expect(screen.getByRole("radio", { name: /^勝率$/ })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: /^連対率$/ })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: /^複勝率$/ })).toHaveProperty("checked", false);
  expect(screen.getAllByText("勝").length).toBe(8);
  expect(screen.getAllByText("連").length).toBe(8);
  expect(screen.getAllByText("複").length).toBe(8);
  expect(screen.getByText("15.0")).toBeDefined();
  expect(screen.getAllByText("30.0").length).toBe(2);
  expect(screen.getByText("45.0")).toBeDefined();
  expect(screen.queryByText("15.0%")).toBeNull();
  expect(screen.queryByText("30.0%")).toBeNull();
  expect(screen.queryByText("45.0%")).toBeNull();
  expect(
    screen.getByRole("figure", {
      name: "勝率、連対率、複勝率の色は0%から40%以上まで濃くなります",
    }),
  ).toBeDefined();
  expect(document.querySelector(".win-rate-heatmap-color-scale")?.className).toBe(
    "win-rate-heatmap-color-scale",
  );
  expect(document.querySelectorAll(".win-rate-heatmap-color-scale-bar").length).toBe(1);
  const combinedScaleBar = document.querySelector(".win-rate-heatmap-color-scale-bar");
  if (!(combinedScaleBar instanceof HTMLDivElement)) {
    throw new Error("expected combined color scale bar");
  }
  expect(combinedScaleBar.style.backgroundImage).toBe(
    "linear-gradient(to right, hsl(272, 22%, 96%) 0%, hsl(272, 40%, 79%) 25%, hsl(272, 59%, 62%) 50%, hsl(272, 77%, 45%) 75%, hsl(272, 95%, 28%) 100%)",
  );
  const combinedSwatches = document.querySelectorAll("td.win-rate-heatmap-swatch");
  const combinedWinSwatch = combinedSwatches[0];
  const combinedQuinellaSwatch = combinedSwatches[1];
  const combinedShowSwatch = combinedSwatches[2];
  if (!(combinedWinSwatch instanceof HTMLTableCellElement)) {
    throw new Error("expected combined win swatch");
  }
  if (!(combinedQuinellaSwatch instanceof HTMLTableCellElement)) {
    throw new Error("expected combined quinella swatch");
  }
  if (!(combinedShowSwatch instanceof HTMLTableCellElement)) {
    throw new Error("expected combined show swatch");
  }
  expect(combinedWinSwatch.style.backgroundColor).toBe("hsl(272, 49%, 71%)");
  expect(combinedQuinellaSwatch.style.backgroundColor).toBe("hsl(272, 77%, 45%)");
  expect(combinedShowSwatch.style.backgroundColor).toBe("hsl(272, 95%, 28%)");
});

it("writes a compact zero without a decimal in the combined heatmap cells", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[
        {
          ...frameOne,
          quinellaCount: 0,
          quinellaRate: 0,
          showCount: 0,
          showRate: 0,
          winCount: 0,
          winRate: 0,
        },
      ]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  fireEvent.click(screen.getByRole("radio", { name: "勝率+連対率+複勝率" }));
  expect(screen.getAllByText("0").length).toBe(3);
  expect(screen.queryByText("0.0%")).toBeNull();
  expect(screen.queryByText("0.0")).toBeNull();
});

it("truncates three-digit combined heatmap values to an integer", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[
        {
          ...frameOne,
          quinellaCount: 40,
          quinellaRate: 100,
          showCount: 40,
          showRate: 100.9,
          winCount: 40,
          winRate: 100,
        },
      ]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  fireEvent.click(screen.getByRole("radio", { name: "勝率+連対率+複勝率" }));
  expect(screen.getAllByText("100").length).toBe(3);
  expect(screen.queryByText("100.0")).toBeNull();
  expect(screen.queryByText("100.0%")).toBeNull();
  expect(screen.queryByText("100.9")).toBeNull();
});

it("shows computed frame win rate when the payload omits rate fields but includes counts", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[{ ...frameOne, winRate: Number.NaN }]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  expect(screen.getByText("15.0%")).toBeDefined();
});

it("renders a dash instead of throwing when frame win rate is not a finite number", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[
        {
          ...frameOne,
          count: Number.NaN,
          quinellaCount: Number.NaN,
          quinellaRate: Number.NaN,
          showCount: Number.NaN,
          showRate: Number.NaN,
          winCount: Number.NaN,
          winRate: Number.NaN,
        },
      ]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  expect(
    document.querySelector(".win-rate-heatmap-tooltip .frame-number-badge.frame-1"),
  ).toBeDefined();
});

it("shows missing frame rates as dashes when no matching frame row exists", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  expect(
    document.querySelector(".win-rate-heatmap-tooltip .frame-number-badge.frame-1"),
  ).toBeDefined();
  expect(screen.getAllByRole("tooltip").length).toBe(8);
});

it("does not pin a heatmap tooltip on click in desktop view", () => {
  installMatchMediaMock(false);
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[frameOne]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  const swatch = document.querySelector(".win-rate-heatmap-swatch");
  const button = document.querySelector(".win-rate-heatmap-swatch-button");
  if (!(swatch instanceof HTMLTableCellElement)) {
    throw new Error("expected heatmap swatch");
  }
  if (!(button instanceof HTMLButtonElement)) {
    throw new Error("expected heatmap swatch button");
  }
  fireEvent.click(button);
  expect(swatch.className).toBe("win-rate-heatmap-swatch win-rate-heatmap-tooltip-above");
  expect(button.getAttribute("aria-expanded")).toBe("false");
});

it("opens a heatmap tooltip on click and closes it on a second click in mobile view", () => {
  installMatchMediaMock(true);
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[frameOne]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  const swatch = document.querySelector(".win-rate-heatmap-swatch");
  const button = document.querySelector(".win-rate-heatmap-swatch-button");
  if (!(swatch instanceof HTMLTableCellElement)) {
    throw new Error("expected heatmap swatch");
  }
  if (!(button instanceof HTMLButtonElement)) {
    throw new Error("expected heatmap swatch button");
  }
  fireEvent.click(button);
  expect(swatch.className).toBe(
    "win-rate-heatmap-swatch win-rate-heatmap-tooltip-above tooltip-open",
  );
  expect(button.getAttribute("aria-expanded")).toBe("true");
  fireEvent.click(button);
  expect(swatch.className).toBe("win-rate-heatmap-swatch win-rate-heatmap-tooltip-above");
  expect(button.getAttribute("aria-expanded")).toBe("false");
});

it("keeps only one heatmap tooltip open when another cell is clicked in mobile view", () => {
  installMatchMediaMock(true);
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[frameOne]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  const swatches = document.querySelectorAll(".win-rate-heatmap-swatch");
  const buttons = document.querySelectorAll(".win-rate-heatmap-swatch-button");
  const firstSwatch = swatches[0];
  const secondSwatch = swatches[1];
  const firstButton = buttons[0];
  const secondButton = buttons[1];
  if (!(firstSwatch instanceof HTMLTableCellElement)) {
    throw new Error("expected first heatmap swatch");
  }
  if (!(secondSwatch instanceof HTMLTableCellElement)) {
    throw new Error("expected second heatmap swatch");
  }
  if (!(firstButton instanceof HTMLButtonElement)) {
    throw new Error("expected first heatmap swatch button");
  }
  if (!(secondButton instanceof HTMLButtonElement)) {
    throw new Error("expected second heatmap swatch button");
  }
  fireEvent.click(firstButton);
  expect(firstSwatch.className).toBe(
    "win-rate-heatmap-swatch win-rate-heatmap-tooltip-above tooltip-open",
  );
  fireEvent.click(secondButton);
  expect(firstSwatch.className).toBe("win-rate-heatmap-swatch win-rate-heatmap-tooltip-above");
  expect(secondSwatch.className).toBe(
    "win-rate-heatmap-swatch win-rate-heatmap-tooltip-above tooltip-open",
  );
  expect(document.querySelectorAll(".win-rate-heatmap-swatch.tooltip-open").length).toBe(1);
});

it("keeps the mobile tooltip open when pointerdown stays on a swatch button", () => {
  installMatchMediaMock(true);
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[frameOne]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  const swatch = document.querySelector(".win-rate-heatmap-swatch");
  const button = document.querySelector(".win-rate-heatmap-swatch-button");
  if (!(swatch instanceof HTMLTableCellElement)) {
    throw new Error("expected heatmap swatch");
  }
  if (!(button instanceof HTMLButtonElement)) {
    throw new Error("expected heatmap swatch button");
  }
  fireEvent.click(button);
  fireEvent.pointerDown(button);
  expect(swatch.className).toBe(
    "win-rate-heatmap-swatch win-rate-heatmap-tooltip-above tooltip-open",
  );
});

it("closes the open mobile heatmap tooltip when clicking outside the swatches", () => {
  installMatchMediaMock(true);
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[frameOne]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  const swatch = document.querySelector(".win-rate-heatmap-swatch");
  const button = document.querySelector(".win-rate-heatmap-swatch-button");
  if (!(swatch instanceof HTMLTableCellElement)) {
    throw new Error("expected heatmap swatch");
  }
  if (!(button instanceof HTMLButtonElement)) {
    throw new Error("expected heatmap swatch button");
  }
  fireEvent.click(button);
  expect(swatch.className).toBe(
    "win-rate-heatmap-swatch win-rate-heatmap-tooltip-above tooltip-open",
  );
  fireEvent.pointerDown(document.body);
  expect(swatch.className).toBe("win-rate-heatmap-swatch win-rate-heatmap-tooltip-above");
});

it("closes the pinned mobile tooltip when the viewport becomes desktop", () => {
  const controller = installMatchMediaMock(true);
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[frameOne]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  const swatch = document.querySelector(".win-rate-heatmap-swatch");
  const button = document.querySelector(".win-rate-heatmap-swatch-button");
  if (!(swatch instanceof HTMLTableCellElement)) {
    throw new Error("expected heatmap swatch");
  }
  if (!(button instanceof HTMLButtonElement)) {
    throw new Error("expected heatmap swatch button");
  }
  fireEvent.click(button);
  expect(swatch.className).toBe(
    "win-rate-heatmap-swatch win-rate-heatmap-tooltip-above tooltip-open",
  );
  act(() => {
    controller.fire(false);
  });
  expect(swatch.className).toBe("win-rate-heatmap-swatch win-rate-heatmap-tooltip-above");
});

it("does not pin a heatmap tooltip when matchMedia is unavailable", () => {
  vi.stubGlobal("matchMedia", undefined);
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[frameOne]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  const swatch = document.querySelector(".win-rate-heatmap-swatch");
  const button = document.querySelector(".win-rate-heatmap-swatch-button");
  if (!(swatch instanceof HTMLTableCellElement)) {
    throw new Error("expected heatmap swatch");
  }
  if (!(button instanceof HTMLButtonElement)) {
    throw new Error("expected heatmap swatch button");
  }
  fireEvent.click(button);
  expect(swatch.className).toBe("win-rate-heatmap-swatch win-rate-heatmap-tooltip-above");
});

it("subscribes with addListener when addEventListener is missing", () => {
  const listeners = new Set<(event: MockMediaQueryEvent) => void>();
  const mediaQueryList = {
    addListener: (listener: (event: MockMediaQueryEvent) => void) => {
      listeners.add(listener);
    },
    matches: true,
    removeListener: (listener: (event: MockMediaQueryEvent) => void) => {
      listeners.delete(listener);
    },
  };
  vi.stubGlobal(
    "matchMedia",
    vi.fn(() => mediaQueryList),
  );
  const { unmount } = render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[frameOne]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  expect(listeners.size).toBe(1);
  unmount();
  expect(listeners.size).toBe(0);
});

it("shows heatmap start counts in the tooltip when the レース数 checkbox is checked", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[bloodlineSire]}
      frameStats={[frameOne]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[similarJockey, similarTrainer]}
    />,
  );
  fireEvent.click(screen.getByRole("checkbox", { name: "レース数" }));
  expect(document.querySelector(".win-rate-heatmap-swatch-value")?.textContent).toBe("15.0%");
  expect(
    [...document.querySelectorAll(".win-rate-heatmap-tooltip-starts")].map(
      (node) => node.textContent,
    ),
  ).toStrictEqual(["(40)", "(80)", "(50)", "(200)"]);
});

it("hides the 斤量 column for ばんえい races", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[]}
      horseResults={[]}
      keibajoCode="83"
      realtimeRequest={{
        apiBaseUrl: "https://realtime.test",
        day: "21",
        keibajoCode: "83",
        month: "08",
        raceNumber: "11",
        source: "nar",
        year: "2026",
      }}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  expect(screen.queryByText("斤量")).toBeNull();
  expect(screen.getAllByText("勝").length).toBe(7);
});

it("hides the horse-weight column for overseas races even when a runner has a weight", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[]}
      horseResults={[horsePastWin]}
      keibajoCode="A8"
      realtimeRequest={{
        apiBaseUrl: "https://realtime.test",
        day: "21",
        keibajoCode: "A8",
        month: "08",
        raceNumber: "11",
        source: "jra",
        year: "2026",
      }}
      runners={[{ ...runner, bataiju: "480" }]}
      similarRows={[]}
    />,
  );
  expect(screen.queryByText("馬体重")).toBeNull();
  expect(screen.getAllByText("勝").length).toBe(7);
});

it("shows the horse-weight column after 枠 when a domestic runner has a published weight", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[frameOne]}
      horseResults={[horsePastWin]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[{ ...runner, bataiju: "485" }]}
      similarRows={[]}
    />,
  );
  expect(screen.getByText("馬体重")).toBeDefined();
  expect(screen.getByText("斤量")).toBeDefined();
  expect(screen.getAllByText("勝").length).toBe(9);
  expect(screen.getAllByText("100.0%").length).toBe(3);
  expect(screen.getByText("480-499kg")).toBeDefined();
  expect(screen.getByText("55.5kg以上57kg以下")).toBeDefined();
  const headings = [...document.querySelectorAll("thead tr:first-child th")].map(
    (heading) => heading.textContent,
  );
  expect(headings).toStrictEqual([
    "番",
    "枠",
    "馬体重",
    "斤量",
    "馬",
    "騎手",
    "調教師",
    "父",
    "母父",
    "父父",
  ]);
});

it("points the last-row heatmap tooltip up and leaves earlier rows pointing down", () => {
  installMatchMediaMock(true);
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[frameOne]}
      horseResults={[]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner, { ...runner, bamei: "Beta", kettoTorokuBango: "2020100002", umaban: "02" }]}
      similarRows={[]}
    />,
  );
  const firstRowSwatch = document.querySelector("tbody tr:first-child td.win-rate-heatmap-swatch");
  const lastRowSwatch = document.querySelector("tbody tr:last-child td.win-rate-heatmap-swatch");
  if (!(firstRowSwatch instanceof HTMLTableCellElement)) {
    throw new Error("expected first-row heatmap swatch");
  }
  if (!(lastRowSwatch instanceof HTMLTableCellElement)) {
    throw new Error("expected last-row heatmap swatch");
  }
  expect(firstRowSwatch.className).toBe("win-rate-heatmap-swatch");
  expect(lastRowSwatch.className).toBe("win-rate-heatmap-swatch win-rate-heatmap-tooltip-above");
  const firstRowButton = firstRowSwatch.querySelector(".win-rate-heatmap-swatch-button");
  if (!(firstRowButton instanceof HTMLButtonElement)) {
    throw new Error("expected first-row heatmap swatch button");
  }
  fireEvent.click(firstRowButton);
  expect(firstRowSwatch.className).toBe("win-rate-heatmap-swatch tooltip-open");
});

it("shows the horse-weight column from the live weight stream when stored bataiju is still empty", () => {
  vi.mocked(useHorseWeightStream).mockReturnValue(liveWeightSnapshot);
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[frameOne]}
      horseResults={[horsePastWin]}
      keibajoCode="05"
      realtimeRequest={heatmapRealtimeRequest}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  expect(screen.getByText("馬体重")).toBeDefined();
  expect(screen.getByText("480-499kg")).toBeDefined();
});

it("shows similar-race weight-class rates for a blank NAR bataiju plus live kilograms", () => {
  vi.mocked(useHorseWeightStream).mockReturnValue({
    fetchedAt: "2026-08-21T13:58:41+09:00",
    horses: [
      {
        changeAmount: 6,
        changeSign: "+",
        horseName: "Alpha",
        horseNumber: "1",
        weight: 453,
      },
    ],
  });
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[frameOne]}
      horseResults={[horsePastWin]}
      keibajoCode="48"
      realtimeRequest={{
        apiBaseUrl: "https://realtime.test",
        day: "21",
        keibajoCode: "48",
        month: "08",
        raceNumber: "01",
        source: "nar",
        year: "2026",
      }}
      runners={[runner]}
      similarRows={[]}
      weightClassStats={[
        {
          key: "440-459",
          quinellaCount: 20,
          quinellaRate: 25,
          showCount: 32,
          showRate: 40,
          starts: 80,
          winCount: 12,
          winRate: 15,
        },
      ]}
    />,
  );
  expect(screen.getByText("馬体重")).toBeDefined();
  expect(screen.getByText("440-459kg")).toBeDefined();
  expect(screen.getAllByText("15.0%").length).toBe(2);
  expect(screen.queryByText("0.0%")).toBeNull();
});
