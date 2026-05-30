import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { RaceListItem } from "../../../../../lib/race-types";
import { RaceDateFilter } from "./race-date-filter";

const race = (overrides: Partial<RaceListItem>): RaceListItem => ({
  gradeCode: null,
  hassoJikoku: null,
  kaisaiNen: "2026",
  kaisaiTsukihi: "0510",
  keibajoCode: "04",
  kyori: "1800",
  kyosomeiFukudai: null,
  kyosomeiHondai: null,
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosoShubetsuCode: null,
  juryoShubetsuCode: null,
  jockeyNames: [],
  raceBango: "01",
  shussoTosu: "15",
  source: "jra",
  trackCode: "23",
  ...overrides,
});

const races: RaceListItem[] = [
  race({
    hassoJikoku: "0945",
    kyosomeiHondai: null,
    kyosoJokenCode: "703",
    kyosoKigoCode: "023",
    kyosoShubetsuCode: "02",
    jockeyNames: ["新潟騎手"],
  }),
  race({
    hassoJikoku: "1015",
    keibajoCode: "05",
    kyori: "1600",
    kyosomeiHondai: "ＮＨＫマイルカップ",
    raceBango: "11",
    source: "jra",
    trackCode: "11",
    gradeCode: "A",
    jockeyNames: ["東京太郎", "東京次郎"],
    kyosoShubetsuCode: "12",
  }),
  race({
    hassoJikoku: "1100",
    keibajoCode: "44",
    kyori: "1200",
    kyosomeiHondai: "地方特別",
    raceBango: "03",
    jockeyNames: ["地方花子"],
    source: "nar",
    trackCode: "24",
  }),
];

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe("race date filter", () => {
  it("renders grouped races and generated tags", () => {
    render(<RaceDateFilter day="10" month="05" races={races} year="2026" />);

    expect(screen.getByText("3 / 3 レース")).toBeTruthy();
    expect(screen.getByRole("heading", { name: "JRA 中央競馬" })).toBeTruthy();
    expect(screen.getByRole("heading", { name: "NAR 地方競馬" })).toBeTruthy();
    expect(screen.getByText("一般競走")).toBeTruthy();
    expect(screen.getAllByText("牝馬限定").length).toBeGreaterThanOrEqual(2);
    expect(screen.getByRole("link", { name: /新潟 1R/ }).getAttribute("href")).toBe(
      "/races/2026/05/10/04/01",
    );
  });

  it("filters by source, venue, tag, and keyword, then resets", () => {
    render(<RaceDateFilter day="10" month="05" races={races} year="2026" />);

    const filters = screen.getByLabelText("race filters");
    const selects = within(filters).getAllByRole("combobox");
    const sourceSelect = selects.at(0);
    const venueSelect = selects.at(1);
    const tagSelect = selects.at(2);
    if (!sourceSelect || !venueSelect || !tagSelect) {
      throw new Error("Expected all filter select controls to be rendered.");
    }
    fireEvent.change(sourceSelect, { target: { value: "nar" } });
    expect(screen.getByText("1 / 3 レース")).toBeTruthy();
    expect(screen.queryByText("ＮＨＫマイルカップ")).toBeNull();

    fireEvent.change(sourceSelect, { target: { value: "all" } });
    fireEvent.change(venueSelect, { target: { value: "05" } });
    expect(screen.getByText("1 / 3 レース")).toBeTruthy();
    expect(screen.getByText("ＮＨＫマイルカップ")).toBeTruthy();

    fireEvent.change(venueSelect, { target: { value: "all" } });
    fireEvent.change(tagSelect, { target: { value: "牝馬限定" } });
    expect(screen.getByText("1 / 3 レース")).toBeTruthy();
    expect(screen.getByText("一般競走")).toBeTruthy();

    fireEvent.change(tagSelect, { target: { value: "all" } });
    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "東京" } });
    expect(screen.getByText("1 / 3 レース")).toBeTruthy();
    expect(screen.getByText("ＮＨＫマイルカップ")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "リセット" }));
    expect(screen.getByText("3 / 3 レース")).toBeTruthy();
  });

  it("shows an empty state when no race matches", () => {
    render(<RaceDateFilter day="10" month="05" races={races} year="2026" />);

    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "存在しない" } });
    expect(screen.getByText("0 / 3 レース")).toBeTruthy();
    expect(screen.getByText("条件に一致するレースはありません。")).toBeTruthy();
  });

  it("filters by start time, end time, and selected jockeys", () => {
    const { container } = render(<RaceDateFilter day="10" month="05" races={races} year="2026" />);

    fireEvent.change(screen.getByLabelText("開始時間"), { target: { value: "10:00" } });
    expect(screen.getByText("2 / 3 レース")).toBeTruthy();
    expect(screen.queryByText("一般競走")).toBeNull();

    fireEvent.change(screen.getByLabelText("終了時間"), { target: { value: "10:30" } });
    expect(screen.getByText("1 / 3 レース")).toBeTruthy();
    expect(screen.getByText("ＮＨＫマイルカップ")).toBeTruthy();

    fireEvent.change(screen.getByLabelText("騎手"), { target: { value: "東京太郎" } });
    const jockeySuggestionList = container.querySelector(".filter-jockey-listbox");
    if (!(jockeySuggestionList instanceof HTMLElement)) {
      throw new Error("jockey suggestion list not rendered");
    }
    fireEvent.mouseDown(within(jockeySuggestionList).getByText("東京太郎"));
    expect(screen.getByLabelText("selected jockey filters").textContent).toContain("東京太郎");
    expect(screen.getByText("1 / 3 レース")).toBeTruthy();

    const selectedJockeysList = screen.getByLabelText("selected jockey filters");
    fireEvent.click(within(selectedJockeysList).getByText(/東京太郎/));
    expect(screen.queryByLabelText("selected jockey filters")).toBeNull();
  });

  it("automatically advances start time on 10-minute boundaries when it has a value", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-05-10T10:09:30+09:00"));

    render(
      <RaceDateFilter day="10" defaultStartTime="10:09" month="05" races={races} year="2026" />,
    );

    expect(screen.getByLabelText("開始時間")).toHaveProperty("value", "10:09");

    act(() => {
      vi.advanceTimersByTime(30_000);
    });

    expect(screen.getByLabelText("開始時間")).toHaveProperty("value", "10:10");
    expect(screen.getByText("2 / 3 レース")).toBeTruthy();

    act(() => {
      vi.advanceTimersByTime(10 * 60 * 1000);
    });

    expect(screen.getByLabelText("開始時間")).toHaveProperty("value", "10:20");
    expect(screen.getByText("1 / 3 レース")).toBeTruthy();
  });

  it("filters by surface and distance range when JRA races are present", () => {
    render(<RaceDateFilter day="10" month="05" races={races} year="2026" />);

    fireEvent.change(screen.getByLabelText("馬場"), { target: { value: "turf" } });
    expect(screen.getByText("1 / 3 レース")).toBeTruthy();
    expect(screen.getByText("ＮＨＫマイルカップ")).toBeTruthy();
    expect(screen.queryByText("地方特別")).toBeNull();

    fireEvent.change(screen.getByLabelText("馬場"), { target: { value: "all" } });
    fireEvent.change(screen.getByLabelText("距離 下限"), { target: { value: "1500" } });
    fireEvent.change(screen.getByLabelText("距離 上限"), { target: { value: "1700" } });
    expect(screen.getByText("1 / 3 レース")).toBeTruthy();
    expect(screen.getByText("ＮＨＫマイルカップ")).toBeTruthy();
    expect(screen.queryByText("一般競走")).toBeNull();
    expect(screen.getByLabelText("距離 下限").getAttribute("step")).toBe("100");
    expect(screen.getByLabelText("距離 上限").getAttribute("step")).toBe("100");
  });

  it("hides surface filter when JRA races are absent", () => {
    render(
      <RaceDateFilter
        day="10"
        month="05"
        races={[race({ keibajoCode: "44", source: "nar", trackCode: "24" })]}
        year="2026"
      />,
    );

    expect(screen.queryByLabelText("馬場")).toBeNull();
    expect(screen.getByLabelText("距離 下限")).toBeTruthy();
    expect(screen.getByLabelText("距離 上限")).toBeTruthy();
  });

  it("supports keyboard selection for jockey suggestions", () => {
    const { container } = render(<RaceDateFilter day="10" month="05" races={races} year="2026" />);

    const jockeyInput = screen.getByLabelText("騎手");
    fireEvent.focus(jockeyInput);
    const openedListbox = container.querySelector(".filter-jockey-listbox");
    if (!(openedListbox instanceof HTMLElement)) {
      throw new Error("jockey suggestion list did not open on focus");
    }
    expect(within(openedListbox).getByText("地方花子")).toBeTruthy();

    fireEvent.keyDown(jockeyInput, { key: "Escape" });
    expect(container.querySelector(".filter-jockey-listbox")).toBeNull();

    fireEvent.change(jockeyInput, { target: { value: "東京" } });
    fireEvent.keyDown(jockeyInput, { key: "ArrowDown" });
    fireEvent.keyDown(jockeyInput, { key: "ArrowUp" });
    fireEvent.keyDown(jockeyInput, { key: "ArrowDown" });
    fireEvent.keyDown(jockeyInput, { key: "Enter" });

    expect(screen.getByLabelText("selected jockey filters").textContent).toContain("東京太郎");
    expect(screen.getByText("1 / 3 レース")).toBeTruthy();
  });

  it("excludes races without start times when time filters are set", () => {
    render(
      <RaceDateFilter
        day="10"
        month="05"
        races={[...races, race({ kyosomeiHondai: "時刻未定", raceBango: "02" })]}
        year="2026"
      />,
    );

    expect(screen.getByText("4 / 4 レース")).toBeTruthy();

    fireEvent.change(screen.getByLabelText("開始時間"), { target: { value: "09:00" } });
    expect(screen.getByText("3 / 4 レース")).toBeTruthy();
    expect(screen.queryByText("時刻未定")).toBeNull();
  });

  it("excludes races with malformed or impossible start times when time filters are set", () => {
    render(
      <RaceDateFilter
        day="10"
        month="05"
        races={[
          race({ hassoJikoku: "0905", kyosomeiHondai: "有効時刻", raceBango: "01" }),
          race({ hassoJikoku: "abcd", kyosomeiHondai: "不正時刻", raceBango: "02" }),
          race({ hassoJikoku: "2460", kyosomeiHondai: "範囲外時刻", raceBango: "03" }),
        ]}
        year="2026"
      />,
    );

    fireEvent.change(screen.getByLabelText("開始時間"), { target: { value: "09:00" } });

    expect(screen.getByText("1 / 3 レース")).toBeTruthy();
    expect(screen.getByText("有効時刻")).toBeTruthy();
    expect(screen.queryByText("不正時刻")).toBeNull();
    expect(screen.queryByText("範囲外時刻")).toBeNull();
  });

  it("keeps all races visible when time filter values are invalid", () => {
    render(<RaceDateFilter day="10" month="05" races={races} year="2026" />);

    fireEvent.change(screen.getByLabelText("開始時間"), { target: { value: "99:99" } });
    fireEvent.change(screen.getByLabelText("終了時間"), { target: { value: "aa:bb" } });

    expect(screen.getByText("3 / 3 レース")).toBeTruthy();
  });

  it("shows empty jockey suggestions when no candidate matches", () => {
    render(<RaceDateFilter day="10" month="05" races={races} year="2026" />);

    fireEvent.change(screen.getByLabelText("騎手"), { target: { value: "該当なし" } });

    expect(screen.getByText("候補なし")).toBeTruthy();
  });

  it("keeps venue fixed on venue pages", () => {
    render(<RaceDateFilter day="10" fixedVenueCode="05" month="05" races={races} year="2026" />);

    expect(screen.queryByText("競馬場")).toBeNull();
    expect(screen.getByText("1 / 3 レース")).toBeTruthy();
    expect(screen.getByText("ＮＨＫマイルカップ")).toBeTruthy();
    expect(screen.queryByText("地方特別")).toBeNull();
  });

  it("restores filters from URL query values", () => {
    render(
      <RaceDateFilter
        day="10"
        initialSearchParams={{
          jockey: "東京太郎",
          maxDistance: "1700",
          minDistance: "1500",
          q: "東京",
          source: "jra",
          surface: "turf",
        }}
        month="05"
        races={races}
        year="2026"
      />,
    );

    expect(screen.getByText("1 / 3 レース")).toBeTruthy();
    expect(screen.getByText("ＮＨＫマイルカップ")).toBeTruthy();
    expect(screen.getByLabelText("selected jockey filters").textContent).toContain("東京太郎");
    expect(screen.getByLabelText("距離 下限")).toHaveProperty("value", "1500");
    expect(screen.getByLabelText("距離 上限")).toHaveProperty("value", "1700");
  });

  it("normalizes invalid URL query filter values and fixed venue defaults", () => {
    render(
      <RaceDateFilter
        day="10"
        fixedVenueCode="05"
        initialSearchParams={{
          jockey: ["地方花子", "東京太郎"],
          source: "invalid",
          surface: "invalid",
          venue: "44",
        }}
        month="05"
        races={races}
        year="2026"
      />,
    );

    expect(screen.queryByText("競馬場")).toBeNull();
    expect(screen.getByText("1 / 3 レース")).toBeTruthy();
    expect(screen.getByText("ＮＨＫマイルカップ")).toBeTruthy();
    expect(screen.getByLabelText("selected jockey filters").textContent).toContain("地方花子");
    expect(screen.getByLabelText("selected jockey filters").textContent).toContain("東京太郎");
  });

  it("handles empty race lists", () => {
    render(<RaceDateFilter day="10" month="05" races={[]} year="2026" />);

    expect(screen.getByText("0 / 0 レース")).toBeTruthy();
    expect(screen.getByText("条件に一致するレースはありません。")).toBeTruthy();
  });

  it("updates query string without navigating when filter values change", async () => {
    window.history.pushState(null, "", "/races/2026/05/10");
    render(<RaceDateFilter day="10" month="05" races={races} year="2026" />);

    fireEvent.change(screen.getByLabelText("馬場"), { target: { value: "turf" } });
    fireEvent.change(screen.getByLabelText("距離 下限"), { target: { value: "1500" } });
    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "東京" } });

    await waitFor(() => {
      expect(window.location.search).toContain("surface=turf");
      expect(window.location.search).toContain("minDistance=1500");
      expect(window.location.search).toContain("q=%E6%9D%B1%E4%BA%AC");
    });
  });
});
