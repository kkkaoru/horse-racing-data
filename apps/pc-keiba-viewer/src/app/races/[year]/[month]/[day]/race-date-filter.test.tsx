import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, it } from "vitest";

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
    kyosoShubetsuCode: "12",
  }),
  race({
    hassoJikoku: "1100",
    keibajoCode: "44",
    kyori: "1200",
    kyosomeiHondai: "地方特別",
    raceBango: "03",
    source: "nar",
    trackCode: "24",
  }),
];

afterEach(cleanup);

describe("race date filter", () => {
  it("renders grouped races and generated tags", () => {
    render(<RaceDateFilter day="10" month="05" races={races} year="2026" />);

    expect(screen.getByText("3 / 3 レース")).toBeTruthy();
    expect(screen.getByRole("heading", { name: "JRA 中央競馬" })).toBeTruthy();
    expect(screen.getByRole("heading", { name: "NAR 地方競馬" })).toBeTruthy();
    expect(screen.getByText("一般競走")).toBeTruthy();
    expect(screen.getAllByText("牝馬限定").length).toBeGreaterThanOrEqual(2);
    expect(screen.getByRole("link", { name: /新潟 1R/ }).getAttribute("href")).toBe(
      "/races/detail/jra/2026/05/10/04/01",
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
});
