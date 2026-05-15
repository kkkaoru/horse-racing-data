import { cleanup, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, it } from "vitest";

import type { EntityRaceResult } from "../lib/race-types";
import { EntityRaceResultsTable } from "./entity-ui";

const row = (overrides: Partial<EntityRaceResult>): EntityRaceResult => ({
  frameNumber: "1",
  horseName: "テストホース",
  horseNumber: "1",
  hassoJikoku: "1010",
  isUpcoming: false,
  corner1: "03",
  corner2: "04",
  corner3: "05",
  corner4: "06",
  jockeyName: "騎手",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0322",
  keibajoCode: "05",
  kettoTorokuBango: "2017100438",
  kyori: "1800",
  last3f: "378",
  popularity: "01",
  ownerName: "馬主",
  raceBango: "01",
  raceName: "テストレース",
  raceTime: "1123",
  rank: "01",
  source: "jra",
  trackCode: "24",
  trainerName: "調教師",
  winOdds: "012",
  ...overrides,
});

afterEach(cleanup);

describe("entity race results table", () => {
  it("shows race time and last 3F for non-ban-ei horse detail rows", () => {
    render(<EntityRaceResultsTable rows={[row({})]} showRaceTimeColumns />);

    expect(screen.getByRole("columnheader", { name: "レースタイム" })).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "上がり3F" })).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "コーナー順位" })).toBeTruthy();
    expect(screen.getByText("1:52.3")).toBeTruthy();
    expect(screen.getByText("37.8")).toBeTruthy();
    expect(screen.getByText("3-4-5-6")).toBeTruthy();
  });

  it("keeps race time and hides last 3F for ban-ei horse detail rows", () => {
    render(
      <EntityRaceResultsTable
        rows={[row({ keibajoCode: "83", raceTime: "3188", source: "nar", trackCode: "90" })]}
        showRaceTimeColumns
      />,
    );

    expect(screen.getByRole("columnheader", { name: "レースタイム" })).toBeTruthy();
    expect(screen.queryByRole("columnheader", { name: "上がり3F" })).toBeNull();
    expect(screen.getByText("3:18.8")).toBeTruthy();
    expect(screen.queryByText("37.8")).toBeNull();
  });

  it("shows a dash when all corner ranks are missing", () => {
    render(
      <EntityRaceResultsTable
        rows={[row({ corner1: "00", corner2: null, corner3: "", corner4: "00" })]}
      />,
    );

    expect(screen.getByRole("columnheader", { name: "コーナー順位" })).toBeTruthy();
    expect(screen.getAllByText("-").length).toBeGreaterThan(0);
  });
});
