import { cleanup, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, it } from "vitest";

import { RaceCalendar } from "./calendar";

afterEach(cleanup);

describe("race calendar", () => {
  it("renders year navigation, summary, and month day links", () => {
    render(
      <RaceCalendar
        selectedYear="2026"
        years={[
          { dayCount: 2, raceCount: 48, year: "2026" },
          { dayCount: 1, raceCount: 12, year: "2025" },
        ]}
        days={[
          { day: "10", jraCount: 12, month: "05", narCount: 2, year: "2026" },
          { day: "11", jraCount: 10, month: "05", narCount: 1, year: "2026" },
          { day: "01", jraCount: 0, month: "06", narCount: 8, year: "2026" },
        ]}
      />,
    );

    expect(screen.getByRole("heading", { level: 1, name: "2026年 開催日一覧" })).toBeTruthy();
    expect(screen.getByLabelText("summary").textContent).toContain("3 日");
    expect(screen.getByLabelText("summary").textContent).toContain("33 レース");
    expect(screen.getByRole("link", { name: /2026/ }).getAttribute("aria-current")).toBe("page");
    expect(screen.getByRole("link", { name: /10日/ }).getAttribute("href")).toBe(
      "/races/2026/05/10",
    );
    expect(screen.getByText("6月")).toBeTruthy();
  });
});
