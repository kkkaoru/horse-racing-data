// Run with: bun run test src/app/races/detail/corner-position-section.test.tsx

import { cleanup, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, test } from "vitest";

import type { RaceCornerPositionRow } from "../../../db/corner-running-style-queries";
import {
  CornerPositionSection,
  formatPercent,
  formatRank,
  STRAIGHT_COURSE_BANNER,
} from "./corner-position-section";

const buildRow = (overrides: Partial<RaceCornerPositionRow>): RaceCornerPositionRow => ({
  corner1Pred: 0.2,
  corner3Pred: 0.25,
  corner4Pred: 0.3,
  kaisaiNen: "2025",
  kaisaiTsukihi: "0517",
  keibajoCode: "05",
  kettoTorokuBango: "2020100001",
  modelVersion: "jra-corner-v1.0",
  raceBango: "11",
  source: "jra",
  umaban: 1,
  ...overrides,
});

afterEach(cleanup);

describe("formatRank", () => {
  test("returns - for null values", () => {
    expect(formatRank(null, 10)).toBe("-");
  });

  test("returns - when only one horse", () => {
    expect(formatRank(0.5, 1)).toBe("-");
  });

  test("returns the 1-indexed rank for the leader (normalized=0)", () => {
    expect(formatRank(0, 10)).toBe("1.0");
  });

  test("returns the field size for the tail runner (normalized=1)", () => {
    expect(formatRank(1, 10)).toBe("10.0");
  });
});

describe("formatPercent", () => {
  test("returns - for null", () => {
    expect(formatPercent(null)).toBe("-");
  });

  test("rounds to whole percent", () => {
    expect(formatPercent(0.327)).toBe("33%");
    expect(formatPercent(0.5)).toBe("50%");
  });
});

describe("CornerPositionSection - straight course banner", () => {
  test("shows the straight-course banner when isStraightCourse is true", () => {
    render(
      <CornerPositionSection
        rows={[]}
        meanMae={null}
        modelVersion="v1"
        isStraightCourse={true}
        bameiByUmaban={{}}
      />,
    );
    expect(screen.getByText(STRAIGHT_COURSE_BANNER)).toBeTruthy();
  });

  test("does not show the prediction table when straight course", () => {
    render(
      <CornerPositionSection
        rows={[buildRow({})]}
        meanMae={null}
        modelVersion="v1"
        isStraightCourse={true}
        bameiByUmaban={{ 1: "テスト" }}
      />,
    );
    expect(screen.queryByText("馬番")).toBe(null);
  });
});

describe("CornerPositionSection - empty state", () => {
  test("shows an empty placeholder when rows is empty and course is not straight", () => {
    render(
      <CornerPositionSection
        rows={[]}
        meanMae={null}
        modelVersion={null}
        isStraightCourse={false}
        bameiByUmaban={{}}
      />,
    );
    expect(screen.getByText("このレースのコーナー予測データはまだありません。")).toBeTruthy();
  });
});

describe("CornerPositionSection - populated table", () => {
  test("renders the table with horses sorted by umaban", () => {
    render(
      <CornerPositionSection
        rows={[
          buildRow({ umaban: 3, kettoTorokuBango: "h3", corner1Pred: 0.2 }),
          buildRow({ umaban: 1, kettoTorokuBango: "h1", corner1Pred: 0.0 }),
          buildRow({ umaban: 2, kettoTorokuBango: "h2", corner1Pred: 0.5 }),
        ]}
        meanMae={1.42}
        modelVersion="jra-corner-v1.0"
        isStraightCourse={false}
        bameiByUmaban={{ 1: "馬A", 2: "馬B", 3: "馬C" }}
      />,
    );
    const tableRows = screen.getAllByRole("row");
    expect(tableRows[1]?.textContent).toContain("馬A");
    expect(tableRows[2]?.textContent).toContain("馬B");
    expect(tableRows[3]?.textContent).toContain("馬C");
  });

  test("falls back to 馬名不明 when bamei is missing", () => {
    render(
      <CornerPositionSection
        rows={[buildRow({ umaban: 5 })]}
        meanMae={null}
        modelVersion="v1"
        isStraightCourse={false}
        bameiByUmaban={{}}
      />,
    );
    expect(screen.getByText("馬名不明")).toBeTruthy();
  });

  test("renders the metrics badge when modelVersion is present", () => {
    render(
      <CornerPositionSection
        rows={[buildRow({})]}
        meanMae={1.42}
        modelVersion="jra-corner-v1.0"
        isStraightCourse={false}
        bameiByUmaban={{ 1: "テスト" }}
      />,
    );
    expect(screen.getByText(/jra-corner-v1\.0/u)).toBeTruthy();
    expect(screen.getByText(/平均 MAE: 1\.420/u)).toBeTruthy();
  });

  test("omits the metrics badge when modelVersion is null", () => {
    render(
      <CornerPositionSection
        rows={[buildRow({})]}
        meanMae={null}
        modelVersion={null}
        isStraightCourse={false}
        bameiByUmaban={{ 1: "テスト" }}
      />,
    );
    expect(screen.queryByText(/モデル:/u)).toBe(null);
  });
});
