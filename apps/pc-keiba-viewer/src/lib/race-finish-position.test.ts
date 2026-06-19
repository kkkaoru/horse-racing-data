import { describe, expect, it } from "vitest";

import { buildD1FinishMap, normalizeD1FinishPosition } from "./race-finish-position";

describe("normalizeD1FinishPosition", () => {
  it("returns null for null", () => {
    expect(normalizeD1FinishPosition(null)).toBeNull();
  });

  it("returns null for undefined", () => {
    expect(normalizeD1FinishPosition(undefined)).toBeNull();
  });

  it("returns null for an empty string", () => {
    expect(normalizeD1FinishPosition("")).toBeNull();
  });

  it("returns null for a whitespace-only string", () => {
    expect(normalizeD1FinishPosition("   ")).toBeNull();
  });

  it("returns null for a single zero", () => {
    expect(normalizeD1FinishPosition("0")).toBeNull();
  });

  it("returns null for a double zero", () => {
    expect(normalizeD1FinishPosition("00")).toBeNull();
  });

  it("returns null for a triple zero", () => {
    expect(normalizeD1FinishPosition("000")).toBeNull();
  });

  it("returns null for a non-numeric status", () => {
    expect(normalizeD1FinishPosition("中止")).toBeNull();
  });

  it("returns null for a negative value", () => {
    expect(normalizeD1FinishPosition("-1")).toBeNull();
  });

  it("returns the trimmed value for a single-digit finish", () => {
    expect(normalizeD1FinishPosition("1")).toBe("1");
  });

  it("returns the trimmed value for a padded finish surrounded by spaces", () => {
    expect(normalizeD1FinishPosition(" 3 ")).toBe("3");
  });

  it("returns the trimmed value for a double-digit finish", () => {
    expect(normalizeD1FinishPosition("18")).toBe("18");
  });

  it("keeps a leading-zero finish as its trimmed text", () => {
    expect(normalizeD1FinishPosition("02")).toBe("02");
  });
});

describe("buildD1FinishMap", () => {
  it("returns an empty map for no entries", () => {
    expect(buildD1FinishMap([])).toStrictEqual(new Map());
  });

  it("keys a single placed finish by its formatted horse number", () => {
    expect(buildD1FinishMap([{ finishPosition: "1", horseNumber: "01" }])).toStrictEqual(
      new Map([["1", "1"]]),
    );
  });

  it("normalizes a padded horse number and a whitespace finish together", () => {
    expect(buildD1FinishMap([{ finishPosition: " 2 ", horseNumber: " 07 " }])).toStrictEqual(
      new Map([["7", "2"]]),
    );
  });

  it("drops an entry whose finish is the all-zero placeholder", () => {
    expect(buildD1FinishMap([{ finishPosition: "0", horseNumber: "05" }])).toStrictEqual(new Map());
  });

  it("drops an entry whose finish is empty", () => {
    expect(buildD1FinishMap([{ finishPosition: "", horseNumber: "05" }])).toStrictEqual(new Map());
  });

  it("drops an entry whose finish is a non-numeric status", () => {
    expect(buildD1FinishMap([{ finishPosition: "中止", horseNumber: "05" }])).toStrictEqual(
      new Map(),
    );
  });

  it("drops an entry whose horse number is unparseable", () => {
    expect(buildD1FinishMap([{ finishPosition: "1", horseNumber: "abc" }])).toStrictEqual(
      new Map(),
    );
  });

  it("drops an entry whose horse number is empty", () => {
    expect(buildD1FinishMap([{ finishPosition: "1", horseNumber: "" }])).toStrictEqual(new Map());
  });

  it("keeps only the placed finishes from a mixed batch", () => {
    expect(
      buildD1FinishMap([
        { finishPosition: "02", horseNumber: "01" },
        { finishPosition: "0", horseNumber: "02" },
        { finishPosition: "01", horseNumber: "03" },
      ]),
    ).toStrictEqual(
      new Map([
        ["1", "02"],
        ["3", "01"],
      ]),
    );
  });

  it("lets a later entry overwrite an earlier one for the same horse number", () => {
    expect(
      buildD1FinishMap([
        { finishPosition: "5", horseNumber: "04" },
        { finishPosition: "3", horseNumber: "4" },
      ]),
    ).toStrictEqual(new Map([["4", "3"]]));
  });
});
