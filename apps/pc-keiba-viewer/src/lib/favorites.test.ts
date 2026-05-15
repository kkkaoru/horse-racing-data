import { describe, expect, it } from "vitest";

import { buildFavoritesSearchParams, parseFavoritesFromSearchParams } from "./favorites";

describe("favorites url helpers", () => {
  it("round trips favorites through URL search params", () => {
    const params = buildFavoritesSearchParams([
      { id: "田口貫太", kind: "jockey", label: "田口貫太" },
      { id: "坂井瑠星", kind: "jockey", label: "坂井瑠星" },
      { id: "2020101234", kind: "horse", label: "テストホース" },
    ]);

    expect(params.getAll("jockey")).toEqual(["坂井瑠星", "田口貫太"]);
    expect(params.get("horseLabel:2020101234")).toBe("テストホース");
    expect(parseFavoritesFromSearchParams(params)).toEqual([
      { id: "2020101234", kind: "horse", label: "テストホース" },
      { id: "坂井瑠星", kind: "jockey", label: "坂井瑠星" },
      { id: "田口貫太", kind: "jockey", label: "田口貫太" },
    ]);
  });

  it("normalizes full-width padding in favorite labels", () => {
    const params = new URLSearchParams();
    params.append("horse", "2023102979");
    params.set("horseLabel:2023102979", "　ジュウリョクピエロ　　　　　");

    expect(parseFavoritesFromSearchParams(params)).toEqual([
      { id: "2023102979", kind: "horse", label: "ジュウリョクピエロ" },
    ]);
    expect(
      buildFavoritesSearchParams([
        { id: "2023102979", kind: "horse", label: "ジュウリョクピエロ　　　　　" },
      ]).get("horseLabel:2023102979"),
    ).toBe("ジュウリョクピエロ");
  });
});
