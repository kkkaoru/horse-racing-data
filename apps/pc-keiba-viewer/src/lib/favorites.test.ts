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
});
