import { describe, expect, it } from "vitest";

import {
  buildFavoritesSearchParams,
  dedupeFavorites,
  favoriteKey,
  isFavoriteKind,
  normalizeFavoriteLabel,
  parseFavoriteKey,
  parseFavoritesFromSearchParams,
} from "./favorites";

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

  it("composes favorite keys from kind and id", () => {
    expect(favoriteKey({ id: "h1", kind: "horse" })).toBe("horse:h1");
    expect(favoriteKey({ id: "t1", kind: "trainer" })).toBe("trainer:t1");
  });

  it("normalizes favorite labels by collapsing whitespace", () => {
    expect(normalizeFavoriteLabel(" 田口  貫太 ")).toBe("田口 貫太");
    expect(normalizeFavoriteLabel("　　foo　bar")).toBe("foo bar");
  });

  it("identifies valid favorite kinds", () => {
    expect(isFavoriteKind("horse")).toBe(true);
    expect(isFavoriteKind("jockey")).toBe(true);
    expect(isFavoriteKind("owner")).toBe(true);
    expect(isFavoriteKind("trainer")).toBe(true);
    expect(isFavoriteKind("unknown")).toBe(false);
    expect(isFavoriteKind("")).toBe(false);
  });

  it("parses favorite keys back to kind and id", () => {
    expect(parseFavoriteKey("horse:abc")).toEqual({ id: "abc", kind: "horse" });
    expect(parseFavoriteKey("jockey:tanaka")).toEqual({ id: "tanaka", kind: "jockey" });
  });

  it("rejects malformed favorite keys", () => {
    expect(parseFavoriteKey(":abc")).toBeNull();
    expect(parseFavoriteKey("noseparator")).toBeNull();
    expect(parseFavoriteKey("horse:")).toBeNull();
    expect(parseFavoriteKey("unknown:abc")).toBeNull();
  });

  it("deduplicates favorites and falls back to id when label is empty", () => {
    expect(
      dedupeFavorites([
        { id: "h1", kind: "horse", label: "" },
        { id: "h1", kind: "horse", label: "first" },
        { id: "h2", kind: "horse", label: "second" },
      ]),
    ).toEqual([
      { id: "h1", kind: "horse", label: "first" },
      { id: "h2", kind: "horse", label: "second" },
    ]);
  });

  it("uses id as the label when normalized label is empty", () => {
    expect(dedupeFavorites([{ id: "fallback-id", kind: "trainer", label: "　　" }])).toEqual([
      { id: "fallback-id", kind: "trainer", label: "fallback-id" },
    ]);
  });

  it("omits label query param when label equals the id", () => {
    const params = buildFavoritesSearchParams([{ id: "h1", kind: "horse", label: "h1" }]);
    expect(params.get("horseLabel:h1")).toBeNull();
    expect(params.getAll("horse")).toEqual(["h1"]);
  });

  it("ignores empty id values from search params", () => {
    const params = new URLSearchParams();
    params.append("horse", "");
    params.append("horse", "h1");
    expect(parseFavoritesFromSearchParams(params)).toEqual([
      { id: "h1", kind: "horse", label: "h1" },
    ]);
  });
});
