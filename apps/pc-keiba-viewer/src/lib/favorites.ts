export type FavoriteKind = "horse" | "jockey" | "owner" | "trainer";

export interface FavoriteItem {
  id: string;
  kind: FavoriteKind;
  label: string;
}

export const FAVORITE_KIND_LABELS: Record<FavoriteKind, string> = {
  horse: "馬",
  jockey: "騎手",
  owner: "馬主",
  trainer: "調教師",
};

export const FAVORITE_QUERY_KEYS: Record<FavoriteKind, string> = {
  horse: "horse",
  jockey: "jockey",
  owner: "owner",
  trainer: "trainer",
};

export const FAVORITE_KINDS: readonly FavoriteKind[] = ["horse", "jockey", "owner", "trainer"];

export const favoriteKey = (item: Pick<FavoriteItem, "id" | "kind">): string =>
  `${item.kind}:${item.id}`;

export const isFavoriteKind = (value: string): value is FavoriteKind =>
  value === "horse" || value === "jockey" || value === "owner" || value === "trainer";

export const parseFavoriteKey = (key: string): { id: string; kind: FavoriteKind } | null => {
  const separator = key.indexOf(":");
  if (separator <= 0) {
    return null;
  }
  const kind = key.slice(0, separator);
  const id = key.slice(separator + 1);
  return isFavoriteKind(kind) && id ? { id, kind } : null;
};

export const parseFavoritesFromSearchParams = (params: URLSearchParams): FavoriteItem[] => {
  const items: FavoriteItem[] = [];
  for (const kind of FAVORITE_KINDS) {
    for (const id of params.getAll(FAVORITE_QUERY_KEYS[kind]).filter(Boolean)) {
      const label = params.get(`${FAVORITE_QUERY_KEYS[kind]}Label:${id}`) ?? id;
      items.push({ id, kind, label });
    }
  }
  return dedupeFavorites(items);
};

export const dedupeFavorites = (items: FavoriteItem[]): FavoriteItem[] =>
  Array.from(new Map(items.map((item) => [favoriteKey(item), item])).values()).toSorted((a, b) =>
    `${a.kind}:${a.label}`.localeCompare(`${b.kind}:${b.label}`, "ja"),
  );

export const buildFavoritesSearchParams = (items: FavoriteItem[]): URLSearchParams => {
  const params = new URLSearchParams();
  for (const item of dedupeFavorites(items)) {
    const key = FAVORITE_QUERY_KEYS[item.kind];
    params.append(key, item.id);
    if (item.label !== item.id) {
      params.set(`${key}Label:${item.id}`, item.label);
    }
  }
  return params;
};
