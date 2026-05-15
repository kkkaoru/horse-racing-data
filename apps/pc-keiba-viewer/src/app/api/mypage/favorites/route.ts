import { NextResponse } from "next/server";

import { getHorseDetailData, getPersonDetailData } from "../../../../db/queries";
import {
  FAVORITE_QUERY_KEYS,
  FAVORITE_KINDS,
  parseFavoritesFromSearchParams,
  type FavoriteItem,
} from "../../../../lib/favorites";
import type { EntityListQuery, EntityRaceResult } from "../../../../lib/race-types";

export const dynamic = "force-dynamic";

interface FavoriteRaceRow extends EntityRaceResult {
  favoriteId: string;
  favoriteKind: FavoriteItem["kind"];
  favoriteLabel: string;
}

interface FavoriteRaceEntry {
  favoriteId: string;
  favoriteKind: FavoriteItem["kind"];
  favoriteLabel: string;
  horseName: string;
  jockeyName: string;
  rank: string | null;
  popularity: string | null;
  winOdds: string | null;
}

interface FavoriteRaceGroup extends Pick<
    EntityRaceResult,
    | "hassoJikoku"
    | "isUpcoming"
    | "kaisaiNen"
  | "kaisaiTsukihi"
  | "keibajoCode"
  | "kyori"
  | "raceBango"
  | "raceName"
  | "source"
  | "trackCode"
> {
  entries: FavoriteRaceEntry[];
}

const defaultQuery: EntityListQuery = {
  date: "",
  dateFrom: "",
  dateTo: "",
  distanceMax: "",
  distanceMin: "",
  jockeyName: "",
  keibajoCode: "",
  last3fMax: "",
  last3fMin: "",
  order: "latest",
  oddsMax: "",
  oddsMin: "",
  popularityMax: "",
  popularityMin: "",
  q: "",
  rank: "all",
  raceNumber: "",
  raceTimeMax: "",
  raceTimeMin: "",
  source: "all",
  surface: "all",
  trainerName: "",
  turn: "all",
};

const raceTimeKey = (
  row: Pick<
    EntityRaceResult,
    "hassoJikoku" | "kaisaiNen" | "kaisaiTsukihi" | "keibajoCode" | "raceBango"
  >,
): string =>
  `${row.kaisaiNen}${row.kaisaiTsukihi}${row.hassoJikoku ?? "9999"}${row.keibajoCode}${row.raceBango.padStart(2, "0")}`;

const toFavoriteRows = (favorite: FavoriteItem, rows: EntityRaceResult[]): FavoriteRaceRow[] =>
  rows.map((row) => ({
    ...row,
    favoriteId: favorite.id,
    favoriteKind: favorite.kind,
    favoriteLabel: favorite.label,
  }));

const getFavoriteRows = async (favorite: FavoriteItem): Promise<FavoriteRaceRow[]> => {
  if (favorite.kind === "horse") {
    const data = await getHorseDetailData(favorite.id, defaultQuery);
    return toFavoriteRows(favorite, data?.results ?? []);
  }
  const kind =
    favorite.kind === "jockey" ? "jockeys" : favorite.kind === "trainer" ? "trainers" : "owners";
  const data = await getPersonDetailData(kind, favorite.id, defaultQuery);
  return toFavoriteRows(favorite, data?.results ?? []);
};

const dedupeRows = (rows: FavoriteRaceRow[]): FavoriteRaceRow[] =>
  Array.from(
    new Map(
      rows.map((row) => [
        [
          row.favoriteKind,
          row.favoriteId,
          row.source,
          row.kaisaiNen,
          row.kaisaiTsukihi,
          row.keibajoCode,
          row.raceBango,
          row.kettoTorokuBango,
          row.horseName,
        ].join(":"),
        row,
      ]),
    ).values(),
  );

const raceGroupKey = (row: EntityRaceResult): string =>
  [row.source, row.kaisaiNen, row.kaisaiTsukihi, row.keibajoCode, row.raceBango].join(":");

const entryKey = (entry: FavoriteRaceEntry): string =>
  [
    entry.favoriteKind,
    entry.favoriteId,
    entry.horseName,
    entry.jockeyName,
    entry.rank ?? "",
    entry.popularity ?? "",
    entry.winOdds ?? "",
  ].join(":");

const toRaceGroups = (rows: FavoriteRaceRow[]): FavoriteRaceGroup[] => {
  const groups = new Map<string, FavoriteRaceGroup>();
  for (const row of rows) {
    const key = raceGroupKey(row);
    const group =
      groups.get(key) ??
      ({
        entries: [],
        hassoJikoku: row.hassoJikoku,
        isUpcoming: row.isUpcoming,
        kaisaiNen: row.kaisaiNen,
        kaisaiTsukihi: row.kaisaiTsukihi,
        keibajoCode: row.keibajoCode,
        kyori: row.kyori,
        raceBango: row.raceBango,
        raceName: row.raceName,
        source: row.source,
        trackCode: row.trackCode,
      } satisfies FavoriteRaceGroup);
    const entry = {
      favoriteId: row.favoriteId,
      favoriteKind: row.favoriteKind,
      favoriteLabel: row.favoriteLabel,
      horseName: row.horseName,
      jockeyName: row.jockeyName,
      popularity: row.popularity,
      rank: row.rank,
      winOdds: row.winOdds,
    } satisfies FavoriteRaceEntry;
    if (!group.entries.some((current) => entryKey(current) === entryKey(entry))) {
      group.entries.push(entry);
    }
    groups.set(key, group);
  }
  return Array.from(groups.values());
};

export async function GET(request: Request) {
  const url = new URL(request.url);
  const favorites = parseFavoritesFromSearchParams(url.searchParams);
  for (const kind of FAVORITE_KINDS) {
    const key = FAVORITE_QUERY_KEYS[kind];
    for (const value of url.searchParams.getAll(key)) {
      if (!favorites.some((item) => item.kind === kind && item.id === value)) {
        favorites.push({ id: value, kind, label: value });
      }
    }
  }

  const groups = toRaceGroups(
    dedupeRows((await Promise.all(favorites.map(getFavoriteRows))).flat()),
  );
  const upcoming = groups
    .filter((row) => row.isUpcoming)
    .toSorted((a, b) => raceTimeKey(a).localeCompare(raceTimeKey(b)))
    .slice(0, 80);
  const recent = groups
    .filter((row) => !row.isUpcoming)
    .toSorted((a, b) => raceTimeKey(b).localeCompare(raceTimeKey(a)))
    .slice(0, 80);

  return NextResponse.json({
    favorites,
    recent,
    upcoming,
  });
}
