import { NextResponse } from "next/server";

import { searchFavoriteHorses, searchFavoritePeople } from "../../../../../db/queries";
import type { FavoriteKind } from "../../../../../lib/favorites";

export const dynamic = "force-dynamic";

const isFavoriteKind = (value: string | null): value is FavoriteKind =>
  value === "horse" || value === "jockey" || value === "owner" || value === "trainer";

const formatLatestDate = (value: string): string =>
  /^\d{8}$/.test(value) ? `${value.slice(0, 4)}/${value.slice(4, 6)}/${value.slice(6, 8)}` : value;

export async function GET(request: Request) {
  const url = new URL(request.url);
  const kind = url.searchParams.get("kind");
  const q = url.searchParams.get("q")?.trim() ?? "";
  if (!isFavoriteKind(kind) || q.length < 1) {
    return NextResponse.json({ results: [] });
  }

  if (kind === "horse") {
    const rows = await searchFavoriteHorses(q);
    return NextResponse.json({
      results: rows.map((row) => ({
        id: row.id,
        kind,
        label: row.label,
        meta: `${row.starts}走 / 最新 ${formatLatestDate(row.latestDate)}`,
      })),
    });
  }

  const personKind = kind === "jockey" ? "jockeys" : kind === "trainer" ? "trainers" : "owners";
  const rows = await searchFavoritePeople(personKind, q);
  return NextResponse.json({
    results: rows.map((row) => ({
      id: row.id,
      kind,
      label: row.label,
      meta: `${row.starts}走 / 最新 ${formatLatestDate(row.latestDate)}`,
    })),
  });
}
