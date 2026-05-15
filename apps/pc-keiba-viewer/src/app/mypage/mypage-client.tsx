"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import {
  buildFavoritesSearchParams,
  dedupeFavorites,
  FAVORITE_KIND_LABELS,
  isFavoriteKind,
  parseFavoritesFromSearchParams,
  type FavoriteItem,
  type FavoriteKind,
} from "../../lib/favorites";
import { getFavorites, saveFavorites } from "../../lib/favorites-indexeddb";
import { SOURCE_LABELS, type RaceSource } from "../../lib/codes";
import {
  cleanText,
  formatDate,
  formatDistance,
  formatKeibajo,
  formatRaceNumber,
  formatTime,
  formatTrack,
} from "../../lib/format";
import type { EntityRaceResult } from "../../lib/race-types";

interface FavoriteRaceEntry {
  favoriteId: string;
  favoriteKind: FavoriteKind;
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

interface FavoritesPayload {
  favorites: FavoriteItem[];
  recent: FavoriteRaceGroup[];
  upcoming: FavoriteRaceGroup[];
}

const isFavoritesPayload = (value: unknown): value is FavoritesPayload => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  return (
    Array.isArray(Reflect.get(value, "favorites")) &&
    Array.isArray(Reflect.get(value, "recent")) &&
    Array.isArray(Reflect.get(value, "upcoming"))
  );
};

const raceDetailPath = (
  row: Pick<EntityRaceResult, "kaisaiNen" | "kaisaiTsukihi" | "keibajoCode" | "raceBango">,
): string =>
  `/races/${row.kaisaiNen}/${row.kaisaiTsukihi.slice(0, 2)}/${row.kaisaiTsukihi.slice(2, 4)}/${row.keibajoCode}/${row.raceBango}`;

const favoriteKindClass = (kind: FavoriteKind): string => `favorite-kind-${kind}`;
const favoriteItemKey = (item: Pick<FavoriteItem, "id" | "kind">): string =>
  `${item.kind}:${item.id}`;
const favoriteEntryKey = (entry: Pick<FavoriteRaceEntry, "favoriteId" | "favoriteKind">): string =>
  `${entry.favoriteKind}:${entry.favoriteId}`;

type SourceFilter = "all" | RaceSource;
type SurfaceFilter = "all" | "dirt" | "turf";

interface MyPageFilters {
  endTime: string;
  filterKind: FavoriteKind | "all";
  maxDistance: string;
  minDistance: string;
  query: string;
  selectedFavoriteKeys: string[];
  source: SourceFilter;
  startTime: string;
  surface: SurfaceFilter;
  venue: string;
}

const TURF_TRACK_CODES = new Set([
  "10",
  "11",
  "12",
  "13",
  "14",
  "15",
  "16",
  "17",
  "18",
  "19",
  "20",
  "21",
  "22",
  "51",
  "54",
  "55",
  "58",
  "59",
]);

const DIRT_TRACK_CODES = new Set(["23", "24", "25", "26", "27", "28", "29", "53"]);

const normalize = (value: string): string => value.trim().toLowerCase();

const parseRaceStartMinutes = (value: string | null): number | null => {
  const normalized = cleanText(value, "").padStart(4, "0");
  if (!/^\d{4}$/u.test(normalized)) {
    return null;
  }
  const hours = Number(normalized.slice(0, 2));
  const minutes = Number(normalized.slice(2, 4));
  return hours <= 23 && minutes <= 59 ? hours * 60 + minutes : null;
};

const parseFilterTimeMinutes = (value: string): number | null => {
  if (!/^\d{2}:\d{2}$/u.test(value)) {
    return null;
  }
  const [hoursText, minutesText] = value.split(":");
  const hours = Number(hoursText);
  const minutes = Number(minutesText);
  return hours <= 23 && minutes <= 59 ? hours * 60 + minutes : null;
};

const parseDistance = (value: string | null): number | null => {
  const parsed = Number(cleanText(value, ""));
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

const parseDistanceFilter = (value: string): number | null => {
  if (value.trim() === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

const isSourceFilter = (value: string): value is SourceFilter =>
  value === "all" || value === "jra" || value === "nar";

const isSurfaceFilter = (value: string): value is SurfaceFilter =>
  value === "all" || value === "turf" || value === "dirt";

const getRaceSurface = (trackCode: string | null): SurfaceFilter => {
  const code = cleanText(trackCode, "");
  if (TURF_TRACK_CODES.has(code)) {
    return "turf";
  }
  if (DIRT_TRACK_CODES.has(code)) {
    return "dirt";
  }
  return "all";
};

const getVenueOptions = (rows: FavoriteRaceGroup[]): Array<{ code: string; name: string }> => {
  const venues = new Map<string, string>();
  for (const row of rows) {
    venues.set(row.keibajoCode, formatKeibajo(row.keibajoCode));
  }
  return [...venues.entries()]
    .map(([code, name]) => ({ code, name }))
    .toSorted((a, b) => a.name.localeCompare(b.name, "ja"));
};

const filterRaceRows = (rows: FavoriteRaceGroup[], filters: MyPageFilters): FavoriteRaceGroup[] => {
  const selectedFavoriteKeys = new Set(filters.selectedFavoriteKeys);
  const normalizedQuery = normalize(filters.query);
  const startMinutes = parseFilterTimeMinutes(filters.startTime);
  const endMinutes = parseFilterTimeMinutes(filters.endTime);
  const minDistanceValue = parseDistanceFilter(filters.minDistance);
  const maxDistanceValue = parseDistanceFilter(filters.maxDistance);

  return rows
    .map((row) => ({
      ...row,
      entries: row.entries.filter((entry) => {
        if (selectedFavoriteKeys.size > 0 && !selectedFavoriteKeys.has(favoriteEntryKey(entry))) {
          return false;
        }
        if (filters.filterKind !== "all" && entry.favoriteKind !== filters.filterKind) {
          return false;
        }
        return true;
      }),
    }))
    .filter((row) => {
      if (row.entries.length === 0) {
        return false;
      }
      if (filters.source !== "all" && row.source !== filters.source) {
        return false;
      }
      if (filters.venue !== "all" && row.keibajoCode !== filters.venue) {
        return false;
      }
      if (filters.surface !== "all" && getRaceSurface(row.trackCode) !== filters.surface) {
        return false;
      }

      const distance = parseDistance(row.kyori);
      if (minDistanceValue !== null && (distance === null || distance < minDistanceValue)) {
        return false;
      }
      if (maxDistanceValue !== null && (distance === null || distance > maxDistanceValue)) {
        return false;
      }

      const raceStartMinutes = parseRaceStartMinutes(row.hassoJikoku);
      if (startMinutes !== null && (raceStartMinutes === null || raceStartMinutes < startMinutes)) {
        return false;
      }
      if (endMinutes !== null && (raceStartMinutes === null || raceStartMinutes > endMinutes)) {
        return false;
      }

      if (normalizedQuery === "") {
        return true;
      }

      return [
        SOURCE_LABELS[row.source],
        formatKeibajo(row.keibajoCode),
        formatRaceNumber(row.raceBango),
        cleanText(row.raceName, ""),
        formatTrack(row.trackCode),
        formatDistance(row.kyori),
        ...row.entries.flatMap((entry) => [
          FAVORITE_KIND_LABELS[entry.favoriteKind],
          entry.favoriteLabel,
          entry.horseName,
          entry.jockeyName,
        ]),
      ]
        .join(" ")
        .toLowerCase()
        .includes(normalizedQuery);
    });
};

function FavoriteRaceList({
  emptyText,
  filters,
  loading,
  rows,
  title,
}: {
  emptyText: string;
  filters: MyPageFilters;
  loading: boolean;
  rows: FavoriteRaceGroup[];
  title: string;
}) {
  const filteredRows = useMemo(() => filterRaceRows(rows, filters), [filters, rows]);

  return (
    <section className="mypage-race-section">
      <div className="section-heading compact">
        <h2>{title}</h2>
        <span>
          {filteredRows.length} / {rows.length} 件
        </span>
      </div>
      {loading ? (
        <div className="mypage-race-list" aria-label={`${title}を読み込み中`}>
          {Array.from({ length: 4 }, (_value, index) => (
            <div className="mypage-race-row mypage-race-skeleton" key={index}>
              <span className="skeleton-line short" />
              <span className="skeleton-line long" />
              <span className="skeleton-line medium" />
            </div>
          ))}
        </div>
      ) : filteredRows.length === 0 ? (
        <p className="empty-state">{emptyText}</p>
      ) : (
        <div className="mypage-race-list">
          {filteredRows.map((row) => (
            <Link
              className="mypage-race-row"
              href={raceDetailPath(row)}
              key={[
                row.source,
                row.kaisaiNen,
                row.kaisaiTsukihi,
                row.keibajoCode,
                row.raceBango,
              ].join(":")}
            >
              <span className="mypage-race-date">
                {formatDate(row.kaisaiNen, row.kaisaiTsukihi)}
              </span>
              <span className="mypage-race-main">
                <strong>
                  {formatKeibajo(row.keibajoCode)} {formatRaceNumber(row.raceBango)}{" "}
                  {cleanText(row.raceName, "一般競走")}
                </strong>
              </span>
              <span className="mypage-race-meta">
                <span className="mypage-race-condition">
                  {formatTime(row.hassoJikoku)} / {SOURCE_LABELS[row.source]} /{" "}
                  {formatTrack(row.trackCode)} {formatDistance(row.kyori)} /{" "}
                  {row.isUpcoming ? "発走予定" : "結果確定済み"}
                </span>
                <span className="mypage-race-favorites">
                  {row.entries.map((entry) => (
                    <span
                      className={`mypage-race-favorite-chip ${favoriteKindClass(entry.favoriteKind)}`}
                      key={`${entry.favoriteKind}:${entry.favoriteId}:${entry.favoriteLabel}`}
                    >
                      <small>{FAVORITE_KIND_LABELS[entry.favoriteKind]}</small>
                      {entry.favoriteLabel}
                    </span>
                  ))}
                </span>
              </span>
            </Link>
          ))}
        </div>
      )}
    </section>
  );
}

export function MyPageClient({ initialFavorites }: { initialFavorites: FavoriteItem[] }) {
  const [favorites, setFavorites] = useState<FavoriteItem[]>(initialFavorites);
  const [payload, setPayload] = useState<FavoritesPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [shareStatus, setShareStatus] = useState("共有URLをコピー");
  const [selectedFavoriteKeys, setSelectedFavoriteKeys] = useState<string[]>([]);
  const [filterKind, setFilterKind] = useState<FavoriteKind | "all">("all");
  const [source, setSource] = useState<SourceFilter>("all");
  const [venue, setVenue] = useState("all");
  const [surface, setSurface] = useState<SurfaceFilter>("all");
  const [query, setQuery] = useState("");
  const [startTime, setStartTime] = useState("");
  const [endTime, setEndTime] = useState("");
  const [minDistance, setMinDistance] = useState("");
  const [maxDistance, setMaxDistance] = useState("");

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      const urlFavorites = parseFavoritesFromSearchParams(
        new URLSearchParams(window.location.search),
      );
      const nextFavorites = urlFavorites.length > 0 ? urlFavorites : await getFavorites();
      if (urlFavorites.length > 0) {
        await saveFavorites(urlFavorites);
      }
      if (!cancelled) {
        setFavorites(nextFavorites);
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, []);

  const shareParams = useMemo(() => buildFavoritesSearchParams(favorites), [favorites]);
  const sharePath = `/mypage${shareParams.toString() ? `?${shareParams}` : ""}`;
  const raceRows = useMemo(
    () => [...(payload?.upcoming ?? []), ...(payload?.recent ?? [])],
    [payload],
  );
  const venueOptions = useMemo(() => getVenueOptions(raceRows), [raceRows]);
  const hasJraRaces = useMemo(() => raceRows.some((row) => row.source === "jra"), [raceRows]);
  const filters = useMemo<MyPageFilters>(
    () => ({
      endTime,
      filterKind,
      maxDistance,
      minDistance,
      query,
      selectedFavoriteKeys,
      source,
      startTime,
      surface,
      venue,
    }),
    [
      endTime,
      filterKind,
      maxDistance,
      minDistance,
      query,
      selectedFavoriteKeys,
      source,
      startTime,
      surface,
      venue,
    ],
  );

  useEffect(() => {
    const nextUrl = `${window.location.pathname}${shareParams.toString() ? `?${shareParams}` : ""}`;
    const currentUrl = `${window.location.pathname}${window.location.search}`;
    if (nextUrl !== currentUrl) {
      window.history.replaceState(window.history.state, "", nextUrl);
    }
  }, [shareParams]);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      const query = buildFavoritesSearchParams(favorites).toString();
      const response = await fetch(`/api/mypage/favorites${query ? `?${query}` : ""}`, {
        cache: "no-store",
      });
      const value: unknown = await response.json();
      if (!cancelled) {
        setPayload(isFavoritesPayload(value) ? value : { favorites: [], recent: [], upcoming: [] });
        setLoading(false);
      }
    };
    void load().catch(() => {
      if (!cancelled) {
        setLoading(false);
      }
    });
    return () => {
      cancelled = true;
    };
  }, [favorites]);

  const removeFavorite = (item: FavoriteItem) => {
    const next = dedupeFavorites(
      favorites.filter((favorite) => favorite.kind !== item.kind || favorite.id !== item.id),
    );
    setSelectedFavoriteKeys((current) => current.filter((key) => key !== favoriteItemKey(item)));
    setFavorites(next);
    void saveFavorites(next);
  };

  const toggleFavoriteFilter = (item: FavoriteItem) => {
    const key = favoriteItemKey(item);
    setSelectedFavoriteKeys((current) =>
      current.includes(key) ? current.filter((itemKey) => itemKey !== key) : [...current, key],
    );
  };

  const resetDetailedFilters = () => {
    setFilterKind("all");
    setSource("all");
    setVenue("all");
    setSurface("all");
    setQuery("");
    setStartTime("");
    setEndTime("");
    setMinDistance("");
    setMaxDistance("");
  };

  return (
    <>
      <section className="mypage-favorites-panel">
        <div className="section-heading compact">
          <h2>お気に入り</h2>
          <div className="mypage-heading-actions">
            <Link className="mypage-link-button" href="/mypage/favorites">
              お気に入り管理
            </Link>
            <button
              type="button"
              onClick={() => {
                const copy = async () => {
                  const url = `${window.location.origin}${sharePath}`;
                  await navigator.clipboard.writeText(url);
                  setShareStatus("コピーしました");
                  window.setTimeout(() => setShareStatus("共有URLをコピー"), 1800);
                };
                void copy();
              }}
            >
              {shareStatus}
            </button>
          </div>
        </div>
        {favorites.length === 0 ? (
          <p className="empty-state">詳細ページからお気に入りを追加してください。</p>
        ) : (
          <div className="mypage-favorite-list">
            {favorites.map((item) => (
              <button
                type="button"
                key={`${item.kind}:${item.id}`}
                onClick={() => removeFavorite(item)}
              >
                <span>{FAVORITE_KIND_LABELS[item.kind]}</span>
                <strong>{item.label}</strong>
                <small>解除</small>
              </button>
            ))}
          </div>
        )}
      </section>

      {loading ? <p className="empty-state">お気に入りのレースを読み込み中です。</p> : null}
      <section className="mypage-filters">
        <label>
          <span>絞り込み</span>
          <input
            type="search"
            value={filterText}
            onChange={(event) => setFilterText(event.target.value)}
            placeholder="競馬場、レース名、お気に入り名"
          />
        </label>
        <label>
          <span>お気に入り項目</span>
          <select
            value={filterFavoriteKey}
            onChange={(event) => setFilterFavoriteKey(event.target.value)}
          >
            <option value="all">すべて</option>
            {favorites.map((item) => (
              <option key={favoriteItemKey(item)} value={favoriteItemKey(item)}>
                {FAVORITE_KIND_LABELS[item.kind]}: {item.label}
              </option>
            ))}
          </select>
        </label>
        <label>
          <span>種別</span>
          <select
            value={filterKind}
            onChange={(event) => {
              const nextKind = event.target.value;
              setFilterKind(isFavoriteKind(nextKind) ? nextKind : "all");
            }}
          >
            <option value="all">すべて</option>
            <option value="horse">馬</option>
            <option value="jockey">騎手</option>
            <option value="trainer">調教師</option>
            <option value="owner">馬主</option>
          </select>
        </label>
      </section>
      <FavoriteRaceList
        emptyText="発走予定のレースは見つかりませんでした。"
        filterFavoriteKey={filterFavoriteKey}
        filterKind={filterKind}
        filterText={filterText}
        rows={payload?.upcoming ?? []}
        title="発走予定"
      />
      <FavoriteRaceList
        emptyText="直近のレースは見つかりませんでした。"
        filterFavoriteKey={filterFavoriteKey}
        filterKind={filterKind}
        filterText={filterText}
        rows={payload?.recent ?? []}
        title="直近のレース"
      />
    </>
  );
}
