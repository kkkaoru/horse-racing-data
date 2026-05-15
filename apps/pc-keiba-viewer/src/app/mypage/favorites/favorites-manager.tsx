"use client";

import { useEffect, useMemo, useState } from "react";

import {
  buildFavoritesSearchParams,
  dedupeFavorites,
  FAVORITE_KIND_LABELS,
  isFavoriteKind,
  type FavoriteItem,
  type FavoriteKind,
} from "../../../lib/favorites";
import { getFavorites, saveFavorites } from "../../../lib/favorites-indexeddb";

interface SearchResult extends FavoriteItem {
  meta: string;
}

const kinds: FavoriteKind[] = ["horse", "jockey", "trainer", "owner"];

const favoriteKindClass = (kind: FavoriteKind): string => `favorite-kind-${kind}`;

const isSearchPayload = (value: unknown): value is { results?: SearchResult[] } =>
  typeof value === "object" &&
  value !== null &&
  (Reflect.get(value, "results") === undefined || Array.isArray(Reflect.get(value, "results")));

export function FavoritesManager() {
  const [favorites, setFavorites] = useState<FavoriteItem[]>([]);
  const [kind, setKind] = useState<FavoriteKind>("horse");
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const items = await getFavorites();
        if (!cancelled) {
          setFavorites(items);
        }
      } catch {
        // IndexedDB may be unavailable in restricted browser contexts.
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!query.trim()) {
      setResults([]);
      return undefined;
    }
    const controller = new AbortController();
    const timer = window.setTimeout(() => {
      const search = async () => {
        setLoading(true);
        try {
          const response = await fetch(
            `/api/mypage/favorites/search?kind=${encodeURIComponent(kind)}&q=${encodeURIComponent(query)}`,
            { cache: "no-store", signal: controller.signal },
          );
          const value: unknown = await response.json();
          setResults(isSearchPayload(value) ? (value.results ?? []) : []);
        } catch (error: unknown) {
          if (!(error instanceof DOMException && error.name === "AbortError")) {
            setResults([]);
          }
        } finally {
          setLoading(false);
        }
      };
      void search();
    }, 180);
    return () => {
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [kind, query]);

  const favoriteKeys = useMemo(
    () => new Set(favorites.map((item) => `${item.kind}:${item.id}`)),
    [favorites],
  );

  const updateFavorites = (items: FavoriteItem[]) => {
    const next = dedupeFavorites(items);
    setFavorites(next);
    void saveFavorites(next);
  };

  const addFavorite = (item: FavoriteItem) => {
    updateFavorites([...favorites, item]);
  };

  const removeFavorite = (item: FavoriteItem) => {
    updateFavorites(
      favorites.filter((favorite) => favorite.kind !== item.kind || favorite.id !== item.id),
    );
  };

  const shareParams = buildFavoritesSearchParams(favorites).toString();
  const mypagePath = `/mypage${shareParams ? `?${shareParams}` : ""}`;

  return (
    <>
      <section className="mypage-favorites-panel">
        <div className="section-heading compact">
          <h2>お気に入り検索</h2>
          <a className="mypage-link-button" href={mypagePath}>
            マイページで見る
          </a>
        </div>
        <div className="favorite-manager-form">
          <label>
            <span>種別</span>
            <select
              value={kind}
              onChange={(event) => {
                const nextKind = event.currentTarget.value;
                if (isFavoriteKind(nextKind)) {
                  setKind(nextKind);
                }
                setResults([]);
              }}
            >
              {kinds.map((item) => (
                <option key={item} value={item}>
                  {FAVORITE_KIND_LABELS[item]}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>検索</span>
            <input
              placeholder={`${FAVORITE_KIND_LABELS[kind]}を検索`}
              type="search"
              value={query}
              onChange={(event) => {
                setQuery(event.currentTarget.value);
              }}
            />
          </label>
        </div>
        <div className="favorite-search-results">
          {loading ? <p className="empty-state">検索中です。</p> : null}
          {!loading && query && results.length === 0 ? (
            <p className="empty-state">候補が見つかりません。</p>
          ) : null}
          {results.map((item) => {
            const active = favoriteKeys.has(`${item.kind}:${item.id}`);
            return (
              <div className="favorite-search-row" key={`${item.kind}:${item.id}`}>
                <div>
                  <strong>{item.label}</strong>
                  <span>{item.meta}</span>
                </div>
                <button
                  type="button"
                  onClick={() => {
                    if (active) {
                      removeFavorite(item);
                    } else {
                      addFavorite(item);
                    }
                  }}
                >
                  {active ? "削除" : "登録"}
                </button>
              </div>
            );
          })}
        </div>
      </section>

      <section className="mypage-favorites-panel">
        <div className="section-heading compact">
          <h2>登録済み</h2>
          <span>{favorites.length} 件</span>
        </div>
        {favorites.length === 0 ? (
          <p className="empty-state">まだお気に入りはありません。</p>
        ) : (
          <div className="mypage-favorite-list">
            {favorites.map((item) => (
              <button
                type="button"
                className={favoriteKindClass(item.kind)}
                key={`${item.kind}:${item.id}`}
                aria-label={`${FAVORITE_KIND_LABELS[item.kind]} ${item.label} を削除`}
                onClick={() => removeFavorite(item)}
              >
                <span className="favorite-kind-label">{FAVORITE_KIND_LABELS[item.kind]}</span>
                <strong className="favorite-name">{item.label}</strong>
                <small className="favorite-remove-mark" aria-hidden="true">
                  ×
                </small>
              </button>
            ))}
          </div>
        )}
      </section>
    </>
  );
}
