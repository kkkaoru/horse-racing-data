"use client";

import { useEffect, useState } from "react";

import { FAVORITE_KIND_LABELS, type FavoriteItem } from "../lib/favorites";
import { isFavorite, toggleFavorite } from "../lib/favorites-indexeddb";

export function FavoriteButton({ item }: { item: FavoriteItem }) {
  const [enabled, setEnabled] = useState(false);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const value = await isFavorite(item);
        if (!cancelled) {
          setEnabled(value);
          setReady(true);
        }
      } catch {
        if (!cancelled) {
          setReady(true);
        }
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [item]);

  const handleClick = async () => {
    try {
      setEnabled(await toggleFavorite(item));
    } catch {
      // Leave the current state unchanged if IndexedDB is unavailable.
    }
  };

  return (
    <button
      className={enabled ? "favorite-button active" : "favorite-button"}
      disabled={!ready}
      type="button"
      onClick={() => {
        void handleClick();
      }}
    >
      <span aria-hidden="true">{enabled ? "★" : "☆"}</span>
      {enabled
        ? `${FAVORITE_KIND_LABELS[item.kind]}のお気に入り解除`
        : `${FAVORITE_KIND_LABELS[item.kind]}をお気に入り`}
    </button>
  );
}
