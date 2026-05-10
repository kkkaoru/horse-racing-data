"use client";

import Link from "next/link";
import { type ChangeEvent, useMemo, useState } from "react";

import { SOURCE_LABELS, type RaceSource } from "../../../../../lib/codes";
import {
  cleanText,
  formatDistance,
  formatKeibajo,
  formatRaceNumber,
  formatTime,
  formatTrack,
} from "../../../../../lib/format";
import { getRaceTags, getRaceTagText } from "../../../../../lib/race-classification";
import type { RaceListItem } from "../../../../../lib/race-types";

interface RaceDateFilterProps {
  day: string;
  month: string;
  races: RaceListItem[];
  year: string;
}

type SourceFilter = "all" | RaceSource;

const normalize = (value: string): string => value.trim().toLowerCase();

const isSourceFilter = (value: string): value is SourceFilter =>
  value === "all" || value === "jra" || value === "nar";

const getVenueOptions = (races: RaceListItem[]): Array<{ code: string; name: string }> => {
  const venues = new Map<string, string>();
  for (const race of races) {
    venues.set(race.keibajoCode, formatKeibajo(race.keibajoCode));
  }
  return [...venues.entries()]
    .map(([code, name]) => ({ code, name }))
    .toSorted((a, b) => a.name.localeCompare(b.name, "ja"));
};

const getTagOptions = (races: RaceListItem[]): string[] => {
  const tags = new Set<string>();
  for (const race of races) {
    for (const tag of getRaceTags(race)) {
      tags.add(tag);
    }
  }
  return [...tags].toSorted((a, b) => a.localeCompare(b, "ja"));
};

export function RaceDateFilter({ day, month, races, year }: RaceDateFilterProps) {
  const [source, setSource] = useState<SourceFilter>("all");
  const [venue, setVenue] = useState("all");
  const [tag, setTag] = useState("all");
  const [query, setQuery] = useState("");

  const venueOptions = useMemo(() => getVenueOptions(races), [races]);
  const tagOptions = useMemo(() => getTagOptions(races), [races]);

  const filteredRaces = useMemo(() => {
    const normalizedQuery = normalize(query);

    return races.filter((race) => {
      if (source !== "all" && race.source !== source) {
        return false;
      }
      if (venue !== "all" && race.keibajoCode !== venue) {
        return false;
      }

      const tags = getRaceTags(race);
      if (tag !== "all" && !tags.includes(tag)) {
        return false;
      }

      if (!normalizedQuery) {
        return true;
      }

      const searchable = [
        SOURCE_LABELS[race.source],
        formatKeibajo(race.keibajoCode),
        formatRaceNumber(race.raceBango),
        cleanText(race.kyosomeiHondai, ""),
        cleanText(race.kyosomeiFukudai, ""),
        getRaceTagText(race),
        formatTrack(race.trackCode),
        formatDistance(race.kyori),
      ]
        .join(" ")
        .toLowerCase();

      return searchable.includes(normalizedQuery);
    });
  }, [query, races, source, tag, venue]);

  const grouped = useMemo(() => {
    const groups = new Map<RaceSource, RaceListItem[]>();
    groups.set("jra", []);
    groups.set("nar", []);
    for (const race of filteredRaces) {
      groups.get(race.source)?.push(race);
    }
    return groups;
  }, [filteredRaces]);

  const resetFilters = () => {
    setSource("all");
    setVenue("all");
    setTag("all");
    setQuery("");
  };

  const handleSourceChange = (event: ChangeEvent<HTMLSelectElement>) => {
    const value = event.currentTarget.value;
    if (isSourceFilter(value)) {
      setSource(value);
    }
  };

  const handleVenueChange = (event: ChangeEvent<HTMLSelectElement>) => {
    setVenue(event.currentTarget.value);
  };

  const handleTagChange = (event: ChangeEvent<HTMLSelectElement>) => {
    setTag(event.currentTarget.value);
  };

  const handleQueryChange = (event: ChangeEvent<HTMLInputElement>) => {
    setQuery(event.currentTarget.value);
  };

  return (
    <>
      <section className="filter-panel" aria-label="race filters">
        <label>
          <span>主催</span>
          <select value={source} onChange={handleSourceChange}>
            <option value="all">すべて</option>
            <option value="jra">JRA</option>
            <option value="nar">NAR</option>
          </select>
        </label>
        <label>
          <span>競馬場</span>
          <select value={venue} onChange={handleVenueChange}>
            <option value="all">すべて</option>
            {venueOptions.map((option) => (
              <option value={option.code} key={option.code}>
                {option.name}
              </option>
            ))}
          </select>
        </label>
        <label>
          <span>区分</span>
          <select value={tag} onChange={handleTagChange}>
            <option value="all">すべて</option>
            {tagOptions.map((option) => (
              <option value={option} key={option}>
                {option}
              </option>
            ))}
          </select>
        </label>
        <label className="filter-search">
          <span>検索</span>
          <input
            placeholder="レース名、競馬場、区分"
            type="search"
            value={query}
            onChange={handleQueryChange}
          />
        </label>
        <button type="button" onClick={resetFilters}>
          リセット
        </button>
      </section>

      <div className="filter-result-count">
        {filteredRaces.length} / {races.length} レース
      </div>

      {filteredRaces.length === 0 ? (
        <p className="empty-state">条件に一致するレースはありません。</p>
      ) : (
        <div className="race-day-layout">
          {(["jra", "nar"] as const).map((raceSource) => {
            const sourceRaces = grouped.get(raceSource) ?? [];

            return (
              <section className="race-list-section" key={raceSource}>
                <div className="section-heading compact">
                  <h2>{SOURCE_LABELS[raceSource]}</h2>
                  <span>{sourceRaces.length} レース</span>
                </div>
                <div className="race-list">
                  {sourceRaces.map((race) => {
                    const tags = getRaceTags(race);

                    return (
                      <Link
                        className="race-row"
                        href={`/races/${year}/${month}/${day}/${race.keibajoCode}/${race.raceBango}`}
                        key={`${race.source}-${race.keibajoCode}-${race.raceBango}`}
                      >
                        <span className="race-time">{formatTime(race.hassoJikoku)}</span>
                        <span className="race-main">
                          <strong>
                            {formatKeibajo(race.keibajoCode)} {formatRaceNumber(race.raceBango)}
                          </strong>
                          <span>{cleanText(race.kyosomeiHondai, "一般競走")}</span>
                          {tags.length > 0 ? (
                            <span className="tag-list">
                              {tags.map((raceTag) => (
                                <span className="race-tag" key={raceTag}>
                                  {raceTag}
                                </span>
                              ))}
                            </span>
                          ) : null}
                        </span>
                        <span className="race-meta">
                          {formatTrack(race.trackCode)} {formatDistance(race.kyori)}
                        </span>
                      </Link>
                    );
                  })}
                </div>
              </section>
            );
          })}
        </div>
      )}
    </>
  );
}
