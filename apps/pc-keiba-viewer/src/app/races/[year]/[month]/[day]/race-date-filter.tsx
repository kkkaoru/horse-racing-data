"use client";

import Link from "next/link";
import { type ChangeEvent, useId, useMemo, useState } from "react";

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
  defaultStartTime?: string;
  fixedVenueCode?: string;
  month: string;
  races: RaceListItem[];
  year: string;
}

type SourceFilter = "all" | RaceSource;

const normalize = (value: string): string => value.trim().toLowerCase();

const parseRaceStartMinutes = (value: string | null): number | null => {
  const normalized = cleanText(value, "").padStart(4, "0");
  if (!/^\d{4}$/u.test(normalized)) {
    return null;
  }
  const hours = Number(normalized.slice(0, 2));
  const minutes = Number(normalized.slice(2, 4));
  if (hours > 23 || minutes > 59) {
    return null;
  }
  return hours * 60 + minutes;
};

const parseFilterTimeMinutes = (value: string): number | null => {
  if (!/^\d{2}:\d{2}$/u.test(value)) {
    return null;
  }
  const [hoursText, minutesText] = value.split(":");
  const hours = Number(hoursText);
  const minutes = Number(minutesText);
  if (hours > 23 || minutes > 59) {
    return null;
  }
  return hours * 60 + minutes;
};

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

const getJockeyOptions = (races: RaceListItem[]): string[] =>
  [...new Set(races.flatMap((race) => (race.jockeyNames ?? []).map((name) => cleanText(name, ""))))]
    .filter(Boolean)
    .toSorted((a, b) => a.localeCompare(b, "ja"));

const raceHasSelectedJockey = (race: RaceListItem, selectedJockeys: string[]): boolean =>
  selectedJockeys.length === 0 ||
  selectedJockeys.some((jockey) => (race.jockeyNames ?? []).includes(jockey));

export function RaceDateFilter({
  day,
  defaultStartTime = "",
  fixedVenueCode,
  month,
  races,
  year,
}: RaceDateFilterProps) {
  const [source, setSource] = useState<SourceFilter>("all");
  const [venue, setVenue] = useState(fixedVenueCode ?? "all");
  const [tag, setTag] = useState("all");
  const [query, setQuery] = useState("");
  const [startTime, setStartTime] = useState(defaultStartTime);
  const [endTime, setEndTime] = useState("");
  const [jockeyQuery, setJockeyQuery] = useState("");
  const [jockeySuggestionsOpen, setJockeySuggestionsOpen] = useState(false);
  const [highlightedJockeyIndex, setHighlightedJockeyIndex] = useState(0);
  const [selectedJockeys, setSelectedJockeys] = useState<string[]>([]);
  const jockeyListboxId = useId();

  const venueOptions = useMemo(() => getVenueOptions(races), [races]);
  const tagOptions = useMemo(() => getTagOptions(races), [races]);
  const jockeyOptions = useMemo(() => getJockeyOptions(races), [races]);
  const jockeySuggestions = useMemo(() => {
    const normalizedQuery = normalize(jockeyQuery);
    return jockeyOptions
      .filter((option) => !selectedJockeys.includes(option))
      .filter((option) => normalizedQuery === "" || normalize(option).includes(normalizedQuery))
      .slice(0, 8);
  }, [jockeyOptions, jockeyQuery, selectedJockeys]);

  const filteredRaces = useMemo(() => {
    const normalizedQuery = normalize(query);
    const startMinutes = parseFilterTimeMinutes(startTime);
    const endMinutes = parseFilterTimeMinutes(endTime);

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

      const raceStartMinutes = parseRaceStartMinutes(race.hassoJikoku);
      if (startMinutes !== null && (raceStartMinutes === null || raceStartMinutes < startMinutes)) {
        return false;
      }
      if (endMinutes !== null && (raceStartMinutes === null || raceStartMinutes > endMinutes)) {
        return false;
      }

      if (!raceHasSelectedJockey(race, selectedJockeys)) {
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
        ...(race.jockeyNames ?? []),
      ]
        .join(" ")
        .toLowerCase();

      return searchable.includes(normalizedQuery);
    });
  }, [endTime, query, races, selectedJockeys, source, startTime, tag, venue]);

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
    setVenue(fixedVenueCode ?? "all");
    setTag("all");
    setQuery("");
    setStartTime(defaultStartTime);
    setEndTime("");
    setJockeyQuery("");
    setJockeySuggestionsOpen(false);
    setHighlightedJockeyIndex(0);
    setSelectedJockeys([]);
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

  const selectJockey = (jockey: string) => {
    if (selectedJockeys.includes(jockey)) {
      return;
    }
    setSelectedJockeys((current) =>
      [...current, jockey].toSorted((a, b) => a.localeCompare(b, "ja")),
    );
    setJockeyQuery("");
    setHighlightedJockeyIndex(0);
    setJockeySuggestionsOpen(false);
  };

  const removeSelectedJockey = (jockey: string) => {
    setSelectedJockeys((current) => current.filter((item) => item !== jockey));
  };

  const listMode = fixedVenueCode ? "venue" : "date";

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
        {fixedVenueCode ? null : (
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
        )}
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
        <label>
          <span>開始時間</span>
          <input
            type="time"
            value={startTime}
            onChange={(event) => {
              setStartTime(event.currentTarget.value);
            }}
          />
        </label>
        <label>
          <span>終了時間</span>
          <input
            type="time"
            value={endTime}
            onChange={(event) => {
              setEndTime(event.currentTarget.value);
            }}
          />
        </label>
        <label className="filter-search filter-jockey-search">
          <span>騎手</span>
          <div className="filter-jockey-input">
            <input
              aria-activedescendant={
                jockeySuggestionsOpen && jockeySuggestions[highlightedJockeyIndex]
                  ? `${jockeyListboxId}-${highlightedJockeyIndex}`
                  : undefined
              }
              aria-autocomplete="list"
              aria-controls={jockeyListboxId}
              aria-expanded={jockeySuggestionsOpen}
              placeholder="騎手名を検索"
              role="combobox"
              type="search"
              value={jockeyQuery}
              onChange={(event) => {
                setJockeyQuery(event.currentTarget.value);
                setHighlightedJockeyIndex(0);
                setJockeySuggestionsOpen(true);
              }}
              onFocus={() => {
                setJockeySuggestionsOpen(true);
              }}
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  event.preventDefault();
                  const highlighted = jockeySuggestions[highlightedJockeyIndex];
                  if (highlighted) {
                    selectJockey(highlighted);
                  }
                }
                if (event.key === "ArrowDown") {
                  event.preventDefault();
                  setJockeySuggestionsOpen(true);
                  setHighlightedJockeyIndex((current) =>
                    Math.min(current + 1, Math.max(jockeySuggestions.length - 1, 0)),
                  );
                }
                if (event.key === "ArrowUp") {
                  event.preventDefault();
                  setHighlightedJockeyIndex((current) => Math.max(current - 1, 0));
                }
                if (event.key === "Escape") {
                  setJockeySuggestionsOpen(false);
                }
              }}
            />
            {jockeySuggestionsOpen ? (
              <div className="filter-jockey-listbox" id={jockeyListboxId}>
                {jockeySuggestions.length === 0 ? (
                  <div className="filter-jockey-empty">候補なし</div>
                ) : (
                  jockeySuggestions.map((option, index) => (
                    <button
                      className={index === highlightedJockeyIndex ? "highlighted" : undefined}
                      id={`${jockeyListboxId}-${index}`}
                      key={option}
                      type="button"
                      onMouseDown={(event) => {
                        event.preventDefault();
                        selectJockey(option);
                      }}
                    >
                      {option}
                    </button>
                  ))
                )}
              </div>
            ) : null}
          </div>
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
      {selectedJockeys.length > 0 ? (
        <div className="filter-selected-list" aria-label="selected jockey filters">
          {selectedJockeys.map((jockey) => (
            <button
              type="button"
              key={jockey}
              onClick={() => {
                removeSelectedJockey(jockey);
              }}
            >
              {jockey}
              <span aria-hidden="true">×</span>
            </button>
          ))}
        </div>
      ) : null}

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
                            {listMode === "date"
                              ? `${formatKeibajo(race.keibajoCode)} ${formatRaceNumber(race.raceBango)}`
                              : `${SOURCE_LABELS[race.source]} ${formatRaceNumber(race.raceBango)}`}
                          </strong>
                          <span>{cleanText(race.kyosomeiHondai, "一般競走")}</span>
                          {(race.jockeyNames ?? []).length > 0 ? (
                            <span className="race-jockeys">
                              {(race.jockeyNames ?? []).slice(0, 4).join(" / ")}
                            </span>
                          ) : null}
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
