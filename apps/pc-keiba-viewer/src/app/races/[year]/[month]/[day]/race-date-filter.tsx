"use client";

import Link from "next/link";
import { type ChangeEvent, useEffect, useId, useMemo, useState } from "react";

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
  initialSearchParams?: Record<string, string | string[] | undefined>;
  month: string;
  races: RaceListItem[];
  year: string;
}

type SourceFilter = "all" | RaceSource;
type SurfaceFilter = "all" | "dirt" | "turf";

interface RaceDateFilterState {
  endTime: string;
  maxDistance: string;
  minDistance: string;
  query: string;
  selectedJockeys: string[];
  source: SourceFilter;
  startTime: string;
  surface: SurfaceFilter;
  tag: string;
  venue: string;
}

const FILTER_QUERY_KEYS = [
  "endTime",
  "jockey",
  "maxDistance",
  "minDistance",
  "q",
  "source",
  "startTime",
  "surface",
  "tag",
  "venue",
] as const;

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

const parseDistance = (value: string | null | undefined): number | null => {
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

const getSearchParam = (
  params: Record<string, string | string[] | undefined> | undefined,
  key: string,
): string | undefined => {
  const value = params?.[key];
  return Array.isArray(value) ? value[0] : value;
};

const getSearchParamValues = (
  params: Record<string, string | string[] | undefined> | undefined,
  key: string,
): string[] => {
  const value = params?.[key];
  if (Array.isArray(value)) {
    return value;
  }
  return value ? [value] : [];
};

const getInitialFilterState = ({
  defaultStartTime,
  fixedVenueCode,
  searchParams,
}: {
  defaultStartTime: string;
  fixedVenueCode?: string;
  searchParams?: Record<string, string | string[] | undefined>;
}): RaceDateFilterState => {
  const sourceParam = getSearchParam(searchParams, "source") ?? "all";
  const surfaceParam = getSearchParam(searchParams, "surface") ?? "all";
  return {
    endTime: getSearchParam(searchParams, "endTime") ?? "",
    maxDistance: getSearchParam(searchParams, "maxDistance") ?? "",
    minDistance: getSearchParam(searchParams, "minDistance") ?? "",
    query: getSearchParam(searchParams, "q") ?? "",
    selectedJockeys: getSearchParamValues(searchParams, "jockey")
      .filter(Boolean)
      .toSorted((a, b) => a.localeCompare(b, "ja")),
    source: isSourceFilter(sourceParam) ? sourceParam : "all",
    startTime: getSearchParam(searchParams, "startTime") ?? defaultStartTime,
    surface: isSurfaceFilter(surfaceParam) ? surfaceParam : "all",
    tag: getSearchParam(searchParams, "tag") ?? "all",
    venue: fixedVenueCode ?? getSearchParam(searchParams, "venue") ?? "all",
  };
};

const getRaceSurface = (trackCode: string | null | undefined): SurfaceFilter => {
  const code = cleanText(trackCode, "");
  if (TURF_TRACK_CODES.has(code)) {
    return "turf";
  }
  if (DIRT_TRACK_CODES.has(code)) {
    return "dirt";
  }
  return "all";
};

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
  initialSearchParams,
  month,
  races,
  year,
}: RaceDateFilterProps) {
  const initialFilters = getInitialFilterState({
    defaultStartTime,
    fixedVenueCode,
    searchParams: initialSearchParams,
  });
  const [source, setSource] = useState<SourceFilter>(initialFilters.source);
  const [venue, setVenue] = useState(initialFilters.venue);
  const [tag, setTag] = useState(initialFilters.tag);
  const [surface, setSurface] = useState<SurfaceFilter>(initialFilters.surface);
  const [query, setQuery] = useState(initialFilters.query);
  const [startTime, setStartTime] = useState(initialFilters.startTime);
  const [endTime, setEndTime] = useState(initialFilters.endTime);
  const [minDistance, setMinDistance] = useState(initialFilters.minDistance);
  const [maxDistance, setMaxDistance] = useState(initialFilters.maxDistance);
  const [jockeyQuery, setJockeyQuery] = useState("");
  const [jockeySuggestionsOpen, setJockeySuggestionsOpen] = useState(false);
  const [highlightedJockeyIndex, setHighlightedJockeyIndex] = useState(0);
  const [selectedJockeys, setSelectedJockeys] = useState<string[]>(initialFilters.selectedJockeys);
  const jockeyListboxId = useId();

  const venueOptions = useMemo(() => getVenueOptions(races), [races]);
  const tagOptions = useMemo(() => getTagOptions(races), [races]);
  const jockeyOptions = useMemo(() => getJockeyOptions(races), [races]);
  const hasJraRaces = useMemo(() => races.some((race) => race.source === "jra"), [races]);
  const jockeySuggestions = useMemo(() => {
    const normalizedQuery = normalize(jockeyQuery);
    return jockeyOptions
      .filter((option) => !selectedJockeys.includes(option))
      .filter((option) => normalizedQuery === "" || normalize(option).includes(normalizedQuery))
      .slice(0, 8);
  }, [jockeyOptions, jockeyQuery, selectedJockeys]);

  useEffect(() => {
    const url = new URL(window.location.href);
    for (const key of FILTER_QUERY_KEYS) {
      url.searchParams.delete(key);
    }
    if (source !== "all") {
      url.searchParams.set("source", source);
    }
    if (!fixedVenueCode && venue !== "all") {
      url.searchParams.set("venue", venue);
    }
    if (tag !== "all") {
      url.searchParams.set("tag", tag);
    }
    if (surface !== "all") {
      url.searchParams.set("surface", surface);
    }
    if (startTime !== defaultStartTime) {
      url.searchParams.set("startTime", startTime);
    }
    if (endTime) {
      url.searchParams.set("endTime", endTime);
    }
    if (minDistance) {
      url.searchParams.set("minDistance", minDistance);
    }
    if (maxDistance) {
      url.searchParams.set("maxDistance", maxDistance);
    }
    if (query) {
      url.searchParams.set("q", query);
    }
    for (const jockey of selectedJockeys) {
      url.searchParams.append("jockey", jockey);
    }
    const nextUrl = `${url.pathname}${url.search}${url.hash}`;
    const currentUrl = `${window.location.pathname}${window.location.search}${window.location.hash}`;
    if (nextUrl !== currentUrl) {
      window.history.replaceState(window.history.state, "", nextUrl);
    }
  }, [
    defaultStartTime,
    endTime,
    fixedVenueCode,
    maxDistance,
    minDistance,
    query,
    selectedJockeys,
    source,
    startTime,
    surface,
    tag,
    venue,
  ]);

  const filteredRaces = useMemo(() => {
    const normalizedQuery = normalize(query);
    const startMinutes = parseFilterTimeMinutes(startTime);
    const endMinutes = parseFilterTimeMinutes(endTime);
    const minDistanceValue = parseDistanceFilter(minDistance);
    const maxDistanceValue = parseDistanceFilter(maxDistance);

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

      if (surface !== "all" && getRaceSurface(race.trackCode) !== surface) {
        return false;
      }

      const distance = parseDistance(race.kyori);
      if (minDistanceValue !== null && (distance === null || distance < minDistanceValue)) {
        return false;
      }
      if (maxDistanceValue !== null && (distance === null || distance > maxDistanceValue)) {
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
  }, [
    endTime,
    maxDistance,
    minDistance,
    query,
    races,
    selectedJockeys,
    source,
    startTime,
    surface,
    tag,
    venue,
  ]);

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
    setSurface("all");
    setQuery("");
    setStartTime(defaultStartTime);
    setEndTime("");
    setMinDistance("");
    setMaxDistance("");
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

  const handleSurfaceChange = (event: ChangeEvent<HTMLSelectElement>) => {
    const value = event.currentTarget.value;
    if (isSurfaceFilter(value)) {
      setSurface(value);
    }
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
        {hasJraRaces ? (
          <label>
            <span>馬場</span>
            <select value={surface} onChange={handleSurfaceChange}>
              <option value="all">すべて</option>
              <option value="turf">芝</option>
              <option value="dirt">ダート</option>
            </select>
          </label>
        ) : null}
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
        <label>
          <span>距離 下限</span>
          <input
            inputMode="numeric"
            min="0"
            placeholder="例 1200"
            step="100"
            type="number"
            value={minDistance}
            onChange={(event) => {
              setMinDistance(event.currentTarget.value);
            }}
          />
        </label>
        <label>
          <span>距離 上限</span>
          <input
            inputMode="numeric"
            min="0"
            placeholder="例 1800"
            step="100"
            type="number"
            value={maxDistance}
            onChange={(event) => {
              setMaxDistance(event.currentTarget.value);
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
