// Run with bun. Phase F: compute past-14-day RaceJobKey targets for each
// (source, keibajoCode, raceBango) that appears in today's, tomorrow's, OR
// yesterday's race list. Yesterday's venue tuples close the gap where a venue
// raced yesterday but is not racing today/tomorrow (e.g. NAR venues that only
// race on non-consecutive days): without it, that venue's own past14 window
// (which always includes yesterday) never becomes a rebuild candidate, so its
// stale finish-position Parquet is never refreshed. Lets the scheduled
// handler backfill recent results into per-race Parquet without any external
// seed script.

import type { TodayRaceKey } from "./scheduled-race-list";
import { shiftYyyymmddByDays } from "./time";
import type { RaceJobKey } from "./types";

const PAST14_LOOKBACK_DAYS = 14;
const PAST14_OFFSET_MIN = -PAST14_LOOKBACK_DAYS;
const YYYYMMDD_YEAR_END = 4;
const YYYYMMDD_DAY_END = 8;

interface VenueRaceTuple {
  source: "jra" | "nar";
  keibajoCode: string;
  raceBango: string;
}

interface BuildPast14TargetsInput {
  todayKeys: TodayRaceKey[];
  tomorrowKeys: TodayRaceKey[];
  yesterdayKeys: TodayRaceKey[];
  todayJst: string;
}

const toVenueTupleKey = (tuple: VenueRaceTuple): string =>
  `${tuple.source}:${tuple.keibajoCode}:${tuple.raceBango}`;

const toVenueTupleFromTodayKey = (entry: TodayRaceKey): VenueRaceTuple => ({
  keibajoCode: entry.keibajoCode,
  raceBango: entry.raceBango,
  source: entry.source,
});

// Takes a list of race-key lists (today / tomorrow / yesterday) rather than
// separate parameters, both to stay under the 3-argument limit and so any
// future additional source list can merge in without another signature change.
const mergeUniqueVenueTuples = (keyLists: TodayRaceKey[][]): VenueRaceTuple[] => {
  const merged = new Map<string, VenueRaceTuple>();
  keyLists.forEach((entries) => {
    entries.forEach((entry) => {
      const tuple = toVenueTupleFromTodayKey(entry);
      merged.set(toVenueTupleKey(tuple), tuple);
    });
  });
  return Array.from(merged.values());
};

const splitYyyymmdd = (yyyymmdd: string): { kaisaiNen: string; kaisaiTsukihi: string } => ({
  kaisaiNen: yyyymmdd.slice(0, YYYYMMDD_YEAR_END),
  kaisaiTsukihi: yyyymmdd.slice(YYYYMMDD_YEAR_END, YYYYMMDD_DAY_END),
});

const buildRaceKeyString = (tuple: VenueRaceTuple, yyyymmdd: string): string => {
  const { kaisaiNen, kaisaiTsukihi } = splitYyyymmdd(yyyymmdd);
  return `${tuple.source}:${kaisaiNen}:${kaisaiTsukihi}:${tuple.keibajoCode}:${tuple.raceBango}`;
};

const toRaceJobKey = (tuple: VenueRaceTuple, yyyymmdd: string): RaceJobKey => {
  const { kaisaiNen, kaisaiTsukihi } = splitYyyymmdd(yyyymmdd);
  return {
    kaisaiNen,
    kaisaiTsukihi,
    keibajoCode: tuple.keibajoCode,
    raceBango: tuple.raceBango,
    raceKey: buildRaceKeyString(tuple, yyyymmdd),
    source: tuple.source,
  };
};

const buildPast14DateList = (todayJst: string): string[] => {
  const offsets = Array.from({ length: PAST14_LOOKBACK_DAYS }, (_, idx) => PAST14_OFFSET_MIN + idx);
  return offsets.map((delta) => shiftYyyymmddByDays(todayJst, delta));
};

const explodeTupleAcrossDates = (tuple: VenueRaceTuple, dates: string[]): RaceJobKey[] =>
  dates.map((yyyymmdd) => toRaceJobKey(tuple, yyyymmdd));

const dedupeByRaceKey = (jobs: RaceJobKey[]): RaceJobKey[] => {
  const seen = new Set<string>();
  return jobs.filter((job) => {
    if (seen.has(job.raceKey)) {
      return false;
    }
    seen.add(job.raceKey);
    return true;
  });
};

export const buildPast14Targets = ({
  todayKeys,
  tomorrowKeys,
  yesterdayKeys,
  todayJst,
}: BuildPast14TargetsInput): RaceJobKey[] => {
  const venueTuples = mergeUniqueVenueTuples([todayKeys, tomorrowKeys, yesterdayKeys]);
  if (venueTuples.length === 0) {
    return [];
  }
  const dates = buildPast14DateList(todayJst);
  const exploded = venueTuples.flatMap((tuple) => explodeTupleAcrossDates(tuple, dates));
  return dedupeByRaceKey(exploded);
};
