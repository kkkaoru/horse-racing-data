// Run with bun. Phase F: compute past-14-day RaceJobKey targets for each
// (source, keibajoCode, raceBango) that appears in today's OR tomorrow's race
// list. Lets the scheduled handler backfill recent results into per-race
// Parquet without any external seed script.

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
  todayJst: string;
}

const toVenueTupleKey = (tuple: VenueRaceTuple): string =>
  `${tuple.source}:${tuple.keibajoCode}:${tuple.raceBango}`;

const toVenueTupleFromTodayKey = (entry: TodayRaceKey): VenueRaceTuple => ({
  keibajoCode: entry.keibajoCode,
  raceBango: entry.raceBango,
  source: entry.source,
});

const collectUniqueVenueTuples = (entries: TodayRaceKey[]): Map<string, VenueRaceTuple> => {
  const map = new Map<string, VenueRaceTuple>();
  entries.forEach((entry) => {
    const tuple = toVenueTupleFromTodayKey(entry);
    map.set(toVenueTupleKey(tuple), tuple);
  });
  return map;
};

const mergeUniqueVenueTuples = (
  todayKeys: TodayRaceKey[],
  tomorrowKeys: TodayRaceKey[],
): VenueRaceTuple[] => {
  const merged = collectUniqueVenueTuples(todayKeys);
  tomorrowKeys.forEach((entry) => {
    const tuple = toVenueTupleFromTodayKey(entry);
    merged.set(toVenueTupleKey(tuple), tuple);
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
  todayJst,
}: BuildPast14TargetsInput): RaceJobKey[] => {
  const venueTuples = mergeUniqueVenueTuples(todayKeys, tomorrowKeys);
  if (venueTuples.length === 0) {
    return [];
  }
  const dates = buildPast14DateList(todayJst);
  const exploded = venueTuples.flatMap((tuple) => explodeTupleAcrossDates(tuple, dates));
  return dedupeByRaceKey(exploded);
};
