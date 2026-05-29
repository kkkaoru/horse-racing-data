// Run with bun. race_key string parser shared by worker fetch + queue handlers.
// Format: `{source}:{kaisaiNen}:{kaisaiTsukihi}:{keibajoCode}:{raceBango}`
// e.g. `nar:2026:0529:42:01`.

import type { RaceJobKey } from "../types";

const RACE_KEY_PARTS = 5;
const SOURCE_INDEX = 0;
const KAISAI_NEN_INDEX = 1;
const KAISAI_TSUKIHI_INDEX = 2;
const KEIBAJO_CODE_INDEX = 3;
const RACE_BANGO_INDEX = 4;
const VALID_SOURCES = new Set<RaceJobKey["source"]>(["jra", "nar"]);

const isValidSource = (value: string): value is RaceJobKey["source"] =>
  VALID_SOURCES.has(value as RaceJobKey["source"]);

export const tryParseRaceKey = (raceKey: string): RaceJobKey | null => {
  const parts = raceKey.split(":");
  if (parts.length !== RACE_KEY_PARTS) {
    return null;
  }
  const source = parts[SOURCE_INDEX]!;
  if (!isValidSource(source)) {
    return null;
  }
  return {
    kaisaiNen: parts[KAISAI_NEN_INDEX]!,
    kaisaiTsukihi: parts[KAISAI_TSUKIHI_INDEX]!,
    keibajoCode: parts[KEIBAJO_CODE_INDEX]!,
    raceBango: parts[RACE_BANGO_INDEX]!,
    raceKey,
    source,
  };
};

export const parseRaceKey = (raceKey: string): RaceJobKey => {
  const parsed = tryParseRaceKey(raceKey);
  if (!parsed) {
    throw new Error(
      `raceKey must match {source}:{kaisaiNen}:{kaisaiTsukihi}:{keibajoCode}:{raceBango}: ${raceKey}`,
    );
  }
  return parsed;
};
