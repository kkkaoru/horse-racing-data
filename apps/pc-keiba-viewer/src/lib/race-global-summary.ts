// Run with bun. Pure helper that builds the ordered span sequence rendered
// inside the race-global-summary header on the race-detail and paddock
// pages. The order is: venue, race number, track surface, distance,
// condition. The countdown is rendered separately by the caller and is
// not part of this list, so its position stays untouched.

import { formatDistance, formatKeibajo, formatRaceNumber, getTrackSurfaceLabel } from "./format";

export interface RaceGlobalSummaryInput {
  conditionLabel: string;
  keibajoCode: string;
  kyori: string | null;
  raceNumber: string;
  trackCode: string | null;
}

export interface RaceGlobalSummaryItem {
  className: string | null;
  key: string;
  text: string;
}

const CONDITION_KEY = "condition";
const VENUE_KEY = "venue";
const RACE_NUMBER_KEY = "raceNumber";
const TRACK_SURFACE_KEY = "trackSurface";
const DISTANCE_KEY = "distance";
const CONDITION_CLASS = "race-global-summary-condition";

const buildConditionItem = (label: string): RaceGlobalSummaryItem | null =>
  label.length > 0 ? { className: CONDITION_CLASS, key: CONDITION_KEY, text: label } : null;

export const buildRaceGlobalSummaryItems = (
  input: RaceGlobalSummaryInput,
): RaceGlobalSummaryItem[] => {
  const conditionItem = buildConditionItem(input.conditionLabel);
  const baseItems: RaceGlobalSummaryItem[] = [
    { className: null, key: VENUE_KEY, text: formatKeibajo(input.keibajoCode) },
    { className: null, key: RACE_NUMBER_KEY, text: formatRaceNumber(input.raceNumber) },
    { className: null, key: TRACK_SURFACE_KEY, text: getTrackSurfaceLabel(input.trackCode) },
    { className: null, key: DISTANCE_KEY, text: formatDistance(input.kyori) },
  ];
  return conditionItem ? [...baseItems, conditionItem] : baseItems;
};
