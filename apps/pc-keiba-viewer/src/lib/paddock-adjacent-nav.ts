// Run with: bun run --filter pc-keiba-viewer test
// Helpers that derive previous / next race navigation for the paddock-edit
// page. The paddock-edit nav reuses the race-detail SSR snapshot's
// `sameVenueRaces` array, then builds links that stay on the paddock-edit
// route instead of jumping back to race detail.

import { formatDistance, formatRaceNumber, formatTime, getTrackSurfaceLabel } from "./format";

interface AdjacentRaceLike {
  hassoJikoku: string | null;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  kyori: string | null;
  raceBango: string;
  trackCode: string | null;
}

interface PaddockAdjacentRace {
  label: string;
  path: string;
  raceBango: string;
}

export interface PaddockAdjacentNav {
  next: PaddockAdjacentRace | null;
  previous: PaddockAdjacentRace | null;
}

const ADJACENT_LABEL_SEPARATOR = " / ";

const getPaddockEditPath = (race: AdjacentRaceLike): string =>
  `/races/${race.kaisaiNen}/${race.kaisaiTsukihi.slice(0, 2)}/${race.kaisaiTsukihi.slice(2, 4)}/${race.keibajoCode}/${race.raceBango}/paddock`;

const getAdjacentRaceLabel = (race: AdjacentRaceLike): string =>
  [
    formatRaceNumber(race.raceBango),
    formatTime(race.hassoJikoku),
    getTrackSurfaceLabel(race.trackCode),
    formatDistance(race.kyori),
  ].join(ADJACENT_LABEL_SEPARATOR);

const buildAdjacentRace = (race: AdjacentRaceLike): PaddockAdjacentRace => ({
  label: getAdjacentRaceLabel(race),
  path: getPaddockEditPath(race),
  raceBango: race.raceBango,
});

interface PaddockAdjacentNavInput {
  currentRaceBango: string;
  sameVenueRaces: AdjacentRaceLike[];
}

export const getPaddockAdjacentNav = ({
  currentRaceBango,
  sameVenueRaces,
}: PaddockAdjacentNavInput): PaddockAdjacentNav => {
  const currentIndex = sameVenueRaces.findIndex((race) => race.raceBango === currentRaceBango);
  const previous = currentIndex > 0 ? sameVenueRaces[currentIndex - 1] : null;
  const next =
    currentIndex >= 0 && currentIndex < sameVenueRaces.length - 1
      ? sameVenueRaces[currentIndex + 1]
      : null;
  return {
    next: next ? buildAdjacentRace(next) : null,
    previous: previous ? buildAdjacentRace(previous) : null,
  };
};
