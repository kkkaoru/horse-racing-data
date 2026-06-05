// Run with: bun run --filter pc-keiba-viewer test

import { expect, test } from "vitest";

import { getPaddockAdjacentNav } from "./paddock-adjacent-nav";

interface SameVenueRaceFixture {
  hassoJikoku: string | null;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  kyori: string | null;
  raceBango: string;
  trackCode: string | null;
}

const buildRace = (overrides: Partial<SameVenueRaceFixture>): SameVenueRaceFixture => ({
  hassoJikoku: "1030",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0606",
  keibajoCode: "09",
  kyori: "1600",
  raceBango: "01",
  trackCode: "10",
  ...overrides,
});

test("returns paddock-edit path for previous race when current race has predecessor", () => {
  const sameVenueRaces = [
    buildRace({ raceBango: "01" }),
    buildRace({ raceBango: "02" }),
    buildRace({ raceBango: "03" }),
  ];
  const nav = getPaddockAdjacentNav({ currentRaceBango: "02", sameVenueRaces });
  expect(nav.previous?.path).toBe("/races/2026/06/06/09/01/paddock");
});

test("returns paddock-edit path for next race when current race has successor", () => {
  const sameVenueRaces = [
    buildRace({ raceBango: "01" }),
    buildRace({ raceBango: "02" }),
    buildRace({ raceBango: "03" }),
  ];
  const nav = getPaddockAdjacentNav({ currentRaceBango: "02", sameVenueRaces });
  expect(nav.next?.path).toBe("/races/2026/06/06/09/03/paddock");
});

test("previous is null when current race is the first race", () => {
  const sameVenueRaces = [buildRace({ raceBango: "01" }), buildRace({ raceBango: "02" })];
  const nav = getPaddockAdjacentNav({ currentRaceBango: "01", sameVenueRaces });
  expect(nav.previous).toBe(null);
});

test("next is null when current race is the last race", () => {
  const sameVenueRaces = [buildRace({ raceBango: "01" }), buildRace({ raceBango: "02" })];
  const nav = getPaddockAdjacentNav({ currentRaceBango: "02", sameVenueRaces });
  expect(nav.next).toBe(null);
});

test("returns previous and next nulls when current race is not in same-venue list", () => {
  const sameVenueRaces = [buildRace({ raceBango: "01" }), buildRace({ raceBango: "02" })];
  const nav = getPaddockAdjacentNav({ currentRaceBango: "99", sameVenueRaces });
  expect(nav.previous).toBe(null);
  expect(nav.next).toBe(null);
});

test("returns previous and next nulls when same-venue list is empty", () => {
  const nav = getPaddockAdjacentNav({ currentRaceBango: "01", sameVenueRaces: [] });
  expect(nav.previous).toBe(null);
  expect(nav.next).toBe(null);
});

test("previous label uses formatted race number / time / track / distance", () => {
  const sameVenueRaces = [
    buildRace({
      hassoJikoku: "0945",
      kyori: "1200",
      raceBango: "01",
      trackCode: "10",
    }),
    buildRace({ raceBango: "02" }),
  ];
  const nav = getPaddockAdjacentNav({ currentRaceBango: "02", sameVenueRaces });
  expect(nav.previous?.label).toBe("1R / 09:45 / 芝 / 1200m");
});

test("next label falls back to dash when hassoJikoku is null", () => {
  const sameVenueRaces = [
    buildRace({ raceBango: "01" }),
    buildRace({
      hassoJikoku: null,
      kyori: "1600",
      raceBango: "02",
      trackCode: "10",
    }),
  ];
  const nav = getPaddockAdjacentNav({ currentRaceBango: "01", sameVenueRaces });
  expect(nav.next?.label).toBe("2R / --:-- / 芝 / 1600m");
});

test("previous race bango is exposed for breadcrumb / aria use", () => {
  const sameVenueRaces = [buildRace({ raceBango: "05" }), buildRace({ raceBango: "06" })];
  const nav = getPaddockAdjacentNav({ currentRaceBango: "06", sameVenueRaces });
  expect(nav.previous?.raceBango).toBe("05");
});

test("date and venue from previous race are baked into the paddock-edit path", () => {
  const sameVenueRaces = [
    buildRace({
      kaisaiNen: "2026",
      kaisaiTsukihi: "0606",
      keibajoCode: "09",
      raceBango: "08",
    }),
    buildRace({ raceBango: "09" }),
  ];
  const nav = getPaddockAdjacentNav({ currentRaceBango: "09", sameVenueRaces });
  expect(nav.previous?.path).toBe("/races/2026/06/06/09/08/paddock");
});
