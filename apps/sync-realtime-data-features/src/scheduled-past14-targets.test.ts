// Run with: bun run --filter sync-realtime-data-features test
import { expect, it } from "vitest";

import { buildPast14Targets } from "./scheduled-past14-targets";

it("returns empty when both today and tomorrow are empty", () => {
  expect(
    buildPast14Targets({ todayJst: "20260529", todayKeys: [], tomorrowKeys: [] }),
  ).toStrictEqual([]);
});

it("produces 14 entries for a single venue tuple from today only", () => {
  const result = buildPast14Targets({
    todayJst: "20260529",
    todayKeys: [
      {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0529",
        keibajoCode: "30",
        raceBango: "08",
        raceKey: "nar:2026:0529:30:08",
        source: "nar",
      },
    ],
    tomorrowKeys: [],
  });
  expect(result.length).toBe(14);
  expect(result[0]).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0515",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0515:30:08",
    source: "nar",
  });
  expect(result[13]).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0528",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0528:30:08",
    source: "nar",
  });
});

it("merges identical (source, keibajo, raceBango) across today and tomorrow only once", () => {
  const result = buildPast14Targets({
    todayJst: "20260529",
    todayKeys: [
      {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0529",
        keibajoCode: "30",
        raceBango: "08",
        raceKey: "nar:2026:0529:30:08",
        source: "nar",
      },
    ],
    tomorrowKeys: [
      {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0530",
        keibajoCode: "30",
        raceBango: "08",
        raceKey: "nar:2026:0530:30:08",
        source: "nar",
      },
    ],
  });
  expect(result.length).toBe(14);
});

it("counts distinct venue tuples across today and tomorrow", () => {
  const result = buildPast14Targets({
    todayJst: "20260529",
    todayKeys: [
      {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0529",
        keibajoCode: "30",
        raceBango: "08",
        raceKey: "nar:2026:0529:30:08",
        source: "nar",
      },
      {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0529",
        keibajoCode: "06",
        raceBango: "12",
        raceKey: "jra:2026:0529:06:12",
        source: "jra",
      },
    ],
    tomorrowKeys: [
      {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0530",
        keibajoCode: "83",
        raceBango: "11",
        raceKey: "nar:2026:0530:83:11",
        source: "nar",
      },
    ],
  });
  expect(result.length).toBe(42);
});

it("handles month boundary backshift correctly", () => {
  const result = buildPast14Targets({
    todayJst: "20260601",
    todayKeys: [
      {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0601",
        keibajoCode: "30",
        raceBango: "08",
        raceKey: "nar:2026:0601:30:08",
        source: "nar",
      },
    ],
    tomorrowKeys: [],
  });
  expect(result.length).toBe(14);
  expect(result[0]).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0518",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0518:30:08",
    source: "nar",
  });
  expect(result[13]).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0531",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0531:30:08",
    source: "nar",
  });
});

it("deduplicates entries that resolve to identical race_key", () => {
  const result = buildPast14Targets({
    todayJst: "20260529",
    todayKeys: [
      {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0529",
        keibajoCode: "30",
        raceBango: "08",
        raceKey: "nar:2026:0529:30:08",
        source: "nar",
      },
      {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0529",
        keibajoCode: "30",
        raceBango: "08",
        raceKey: "nar:2026:0529:30:08",
        source: "nar",
      },
    ],
    tomorrowKeys: [],
  });
  expect(result.length).toBe(14);
});

it("returns empty when only tomorrow but tomorrow itself is empty", () => {
  expect(
    buildPast14Targets({ todayJst: "20260529", todayKeys: [], tomorrowKeys: [] }),
  ).toStrictEqual([]);
});

it("populates only tomorrow keys correctly", () => {
  const result = buildPast14Targets({
    todayJst: "20260529",
    todayKeys: [],
    tomorrowKeys: [
      {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0530",
        keibajoCode: "06",
        raceBango: "12",
        raceKey: "jra:2026:0530:06:12",
        source: "jra",
      },
    ],
  });
  expect(result.length).toBe(14);
  expect(result[0]).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0515",
    keibajoCode: "06",
    raceBango: "12",
    raceKey: "jra:2026:0515:06:12",
    source: "jra",
  });
});
