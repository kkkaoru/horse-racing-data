// This test runs with Bun and Vitest.
import { expect, test } from "vitest";
import type {
  MasterEntityRecord,
  MasterLookupPort,
  OwnerMasterRecord,
  TrainerMasterRecord,
} from "./entity-resolver";
import {
  createBackfillAwareLookup,
  formatMasterBackfillReport,
  planMasterBackfill,
} from "./master-backfill";
import type { MergedRunner } from "./reconcile";

const BASE_RUNNER: MergedRunner = {
  horseNumber: 1,
  gate: 7,
  horseName: "テストホース",
  sex: "せん",
  age: 5,
  coatColour: "鹿",
  weightCarriedKg: 61,
  jockeyAbbrev: "M.ジョッキー",
  trainerAbbrev: "F.トレーナー",
  trainerCountry: "FR",
  owner: "TEST OWNER",
  winOdds: 1.6,
  popularity: 1,
  formRecord: "10.5.1.1",
  sire: "Test Sire",
  dam: "Test Dam",
  damsire: "Test Damsire",
  secondaryHorseId: "2021190001",
  secondaryJockeyId: "05504",
  secondaryTrainerId: "05701",
  secondaryAffiliationLabel: "ForeignYard",
  hasSecondaryMatch: true,
};

const ENTITY_ABSENT: MasterEntityRecord = { exists: false, canonicalName: null };
const TRAINER_ABSENT: TrainerMasterRecord = {
  exists: false,
  canonicalName: null,
  tozaiShozokuCode: null,
};
const OWNER_ABSENT: OwnerMasterRecord = { code: null, canonicalName: null };

const createLookup = (input: {
  readonly horses?: ReadonlyMap<string, MasterEntityRecord>;
  readonly jockeys?: ReadonlyMap<string, MasterEntityRecord>;
  readonly trainers?: ReadonlyMap<string, TrainerMasterRecord>;
}): MasterLookupPort => ({
  findHorse: async (code: string): Promise<MasterEntityRecord> =>
    input.horses?.get(code) ?? ENTITY_ABSENT,
  findJockey: async (code: string): Promise<MasterEntityRecord> =>
    input.jockeys?.get(code) ?? ENTITY_ABSENT,
  findTrainer: async (code: string): Promise<TrainerMasterRecord> =>
    input.trainers?.get(code) ?? TRAINER_ABSENT,
  findOwnerByName: async (): Promise<OwnerMasterRecord> => OWNER_ABSENT,
});

test("shape-valid missing horse/jockey/trainer become insert candidates", async () => {
  const candidates = await planMasterBackfill({
    mergedRunners: [BASE_RUNNER],
    lookup: createLookup({}),
    raceDateCompact: "20260725",
  });

  expect(candidates).toStrictEqual([
    expect.objectContaining({
      kind: "horse",
      code: "2021190001",
      displayName: "テストホース",
      sire: "Test Sire",
      dam: "Test Dam",
      damsire: "Test Damsire",
    }),
    expect.objectContaining({
      kind: "jockey",
      code: "05504",
      displayName: "M.ジョッキー",
    }),
    expect.objectContaining({
      kind: "trainer",
      code: "05701",
      displayName: "F.トレーナー",
    }),
  ]);
});

test("alphanumeric horse id is never a master insert candidate", async () => {
  const candidates = await planMasterBackfill({
    mergedRunners: [
      {
        ...BASE_RUNNER,
        secondaryHorseId: "000a029d4f",
        secondaryJockeyId: "a033f",
        secondaryTrainerId: "05A01",
      },
    ],
    lookup: createLookup({}),
    raceDateCompact: "20260725",
  });

  expect(candidates).toStrictEqual([]);
});

test("shape-valid existing horse is not proposed for insert", async () => {
  const candidates = await planMasterBackfill({
    mergedRunners: [BASE_RUNNER],
    lookup: createLookup({
      horses: new Map([["2021190001", { exists: true, canonicalName: "EXISTING HORSE" }]]),
      jockeys: new Map([["05504", { exists: true, canonicalName: "EXISTING JOCKEY" }]]),
      trainers: new Map([
        ["05701", { exists: true, canonicalName: "EXISTING TRAINER", tozaiShozokuCode: "4" }],
      ]),
    }),
    raceDateCompact: "20260725",
  });

  expect(candidates).toStrictEqual([]);
});

test("placeholder codes are never proposed even though they are all digits", async () => {
  const candidates = await planMasterBackfill({
    mergedRunners: [
      {
        ...BASE_RUNNER,
        secondaryHorseId: "0000000000",
        secondaryJockeyId: "00000",
        secondaryTrainerId: "00000",
      },
    ],
    lookup: createLookup({}),
    raceDateCompact: "20260725",
  });

  expect(candidates).toStrictEqual([]);
});

test("null secondary ids produce no candidates", async () => {
  const candidates = await planMasterBackfill({
    mergedRunners: [
      {
        ...BASE_RUNNER,
        secondaryHorseId: null,
        secondaryJockeyId: null,
        secondaryTrainerId: null,
        hasSecondaryMatch: false,
      },
    ],
    lookup: createLookup({}),
    raceDateCompact: "20260725",
  });

  expect(candidates).toStrictEqual([]);
});

test("duplicate codes across runners yield a single candidate per kind+code", async () => {
  const second: MergedRunner = {
    ...BASE_RUNNER,
    horseNumber: 2,
    horseName: "OTHER",
    secondaryHorseId: "2021190001",
    secondaryJockeyId: "05504",
    secondaryTrainerId: "05701",
  };
  const candidates = await planMasterBackfill({
    mergedRunners: [BASE_RUNNER, second],
    lookup: createLookup({}),
    raceDateCompact: "20260725",
  });

  expect(candidates).toHaveLength(3);
  expect(candidates.filter((c) => c.kind === "horse")).toHaveLength(1);
  expect(candidates.filter((c) => c.kind === "jockey")).toHaveLength(1);
  expect(candidates.filter((c) => c.kind === "trainer")).toHaveLength(1);
});

test("createBackfillAwareLookup reports planned candidates as existing with card names", async () => {
  const baseLookup = createLookup({});
  const candidates = await planMasterBackfill({
    mergedRunners: [BASE_RUNNER],
    lookup: baseLookup,
    raceDateCompact: "20260725",
  });
  const aware = createBackfillAwareLookup(baseLookup, candidates);

  await expect(aware.findHorse("2021190001")).resolves.toStrictEqual({
    exists: true,
    canonicalName: "テストホース",
  });
  await expect(aware.findJockey("05504")).resolves.toStrictEqual({
    exists: true,
    canonicalName: "M.ジョッキー",
  });
  await expect(aware.findTrainer("05701")).resolves.toStrictEqual({
    exists: true,
    canonicalName: "F.トレーナー",
    tozaiShozokuCode: "4",
  });
  await expect(aware.findHorse("9999999999")).resolves.toStrictEqual(ENTITY_ABSENT);
  await expect(aware.findOwnerByName("TEST OWNER")).resolves.toStrictEqual(OWNER_ABSENT);
});

test("createBackfillAwareLookup prefers a real existing master over a planned insert", async () => {
  const baseLookup = createLookup({
    horses: new Map([["2021190001", { exists: true, canonicalName: "CANONICAL FROM DB" }]]),
  });
  const aware = createBackfillAwareLookup(baseLookup, [
    {
      kind: "horse",
      code: "2021190001",
      displayName: "CARD NAME",
      horseName: "CARD NAME",
      sex: "牡",
      coatColour: "鹿",
      sire: "S",
      dam: "D",
      damsire: "DS",
      jockeyAbbrev: "J",
      trainerAbbrev: "T",
      owner: "O",
      trainerCode: "05701",
      raceDate: "20260725",
    },
  ]);

  await expect(aware.findHorse("2021190001")).resolves.toStrictEqual({
    exists: true,
    canonicalName: "CANONICAL FROM DB",
  });
});

test("formatMasterBackfillReport lists candidates or (none)", () => {
  expect(formatMasterBackfillReport([])).toStrictEqual([
    "=== Master backfill (numeric-only) ===",
    "(none)",
  ]);
  expect(
    formatMasterBackfillReport([
      {
        kind: "horse",
        code: "2021190001",
        displayName: "テストホース",
        horseName: "テストホース",
        sex: "せん",
        coatColour: "鹿",
        sire: "Test Sire",
        dam: "Test Dam",
        damsire: "Test Damsire",
        jockeyAbbrev: "M.ジョッキー",
        trainerAbbrev: "F.トレーナー",
        owner: "TEST OWNER",
        trainerCode: "05701",
        raceDate: "20260725",
      },
    ]),
  ).toStrictEqual([
    "=== Master backfill (numeric-only) ===",
    "horse code=2021190001 name=テストホース",
  ]);
});
