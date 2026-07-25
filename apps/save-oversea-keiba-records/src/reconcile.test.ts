// This test runs with Bun and Vitest.
import { expect, test } from "vitest";
import {
  reconcileRunners,
  type JraReconcileRunner,
  type SecondaryReconcileRunner,
} from "./reconcile";

const JRA_RUNNER_ONE: JraReconcileRunner = {
  horseNumber: 1,
  gate: 7,
  horseName: "カラヴァッジョ",
  sex: "牡",
  age: 4,
  coatColour: "鹿",
  weightCarriedKg: 60.5,
  jockeyAbbrev: "M.バルザローナ",
  trainerAbbrev: "A.オブライエン",
  trainerCountry: "IRE",
  owner: "COOLMORE",
  winOdds: 3.2,
  popularity: 1,
  formRecord: "1.1.2.1",
  sire: "Wootton Bassett",
  dam: "Test Dam",
  damsire: "Test Damsire",
};

const JRA_RUNNER_TWO: JraReconcileRunner = {
  horseNumber: 2,
  gate: 3,
  horseName: "サンプルホース",
  sex: "牝",
  age: 4,
  coatColour: "黒鹿",
  weightCarriedKg: 56.5,
  jockeyAbbrev: "手塚 貴久",
  trainerAbbrev: "手塚 貴久",
  trainerCountry: "JPN",
  owner: "SAMPLE OWNER",
  winOdds: null,
  popularity: null,
  formRecord: "2.3.4.5",
  sire: "Sample Sire",
  dam: "Sample Dam",
  damsire: "Sample Damsire",
};

const JRA_RUNNER_THREE: JraReconcileRunner = {
  horseNumber: 9,
  gate: 1,
  horseName: "ジャラオンリー",
  sex: "せん",
  age: 5,
  coatColour: "栗",
  weightCarriedKg: 58,
  jockeyAbbrev: "R.ムーア",
  trainerAbbrev: "J.ゴスデン",
  trainerCountry: "GB",
  owner: "JRA ONLY OWNER",
  winOdds: 12.5,
  popularity: 6,
  formRecord: "0.1.0.2",
  sire: "Only Sire",
  dam: "Only Dam",
  damsire: "Only Damsire",
};

const SECONDARY_RUNNER_ONE: SecondaryReconcileRunner = {
  horseNumber: 1,
  gate: 7,
  horseName: "カラヴァ",
  jockeyName: "バルザロ",
  trainerName: "オブライ",
  horseId: "2021100001",
  jockeyId: "05339",
  trainerId: "01001",
  affiliationLabel: "IRE",
};

const SECONDARY_RUNNER_TWO: SecondaryReconcileRunner = {
  horseNumber: 2,
  gate: 3,
  horseName: "サンプルホース",
  jockeyName: "手塚貴久",
  trainerName: "手塚貴久",
  horseId: "2021100002",
  jockeyId: "01052",
  trainerId: "01052",
  affiliationLabel: "美浦",
};

const SECONDARY_RUNNER_GATE_MISMATCH: SecondaryReconcileRunner = {
  horseNumber: 1,
  gate: 8,
  horseName: "カラヴァ",
  jockeyName: "バルザロ",
  trainerName: "オブライ",
  horseId: "2021100001",
  jockeyId: "05339",
  trainerId: "01001",
  affiliationLabel: "IRE",
};

const SECONDARY_ONLY_RUNNER: SecondaryReconcileRunner = {
  horseNumber: 5,
  gate: 4,
  horseName: "セカンダリ",
  jockeyName: "誰か",
  trainerName: "誰か",
  horseId: "2021100005",
  jockeyId: "09999",
  trainerId: "08888",
  affiliationLabel: "FR",
};

const SECONDARY_ORDERED_BY_GATE: readonly SecondaryReconcileRunner[] = [
  {
    horseNumber: 2,
    gate: 1,
    horseName: "サンプル",
    jockeyName: "手塚",
    trainerName: "手塚",
    horseId: "2021100002",
    jockeyId: "01052",
    trainerId: "01052",
    affiliationLabel: "美浦",
  },
  {
    horseNumber: 1,
    gate: 7,
    horseName: "カラヴァ",
    jockeyName: "バルザロ",
    trainerName: "オブライ",
    horseId: "2021100001",
    jockeyId: "05339",
    trainerId: "01001",
    affiliationLabel: "IRE",
  },
];

test("merges a clean match by horse number and keeps JRA descriptive fields", () => {
  const result = reconcileRunners({
    jraRunners: [JRA_RUNNER_ONE],
    secondaryRunners: [SECONDARY_RUNNER_ONE],
  });

  expect(result.mergedRunners).toStrictEqual([
    {
      horseNumber: 1,
      gate: 7,
      horseName: "カラヴァッジョ",
      sex: "牡",
      age: 4,
      coatColour: "鹿",
      weightCarriedKg: 60.5,
      jockeyAbbrev: "M.バルザローナ",
      trainerAbbrev: "A.オブライエン",
      trainerCountry: "IRE",
      owner: "COOLMORE",
      winOdds: 3.2,
      popularity: 1,
      formRecord: "1.1.2.1",
      sire: "Wootton Bassett",
      dam: "Test Dam",
      damsire: "Test Damsire",
      secondaryHorseId: "2021100001",
      secondaryJockeyId: "05339",
      secondaryTrainerId: "01001",
      secondaryAffiliationLabel: "IRE",
      hasSecondaryMatch: true,
    },
  ]);
  expect(result.report).toStrictEqual({
    runners: [
      {
        horseNumber: 1,
        hasSecondaryMatch: true,
        gateDisagrees: false,
        jraGate: 7,
        secondaryGate: 7,
        nameDifferences: [
          {
            field: "horseName",
            jraValue: "カラヴァッジョ",
            secondaryValue: "カラヴァ",
          },
          {
            field: "jockeyName",
            jraValue: "M.バルザローナ",
            secondaryValue: "バルザロ",
          },
          {
            field: "trainerName",
            jraValue: "A.オブライエン",
            secondaryValue: "オブライ",
          },
        ],
      },
    ],
    unmatchedJraHorseNumbers: [],
    unmatchedSecondaryRunners: [],
    gateDisagreements: [],
    hasGateDisagreement: false,
  });
});

test("joins by horse number even when secondary runners are ordered by gate", () => {
  const result = reconcileRunners({
    jraRunners: [JRA_RUNNER_ONE, JRA_RUNNER_TWO],
    secondaryRunners: SECONDARY_ORDERED_BY_GATE,
  });

  expect(result.mergedRunners[0]).toStrictEqual({
    horseNumber: 1,
    gate: 7,
    horseName: "カラヴァッジョ",
    sex: "牡",
    age: 4,
    coatColour: "鹿",
    weightCarriedKg: 60.5,
    jockeyAbbrev: "M.バルザローナ",
    trainerAbbrev: "A.オブライエン",
    trainerCountry: "IRE",
    owner: "COOLMORE",
    winOdds: 3.2,
    popularity: 1,
    formRecord: "1.1.2.1",
    sire: "Wootton Bassett",
    dam: "Test Dam",
    damsire: "Test Damsire",
    secondaryHorseId: "2021100001",
    secondaryJockeyId: "05339",
    secondaryTrainerId: "01001",
    secondaryAffiliationLabel: "IRE",
    hasSecondaryMatch: true,
  });
  expect(result.mergedRunners[1]).toStrictEqual({
    horseNumber: 2,
    gate: 3,
    horseName: "サンプルホース",
    sex: "牝",
    age: 4,
    coatColour: "黒鹿",
    weightCarriedKg: 56.5,
    jockeyAbbrev: "手塚 貴久",
    trainerAbbrev: "手塚 貴久",
    trainerCountry: "JPN",
    owner: "SAMPLE OWNER",
    winOdds: null,
    popularity: null,
    formRecord: "2.3.4.5",
    sire: "Sample Sire",
    dam: "Sample Dam",
    damsire: "Sample Damsire",
    secondaryHorseId: "2021100002",
    secondaryJockeyId: "01052",
    secondaryTrainerId: "01052",
    secondaryAffiliationLabel: "美浦",
    hasSecondaryMatch: true,
  });
  expect(result.report.runners[0]?.hasSecondaryMatch).toBe(true);
  expect(result.report.runners[1]?.hasSecondaryMatch).toBe(true);
  expect(result.report.unmatchedJraHorseNumbers).toStrictEqual([]);
  expect(result.report.unmatchedSecondaryRunners).toStrictEqual([]);
});

test("flags gate disagreement loudly without overriding the JRA gate", () => {
  const result = reconcileRunners({
    jraRunners: [JRA_RUNNER_ONE],
    secondaryRunners: [SECONDARY_RUNNER_GATE_MISMATCH],
  });

  expect(result.mergedRunners[0]?.gate).toBe(7);
  expect(result.mergedRunners[0]?.secondaryHorseId).toBe("2021100001");
  expect(result.report.hasGateDisagreement).toBe(true);
  expect(result.report.gateDisagreements).toStrictEqual([
    {
      horseNumber: 1,
      jraGate: 7,
      secondaryGate: 8,
      message:
        "WARNING: gate disagreement between JRA and secondary source for horse number 1: JRA gate 7 vs secondary gate 8.",
    },
  ]);
  expect(result.report.runners).toStrictEqual([
    {
      horseNumber: 1,
      hasSecondaryMatch: true,
      gateDisagrees: true,
      jraGate: 7,
      secondaryGate: 8,
      nameDifferences: [
        {
          field: "horseName",
          jraValue: "カラヴァッジョ",
          secondaryValue: "カラヴァ",
        },
        {
          field: "jockeyName",
          jraValue: "M.バルザローナ",
          secondaryValue: "バルザロ",
        },
        {
          field: "trainerName",
          jraValue: "A.オブライエン",
          secondaryValue: "オブライ",
        },
      ],
    },
  ]);
});

test("keeps a JRA-only runner in merged output with null secondary ids", () => {
  const result = reconcileRunners({
    jraRunners: [JRA_RUNNER_ONE, JRA_RUNNER_THREE],
    secondaryRunners: [SECONDARY_RUNNER_ONE],
  });

  expect(result.mergedRunners).toStrictEqual([
    {
      horseNumber: 1,
      gate: 7,
      horseName: "カラヴァッジョ",
      sex: "牡",
      age: 4,
      coatColour: "鹿",
      weightCarriedKg: 60.5,
      jockeyAbbrev: "M.バルザローナ",
      trainerAbbrev: "A.オブライエン",
      trainerCountry: "IRE",
      owner: "COOLMORE",
      winOdds: 3.2,
      popularity: 1,
      formRecord: "1.1.2.1",
      sire: "Wootton Bassett",
      dam: "Test Dam",
      damsire: "Test Damsire",
      secondaryHorseId: "2021100001",
      secondaryJockeyId: "05339",
      secondaryTrainerId: "01001",
      secondaryAffiliationLabel: "IRE",
      hasSecondaryMatch: true,
    },
    {
      horseNumber: 9,
      gate: 1,
      horseName: "ジャラオンリー",
      sex: "せん",
      age: 5,
      coatColour: "栗",
      weightCarriedKg: 58,
      jockeyAbbrev: "R.ムーア",
      trainerAbbrev: "J.ゴスデン",
      trainerCountry: "GB",
      owner: "JRA ONLY OWNER",
      winOdds: 12.5,
      popularity: 6,
      formRecord: "0.1.0.2",
      sire: "Only Sire",
      dam: "Only Dam",
      damsire: "Only Damsire",
      secondaryHorseId: null,
      secondaryJockeyId: null,
      secondaryTrainerId: null,
      secondaryAffiliationLabel: null,
      hasSecondaryMatch: false,
    },
  ]);
  expect(result.report.unmatchedJraHorseNumbers).toStrictEqual([9]);
  expect(result.report.runners[1]).toStrictEqual({
    horseNumber: 9,
    hasSecondaryMatch: false,
    gateDisagrees: false,
    jraGate: 1,
    secondaryGate: null,
    nameDifferences: [],
  });
  expect(result.report.unmatchedSecondaryRunners).toStrictEqual([]);
  expect(result.report.hasGateDisagreement).toBe(false);
});

test("reports a secondary-only runner without inventing a merged entry", () => {
  const result = reconcileRunners({
    jraRunners: [JRA_RUNNER_ONE],
    secondaryRunners: [SECONDARY_RUNNER_ONE, SECONDARY_ONLY_RUNNER],
  });

  expect(result.mergedRunners.length).toBe(1);
  expect(result.mergedRunners[0]?.horseNumber).toBe(1);
  expect(result.report.unmatchedSecondaryRunners).toStrictEqual([
    {
      horseNumber: 5,
      gate: 4,
      horseName: "セカンダリ",
      jockeyName: "誰か",
      trainerName: "誰か",
      horseId: "2021100005",
      jockeyId: "09999",
      trainerId: "08888",
      affiliationLabel: "FR",
    },
  ]);
  expect(result.report.unmatchedJraHorseNumbers).toStrictEqual([]);
});

test("records name differences as informational notes when values differ", () => {
  const result = reconcileRunners({
    jraRunners: [JRA_RUNNER_TWO],
    secondaryRunners: [SECONDARY_RUNNER_TWO],
  });

  expect(result.mergedRunners[0]?.horseName).toBe("サンプルホース");
  expect(result.mergedRunners[0]?.jockeyAbbrev).toBe("手塚 貴久");
  expect(result.mergedRunners[0]?.trainerAbbrev).toBe("手塚 貴久");
  expect(result.report.runners[0]?.nameDifferences).toStrictEqual([
    {
      field: "jockeyName",
      jraValue: "手塚 貴久",
      secondaryValue: "手塚貴久",
    },
    {
      field: "trainerName",
      jraValue: "手塚 貴久",
      secondaryValue: "手塚貴久",
    },
  ]);
  expect(result.report.hasGateDisagreement).toBe(false);
});

test("produces empty name differences when secondary names match JRA exactly", () => {
  const result = reconcileRunners({
    jraRunners: [JRA_RUNNER_ONE],
    secondaryRunners: [
      {
        horseNumber: 1,
        gate: 7,
        horseName: "カラヴァッジョ",
        jockeyName: "M.バルザローナ",
        trainerName: "A.オブライエン",
        horseId: "2021100001",
        jockeyId: "05339",
        trainerId: "01001",
        affiliationLabel: "IRE",
      },
    ],
  });

  expect(result.report.runners[0]?.nameDifferences).toStrictEqual([]);
  expect(result.mergedRunners[0]?.secondaryHorseId).toBe("2021100001");
  expect(result.report.hasGateDisagreement).toBe(false);
});

test("handles empty secondary input by keeping all JRA runners unmatched", () => {
  const result = reconcileRunners({
    jraRunners: [JRA_RUNNER_ONE, JRA_RUNNER_TWO],
    secondaryRunners: [],
  });

  expect(result.mergedRunners).toStrictEqual([
    {
      horseNumber: 1,
      gate: 7,
      horseName: "カラヴァッジョ",
      sex: "牡",
      age: 4,
      coatColour: "鹿",
      weightCarriedKg: 60.5,
      jockeyAbbrev: "M.バルザローナ",
      trainerAbbrev: "A.オブライエン",
      trainerCountry: "IRE",
      owner: "COOLMORE",
      winOdds: 3.2,
      popularity: 1,
      formRecord: "1.1.2.1",
      sire: "Wootton Bassett",
      dam: "Test Dam",
      damsire: "Test Damsire",
      secondaryHorseId: null,
      secondaryJockeyId: null,
      secondaryTrainerId: null,
      secondaryAffiliationLabel: null,
      hasSecondaryMatch: false,
    },
    {
      horseNumber: 2,
      gate: 3,
      horseName: "サンプルホース",
      sex: "牝",
      age: 4,
      coatColour: "黒鹿",
      weightCarriedKg: 56.5,
      jockeyAbbrev: "手塚 貴久",
      trainerAbbrev: "手塚 貴久",
      trainerCountry: "JPN",
      owner: "SAMPLE OWNER",
      winOdds: null,
      popularity: null,
      formRecord: "2.3.4.5",
      sire: "Sample Sire",
      dam: "Sample Dam",
      damsire: "Sample Damsire",
      secondaryHorseId: null,
      secondaryJockeyId: null,
      secondaryTrainerId: null,
      secondaryAffiliationLabel: null,
      hasSecondaryMatch: false,
    },
  ]);
  expect(result.report).toStrictEqual({
    runners: [
      {
        horseNumber: 1,
        hasSecondaryMatch: false,
        gateDisagrees: false,
        jraGate: 7,
        secondaryGate: null,
        nameDifferences: [],
      },
      {
        horseNumber: 2,
        hasSecondaryMatch: false,
        gateDisagrees: false,
        jraGate: 3,
        secondaryGate: null,
        nameDifferences: [],
      },
    ],
    unmatchedJraHorseNumbers: [1, 2],
    unmatchedSecondaryRunners: [],
    gateDisagreements: [],
    hasGateDisagreement: false,
  });
});

test("handles empty JRA input by reporting all secondary runners as unmatched", () => {
  const result = reconcileRunners({
    jraRunners: [],
    secondaryRunners: [SECONDARY_RUNNER_ONE, SECONDARY_ONLY_RUNNER],
  });

  expect(result.mergedRunners).toStrictEqual([]);
  expect(result.report.runners).toStrictEqual([]);
  expect(result.report.unmatchedJraHorseNumbers).toStrictEqual([]);
  expect(result.report.unmatchedSecondaryRunners).toStrictEqual([
    {
      horseNumber: 1,
      gate: 7,
      horseName: "カラヴァ",
      jockeyName: "バルザロ",
      trainerName: "オブライ",
      horseId: "2021100001",
      jockeyId: "05339",
      trainerId: "01001",
      affiliationLabel: "IRE",
    },
    {
      horseNumber: 5,
      gate: 4,
      horseName: "セカンダリ",
      jockeyName: "誰か",
      trainerName: "誰か",
      horseId: "2021100005",
      jockeyId: "09999",
      trainerId: "08888",
      affiliationLabel: "FR",
    },
  ]);
  expect(result.report.gateDisagreements).toStrictEqual([]);
  expect(result.report.hasGateDisagreement).toBe(false);
});

test("preserves null secondary entity ids when the secondary match supplies them", () => {
  const result = reconcileRunners({
    jraRunners: [JRA_RUNNER_ONE],
    secondaryRunners: [
      {
        horseNumber: 1,
        gate: 7,
        horseName: "カラヴァッジョ",
        jockeyName: "M.バルザローナ",
        trainerName: "A.オブライエン",
        horseId: null,
        jockeyId: null,
        trainerId: null,
        affiliationLabel: "UNK",
      },
    ],
  });

  expect(result.mergedRunners[0]).toStrictEqual({
    horseNumber: 1,
    gate: 7,
    horseName: "カラヴァッジョ",
    sex: "牡",
    age: 4,
    coatColour: "鹿",
    weightCarriedKg: 60.5,
    jockeyAbbrev: "M.バルザローナ",
    trainerAbbrev: "A.オブライエン",
    trainerCountry: "IRE",
    owner: "COOLMORE",
    winOdds: 3.2,
    popularity: 1,
    formRecord: "1.1.2.1",
    sire: "Wootton Bassett",
    dam: "Test Dam",
    damsire: "Test Damsire",
    secondaryHorseId: null,
    secondaryJockeyId: null,
    secondaryTrainerId: null,
    secondaryAffiliationLabel: "UNK",
    hasSecondaryMatch: true,
  });
  expect(result.report.runners[0]?.hasSecondaryMatch).toBe(true);
  expect(result.report.runners[0]?.nameDifferences).toStrictEqual([]);
});
