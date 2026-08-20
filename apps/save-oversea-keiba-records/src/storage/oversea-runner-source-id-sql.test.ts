// This test runs with Bun and Vitest.
import { expect, test } from "vitest";
import type { SecondarySourceRunner } from "../sources/secondary-source-parser";
import {
  buildOverseaPersistStatements,
  buildOverseaRunnerIdentityUpsert,
  buildOverseaRunnerSourceIdUpsert,
  formatOverseaRunnerSourceIdPlanReport,
  planOverseaRunnerPersist,
  shouldWriteOverseaRunnerIdentity,
  type OverseaRunnerIdentityRow,
  type OverseaRunnerSourceIdRow,
  type PlanOverseaRunnerPersistInput,
} from "./oversea-runner-source-id-sql";

const COMPLETE_RUNNER: SecondarySourceRunner = {
  horseNumber: 1,
  gate: 7,
  horseName: "テストホース",
  horseId: "2021190001",
  jockeyId: "05504",
  trainerId: "05701",
  trainerAffiliation: "ForeignYard",
};

const SECOND_RUNNER: SecondarySourceRunner = {
  horseNumber: 10,
  gate: 3,
  horseName: "サンプルホース",
  horseId: "000a029d4f",
  jockeyId: "c5271",
  trainerId: "d1038",
  trainerAffiliation: "StableHome",
};

const NETKEIBA_PLAN_INPUT: PlanOverseaRunnerPersistInput = {
  runners: [COMPLETE_RUNNER],
  raceDate: "2026-07-25",
  venueCode: "A6",
  raceNumber: "5",
  source: "netkeiba",
};

test("shouldWriteOverseaRunnerIdentity is true only for the jra-van display source", () => {
  expect(shouldWriteOverseaRunnerIdentity("jra-van")).toBe(true);
  expect(shouldWriteOverseaRunnerIdentity("netkeiba")).toBe(false);
});

test("planOverseaRunnerPersist maps a complete netkeiba runner onto source_id only", () => {
  expect(planOverseaRunnerPersist(NETKEIBA_PLAN_INPUT)).toStrictEqual({
    sourceIdRows: [
      {
        race_source: "jra",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0725",
        keibajo_code: "A6",
        race_bango: "05",
        umaban: "01",
        source: "netkeiba",
        source_horse_id: "2021190001",
        source_jockey_id: "05504",
        source_trainer_id: "05701",
        source_owner_id: null,
        gate_number: 7,
        source_url: null,
      },
    ],
    identityRows: [],
    reportLines: [
      {
        umaban: "01",
        sourceHorseIdPresent: true,
        planned: true,
      },
    ],
    skippedMissingHorseNumber: 0,
    skippedMissingHorseId: 0,
    writesIdentity: false,
  });
});

test("planOverseaRunnerPersist accepts a compact race date without dashes", () => {
  expect(
    planOverseaRunnerPersist({
      runners: [COMPLETE_RUNNER],
      raceDate: "20260816",
      venueCode: "A8",
      raceNumber: "04",
      source: "netkeiba",
    }).sourceIdRows,
  ).toStrictEqual([
    {
      race_source: "jra",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0816",
      keibajo_code: "A8",
      race_bango: "04",
      umaban: "01",
      source: "netkeiba",
      source_horse_id: "2021190001",
      source_jockey_id: "05504",
      source_trainer_id: "05701",
      source_owner_id: null,
      gate_number: 7,
      source_url: null,
    },
  ]);
});

test("planOverseaRunnerPersist zero-pads umaban 10 and keeps alphanumeric horse ids", () => {
  expect(
    planOverseaRunnerPersist({
      runners: [SECOND_RUNNER],
      raceDate: "2026-07-25",
      venueCode: "A6",
      raceNumber: "05",
      source: "netkeiba",
    }).sourceIdRows,
  ).toStrictEqual([
    {
      race_source: "jra",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0725",
      keibajo_code: "A6",
      race_bango: "05",
      umaban: "10",
      source: "netkeiba",
      source_horse_id: "000a029d4f",
      source_jockey_id: "c5271",
      source_trainer_id: "d1038",
      source_owner_id: null,
      gate_number: 3,
      source_url: null,
    },
  ]);
});

test("planOverseaRunnerPersist skips a runner with a missing horseNumber", () => {
  expect(
    planOverseaRunnerPersist({
      runners: [
        {
          horseNumber: null,
          gate: 7,
          horseName: "テストホース",
          horseId: "2021190001",
          jockeyId: "05504",
          trainerId: "05701",
          trainerAffiliation: "ForeignYard",
        },
      ],
      raceDate: "2026-07-25",
      venueCode: "A6",
      raceNumber: "05",
      source: "netkeiba",
    }),
  ).toStrictEqual({
    sourceIdRows: [],
    identityRows: [],
    reportLines: [
      {
        umaban: null,
        sourceHorseIdPresent: true,
        planned: false,
      },
    ],
    skippedMissingHorseNumber: 1,
    skippedMissingHorseId: 0,
    writesIdentity: false,
  });
});

test("planOverseaRunnerPersist skips a runner with a missing horseId", () => {
  expect(
    planOverseaRunnerPersist({
      runners: [
        {
          horseNumber: 1,
          gate: 7,
          horseName: "テストホース",
          horseId: null,
          jockeyId: "05504",
          trainerId: "05701",
          trainerAffiliation: "ForeignYard",
        },
      ],
      raceDate: "2026-07-25",
      venueCode: "A6",
      raceNumber: "05",
      source: "netkeiba",
    }),
  ).toStrictEqual({
    sourceIdRows: [],
    identityRows: [],
    reportLines: [
      {
        umaban: "01",
        sourceHorseIdPresent: false,
        planned: false,
      },
    ],
    skippedMissingHorseNumber: 0,
    skippedMissingHorseId: 1,
    writesIdentity: false,
  });
});

test("planOverseaRunnerPersist treats an empty horseId string as missing", () => {
  expect(
    planOverseaRunnerPersist({
      runners: [
        {
          horseNumber: 2,
          gate: 3,
          horseName: "サンプルホース",
          horseId: "",
          jockeyId: "05271",
          trainerId: "01038",
          trainerAffiliation: "StableHome",
        },
      ],
      raceDate: "2026-07-25",
      venueCode: "A6",
      raceNumber: "05",
      source: "netkeiba",
    }),
  ).toStrictEqual({
    sourceIdRows: [],
    identityRows: [],
    reportLines: [
      {
        umaban: "02",
        sourceHorseIdPresent: false,
        planned: false,
      },
    ],
    skippedMissingHorseNumber: 0,
    skippedMissingHorseId: 1,
    writesIdentity: false,
  });
});

test("planOverseaRunnerPersist counts a runner missing both number and id as a horseNumber skip", () => {
  expect(
    planOverseaRunnerPersist({
      runners: [
        {
          horseNumber: null,
          gate: null,
          horseName: null,
          horseId: null,
          jockeyId: null,
          trainerId: null,
          trainerAffiliation: null,
        },
      ],
      raceDate: "2026-07-25",
      venueCode: "A6",
      raceNumber: "05",
      source: "netkeiba",
    }),
  ).toStrictEqual({
    sourceIdRows: [],
    identityRows: [],
    reportLines: [
      {
        umaban: null,
        sourceHorseIdPresent: false,
        planned: false,
      },
    ],
    skippedMissingHorseNumber: 1,
    skippedMissingHorseId: 0,
    writesIdentity: false,
  });
});

test("planOverseaRunnerPersist stores null for missing jockey, trainer, owner, url, and invalid gates", () => {
  expect(
    planOverseaRunnerPersist({
      runners: [
        {
          horseNumber: 4,
          gate: null,
          horseName: "NULL GATE",
          horseId: "abcd1234",
          jockeyId: null,
          trainerId: "",
          trainerAffiliation: null,
        },
        {
          horseNumber: 5,
          gate: 0,
          horseName: "ZERO GATE",
          horseId: "efgh5678",
          jockeyId: "",
          trainerId: null,
          trainerAffiliation: null,
        },
        {
          horseNumber: 6,
          gate: 1.5,
          horseName: "FRACTION GATE",
          horseId: "ijkl9012",
          jockeyId: "j1",
          trainerId: "t1",
          trainerAffiliation: null,
        },
      ],
      raceDate: "2026-07-25",
      venueCode: "A6",
      raceNumber: "05",
      source: "netkeiba",
    }).sourceIdRows,
  ).toStrictEqual([
    {
      race_source: "jra",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0725",
      keibajo_code: "A6",
      race_bango: "05",
      umaban: "04",
      source: "netkeiba",
      source_horse_id: "abcd1234",
      source_jockey_id: null,
      source_trainer_id: null,
      source_owner_id: null,
      gate_number: null,
      source_url: null,
    },
    {
      race_source: "jra",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0725",
      keibajo_code: "A6",
      race_bango: "05",
      umaban: "05",
      source: "netkeiba",
      source_horse_id: "efgh5678",
      source_jockey_id: null,
      source_trainer_id: null,
      source_owner_id: null,
      gate_number: null,
      source_url: null,
    },
    {
      race_source: "jra",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0725",
      keibajo_code: "A6",
      race_bango: "05",
      umaban: "06",
      source: "netkeiba",
      source_horse_id: "ijkl9012",
      source_jockey_id: "j1",
      source_trainer_id: "t1",
      source_owner_id: null,
      gate_number: null,
      source_url: null,
    },
  ]);
});

test("planOverseaRunnerPersist writes identity rows only for the jra-van display source", () => {
  expect(
    planOverseaRunnerPersist({
      runners: [COMPLETE_RUNNER, SECOND_RUNNER],
      raceDate: "2026-08-16",
      venueCode: "A8",
      raceNumber: "04",
      source: "jra-van",
    }).identityRows,
  ).toStrictEqual([
    {
      race_source: "jra",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0816",
      keibajo_code: "A8",
      race_bango: "04",
      umaban: "01",
      source: "jra-van",
      source_horse_id: "2021190001",
      horse_name_full: "テストホース",
      jockey_name_full: null,
      trainer_name_full: null,
      owner_name_full: null,
      source_url: null,
    },
    {
      race_source: "jra",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0816",
      keibajo_code: "A8",
      race_bango: "04",
      umaban: "10",
      source: "jra-van",
      source_horse_id: "000a029d4f",
      horse_name_full: "サンプルホース",
      jockey_name_full: null,
      trainer_name_full: null,
      owner_name_full: null,
      source_url: null,
    },
  ]);
});

test("planOverseaRunnerPersist does not write identity when the jra-van horse name is missing", () => {
  expect(
    planOverseaRunnerPersist({
      runners: [
        {
          horseNumber: 1,
          gate: 6,
          horseName: null,
          horseId: "H1021714",
          jockeyId: null,
          trainerId: null,
          trainerAffiliation: null,
        },
        {
          horseNumber: 2,
          gate: 2,
          horseName: "",
          horseId: "H1021966",
          jockeyId: null,
          trainerId: null,
          trainerAffiliation: null,
        },
      ],
      raceDate: "2026-08-16",
      venueCode: "A8",
      raceNumber: "04",
      source: "jra-van",
    }),
  ).toStrictEqual({
    sourceIdRows: [
      {
        race_source: "jra",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0816",
        keibajo_code: "A8",
        race_bango: "04",
        umaban: "01",
        source: "jra-van",
        source_horse_id: "H1021714",
        source_jockey_id: null,
        source_trainer_id: null,
        source_owner_id: null,
        gate_number: 6,
        source_url: null,
      },
      {
        race_source: "jra",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0816",
        keibajo_code: "A8",
        race_bango: "04",
        umaban: "02",
        source: "jra-van",
        source_horse_id: "H1021966",
        source_jockey_id: null,
        source_trainer_id: null,
        source_owner_id: null,
        gate_number: 2,
        source_url: null,
      },
    ],
    identityRows: [],
    reportLines: [
      {
        umaban: "01",
        sourceHorseIdPresent: true,
        planned: true,
      },
      {
        umaban: "02",
        sourceHorseIdPresent: true,
        planned: true,
      },
    ],
    skippedMissingHorseNumber: 0,
    skippedMissingHorseId: 0,
    writesIdentity: true,
  });
});

test("formatOverseaRunnerSourceIdPlanReport prints present and absent source horse ids", () => {
  expect(
    formatOverseaRunnerSourceIdPlanReport(
      planOverseaRunnerPersist({
        runners: [
          COMPLETE_RUNNER,
          {
            horseNumber: null,
            gate: 1,
            horseName: "NO NUM",
            horseId: "abc",
            jockeyId: null,
            trainerId: null,
            trainerAffiliation: null,
          },
          {
            horseNumber: 3,
            gate: 2,
            horseName: "NO ID",
            horseId: null,
            jockeyId: null,
            trainerId: null,
            trainerAffiliation: null,
          },
        ],
        raceDate: "2026-07-25",
        venueCode: "A6",
        raceNumber: "05",
        source: "netkeiba",
      }),
    ),
  ).toStrictEqual([
    "=== Planned oversea_runner_source_id upserts ===",
    "umaban=01 source_horse_id=present",
    "umaban=(none) source_horse_id=present",
    "umaban=03 source_horse_id=absent",
    "skipped missing horseNumber=1 missing horseId=1",
    "identity table write=no",
  ]);
});

test("formatOverseaRunnerSourceIdPlanReport prints (none) when there are no runners", () => {
  expect(
    formatOverseaRunnerSourceIdPlanReport(
      planOverseaRunnerPersist({
        runners: [],
        raceDate: "2026-07-25",
        venueCode: "A6",
        raceNumber: "05",
        source: "netkeiba",
      }),
    ),
  ).toStrictEqual([
    "=== Planned oversea_runner_source_id upserts ===",
    "(none)",
    "skipped missing horseNumber=0 missing horseId=0",
    "identity table write=no",
  ]);
});

test("formatOverseaRunnerSourceIdPlanReport marks identity write=yes for jra-van", () => {
  expect(
    formatOverseaRunnerSourceIdPlanReport(
      planOverseaRunnerPersist({
        runners: [COMPLETE_RUNNER],
        raceDate: "2026-08-16",
        venueCode: "A8",
        raceNumber: "04",
        source: "jra-van",
      }),
    ),
  ).toStrictEqual([
    "=== Planned oversea_runner_source_id upserts ===",
    "umaban=01 source_horse_id=present",
    "skipped missing horseNumber=0 missing horseId=0",
    "identity table write=yes",
  ]);
});

test("buildOverseaRunnerSourceIdUpsert emits a parameterized non-destructive upsert", () => {
  const row: OverseaRunnerSourceIdRow = {
    race_source: "jra",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0725",
    keibajo_code: "A6",
    race_bango: "05",
    umaban: "01",
    source: "netkeiba",
    source_horse_id: "2021190001",
    source_jockey_id: "05504",
    source_trainer_id: "05701",
    source_owner_id: null,
    gate_number: 7,
    source_url: null,
  };
  expect(buildOverseaRunnerSourceIdUpsert(row)).toStrictEqual({
    text: "INSERT INTO oversea_runner_source_id (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, source, source_horse_id, source_jockey_id, source_trainer_id, source_owner_id, gate_number, source_url) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''), NULLIF($10, ''), NULLIF($11, ''), NULLIF($12, '')::smallint, NULLIF($13, '')) ON CONFLICT (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, source) DO UPDATE SET source_horse_id = excluded.source_horse_id, source_jockey_id = excluded.source_jockey_id, source_trainer_id = excluded.source_trainer_id, source_owner_id = excluded.source_owner_id, gate_number = excluded.gate_number, source_url = excluded.source_url, updated_at = now()",
    values: [
      "jra",
      "2026",
      "0725",
      "A6",
      "05",
      "01",
      "netkeiba",
      "2021190001",
      "05504",
      "05701",
      "",
      "7",
      "",
    ],
  });
});

test("buildOverseaRunnerSourceIdUpsert turns null optional fields into empty SQL values", () => {
  expect(
    buildOverseaRunnerSourceIdUpsert({
      race_source: "jra",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0816",
      keibajo_code: "A8",
      race_bango: "04",
      umaban: "08",
      source: "netkeiba",
      source_horse_id: "000a02ca97",
      source_jockey_id: null,
      source_trainer_id: null,
      source_owner_id: null,
      gate_number: null,
      source_url: null,
    }),
  ).toStrictEqual({
    text: "INSERT INTO oversea_runner_source_id (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, source, source_horse_id, source_jockey_id, source_trainer_id, source_owner_id, gate_number, source_url) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''), NULLIF($10, ''), NULLIF($11, ''), NULLIF($12, '')::smallint, NULLIF($13, '')) ON CONFLICT (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, source) DO UPDATE SET source_horse_id = excluded.source_horse_id, source_jockey_id = excluded.source_jockey_id, source_trainer_id = excluded.source_trainer_id, source_owner_id = excluded.source_owner_id, gate_number = excluded.gate_number, source_url = excluded.source_url, updated_at = now()",
    values: ["jra", "2026", "0816", "A8", "04", "08", "netkeiba", "000a02ca97", "", "", "", "", ""],
  });
});

test("buildOverseaRunnerIdentityUpsert emits a parameterized identity upsert", () => {
  const row: OverseaRunnerIdentityRow = {
    race_source: "jra",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0816",
    keibajo_code: "A8",
    race_bango: "04",
    umaban: "01",
    source: "jra-van",
    source_horse_id: "H1021714",
    horse_name_full: "Zeus Olympios",
    jockey_name_full: "C．リー",
    trainer_name_full: "K．バーク",
    owner_name_full: "Owner",
    source_url: null,
  };
  expect(buildOverseaRunnerIdentityUpsert(row)).toStrictEqual({
    text: "INSERT INTO oversea_runner_identity (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, source, source_horse_id, horse_name_full, jockey_name_full, trainer_name_full, owner_name_full, source_url) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULLIF($10, ''), NULLIF($11, ''), NULLIF($12, ''), NULLIF($13, '')) ON CONFLICT (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban) DO UPDATE SET source = excluded.source, source_horse_id = excluded.source_horse_id, horse_name_full = excluded.horse_name_full, jockey_name_full = excluded.jockey_name_full, trainer_name_full = excluded.trainer_name_full, owner_name_full = excluded.owner_name_full, source_url = excluded.source_url, updated_at = now()",
    values: [
      "jra",
      "2026",
      "0816",
      "A8",
      "04",
      "01",
      "jra-van",
      "H1021714",
      "Zeus Olympios",
      "C．リー",
      "K．バーク",
      "Owner",
      "",
    ],
  });
});

test("buildOverseaRunnerIdentityUpsert turns null name fields into empty SQL values", () => {
  expect(
    buildOverseaRunnerIdentityUpsert({
      race_source: "jra",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0816",
      keibajo_code: "A8",
      race_bango: "04",
      umaban: "03",
      source: "jra-van",
      source_horse_id: "H1019915",
      horse_name_full: "Sixpence",
      jockey_name_full: null,
      trainer_name_full: null,
      owner_name_full: null,
      source_url: null,
    }),
  ).toStrictEqual({
    text: "INSERT INTO oversea_runner_identity (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, source, source_horse_id, horse_name_full, jockey_name_full, trainer_name_full, owner_name_full, source_url) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULLIF($10, ''), NULLIF($11, ''), NULLIF($12, ''), NULLIF($13, '')) ON CONFLICT (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban) DO UPDATE SET source = excluded.source, source_horse_id = excluded.source_horse_id, horse_name_full = excluded.horse_name_full, jockey_name_full = excluded.jockey_name_full, trainer_name_full = excluded.trainer_name_full, owner_name_full = excluded.owner_name_full, source_url = excluded.source_url, updated_at = now()",
    values: [
      "jra",
      "2026",
      "0816",
      "A8",
      "04",
      "03",
      "jra-van",
      "H1019915",
      "Sixpence",
      "",
      "",
      "",
      "",
    ],
  });
});

test("buildOverseaPersistStatements emits only source_id upserts for netkeiba", () => {
  const statements = buildOverseaPersistStatements(planOverseaRunnerPersist(NETKEIBA_PLAN_INPUT));
  expect(statements).toStrictEqual([
    {
      text: "INSERT INTO oversea_runner_source_id (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, source, source_horse_id, source_jockey_id, source_trainer_id, source_owner_id, gate_number, source_url) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''), NULLIF($10, ''), NULLIF($11, ''), NULLIF($12, '')::smallint, NULLIF($13, '')) ON CONFLICT (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, source) DO UPDATE SET source_horse_id = excluded.source_horse_id, source_jockey_id = excluded.source_jockey_id, source_trainer_id = excluded.source_trainer_id, source_owner_id = excluded.source_owner_id, gate_number = excluded.gate_number, source_url = excluded.source_url, updated_at = now()",
      values: [
        "jra",
        "2026",
        "0725",
        "A6",
        "05",
        "01",
        "netkeiba",
        "2021190001",
        "05504",
        "05701",
        "",
        "7",
        "",
      ],
    },
  ]);
});

test("buildOverseaPersistStatements emits source_id then identity for jra-van", () => {
  const statements = buildOverseaPersistStatements(
    planOverseaRunnerPersist({
      runners: [COMPLETE_RUNNER],
      raceDate: "2026-08-16",
      venueCode: "A8",
      raceNumber: "04",
      source: "jra-van",
    }),
  );
  expect(statements).toStrictEqual([
    {
      text: "INSERT INTO oversea_runner_source_id (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, source, source_horse_id, source_jockey_id, source_trainer_id, source_owner_id, gate_number, source_url) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''), NULLIF($10, ''), NULLIF($11, ''), NULLIF($12, '')::smallint, NULLIF($13, '')) ON CONFLICT (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, source) DO UPDATE SET source_horse_id = excluded.source_horse_id, source_jockey_id = excluded.source_jockey_id, source_trainer_id = excluded.source_trainer_id, source_owner_id = excluded.source_owner_id, gate_number = excluded.gate_number, source_url = excluded.source_url, updated_at = now()",
      values: [
        "jra",
        "2026",
        "0816",
        "A8",
        "04",
        "01",
        "jra-van",
        "2021190001",
        "05504",
        "05701",
        "",
        "7",
        "",
      ],
    },
    {
      text: "INSERT INTO oversea_runner_identity (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, source, source_horse_id, horse_name_full, jockey_name_full, trainer_name_full, owner_name_full, source_url) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULLIF($10, ''), NULLIF($11, ''), NULLIF($12, ''), NULLIF($13, '')) ON CONFLICT (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban) DO UPDATE SET source = excluded.source, source_horse_id = excluded.source_horse_id, horse_name_full = excluded.horse_name_full, jockey_name_full = excluded.jockey_name_full, trainer_name_full = excluded.trainer_name_full, owner_name_full = excluded.owner_name_full, source_url = excluded.source_url, updated_at = now()",
      values: [
        "jra",
        "2026",
        "0816",
        "A8",
        "04",
        "01",
        "jra-van",
        "2021190001",
        "テストホース",
        "",
        "",
        "",
        "",
      ],
    },
  ]);
});

test("buildOverseaPersistStatements is empty when every runner is skipped", () => {
  expect(
    buildOverseaPersistStatements(
      planOverseaRunnerPersist({
        runners: [
          {
            horseNumber: null,
            gate: 1,
            horseName: "SKIP",
            horseId: null,
            jockeyId: null,
            trainerId: null,
            trainerAffiliation: null,
          },
        ],
        raceDate: "2026-07-25",
        venueCode: "A6",
        raceNumber: "05",
        source: "netkeiba",
      }),
    ),
  ).toStrictEqual([]);
});
