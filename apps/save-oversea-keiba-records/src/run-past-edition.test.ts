// This test runs with Bun and Vitest. Files and the database are mocked.
import { beforeEach, expect, it, vi } from "vitest";
import { parsePastEditionArgs, runPastEdition, type PastEditionPorts } from "./run-past-edition";

const PROFILE: string = JSON.stringify({
  tableMarker: 'data-demo="result"',
  horsePathPrefix: "/animal/",
  jockeyPathPrefix: "/rider/",
  trainerPathPrefix: "/coach/",
  datePattern: "Date (\\d{4})/(\\d{1,2})/(\\d{1,2})",
  coursePattern: "Course (芝|ダ)(左|右|直) (\\d+)m",
  trainerPrefixes: ["[X]"],
  fields: {
    finishPosition: 0,
    gate: 1,
    horseNumber: 2,
    horseName: 3,
    sexAge: 4,
    weightCarried: 5,
    jockey: 6,
    time: 7,
    winOdds: 8,
    popularity: 9,
    trainer: 10,
  },
});
const HTML: string =
  '<html>Date 2025/11/1 Course ダ左 2000m<table data-demo="result">' +
  '<tr><td>1</td><td>3</td><td>5</td><td><a href="/animal/h1/">Alpha</a></td><td>牡4</td><td>57</td><td><a href="/rider/r1/">ライダ</a></td><td>2:00.19</td><td>3.4</td><td>1</td><td><a href="/coach/c1/">[X] コーチ</a></td></tr>' +
  "<tr><td>中</td><td></td><td>2</td><td>Gamma</td><td>牝4</td><td>55</td><td>ライダ３</td><td></td><td></td><td></td><td>コーチ３</td></tr>" +
  "<tr><td>取</td><td></td><td>7</td><td>Beta</td><td>牝4</td><td></td><td>ライダ２</td><td></td><td></td><td></td><td>コーチ２</td></tr>" +
  "</table></html>";
const ARGS: readonly string[] = [
  "/private/result.html",
  "--source-race-id",
  "2025FP010109",
  "--race-name",
  "テストカップ",
  "--venue-code",
  "A4",
  "--race-number",
  "09",
];
const ENV = { OVERSEA_RACE_RESULT_PROFILE_PATH: "/private/profile.json" };
const execute = vi.fn();
const transactionExecute = vi.fn();
const log = vi.fn();
const readFile = vi.fn();
const ports: PastEditionPorts = {
  readFile,
  executor: { execute },
  withTransaction: async (callback) => callback({ execute: transactionExecute }),
  log,
};

beforeEach(() => {
  vi.resetAllMocks();
  readFile.mockImplementation(async (path: string) =>
    path === "/private/profile.json" ? PROFILE : HTML,
  );
  transactionExecute.mockResolvedValue({ rowCount: 1, rows: [] });
});

it("plans a missing race and all runners in dry-run mode without writing", async () => {
  execute.mockImplementation(async ({ text }: { text: string }) =>
    text.includes("count(*)") ? { rowCount: 1, rows: [{ count: "0" }] } : { rowCount: 0, rows: [] },
  );
  await expect(runPastEdition({ argv: ARGS, env: ENV, ports })).resolves.toBe(
    '{"raceKey":"2025/1101/A4/09","mode":"dry-run","insertRace":true,"insertRunners":["05","02","07"],"keptExistingRunners":0,"sourceIdInserts":1,"parsedRunners":3,"starters":2}',
  );
  expect(execute.mock.calls[0]?.[0].values).toStrictEqual(["2025", "1101", "A4", "09"]);
  expect(transactionExecute).not.toHaveBeenCalled();
  expect(log).toHaveBeenCalledTimes(1);
});

it("writes only missing runners and never rewrites an existing race", async () => {
  execute.mockImplementation(async ({ text }: { text: string }) =>
    text.includes("count(*)")
      ? { rowCount: 1, rows: [{ count: "1" }] }
      : { rowCount: 1, rows: [{ umaban: "05" }] },
  );
  await expect(runPastEdition({ argv: [...ARGS, "--apply"], env: ENV, ports })).resolves.toBe(
    '{"raceKey":"2025/1101/A4/09","mode":"applied","insertRace":false,"insertRunners":["02","07"],"keptExistingRunners":1,"sourceIdInserts":1,"parsedRunners":3,"starters":2}',
  );
  const statements = transactionExecute.mock.calls.map(([statement]) => statement);
  expect(
    statements.map((statement) => /INSERT INTO (\w+)/u.exec(statement.text)?.[1]),
  ).toStrictEqual(["jvd_se", "jvd_se", "oversea_runner_source_id"]);
  expect(statements[2].values).toStrictEqual([
    "2025",
    "1101",
    "A4",
    "09",
    "05",
    "h1",
    "r1",
    "c1",
    "https://db.netkeiba.com/race/2025FP010109/",
  ]);
});

it("stores finish, abnormality and time codes on inserted runners", async () => {
  execute.mockImplementation(async ({ text }: { text: string }) =>
    text.includes("count(*)") ? { rowCount: 1, rows: [{ count: "0" }] } : { rowCount: 0, rows: [] },
  );
  await runPastEdition({ argv: [...ARGS, "--apply"], env: ENV, ports });
  const statements = transactionExecute.mock.calls.map(([statement]) => statement);
  const columns: string[] = statements[1].text.match(/\(([^)]+)\) VALUES/u)[1].split(", ");
  const value = (index: number, column: string): string =>
    statements[index].values[columns.indexOf(column)];
  expect([
    value(1, "kakutei_chakujun"),
    value(1, "ijo_kubun_code"),
    value(1, "soha_time"),
    value(1, "chokyoshimei_ryakusho"),
  ]).toStrictEqual(["01", "0", "2001", "コーチ　"]);
  expect([
    value(2, "kakutei_chakujun"),
    value(2, "ijo_kubun_code"),
    value(2, "soha_time"),
  ]).toStrictEqual(["00", "4", "0000"]);
  expect(value(3, "ijo_kubun_code")).toBe("1");
  const raceColumns: string[] = statements[0].text.match(/\(([^)]+)\) VALUES/u)[1].split(", ");
  expect(statements[0].values[raceColumns.indexOf("shusso_tosu")]).toBe("02");
});

it("requires the private result profile", async () => {
  await expect(runPastEdition({ argv: ARGS, env: {}, ports })).rejects.toThrow(
    "Set OVERSEA_RACE_RESULT_PROFILE_PATH to the private race-result profile.",
  );
});

it("rejects an invalid private result profile", async () => {
  readFile.mockImplementation(async (path: string) =>
    path === "/private/profile.json" ? "{}" : HTML,
  );
  await expect(runPastEdition({ argv: ARGS, env: ENV, ports })).rejects.toThrow(
    "Private race-result profile is invalid.",
  );
});

it("parses explicit options and rejects incomplete or unknown arguments", () => {
  expect(parsePastEditionArgs([...ARGS, "--apply"])).toStrictEqual({
    file: "/private/result.html",
    sourceRaceId: "2025FP010109",
    raceName: "テストカップ",
    venueCode: "A4",
    raceNumber: "09",
    apply: true,
  });
  expect(() => parsePastEditionArgs(ARGS.slice(0, 7))).toThrow(
    "Use past-edition FILE --source-race-id ID --race-name NAME --venue-code CODE --race-number NN [--apply].",
  );
  expect(() => parsePastEditionArgs([...ARGS, "--force"])).toThrow(
    "Use past-edition FILE --source-race-id ID --race-name NAME --venue-code CODE --race-number NN [--apply].",
  );
  expect(() => parsePastEditionArgs([])).toThrow(
    "Use past-edition FILE --source-race-id ID --race-name NAME --venue-code CODE --race-number NN [--apply].",
  );
});
