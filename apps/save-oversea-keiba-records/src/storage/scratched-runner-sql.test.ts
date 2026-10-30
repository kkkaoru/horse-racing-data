// This file runs with Bun and Vitest. Pure statement building, no database.
import { expect, it } from "vitest";
import { buildScratchedRunnerStatements } from "./scratched-runner-sql";

const RACE_KEY = {
  kaisai_nen: "2026",
  kaisai_tsukihi: "1101",
  keibajo_code: "A4",
  race_bango: "12",
};

it("clears the gate before marking the unplaced row 取消, per scratched horse", () => {
  const statements = buildScratchedRunnerStatements({
    raceKey: RACE_KEY,
    horseNames: ["フォーエバー ヤング　"],
  });
  expect(
    statements.map((statement) => statement.text.split(" ").slice(0, 2).join(" ")),
  ).toStrictEqual(["UPDATE oversea_runner_source_id", "UPDATE jvd_se"]);
  expect(statements.map((statement) => statement.values)).toStrictEqual([
    ["2026", "1101", "A4", "12", "フォーエバーヤング"],
    ["2026", "1101", "A4", "12", "フォーエバーヤング", "1"],
  ]);
  expect(statements[1]?.text).toBe(`UPDATE jvd_se s SET ijo_kubun_code = $6
WHERE s.kaisai_nen = $1 AND s.kaisai_tsukihi = $2 AND s.keibajo_code = $3 AND s.race_bango = $4 AND replace(replace(s.bamei, chr(12288), ''), ' ', '') = $5 AND btrim(s.ijo_kubun_code) IN ('', '0') AND btrim(s.kakutei_chakujun) IN ('', '0', '00')`);
});

it("builds nothing when the card has no scratched horse", () => {
  expect(buildScratchedRunnerStatements({ raceKey: RACE_KEY, horseNames: [] })).toStrictEqual([]);
});
