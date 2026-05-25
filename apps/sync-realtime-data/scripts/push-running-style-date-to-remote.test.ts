// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";
import {
  buildInsertSql,
  parsePushRunningStyleDateCliArgs,
} from "./push-running-style-date-to-remote";

vi.mock("../src/running-style-date-progress", () => ({
  resolveRunningStyleDateYmd: vi.fn((dateRaw: string, _year: number | undefined, _now: Date) =>
    dateRaw.replaceAll("-", ""),
  ),
}));

afterEach(() => {
  vi.restoreAllMocks();
});

it("parsePushRunningStyleDateCliArgs reads --date", () => {
  expect(parsePushRunningStyleDateCliArgs(["--date", "20260524"]).dateYmd).toBe("20260524");
});

it("parsePushRunningStyleDateCliArgs reads --date with --year", () => {
  expect(
    parsePushRunningStyleDateCliArgs(["--date", "05-24", "--year", "2026"]).dateYmd,
  ).toBe("0524");
});

it("parsePushRunningStyleDateCliArgs throws when --date missing", () => {
  expect(() => parsePushRunningStyleDateCliArgs([])).toThrow(
    "Usage: bun run running-style:push-remote",
  );
});

it("parsePushRunningStyleDateCliArgs throws when --date has no value", () => {
  expect(() => parsePushRunningStyleDateCliArgs(["--date"])).toThrow("--date requires a value");
});

it("parsePushRunningStyleDateCliArgs throws on unknown argument", () => {
  expect(() => parsePushRunningStyleDateCliArgs(["--date", "20260524", "--other"])).toThrow(
    "Unknown argument: --other",
  );
});

it("parsePushRunningStyleDateCliArgs throws when --year has no value", () => {
  expect(() => parsePushRunningStyleDateCliArgs(["--date", "20260524", "--year"])).toThrow(
    "--year requires a value",
  );
});

it("buildInsertSql renders SQL with null bamei and decimal probabilities", () => {
  expect(
    buildInsertSql({
      bamei: null,
      category: "jra",
      horse_number: 1,
      kaisai_nen: "2026",
      ketto_toroku_bango: "2024100001",
      model_version: "v7-lineage",
      p_nige: 0.1,
      p_oikomi: 0.2,
      p_sashi: 0.3,
      p_senkou: 0.4,
      predicted_at: "2026-05-24T11:00:00+09:00",
      predicted_label: "senkou",
      race_key: "jra:2026:0524:08:01",
    }),
  ).toBe(
    "insert or replace into race_running_styles (\n  race_key, horse_number, ketto_toroku_bango, bamei, category, kaisai_nen,\n  model_version, p_nige, p_senkou, p_sashi, p_oikomi, predicted_label, predicted_at\n) values (\n  'jra:2026:0524:08:01',\n  1,\n  '2024100001',\n  null,\n  'jra',\n  '2026',\n  'v7-lineage',\n  0.1,\n  0.4,\n  0.3,\n  0.2,\n  'senkou',\n  '2026-05-24T11:00:00+09:00'\n);",
  );
});

it("buildInsertSql escapes single quotes in string values", () => {
  expect(
    buildInsertSql({
      bamei: "サ'ンプル",
      category: "jra",
      horse_number: 2,
      kaisai_nen: "2026",
      ketto_toroku_bango: "2024100002",
      model_version: "v7-lineage",
      p_nige: 0,
      p_oikomi: 0,
      p_sashi: 0,
      p_senkou: 1,
      predicted_at: "x",
      predicted_label: "nige",
      race_key: "key",
    }),
  ).toBe(
    "insert or replace into race_running_styles (\n  race_key, horse_number, ketto_toroku_bango, bamei, category, kaisai_nen,\n  model_version, p_nige, p_senkou, p_sashi, p_oikomi, predicted_label, predicted_at\n) values (\n  'key',\n  2,\n  '2024100002',\n  'サ''ンプル',\n  'jra',\n  '2026',\n  'v7-lineage',\n  0,\n  1,\n  0,\n  0,\n  'nige',\n  'x'\n);",
  );
});
