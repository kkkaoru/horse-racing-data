// Run with bun. Tests for focused full completion guard.

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";

const { neonMock, queryMock } = vi.hoisted(() => {
  const query = vi.fn(async () => [{ actual_rows: 12, complete: true, expected_rows: 12 }]);
  return { neonMock: vi.fn(() => ({ query })), queryMock: query };
});

vi.mock("@neondatabase/serverless", () => ({ neon: neonMock }));

import { isFocusedFullPredictionComplete } from "./focused-full-completion";

const makeEnv = (): Env => ({ NEON_DATABASE_URL: "postgres://example" }) as Env;

beforeEach(() => {
  neonMock.mockClear();
  queryMock.mockClear();
  queryMock.mockResolvedValue([{ actual_rows: 12, complete: true, expected_rows: 12 }]);
});

test("isFocusedFullPredictionComplete returns true when actual rows cover expected rows", async () => {
  await expect(
    isFocusedFullPredictionComplete({
      category: "nar",
      env: makeEnv(),
      keibajoCode: "50",
      raceBango: "12",
      runYmd: "20260701",
    }),
  ).resolves.toBe(true);
  expect(neonMock).toHaveBeenCalledWith("postgres://example");
  expect(queryMock).toHaveBeenCalledWith(expect.stringContaining("race_entry_corner_features"), [
    "nar",
    "2026",
    "0701",
    "50",
    "12",
    "nar",
    "iter40-nar-settransformer-blend-v1",
  ]);
});

test("isFocusedFullPredictionComplete checks NAR transformer rows by default", async () => {
  await expect(
    isFocusedFullPredictionComplete({
      category: "nar",
      env: makeEnv(),
      keibajoCode: "45",
      raceBango: "03",
      runYmd: "20260710",
    }),
  ).resolves.toBe(true);
  expect(queryMock).toHaveBeenLastCalledWith(expect.stringContaining("else $7"), [
    "nar",
    "2026",
    "0710",
    "45",
    "03",
    "nar",
    "iter40-nar-settransformer-blend-v1",
  ]);
});

test("isFocusedFullPredictionComplete checks NAR clean188 rows when transformer is disabled", async () => {
  await expect(
    isFocusedFullPredictionComplete({
      category: "nar",
      env: { ...makeEnv(), NAR_TRANSFORMER_BLEND_ENABLED: "off" },
      keibajoCode: "45",
      raceBango: "03",
      runYmd: "20260710",
    }),
  ).resolves.toBe(true);
  expect(queryMock).toHaveBeenLastCalledWith(expect.any(String), [
    "nar",
    "2026",
    "0710",
    "45",
    "03",
    "nar",
    "iter12-nar-xgb-hpo-v8-clean188",
  ]);
});

test("isFocusedFullPredictionComplete returns false when expected rows are absent", async () => {
  queryMock.mockResolvedValue([{ actual_rows: 0, complete: true, expected_rows: 0 }]);
  await expect(
    isFocusedFullPredictionComplete({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "05",
      raceBango: "11",
      runYmd: "20260628",
    }),
  ).resolves.toBe(false);
});

test("isFocusedFullPredictionComplete returns false when no single model covers expected rows", async () => {
  queryMock.mockResolvedValue([
    {
      actual_rows: "12" as unknown as number,
      complete: false,
      expected_rows: "12" as unknown as number,
    },
  ]);
  await expect(
    isFocusedFullPredictionComplete({
      category: "ban-ei",
      env: makeEnv(),
      keibajoCode: "65",
      raceBango: "01",
      runYmd: "20260701",
    }),
  ).resolves.toBe(false);
  expect(queryMock).toHaveBeenCalledWith(expect.any(String), [
    "nar",
    "2026",
    "0701",
    "65",
    "01",
    "ban-ei",
    "iter40-nar-settransformer-blend-v1",
  ]);
});

test("isFocusedFullPredictionComplete checks the current routed model version", async () => {
  await expect(
    isFocusedFullPredictionComplete({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "02",
      raceBango: "02",
      runYmd: "20260621",
    }),
  ).resolves.toBe(true);
  expect(queryMock).toHaveBeenLastCalledWith(expect.stringContaining("kyoso_joken_code"), [
    "jra",
    "2026",
    "0621",
    "02",
    "02",
    "jra",
    "iter40-nar-settransformer-blend-v1",
  ]);
  expect(queryMock).toHaveBeenLastCalledWith(
    expect.stringContaining("jra-cb-v9-sim-2013-clean-jockey-pedigree269"),
    ["jra", "2026", "0621", "02", "02", "jra", "iter40-nar-settransformer-blend-v1"],
  );
  expect(queryMock).toHaveBeenLastCalledWith(
    expect.stringContaining("p.model_version = expected_model.expected_model_version"),
    ["jra", "2026", "0621", "02", "02", "jra", "iter40-nar-settransformer-blend-v1"],
  );
});

test("isFocusedFullPredictionComplete checks the JRA prior-corner routed model version", async () => {
  await expect(
    isFocusedFullPredictionComplete({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "10",
      raceBango: "06",
      runYmd: "20260705",
    }),
  ).resolves.toBe(true);
  expect(queryMock).toHaveBeenLastCalledWith(
    expect.stringContaining("jra-cb-v10-prior-corner274-2013"),
    ["jra", "2026", "0705", "10", "06", "jra", "iter40-nar-settransformer-blend-v1"],
  );
  expect(queryMock).toHaveBeenLastCalledWith(expect.stringContaining("track_code"), [
    "jra",
    "2026",
    "0705",
    "10",
    "06",
    "jra",
    "iter40-nar-settransformer-blend-v1",
  ]);
  expect(queryMock).toHaveBeenLastCalledWith(expect.stringContaining("shusso_tosu"), [
    "jra",
    "2026",
    "0705",
    "10",
    "06",
    "jra",
    "iter40-nar-settransformer-blend-v1",
  ]);
});

test("isFocusedFullPredictionComplete accepts Postgres text true booleans", async () => {
  queryMock.mockResolvedValue([
    {
      actual_rows: "12" as unknown as number,
      complete: "t" as unknown as boolean,
      expected_rows: "12" as unknown as number,
    },
  ]);
  await expect(
    isFocusedFullPredictionComplete({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "05",
      raceBango: "11",
      runYmd: "20260628",
    }),
  ).resolves.toBe(true);
});

test("isFocusedFullPredictionComplete returns false when Neon returns no rows", async () => {
  queryMock.mockResolvedValue([]);
  await expect(
    isFocusedFullPredictionComplete({
      category: "nar",
      env: makeEnv(),
      keibajoCode: "50",
      raceBango: "12",
      runYmd: "20260701",
    }),
  ).resolves.toBe(false);
});
