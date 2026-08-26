// Run with bun. Tests for fail-closed race-chain Container routing.

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";

import {
  qualifyPredictionContainerDoName,
  resolveContainerNamespaceForRole,
  resolveRaceContainerRoute,
} from "./race-container-routing";

const legacyNamespace = { role: "legacy" };
const raceNamespace = { role: "race-chain" };

const makeEnv = (overrides: Partial<Env> = {}): Env =>
  ({
    FINISH_POSITION_PREDICT_CONTAINER: legacyNamespace,
    FINISH_POSITION_RACE_CHAIN_CONTAINER: raceNamespace,
    RACE_CHAIN_CONTAINER_CATEGORIES: "jra,nar",
    RACE_CHAIN_CONTAINER_ENABLED: "1",
    ...overrides,
  }) as unknown as Env;

beforeEach(() => {
  vi.clearAllMocks();
});

test("routes an allowlisted focused-full R2 day-base hit to the race-chain binding", async () => {
  const route = await resolveRaceContainerRoute({
    category: "jra",
    env: makeEnv(),
    focusedFull: true,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260823",
  });

  expect(route).toStrictEqual({ namespace: raceNamespace, role: "race-chain" });
});

test("keeps the production flag off path on the legacy binding without an R2 read", async () => {
  const route = await resolveRaceContainerRoute({
    category: "jra",
    env: makeEnv({ RACE_CHAIN_CONTAINER_ENABLED: "0" }),
    focusedFull: true,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260823",
  });

  expect(route).toStrictEqual({ namespace: legacyNamespace, role: "legacy" });
});

test("keeps a DAY_BASE_REQUIRED replacement on legacy without another R2 read", async () => {
  const route = await resolveRaceContainerRoute({
    category: "jra",
    env: makeEnv(),
    forceLegacy: true,
    focusedFull: true,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260823",
  });

  expect(route).toStrictEqual({ namespace: legacyNamespace, role: "legacy" });
});

test("keeps non-focused and non-allowlisted work on the legacy binding", async () => {
  const nonFocused = await resolveRaceContainerRoute({
    category: "jra",
    env: makeEnv(),
    focusedFull: false,
    runYmd: "20260823",
  });
  const nonAllowlisted = await resolveRaceContainerRoute({
    category: "ban-ei",
    env: makeEnv(),
    focusedFull: true,
    keibajoCode: "83",
    raceBango: "01",
    runYmd: "20260823",
  });

  expect(nonFocused).toStrictEqual({ namespace: legacyNamespace, role: "legacy" });
  expect(nonAllowlisted).toStrictEqual({ namespace: legacyNamespace, role: "legacy" });
});

test("keeps routing independent from cache probes and falls back only when binding is absent", async () => {
  const route = await resolveRaceContainerRoute({
    category: "nar",
    env: makeEnv(),
    focusedFull: true,
    keibajoCode: "44",
    raceBango: "01",
    runYmd: "20260823",
  });
  const absent = await resolveRaceContainerRoute({
    category: "nar",
    env: makeEnv({ FINISH_POSITION_RACE_CHAIN_CONTAINER: undefined }),
    focusedFull: true,
    keibajoCode: "44",
    raceBango: "01",
    runYmd: "20260823",
  });

  expect(route).toStrictEqual({ namespace: raceNamespace, role: "race-chain" });
  expect(absent).toStrictEqual({ namespace: legacyNamespace, role: "legacy" });
});

test("qualifies race DO names and resolves stop bindings by explicit role", () => {
  const env = makeEnv();

  expect(qualifyPredictionContainerDoName("predict-jra-1", "legacy")).toBe("predict-jra-1");
  expect(qualifyPredictionContainerDoName("predict-jra-1", "race-chain")).toBe(
    "race-chain-predict-jra-1",
  );
  expect(resolveContainerNamespaceForRole(env, undefined)).toBe(legacyNamespace);
  expect(resolveContainerNamespaceForRole(env, "legacy")).toBe(legacyNamespace);
  expect(resolveContainerNamespaceForRole(env, "race-chain")).toBe(raceNamespace);
  expect(() =>
    resolveContainerNamespaceForRole(
      makeEnv({ FINISH_POSITION_RACE_CHAIN_CONTAINER: undefined }),
      "race-chain",
    ),
  ).toThrow("Race-chain container binding is unavailable");
});
