// Run with bun. Tests for fail-closed race-chain Container routing.

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";

const { headDayBaseObjectMock } = vi.hoisted(() => ({
  headDayBaseObjectMock: vi.fn(async (): Promise<R2Object | null> => null),
}));

vi.mock("./day-base-prewarm-pickup", () => ({ headDayBaseObject: headDayBaseObjectMock }));

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
  headDayBaseObjectMock.mockClear();
  headDayBaseObjectMock.mockResolvedValue({} as R2Object);
});

test("routes an allowlisted focused-full R2 day-base hit to the race-chain binding", async () => {
  const route = await resolveRaceContainerRoute({
    category: "jra",
    env: makeEnv(),
    focusedFull: true,
    runYmd: "20260823",
  });

  expect(route).toStrictEqual({ namespace: raceNamespace, role: "race-chain" });
  expect(headDayBaseObjectMock).toHaveBeenCalledTimes(1);
});

test("keeps the production flag off path on the legacy binding without an R2 read", async () => {
  const route = await resolveRaceContainerRoute({
    category: "jra",
    env: makeEnv({ RACE_CHAIN_CONTAINER_ENABLED: "0" }),
    focusedFull: true,
    runYmd: "20260823",
  });

  expect(route).toStrictEqual({ namespace: legacyNamespace, role: "legacy" });
  expect(headDayBaseObjectMock).not.toHaveBeenCalled();
});

test("keeps a DAY_BASE_REQUIRED replacement on legacy without another R2 read", async () => {
  const route = await resolveRaceContainerRoute({
    category: "jra",
    env: makeEnv(),
    forceLegacy: true,
    focusedFull: true,
    runYmd: "20260823",
  });

  expect(route).toStrictEqual({ namespace: legacyNamespace, role: "legacy" });
  expect(headDayBaseObjectMock).not.toHaveBeenCalled();
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
    runYmd: "20260823",
  });

  expect(nonFocused).toStrictEqual({ namespace: legacyNamespace, role: "legacy" });
  expect(nonAllowlisted).toStrictEqual({ namespace: legacyNamespace, role: "legacy" });
  expect(headDayBaseObjectMock).not.toHaveBeenCalled();
});

test("falls back when R2 misses, HEAD fails, or the race binding is absent", async () => {
  headDayBaseObjectMock.mockResolvedValueOnce(null);
  const miss = await resolveRaceContainerRoute({
    category: "nar",
    env: makeEnv(),
    focusedFull: true,
    runYmd: "20260823",
  });
  headDayBaseObjectMock.mockRejectedValueOnce(new Error("R2 unavailable"));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const failed = await resolveRaceContainerRoute({
    category: "nar",
    env: makeEnv(),
    focusedFull: true,
    runYmd: "20260823",
  });
  const absent = await resolveRaceContainerRoute({
    category: "nar",
    env: makeEnv({ FINISH_POSITION_RACE_CHAIN_CONTAINER: undefined }),
    focusedFull: true,
    runYmd: "20260823",
  });

  expect(miss).toStrictEqual({ namespace: legacyNamespace, role: "legacy" });
  expect(failed).toStrictEqual({ namespace: legacyNamespace, role: "legacy" });
  expect(absent).toStrictEqual({ namespace: legacyNamespace, role: "legacy" });
  expect(warnSpy).toHaveBeenCalledWith(
    "[race-container-routing] day-base HEAD failed category=nar runYmd=20260823: Error: R2 unavailable",
  );
  warnSpy.mockRestore();
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
