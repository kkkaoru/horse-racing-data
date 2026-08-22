// Run with bun. Tests for the race-sharded Container DO name resolver.

import { expect, test } from "vitest";
import {
  listDayBasePickupDoNames,
  PREDICT_DO_NAME_PREFIX,
  resolvePredictDoName,
} from "./predict-do-shard";
import type { Env } from "./types";

const makeEnv = (overrides: Partial<Env> = {}): Env => ({ ...overrides }) as Env;

test("PREDICT_DO_NAME_PREFIX is predict-", () => {
  expect(PREDICT_DO_NAME_PREFIX).toBe("predict-");
});

// --- flag off: byte-identical to the pre-sharding scheme ---

test("flag unset returns the unsharded category-only name even with race scope present", () => {
  expect(
    resolvePredictDoName({ category: "jra", env: makeEnv(), keibajoCode: "05", raceBango: "01" }),
  ).toBe("predict-jra");
});

test("flag set to a non-1 value returns the unsharded category-only name", () => {
  expect(
    resolvePredictDoName({
      category: "jra",
      env: makeEnv({ RACE_SHARDED_DO: "0" }),
      keibajoCode: "05",
      raceBango: "01",
    }),
  ).toBe("predict-jra");
});

test("flag on but no race scope returns the unsharded category-only name", () => {
  expect(resolvePredictDoName({ category: "nar", env: makeEnv({ RACE_SHARDED_DO: "1" }) })).toBe(
    "predict-nar",
  );
});

// --- determinism ---

test("resolves the exact same DO name across repeated calls with identical inputs", () => {
  const env = makeEnv({ RACE_SHARDED_DO: "1" });
  const first = resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "01" });
  const second = resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "01" });
  expect(first).toBe("predict-jra-0");
  expect(second).toBe("predict-jra-0");
});

// --- same race -> same DO, different race -> different DO ---

test("the same race resolves to the same DO on separate calls", () => {
  const env = makeEnv({ RACE_SHARDED_DO: "1" });
  expect(resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "01" })).toBe(
    "predict-jra-0",
  );
  expect(resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "01" })).toBe(
    "predict-jra-0",
  );
});

test("a different race at the same venue resolves to a different DO", () => {
  const env = makeEnv({ RACE_SHARDED_DO: "1" });
  expect(resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "01" })).toBe(
    "predict-jra-0",
  );
  expect(resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "02" })).toBe(
    "predict-jra-2",
  );
});

test("race 1 at two different venues does not collide onto the same shard", () => {
  const env = makeEnv({ RACE_SHARDED_DO: "1" });
  expect(resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "01" })).toBe(
    "predict-jra-0",
  );
  expect(resolvePredictDoName({ category: "jra", env, keibajoCode: "06", raceBango: "01" })).toBe(
    "predict-jra-0",
  );
  expect(resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "03" })).toBe(
    "predict-jra-1",
  );
});

// --- concurrency upper bound (RACE_SHARD_MAX_CONCURRENT) ---

test("defaults the shard modulus to 3 when RACE_SHARD_MAX_CONCURRENT is unset", () => {
  const env = makeEnv({ RACE_SHARDED_DO: "1" });
  expect(resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "02" })).toBe(
    "predict-jra-2",
  );
});

test("honors a custom RACE_SHARD_MAX_CONCURRENT to widen the shard modulus", () => {
  const env = makeEnv({ RACE_SHARDED_DO: "1", RACE_SHARD_MAX_CONCURRENT: "5" });
  expect(resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "01" })).toBe(
    "predict-jra-2",
  );
  expect(resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "02" })).toBe(
    "predict-jra-1",
  );
});

test("collapses every race onto a single shard when RACE_SHARD_MAX_CONCURRENT is 1", () => {
  const env = makeEnv({ RACE_SHARDED_DO: "1", RACE_SHARD_MAX_CONCURRENT: "1" });
  expect(resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "01" })).toBe(
    "predict-jra-0",
  );
  expect(resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "02" })).toBe(
    "predict-jra-0",
  );
});

test("falls back to the default modulus when RACE_SHARD_MAX_CONCURRENT is zero", () => {
  const env = makeEnv({ RACE_SHARDED_DO: "1", RACE_SHARD_MAX_CONCURRENT: "0" });
  expect(resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "01" })).toBe(
    "predict-jra-0",
  );
});

test("falls back to the default modulus when RACE_SHARD_MAX_CONCURRENT is negative", () => {
  const env = makeEnv({ RACE_SHARDED_DO: "1", RACE_SHARD_MAX_CONCURRENT: "-1" });
  expect(resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "01" })).toBe(
    "predict-jra-0",
  );
});

test("falls back to the default modulus when RACE_SHARD_MAX_CONCURRENT is not an integer", () => {
  const env = makeEnv({ RACE_SHARDED_DO: "1", RACE_SHARD_MAX_CONCURRENT: "2.5" });
  expect(resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "01" })).toBe(
    "predict-jra-0",
  );
});

test("falls back to the default modulus when RACE_SHARD_MAX_CONCURRENT is not numeric", () => {
  const env = makeEnv({ RACE_SHARDED_DO: "1", RACE_SHARD_MAX_CONCURRENT: "abc" });
  expect(resolvePredictDoName({ category: "jra", env, keibajoCode: "05", raceBango: "01" })).toBe(
    "predict-jra-0",
  );
});

test("shards ban-ei races independently of jra using the same category prefix scheme", () => {
  const env = makeEnv({ RACE_SHARDED_DO: "1" });
  expect(
    resolvePredictDoName({ category: "ban-ei", env, keibajoCode: "44", raceBango: "01" }),
  ).toBe("predict-ban-ei-2");
});

test("shareCategoryInstance keeps a JRA rescore on the unsharded category DO when sharding is on", () => {
  expect(
    resolvePredictDoName({
      category: "jra",
      env: makeEnv({ RACE_SHARDED_DO: "1" }),
      keibajoCode: "05",
      raceBango: "02",
      shareCategoryInstance: true,
    }),
  ).toBe("predict-jra");
});

test("listDayBasePickupDoNames is the unsharded category DO when sharding is off", () => {
  expect(listDayBasePickupDoNames({ category: "ban-ei", env: makeEnv() })).toStrictEqual([
    "predict-ban-ei",
  ]);
});

test("listDayBasePickupDoNames only probes the day-base owner when race sharding is on", () => {
  expect(
    listDayBasePickupDoNames({ category: "ban-ei", env: makeEnv({ RACE_SHARDED_DO: "1" }) }),
  ).toStrictEqual(["predict-ban-ei"]);
});

test("shareCategoryInstance keeps a NAR rescore on the unsharded category DO when sharding is on", () => {
  expect(
    resolvePredictDoName({
      category: "nar",
      env: makeEnv({ RACE_SHARDED_DO: "1" }),
      keibajoCode: "44",
      raceBango: "01",
      shareCategoryInstance: true,
    }),
  ).toBe("predict-nar");
});

test("shareCategoryInstance keeps a Ban-ei rescore on the unsharded category DO when sharding is on", () => {
  expect(
    resolvePredictDoName({
      category: "ban-ei",
      env: makeEnv({ RACE_SHARDED_DO: "1" }),
      keibajoCode: "83",
      raceBango: "01",
      shareCategoryInstance: true,
    }),
  ).toBe("predict-ban-ei");
});

test("focused-full without shareCategoryInstance still shards when RACE_SHARDED_DO is on", () => {
  expect(
    resolvePredictDoName({
      category: "jra",
      env: makeEnv({ RACE_SHARDED_DO: "1" }),
      keibajoCode: "05",
      raceBango: "02",
      shareCategoryInstance: false,
    }),
  ).toBe("predict-jra-2");
});
