import { expect, it } from "vitest";

import {
  encodedRaceTimeTenthsSql,
  parseEncodedRaceTimeSeconds,
  parseEncodedRaceTimeTenths,
} from "./race-time";

it("decodes PC-KEIBA MSSD race times", () => {
  expect(parseEncodedRaceTimeTenths("1008")).toBe(608);
  expect(parseEncodedRaceTimeTenths(595)).toBe(595);
  expect(parseEncodedRaceTimeTenths(3188n)).toBe(1988);
  expect(parseEncodedRaceTimeSeconds("1008")).toBe(60.8);
});

it("rejects absent and invalid PC-KEIBA race times", () => {
  expect(parseEncodedRaceTimeTenths(null)).toBeNull();
  expect(parseEncodedRaceTimeTenths("0000")).toBeNull();
  expect(parseEncodedRaceTimeTenths("9999")).toBeNull();
  expect(parseEncodedRaceTimeTenths("invalid")).toBeNull();
  expect(parseEncodedRaceTimeSeconds(undefined)).toBeNull();
});

it("builds a positional race-time SQL expression", () => {
  expect(encodedRaceTimeTenthsSql("try_cast(se.soha_time AS DOUBLE)")).toBe(
    "(CASE\n    WHEN try_cast(se.soha_time AS DOUBLE) IS NULL\n      OR try_cast(se.soha_time AS DOUBLE) <= 0\n      OR floor(try_cast(se.soha_time AS DOUBLE) / 10) % 100 >= 60\n    THEN NULL\n    ELSE floor(try_cast(se.soha_time AS DOUBLE) / 1000) * 600\n      + floor(try_cast(se.soha_time AS DOUBLE) / 10) % 100 * 10\n      + try_cast(se.soha_time AS DOUBLE) % 10\n  END)",
  );
});
