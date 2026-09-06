import { expect, test } from "vitest";
import { assertLegacyReplicaWriteEnabled } from "./legacy-replica-authority";

test("rejects local Catalog writes by default", () => {
  expect(() => assertLegacyReplicaWriteEnabled(undefined)).toThrow(
    "daily-keiba-sync Worker is the production authority",
  );
});

test("allows an explicit break-glass fallback", () => {
  expect(assertLegacyReplicaWriteEnabled("break-glass")).toBeUndefined();
});
