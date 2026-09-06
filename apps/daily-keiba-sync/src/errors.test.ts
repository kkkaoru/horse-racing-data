import { describe, expect, test } from "vitest";
import {
  isPermanentJobError,
  isTransientJobError,
  permanentFailure,
  PermanentJobError,
  transientFailure,
  TransientJobError,
} from "./errors";

describe("safe job failures", () => {
  test("preserves an already-classified permanent failure", () => {
    const error = new PermanentJobError("catalog-schema");
    expect(permanentFailure("other", error)).toBe(error);
    expect(isPermanentJobError(error)).toBe(true);
    expect(isPermanentJobError(new Error("temporary"))).toBe(false);
  });

  test("classifies retryable failures without making them permanent", () => {
    const error = new TransientJobError("catalog-transaction");
    expect(transientFailure("other", error)).toBe(error);
    expect(isTransientJobError(error)).toBe(true);
    expect(isTransientJobError(new Error("unclassified"))).toBe(false);
    expect(isPermanentJobError(error)).toBe(false);
  });

  test("wraps an unsafe cause without exposing it in the public message", () => {
    const error = permanentFailure("catalog-not-found", new Error("sensitive detail"));
    expect(error.message).toBe("Permanent sync failure at catalog-not-found");
    expect(error.safeStage).toBe("catalog-not-found");
    const transient = transientFailure("catalog-append-stage", new Error("sensitive detail"));
    expect(transient.message).toBe("Transient sync failure at catalog-append-stage");
    expect(transient.safeStage).toBe("catalog-append-stage");
  });
});
