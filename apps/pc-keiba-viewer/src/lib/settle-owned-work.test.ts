// Run with bun (vitest).
import { expect, it, vi } from "vitest";

import { settleOwnedWork } from "./settle-owned-work";
it("preserves ordered results", async () => {
  expect(await settleOwnedWork([Promise.resolve(1), "two"])).toStrictEqual([1, "two"]);
});
it("retains the first rejection until every publishing branch settles", async () => {
  const branch = Promise.withResolvers<void>();
  const returned = vi.fn<(value: unknown) => void>();
  const work = settleOwnedWork([Promise.reject(new Error("first")), branch.promise]).catch(
    returned,
  );
  await Promise.resolve();
  await Promise.resolve();
  expect(returned).not.toHaveBeenCalled();
  branch.reject(new Error("later"));
  await work;
  expect(returned).toHaveBeenCalledWith(new Error("first"));
});
