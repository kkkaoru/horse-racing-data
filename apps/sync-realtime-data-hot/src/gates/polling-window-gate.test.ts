// Run with bun.
import { expect, it } from "vitest";

import { shouldRunOddsCron } from "./polling-window-gate";

it("returns true so odds polling runs 24/7", () => {
  expect(shouldRunOddsCron()).toBe(true);
});
