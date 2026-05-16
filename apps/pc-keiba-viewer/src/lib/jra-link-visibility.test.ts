import { describe, expect, it } from "vitest";

import { isJraResultLinkAvailable } from "./jra-link-visibility";

describe("jra link visibility", () => {
  it("hides the result link until five race-date days have elapsed", () => {
    expect(
      isJraResultLinkAvailable("2026", "05", "09", Date.parse("2026-05-13T23:59:59+09:00")),
    ).toBe(false);
  });

  it("shows the result link from five days after the race date", () => {
    expect(
      isJraResultLinkAvailable("2026", "05", "09", Date.parse("2026-05-14T00:00:00+09:00")),
    ).toBe(true);
  });
});
