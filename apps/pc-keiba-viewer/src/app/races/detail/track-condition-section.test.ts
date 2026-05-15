import { describe, expect, it } from "vitest";

import { getTrackConditionSurface } from "./track-condition-section";

describe("getTrackConditionSurface", () => {
  it("uses turf condition for turf races", () => {
    expect(getTrackConditionSurface("17")).toBe("turf");
    expect(getTrackConditionSurface("51")).toBe("turf");
  });

  it("uses dirt condition for dirt races", () => {
    expect(getTrackConditionSurface("24")).toBe("dirt");
    expect(getTrackConditionSurface("29")).toBe("dirt");
  });

  it("keeps both surfaces for mixed or unknown track codes", () => {
    expect(getTrackConditionSurface("52")).toBe("both");
    expect(getTrackConditionSurface(null)).toBe("both");
  });
});
