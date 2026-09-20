import { describe, expect, it, vi } from "vitest";

import { loadOptionalRaceDetailData } from "./race-detail-optional-data";

describe("loadOptionalRaceDetailData", () => {
  it("returns the loaded value on success", async () => {
    const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
    await expect(
      loadOptionalRaceDetailData(async () => ["race"], [], { data: "same_venue_races" }),
    ).resolves.toStrictEqual(["race"]);
    expect(log).not.toHaveBeenCalled();
    log.mockRestore();
  });

  it("falls back and logs when the optional read fails", async () => {
    const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
    await expect(
      loadOptionalRaceDetailData(
        async () => {
          throw new Error("optional provider detail");
        },
        [],
        { data: "same_venue_races", date: "20260920" },
      ),
    ).resolves.toStrictEqual([]);
    expect(log).toHaveBeenCalledWith(
      '{"event":"race_detail_optional_data_failed","data":"same_venue_races","date":"20260920"}',
    );
    expect(JSON.stringify(log.mock.calls)).not.toContain("optional provider detail");
    log.mockRestore();
  });
});
