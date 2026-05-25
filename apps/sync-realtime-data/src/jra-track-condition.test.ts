// run with: bun run test
import { expect, it } from "vitest";
import { fetchJraTrackConditionWithPlaywright } from "./jra-track-condition";

it("fetchJraTrackConditionWithPlaywright throws when browserBinding is undefined", async () => {
  await expect(
    fetchJraTrackConditionWithPlaywright(undefined, {
      kaisaiNen: "2026",
      keibajoCode: "08",
    }),
  ).rejects.toThrow("JRA_BROWSER binding is required");
});
