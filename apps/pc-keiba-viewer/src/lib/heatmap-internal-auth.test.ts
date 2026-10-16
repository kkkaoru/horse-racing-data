// Run with bun (vitest).
import { expect, it } from "vitest";

import { isHeatmapInternalRequest } from "./heatmap-internal-auth";
it.each([undefined, "", "wrong"])("rejects missing/incorrect configured token %s", (token) => {
  expect(
    isHeatmapInternalRequest(
      new Request("https://local/", { headers: { "x-pc-keiba-internal-token": "secret" } }),
      token,
    ),
  ).toBe(false);
});
it("requires the secret header and accepts an exact token", () => {
  expect(isHeatmapInternalRequest(new Request("https://local/"), "secret")).toBe(false);
  expect(
    isHeatmapInternalRequest(
      new Request("https://local/", { headers: { "x-pc-keiba-internal-token": "secret" } }),
      "secret",
    ),
  ).toBe(true);
});
