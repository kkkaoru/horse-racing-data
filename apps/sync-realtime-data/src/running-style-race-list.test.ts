// run with: bun run test
import { beforeEach, expect, it, vi } from "vitest";
import type { Env } from "./types";

vi.mock("./running-style-catalog-client", () => ({
  fetchRunningStyleRaceKeysFromCatalog: vi.fn(),
}));

const makeEnv = (): Env =>
  Object.assign(JSON.parse("{}"), {
    PC_KEIBA_R2_CATALOG: { fetch: vi.fn() },
  });

beforeEach(() => {
  vi.clearAllMocks();
});

it("listRunningStyleRacesByDate returns only Catalog race keys", async () => {
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { fetchRunningStyleRaceKeysFromCatalog } = await import("./running-style-catalog-client");
  vi.mocked(fetchRunningStyleRaceKeysFromCatalog).mockResolvedValue([
    {
      kaisai_nen: "2026",
      kaisai_tsukihi: "0512",
      keibajo_code: "08",
      race_bango: "01",
      source: "jra",
    },
  ]);
  const env = makeEnv();
  await expect(listRunningStyleRacesByDate(env, "20260512")).resolves.toStrictEqual({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
    ],
    source: "catalog",
  });
  expect(fetchRunningStyleRaceKeysFromCatalog).toHaveBeenCalledWith(
    env.PC_KEIBA_R2_CATALOG,
    "20260512",
  );
});

it("listRunningStyleRacesByDate returns an authoritative empty Catalog result", async () => {
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { fetchRunningStyleRaceKeysFromCatalog } = await import("./running-style-catalog-client");
  vi.mocked(fetchRunningStyleRaceKeysFromCatalog).mockResolvedValue([]);
  await expect(listRunningStyleRacesByDate(makeEnv(), "20260512")).resolves.toStrictEqual({
    races: [],
    source: "catalog",
  });
});

it("listRunningStyleRacesByDate propagates a Catalog failure", async () => {
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { fetchRunningStyleRaceKeysFromCatalog } = await import("./running-style-catalog-client");
  vi.mocked(fetchRunningStyleRaceKeysFromCatalog).mockRejectedValue(
    new Error("catalog unavailable"),
  );
  await expect(listRunningStyleRacesByDate(makeEnv(), "20260512")).rejects.toThrow(
    "catalog unavailable",
  );
});
