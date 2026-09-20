// Run with bun.
import { expect, it, vi } from "vitest";

import {
  MATCHED_PROFILE_FLAGS,
  readCatalogRaceMatchedProfile,
} from "./race-matched-profile-catalog";
import type {
  CatalogRaceMatchedProfileBinding,
  CatalogRaceMatchedProfileQuery,
} from "./race-matched-profile-catalog";

const query: CatalogRaceMatchedProfileQuery = {
  source: "jra",
  date: "20260920",
  keibajoCode: "05",
  raceBango: "08",
  kyori: "1600",
  kyosoShubetsuCode: "01",
  kyosoJokenCode: "999",
  kyosoJokenMeisho: "オープン",
  trackCode: "10",
  gradeCode: "A",
  kyosomeiHondai: "テストステークス",
  years: "3",
  limit: 5000,
  flags: ["includeVenue", "includeDistance"],
  runnerCount: 16,
};

const bindingFor = (
  body: unknown,
  init: ResponseInit = {},
): { binding: CatalogRaceMatchedProfileBinding; fetch: ReturnType<typeof vi.fn> } => {
  const fetch = vi
    .fn<CatalogRaceMatchedProfileBinding["fetch"]>()
    .mockResolvedValue(
      Response.json(body, { ...init, headers: init.headers ?? { "cache-control": "no-store" } }),
    );
  return { binding: { fetch }, fetch };
};

const profile = {
  targetRaceTime: 940,
  targetLast3f: 340,
  targetBodyWeight: 480,
  targetCarriedWeight: 550,
  targetMargin: 5,
};

it("reads the target profile and sends every parameter", async () => {
  const { binding, fetch } = bindingFor({ profile });
  await expect(readCatalogRaceMatchedProfile(binding, query)).resolves.toStrictEqual(profile);
  const url: URL = new URL(fetch.mock.calls[0]?.[0]?.url ?? "");
  expect(url.pathname).toBe("/v1/race-matched-profile");
  expect(url.searchParams.get("source")).toBe("jra");
  expect(url.searchParams.get("runnerCount")).toBe("16");
  expect(url.searchParams.get("limit")).toBe("5000");
  expect(url.searchParams.get("includeVenue")).toBe("1");
  expect(url.searchParams.get("includeDistance")).toBe("1");
  // Unrequested flags are sent explicitly as 0 so the SQL stays deterministic.
  expect(url.searchParams.get("includeAge")).toBe("0");
  expect(url.searchParams.get("includeRunnerCount")).toBe("0");
  expect(fetch.mock.calls[0]?.[0]?.redirect).toBe("manual");
});

it("accepts null targets and omits an absent runner count", async () => {
  const { binding, fetch } = bindingFor({
    profile: { ...profile, targetRaceTime: null, targetMargin: "5" },
  });
  await expect(
    readCatalogRaceMatchedProfile(binding, { ...query, runnerCount: null }),
  ).resolves.toStrictEqual({ ...profile, targetRaceTime: null, targetMargin: 5 });
  expect(new URL(fetch.mock.calls[0]?.[0]?.url ?? "").searchParams.has("runnerCount")).toBe(false);
});

it("fails closed on an unusable binding or query", async () => {
  await expect(readCatalogRaceMatchedProfile(undefined, query)).rejects.toThrow(
    "Catalog matched profile unavailable",
  );
  const { binding } = bindingFor({ profile });
  await expect(readCatalogRaceMatchedProfile(binding, { ...query, gradeCode: "" })).rejects.toThrow(
    "Catalog matched profile unavailable",
  );
  await expect(readCatalogRaceMatchedProfile(binding, { ...query, limit: 0 })).rejects.toThrow(
    "Catalog matched profile unavailable",
  );
  await expect(
    readCatalogRaceMatchedProfile(binding, { ...query, flags: ["includeNope"] }),
  ).rejects.toThrow("Catalog matched profile unavailable");
  await expect(
    readCatalogRaceMatchedProfile(binding, { ...query, runnerCount: -1 }),
  ).rejects.toThrow("Catalog matched profile unavailable");
});

it.each([
  { body: { profile }, init: { status: 503 } },
  { body: { profile }, init: { status: 200, headers: {} } },
  { body: { profile: { ...profile, extra: 1 } } },
  { body: { profile: { ...profile, targetMargin: "abc" } } },
  { body: {} },
  { body: { profile: null } },
])("rejects a malformed catalog payload %j", async ({ body, init }) => {
  await expect(
    readCatalogRaceMatchedProfile(bindingFor(body, init ?? {}).binding, query),
  ).rejects.toThrow("Catalog matched profile unavailable");
});

it("exposes exactly the ten supported flags", () => {
  expect(MATCHED_PROFILE_FLAGS).toHaveLength(10);
});
