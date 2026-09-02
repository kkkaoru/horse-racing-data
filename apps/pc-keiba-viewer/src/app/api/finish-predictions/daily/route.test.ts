// Run with bun (bunx vitest).
import { beforeEach, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  getDailyFinishPredictions: vi.fn<() => Promise<unknown>>(),
}));

vi.mock("../../../../lib/daily-finish-predictions.server", () => mocks);

import { GET } from "./route";

beforeEach(() => {
  vi.clearAllMocks();
});

it("returns a no-store daily JRA prediction response", async () => {
  mocks.getDailyFinishPredictions.mockResolvedValue({
    availableRaceCount: 1,
    date: "2026-05-24",
    raceCount: 1,
    races: [{ raceId: "jra:2026:0524:05:11" }],
    source: "jra",
    unavailableRaceIds: [],
  });

  const response = await GET(
    new Request(
      "https://viewer.example.test/api/finish-predictions/daily?year=2026&month=05&day=24&source=jra",
    ),
  );

  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("private, max-age=0, no-store");
  expect(await response.json()).toStrictEqual({
    availableRaceCount: 1,
    date: "2026-05-24",
    raceCount: 1,
    races: [{ raceId: "jra:2026:0524:05:11" }],
    source: "jra",
    unavailableRaceIds: [],
  });
  expect(mocks.getDailyFinishPredictions).toHaveBeenCalledWith({
    day: "24",
    month: "05",
    source: "jra",
    year: "2026",
  });
});

it("forwards an optional race scope to the daily prediction loader", async () => {
  mocks.getDailyFinishPredictions.mockResolvedValue({
    availableRaceCount: 1,
    date: "2026-05-24",
    raceCount: 1,
    races: [{ raceId: "jra:2026:0524:05:11" }],
    source: "jra",
    unavailableRaceIds: [],
  });

  const response = await GET(
    new Request(
      "https://viewer.example.test/api/finish-predictions/daily?year=2026&month=05&day=24&source=jra&keibajoCode=05&raceNumber=11",
    ),
  );

  expect(response.status).toBe(200);
  expect(mocks.getDailyFinishPredictions).toHaveBeenCalledWith({
    day: "24",
    month: "05",
    race: { keibajoCode: "05", raceNumber: "11" },
    source: "jra",
    year: "2026",
  });
});

it("rejects a partial race scope", async () => {
  const missingRaceNumber = await GET(
    new Request(
      "https://viewer.example.test/api/finish-predictions/daily?year=2026&month=05&day=24&source=jra&keibajoCode=05",
    ),
  );
  const missingVenue = await GET(
    new Request(
      "https://viewer.example.test/api/finish-predictions/daily?year=2026&month=05&day=24&source=jra&raceNumber=11",
    ),
  );

  expect(missingRaceNumber.status).toBe(400);
  expect(missingVenue.status).toBe(400);
  expect(mocks.getDailyFinishPredictions).not.toHaveBeenCalled();
});

it("rejects malformed race scope values", async () => {
  const invalidVenue = await GET(
    new Request(
      "https://viewer.example.test/api/finish-predictions/daily?year=2026&month=05&day=24&source=jra&keibajoCode=5&raceNumber=11",
    ),
  );
  const invalidRaceNumber = await GET(
    new Request(
      "https://viewer.example.test/api/finish-predictions/daily?year=2026&month=05&day=24&source=jra&keibajoCode=05&raceNumber=1",
    ),
  );

  expect(invalidVenue.status).toBe(400);
  expect(invalidRaceNumber.status).toBe(400);
  expect(mocks.getDailyFinishPredictions).not.toHaveBeenCalled();
});

it("rejects malformed or missing query parameters", async () => {
  const invalidYear = await GET(
    new Request(
      "https://viewer.example.test/api/finish-predictions/daily?year=26&month=05&day=24&source=jra",
    ),
  );
  const invalidMonth = await GET(
    new Request(
      "https://viewer.example.test/api/finish-predictions/daily?year=2026&month=5&day=24&source=jra",
    ),
  );
  const invalidDay = await GET(
    new Request(
      "https://viewer.example.test/api/finish-predictions/daily?year=2026&month=05&day=2&source=jra",
    ),
  );
  const invalidSource = await GET(
    new Request(
      "https://viewer.example.test/api/finish-predictions/daily?year=2026&month=05&day=24&source=overseas",
    ),
  );
  const missing = await GET(
    new Request("https://viewer.example.test/api/finish-predictions/daily"),
  );

  expect(invalidYear.status).toBe(400);
  expect(invalidMonth.status).toBe(400);
  expect(invalidDay.status).toBe(400);
  expect(invalidSource.status).toBe(400);
  expect(missing.status).toBe(400);
  expect(await missing.json()).toStrictEqual({
    error:
      "year, month, day, and source=jra|nar are required; optional keibajoCode and raceNumber must be supplied together in two-character format",
  });
  expect(mocks.getDailyFinishPredictions).not.toHaveBeenCalled();
});
