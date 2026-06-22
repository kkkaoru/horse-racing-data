import { expect, it, vi } from "vitest";
import { upsertVenueWeather } from "./weather-d1";

const TOKYO_VENUE = { name: "東京", lat: 35.6622, lon: 139.4856 };

it("returns 0 for empty rows without calling db", async () => {
  const mockBind = vi.fn().mockReturnThis();
  const mockPrepare = vi.fn(() => ({ bind: mockBind }));
  const mockBatch = vi.fn().mockResolvedValue([]);
  const mockDb = {
    prepare: mockPrepare,
    batch: mockBatch,
  } as unknown as D1Database;

  const result = await upsertVenueWeather({
    db: mockDb,
    keibajoCode: "05",
    raceDate: "2026-06-22",
    weatherType: "forecast",
    venue: TOKYO_VENUE,
    rows: [],
    fetchedAt: "2026-06-22T10:00:00.000Z",
  });

  expect(result).toBe(0);
  expect(mockPrepare).not.toHaveBeenCalled();
  expect(mockBatch).not.toHaveBeenCalled();
});

it("calls prepare and bind for each row and returns row count", async () => {
  const mockBind = vi.fn().mockReturnThis();
  const mockPrepare = vi.fn(() => ({ bind: mockBind }));
  const mockBatch = vi.fn().mockResolvedValue([]);
  const mockDb = {
    prepare: mockPrepare,
    batch: mockBatch,
  } as unknown as D1Database;

  const rows = [
    {
      date: "2026-06-22",
      hour: 10,
      weatherCode: 3,
      temperature: 20.5,
      precipitation: 0.0,
      windSpeed: 5.2,
      windGusts: 8.1,
    },
    {
      date: "2026-06-22",
      hour: 11,
      weatherCode: null,
      temperature: 21.0,
      precipitation: 0.2,
      windSpeed: 4.0,
      windGusts: 6.0,
    },
  ];

  const result = await upsertVenueWeather({
    db: mockDb,
    keibajoCode: "05",
    raceDate: "2026-06-22",
    weatherType: "forecast",
    venue: TOKYO_VENUE,
    rows,
    fetchedAt: "2026-06-22T10:00:00.000Z",
  });

  expect(result).toBe(2);
  expect(mockPrepare).toHaveBeenCalledTimes(2);
  expect(mockBatch).toHaveBeenCalledTimes(1);
});

it("passes correct bind arguments for a single row", async () => {
  const mockBind = vi.fn().mockReturnThis();
  const mockPrepare = vi.fn(() => ({ bind: mockBind }));
  const mockBatch = vi.fn().mockResolvedValue([]);
  const mockDb = {
    prepare: mockPrepare,
    batch: mockBatch,
  } as unknown as D1Database;

  const rows = [
    {
      date: "2026-06-22",
      hour: 8,
      weatherCode: 1,
      temperature: 18.5,
      precipitation: 0.0,
      windSpeed: 3.5,
      windGusts: 5.0,
    },
  ];

  await upsertVenueWeather({
    db: mockDb,
    keibajoCode: "05",
    raceDate: "2026-06-22",
    weatherType: "actual",
    venue: TOKYO_VENUE,
    rows,
    fetchedAt: "2026-06-22T09:00:00.000Z",
  });

  expect(mockBind).toHaveBeenCalledWith(
    "05",
    "2026-06-22",
    8,
    "actual",
    "東京",
    35.6622,
    139.4856,
    1,
    18.5,
    0.0,
    3.5,
    5.0,
    "2026-06-22T09:00:00.000Z",
  );
});

it("calls batch with array of prepared statements", async () => {
  const statement1 = { bind: vi.fn().mockReturnThis() };
  const statement2 = { bind: vi.fn().mockReturnThis() };
  let callCount = 0;
  const mockPrepare = vi.fn(() => {
    callCount += 1;
    return callCount === 1 ? statement1 : statement2;
  });
  const mockBatch = vi.fn().mockResolvedValue([]);
  const mockDb = {
    prepare: mockPrepare,
    batch: mockBatch,
  } as unknown as D1Database;

  await upsertVenueWeather({
    db: mockDb,
    keibajoCode: "05",
    raceDate: "2026-06-22",
    weatherType: "forecast",
    venue: TOKYO_VENUE,
    rows: [
      {
        date: "2026-06-22",
        hour: 10,
        weatherCode: 0,
        temperature: 20.0,
        precipitation: 0.0,
        windSpeed: 2.0,
        windGusts: 3.0,
      },
      {
        date: "2026-06-22",
        hour: 11,
        weatherCode: 1,
        temperature: 21.0,
        precipitation: 0.0,
        windSpeed: 2.5,
        windGusts: 4.0,
      },
    ],
    fetchedAt: "2026-06-22T10:00:00.000Z",
  });

  expect(mockBatch).toHaveBeenCalledWith([statement1, statement2]);
});
