import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  buildWeatherUrl,
  fetchVenueWeather,
  fetchWithCache,
  isArchiveDate,
  parseWeatherResponse,
} from "./weather-api";

const TOKYO_VENUE = { name: "東京", lat: 35.6622, lon: 139.4856 };

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("isArchiveDate", () => {
  it("returns true for a date 10 days ago", () => {
    const past = new Date();
    past.setUTCDate(past.getUTCDate() - 10);
    const dateStr = past.toISOString().slice(0, 10);
    expect(isArchiveDate(dateStr)).toBe(true);
  });

  it("returns false for today", () => {
    const today = new Date().toISOString().slice(0, 10);
    expect(isArchiveDate(today)).toBe(false);
  });

  it("returns false for a date 2 days ago (within lag)", () => {
    const recent = new Date();
    recent.setUTCDate(recent.getUTCDate() - 2);
    const dateStr = recent.toISOString().slice(0, 10);
    expect(isArchiveDate(dateStr)).toBe(false);
  });
});

describe("buildWeatherUrl", () => {
  it("uses forecast URL for forecast type", () => {
    const url = buildWeatherUrl({
      venue: TOKYO_VENUE,
      raceDate: "2026-06-22",
      weatherType: "forecast",
    });
    expect(url.startsWith("https://api.open-meteo.com/v1/forecast")).toBe(true);
  });

  it("uses forecast URL for actual type on recent date", () => {
    const today = new Date().toISOString().slice(0, 10);
    const url = buildWeatherUrl({
      venue: TOKYO_VENUE,
      raceDate: today,
      weatherType: "actual",
    });
    expect(url.startsWith("https://api.open-meteo.com/v1/forecast")).toBe(true);
  });

  it("uses archive URL for actual type on old date", () => {
    const url = buildWeatherUrl({
      venue: TOKYO_VENUE,
      raceDate: "2025-01-01",
      weatherType: "actual",
    });
    expect(url.startsWith("https://archive-api.open-meteo.com/v1/archive")).toBe(true);
  });

  it("includes correct query params", () => {
    const url = buildWeatherUrl({
      venue: TOKYO_VENUE,
      raceDate: "2026-06-22",
      weatherType: "forecast",
    });
    expect(url).toContain("latitude=35.6622");
    expect(url).toContain("longitude=139.4856");
    expect(url).toContain("timezone=Asia%2FTokyo");
    expect(url).toContain("start_date=2026-06-22");
    expect(url).toContain("end_date=2026-06-22");
    expect(url).toContain(
      "hourly=weather_code%2Ctemperature_2m%2Cprecipitation%2Cwind_speed_10m%2Cwind_gusts_10m",
    );
  });
});

describe("fetchWithCache", () => {
  it("returns cached text on cache hit", async () => {
    const cachedResponse = new Response("cached-data");
    const mockMatch = vi.fn().mockResolvedValue(cachedResponse);
    const mockPut = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal("caches", {
      default: { match: mockMatch, put: mockPut },
    });

    const result = await fetchWithCache("https://example.com/data", 1800);
    expect(result).toBe("cached-data");
    expect(mockMatch).toHaveBeenCalledTimes(1);
  });

  it("fetches and stores on cache miss", async () => {
    const mockMatch = vi.fn().mockResolvedValue(undefined);
    const mockPut = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal("caches", {
      default: { match: mockMatch, put: mockPut },
    });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("fresh-data", { status: 200 })));

    const result = await fetchWithCache("https://example.com/data", 1800);
    expect(result).toBe("fresh-data");
    expect(mockPut).toHaveBeenCalledTimes(1);
  });

  it("falls through to fetch when caches is undefined", async () => {
    vi.stubGlobal("caches", undefined);
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response("no-cache-data", { status: 200 })),
    );

    const result = await fetchWithCache("https://example.com/data", 1800);
    expect(result).toBe("no-cache-data");
  });

  it("throws on HTTP error", async () => {
    vi.stubGlobal("caches", undefined);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("not found", { status: 404 })));

    await expect(fetchWithCache("https://example.com/missing", 1800)).rejects.toThrow(
      "HTTP 404: https://example.com/missing",
    );
  });

  it("stores with correct Cache-Control header on cache miss", async () => {
    const mockMatch = vi.fn().mockResolvedValue(undefined);
    const mockPut = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal("caches", {
      default: { match: mockMatch, put: mockPut },
    });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("data", { status: 200 })));

    await fetchWithCache("https://example.com/data", 3600);
    const putCall = mockPut.mock.calls[0]!;
    const storedResponse = putCall[1] as Response;
    expect(storedResponse.headers.get("Cache-Control")).toBe("public, max-age=3600");
  });

  it("handles caches.default being null", async () => {
    vi.stubGlobal("caches", { default: null });
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response("null-cache-data", { status: 200 })),
    );

    const result = await fetchWithCache("https://example.com/data", 1800);
    expect(result).toBe("null-cache-data");
  });
});

describe("parseWeatherResponse", () => {
  it("parses a valid response with all fields", () => {
    const raw = JSON.stringify({
      hourly: {
        time: ["2026-06-22T00:00", "2026-06-22T01:00"],
        weather_code: [3, null],
        temperature_2m: [20.5, 19.8],
        precipitation: [0.0, 0.1],
        wind_speed_10m: [5.2, 4.8],
        wind_gusts_10m: [8.1, 7.5],
      },
    });

    const rows = parseWeatherResponse(raw);
    expect(rows).toHaveLength(2);
    expect(rows[0]).toStrictEqual({
      date: "2026-06-22",
      hour: 0,
      weatherCode: 3,
      temperature: 20.5,
      precipitation: 0.0,
      windSpeed: 5.2,
      windGusts: 8.1,
    });
    expect(rows[1]).toStrictEqual({
      date: "2026-06-22",
      hour: 1,
      weatherCode: null,
      temperature: 19.8,
      precipitation: 0.1,
      windSpeed: 4.8,
      windGusts: 7.5,
    });
  });

  it("returns empty array when hourly is missing", () => {
    const raw = JSON.stringify({});
    expect(parseWeatherResponse(raw)).toStrictEqual([]);
  });

  it("returns empty array when hourly.time is missing", () => {
    const raw = JSON.stringify({ hourly: {} });
    expect(parseWeatherResponse(raw)).toStrictEqual([]);
  });

  it("handles null values in weather fields", () => {
    const raw = JSON.stringify({
      hourly: {
        time: ["2026-06-22T12:00"],
        weather_code: [null],
        temperature_2m: [null],
        precipitation: [null],
        wind_speed_10m: [null],
        wind_gusts_10m: [null],
      },
    });

    const rows = parseWeatherResponse(raw);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toStrictEqual({
      date: "2026-06-22",
      hour: 12,
      weatherCode: null,
      temperature: null,
      precipitation: null,
      windSpeed: null,
      windGusts: null,
    });
  });
});

describe("fetchVenueWeather", () => {
  beforeEach(() => {
    vi.stubGlobal("caches", undefined);
  });

  it("calls fetch with the correct URL and returns parsed rows for forecast", async () => {
    const mockResponseBody = JSON.stringify({
      hourly: {
        time: ["2026-06-22T10:00"],
        weather_code: [1],
        temperature_2m: [22.0],
        precipitation: [0.0],
        wind_speed_10m: [3.0],
        wind_gusts_10m: [5.0],
      },
    });
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response(mockResponseBody, { status: 200 })),
    );

    const rows = await fetchVenueWeather({
      venue: TOKYO_VENUE,
      raceDate: "2026-06-22",
      weatherType: "forecast",
    });

    expect(rows).toHaveLength(1);
    expect(rows[0]).toStrictEqual({
      date: "2026-06-22",
      hour: 10,
      weatherCode: 1,
      temperature: 22.0,
      precipitation: 0.0,
      windSpeed: 3.0,
      windGusts: 5.0,
    });
  });

  it("calls fetch with archive URL for old actual dates", async () => {
    const mockFetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          hourly: {
            time: [],
            weather_code: [],
            temperature_2m: [],
            precipitation: [],
            wind_speed_10m: [],
            wind_gusts_10m: [],
          },
        }),
        { status: 200 },
      ),
    );
    vi.stubGlobal("fetch", mockFetch);

    await fetchVenueWeather({
      venue: TOKYO_VENUE,
      raceDate: "2025-01-01",
      weatherType: "actual",
    });

    const calledUrl = mockFetch.mock.calls[0]![0] as string;
    expect(calledUrl.startsWith("https://archive-api.open-meteo.com")).toBe(true);
  });
});
