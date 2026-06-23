import { beforeEach, expect, it, vi } from "vitest";
import { handleWeatherBatch, processWeatherJob } from "./weather-queue";

vi.mock("./weather-api", () => ({
  fetchVenueWeather: vi.fn().mockResolvedValue([
    {
      date: "2026-06-22",
      hour: 10,
      weatherCode: 1,
      temperature: 20.0,
      precipitation: 0.0,
      windSpeed: 3.0,
      windGusts: 5.0,
    },
  ]),
}));

vi.mock("./weather-d1", () => ({
  upsertVenueWeather: vi.fn().mockResolvedValue(1),
}));

import { fetchVenueWeather } from "./weather-api";
import { upsertVenueWeather } from "./weather-d1";

const mockDb = {} as unknown as D1Database;
const mockSendBatch = vi.fn().mockResolvedValue(undefined);
const mockEnv = {
  WEATHER_DB: mockDb,
  WEATHER_JOBS: { sendBatch: mockSendBatch },
} as unknown as import("./types").Env;

beforeEach(() => {
  vi.clearAllMocks();
});

it("processWeatherJob skips unknown keibajo_code", async () => {
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

  await processWeatherJob({ type: "forecast", keibajoCode: "99", raceDate: "2026-06-22" }, mockEnv);

  expect(consoleSpy).toHaveBeenCalledWith("Unknown keibajo_code: 99");
  expect(fetchVenueWeather).not.toHaveBeenCalled();
  expect(upsertVenueWeather).not.toHaveBeenCalled();
});

it("processWeatherJob calls fetchVenueWeather and upsertVenueWeather for known venue", async () => {
  await processWeatherJob({ type: "forecast", keibajoCode: "05", raceDate: "2026-06-22" }, mockEnv);

  expect(fetchVenueWeather).toHaveBeenCalledTimes(1);
  expect(fetchVenueWeather).toHaveBeenCalledWith({
    venue: { name: "東京", lat: 35.6622, lon: 139.4856 },
    raceDate: "2026-06-22",
    weatherType: "forecast",
  });
  expect(upsertVenueWeather).toHaveBeenCalledTimes(1);
});

it("processWeatherJob passes correct params to upsertVenueWeather", async () => {
  await processWeatherJob({ type: "actual", keibajoCode: "01", raceDate: "2026-06-15" }, mockEnv);

  const upsertCall = vi.mocked(upsertVenueWeather).mock.calls[0]![0]!;
  expect(upsertCall.keibajoCode).toBe("01");
  expect(upsertCall.raceDate).toBe("2026-06-15");
  expect(upsertCall.weatherType).toBe("actual");
  expect(upsertCall.venue).toStrictEqual({
    name: "札幌",
    lat: 43.0775,
    lon: 141.3269,
  });
  expect(upsertCall.db).toBe(mockDb);
});

it("handleWeatherBatch calls processWeatherJob for each message and acks", async () => {
  const mockAck1 = vi.fn();
  const mockAck2 = vi.fn();

  const batch = {
    messages: [
      {
        body: { type: "forecast", keibajoCode: "05", raceDate: "2026-06-22" },
        ack: mockAck1,
      },
      {
        body: { type: "actual", keibajoCode: "09", raceDate: "2026-06-22" },
        ack: mockAck2,
      },
    ],
  } as unknown as MessageBatch<import("./types").WeatherJob>;

  await handleWeatherBatch(batch, mockEnv);

  expect(fetchVenueWeather).toHaveBeenCalledTimes(2);
  expect(upsertVenueWeather).toHaveBeenCalledTimes(2);
  expect(mockAck1).toHaveBeenCalledTimes(1);
  expect(mockAck2).toHaveBeenCalledTimes(1);
});

it("handleWeatherBatch handles empty message batch", async () => {
  const batch = {
    messages: [],
  } as unknown as MessageBatch<import("./types").WeatherJob>;

  await handleWeatherBatch(batch, mockEnv);

  expect(fetchVenueWeather).not.toHaveBeenCalled();
  expect(upsertVenueWeather).not.toHaveBeenCalled();
});
