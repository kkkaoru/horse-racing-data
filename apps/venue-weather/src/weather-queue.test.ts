import { beforeEach, expect, it, vi } from "vitest";
import { handleWeatherBatch, processWeatherJob } from "./weather-queue";

vi.mock("./weather-api", () => ({
  fetchVenueWeather: vi.fn().mockResolvedValue(
    Array.from({ length: 24 }, (_, hour) => ({
      date: "2026-06-22",
      hour,
      precipitation: 0,
      temperature: 20,
      weatherCode: 1,
      windGusts: 5,
      windSpeed: 3,
    })),
  ),
}));

vi.mock("./weather-r2-store", () => ({
  upsertVenueWeather: vi.fn().mockResolvedValue(1),
}));

import { fetchVenueWeather } from "./weather-api";
import { upsertVenueWeather } from "./weather-r2-store";

const mockArchive = {} as unknown as R2Bucket;
const mockCatalogStream = { send: vi.fn().mockResolvedValue(undefined) };
const mockSendBatch = vi.fn().mockResolvedValue(undefined);
const mockKvDelete = vi.fn().mockResolvedValue(undefined);
const mockEnv = {
  WEATHER_ARCHIVE: mockArchive,
  WEATHER_CATALOG_STREAM: mockCatalogStream,
  WEATHER_JOBS: { sendBatch: mockSendBatch },
  WEATHER_KV: { delete: mockKvDelete },
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
  expect(mockKvDelete).not.toHaveBeenCalled();
});

it("processWeatherJob invalidates KV after upsert", async () => {
  await processWeatherJob({ type: "forecast", keibajoCode: "05", raceDate: "2026-06-22" }, mockEnv);

  expect(upsertVenueWeather).toHaveBeenCalledTimes(1);
  expect(mockKvDelete).toHaveBeenCalledWith("weather:2026-06-22");
});

it("processWeatherJob calls fetchVenueWeather and upsertVenueWeather for known venue", async () => {
  await processWeatherJob({ type: "forecast", keibajoCode: "05", raceDate: "2026-06-22" }, mockEnv);

  expect(fetchVenueWeather).toHaveBeenCalledTimes(1);
  expect(fetchVenueWeather).toHaveBeenCalledWith({
    raceDate: "2026-06-22",
    venue: { lat: 35.6622, lon: 139.4856, name: "東京" },
    weatherType: "forecast",
  });
  expect(upsertVenueWeather).toHaveBeenCalledTimes(1);
});

it("processWeatherJob passes correct params to upsertVenueWeather", async () => {
  await processWeatherJob({ type: "actual", keibajoCode: "01", raceDate: "2026-06-15" }, mockEnv);

  const upsertCall = vi.mocked(upsertVenueWeather).mock.calls[0]![0]!;
  expect(upsertCall.archive).toBe(mockArchive);
  expect(upsertCall.catalogStream).toBe(mockCatalogStream);
  expect(upsertCall.keibajoCode).toBe("01");
  expect(upsertCall.raceDate).toBe("2026-06-15");
  expect(upsertCall.weatherType).toBe("actual");
  expect(upsertCall.venue).toStrictEqual({
    lat: 43.0775,
    lon: 141.3269,
    name: "札幌",
  });
});

it("handleWeatherBatch calls processWeatherJob for each message and acks", async () => {
  const mockAck1 = vi.fn();
  const mockAck2 = vi.fn();
  const batch = {
    messages: [
      {
        ack: mockAck1,
        body: { keibajoCode: "05", raceDate: "2026-06-22", type: "forecast" },
      },
      {
        ack: mockAck2,
        body: { keibajoCode: "09", raceDate: "2026-06-22", type: "actual" },
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

it("handleWeatherBatch leaves a failed message unacked for queue retry", async () => {
  vi.mocked(fetchVenueWeather).mockRejectedValueOnce(new Error("incomplete weather"));
  const ack = vi.fn();
  const batch = {
    messages: [
      {
        ack,
        body: { keibajoCode: "05", raceDate: "2026-06-22", type: "forecast" },
      },
    ],
  } as unknown as MessageBatch<import("./types").WeatherJob>;

  await expect(handleWeatherBatch(batch, mockEnv)).rejects.toThrow("incomplete weather");
  expect(ack).not.toHaveBeenCalled();
  expect(upsertVenueWeather).not.toHaveBeenCalled();
});
