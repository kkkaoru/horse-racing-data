import { expect, it, vi } from "vitest";
import {
  buildLiveWeatherV2R2Key,
  buildSnapshotWeatherV2R2Key,
  putVenueWeatherV2RuntimeObjects,
  readWeatherV2ByDate,
  sendVenueWeatherV2CatalogEvents,
} from "./weather-v2-store";

it("builds isolated v2 live and snapshot keys", () => {
  expect(buildLiveWeatherV2R2Key("2026-08-25", "05", "forecast")).toBe(
    "venue-weather-live/v2/race_date=2026-08-25/keibajo_code=05/forecast.json",
  );
  expect(
    buildSnapshotWeatherV2R2Key("2026-08-25", "05", "forecast", "2026-08-24T16:30:00.000Z"),
  ).toBe(
    "venue-weather-snapshots/v2/race_date=2026-08-25/keibajo_code=05/forecast/2026-08-24T16:30:00_000Z.json",
  );
});

it("writes complete v2 rows to isolated runtime objects", async () => {
  const put = vi.fn().mockResolvedValue(undefined);
  const rows = await putVenueWeatherV2RuntimeObjects({
    archive: { put } as unknown as R2Bucket,
    fetchedAt: "2026-08-24T16:30:00.000Z",
    keibajoCode: "05",
    raceDate: "2026-08-25",
    rows: [
      {
        date: "2026-08-25",
        dewPoint: 22.5,
        hour: 10,
        precipitation: 0,
        relativeHumidity: 78,
        shortwaveRadiation: 410,
        temperature: 27,
        weatherCode: 1,
        wetBulbTemperature: 24,
        windGusts: 8,
        windSpeed: 4,
      },
    ],
    venue: { lat: 35.6622, lon: 139.4856, name: "東京" },
    weatherType: "forecast",
  });

  expect(rows).toBe(1);
  expect(put).toHaveBeenCalledTimes(2);
  expect(JSON.parse(String(put.mock.calls[0]?.[1]))).toStrictEqual({
    fetchedAt: "2026-08-24T16:30:00.000Z",
    keibajoCode: "05",
    raceDate: "2026-08-25",
    rows: [
      {
        dew_point: 22.5,
        keibajo_code: "05",
        precipitation: 0,
        race_date: "2026-08-25",
        relative_humidity: 78,
        shortwave_radiation: 410,
        temperature: 27,
        weather_code: 1,
        weather_hour: 10,
        wet_bulb_temperature: 24,
        wind_gusts: 8,
        wind_speed: 4,
      },
    ],
    schemaVersion: 2,
    weatherDataType: "forecast",
  });
});

it("sends complete v2 rows to the isolated catalog stream", async () => {
  const send = vi.fn().mockResolvedValue(undefined);
  const rows = await sendVenueWeatherV2CatalogEvents({
    catalogStream: { send },
    fetchedAt: "2026-08-24T16:30:00.000Z",
    keibajoCode: "05",
    raceDate: "2026-08-25",
    rows: [
      {
        date: "2026-08-25",
        dewPoint: 22.5,
        hour: 10,
        precipitation: 0,
        relativeHumidity: 78,
        shortwaveRadiation: 410,
        temperature: 27,
        weatherCode: 1,
        wetBulbTemperature: 24,
        windGusts: 8,
        windSpeed: 4,
      },
    ],
    venue: { lat: 35.6622, lon: 139.4856, name: "東京" },
    weatherType: "forecast",
  });

  expect(rows).toBe(1);
  expect(send).toHaveBeenCalledWith([
    {
      dew_point: 22.5,
      fetched_at: "2026-08-24T16:30:00.000Z",
      keibajo_code: "05",
      latitude: 35.6622,
      longitude: 139.4856,
      precipitation: 0,
      race_date: "2026-08-25",
      relative_humidity: 78,
      shortwave_radiation: 410,
      temperature: 27,
      venue_name: "東京",
      weather_code: 1,
      weather_data_type: "forecast",
      weather_hour: 10,
      wet_bulb_temperature: 24,
      wind_gusts: 8,
      wind_speed: 4,
    },
  ]);
});

it("skips catalog writes when the optional stream binding is absent", async () => {
  const rows = await sendVenueWeatherV2CatalogEvents({
    catalogStream: undefined,
    fetchedAt: "2026-08-24T16:30:00.000Z",
    keibajoCode: "05",
    raceDate: "2026-08-25",
    rows: [],
    venue: { lat: 35.6622, lon: 139.4856, name: "東京" },
    weatherType: "forecast",
  });

  expect(rows).toBe(0);
});

it("reads paginated v2 objects, prefers actual, and sorts venue-hours", async () => {
  const payload = (code: string, hour: number, weatherDataType: "actual" | "forecast") => ({
    fetchedAt: "2026-08-25T01:30:00.000Z",
    keibajoCode: code,
    raceDate: "2026-08-25",
    rows: [
      {
        dew_point: 22,
        keibajo_code: code,
        precipitation: 0,
        race_date: "2026-08-25",
        relative_humidity: 80,
        shortwave_radiation: 100,
        temperature: 25,
        weather_code: 1,
        weather_hour: hour,
        wet_bulb_temperature: 23,
        wind_gusts: 8,
        wind_speed: 4,
      },
    ],
    schemaVersion: 2,
    weatherDataType,
  });
  const list = vi
    .fn()
    .mockResolvedValueOnce({
      cursor: "next",
      objects: [
        { key: "venue-weather-live/v2/race_date=2026-08-25/keibajo_code=02/forecast.json" },
        { key: "venue-weather-live/v2/race_date=2026-08-25/keibajo_code=02/actual.json" },
      ],
      truncated: true,
    })
    .mockResolvedValueOnce({
      objects: [
        { key: "venue-weather-live/v2/race_date=2026-08-25/keibajo_code=01/forecast.json" },
      ],
      truncated: false,
    });
  const get = vi.fn(async (key: string) => ({
    json: vi
      .fn()
      .mockResolvedValue(
        key.includes("keibajo_code=02") ? payload("02", 2, "actual") : payload("01", 1, "forecast"),
      ),
  }));

  const rows = await readWeatherV2ByDate({ list, get } as unknown as R2Bucket, "2026-08-25");

  expect(rows.map((row) => `${row.keibajo_code}:${String(row.weather_hour)}`)).toStrictEqual([
    "01:1",
    "02:2",
  ]);
  expect(get).toHaveBeenCalledTimes(2);
});

it("drops missing, malformed, and wrong-date v2 objects", async () => {
  const list = vi.fn().mockResolvedValue({
    objects: [{ key: "missing" }, { key: "malformed" }, { key: "wrong-date" }],
    truncated: false,
  });
  const get = vi
    .fn()
    .mockRejectedValueOnce(new Error("missing"))
    .mockResolvedValueOnce({ json: vi.fn().mockResolvedValue({ schemaVersion: 1 }) })
    .mockResolvedValueOnce({
      json: vi.fn().mockResolvedValue({
        keibajoCode: "01",
        raceDate: "2026-08-24",
        rows: [],
        schemaVersion: 2,
      }),
    });

  const rows = await readWeatherV2ByDate({ list, get } as unknown as R2Bucket, "2026-08-25");

  expect(rows).toStrictEqual([]);
});

it("fails closed without writing when any v2 metric is absent", async () => {
  const put = vi.fn().mockResolvedValue(undefined);
  const rows = await putVenueWeatherV2RuntimeObjects({
    archive: { put } as unknown as R2Bucket,
    fetchedAt: "2026-08-24T16:30:00.000Z",
    keibajoCode: "05",
    raceDate: "2026-08-25",
    rows: [
      {
        date: "2026-08-25",
        hour: 10,
        precipitation: 0,
        temperature: 27,
        weatherCode: 1,
        windGusts: 8,
        windSpeed: 4,
      },
    ],
    venue: { lat: 35.6622, lon: 139.4856, name: "東京" },
    weatherType: "forecast",
  });

  expect(rows).toBe(0);
  expect(put).not.toHaveBeenCalled();
});
