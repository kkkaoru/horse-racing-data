// Run with bun. Tests timestamp-independent running-style content fingerprints.

import { expect, test, vi } from "vitest";

import { computeRunningStyleContentFingerprint } from "./running-style-content-hash";

const row = (overrides: Record<string, unknown> = {}) => ({
  horse_number: 1,
  ketto_toroku_bango: "HORSE-1",
  p_nige: 0.1,
  p_oikomi: 0.4,
  p_sashi: 0.3,
  p_senkou: 0.2,
  predicted_label: "nige",
  race_key: "nar:20260910:50:11",
  ...overrides,
});

const database = (results: unknown[]) => {
  const all = vi.fn(async () => ({ results }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  return { all, bind, db: { prepare } as unknown as D1Database, prepare };
};

test("hashes normalized feature content independently of input ordering", async () => {
  const first = database([
    row({ horse_number: 2, ketto_toroku_bango: "HORSE-2", predicted_label: "senkou" }),
    row(),
  ]);
  const second = database([
    row(),
    row({ horse_number: 2, ketto_toroku_bango: "HORSE-2", predicted_label: "senkou" }),
  ]);

  const left = await computeRunningStyleContentFingerprint({
    category: "nar",
    db: first.db,
    runYmd: "20260910",
  });
  const right = await computeRunningStyleContentFingerprint({
    category: "nar",
    db: second.db,
    runYmd: "20260910",
  });

  expect(left).toStrictEqual({
    contentHash: "e7e716489c0065429f998fabd7ac83d9ce443d8f2763521fc0d6d034b2c34df5",
    rowCount: 2,
  });
  expect(right).toStrictEqual(left);
  expect(first.bind).toHaveBeenCalledWith("nar:20260910:%", "nar:20260910:83:%");
});

test("isolates JRA and maps every running-style label", async () => {
  const fixture = database([
    row({ predicted_label: "oikomi", race_key: "jra:20260910:5:1" }),
    row({ horse_number: 2, predicted_label: "sashi", race_key: "jra:20260910:05:01" }),
  ]);

  const result = await computeRunningStyleContentFingerprint({
    category: "jra",
    db: fixture.db,
    runYmd: "20260910",
  });

  expect(result.rowCount).toBe(2);
  expect(result.contentHash).toMatch(/^[0-9a-f]{64}$/);
  expect(fixture.bind).toHaveBeenCalledWith("jra:20260910:%", "jra:20260910:__never__:%");
});

test("returns the stable absent fingerprint for Ban-ei without querying D1", async () => {
  const fixture = database([]);

  await expect(
    computeRunningStyleContentFingerprint({
      category: "ban-ei",
      db: fixture.db,
      runYmd: "20260910",
    }),
  ).resolves.toStrictEqual({ contentHash: "none", rowCount: 0 });
  expect(fixture.prepare).not.toHaveBeenCalled();
});

test.each([
  { horse_number: 0 },
  { ketto_toroku_bango: " " },
  { p_nige: Number.NaN },
  { predicted_label: "unknown" },
  { race_key: "invalid" },
  { race_key: "nar:bad:50:11" },
  { race_key: "other:20260910:50:11" },
  { race_key: "nar:20260910:0:11" },
  { race_key: "nar:20260910:50:0" },
])("rejects an invalid running-style content row: $race_key", async (overrides) => {
  const fixture = database([row(overrides)]);

  await expect(
    computeRunningStyleContentFingerprint({
      category: "nar",
      db: fixture.db,
      runYmd: "20260910",
    }),
  ).rejects.toThrow("running-style-row-invalid");
});

test("rejects an unbounded running-style result", async () => {
  const fixture = database(Array.from({ length: 1_025 }, () => row()));

  await expect(
    computeRunningStyleContentFingerprint({
      category: "nar",
      db: fixture.db,
      runYmd: "20260910",
    }),
  ).rejects.toThrow("running-style-row-limit");
});
