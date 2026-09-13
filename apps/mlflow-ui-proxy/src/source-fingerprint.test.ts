// Run with bun.
import { afterEach, expect, test, vi } from "vitest";
import { neon } from "@neondatabase/serverless";
import { readSourceFingerprints } from "./source-fingerprint";

const mocks = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("@neondatabase/serverless", () => ({ neon: vi.fn(() => mocks.query) }));
const input = { connectionString: "postgresql://test", dateFrom: "20260911", dateTo: "20260913" };
afterEach(() => {
  mocks.query.mockReset();
  vi.mocked(neon).mockClear();
});

test("maps daily generation rows including empty dates", async () => {
  mocks.query.mockResolvedValue([
    { date: "20260911", fingerprint: "abc" },
    { date: "20260912", fingerprint: "empty" },
  ]);
  expect(await readSourceFingerprints(input)).toStrictEqual([
    { date: "20260911", fingerprint: "abc" },
    { date: "20260912", fingerprint: "empty" },
  ]);
  expect(mocks.query).toHaveBeenCalledOnce();
});

test("binds indexed year and month-day bounds across year rollover", async () => {
  mocks.query.mockResolvedValue([{ date: "20261231", fingerprint: "empty" }]);
  await readSourceFingerprints({ ...input, dateFrom: "20261231", dateTo: "20270102" });
  expect(mocks.query.mock.calls[0]?.slice(1)).toStrictEqual([
    "2026",
    "1231",
    "2027",
    "0102",
    "2026",
    "1231",
    "2027",
    "0102",
    "20261231",
    "20270102",
  ]);
});

test("rejects malformed start date before accessing the database", async () => {
  await expect(readSourceFingerprints({ ...input, dateFrom: "bad" })).rejects.toThrow(
    "Invalid MLflow sync date window",
  );
  expect(neon).not.toHaveBeenCalled();
});

test("rejects malformed end date", async () => {
  await expect(readSourceFingerprints({ ...input, dateTo: "bad" })).rejects.toThrow(
    "Invalid MLflow sync date window",
  );
});

test("empty response cannot silently skip reconciliation", async () => {
  mocks.query.mockResolvedValue([]);
  await expect(readSourceFingerprints(input)).rejects.toThrow(
    "Empty MLflow source fingerprint window",
  );
});

test("rejects non-string date", async () => {
  mocks.query.mockResolvedValue([{ date: 20260911, fingerprint: "abc" }]);
  await expect(readSourceFingerprints(input)).rejects.toThrow(
    "Invalid MLflow source fingerprint response",
  );
});

test("rejects missing fingerprint", async () => {
  mocks.query.mockResolvedValue([{ date: "20260911" }]);
  await expect(readSourceFingerprints(input)).rejects.toThrow(
    "Invalid MLflow source fingerprint response",
  );
});
