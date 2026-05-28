// run with: bun run test
import { EventEmitter } from "node:events";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import {
  buildInsertSql,
  parsePushRunningStyleDateCliArgs,
  readLocalRows,
  run,
  spawnWrangler,
} from "./push-running-style-date-to-remote";

vi.mock("../src/running-style-date-progress", () => ({
  resolveRunningStyleDateYmd: vi.fn((dateRaw: string, _year: number | undefined, _now: Date) =>
    dateRaw.replaceAll("-", ""),
  ),
}));

interface FakeChildProcess extends EventEmitter {
  stderr: EventEmitter;
  stdout: EventEmitter;
}

const fakeChild = (): FakeChildProcess => {
  const child = new EventEmitter() as FakeChildProcess;
  child.stderr = new EventEmitter();
  child.stdout = new EventEmitter();
  return child;
};

vi.mock("node:child_process", () => ({
  spawn: vi.fn(() => {
    const child = fakeChild();
    queueMicrotask(() => child.emit("close", 0));
    return child;
  }),
}));

vi.mock("node:fs/promises", () => ({
  mkdtemp: vi.fn(async () => "/tmp/running-style-push-1234"),
  rm: vi.fn(async () => undefined),
  writeFile: vi.fn(async () => undefined),
}));

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("parsePushRunningStyleDateCliArgs reads --date", () => {
  expect(parsePushRunningStyleDateCliArgs(["--date", "20260524"]).dateYmd).toBe("20260524");
});

it("parsePushRunningStyleDateCliArgs reads --date with --year", () => {
  expect(parsePushRunningStyleDateCliArgs(["--date", "05-24", "--year", "2026"]).dateYmd).toBe(
    "0524",
  );
});

it("parsePushRunningStyleDateCliArgs throws when --date missing", () => {
  expect(() => parsePushRunningStyleDateCliArgs([])).toThrow(
    "Usage: bun run running-style:push-remote",
  );
});

it("parsePushRunningStyleDateCliArgs throws when --date has no value", () => {
  expect(() => parsePushRunningStyleDateCliArgs(["--date"])).toThrow("--date requires a value");
});

it("parsePushRunningStyleDateCliArgs throws on unknown argument", () => {
  expect(() => parsePushRunningStyleDateCliArgs(["--date", "20260524", "--other"])).toThrow(
    "Unknown argument: --other",
  );
});

it("parsePushRunningStyleDateCliArgs throws when --year has no value", () => {
  expect(() => parsePushRunningStyleDateCliArgs(["--date", "20260524", "--year"])).toThrow(
    "--year requires a value",
  );
});

it("buildInsertSql renders SQL with null bamei and decimal probabilities", () => {
  expect(
    buildInsertSql({
      bamei: null,
      category: "jra",
      horse_number: 1,
      kaisai_nen: "2026",
      ketto_toroku_bango: "2024100001",
      model_version: "v7-lineage",
      p_nige: 0.1,
      p_oikomi: 0.2,
      p_sashi: 0.3,
      p_senkou: 0.4,
      predicted_at: "2026-05-24T11:00:00+09:00",
      predicted_label: "senkou",
      race_key: "jra:2026:0524:08:01",
    }),
  ).toBe(
    "insert or replace into race_running_styles (\n  race_key, horse_number, ketto_toroku_bango, bamei, category, kaisai_nen,\n  model_version, p_nige, p_senkou, p_sashi, p_oikomi, predicted_label, predicted_at\n) values (\n  'jra:2026:0524:08:01',\n  1,\n  '2024100001',\n  null,\n  'jra',\n  '2026',\n  'v7-lineage',\n  0.1,\n  0.4,\n  0.3,\n  0.2,\n  'senkou',\n  '2026-05-24T11:00:00+09:00'\n);",
  );
});

it("buildInsertSql escapes single quotes in string values", () => {
  expect(
    buildInsertSql({
      bamei: "サ'ンプル",
      category: "jra",
      horse_number: 2,
      kaisai_nen: "2026",
      ketto_toroku_bango: "2024100002",
      model_version: "v7-lineage",
      p_nige: 0,
      p_oikomi: 0,
      p_sashi: 0,
      p_senkou: 1,
      predicted_at: "x",
      predicted_label: "nige",
      race_key: "key",
    }),
  ).toBe(
    "insert or replace into race_running_styles (\n  race_key, horse_number, ketto_toroku_bango, bamei, category, kaisai_nen,\n  model_version, p_nige, p_senkou, p_sashi, p_oikomi, predicted_label, predicted_at\n) values (\n  'key',\n  2,\n  '2024100002',\n  'サ''ンプル',\n  'jra',\n  '2026',\n  'v7-lineage',\n  0,\n  1,\n  0,\n  0,\n  'nige',\n  'x'\n);",
  );
});

it("spawnWrangler resolves when the child closes with exit code 0", async () => {
  const childProcess = await import("node:child_process");
  const child = fakeChild();
  vi.mocked(childProcess.spawn).mockReturnValueOnce(child as never);
  const promise = spawnWrangler(["wrangler", "d1", "execute"]);
  queueMicrotask(() => child.emit("close", 0));
  await expect(promise).resolves.toBeUndefined();
});

it("spawnWrangler throws when the child closes with non-zero exit code", async () => {
  const childProcess = await import("node:child_process");
  const child = fakeChild();
  vi.mocked(childProcess.spawn).mockReturnValueOnce(child as never);
  const promise = spawnWrangler(["wrangler"]);
  queueMicrotask(() => {
    child.stderr.emit("data", Buffer.from("boom"));
    child.emit("close", 1);
  });
  await expect(promise).rejects.toThrow("wrangler failed (exit 1)");
});

it("spawnWrangler rejects when the child emits an error event", async () => {
  const childProcess = await import("node:child_process");
  const child = fakeChild();
  vi.mocked(childProcess.spawn).mockReturnValueOnce(child as never);
  const promise = spawnWrangler(["wrangler"]);
  queueMicrotask(() => child.emit("error", new Error("spawn failed")));
  await expect(promise).rejects.toThrow("spawn failed");
});

it("readLocalRows returns results from wrangler --json stdout", async () => {
  const childProcess = await import("node:child_process");
  const child = fakeChild();
  vi.mocked(childProcess.spawn).mockReturnValueOnce(child as never);
  const promise = readLocalRows("20260524");
  queueMicrotask(() => {
    child.stdout.emit(
      "data",
      Buffer.from(
        JSON.stringify([
          {
            results: [
              {
                bamei: "テスト",
                category: "jra",
                horse_number: 1,
                kaisai_nen: "2026",
                ketto_toroku_bango: "abc",
                model_version: "v7",
                p_nige: 0.1,
                p_oikomi: 0.2,
                p_sashi: 0.3,
                p_senkou: 0.4,
                predicted_at: "2026-05-24T11:00:00+09:00",
                predicted_label: "senkou",
                race_key: "jra:2026:0524:08:01",
              },
            ],
          },
        ]),
      ),
    );
    child.emit("close", 0);
  });
  const rows = await promise;
  expect(rows).toHaveLength(1);
  expect(rows[0]?.race_key).toBe("jra:2026:0524:08:01");
});

it("readLocalRows throws on non-zero exit", async () => {
  const childProcess = await import("node:child_process");
  const child = fakeChild();
  vi.mocked(childProcess.spawn).mockReturnValueOnce(child as never);
  const promise = readLocalRows("20260524");
  queueMicrotask(() => {
    child.stderr.emit("data", Buffer.from("d1 boom"));
    child.emit("close", 2);
  });
  await expect(promise).rejects.toThrow("local d1 read failed (exit 2)");
});

it("spawnWrangler treats null close code as exit 0", async () => {
  const childProcess = await import("node:child_process");
  const child = fakeChild();
  vi.mocked(childProcess.spawn).mockReturnValueOnce(child as never);
  const promise = spawnWrangler(["wrangler", "noop"]);
  queueMicrotask(() => child.emit("close", null));
  await expect(promise).resolves.toBeUndefined();
});

it("readLocalRows returns [] when stdout payload is an empty array", async () => {
  const childProcess = await import("node:child_process");
  const child = fakeChild();
  vi.mocked(childProcess.spawn).mockReturnValueOnce(child as never);
  const promise = readLocalRows("20260524");
  queueMicrotask(() => {
    child.stdout.emit("data", Buffer.from(JSON.stringify([])));
    child.emit("close", 0);
  });
  await expect(promise).resolves.toStrictEqual([]);
});

it("run throws when no local rows found for the date", async () => {
  const childProcess = await import("node:child_process");
  const child = fakeChild();
  vi.mocked(childProcess.spawn).mockReturnValueOnce(child as never);
  vi.stubGlobal("process", {
    ...process,
    argv: ["bun", "scripts/push.ts", "--date", "20260524"],
  });
  const promise = run();
  queueMicrotask(() => {
    child.stdout.emit("data", Buffer.from(JSON.stringify([{ results: [] }])));
    child.emit("close", 0);
  });
  await expect(promise).rejects.toThrow("No local race_running_styles rows found for 20260524");
  vi.unstubAllGlobals();
});

it("readLocalRows treats null close code as exit 0", async () => {
  const childProcess = await import("node:child_process");
  const child = fakeChild();
  vi.mocked(childProcess.spawn).mockReturnValueOnce(child as never);
  const promise = readLocalRows("20260524");
  queueMicrotask(() => {
    child.stdout.emit("data", Buffer.from(JSON.stringify([{ results: [] }])));
    child.emit("close", null);
  });
  await expect(promise).resolves.toStrictEqual([]);
});

it("run writes SQL and invokes wrangler when local rows are present", async () => {
  const childProcess = await import("node:child_process");
  const readChild = fakeChild();
  vi.mocked(childProcess.spawn).mockImplementationOnce(() => {
    queueMicrotask(() => {
      readChild.stdout.emit(
        "data",
        Buffer.from(
          JSON.stringify([
            {
              results: [
                {
                  bamei: "テスト",
                  category: "jra",
                  horse_number: 1,
                  kaisai_nen: "2026",
                  ketto_toroku_bango: "abc",
                  model_version: "v7",
                  p_nige: 0.1,
                  p_oikomi: 0.2,
                  p_sashi: 0.3,
                  p_senkou: 0.4,
                  predicted_at: "2026-05-24T11:00:00+09:00",
                  predicted_label: "senkou",
                  race_key: "jra:2026:0524:08:01",
                },
              ],
            },
          ]),
        ),
      );
      readChild.emit("close", 0);
    });
    return readChild as never;
  });
  vi.stubGlobal("process", {
    ...process,
    argv: ["bun", "scripts/push.ts", "--date", "20260524"],
  });
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  await expect(run()).resolves.toBeUndefined();
  const fs = await import("node:fs/promises");
  expect(fs.writeFile).toHaveBeenCalledTimes(1);
  vi.unstubAllGlobals();
});
