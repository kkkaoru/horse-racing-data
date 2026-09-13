// Run with bun. Exercise lifecycle wiring with the Container runtime mocked.
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { SYNC_EXEC_TIMEOUT_MS } from "./exec-output";
import { MlflowContainer } from "./index";

const mocks = vi.hoisted(() => ({
  probe: vi.fn(),
  get: vi.fn(),
  put: vi.fn(),
  exec: vi.fn(),
  output: vi.fn(),
  ready: vi.fn(),
  fetch: vi.fn(),
  expire: vi.fn(),
  renew: vi.fn(),
  env: { NEON_PRIMARY_URL: "postgresql://test", MLFLOW_SOURCE_GATE_ENABLED: "1" },
}));
vi.mock("./source-fingerprint", () => ({ readSourceFingerprints: mocks.probe }));
vi.mock("@cloudflare/containers", () => ({
  Container: class {
    env = mocks.env;
    ctx = { storage: { get: mocks.get, put: mocks.put }, container: { exec: mocks.exec } };
    startAndWaitForPorts = mocks.ready;
    containerFetch = mocks.fetch;
    renewActivityTimeout = mocks.renew;
    onActivityExpired(): Promise<void> {
      return mocks.expire();
    }
  },
}));

const makeContainer = (): MlflowContainer => {
  const value: unknown = Reflect.construct(MlflowContainer, []);
  if (!(value instanceof MlflowContainer)) throw new Error("Invalid Container mock");
  return value;
};

beforeEach(() => {
  vi.resetAllMocks();
  mocks.env.MLFLOW_SOURCE_GATE_ENABLED = "1";
  mocks.probe.mockResolvedValue([{ date: "20260911", fingerprint: "a" }]);
  mocks.get.mockResolvedValue(undefined);
  mocks.output.mockResolvedValue({
    exitCode: 0,
    stdout: new TextEncoder().encode("updated"),
    stderr: new Uint8Array(),
  });
  mocks.exec.mockResolvedValue({ output: mocks.output });
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

test.each([
  { killFails: true, outputFails: false },
  { killFails: false, outputFails: true },
])("holds serialization until timed-out output settles: %j", async ({ killFails, outputFails }) => {
  vi.useFakeTimers();
  vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const pending = Promise.withResolvers<ExecOutput>();
  const kill = vi.fn(() => {
    if (killFails) throw new Error("Process termination is unconfirmed");
  });
  mocks.output.mockReturnValueOnce(pending.promise);
  mocks.exec.mockResolvedValue({ output: mocks.output, kill });
  const container = makeContainer();
  const first = container.syncProductionPreview("20260911", "20260913");
  const rejected = expect(first).rejects.toThrow("MLflow sync exec exceeded ten minutes");
  await vi.advanceTimersByTimeAsync(SYNC_EXEC_TIMEOUT_MS);
  await rejected;
  expect(kill).toHaveBeenCalledWith(9);
  expect(mocks.put).not.toHaveBeenCalled();
  const second = container.syncProductionPreview("20260912", "20260914");
  await vi.advanceTimersByTimeAsync(0);
  await container.onActivityExpired();
  expect(mocks.exec).toHaveBeenCalledOnce();
  expect(mocks.expire).not.toHaveBeenCalled();
  if (outputFails) pending.reject(new Error("Process terminated"));
  else pending.resolve({ exitCode: 0, stdout: new ArrayBuffer(0), stderr: new ArrayBuffer(0) });
  await second;
  expect(mocks.exec).toHaveBeenCalledTimes(2);
  await container.onActivityExpired();
  expect(mocks.expire).toHaveBeenCalledOnce();
});

test("rollback switch bypasses the fingerprint gate", async () => {
  mocks.env.MLFLOW_SOURCE_GATE_ENABLED = "0";
  const container = makeContainer();
  await container.syncProductionPreview("20260911", "20260913");
  expect(mocks.probe).not.toHaveBeenCalled();
  expect(mocks.put).not.toHaveBeenCalled();
  expect(mocks.exec.mock.calls[0]?.[0]).toStrictEqual([
    "python",
    "-m",
    "mlflow_tracking.cli",
    "sync-production-preview",
    "--date-from",
    "20260911",
    "--date-to",
    "20260913",
    "--categories",
    "jra,nar,banei",
  ]);
});

test("unchanged source never boots the runtime", async () => {
  mocks.get.mockResolvedValue({ fingerprint: "a", syncedAt: Date.now() });
  const container = makeContainer();
  expect(await container.syncProductionPreview("20260911", "20260913")).toStrictEqual({
    exitCode: 0,
    stdout: "",
    stderr: "",
  });
  expect(mocks.ready).not.toHaveBeenCalled();
  expect(container.sleepAfter).toBe("30s");
});

test("cron sync uses changed date and short idle grace after completion", async () => {
  const container = makeContainer();
  expect(await container.syncProductionPreview("20260911", "20260913")).toStrictEqual({
    exitCode: 0,
    stdout: "updated",
    stderr: "",
  });
  expect(mocks.exec.mock.calls[0]?.[0]).toStrictEqual([
    "python",
    "-m",
    "mlflow_tracking.cli",
    "sync-production-preview",
    "--date-from",
    "20260911",
    "--date-to",
    "20260911",
    "--categories",
    "jra,nar,banei",
  ]);
  expect(container.sleepAfter).toBe("30s");
  expect(mocks.put).toHaveBeenCalledOnce();
});

test("UI activity preserves the existing five-minute grace", async () => {
  mocks.fetch.mockResolvedValue(new Response("UI"));
  const container = makeContainer();
  await container.fetch(new Request("https://example.test/"));
  await container.syncProductionPreview("20260911", "20260913");
  expect(container.sleepAfter).toBe("5m");
});

test("failed sync is not checkpointed and does not poison later syncs", async () => {
  mocks.output.mockResolvedValueOnce({
    exitCode: 1,
    stdout: new Uint8Array(),
    stderr: new TextEncoder().encode("failure"),
  });
  const container = makeContainer();
  await expect(container.syncProductionPreview("20260911", "20260913")).rejects.toThrow(
    "MLflow preview sync failed: failure",
  );
  expect(mocks.put).not.toHaveBeenCalled();
  await container.syncProductionPreview("20260911", "20260913");
  expect(mocks.put).toHaveBeenCalledOnce();
});

test("active exec is protected from idle expiry and simultaneous cron execution", async () => {
  const pending = Promise.withResolvers<{
    exitCode: number;
    stdout: Uint8Array;
    stderr: Uint8Array;
  }>();
  mocks.output.mockReturnValueOnce(pending.promise);
  const container = makeContainer();
  const first = container.syncProductionPreview("20260911", "20260913");
  await vi.waitFor(() => expect(mocks.output).toHaveBeenCalledOnce());
  const second = container.syncProductionPreview("20260912", "20260914");
  await container.onActivityExpired();
  expect(mocks.expire).not.toHaveBeenCalled();
  expect(mocks.exec).toHaveBeenCalledOnce();
  pending.resolve({ exitCode: 0, stdout: new Uint8Array(), stderr: new Uint8Array() });
  await Promise.all([first, second]);
  await container.onActivityExpired();
  expect(mocks.expire).toHaveBeenCalledOnce();
});
