// Runs with bun via Vitest; all journal and storage effects are mocked.
import { expect, test, vi } from "vitest";
import { retainTrackedDeadLetter, type ArchiveAttemptPorts } from "./ingestion-archive-attempt";
import { prepareIngestionEnvelope } from "./ingestion-buffer";

test("invalid SDK timestamps fail before journaling or retention", async () => {
  const ports = {
    begin: vi.fn<ArchiveAttemptPorts["begin"]>(),
    accept: vi.fn<ArchiveAttemptPorts["accept"]>(),
    recordReceipt: vi.fn<ArchiveAttemptPorts["recordReceipt"]>(),
  } satisfies ArchiveAttemptPorts;
  await expect(
    retainTrackedDeadLetter({ id: "bad-time", timestamp: new Date("invalid"), body: {} }, ports),
  ).rejects.toThrow();
  expect(ports.begin).not.toHaveBeenCalled();
  expect(ports.accept).not.toHaveBeenCalled();
});

test("persists pending identity before retention, then requires outcome journaling", async () => {
  const events: string[] = [];
  const ports = {
    begin: vi.fn<ArchiveAttemptPorts["begin"]>(async () => {
      events.push("begin");
    }),
    accept: vi.fn<ArchiveAttemptPorts["accept"]>(async (envelope) => {
      events.push("retain");
      const pointer = prepareIngestionEnvelope(envelope).pointer;
      return {
        source: pointer.source,
        requestId: pointer.requestId,
        digest: pointer.digest,
        sequence: "9007199254740993",
        accepted: true,
      };
    }),
    recordReceipt: vi.fn<ArchiveAttemptPorts["recordReceipt"]>(async () => {
      events.push("receipt");
    }),
  } satisfies ArchiveAttemptPorts;
  expect(
    await retainTrackedDeadLetter(
      { id: "job", timestamp: new Date("2026-09-16"), body: { type: "fetch-odds" } },
      ports,
    ),
  ).toMatchObject({ accepted: true, sequence: "9007199254740993" });
  expect(events).toStrictEqual(["begin", "retain", "receipt"]);
  expect(ports.begin).toHaveBeenCalledWith({
    requestId: expect.stringMatching(/^dlq_[a-f0-9]{64}$/u),
    queuedAt: "2026-09-16T00:00:00.000Z",
  });
});
test("unsupported body remains pending after begin without calling retention or completion", async () => {
  const ports = {
    begin: vi.fn<ArchiveAttemptPorts["begin"]>().mockResolvedValue(undefined),
    accept: vi.fn<ArchiveAttemptPorts["accept"]>(),
    recordReceipt: vi.fn<ArchiveAttemptPorts["recordReceipt"]>(),
  } satisfies ArchiveAttemptPorts;
  await expect(
    retainTrackedDeadLetter(
      { id: "bad-body", timestamp: new Date("2026-09-16"), body: { missing: undefined } },
      ports,
    ),
  ).rejects.toThrow("lossless");
  expect(ports.begin).toHaveBeenCalledOnce();
  expect(ports.accept).not.toHaveBeenCalled();
  expect(ports.recordReceipt).not.toHaveBeenCalled();
});
test("a failed begin does not perform archival", async () => {
  const ports = {
    begin: vi
      .fn<ArchiveAttemptPorts["begin"]>()
      .mockRejectedValue(new Error("journal unavailable")),
    accept: vi.fn<ArchiveAttemptPorts["accept"]>(),
    recordReceipt: vi.fn<ArchiveAttemptPorts["recordReceipt"]>(),
  } satisfies ArchiveAttemptPorts;
  await expect(
    retainTrackedDeadLetter({ id: "job", timestamp: new Date("2026-09-16"), body: {} }, ports),
  ).rejects.toThrow("journal unavailable");
  expect(ports.accept).not.toHaveBeenCalled();
});
test("failed retention never records completion", async () => {
  const ports = {
    begin: vi.fn<ArchiveAttemptPorts["begin"]>().mockResolvedValue(undefined),
    accept: vi.fn<ArchiveAttemptPorts["accept"]>().mockRejectedValue(new Error("retention failed")),
    recordReceipt: vi.fn<ArchiveAttemptPorts["recordReceipt"]>(),
  } satisfies ArchiveAttemptPorts;
  await expect(
    retainTrackedDeadLetter({ id: "job", timestamp: new Date("2026-09-16"), body: {} }, ports),
  ).rejects.toThrow("retention failed");
  expect(ports.recordReceipt).not.toHaveBeenCalled();
});
test("lost completion acknowledgement propagates for idempotent retry, not Queue acknowledgement", async () => {
  const ports = {
    begin: vi.fn<ArchiveAttemptPorts["begin"]>().mockResolvedValue(undefined),
    accept: vi.fn<ArchiveAttemptPorts["accept"]>(async (envelope) => {
      const pointer = prepareIngestionEnvelope(envelope).pointer;
      return {
        source: pointer.source,
        requestId: pointer.requestId,
        digest: pointer.digest,
        sequence: "1",
        accepted: true,
      };
    }),
    recordReceipt: vi
      .fn<ArchiveAttemptPorts["recordReceipt"]>()
      .mockRejectedValue(new Error("receipt acknowledgement lost")),
  } satisfies ArchiveAttemptPorts;
  await expect(
    retainTrackedDeadLetter({ id: "job", timestamp: new Date("2026-09-16"), body: {} }, ports),
  ).rejects.toThrow("acknowledgement lost");
  expect(ports.recordReceipt).toHaveBeenCalledOnce();
});
