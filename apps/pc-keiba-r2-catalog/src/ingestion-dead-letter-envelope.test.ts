// Runs with bun via Vitest; pure envelope preparation does not import the service or Queue handler.
import { expect, test } from "vitest";
import { deadLetterEnvelope, deadLetterRequestId } from "./ingestion-dead-letter-envelope";

test("preserves JSON primitive arrays without service bindings", () => {
  const envelope = deadLetterEnvelope({
    id: "array",
    timestamp: new Date("2026-09-16"),
    body: [1, true, null, "競馬"],
  });
  expect(JSON.parse(envelope.payload)).toStrictEqual({
    formatVersion: 1,
    kind: "dead-letter",
    queue: "sync-realtime-data-hot-ingestion-dlq",
    messageId: "array",
    queuedAt: "2026-09-16T00:00:00.000Z",
    body: [1, true, null, "競馬"],
  });
  expect(envelope.requestId === deadLetterRequestId("array")).toBe(true);
});
test("different received message identities cannot silently share a receipt key", () => {
  expect(deadLetterRequestId("first") === deadLetterRequestId("second")).toBe(false);
});
