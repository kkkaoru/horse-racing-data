// Run with: bun run --filter pipeline-health-monitor test
import { afterEach, expect, it, vi } from "vitest";

import { runQueue } from "./queue-handler";
import type { AlertMessage, Env } from "./types";

const SAMPLE_MESSAGE: AlertMessage = {
  checkName: "fetch-results-staleness",
  severity: "critical",
  title: "[CRITICAL] fetch-results-staleness",
  description: "exceeded freshness threshold (value=45, threshold=30)",
  fields: [
    { name: "Check", value: "fetch-results-staleness" },
    { name: "Value", value: "45" },
    { name: "Threshold", value: "30" },
    { name: "Failure Count", value: "3" },
  ],
  timestampJst: "2026-06-28T15:30:00+09:00",
};

interface MessageStubs {
  ack: ReturnType<typeof vi.fn>;
  retry: ReturnType<typeof vi.fn>;
}

const buildBatch = (stubs: MessageStubs) => ({
  messages: [
    {
      body: SAMPLE_MESSAGE,
      ack: stubs.ack,
      retry: stubs.retry,
    },
  ],
});

const buildEnv = (overrides: Partial<Env> = {}): Env =>
  ({
    DISCORD_ALERT_WEBHOOK_URL: undefined,
    SLACK_ALERT_WEBHOOK_URL: undefined,
    CUSTOM_ALERT_WEBHOOK_URL: undefined,
    ...overrides,
  }) as unknown as Env;

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

it("runQueue posts to all three webhooks when all are configured and then acks the message", async () => {
  const fetchMock = vi.fn(async () => new Response("", { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);
  const stubs: MessageStubs = { ack: vi.fn(), retry: vi.fn() };
  const env = buildEnv({
    DISCORD_ALERT_WEBHOOK_URL: "https://discord.test/hook",
    SLACK_ALERT_WEBHOOK_URL: "https://hooks.slack.test/x",
    CUSTOM_ALERT_WEBHOOK_URL: "https://custom.test/x",
  });
  await runQueue({ batch: buildBatch(stubs) as never, env });
  expect(fetchMock).toHaveBeenCalledTimes(3);
  expect(stubs.ack).toHaveBeenCalledTimes(1);
  expect(stubs.retry).not.toHaveBeenCalled();
});

it("runQueue posts only to Discord when only DISCORD_ALERT_WEBHOOK_URL is configured", async () => {
  const fetchMock = vi.fn(async () => new Response("", { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);
  const stubs: MessageStubs = { ack: vi.fn(), retry: vi.fn() };
  const env = buildEnv({ DISCORD_ALERT_WEBHOOK_URL: "https://discord.test/hook" });
  await runQueue({ batch: buildBatch(stubs) as never, env });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(stubs.ack).toHaveBeenCalledTimes(1);
});

it("runQueue posts only to Slack when only SLACK_ALERT_WEBHOOK_URL is configured", async () => {
  const fetchMock = vi.fn(async () => new Response("", { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);
  const stubs: MessageStubs = { ack: vi.fn(), retry: vi.fn() };
  const env = buildEnv({ SLACK_ALERT_WEBHOOK_URL: "https://hooks.slack.test/x" });
  await runQueue({ batch: buildBatch(stubs) as never, env });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(stubs.ack).toHaveBeenCalledTimes(1);
});

it("runQueue posts only to Custom when only CUSTOM_ALERT_WEBHOOK_URL is configured", async () => {
  const fetchMock = vi.fn(async () => new Response("", { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);
  const stubs: MessageStubs = { ack: vi.fn(), retry: vi.fn() };
  const env = buildEnv({ CUSTOM_ALERT_WEBHOOK_URL: "https://custom.test/x" });
  await runQueue({ batch: buildBatch(stubs) as never, env });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(stubs.ack).toHaveBeenCalledTimes(1);
});

it("runQueue acks (no notifiers called) when no webhook URLs are configured", async () => {
  const fetchMock = vi.fn(async () => new Response("", { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);
  const stubs: MessageStubs = { ack: vi.fn(), retry: vi.fn() };
  const env = buildEnv();
  await runQueue({ batch: buildBatch(stubs) as never, env });
  expect(fetchMock).not.toHaveBeenCalled();
  expect(stubs.ack).toHaveBeenCalledTimes(1);
});

it("runQueue ignores empty-string webhook URLs", async () => {
  const fetchMock = vi.fn(async () => new Response("", { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);
  const stubs: MessageStubs = { ack: vi.fn(), retry: vi.fn() };
  const env = buildEnv({
    DISCORD_ALERT_WEBHOOK_URL: "",
    SLACK_ALERT_WEBHOOK_URL: "",
    CUSTOM_ALERT_WEBHOOK_URL: "",
  });
  await runQueue({ batch: buildBatch(stubs) as never, env });
  expect(fetchMock).not.toHaveBeenCalled();
  expect(stubs.ack).toHaveBeenCalledTimes(1);
});

it("runQueue calls retry instead of ack when any webhook returns a non-ok status", async () => {
  const fetchMock = vi.fn(async () => new Response("err", { status: 500 }));
  vi.stubGlobal("fetch", fetchMock);
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const stubs: MessageStubs = { ack: vi.fn(), retry: vi.fn() };
  const env = buildEnv({ DISCORD_ALERT_WEBHOOK_URL: "https://discord.test/hook" });
  await runQueue({ batch: buildBatch(stubs) as never, env });
  expect(stubs.ack).not.toHaveBeenCalled();
  expect(stubs.retry).toHaveBeenCalledTimes(1);
  expect(errorSpy).toHaveBeenCalled();
});
