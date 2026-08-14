import { beforeEach, expect, it, vi } from "vitest";

vi.mock("./notifiers", () => ({
  notifyCustom: vi.fn(async () => undefined),
  notifyDiscord: vi.fn(async () => undefined),
  notifySlack: vi.fn(async () => undefined),
}));

import { acknowledgeIncident, getIncident } from "./incident-state";
import { processIncidentSignal, shouldSendIncident, type IncidentSignal } from "./incident-engine";
import { notifyDiscord } from "./notifiers";
import type { Env } from "./types";

const makeEnv = (configured = true): { env: Env; store: Map<string, string> } => {
  const store = new Map<string, string>();
  const env = {
    DISCORD_ALERT_WEBHOOK_URL: configured ? "https://discord.example/webhook" : undefined,
    STATE_KV: {
      delete: vi.fn(async (key: string) => {
        store.delete(key);
      }),
      get: vi.fn(async (key: string, type?: string) => {
        const value = store.get(key) ?? null;
        return type === "json" && value !== null ? JSON.parse(value) : value;
      }),
      put: vi.fn(async (key: string, value: string) => {
        store.set(key, value);
      }),
    },
  } as unknown as Env;
  return { env, store };
};

const failingSignal = (stage = "T-60"): IncidentSignal => ({
  description: "2 predictions missing",
  fields: [{ name: "Coverage", value: "8/10" }],
  key: "readiness:jra:05:01",
  ok: false,
  severity: "critical",
  stage,
  title: "predictions incomplete",
});

beforeEach(() => {
  vi.clearAllMocks();
});

it("sends the initial critical alert through direct Discord after persisting outbox", async () => {
  const { env, store } = makeEnv();
  const now = new Date("2026-08-15T00:00:00Z");
  await processIncidentSignal(env, failingSignal(), now);
  expect(notifyDiscord).toHaveBeenCalledTimes(1);
  const state = await getIncident(env, failingSignal().key);
  expect(state?.sendCount).toBe(1);
  expect(state?.lastStage).toBe("T-60");
  expect([...store.keys()].some((key) => key.startsWith("incident-outbox:"))).toBe(false);
});

it("resends unacknowledged incidents at 10 and 30 minutes then hourly", async () => {
  const { env } = makeEnv();
  const opened = new Date("2026-08-15T00:00:00Z");
  await processIncidentSignal(env, failingSignal(), opened);
  await processIncidentSignal(env, failingSignal(), new Date("2026-08-15T00:09:00Z"));
  await processIncidentSignal(env, failingSignal(), new Date("2026-08-15T00:10:00Z"));
  await processIncidentSignal(env, failingSignal(), new Date("2026-08-15T00:30:00Z"));
  await processIncidentSignal(env, failingSignal(), new Date("2026-08-15T01:29:00Z"));
  await processIncidentSignal(env, failingSignal(), new Date("2026-08-15T01:30:00Z"));
  expect(notifyDiscord).toHaveBeenCalledTimes(4);
});

it("sends immediately when a warning escalates to a critical deadline", async () => {
  const { env } = makeEnv();
  const warning = { ...failingSignal("T-120"), severity: "warning" as const };
  await processIncidentSignal(env, warning, new Date("2026-08-15T00:00:00Z"));
  await processIncidentSignal(env, failingSignal("T-60"), new Date("2026-08-15T00:05:00Z"));
  expect(notifyDiscord).toHaveBeenCalledTimes(2);
});

it("acknowledgement suppresses the 10/30 minute resends but keeps hourly reminders", async () => {
  const { env } = makeEnv();
  await processIncidentSignal(env, failingSignal(), new Date("2026-08-15T00:00:00Z"));
  const state = await getIncident(env, failingSignal().key);
  expect(state).not.toBeNull();
  await acknowledgeIncident(env, state?.incidentId ?? "", new Date("2026-08-15T00:05:00Z"));
  await processIncidentSignal(env, failingSignal(), new Date("2026-08-15T00:30:00Z"));
  await processIncidentSignal(env, failingSignal(), new Date("2026-08-15T01:00:00Z"));
  expect(notifyDiscord).toHaveBeenCalledTimes(2);
});

it("sends one recovery, closes the incident, and ignores later healthy ticks", async () => {
  const { env } = makeEnv();
  await processIncidentSignal(env, failingSignal(), new Date("2026-08-15T00:00:00Z"));
  const healthy = { ...failingSignal(), ok: true };
  await processIncidentSignal(env, healthy, new Date("2026-08-15T00:05:00Z"));
  await processIncidentSignal(env, healthy, new Date("2026-08-15T00:10:00Z"));
  expect(notifyDiscord).toHaveBeenCalledTimes(2);
  expect((await getIncident(env, failingSignal().key))?.closedAt).toBe("2026-08-15T00:05:00.000Z");
});

it("keeps the outbox pending when no direct notifier is configured", async () => {
  const { env, store } = makeEnv(false);
  await expect(
    processIncidentSignal(env, failingSignal(), new Date("2026-08-15T00:00:00Z")),
  ).rejects.toThrow("no direct incident notifier configured");
  expect([...store.keys()].some((key) => key.startsWith("incident-outbox:"))).toBe(true);
});

it("evaluates an unsent state as due", () => {
  expect(
    shouldSendIncident(
      {
        acknowledgedAt: null,
        closedAt: null,
        incidentId: "id",
        lastSentAt: null,
        lastSeverity: null,
        lastStage: null,
        openedAt: "2026-08-15T00:00:00.000Z",
        sendCount: 0,
        signalKey: "key",
      },
      failingSignal(),
      new Date("2026-08-15T00:00:00Z"),
    ),
  ).toBe(true);
});
