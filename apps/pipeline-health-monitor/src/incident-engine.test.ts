import { beforeEach, expect, it, vi } from "vitest";

vi.mock("./notifiers", () => ({
  notifyCustom: vi.fn(async () => undefined),
  notifyDiscord: vi.fn(async () => undefined),
  notifySlack: vi.fn(async () => undefined),
}));

import { acknowledgeIncident, getIncident } from "./incident-state";
import {
  drainIncidentOutbox,
  processIncidentSignal,
  sendDailyMonitorHeartbeat,
  shouldSendIncident,
  terminalCloseIncident,
  type IncidentSignal,
} from "./incident-engine";
import { notifyCustom, notifyDiscord, notifySlack } from "./notifiers";
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
      list: vi.fn(async (options: { prefix?: string }) => ({
        keys: [...store.keys()]
          .filter((key) => key.startsWith(options.prefix ?? ""))
          .map((name) => ({ name })),
        list_complete: true,
      })),
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
  vi.mocked(notifyDiscord).mockResolvedValue(undefined);
  vi.mocked(notifySlack).mockResolvedValue(undefined);
  vi.mocked(notifyCustom).mockResolvedValue(undefined);
});

it("sends the initial critical alert through direct Discord after persisting outbox", async () => {
  const { env, store } = makeEnv();
  const opened = new Date("2026-08-15T00:00:00Z");
  const now = new Date("2026-08-18T02:04:05Z");
  await processIncidentSignal(env, failingSignal(), opened);
  await processIncidentSignal(env, failingSignal(), now);
  expect(notifyDiscord).toHaveBeenCalledTimes(2);
  const message = vi.mocked(notifyDiscord).mock.calls[1]?.[0].message;
  expect(message?.fields.map((field) => field.name)).toStrictEqual([
    "Incident ID",
    "First detected (JST)",
    "Duration",
    "Action",
    "Stage",
    "Coverage",
  ]);
  expect(message?.fields.find((field) => field.name === "First detected (JST)")?.value).toBe(
    "2026-08-15T09:00:00+09:00",
  );
  expect(message?.fields.find((field) => field.name === "Duration")?.value).toBe("3d 2h 4m 5s");
  expect(message?.fields.find((field) => field.name === "Action")?.value).toBe(
    "Immediately inspect delivery_paused; if true run `bunx wrangler queues resume-delivery finish-position-predict-queue`, then verify canary consumption and prediction rows before acknowledging this incident.",
  );
  const state = await getIncident(env, failingSignal().key);
  expect(state?.sendCount).toBe(2);
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

it("does not resend an advisory warning but sends immediately at the critical deadline", async () => {
  const { env } = makeEnv();
  const warning = { ...failingSignal("T-120"), severity: "warning" as const };
  await processIncidentSignal(env, warning, new Date("2026-08-15T00:00:00Z"));
  await processIncidentSignal(env, warning, new Date("2026-08-15T00:30:00Z"));
  expect(notifyDiscord).toHaveBeenCalledTimes(1);
  await processIncidentSignal(env, failingSignal("T-60"), new Date("2026-08-15T00:35:00Z"));
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
  const recovery = vi.mocked(notifyDiscord).mock.calls[1]?.[0].message;
  expect(recovery?.severity).toBe("recovery");
  expect(recovery?.fields.find((field) => field.name === "Action")?.value).toBe(
    "Verify prediction coverage and queue delivery are restored; take no further action unless the incident recurs.",
  );
  expect((await getIncident(env, failingSignal().key))?.closedAt).toBe("2026-08-15T00:05:00.000Z");
});

it("terminal-closes an unobservable incident without calling it recovered", async () => {
  const { env } = makeEnv();
  const now = new Date("2026-08-16T00:00:00Z");
  await processIncidentSignal(env, failingSignal(), new Date("2026-08-15T00:00:00Z"));
  const state = await getIncident(env, failingSignal().key);
  expect(state).not.toBeNull();
  if (state === null) return;
  await terminalCloseIncident(
    env,
    state,
    now,
    "The active-day observation window ended while predictions remained incomplete.",
  );
  expect(vi.mocked(notifyDiscord).mock.calls[1]?.[0].message.title).toBe(
    "[CLOSED UNRESOLVED] readiness:jra:05:01",
  );
  expect(vi.mocked(notifyDiscord).mock.calls[1]?.[0].message.description).toBe(
    "The active-day observation window ended while predictions remained incomplete.\nRunbook: apps/finish-position-cron/docs/prediction-readiness-monitor-design-2026-08-15.md",
  );
  const closed = await getIncident(env, failingSignal().key);
  expect(closed?.closedAt).toBe("2026-08-16T00:00:00.000Z");
  await terminalCloseIncident(env, closed ?? state, now, "ignored");
  expect(notifyDiscord).toHaveBeenCalledTimes(2);
});

it("keeps the outbox pending when no direct notifier is configured", async () => {
  const { env, store } = makeEnv(false);
  await expect(
    processIncidentSignal(env, failingSignal(), new Date("2026-08-15T00:00:00Z")),
  ).rejects.toThrow("incident delivery pending destinations=discord");
  expect([...store.keys()].some((key) => key.startsWith("incident-outbox:"))).toBe(true);
});

it("drains only the destination that failed and finalizes incident state", async () => {
  const { env, store } = makeEnv();
  env.SLACK_ALERT_WEBHOOK_URL = "https://slack.example/webhook";
  vi.mocked(notifySlack).mockRejectedValueOnce(new Error("slack unavailable"));
  await expect(
    processIncidentSignal(env, failingSignal(), new Date("2026-08-15T00:00:00Z")),
  ).rejects.toThrow("pending destinations=slack");
  expect(notifyDiscord).toHaveBeenCalledTimes(1);
  expect(notifySlack).toHaveBeenCalledTimes(1);
  expect([...store.keys()].some((key) => key.startsWith("incident-outbox:"))).toBe(true);
  await drainIncidentOutbox(env);
  expect(notifyDiscord).toHaveBeenCalledTimes(1);
  expect(notifySlack).toHaveBeenCalledTimes(2);
  expect((await getIncident(env, failingSignal().key))?.sendCount).toBe(1);
  expect([...store.keys()].some((key) => key.startsWith("incident-outbox:"))).toBe(false);
});

it("delivers to every configured direct destination", async () => {
  const { env } = makeEnv();
  env.SLACK_ALERT_WEBHOOK_URL = "https://slack.example/webhook";
  env.CUSTOM_ALERT_WEBHOOK_URL = "https://custom.example/webhook";
  await processIncidentSignal(env, failingSignal(), new Date("2026-08-15T00:00:00Z"));
  expect(notifyDiscord).toHaveBeenCalledTimes(1);
  expect(notifySlack).toHaveBeenCalledTimes(1);
  expect(notifyCustom).toHaveBeenCalledTimes(1);
});

it("discards legacy outbox payloads that cannot be retried safely", async () => {
  const { env, store } = makeEnv();
  store.set("incident-outbox:legacy", JSON.stringify({ checkName: "legacy" }));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  await drainIncidentOutbox(env);
  expect(store.has("incident-outbox:legacy")).toBe(false);
  expect(errorSpy).toHaveBeenCalledWith(
    "pipeline-health-monitor discarding unreadable legacy outbox",
    "incident-outbox:legacy",
  );
});

it("sends one daily healthy heartbeat and uses the next JST date after 15:00 UTC", async () => {
  const { env } = makeEnv();
  await sendDailyMonitorHeartbeat(env, new Date("2026-08-15T14:59:00Z"));
  await sendDailyMonitorHeartbeat(env, new Date("2026-08-15T15:00:00Z"));
  await sendDailyMonitorHeartbeat(env, new Date("2026-08-15T15:05:00Z"));
  expect(notifyDiscord).toHaveBeenCalledTimes(2);
  expect(vi.mocked(notifyDiscord).mock.calls[1]?.[0].message.fields).toContainEqual({
    name: "JST date",
    value: "2026-08-16",
  });
  expect(vi.mocked(notifyDiscord).mock.calls[1]?.[0].message.timestampJst).toBe(
    "2026-08-16T00:00:00+09:00",
  );
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
