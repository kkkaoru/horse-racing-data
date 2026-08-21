// Run with bun. Durable incident acknowledgement, resend, outbox, and recovery state.

import type { Env } from "./types";

const STATE_PREFIX = "incident-state:";
const ID_INDEX_PREFIX = "incident-id:";
const OUTBOX_PREFIX = "incident-outbox:";
const STATE_TTL_SECONDS = 7 * 24 * 60 * 60;

export interface IncidentState {
  incidentId: string;
  signalKey: string;
  openedAt: string;
  lastSentAt: string | null;
  sendCount: number;
  acknowledgedAt: string | null;
  closedAt: string | null;
  lastSeverity: "critical" | "warning" | null;
  lastStage: string | null;
}

const stateKey = (signalKey: string): string => `${STATE_PREFIX}${signalKey}`;
const idIndexKey = (incidentId: string): string => `${ID_INDEX_PREFIX}${incidentId}`;
export const incidentOutboxKey = (incidentId: string): string => `${OUTBOX_PREFIX}${incidentId}`;

export const getIncident = async (env: Env, signalKey: string): Promise<IncidentState | null> =>
  env.STATE_KV.get<IncidentState>(stateKey(signalKey), "json");

const listOpenIncidentPage = async (
  env: Env,
  prefix: string,
  cursor?: string,
): Promise<IncidentState[]> => {
  const page = await env.STATE_KV.list(cursor === undefined ? { prefix } : { cursor, prefix });
  const states = await Promise.all(
    page.keys.map((key) => env.STATE_KV.get<IncidentState>(key.name, "json")),
  );
  const openStates = states.filter(
    (state): state is IncidentState => state !== null && state.closedAt === null,
  );
  if (page.list_complete) return openStates;
  return [...openStates, ...(await listOpenIncidentPage(env, prefix, page.cursor))];
};

export const listOpenIncidentsBySignalPrefix = async (
  env: Env,
  signalPrefix: string,
): Promise<IncidentState[]> => listOpenIncidentPage(env, stateKey(signalPrefix));

export const putIncident = async (env: Env, state: IncidentState): Promise<void> => {
  await Promise.all([
    env.STATE_KV.put(stateKey(state.signalKey), JSON.stringify(state), {
      expirationTtl: STATE_TTL_SECONDS,
    }),
    env.STATE_KV.put(idIndexKey(state.incidentId), state.signalKey, {
      expirationTtl: STATE_TTL_SECONDS,
    }),
  ]);
};

export const openIncident = async (
  env: Env,
  signalKey: string,
  now: Date,
): Promise<IncidentState> => {
  const existing = await getIncident(env, signalKey);
  if (existing !== null && existing.closedAt === null) return existing;
  const state: IncidentState = {
    acknowledgedAt: null,
    closedAt: null,
    incidentId: crypto.randomUUID(),
    lastSentAt: null,
    lastSeverity: null,
    lastStage: null,
    openedAt: now.toISOString(),
    sendCount: 0,
    signalKey,
  };
  await putIncident(env, state);
  return state;
};

export const acknowledgeIncident = async (
  env: Env,
  incidentId: string,
  now: Date,
): Promise<IncidentState | null> => {
  const signalKey = await env.STATE_KV.get(idIndexKey(incidentId));
  if (signalKey === null) return null;
  const state = await getIncident(env, signalKey);
  if (state === null || state.incidentId !== incidentId || state.closedAt !== null) return null;
  const acknowledged = { ...state, acknowledgedAt: now.toISOString() };
  await putIncident(env, acknowledged);
  return acknowledged;
};

export const closeIncident = async (
  env: Env,
  state: IncidentState,
  now: Date,
): Promise<IncidentState> => {
  const closed = { ...state, closedAt: now.toISOString() };
  await putIncident(env, closed);
  await env.STATE_KV.delete(incidentOutboxKey(state.incidentId));
  return closed;
};
