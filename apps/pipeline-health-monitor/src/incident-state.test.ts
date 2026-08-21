// Run with bun. Tests paginated incident-state lookup for lifecycle reconciliation.

import { expect, it, vi } from "vitest";

import { listOpenIncidentsBySignalPrefix, type IncidentState } from "./incident-state";
import type { Env } from "./types";

const openState: IncidentState = {
  acknowledgedAt: null,
  closedAt: null,
  incidentId: "open-id",
  lastSentAt: "2026-08-20T00:00:00Z",
  lastSeverity: "critical",
  lastStage: "post",
  openedAt: "2026-08-20T00:00:00Z",
  sendCount: 1,
  signalKey: "finish-position-readiness:20260820:nar:30:11",
};

it("lists every page and returns only open incidents under the signal prefix", async () => {
  const closedState: IncidentState = {
    ...openState,
    closedAt: "2026-08-20T01:00:00Z",
    incidentId: "closed-id",
    signalKey: "finish-position-readiness:20260820:nar:30:12",
  };
  const values = new Map<string, IncidentState>([
    ["incident-state:finish-position-readiness:20260820:nar:30:11", openState],
    ["incident-state:finish-position-readiness:20260820:nar:30:12", closedState],
  ]);
  const listMock = vi
    .fn()
    .mockResolvedValueOnce({
      cursor: "next-page",
      keys: [
        { name: "incident-state:finish-position-readiness:20260820:nar:30:11" },
        { name: "incident-state:finish-position-readiness:missing" },
      ],
      list_complete: false,
    })
    .mockResolvedValueOnce({
      keys: [{ name: "incident-state:finish-position-readiness:20260820:nar:30:12" }],
      list_complete: true,
    });
  const env = {
    STATE_KV: {
      get: vi.fn(async (key: string) => values.get(key) ?? null),
      list: listMock,
    },
  } as unknown as Env;
  await expect(
    listOpenIncidentsBySignalPrefix(env, "finish-position-readiness:"),
  ).resolves.toStrictEqual([openState]);
  expect(listMock).toHaveBeenNthCalledWith(1, {
    prefix: "incident-state:finish-position-readiness:",
  });
  expect(listMock).toHaveBeenNthCalledWith(2, {
    cursor: "next-page",
    prefix: "incident-state:finish-position-readiness:",
  });
});
