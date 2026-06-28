// Run with: bun run --filter pipeline-health-monitor test
import { afterEach, expect, it, vi } from "vitest";

import { fetchWithTimeout, notifyCustom, notifyDiscord, notifySlack } from "./notifiers";
import type { AlertMessage } from "./types";

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

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

interface CapturedCall {
  url: string;
  init: RequestInit;
}

const buildCapturingFetch = (
  captured: CapturedCall[],
  responseFactory: () => Response,
): ReturnType<typeof vi.fn> =>
  vi.fn(async (url: string, init: RequestInit) => {
    captured.push({ url, init });
    return responseFactory();
  });

it("notifyDiscord posts an embed payload with the severity color", async () => {
  const captured: CapturedCall[] = [];
  const fetchMock = buildCapturingFetch(captured, () => new Response("", { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);
  await notifyDiscord({ webhookUrl: "https://discord.test/hook", message: SAMPLE_MESSAGE });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(captured[0]?.url).toBe("https://discord.test/hook");
  expect(captured[0]?.init.method).toBe("POST");
  const body = JSON.parse(captured[0]?.init.body as string) as {
    embeds: { color: number; title: string }[];
  };
  expect(body.embeds[0]?.color).toBe(15158332);
  expect(body.embeds[0]?.title).toBe("[CRITICAL] fetch-results-staleness");
});

it("notifyDiscord throws when the response is not ok", async () => {
  const fetchMock = vi.fn(async () => new Response("err", { status: 500 }));
  vi.stubGlobal("fetch", fetchMock);
  await expect(
    notifyDiscord({ webhookUrl: "https://discord.test/hook", message: SAMPLE_MESSAGE }),
  ).rejects.toThrow("discord webhook returned status 500");
});

it("notifySlack posts a Block Kit payload with header and section blocks", async () => {
  const captured: CapturedCall[] = [];
  const fetchMock = buildCapturingFetch(captured, () => new Response("", { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);
  await notifySlack({ webhookUrl: "https://hooks.slack.test/x", message: SAMPLE_MESSAGE });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  const body = JSON.parse(captured[0]?.init.body as string) as { blocks: { type: string }[] };
  expect(body.blocks[0]?.type).toBe("header");
  expect(body.blocks[1]?.type).toBe("section");
});

it("notifySlack throws when the response is not ok", async () => {
  const fetchMock = vi.fn(async () => new Response("err", { status: 502 }));
  vi.stubGlobal("fetch", fetchMock);
  await expect(
    notifySlack({ webhookUrl: "https://hooks.slack.test/x", message: SAMPLE_MESSAGE }),
  ).rejects.toThrow("slack webhook returned status 502");
});

it("notifyCustom posts the raw AlertMessage JSON body", async () => {
  const captured: CapturedCall[] = [];
  const fetchMock = buildCapturingFetch(captured, () => new Response("", { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);
  await notifyCustom({ webhookUrl: "https://custom.test/x", message: SAMPLE_MESSAGE });
  const body = JSON.parse(captured[0]?.init.body as string) as AlertMessage;
  expect(body).toStrictEqual(SAMPLE_MESSAGE);
});

it("notifyCustom throws when the response is not ok", async () => {
  const fetchMock = vi.fn(async () => new Response("err", { status: 400 }));
  vi.stubGlobal("fetch", fetchMock);
  await expect(
    notifyCustom({ webhookUrl: "https://custom.test/x", message: SAMPLE_MESSAGE }),
  ).rejects.toThrow("custom webhook returned status 400");
});

it("fetchWithTimeout resolves with the fetch response when fetch completes before the timeout", async () => {
  const fetchMock = vi.fn(async () => new Response("ok", { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);
  const response = await fetchWithTimeout({
    url: "https://example.test/x",
    init: { method: "GET" },
    timeoutMs: 1000,
  });
  expect(response.status).toBe(200);
});

it("fetchWithTimeout aborts and rejects when the fetch never resolves before the timeout", async () => {
  const fetchMock = vi.fn(
    (_input: RequestInfo | URL, init?: RequestInit) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => {
          reject(new Error("aborted"));
        });
      }),
  );
  vi.stubGlobal("fetch", fetchMock);
  await expect(
    fetchWithTimeout({
      url: "https://example.test/never",
      init: { method: "GET" },
      timeoutMs: 5,
    }),
  ).rejects.toThrow("aborted");
});
