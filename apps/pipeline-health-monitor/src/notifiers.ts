// Run with bun.
import type { AlertMessage, AlertSeverity } from "./types";

interface NotifierInput {
  webhookUrl: string;
  message: AlertMessage;
}

interface FetchWithTimeoutInput {
  url: string;
  init: RequestInit;
  timeoutMs: number;
}

const FETCH_TIMEOUT_MS = 10_000;

// Discord embed colors (decimal RGB) per severity.
const DISCORD_COLOR_BY_SEVERITY: Record<AlertSeverity, number> = {
  warning: 16776960,
  critical: 15158332,
  recovery: 3066993,
};

const JSON_CONTENT_TYPE = "application/json";
const POST_METHOD = "POST";

export const fetchWithTimeout = async (input: FetchWithTimeoutInput): Promise<Response> => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), input.timeoutMs);
  try {
    return await fetch(input.url, { ...input.init, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
};

const ensureOkResponse = (response: Response, channel: string): void => {
  if (!response.ok) {
    throw new Error(`${channel} webhook returned status ${response.status}`);
  }
};

const buildDiscordBody = (message: AlertMessage): string =>
  JSON.stringify({
    embeds: [
      {
        title: message.title,
        description: message.description,
        color: DISCORD_COLOR_BY_SEVERITY[message.severity],
        timestamp: message.timestampJst,
        fields: message.fields.map((field) => ({
          name: field.name,
          value: field.value,
          inline: true,
        })),
      },
    ],
  });

export const notifyDiscord = async (input: NotifierInput): Promise<void> => {
  const response = await fetchWithTimeout({
    url: input.webhookUrl,
    init: {
      method: POST_METHOD,
      headers: { "content-type": JSON_CONTENT_TYPE },
      body: buildDiscordBody(input.message),
    },
    timeoutMs: FETCH_TIMEOUT_MS,
  });
  ensureOkResponse(response, "discord");
};

const buildSlackBody = (message: AlertMessage): string =>
  JSON.stringify({
    blocks: [
      {
        type: "header",
        text: { type: "plain_text", text: message.title },
      },
      {
        type: "section",
        text: { type: "mrkdwn", text: message.description },
        fields: message.fields.map((field) => ({
          type: "mrkdwn",
          text: `*${field.name}*\n${field.value}`,
        })),
      },
    ],
  });

export const notifySlack = async (input: NotifierInput): Promise<void> => {
  const response = await fetchWithTimeout({
    url: input.webhookUrl,
    init: {
      method: POST_METHOD,
      headers: { "content-type": JSON_CONTENT_TYPE },
      body: buildSlackBody(input.message),
    },
    timeoutMs: FETCH_TIMEOUT_MS,
  });
  ensureOkResponse(response, "slack");
};

export const notifyCustom = async (input: NotifierInput): Promise<void> => {
  const response = await fetchWithTimeout({
    url: input.webhookUrl,
    init: {
      method: POST_METHOD,
      headers: { "content-type": JSON_CONTENT_TYPE },
      body: JSON.stringify(input.message),
    },
    timeoutMs: FETCH_TIMEOUT_MS,
  });
  ensureOkResponse(response, "custom");
};
