// Run with bun.
import type { MessageBatch, Message } from "@cloudflare/workers-types";

import { notifyCustom, notifyDiscord, notifySlack } from "./notifiers";
import type { AlertMessage, Env } from "./types";

interface RunQueueInput {
  batch: MessageBatch<AlertMessage>;
  env: Env;
}

interface DispatchMessageInput {
  message: Message<AlertMessage>;
  env: Env;
}

const collectNotifierPromises = (input: DispatchMessageInput): Promise<void>[] => {
  const promises: Promise<void>[] = [];
  const discordUrl = input.env.DISCORD_ALERT_WEBHOOK_URL;
  const slackUrl = input.env.SLACK_ALERT_WEBHOOK_URL;
  const customUrl = input.env.CUSTOM_ALERT_WEBHOOK_URL;
  if (discordUrl !== undefined && discordUrl !== "") {
    promises.push(notifyDiscord({ webhookUrl: discordUrl, message: input.message.body }));
  }
  if (slackUrl !== undefined && slackUrl !== "") {
    promises.push(notifySlack({ webhookUrl: slackUrl, message: input.message.body }));
  }
  if (customUrl !== undefined && customUrl !== "") {
    promises.push(notifyCustom({ webhookUrl: customUrl, message: input.message.body }));
  }
  return promises;
};

const dispatchMessage = async (input: DispatchMessageInput): Promise<void> => {
  const promises = collectNotifierPromises(input);
  try {
    await Promise.all(promises);
    input.message.ack();
  } catch (error) {
    console.error("pipeline-health-monitor notifier failed", error);
    input.message.retry();
  }
};

export const runQueue = async (input: RunQueueInput): Promise<void> => {
  await Promise.all(
    input.batch.messages.map((message) => dispatchMessage({ env: input.env, message })),
  );
};
