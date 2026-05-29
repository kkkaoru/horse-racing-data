import { NextResponse } from "next/server";

import { safeGetCloudflareEnv } from "../../../../lib/cloudflare-context.server";

export const dynamic = "force-dynamic";

const TONE_PROMPT_KEY = "race-ai:tone-prompt";
const MAX_TONE_PROMPT_LENGTH = 4_000;

interface TonePromptRecord {
  prompt: string;
  schemaVersion: 1;
  updatedAt: string;
}

type TonePromptStorage = "cloudflare-kv" | "unavailable";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const normalizePrompt = (prompt: string): string => prompt.trim();

const parseTonePromptRecord = (value: unknown): TonePromptRecord | null => {
  if (!isRecord(value) || typeof value.prompt !== "string") {
    return null;
  }
  return {
    prompt: normalizePrompt(value.prompt),
    schemaVersion: 1,
    updatedAt: typeof value.updatedAt === "string" ? value.updatedAt : "",
  };
};

const readTonePromptRecord = async (): Promise<{
  record: TonePromptRecord | null;
  storage: TonePromptStorage;
}> => {
  const env = await safeGetCloudflareEnv();
  const kv = env?.PADDOCK_STATE_KV;
  if (kv) {
    const record = parseTonePromptRecord(
      await kv.get<Partial<TonePromptRecord>>(TONE_PROMPT_KEY, { type: "json" }),
    );
    return { record, storage: "cloudflare-kv" };
  }
  return { record: null, storage: "unavailable" };
};

const writeTonePromptRecord = async (record: TonePromptRecord): Promise<TonePromptStorage> => {
  const env = await safeGetCloudflareEnv();
  const kv = env?.PADDOCK_STATE_KV;
  if (kv) {
    await kv.put(TONE_PROMPT_KEY, JSON.stringify(record));
    return "cloudflare-kv";
  }
  return "unavailable";
};

const tonePromptResponse = (record: TonePromptRecord | null, storage: TonePromptStorage) =>
  NextResponse.json({
    prompt: record?.prompt ?? "",
    storage,
    updatedAt: record?.updatedAt || null,
  });

export async function GET() {
  const { record, storage } = await readTonePromptRecord();
  return tonePromptResponse(record, storage);
}

export async function PUT(request: Request) {
  let payload: unknown;
  try {
    payload = await request.json();
  } catch {
    return NextResponse.json({ error: "invalid_json" }, { status: 400 });
  }
  if (!isRecord(payload) || typeof payload.prompt !== "string") {
    return NextResponse.json({ error: "invalid_prompt" }, { status: 400 });
  }

  const prompt = normalizePrompt(payload.prompt);
  if (prompt.length > MAX_TONE_PROMPT_LENGTH) {
    return NextResponse.json({ error: "prompt_too_long" }, { status: 400 });
  }

  const record: TonePromptRecord = {
    prompt,
    schemaVersion: 1,
    updatedAt: new Date().toISOString(),
  };
  const storage = await writeTonePromptRecord(record);
  if (storage === "unavailable") {
    return NextResponse.json({ error: "storage_unavailable" }, { status: 503 });
  }
  return tonePromptResponse(record, storage);
}
