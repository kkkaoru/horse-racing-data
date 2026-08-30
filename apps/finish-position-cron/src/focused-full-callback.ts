// Run with bun. Scoped completion callback for detached focused-full Containers.

import {
  createFocusedFullWatchTickMessage,
  parseFocusedFullWatchPayloadValue,
  sendFocusedFullWatchMessageDurably,
  type ValidatedFocusedFullWatchPayload,
} from "./focused-full-watch";
import type { Env } from "./types";

export const FOCUSED_FULL_CALLBACK_PATH: string = "/api/internal/focused-full-completion-callback";
export const FOCUSED_FULL_CALLBACK_HEADER: string = "x-focused-full-completion-callback";

const PAYLOAD_PARAM: string = "payload";
const SIGNATURE_PARAM: string = "signature";
const HTTP_ACCEPTED: number = 202;
const HTTP_BAD_REQUEST: number = 400;
const HTTP_UNAUTHORIZED: number = 401;
const encoder: TextEncoder = new TextEncoder();
const decoder: TextDecoder = new TextDecoder();

interface CallbackDependencies {
  now: () => number;
  send: (env: Env, message: ReturnType<typeof createFocusedFullWatchTickMessage>) => Promise<void>;
}

const defaultDependencies: CallbackDependencies = {
  now: Date.now,
  send: sendFocusedFullWatchMessageDurably,
};

const bytesToBase64Url = (bytes: Uint8Array): string => {
  let binary = "";
  for (const value of bytes) binary += String.fromCharCode(value);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/u, "");
};

const base64UrlToBytes = (value: string): Uint8Array | undefined => {
  if (!/^[A-Za-z0-9_-]+$/u.test(value)) return undefined;
  const padded = value
    .replaceAll("-", "+")
    .replaceAll("_", "/")
    .padEnd(Math.ceil(value.length / 4) * 4, "=");
  try {
    return Uint8Array.from(atob(padded), (character) => character.charCodeAt(0));
  } catch {
    return undefined;
  }
};

const signingKey = async (secret: string): Promise<CryptoKey> =>
  crypto.subtle.importKey("raw", encoder.encode(secret), { hash: "SHA-256", name: "HMAC" }, false, [
    "sign",
    "verify",
  ]);

const signPayload = async (payload: string, secret: string): Promise<string> =>
  bytesToBase64Url(
    new Uint8Array(
      await crypto.subtle.sign("HMAC", await signingKey(secret), encoder.encode(payload)),
    ),
  );

const verifyPayload = async (
  payload: string,
  signature: string,
  secret: string,
): Promise<boolean> => {
  const signatureBytes = base64UrlToBytes(signature);
  if (signatureBytes === undefined) return false;
  return crypto.subtle.verify(
    "HMAC",
    await signingKey(secret),
    signatureBytes,
    encoder.encode(payload),
  );
};

export const buildFocusedFullCompletionCallbackUrl = async (
  env: Pick<Env, "FOCUSED_FULL_CALLBACK_URL" | "TRIGGER_TOKEN">,
  payload: ValidatedFocusedFullWatchPayload,
): Promise<string | undefined> => {
  const callbackUrl = env.FOCUSED_FULL_CALLBACK_URL;
  if (callbackUrl === undefined || callbackUrl.length === 0) return undefined;
  const encodedPayload = bytesToBase64Url(encoder.encode(JSON.stringify(payload)));
  const url = new URL(callbackUrl);
  url.searchParams.set(PAYLOAD_PARAM, encodedPayload);
  url.searchParams.set(SIGNATURE_PARAM, await signPayload(encodedPayload, env.TRIGGER_TOKEN));
  return url.toString();
};

const decodePayload = (encodedPayload: string): ValidatedFocusedFullWatchPayload | undefined => {
  const bytes = base64UrlToBytes(encodedPayload);
  if (bytes === undefined) return undefined;
  try {
    return parseFocusedFullWatchPayloadValue(JSON.parse(decoder.decode(bytes)) as unknown);
  } catch {
    return undefined;
  }
};

export const handleFocusedFullCompletionCallback = async (
  request: Request,
  env: Env,
  dependencies: CallbackDependencies = defaultDependencies,
): Promise<Response> => {
  const url = new URL(request.url);
  const encodedPayload = url.searchParams.get(PAYLOAD_PARAM);
  const signature = url.searchParams.get(SIGNATURE_PARAM);
  if (encodedPayload === null || signature === null) {
    return Response.json({ error: "invalid callback", ok: false }, { status: HTTP_BAD_REQUEST });
  }
  if (!(await verifyPayload(encodedPayload, signature, env.TRIGGER_TOKEN))) {
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const payload = decodePayload(encodedPayload);
  if (payload === undefined) {
    return Response.json({ error: "invalid callback", ok: false }, { status: HTTP_BAD_REQUEST });
  }
  await dependencies.send(env, createFocusedFullWatchTickMessage(payload, dependencies.now()));
  console.log(
    `[predict-worker] focused-full completion callback accepted watchId=${payload.watchId} doName=${payload.doName}`,
  );
  return Response.json({ ok: true }, { status: HTTP_ACCEPTED });
};
