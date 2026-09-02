// Run with bun. Worker-native JVRTOpen using the official JV-Link 5.0.0 oracle contract.

import { splitJvRecords, type JvFetch } from "./acquisition";
import { randomCredentialSeeds, type CoreConfig } from "./protocol";
import { buildGateBodyInRust, buildRealtimeAuthorizationBodyInRust } from "./rust-core";

export interface RealtimeQuery {
  dataSpec: string;
  key: string;
}

export interface RealtimeResult {
  records: readonly Uint8Array[];
  status: number;
  transitions: readonly RealtimeTransition[];
}

export type RealtimeTransition =
  | "configured"
  | "session-encoded"
  | "authorized"
  | "gate-opened"
  | "payload-decoded";

const AUTH_URL: string = "http://authlab.jra-van.ne.jp/Browsing/JVServlet/";
const GATE_URL: string = "http://reallab.jra-van.ne.jp/Browsing/GateServlet/";
const FORM_CONTENT_TYPE: string = "application/x-www-form-urlencoded";
const ASCII_DECODER: TextDecoder = new TextDecoder("ascii", { fatal: true, ignoreBOM: false });
const ENVELOPE_BYTES: number = 18;
const DATA_SPEC_PATTERN: RegExp = /^[A-Z0-9]{4}$/;
const KEY_PATTERN: RegExp = /^[A-Z0-9]{8,32}$/;
const DIGITS_PATTERN: RegExp = /^\d+$/;

const inflateDeflate = async (compressed: Uint8Array): Promise<Uint8Array> => {
  const source = new ReadableStream<Uint8Array>({
    start(controller): void {
      controller.enqueue(compressed);
      controller.close();
    },
  });
  return new Uint8Array(
    await new Response(source.pipeThrough(new DecompressionStream("deflate"))).arrayBuffer(),
  );
};

const postForm = async (
  url: string,
  body: string,
  fetcher: JvFetch,
  softwareId: string = "UNKNOWN",
): Promise<Uint8Array> => {
  const response = await fetcher(url, {
    body,
    headers: {
      "Content-Type": FORM_CONTENT_TYPE,
      "User-Agent": `${softwareId}:`,
    },
    method: "POST",
    redirect: "manual",
  });
  if (!response.ok) throw new Error("JV realtime endpoint returned an HTTP error");
  return new Uint8Array(await response.arrayBuffer());
};

export const validateRealtimeQuery = (query: RealtimeQuery): RealtimeQuery => {
  if (!DATA_SPEC_PATTERN.test(query.dataSpec))
    throw new Error("dataSpec must be one four-character ASCII code");
  if (!KEY_PATTERN.test(query.key)) throw new Error("key has an invalid JV realtime shape");
  return query;
};

export const buildRealtimeAuthorizationBody = (
  config: CoreConfig,
  seeds: readonly [number, number],
): string => buildRealtimeAuthorizationBodyInRust(config, seeds);

export const buildRealtimeGateBody = (config: CoreConfig, query: RealtimeQuery): string =>
  buildGateBodyInRust(config, `0200${query.dataSpec}${query.key}`);

const decodeAuthorization = async (response: Uint8Array): Promise<number> => {
  if (response.length < ENVELOPE_BYTES) throw new Error("JV realtime authorization is too short");
  const header = ASCII_DECODER.decode(response.subarray(0, 16));
  if (!/^02001003\d{3}05001$/.test(header) || response[16] !== 0x0d || response[17] !== 0x0a)
    throw new Error("JV realtime authorization envelope is invalid");
  const payload = ASCII_DECODER.decode(await inflateDeflate(response.subarray(ENVELOPE_BYTES)));
  const tags = new Map(
    payload
      .split("\r\n")
      .filter((line) => line.length > 0)
      .map((line) => [line.slice(0, 2), line.slice(2)]),
  );
  for (const tag of ["AN", "DB", "DL", "UB", "UL", "OB", "OL", "CB"])
    if (!DIGITS_PATTERN.test(tags.get(tag) ?? ""))
      throw new Error("JV realtime authorization body is invalid");
  if (!/^(?:\d{14}| {14})$/.test(tags.get("IT") ?? ""))
    throw new Error("JV realtime authorization timestamp is invalid");
  return Number(header.slice(8, 11));
};

const decodeGate = async (response: Uint8Array, dataSpec: string): Promise<Uint8Array> => {
  if (response.length < ENVELOPE_BYTES) throw new Error("JV realtime gate response is too short");
  const header = ASCII_DECODER.decode(response.subarray(0, 16));
  const expectedApplication = `1${dataSpec.slice(1)}`;
  if (
    header.slice(0, 4) !== "0200" ||
    header.slice(4, 8) !== expectedApplication ||
    !DIGITS_PATTERN.test(header.slice(8, 15)) ||
    header[15] !== "1" ||
    response[16] !== 0x0d ||
    response[17] !== 0x0a
  )
    throw new Error("JV realtime gate envelope is invalid");
  const status = Number(header.slice(8, 11));
  if (status !== 0) throw new Error(`JV realtime gate returned status ${status}`);
  return inflateDeflate(response.subarray(ENVELOPE_BYTES));
};

export const authorizeRealtimeCredentials = async (
  config: CoreConfig,
  seeds: readonly [number, number],
  fetcher: JvFetch = fetch,
  softwareId: string = "UNKNOWN",
): Promise<void> => {
  const authorization = await postForm(
    AUTH_URL,
    buildRealtimeAuthorizationBody(config, seeds),
    fetcher,
    softwareId,
  );
  const status = await decodeAuthorization(authorization);
  if (status !== 0) throw new Error("JV realtime authorization was rejected");
};

export const acquireRealtimeData = async (
  config: CoreConfig,
  query: RealtimeQuery,
  fetcher: JvFetch = fetch,
): Promise<RealtimeResult> => {
  const validated = validateRealtimeQuery(query);
  const transitions: RealtimeTransition[] = ["configured"];
  const seeds = randomCredentialSeeds();
  transitions.push("session-encoded");
  await authorizeRealtimeCredentials(config, seeds, fetcher);
  transitions.push("authorized");
  const gate = await postForm(GATE_URL, buildRealtimeGateBody(config, validated), fetcher);
  transitions.push("gate-opened");
  const records = splitJvRecords(await decodeGate(gate, validated.dataSpec));
  transitions.push("payload-decoded");
  return { records, status: 0, transitions };
};
