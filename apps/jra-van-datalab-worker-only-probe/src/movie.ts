// Run with bun. Worker-native JVMVOpen/JVMVRead movie-list equivalent.

import type { JvFetch } from "./acquisition";
import { randomCredentialSeeds, type CoreConfig } from "./protocol";
import { authorizeRealtimeCredentials } from "./realtime";
import { buildGateBodyInRust } from "./rust-core";

export interface MovieListQuery {
  movieType: "11" | "12" | "13";
  searchKey: string;
  softwareId?: string;
}

export interface MovieListResult {
  keys: readonly Uint8Array[];
  status: number;
}

const AUTH_URL: string = "http://authlab.jra-van.ne.jp/Browsing/JVServlet/";
const GATE_URL: string = "http://reallab.jra-van.ne.jp/Browsing/GateServlet/";
const FORM_CONTENT_TYPE: string = "application/x-www-form-urlencoded";
const DEFAULT_SOFTWARE_ID: string = "SA000000/SD000004";
const SOFTWARE_ID_PATTERN: RegExp = /^[A-Za-z0-9 _./]{1,64}$/;
const ASCII_DECODER: TextDecoder = new TextDecoder("ascii", { fatal: true, ignoreBOM: false });
const ENVELOPE_BYTES: number = 18;

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
  softwareId: string,
  fetcher: JvFetch,
): Promise<Uint8Array> => {
  const response = await fetcher(url, {
    body,
    headers: { "Content-Type": FORM_CONTENT_TYPE, "User-Agent": `${softwareId}:` },
    method: "POST",
    redirect: "manual",
  });
  if (!response.ok) throw new Error("JV movie endpoint returned an HTTP error");
  return new Uint8Array(await response.arrayBuffer());
};

export const normalizeMovieSearch = (query: MovieListQuery): string => {
  if (!SOFTWARE_ID_PATTERN.test(query.softwareId ?? DEFAULT_SOFTWARE_ID))
    throw new Error("softwareId has an invalid JVInit shape");
  if (query.movieType === "11" && /^\d{8}$/.test(query.searchKey))
    return `${query.searchKey}0000000000`;
  if (query.movieType === "12" && /^\d{18}$/.test(query.searchKey)) return query.searchKey;
  if (query.movieType === "13" && /^\d{10}$/.test(query.searchKey))
    return `00000000${query.searchKey}`;
  throw new Error("movieType and searchKey have an invalid JVMVOpen combination");
};

export const buildMovieAuthorizationBody = (): string =>
  new URLSearchParams([
    ["VER", "0200"],
    ["APPL", "0006"],
    ["RKEY", ""],
    ["UKEY", ""],
    ["JVER", "0500"],
    ["OS", "000200100000"],
    ["JVBIT", "1"],
  ]).toString();

const decodeMovieAuthorization = (response: Uint8Array): void => {
  if (response.length !== 19) throw new Error("JV movie authorization has an invalid length");
  const header = ASCII_DECODER.decode(response.subarray(0, 16));
  if (
    !/^02001006\d{3}05000$/.test(header) ||
    response[16] !== 0x0d ||
    response[17] !== 0x0a ||
    (response[18] !== 0x30 && response[18] !== 0x31)
  )
    throw new Error("JV movie authorization envelope is invalid");
  const status = Number(header.slice(8, 11));
  if (status !== 0) throw new Error(`JV movie authorization returned status ${status}`);
  if (response[18] !== 0x31) throw new Error("JV movie operation is not authorized");
};

const decodeMovieList = async (response: Uint8Array): Promise<MovieListResult> => {
  if (response.length < ENVELOPE_BYTES) throw new Error("JV movie list response is too short");
  const header = ASCII_DECODER.decode(response.subarray(0, 16));
  if (!/^02001B8C\d{3}\d{4}1$/.test(header) || response[16] !== 0x0d || response[17] !== 0x0a)
    throw new Error("JV movie list envelope is invalid");
  const status = Number(header.slice(8, 11));
  if (status !== 0) throw new Error(`JV movie list returned status ${status}`);
  const decoded = await inflateDeflate(response.subarray(ENVELOPE_BYTES));
  const lines: Uint8Array[] = [];
  let start = 0;
  for (let index = 0; index + 1 < decoded.length; index += 1) {
    if (decoded[index] !== 0x0d || decoded[index + 1] !== 0x0a) continue;
    lines.push(decoded.slice(start, index));
    start = index + 2;
    index += 1;
  }
  if (start !== decoded.length || lines.length < 2 || lines[0]!.length !== 0)
    throw new Error("JV movie list control framing is invalid");
  if (lines[1]!.length !== 46) throw new Error("JV movie list control record is invalid");
  const keys = lines.slice(2);
  if (keys.some((key) => !/^\d{18}$/.test(ASCII_DECODER.decode(key))))
    throw new Error("JV movie list key record is invalid");
  return { keys, status };
};

export const acquireMovieList = async (
  config: CoreConfig,
  query: MovieListQuery,
  fetcher: JvFetch = fetch,
): Promise<MovieListResult> => {
  const search = normalizeMovieSearch(query);
  const softwareId = query.softwareId ?? DEFAULT_SOFTWARE_ID;
  await authorizeRealtimeCredentials(config, randomCredentialSeeds(), fetcher, softwareId);
  const authorization = await postForm(
    AUTH_URL,
    buildMovieAuthorizationBody(),
    softwareId,
    fetcher,
  );
  decodeMovieAuthorization(authorization);
  const gateBody = buildGateBodyInRust(config, `02000B8C${search}`);
  const response = await postForm(GATE_URL, gateBody, softwareId, fetcher);
  return decodeMovieList(response);
};
