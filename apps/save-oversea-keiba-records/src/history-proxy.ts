// This file runs with Bun. Source requests use the existing production proxy, never direct fallback.
import { createHash } from "node:crypto";
import { access, mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import type { HistoryFetchInput } from "./history-cli";
import { isHistoryRecord } from "./history-json";

export const HISTORY_PROXY_ORIGIN: string = "https://reverse-proxy.api.kkk4oru.com";
const SOURCE_HOST: string = "netkeiba.com";
const PROXY_TIMEOUT_MS: number = 150000;
const PRIVATE_DIRECTORY_MODE: number = 0o700;
const PRIVATE_FILE_MODE: number = 0o600;
const SUCCESS_STATUS: number = 200;
const HTML_UTF8: RegExp = /text\/html\b.*charset\s*=\s*["']?utf-8\b/iu;
const TRAILING_DOT: RegExp = /\.$/u;

export const isHistoryProxySource = (source: string): boolean => {
  const hostname: string = new URL(source).hostname.replace(TRAILING_DOT, "");
  return hostname === SOURCE_HOST || hostname.endsWith(`.${SOURCE_HOST}`);
};

export const historyProxyUrl = (source: string): string => {
  const url: URL = new URL(source);
  if (
    !isHistoryProxySource(source) ||
    url.protocol !== "https:" ||
    url.port ||
    url.username ||
    url.password ||
    url.hash
  ) {
    throw new Error(
      "History proxy requires a public HTTPS source without credentials or fragments.",
    );
  }
  const proxy: URL = new URL("/playwright", HISTORY_PROXY_ORIGIN);
  proxy.searchParams.set("url", url.href);
  return proxy.href;
};

const requireAbsent = async (path: string): Promise<void> => {
  await access(path).then(
    () => {
      throw new Error(
        "Existing raw source evidence must be inspected, never overwritten or refetched.",
      );
    },
    (error: unknown) => {
      if (!isHistoryRecord(error) || error.code !== "ENOENT") throw error;
    },
  );
};

export const fetchProxyHistoryPage = async ({
  url,
  rawPath,
}: HistoryFetchInput): Promise<string> => {
  const transportUrl: string = historyProxyUrl(url);
  await Promise.all([requireAbsent(rawPath), requireAbsent(`${rawPath}.meta.json`)]);
  await mkdir(dirname(rawPath), { recursive: true, mode: PRIVATE_DIRECTORY_MODE });
  // Exclusive reservation also blocks blind replay after an unknown network outcome.
  await writeFile(
    `${rawPath}.pending.json`,
    JSON.stringify({ url, transportUrl, startedAt: new Date().toISOString() }),
    { flag: "wx", mode: PRIVATE_FILE_MODE },
  );
  const response: Response = await fetch(transportUrl, {
    redirect: "manual",
    signal: AbortSignal.timeout(PROXY_TIMEOUT_MS),
  });
  const bytes: Uint8Array = new Uint8Array(await response.arrayBuffer());
  await writeFile(rawPath, bytes, { flag: "wx", mode: PRIVATE_FILE_MODE });
  await writeFile(
    `${rawPath}.meta.json`,
    JSON.stringify({
      url,
      transportUrl,
      transport: "reverse-proxy-browser-get",
      fetchedAt: new Date().toISOString(),
      status: response.status,
      contentType: response.headers.get("content-type"),
      encoding: "utf-8",
      bytes: bytes.byteLength,
      sha256: createHash("sha256").update(bytes).digest("hex"),
      cache: response.headers.get("x-cache"),
      retryAfter: response.headers.get("retry-after"),
    }),
    { flag: "wx", mode: PRIVATE_FILE_MODE },
  );
  if (
    response.status !== SUCCESS_STATUS ||
    !HTML_UTF8.test(response.headers.get("content-type") ?? "")
  ) {
    throw new Error(
      "Proxy source response was not successful UTF-8 HTML; inspect preserved raw evidence before retrying.",
    );
  }
  return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
};
