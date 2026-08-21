// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

import { toBase64Url } from "./mcp-oauth-pkce";

const RANDOM_BYTES: number = 32;

export const randomToken = (): string => {
  const bytes = new Uint8Array(RANDOM_BYTES);
  crypto.getRandomValues(bytes);
  return toBase64Url(bytes);
};
