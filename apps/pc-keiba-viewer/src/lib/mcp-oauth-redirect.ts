// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

const HTTP_PROTOCOL: string = "http:";
const HTTPS_PROTOCOL: string = "https:";

export const isAllowedOAuthRedirectUri = (value: string): boolean => {
  try {
    const url = new URL(value);
    if (url.hash.length > 0) {
      return false;
    }
    if (url.protocol === HTTPS_PROTOCOL) {
      return url.hostname.length > 0;
    }
    if (url.protocol !== HTTP_PROTOCOL) {
      return false;
    }
    return url.hostname === "127.0.0.1" || url.hostname === "localhost";
  } catch {
    return false;
  }
};

export const redirectUrisInclude = (allowed: readonly string[], candidate: string): boolean =>
  allowed.some((uri) => uri === candidate);
