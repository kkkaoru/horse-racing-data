// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

const S256: string = "S256";

export const toBase64Url = (bytes: Uint8Array): string => {
  let binary = "";
  bytes.forEach((byte) => {
    binary += String.fromCharCode(byte);
  });
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
};

export const sha256Base64Url = async (value: string): Promise<string> => {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return toBase64Url(new Uint8Array(digest));
};

export const verifyPkceS256 = async (verifier: string, challenge: string): Promise<boolean> => {
  if (verifier.length < 43 || verifier.length > 128 || challenge.length === 0) {
    return false;
  }
  const computed = await sha256Base64Url(verifier);
  return computed === challenge;
};

export const isS256Method = (method: string | null): boolean => method === S256;
