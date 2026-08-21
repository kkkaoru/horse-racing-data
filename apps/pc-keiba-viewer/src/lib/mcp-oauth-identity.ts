// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

const ACCESS_EMAIL_HEADER: string = "Cf-Access-Authenticated-User-Email";
const ACCESS_JWT_HEADER: string = "Cf-Access-Jwt-Assertion";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const decodeJwtPayload = (token: string): Record<string, unknown> | null => {
  const parts = token.split(".");
  const payload = parts[1];
  if (payload === undefined) {
    return null;
  }
  try {
    const padded = payload.replaceAll("-", "+").replaceAll("_", "/");
    const padLength = (4 - (padded.length % 4)) % 4;
    const json = atob(`${padded}${"=".repeat(padLength)}`);
    const parsed: unknown = JSON.parse(json);
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
};

export const readAccessUserSubject = (request: Request): string | null => {
  const emailHeader = request.headers.get(ACCESS_EMAIL_HEADER)?.trim();
  if (emailHeader !== undefined && emailHeader.length > 0) {
    return emailHeader;
  }
  const assertion = request.headers.get(ACCESS_JWT_HEADER);
  if (assertion === null) {
    return null;
  }
  const payload = decodeJwtPayload(assertion);
  if (payload === null) {
    return null;
  }
  const email = payload.email;
  if (typeof email === "string" && email.length > 0) {
    return email;
  }
  const sub = payload.sub;
  return typeof sub === "string" && sub.length > 0 ? sub : null;
};
