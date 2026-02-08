// Run with: bun
// mTLS verification middleware for Cloudflare API Shield

import type { Context, Next } from "hono";
import type { AppEnv } from "../types.ts";

interface TlsClientAuth {
  readonly certPresented: string;
  readonly certVerified: string;
}

const CERT_PRESENTED_VALUE = "1";
const CERT_VERIFIED_VALUE = "SUCCESS";
const SKIP_MTLS_VALUE = "1";
const FORBIDDEN_STATUS = 403;

const isMtlsSkipped = (env: AppEnv["Bindings"]): boolean => env.SKIP_MTLS === SKIP_MTLS_VALUE;

const isCertValid = (tlsClientAuth: TlsClientAuth): boolean =>
  tlsClientAuth.certPresented === CERT_PRESENTED_VALUE &&
  tlsClientAuth.certVerified === CERT_VERIFIED_VALUE;

const mtlsMiddleware = async (c: Context<AppEnv>, next: Next): Promise<Response> => {
  if (isMtlsSkipped(c.env)) {
    await next();
    return c.res;
  }

  const tlsClientAuth = (c.req.raw as unknown as { cf?: { tlsClientAuth?: TlsClientAuth } }).cf
    ?.tlsClientAuth;

  if (!tlsClientAuth || !isCertValid(tlsClientAuth)) {
    return c.json(
      { error: "Forbidden: valid mTLS client certificate required", status: FORBIDDEN_STATUS },
      FORBIDDEN_STATUS,
    );
  }

  await next();
  return c.res;
};

export { mtlsMiddleware, isMtlsSkipped, isCertValid };
export type { TlsClientAuth };
