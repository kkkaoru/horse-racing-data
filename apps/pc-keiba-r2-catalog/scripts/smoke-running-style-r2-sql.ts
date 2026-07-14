import { parseRunningStyleSmokeArgs, runRunningStyleSmoke } from "../src/running-style-smoke";

const requireToken = (): string => {
  const token =
    Bun.env.R2_SQL_TOKEN ?? Bun.env.WRANGLER_R2_SQL_AUTH_TOKEN ?? Bun.env.R2_CATALOG_TOKEN;
  if (!token) {
    throw new Error("R2_SQL_TOKEN, WRANGLER_R2_SQL_AUTH_TOKEN, or R2_CATALOG_TOKEN is required");
  }
  return token;
};

const config = {
  R2_SQL_ACCOUNT_ID: Bun.env.R2_SQL_ACCOUNT_ID ?? "78109ec18c7c85b194b19fb32e3bb149",
  R2_SQL_BUCKET_NAME: Bun.env.R2_SQL_BUCKET_NAME ?? "pc-keiba-r2-catalog",
  R2_SQL_NAMESPACE: Bun.env.R2_SQL_NAMESPACE ?? "pc_keiba",
  R2_SQL_TOKEN: requireToken(),
};
const filters = parseRunningStyleSmokeArgs(Bun.argv.slice(2));
const result = await runRunningStyleSmoke(config, filters, fetch);
console.log(JSON.stringify({ filters, ok: true, ...result }));
