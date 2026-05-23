import "server-only";

const DEFAULT_PRODUCTION_API_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";

export const getProductionApiOrigin = (): string =>
  (process.env.PC_KEIBA_PRODUCTION_API_ORIGIN ?? DEFAULT_PRODUCTION_API_ORIGIN).replace(/\/+$/u, "");

export const getProductionAccessHeaders = (): Record<string, string> | null => {
  const clientId = process.env.PC_KEIBA_ACCESS_CLIENT_ID?.trim();
  const clientSecret = process.env.PC_KEIBA_ACCESS_CLIENT_SECRET?.trim();
  if (!clientId || !clientSecret) {
    return null;
  }
  return {
    "CF-Access-Client-Id": clientId,
    "CF-Access-Client-Secret": clientSecret,
  };
};

export const useProductionApiProxy = (): boolean =>
  process.env.NODE_ENV === "development" &&
  process.env.PC_KEIBA_PRODUCTION_API_PROXY !== "0" &&
  getProductionAccessHeaders() !== null;

export const getProductionLiveRelayOrigin = (): string | null => {
  if (!useProductionApiProxy()) {
    return null;
  }
  const configured = process.env.PC_KEIBA_PRODUCTION_LIVE_RELAY_ORIGIN?.trim();
  if (configured) {
    return configured.replace(/\/+$/u, "");
  }
  const port = process.env.PC_KEIBA_PRODUCTION_LIVE_RELAY_PORT ?? "3010";
  return `ws://127.0.0.1:${port}`;
};
