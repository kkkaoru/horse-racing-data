// Run with bun via the pc-keiba-viewer vitest config.
const DEFAULT_LIVE_RELAY_PORT = "3010";
const PRODUCTION_NODE_ENV = "production";

const isProductionNodeEnv = (): boolean => process.env.NODE_ENV === PRODUCTION_NODE_ENV;

const getLiveRelayOrigin = (): string | null => {
  if (process.env.NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY === "0") {
    return null;
  }
  const configured = process.env.NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_ORIGIN?.trim();
  if (configured) {
    return configured.replace(/\/+$/u, "");
  }
  if (isProductionNodeEnv()) {
    return null;
  }
  const port =
    process.env.NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_PORT ?? DEFAULT_LIVE_RELAY_PORT;
  return `ws://127.0.0.1:${port}`;
};

export const getProductionLiveWebSocketUrl = (path: string): string | null => {
  const relayOrigin = getLiveRelayOrigin();
  if (!relayOrigin) {
    return null;
  }
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${relayOrigin}${normalizedPath}`;
};
