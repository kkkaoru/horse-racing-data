const DEFAULT_LIVE_RELAY_PORT = "3010";

const getLiveRelayOrigin = (): string | null => {
  if (process.env.NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY === "0") {
    return null;
  }
  const configured = process.env.NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_ORIGIN?.trim();
  if (configured) {
    return configured.replace(/\/+$/u, "");
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
