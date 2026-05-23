import { getProductionLiveWebSocketUrl } from "./production-live-client-url";

interface PaddockLocation {
  host: string;
  hostname: string;
  protocol: string;
}

const getCurrentLocation = (): PaddockLocation | null =>
  typeof window === "undefined" ? null : window.location;

export const isLocalPaddockHost = (hostname: string): boolean =>
  hostname === "localhost" || hostname === "127.0.0.1" || hostname === "0.0.0.0";

export const getPaddockRequestUrl = (
  path: string,
  _location: PaddockLocation | null = getCurrentLocation(),
): string => path;

export const getPaddockLiveUrl = (
  path: string,
  location: PaddockLocation | null = getCurrentLocation(),
): string => {
  const relayUrl = getProductionLiveWebSocketUrl(path);
  if (relayUrl) {
    return relayUrl;
  }
  if (!location) {
    return path;
  }
  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  return `${protocol}//${location.host}${path.startsWith("/") ? path : `/${path}`}`;
};

export const getRaceTrendLiveUrl = (path: string): string => {
  const relayUrl = getProductionLiveWebSocketUrl(path);
  if (relayUrl) {
    return relayUrl;
  }
  if (typeof window === "undefined") {
    return path;
  }
  const url = new URL(path, window.location.href);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
};
