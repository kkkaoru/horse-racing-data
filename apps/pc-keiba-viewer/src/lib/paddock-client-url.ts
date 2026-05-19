const DEFAULT_REMOTE_PADDOCK_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";

interface PaddockLocation {
  host: string;
  hostname: string;
  protocol: string;
}

const getCurrentLocation = (): PaddockLocation | null =>
  typeof window === "undefined" ? null : window.location;

export const isLocalPaddockHost = (hostname: string): boolean =>
  hostname === "localhost" || hostname === "127.0.0.1" || hostname === "0.0.0.0";

const getRemotePaddockOrigin = (): string => {
  const configured = process.env.NEXT_PUBLIC_PC_KEIBA_PADDOCK_REMOTE_ORIGIN?.trim();
  return (configured || DEFAULT_REMOTE_PADDOCK_ORIGIN).replace(/\/+$/u, "");
};

const isRemotePaddockBindingEnabled = (): boolean =>
  process.env.NEXT_PUBLIC_PC_KEIBA_PADDOCK_REMOTE_BINDINGS !== "0";

const withOrigin = (origin: string, path: string): string =>
  `${origin}${path.startsWith("/") ? path : `/${path}`}`;

export const getPaddockRequestUrl = (
  path: string,
  location: PaddockLocation | null = getCurrentLocation(),
): string => {
  if (location && isLocalPaddockHost(location.hostname) && isRemotePaddockBindingEnabled()) {
    return withOrigin(getRemotePaddockOrigin(), path);
  }
  return path;
};

export const getPaddockLiveUrl = (
  path: string,
  location: PaddockLocation | null = getCurrentLocation(),
): string => {
  const requestUrl = getPaddockRequestUrl(path, location);
  if (!requestUrl.startsWith("http")) {
    if (!location) {
      return requestUrl;
    }
    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    return `${protocol}//${location.host}${requestUrl}`;
  }
  const liveUrl = new URL(requestUrl);
  liveUrl.protocol = liveUrl.protocol === "http:" ? "ws:" : "wss:";
  return liveUrl.toString();
};
