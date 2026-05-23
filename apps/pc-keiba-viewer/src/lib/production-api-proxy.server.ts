import "server-only";

import {
  getProductionAccessHeaders,
  getProductionApiOrigin,
  useProductionApiProxy,
} from "./production-access.server";

export { useProductionApiProxy };

export const buildProductionApiUrl = (path: string): string => {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${getProductionApiOrigin()}${normalizedPath}`;
};

export const fetchProductionApi = (path: string, init?: RequestInit): Promise<Response> => {
  const accessHeaders = getProductionAccessHeaders();
  if (!accessHeaders) {
    throw new Error("Production Access credentials are unavailable.");
  }

  const headers = new Headers(init?.headers);
  for (const [key, value] of Object.entries(accessHeaders)) {
    headers.set(key, value);
  }

  return fetch(buildProductionApiUrl(path), {
    ...init,
    cache: "no-store",
    headers,
  });
};
