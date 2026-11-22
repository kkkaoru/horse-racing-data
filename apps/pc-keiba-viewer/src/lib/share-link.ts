export const SHARE_HOST = "pc-keiba-viewer.kkk4oru.com";

export const SHARE_ORIGIN = `https://${SHARE_HOST}`;

export const DEFAULT_SHARE_HOURS = 48;

export const SHARE_HOUR_OPTIONS = [24, 48, 72, 168] as const;

const STORAGE_PREFIX = "pc-keiba-shared-page:";

function shareStorage(): Storage | null {
  if (typeof window === "undefined" || typeof window.localStorage === "undefined") {
    return null;
  }
  return window.localStorage;
}

export function buildShareUrl(path: string, search = ""): string {
  return `${SHARE_ORIGIN}${path}${search}`;
}

// shortcut: the copied URL has no server-side expiry, so the deadline only bounds
// how long the page counts as already shared; add a share-token API when recipients
// themselves must be blocked after the deadline.
export function readShareDeadline(path: string, now = Date.now()): number {
  const deadline = Number(shareStorage()?.getItem(`${STORAGE_PREFIX}${path}`));
  return deadline > now ? deadline : 0;
}

export function writeShareDeadline(path: string, deadline: number): void {
  shareStorage()?.setItem(`${STORAGE_PREFIX}${path}`, String(deadline));
}
