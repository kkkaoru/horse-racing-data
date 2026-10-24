// Run with bun. A race page is shared publicly through a Cloudflare Access
// bypass app scoped to that race; Access cannot expire or restrict methods, so
// the Worker enforces both from the KV share entry written by share-race-page.

export const SHARED_PAGE_HEADER: string = "x-pc-keiba-shared-until";
export const SHARED_PAGE_HOST: string = "pc-keiba-viewer.kkk4oru.com";
const SHARED_PAGE_KEY_PREFIX: string = "shared-page:v1:";
const RACE_PATH_PATTERN: RegExp =
  /^(?<api>\/api)?(?<page>\/races\/\d{4}\/\d{2}\/\d{2}\/[0-9A-Z]{2}\/\d{2})(?<rest>\/.*)?$/u;
const READ_METHODS: ReadonlySet<string> = new Set(["GET", "HEAD"]);
const FORBIDDEN_STATUS: number = 403;

export interface SharedPageKv {
  get(key: string): Promise<string | null>;
}

export interface SharedPageEntry {
  expiresAt: string;
}

interface RacePathMatch {
  pagePath: string;
  // The page itself or one of its /api/races/... resources; sibling pages
  // such as .../paddock are not shared.
  shareable: boolean;
}

interface ResolveSharedPageRequestInput {
  kv: SharedPageKv | undefined;
  nowMs: number;
  request: Request;
}

export const buildSharedPageKey = (pagePath: string): string =>
  `${SHARED_PAGE_KEY_PREFIX}${pagePath}`;

// Access adds a JWT to every request it lets through for an identity or a
// service token, and only edge requests carry cf-connecting-ip, so this is a
// browser that reached the Worker through a bypass app. Service-binding and
// self requests carry neither header.
const isBypassedEdgeRequest = (request: Request, url: URL): boolean =>
  url.hostname === SHARED_PAGE_HOST &&
  request.headers.has("cf-connecting-ip") &&
  !request.headers.has("cf-access-jwt-assertion");

const matchRacePath = (pathname: string): RacePathMatch | null => {
  const groups = RACE_PATH_PATTERN.exec(pathname)?.groups;
  if (groups === undefined) return null;
  return {
    pagePath: String(groups.page),
    shareable: groups.api === undefined ? groups.rest === undefined : groups.rest !== undefined,
  };
};

const parseSharedPageEntry = (value: string | null): SharedPageEntry | null => {
  if (value === null) return null;
  const parsed: unknown = JSON.parse(value);
  return typeof parsed === "object" &&
    parsed !== null &&
    "expiresAt" in parsed &&
    typeof parsed.expiresAt === "string"
    ? { expiresAt: parsed.expiresAt }
    : null;
};

const forbidden = (): Response =>
  new Response("This page is not shared.", { status: FORBIDDEN_STATUS });

// Only races with a share entry are gated, so a request this cannot classify
// keeps the decision the Access edge already made.
export const resolveSharedPageRequest = async ({
  kv,
  nowMs,
  request,
}: ResolveSharedPageRequestInput): Promise<Request | Response> => {
  const url = new URL(request.url);
  const match = isBypassedEdgeRequest(request, url) ? matchRacePath(url.pathname) : null;
  if (match === null || kv === undefined) return request;
  const entry = parseSharedPageEntry(await kv.get(buildSharedPageKey(match.pagePath)));
  if (entry === null) return request;
  const active = Date.parse(entry.expiresAt) > nowMs;
  if (!active || !match.shareable || !READ_METHODS.has(request.method)) return forbidden();
  const headers = new Headers(request.headers);
  headers.set(SHARED_PAGE_HEADER, entry.expiresAt);
  return new Request(request, { headers });
};
