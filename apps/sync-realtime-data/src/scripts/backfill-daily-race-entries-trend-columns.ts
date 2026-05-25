// Run with bun: `bun run src/scripts/backfill-daily-race-entries-trend-columns.ts \
//   --from YYYYMMDD --to YYYYMMDD [--scope all|jra|nar|ban-ei] [--origin https://...]`
//
// Posts a `build-daily-features` job to the sync-realtime-data Worker's /api/jobs
// endpoint for every JST date in the range. Each job re-runs the Neon→D1 daily
// feature upsert for that day, including the trend columns added in migration
// 0025 (wakuban, race_name, hasso_jikoku, corner_1..4, bataiju, zogen_fugo,
// zogen_sa). Idempotent: D1 upserts overwrite existing rows.

interface BunRuntime {
  argv: string[];
  env: Record<string, string | undefined>;
}

const getBunRuntime = (): BunRuntime | null => {
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion
  const candidate = (globalThis as { Bun?: BunRuntime }).Bun;
  return candidate ?? null;
};

export type BackfillScope = "all" | "ban-ei" | "jra" | "nar";

export interface BackfillOptions {
  fromDate: string;
  origin: string;
  scope: BackfillScope;
  token: string;
  toDate: string;
}

export interface JobPostBody {
  date: string;
  sourceScope: BackfillScope;
  type: "build-daily-features";
}

const YYYYMMDD_PATTERN = /^\d{8}$/u;
const DEFAULT_SCOPE = "all" satisfies BackfillScope;
const POST_CONCURRENCY = 4;
const MAX_DAYS = 366;
const MS_PER_DAY = 86_400_000;

export const findFlag = (argv: ReadonlyArray<string>, name: string): string | null => {
  const index = argv.indexOf(`--${name}`);
  if (index === -1 || index + 1 >= argv.length) return null;
  return argv[index + 1]!;
};

export const requireYyyymmdd = (value: string | null, label: string): string => {
  if (value === null || !YYYYMMDD_PATTERN.test(value)) {
    throw new Error(`${label} must match YYYYMMDD: ${value ?? ""}`);
  }
  return value;
};

export const requireScope = (value: string | null): BackfillScope => {
  if (value === null) return DEFAULT_SCOPE;
  if (value === "all" || value === "jra" || value === "nar" || value === "ban-ei") return value;
  throw new Error(`unknown scope: ${value}`);
};

export interface ParseOptionsInput {
  argv: ReadonlyArray<string>;
  env: Record<string, string | undefined>;
}

export const parseOptions = ({ argv, env }: ParseOptionsInput): BackfillOptions => {
  const fromDate = requireYyyymmdd(findFlag(argv, "from"), "--from");
  const toDate = requireYyyymmdd(findFlag(argv, "to"), "--to");
  const scope = requireScope(findFlag(argv, "scope"));
  const origin = findFlag(argv, "origin") ?? env.SYNC_REALTIME_ORIGIN ?? null;
  const token = findFlag(argv, "token") ?? env.REALTIME_ADMIN_TOKEN ?? null;
  if (origin === null) {
    throw new Error("--origin or SYNC_REALTIME_ORIGIN must be set");
  }
  if (token === null) {
    throw new Error("--token or REALTIME_ADMIN_TOKEN must be set");
  }
  return { fromDate, origin, scope, token, toDate };
};

export const enumerateDates = (fromDate: string, toDate: string): string[] => {
  const fromMs = Date.UTC(
    Number(fromDate.slice(0, 4)),
    Number(fromDate.slice(4, 6)) - 1,
    Number(fromDate.slice(6, 8)),
  );
  const toMs = Date.UTC(
    Number(toDate.slice(0, 4)),
    Number(toDate.slice(4, 6)) - 1,
    Number(toDate.slice(6, 8)),
  );
  if (toMs < fromMs) {
    throw new Error(`--to ${toDate} is before --from ${fromDate}`);
  }
  const dayCount = (toMs - fromMs) / MS_PER_DAY + 1;
  if (dayCount > MAX_DAYS) {
    throw new Error(`date range too large: ${dayCount} days`);
  }
  return Array.from({ length: dayCount }, (_, index) => {
    const date = new Date(fromMs + index * MS_PER_DAY);
    return `${date.getUTCFullYear()}${String(date.getUTCMonth() + 1).padStart(2, "0")}${String(
      date.getUTCDate(),
    ).padStart(2, "0")}`;
  });
};

export const buildJobBody = (date: string, scope: BackfillScope): JobPostBody => ({
  date,
  sourceScope: scope,
  type: "build-daily-features",
});

export interface PostJobInput {
  body: JobPostBody;
  options: BackfillOptions;
}

export const postJob = async ({ body, options }: PostJobInput): Promise<void> => {
  const response = await fetch(`${options.origin}/api/jobs`, {
    body: JSON.stringify(body),
    headers: {
      authorization: `Bearer ${options.token}`,
      "content-type": "application/json",
    },
    method: "POST",
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`POST /api/jobs ${body.date} failed: HTTP ${response.status} ${text}`);
  }
};

export interface RunWithLimitInput<T> {
  handler: (item: T) => Promise<void>;
  items: ReadonlyArray<T>;
  limit: number;
}

export const runWithLimit = async <T>({
  handler,
  items,
  limit,
}: RunWithLimitInput<T>): Promise<void> => {
  const indexState = { value: 0 };
  const worker = async (): Promise<void> => {
    const index = indexState.value;
    indexState.value = index + 1;
    if (index >= items.length) return;
    const item = items[index];
    if (item === undefined) return;
    await handler(item);
    await worker();
  };
  const workers = Array.from({ length: Math.min(limit, items.length) }, () => worker());
  await Promise.all(workers);
};

export const runBackfill = async (options: BackfillOptions): Promise<number> => {
  const dates = enumerateDates(options.fromDate, options.toDate);
  console.log(
    `enqueuing build-daily-features jobs: scope=${options.scope} dates=${dates.length} (${options.fromDate}..${options.toDate})`,
  );
  await runWithLimit({
    handler: async (date: string): Promise<void> => {
      await postJob({ body: buildJobBody(date, options.scope), options });
      console.log(`enqueued ${date}`);
    },
    items: dates,
    limit: POST_CONCURRENCY,
  });
  console.log(`done: enqueued ${dates.length} jobs`);
  return dates.length;
};

const isMainModule = (url: string, runtime: BunRuntime): boolean => {
  const argv1 = runtime.argv[1];
  if (argv1 === undefined) return false;
  return url === `file://${argv1}` || url.endsWith(argv1);
};

const bunRuntime = getBunRuntime();
if (bunRuntime !== null && isMainModule(import.meta.url, bunRuntime)) {
  const options = parseOptions({ argv: bunRuntime.argv.slice(2), env: bunRuntime.env });
  await runBackfill(options);
}
