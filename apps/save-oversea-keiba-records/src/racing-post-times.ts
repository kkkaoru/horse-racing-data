// This file runs with Bun. Race times of overseas history rows from the secondary results-by-date
// pages (fetched serially through the production proxy and cached; cached days are never refetched).
// The winner's published time is measured; a placed runner's time is estimated from its margin.
import assert from "node:assert/strict";
import { access, mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { HISTORY_PROXY_ORIGIN } from "./history-proxy";
import { applyRaceTimeRows, type RaceTimeRow, type RaceTimeStorePlan } from "./race-time-store";

export interface RacingPostProfile {
  readonly dayUrlTemplate: string;
  readonly dataScriptId: string;
  readonly racesPath: readonly string[];
  readonly raceUrlOrigin: string;
  readonly raceResultPath: readonly string[];
}
export interface RacingPostRunner {
  readonly position: string;
  readonly horseName: string;
  readonly distanceToWinner: string | null;
}
export interface RacingPostRace {
  readonly winnerTime: string | null;
  readonly runners: readonly RacingPostRunner[];
  readonly courseKey?: string;
  readonly countryCode?: string;
  readonly distance?: string;
  readonly runnersCount?: number;
  readonly fullResultLink?: string;
}
export interface RacingPostRaceRunner {
  readonly outcomeCode: string;
  readonly horseName: string;
  readonly beatenDistanceToWinner: string | null;
}
export interface RacingPostRaceLink {
  readonly date: string;
  readonly raceId: string;
  readonly link: string;
}
export interface MissingTimeRow {
  readonly source_horse_id: string;
  readonly source_race_id: string;
  readonly d: string;
  readonly venue: string;
  readonly finish_position: number | null;
  readonly distance_metres: number;
}
export interface RacingPostTimesInput {
  readonly argv: readonly string[];
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly fetchImpl?: (url: string, init: RequestInit) => Promise<Response>;
  readonly sleep?: (ms: number) => Promise<void>;
}
export interface RacingPostTimesReport {
  readonly status: "complete" | "blocked";
  readonly operation: "fetched" | "planned" | "applied";
  readonly days: number;
  readonly fetched: number;
  readonly candidates: number;
  readonly measured: number;
  readonly estimated: number;
  readonly unmatched: number;
  readonly filled: number;
  readonly conflicts: readonly string[];
}

const PROFILE_ENV: string = "OVERSEA_RACING_POST_PROFILE_PATH";
const REQUEST_INTERVAL_MS: number = 6000;
const PROXY_TIMEOUT_MS: number = 150000;
const SUCCESS_STATUS: number = 200;
const PRIVATE_FILE_MODE: number = 0o600;
const PRIVATE_DIRECTORY_MODE: number = 0o700;
// Seconds per length used to estimate a beaten runner's time from the winner's time.
export const SECONDS_PER_LENGTH: number = 0.2;
const TIME_DECIMALS: number = 2;
const SECONDS_PER_MINUTE: number = 60;
const WINNER_TIME_PATTERN: RegExp = /^(?:(\d+)m\s*)?(\d+(?:\.\d+)?)s$/u;
// JRA venues carry the meeting ordinals (e.g. 2小倉4); their times come from the JRA pages.
const JRA_VENUE_PATTERN: RegExp = /^\d.*\d$/u;
const MARGIN_WORDS: ReadonlyMap<string, number> = new Map([
  ["dht", 0],
  ["nse", 0.05],
  ["shd", 0.1],
  ["hd", 0.2],
  ["snk", 0.25],
  ["nk", 0.3],
]);
const FRACTIONS: ReadonlyMap<string, number> = new Map([
  ["¼", 0.25],
  ["½", 0.5],
  ["¾", 0.75],
]);
const LENGTHS_PATTERN: RegExp = /^(\d*)([¼½¾]?)l?$/u;
const NAME_SUFFIX_PATTERN: RegExp = /\s*\([A-Z]{2,3}\)\s*$/u;
// Titles read "カナ (English)", "English (カナ)" or just "English"; the Latin part is the name.
const TITLE_PATTERN: RegExp = /<title>([^(<|]*?)\s*(?:\(([^)<]+)\))?\s*\|/u;
const LATIN_PATTERN: RegExp = /[A-Za-z]/u;
const RACE_DISTANCE_PATTERN: RegExp = /^(?:(\d+)m)?(?:(\d*)([¼½¾]?)f)?(?:(\d+)y)?$/u;
const METRES_PER_MILE: number = 1609.344;
const METRES_PER_FURLONG: number = 201.168;
const METRES_PER_YARD: number = 0.9144;
// Half a furlong: netkeiba rounds overseas distances to the nearest 100 m.
const DISTANCE_TOLERANCE_METRES: number = 110;
const RACE_ID_PATTERN: RegExp = /\/(\d+)$/u;
const WINNER_OUTCOME: string = "1";
// netkeiba venue labels and the result courses they cover; country labels match any course there.
const VENUE_COURSES: ReadonlyMap<string, readonly string[]> = new Map([
  ["ドーヴィル", ["deauville"]],
  ["アケダクト", ["aqueduct", "belmont-at-the-big-a"]],
  ["サンクルー", ["saint-cloud"]],
  ["ピムリコ", ["pimlico"]],
  ["パリロンシャン", ["longchamp"]],
  ["ニューマーケット", ["newmarket", "newmarket-july"]],
  ["グッドウッド", ["goodwood"]],
  ["サラトガ", ["saratoga"]],
  ["ウィンザー", ["windsor"]],
  ["キーンランド", ["keeneland"]],
  ["アスコット", ["ascot"]],
  ["サンタアニタパー", ["santa-anita"]],
  ["メイダン", ["meydan"]],
  ["チャーチルダウン", ["churchill-downs"]],
  ["レパーズタウン", ["leopardstown"]],
  ["シャンティイ", ["chantilly"]],
  ["ヘイドックパーク", ["haydock"]],
  ["ニューベリー", ["newbury"]],
  ["カラ", ["curragh"]],
  ["デルマー", ["del-mar"]],
  ["ミュンヘン", ["munich"]],
  ["フェアグラウンズ", ["fair-grounds"]],
  ["タンパベイダウン", ["tampa-bay-downs"]],
  ["ガルフストリーム", ["gulfstream-park"]],
  ["オークローンパー", ["oaklawn-park"]],
  ["サンダウン", ["sandown"]],
  ["ヨーク", ["york"]],
  ["ケルン", ["cologne"]],
  ["ドンカスター", ["doncaster"]],
  ["チェスター", ["chester"]],
  ["シャティン", ["sha-tin"]],
  ["モンマスパーク", ["monmouth-park"]],
  ["ウッドバイン", ["woodbine"]],
  ["ベルモントパーク", ["belmont-park", "belmont-at-the-big-a"]],
  ["キングアブドゥル", ["king-abdulaziz"]],
  ["ランドウィック", ["randwick"]],
  ["フレミントン", ["flemington"]],
  ["エプソム", ["epsom"]],
]);
const VENUE_COUNTRIES: ReadonlyMap<string, string> = new Map([
  ["アメリカ", "USA"],
  ["イギリス", "GB"],
  ["アイルランド", "IRE"],
  ["イタリア", "ITY"],
  ["カタール", "QA"],
]);
const USAGE: string =
  "Use racing-post-times-fetch MISSING_JSON CACHE_DIR, racing-post-races-fetch MISSING_JSON CACHE_DIR HISTORY_ROOT, racing-post-times-plan MISSING_JSON CACHE_DIR HISTORY_ROOT TARGET or racing-post-times-apply MISSING_JSON CACHE_DIR HISTORY_ROOT TARGET --confirm-write.";

export const normaliseHorseName = (name: string): string =>
  name
    .replace(NAME_SUFFIX_PATTERN, "")
    .normalize("NFKD")
    .replace(/[^A-Za-z0-9]/gu, "")
    .toLowerCase();

// "1m 47.91s" or "58.12s".
export const winnerTimeSeconds = (text: string | null): number | null => {
  const match: RegExpExecArray | null = WINNER_TIME_PATTERN.exec(text?.trim() ?? "");
  return match === null ? null : Number(match[1] ?? "0") * SECONDS_PER_MINUTE + Number(match[2]);
};

// Cumulative margin to the winner ("1¼", "1¼l", "nk", "hd"); unknown wording is not estimated.
export const marginLengths = (text: string | null): number | null => {
  const value: string = (text ?? "").trim().toLowerCase();
  const word: number | undefined = MARGIN_WORDS.get(value);
  if (word !== undefined) return word;
  const match: RegExpExecArray | null = LENGTHS_PATTERN.exec(value);
  if (match === null || (match[1] === "" && match[2] === "")) return null;
  return Number(match[1] || "0") + (FRACTIONS.get(match[2] ?? "") ?? 0);
};

const timeText = (seconds: number): string => {
  const minutes: number = Math.floor(seconds / SECONDS_PER_MINUTE);
  const rest: string = (seconds - minutes * SECONDS_PER_MINUTE)
    .toFixed(TIME_DECIMALS)
    .padStart(5, "0");
  return minutes > 0 ? `${String(minutes)}:${rest}` : rest;
};

const dig = (value: unknown, path: readonly string[]): unknown => {
  const [key, ...rest] = path;
  if (key === undefined) return value;
  return typeof value === "object" && value !== null && key in value
    ? dig(Reflect.get(value, key), rest)
    : undefined;
};

const isRace = (value: unknown): value is RacingPostRace =>
  typeof value === "object" && value !== null && "runners" in value && Array.isArray(value.runners);

export const parseRacingPostDay = (html: string, profile: RacingPostProfile): RacingPostRace[] => {
  const start: number = html.indexOf(`id="${profile.dataScriptId}"`);
  assert(start >= 0, "Results day page has no data script.");
  const body: string = html.slice(html.indexOf(">", start) + 1, html.indexOf("</script>", start));
  const groups: unknown = dig(JSON.parse(body), profile.racesPath);
  assert(Array.isArray(groups), "Results day data has no race groups.");
  return groups.flatMap((group: unknown) => {
    const races: unknown = dig(group, ["races"]);
    return Array.isArray(races) ? races.filter(isRace) : [];
  });
};

// One stored history row matched by date and horse name; the same race may appear in several groups.
export const matchRacingPostTime = (
  row: MissingTimeRow,
  englishName: string,
  races: readonly RacingPostRace[],
): RaceTimeRow | null => {
  const name: string = normaliseHorseName(englishName);
  const hits = races.flatMap((race) =>
    race.runners
      .filter((runner) => normaliseHorseName(runner.horseName) === name)
      .map((runner) => ({ race, runner })),
  );
  const first = hits[0];
  if (first === undefined) return null;
  const winner: number | null = winnerTimeSeconds(first.race.winnerTime);
  const winnerRow: boolean = first.runner.position === "1st";
  const margin: number | null = winnerRow ? 0 : marginLengths(first.runner.distanceToWinner);
  if (winner === null || margin === null) return null;
  const seconds: number = winner + margin * SECONDS_PER_LENGTH;
  return {
    sourceHorseId: row.source_horse_id,
    sourceRaceId: row.source_race_id,
    raceTimeText: timeText(seconds),
    raceTimeSeconds: seconds.toFixed(TIME_DECIMALS),
    lastThreeFurlongsSeconds: null,
    raceTimeSource: "racingpost",
    raceTimeKind: winnerRow ? "measured" : "estimated",
  };
};

const isProfile = (value: unknown): value is RacingPostProfile =>
  typeof value === "object" &&
  value !== null &&
  "dayUrlTemplate" in value &&
  typeof value.dayUrlTemplate === "string" &&
  "dataScriptId" in value &&
  typeof value.dataScriptId === "string" &&
  "racesPath" in value &&
  Array.isArray(value.racesPath) &&
  "raceUrlOrigin" in value &&
  typeof value.raceUrlOrigin === "string" &&
  "raceResultPath" in value &&
  Array.isArray(value.raceResultPath);

const loadProfile = async (env: RacingPostTimesInput["env"]): Promise<RacingPostProfile> => {
  const path: string | undefined = env[PROFILE_ENV];
  assert(path !== undefined && path !== "", `${PROFILE_ENV} is required.`);
  const profile: unknown = JSON.parse(await readFile(path, "utf8"));
  assert(isProfile(profile), "The results profile is incomplete.");
  return profile;
};

export const overseasDates = (rows: readonly MissingTimeRow[]): string[] => [
  ...new Set(
    rows
      .filter((row) => row.finish_position !== null && !JRA_VENUE_PATTERN.test(row.venue))
      .map((row) => row.d),
  ),
];

const exists = (path: string): Promise<boolean> =>
  access(path).then(
    () => true,
    () => false,
  );

interface PageRequest {
  readonly url: string;
  readonly path: string;
}

const fetchPages = async (
  pages: readonly PageRequest[],
  profile: RacingPostProfile,
  { fetchImpl = fetch, sleep = Bun.sleep }: Pick<RacingPostTimesInput, "fetchImpl" | "sleep">,
): Promise<{ fetched: number; blocked: boolean }> => {
  const missing: PageRequest[] = [];
  for (const page of pages) {
    if (!(await exists(page.path))) missing.push(page);
  }
  for (const [index, page] of missing.entries()) {
    if (index > 0) await sleep(REQUEST_INTERVAL_MS);
    await mkdir(dirname(page.path), { recursive: true, mode: PRIVATE_DIRECTORY_MODE });
    const proxy: URL = new URL("/playwright", HISTORY_PROXY_ORIGIN);
    proxy.searchParams.set("url", page.url);
    const response: Response = await fetchImpl(proxy.href, {
      redirect: "manual",
      signal: AbortSignal.timeout(PROXY_TIMEOUT_MS),
    });
    const html: string = await response.text();
    // A challenge or error page stops the run; protection is never worked around.
    if (response.status !== SUCCESS_STATUS || !html.includes(`id="${profile.dataScriptId}"`)) {
      await writeFile(page.path.replace(/\.html$/u, ".rejected.html"), html, {
        mode: PRIVATE_FILE_MODE,
      });
      return { fetched: index, blocked: true };
    }
    await writeFile(page.path, html, { flag: "wx", mode: PRIVATE_FILE_MODE });
  }
  return { fetched: missing.length, blocked: false };
};

const dayRequests = (
  dates: readonly string[],
  cacheDir: string,
  profile: RacingPostProfile,
): PageRequest[] =>
  dates.map((date) => ({
    url: profile.dayUrlTemplate.replaceAll("{DATE}", date),
    path: join(cacheDir, `${date}.html`),
  }));

export const raceDistanceMetres = (text: string | undefined): number | null => {
  const match: RegExpExecArray | null = RACE_DISTANCE_PATTERN.exec(text ?? "");
  if (match === null || !text) return null;
  return (
    Number(match[1] ?? "0") * METRES_PER_MILE +
    (Number(match[2] || "0") + (FRACTIONS.get(match[3] ?? "") ?? 0)) * METRES_PER_FURLONG +
    Number(match[4] ?? "0") * METRES_PER_YARD
  );
};

const courseMatches = (venue: string, race: RacingPostRace): boolean =>
  VENUE_COURSES.get(venue)?.includes(String(race.courseKey)) ??
  VENUE_COUNTRIES.get(venue) === race.countryCode;

const distanceMatches = (row: MissingTimeRow, race: RacingPostRace): boolean =>
  Math.abs((raceDistanceMetres(race.distance) ?? 0) - row.distance_metres) <=
  DISTANCE_TOLERANCE_METRES;

// Full results of the same course and distance on that day, for a runner the day page does not list.
export const candidateRaceLinks = (
  row: MissingTimeRow,
  englishName: string,
  races: readonly RacingPostRace[],
): RacingPostRaceLink[] =>
  matchRacingPostTime(row, englishName, races) !== null
    ? []
    : races
        .filter(
          (race) =>
            race.fullResultLink !== undefined &&
            (race.runnersCount ?? 0) >= Number(row.finish_position) &&
            courseMatches(row.venue, race) &&
            distanceMatches(row, race),
        )
        .map((race) => ({
          date: row.d,
          raceId: String(RACE_ID_PATTERN.exec(String(race.fullResultLink))?.[1]),
          link: String(race.fullResultLink),
        }));

const isRaceRunner = (value: unknown): value is RacingPostRaceRunner =>
  typeof value === "object" && value !== null && "outcomeCode" in value && "horseName" in value;

// A full result page as a race of the day page shape: the winner's outcome is "1".
export const parseRacingPostRace = (html: string, profile: RacingPostProfile): RacingPostRace => {
  const start: number = html.indexOf(`id="${profile.dataScriptId}"`);
  assert(start >= 0, "Race result page has no data script.");
  const result: unknown = dig(
    JSON.parse(html.slice(html.indexOf(">", start) + 1, html.indexOf("</script>", start))),
    profile.raceResultPath,
  );
  const runners: unknown = dig(result, ["runners"]);
  assert(Array.isArray(runners), "Race result data has no runners.");
  const winningTime: unknown = dig(result, ["details", "winningTime"]);
  return {
    winnerTime: typeof winningTime === "string" ? winningTime : null,
    runners: runners.filter(isRaceRunner).map((runner) => ({
      position: String(runner.outcomeCode) === WINNER_OUTCOME ? "1st" : String(runner.outcomeCode),
      horseName: runner.horseName,
      distanceToWinner: runner.beatenDistanceToWinner,
    })),
  };
};

// English names come from the titles of the horse pages archived by `history collect`.
const englishNames = async (historyRoot: string): Promise<ReadonlyMap<string, string>> => {
  const directories: string[] = (await readdir(historyRoot)).filter((name) =>
    name.startsWith("horse-"),
  );
  const pairs = await Promise.all(
    directories.map(async (directory): Promise<[string, string] | null> => {
      const pages: string[] = (await readdir(join(historyRoot, directory, "pages"))).filter(
        (name) => name.endsWith(".html"),
      );
      const page: string | undefined = pages.toSorted()[0];
      if (page === undefined) return null;
      const title: RegExpExecArray | null = TITLE_PATTERN.exec(
        await readFile(join(historyRoot, directory, "pages", page), "utf8"),
      );
      const name: string | undefined = [title?.[1], title?.[2]].find(
        (part): part is string => part !== undefined && LATIN_PATTERN.test(part),
      );
      return name === undefined ? null : [directory.replace(/^horse-|-v\d+$/gu, ""), name];
    }),
  );
  return new Map(pairs.filter((pair): pair is [string, string] => pair !== null));
};

const candidateRows = (rows: readonly MissingTimeRow[]): MissingTimeRow[] => {
  const dates: ReadonlySet<string> = new Set(overseasDates(rows));
  return rows.filter((row) => dates.has(row.d) && row.finish_position !== null);
};

const readRaces = async (
  directory: string,
  profile: RacingPostProfile,
): Promise<RacingPostRace[]> =>
  (await exists(directory))
    ? Promise.all(
        (await readdir(directory))
          .filter((name) => name.endsWith(".html") && !name.endsWith(".rejected.html"))
          .map(async (name) =>
            parseRacingPostRace(await readFile(join(directory, name), "utf8"), profile),
          ),
      )
    : [];

// Cached full result pages come first: they list every finisher, the day page only the first three.
const loadDays = async (
  dates: readonly string[],
  cacheDir: string,
  profile: RacingPostProfile,
): Promise<ReadonlyMap<string, RacingPostRace[]>> => {
  const days: Map<string, RacingPostRace[]> = new Map();
  for (const date of dates) {
    const path: string = join(cacheDir, `${date}.html`);
    if (await exists(path)) {
      days.set(date, [
        ...(await readRaces(join(cacheDir, "races", date), profile)),
        ...parseRacingPostDay(await readFile(path, "utf8"), profile),
      ]);
    }
  }
  return days;
};

interface MatchContext {
  readonly candidates: readonly MissingTimeRow[];
  readonly names: ReadonlyMap<string, string>;
  readonly days: ReadonlyMap<string, RacingPostRace[]>;
}

const matchContext = async (
  rows: readonly MissingTimeRow[],
  cacheDir: string,
  historyRoot: string,
  profile: RacingPostProfile,
): Promise<MatchContext> => ({
  candidates: candidateRows(rows),
  names: await englishNames(historyRoot),
  days: await loadDays(overseasDates(rows), cacheDir, profile),
});

const namedRows = ({ candidates, names, days }: MatchContext) =>
  candidates.flatMap((row) => {
    const name: string | undefined = names.get(row.source_horse_id);
    const races: RacingPostRace[] | undefined = days.get(row.d);
    return name === undefined || races === undefined ? [] : [{ row, name, races }];
  });

const matchRows = (context: MatchContext): RaceTimeRow[] =>
  namedRows(context).flatMap(({ row, name, races }) => {
    const hit: RaceTimeRow | null = matchRacingPostTime(row, name, races);
    return hit === null ? [] : [hit];
  });

const raceRequests = (
  context: MatchContext,
  cacheDir: string,
  profile: RacingPostProfile,
): PageRequest[] => [
  ...new Map(
    namedRows(context)
      .flatMap(({ row, name, races }) => candidateRaceLinks(row, name, races))
      .map((race): [string, PageRequest] => [
        race.raceId,
        {
          url: `${profile.raceUrlOrigin}${race.link}`,
          path: join(cacheDir, "races", race.date, `${race.raceId}.html`),
        },
      ]),
  ).values(),
];

export const runRacingPostTimes = async (
  input: RacingPostTimesInput,
): Promise<RacingPostTimesReport> => {
  const [action, missingPath, cacheDir, historyRoot, target, confirmation]: readonly (
    | string
    | undefined
  )[] = input.argv;
  const fetchDaysOnly: boolean = action === "racing-post-times-fetch" && input.argv.length === 3;
  const fetchRaces: boolean = action === "racing-post-races-fetch" && input.argv.length === 4;
  const plan: boolean = action === "racing-post-times-plan" && input.argv.length === 5;
  const apply: boolean =
    action === "racing-post-times-apply" &&
    input.argv.length === 6 &&
    confirmation === "--confirm-write";
  assert(
    missingPath !== undefined &&
      cacheDir !== undefined &&
      (fetchDaysOnly ||
        (fetchRaces && historyRoot !== undefined) ||
        ((plan || apply) &&
          historyRoot !== undefined &&
          (target === "local" || target === "production"))),
    USAGE,
  );
  const profile: RacingPostProfile = await loadProfile(input.env);
  const rows: MissingTimeRow[] = JSON.parse(await readFile(missingPath, "utf8"));
  const dates: string[] = overseasDates(rows);
  const empty = {
    candidates: 0,
    measured: 0,
    estimated: 0,
    unmatched: 0,
    filled: 0,
    conflicts: [],
  };
  if (fetchDaysOnly || fetchRaces) {
    const pages: PageRequest[] = fetchDaysOnly
      ? dayRequests(dates, cacheDir, profile)
      : raceRequests(
          await matchContext(rows, cacheDir, String(historyRoot), profile),
          cacheDir,
          profile,
        );
    const result = await fetchPages(pages, profile, input);
    return {
      status: result.blocked ? "blocked" : "complete",
      operation: "fetched",
      days: dates.length,
      fetched: result.fetched,
      ...empty,
      candidates: pages.length,
    };
  }
  const context: MatchContext = await matchContext(rows, cacheDir, String(historyRoot), profile);
  const matched: RaceTimeRow[] = matchRows(context);
  const stored: RaceTimeStorePlan = await applyRaceTimeRows({
    env: input.env,
    target: target === "production" ? "production" : "local",
    rows: matched,
    apply,
  });
  return {
    status: "complete",
    operation: apply ? "applied" : "planned",
    days: dates.length,
    fetched: 0,
    candidates: context.candidates.length,
    measured: matched.filter((row) => row.raceTimeKind === "measured").length,
    estimated: matched.filter((row) => row.raceTimeKind === "estimated").length,
    unmatched: context.candidates.length - matched.length,
    filled: stored.fills.length,
    conflicts: stored.conflicts,
  };
};
