// Runs with Bun for offline checks and the native Workers HTMLRewriter in production.
import { isArcPredictionRace, type ArcRaceScope } from "./arc-prediction";
import type { HorseRaceResult, Runner } from "./race-types";

export interface OfficialPast {
  date: string;
  distance: string;
  fieldSize: string;
  finish: string | null;
  time: string | null;
  corners: (string | null)[];
}
export interface OfficialHorse {
  number: number;
  name: string;
  gate: number;
  owner: string;
  sire: string;
  damSire: string;
  odds: number;
  popularity: number;
  past: OfficialPast[];
}
export interface OfficialCard {
  url: string;
  retrievedAt: string;
  horses: OfficialHorse[];
}
interface NativeText {
  text: string;
}
interface NativeElement {
  getAttribute(name: string): string | null;
}
interface NativeHandlers {
  text?: (chunk: NativeText) => void;
  element?: (element: NativeElement) => void;
}
interface NativeRewriter {
  on(selector: string, handlers: NativeHandlers): NativeRewriter;
  transform(response: Response): Response;
}
declare const HTMLRewriter: { new (): NativeRewriter };
interface Fields {
  [key: string]: string;
}
interface ParseInput {
  html: string;
  retrievedAt: string;
  url: string;
}
export const ARC_OFFICIAL_URL: string =
  "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0110520260101051/9D";
const DECLARED_NAMES: readonly string[] = [
  "ダリズ",
  "サダッド",
  "チェスナットロケット",
  "アドマイヤテラ",
  "メイショウタバル",
  "アローイーグル",
  "ベイシティローラー",
  "ミニーホーク",
  "カルパナ",
  "フレンドリーソウル",
  "ヴァランディール",
  "ベンヴェヌートチェッリーニ",
  "ブライトライト",
  "モルティーズクロス",
  "サンダリングオン",
  "ダイヤモンドネックレス",
];
const DECLARED_GATES: readonly number[] = [5, 9, 2, 12, 13, 14, 15, 10, 6, 7, 4, 1, 8, 11, 3, 16];
const FIELD_SELECTORS: Readonly<Record<string, string>> = {
  number: "td.num",
  name: ".horse .name .txt",
  gate: "td.waku",
  owner: "p.owner",
  sire: ".sire",
  damSire: ".bloodmare",
  odds: ".odds strong",
  popularity: ".pop_rank",
};
const PAST_SELECTORS: Readonly<Record<string, string>> = {
  date: ".date",
  distance: ".dist",
  fieldSize: ".place_line .num .max",
  finish: ".place",
  time: ".info_line2 p.time",
  c1: 'li[title="1コーナー通過順位"]',
  c2: 'li[title="2コーナー通過順位"]',
  c3: 'li[title="3コーナー通過順位"]',
  c4: 'li[title="4コーナー通過順位"]',
};
const clean = (value: string | undefined): string => (value ?? "").replace(/\s+/gu, " ").trim();
const positive = (value: string): number =>
  /^\d+(?:\.\d+)?$/u.test(value) && Number(value) > 0 ? Number(value) : NaN;
const corner = (value: string | undefined): string | null =>
  /^\d+$/u.test(clean(value)) && Number(value) > 0 ? clean(value) : null;
const past = (fields: Fields, prefix: string): OfficialPast => {
  const date = clean(fields[`${prefix}date`]).match(/^(\d{4})年(\d{1,2})月(\d{1,2})日$/u);
  const distance = clean(fields[`${prefix}distance`]).match(/^(\d+)芝$/u);
  const finish = clean(fields[`${prefix}finish`]).match(/^(\d+)着$/u);
  const time = clean(fields[`${prefix}time`]).match(/^(\d+):(\d{2})\.(\d)$/u);
  const count = clean(fields[`${prefix}fieldSize`]).match(/^([1-9]\d?)頭$/u);
  if (!date || !distance || !count) throw new Error("Invalid official past identity");
  const dateKey: string = `${date[1]}${date[2]?.padStart(2, "0")}${date[3]?.padStart(2, "0")}`;
  const parsedDate: number = Date.parse(
    `${dateKey.slice(0, 4)}-${dateKey.slice(4, 6)}-${dateKey.slice(6, 8)}`,
  );
  const corners: (string | null)[] = ["c1", "c2", "c3", "c4"].map((key) =>
    corner(fields[`${prefix}${key}`]),
  );
  if (
    !Number.isFinite(parsedDate) ||
    new Date(parsedDate).toISOString().slice(0, 10).replaceAll("-", "") !== dateKey ||
    dateKey >= "20261004" ||
    corners.some((value) => value !== null && Number(value) > Number(count[1])) ||
    (time !== null && Number(time[2]) >= 60)
  )
    throw new Error("Invalid official past identity");
  return {
    date: dateKey,
    distance: distance[1] ?? "",
    fieldSize: count[1] ?? "",
    finish: finish?.[1] ?? null,
    time: time ? `${time[1]}${time[2]}${time[3]}` : null,
    corners,
  };
};
const horse = (fields: Fields): OfficialHorse => {
  const row: OfficialHorse = {
    number: positive(clean(fields.number)),
    name: clean(fields.name),
    gate: positive(clean(fields.gate)),
    owner: clean(fields.owner),
    sire: clean(fields.sire).replace(/^父：/u, ""),
    damSire: clean(fields.damSire).replace(/^\(母の父：|\)$/gu, ""),
    odds: positive(clean(fields.odds)),
    popularity: positive(clean(fields.popularity).replace(/^\(|番人気\)$/gu, "")),
    past: [1, 2, 3, 4].map((n) => past(fields, `p${n}.`)),
  };
  if (
    !row.name ||
    !row.owner ||
    !Number.isInteger(row.number) ||
    !Number.isInteger(row.gate) ||
    row.gate > 16 ||
    row.number > 16 ||
    !Number.isFinite(row.odds) ||
    !Number.isInteger(row.popularity) ||
    row.popularity > 16
  )
    throw new Error("Invalid official declaration");
  return row;
};
export const parseOfficialCard = async (input: ParseInput): Promise<OfficialCard> => {
  if (
    input.url !== ARC_OFFICIAL_URL ||
    !Number.isFinite(Date.parse(input.retrievedAt)) ||
    input.html.includes("\ufffd")
  )
    throw new Error("Invalid official provenance or encoding");
  const rows: Fields[] = [];
  const heading: string[] = [];
  const links: string[] = [];
  const rewriter = new HTMLRewriter()
    .on("h1", {
      text(chunk) {
        heading.push(chunk.text);
      },
    })
    .on("a[href]", {
      element(element) {
        links.push(element.getAttribute("href") ?? "");
      },
    })
    .on("table.overseas tbody tr", {
      element() {
        rows.push({});
      },
    });
  const selectors: Record<string, string> = { ...FIELD_SELECTORS };
  [1, 2, 3, 4].forEach((n) =>
    Object.entries(PAST_SELECTORS).forEach(([key, selector]) => {
      selectors[`p${n}.${key}`] = `.past.p${n} ${selector}`;
    }),
  );
  Object.entries(selectors).forEach(([key, selector]) => {
    rewriter.on(`table.overseas tbody tr ${selector}`, {
      text(chunk) {
        const row = rows.at(-1);
        if (row) row[key] = (row[key] ?? "") + chunk.text;
      },
    });
  });
  await rewriter.transform(new Response(input.html)).text();
  const title = clean(heading.join(""));
  if (
    !title.includes("2026年10月4日") ||
    !title.includes("凱旋門賞") ||
    !links.includes(new URL(ARC_OFFICIAL_URL).pathname + new URL(ARC_OFFICIAL_URL).search)
  )
    throw new Error("Official race scope mismatch");
  const horses = rows.map(horse);
  if (
    horses.length !== 16 ||
    horses.some(
      (row) =>
        row.name !== DECLARED_NAMES[row.number - 1] || row.gate !== DECLARED_GATES[row.number - 1],
    ) ||
    new Set(horses.map((row) => row.number)).size !== 16 ||
    new Set(horses.map((row) => row.gate)).size !== 16
  )
    throw new Error("Official roster incomplete");
  return { url: input.url, retrievedAt: input.retrievedAt, horses };
};
export const validateOfficialRoster = (card: OfficialCard, runners: readonly Runner[]): boolean =>
  runners.length === 16 &&
  new Set(runners.map((row) => Number(row.umaban))).size === 16 &&
  runners.every((row) =>
    card.horses.some(
      (h) =>
        h.number === Number(row.umaban) &&
        h.name === row.bamei?.trim() &&
        h.gate === Number(row.wakuban),
    ),
  );
export const enrichOfficialRunners = (runners: Runner[], card: OfficialCard | null): Runner[] => {
  if (!card || !validateOfficialRoster(card, runners)) return runners;
  return runners.map((row) => {
    const h = card.horses.find((item) => item.number === Number(row.umaban));
    return h
      ? {
          ...row,
          officialCardProvenance: { url: card.url, retrievedAt: card.retrievedAt },
          ownerNameFull: row.ownerNameFull?.trim() ? row.ownerNameFull : h.owner,
          sireName: row.sireName?.trim() ? row.sireName : h.sire,
          damSireName: row.damSireName?.trim() ? row.damSireName : h.damSire,
        }
      : row;
  });
};
export const enrichOfficialHistory = (input: {
  scope: ArcRaceScope;
  runners: Runner[];
  results: HorseRaceResult[];
  card: OfficialCard | null;
}): HorseRaceResult[] => {
  if (
    !isArcPredictionRace(input.scope) ||
    !input.card ||
    !validateOfficialRoster(input.card, input.runners)
  )
    return input.results;
  const card = input.card;
  return input.results.map((row) => {
    const runner = input.runners.find(
      (r) =>
        r.umaban === row.currentUmaban &&
        (r.sourceHorseId ?? r.kettoTorokuBango) === row.kettoTorokuBango,
    );
    const h = card.horses.find((r) => r.number === Number(runner?.umaban));
    const matches = h?.past.filter(
      (p) =>
        p.finish !== null &&
        p.date === row.kaisaiNen + row.kaisaiTsukihi &&
        p.distance === row.kyori &&
        Number(p.finish) === Number(row.kakuteiChakujun),
    );
    const match = matches?.length === 1 ? matches[0] : undefined;
    if (!match) return row;
    return {
      ...row,
      corner1: row.corner1 ?? match.corners[0] ?? null,
      corner2: row.corner2 ?? match.corners[1] ?? null,
      corner3: row.corner3 ?? match.corners[2] ?? null,
      corner4: row.corner4 ?? match.corners[3] ?? null,
      sohaTime: row.sohaTime ?? match.time,
      shussoTosu: Number(row.shussoTosu) > 0 ? row.shussoTosu : match.fieldSize,
      officialCardProvenance: {
        url: card.url,
        retrievedAt: card.retrievedAt,
        horseNumber: h?.number,
        date: match.date,
      },
    };
  });
};
