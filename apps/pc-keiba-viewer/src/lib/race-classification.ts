import type { RaceSource } from "./codes";
import type { RaceListItem } from "./race-types";

const clean = (value: string | null | undefined): string => value?.trim() ?? "";

const getFirstToken = (value: string): string => clean(value).split(/\s+/)[0] ?? "";

const toHalfWidthAlphaNumeric = (value: string): string =>
  value
    .replace(/[Ａ-Ｚａ-ｚ０-９]/g, (char) => String.fromCharCode(char.charCodeAt(0) - 0xfee0))
    .replace(/[－ー―‐]/g, "-")
    .replace(/\s+/g, " ")
    .trim();

const AGE_LABELS: Record<string, string> = {
  "01": "2歳",
  "02": "3歳",
  "03": "4歳",
  "04": "3歳以上",
  "05": "4歳以上",
  "06": "5歳以上",
  "07": "2・3歳",
  "08": "3・4歳",
  "09": "一般",
  "11": "2歳",
  "12": "3歳",
  "13": "3歳以上",
  "14": "4歳以上",
  "18": "障害3歳以上",
  "19": "障害4歳以上",
  "21": "アラブ2歳",
  "22": "アラブ3歳",
  "23": "アラブ3歳以上",
  "24": "アラブ4歳以上",
  "43": "4歳",
  "46": "5歳以上",
  "47": "2・3歳",
  "48": "3・4歳",
  "49": "一般",
};

const CONDITION_LABELS: Record<string, string> = {
  "005": "1勝クラス",
  "010": "2勝クラス",
  "016": "3勝クラス",
  "701": "新馬",
  "702": "未出走",
  "703": "未勝利",
  "999": "オープン",
};

export const getAgeLabel = (value: string | null | undefined): string => {
  const code = clean(value);
  return code ? (AGE_LABELS[code] ?? `年齢条件 ${code}`) : "-";
};

export const getConditionLabel = (value: string | null | undefined): string => {
  const code = clean(value);
  return code ? (CONDITION_LABELS[code] ?? `条件 ${code}`) : "-";
};

export const getRaceClassLabel = (
  kyosoShubetsuCode: string | null | undefined,
  kyosoJokenCode: string | null | undefined,
): string =>
  [getAgeLabel(kyosoShubetsuCode), getConditionLabel(kyosoJokenCode)]
    .filter((value) => value !== "-")
    .join(" ");

const GRADE_LABELS: Record<string, string> = {
  A: "G1",
  B: "G2",
  C: "G3",
  D: "重賞",
  E: "特別",
  F: "Jpn1",
  G: "Jpn2",
  H: "Jpn3",
  L: "リステッド",
};

const NAR_GRADE_LABELS: Record<string, string> = {
  A: "Jpn1",
  B: "Jpn2",
  C: "Jpn3",
  D: "重賞",
  E: "特別",
  P: "地区限定重賞 1",
  Q: "地区限定重賞 2",
  R: "地区限定重賞 3",
  S: "地区限定重賞",
  T: "準重賞",
};

export const getGradeLabel = (
  value: string | null | undefined,
  source?: RaceSource | null,
): string => {
  const code = clean(value);
  if (!code && source === "nar") {
    return "普通";
  }

  return code
    ? ((source === "nar" ? NAR_GRADE_LABELS[code] : undefined) ??
        GRADE_LABELS[code] ??
        `グレード ${code}`)
    : "-";
};

const WEIGHT_LABELS: Record<string, string> = {
  "0": "指定なし",
  "1": "ハンデ",
  "2": "別定",
  "3": "馬齢",
  "4": "定量",
  "6": "騎手ハンデ",
  "7": "賞金ハンデ",
  "8": "規定",
  "9": "その他",
};

export const getWeightLabel = (value: string | null | undefined): string => {
  const code = clean(value);
  return code ? (WEIGHT_LABELS[code] ?? `重量種別 ${code}`) : "-";
};

const KIGO_LABELS: Record<string, string> = {
  "000": "制限なし",
  "002": "若手騎手",
  "020": "牝馬限定",
  "021": "牝馬限定",
  "023": "牝馬限定",
  "024": "牝馬限定",
  A02: "若手騎手",
  A20: "牝馬限定",
  A21: "牝馬限定",
  A23: "牝馬限定",
  A24: "牝馬限定",
  N20: "牝馬限定",
  N21: "牝馬限定",
  N23: "牝馬限定",
  N24: "牝馬限定",
};

const KIGO_DETAIL_LABELS: Record<string, string> = {
  "000": "制限なし",
  "001": "(指定)",
  "002": "若手騎手",
  "003": "[指定]",
  "004": "(特指)",
  "010": "牡馬限定",
  "020": "牝馬限定",
  "021": "牝馬限定 (指定)",
  "023": "牝馬限定 [指定]",
  "024": "牝馬限定 (特指)",
  "030": "牡馬・せん馬限定",
  "040": "牡馬・牝馬限定",
  "041": "牡馬・牝馬限定 (指定)",
  A00: "混合",
  A01: "混合 (指定)",
  A02: "混合 若手騎手",
  A03: "混合 [指定]",
  A04: "混合 (特指)",
  A20: "混合 牝馬限定",
  A21: "混合 牝馬限定 (指定)",
  A23: "混合 牝馬限定 [指定]",
  A24: "混合 牝馬限定 (特指)",
  A30: "混合 牡馬・せん馬限定",
  A31: "混合 牡馬・せん馬限定 (指定)",
  A34: "混合 牡馬・せん馬限定 (特指)",
  B00: "父内国産馬限定",
  C00: "市場取引馬限定",
  D00: "抽せん馬限定",
  E00: "抽せん馬限定",
  N00: "国際競走",
  N01: "国際競走 (指定)",
  N03: "国際競走 [指定]",
  N04: "国際競走 (特指)",
  N20: "国際競走 牝馬限定",
  N21: "国際競走 牝馬限定 (指定)",
  N23: "国際競走 牝馬限定 [指定]",
  N24: "国際競走 牝馬限定 (特指)",
  X00: "認定競走",
  Y00: "指定競走",
};

export const getRaceSymbolLabel = (value: string | null | undefined): string => {
  const code = clean(value);
  return code ? (KIGO_LABELS[code] ?? `競走記号 ${code}`) : "-";
};

export const getRaceSymbolDetailLabel = (value: string | null | undefined): string => {
  const code = clean(value);
  return code ? (KIGO_DETAIL_LABELS[code] ?? getRaceSymbolLabel(code)) : "-";
};

const appendUnique = (items: string[], value: string): void => {
  if (value && !items.includes(value)) {
    items.push(value);
  }
};

const getConditionNameLabel = (value: string | null | undefined): string => {
  const normalized = toHalfWidthAlphaNumeric(clean(value));
  if (!normalized) {
    return "";
  }

  if (/[0-9]歳上OP/.test(normalized)) {
    return clean(value).split(/\s+/)[0] ?? "";
  }

  if (/オープン|OP/.test(normalized)) {
    return clean(value);
  }

  const localClass = getFirstToken(normalized);
  return /^[A-Z][0-9]+(?:-[0-9]+)?$/.test(localClass) ? localClass : "";
};

type RaceTagInput = Pick<
  RaceListItem,
  | "kyosoShubetsuCode"
  | "kyosoKigoCode"
  | "juryoShubetsuCode"
  | "kyosoJokenCode"
  | "kyosoJokenMeisho"
  | "gradeCode"
> & {
  source?: RaceSource | null;
};

export const getRaceTags = (race: RaceTagInput): string[] => {
  const tags: string[] = [];
  appendUnique(tags, AGE_LABELS[clean(race.kyosoShubetsuCode)] ?? "");

  const grade = getGradeLabel(race.gradeCode, race.source);
  const condition =
    CONDITION_LABELS[clean(race.kyosoJokenCode)] ?? getConditionNameLabel(race.kyosoJokenMeisho);
  if (/^(?:G|Jpn)[1-3]$/.test(grade)) {
    appendUnique(tags, grade);
  } else if (grade === "リステッド") {
    appendUnique(tags, "リステッド競走");
  } else {
    appendUnique(tags, condition);
  }

  appendUnique(tags, KIGO_LABELS[clean(race.kyosoKigoCode)] ?? "");

  const weight = getWeightLabel(race.juryoShubetsuCode);
  if (weight.includes("ハンデ")) {
    appendUnique(tags, "ハンデ戦");
  }

  if (tags.length === 0 && condition) {
    appendUnique(tags, condition);
  }

  return tags;
};

export const getRaceTagText = (race: Parameters<typeof getRaceTags>[0]): string =>
  getRaceTags(race).join(" ");
