// This file runs with Bun.
import type { ParsedJraRace, ParsedRunner, RaceGrade } from "../types";

interface RequiredMatchInput {
  html: string;
  pattern: RegExp;
  fieldName: string;
}

interface RunnerBasics {
  horseNumber: number;
  gate: number;
  horseName: string;
  sex: string;
  age: number;
  coatColour: string;
  weightCarriedKg: number;
}

const BODY_PATTERN: RegExp = /<tbody[^>]*>([\s\S]*?)<\/tbody>/i;
const RUNNER_PATTERN: RegExp = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
const TAG_PATTERN: RegExp = /<[^>]+>/g;
const WHITESPACE_PATTERN: RegExp = /\s+/g;
const TRAINER_COUNTRY_PATTERN: RegExp = /\s*\(([^)]+)\)$/;
const GRADE_PATTERN: RegExp = /\((G[123])\)\s*$/i;
const AGE_PATTERN: RegExp = /^(.+?)(\d+)\/(.+)$/;
const DATE_PATTERN: RegExp =
  /class="cell date"[^>]*>\s*(\d{4})年(\d{1,2})月(\d{1,2})日[^<]*?\s+([^<]+?)競馬場\s*<span class="country">（([^）]+)）<\/span>/i;
const COURSE_PATTERN: RegExp =
  /class="cell course"[^>]*>[\s\S]*?([\d,]+)\s*<span class="unit">メートル<\/span>\s*<span class="detail">（([^・）]+)・([^）]+)）<\/span>/i;
const START_TIME_PATTERN: RegExp = /発走時刻：[\s\S]*?<strong>(\d{1,2})時(\d{2})分<\/strong>/i;
const LOCAL_START_TIME_PATTERN: RegExp =
  /class="local_time"[^>]*>（現地時間：\d{1,2}月\d{1,2}日\s+(\d{1,2})時(\d{2})分）<\/span>/i;
const RACE_NAME_PATTERN: RegExp = /class="race_name"[^>]*>([\s\S]*?)<\/span>/i;
const HORSE_NUMBER_PATTERN: RegExp = /<td\s+class="num"[^>]*>\s*(\d+)\s*<\/td>/i;
const HORSE_NAME_PATTERN: RegExp =
  /<div\s+class="name"[^>]*>[\s\S]*?<div\s+class="txt"[^>]*>([\s\S]*?)<\/div>/i;
const GATE_PATTERN: RegExp = /<td\s+class="waku"[^>]*>\s*(\d+)\s*<\/td>/i;
const AGE_COAT_PATTERN: RegExp = /<p\s+class="age"[^>]*>([\s\S]*?)<\/p>/i;
const WEIGHT_PATTERN: RegExp = /<p\s+class="weight"[^>]*>\s*([\d.]+)/i;
const ODDS_PATTERN: RegExp =
  /<div\s+class="odds"[^>]*>[\s\S]*?<strong[^>]*>\s*([\d.]+)\s*<\/strong>/i;
const POPULARITY_PATTERN: RegExp = /class="pop_rank"[^>]*>\s*\((\d+)/i;
const FORM_PATTERN: RegExp = /class="cell result"[^>]*>\s*\(([^)]*)\)\s*<\/div>/i;
const OWNER_PATTERN: RegExp = /<p\s+class="owner"[^>]*>([\s\S]*?)<\/p>/i;
const TRAINER_PATTERN: RegExp = /<p\s+class="trainer"[^>]*>([\s\S]*?)<\/p>/i;
const JOCKEY_PATTERN: RegExp = /<p\s+class="jockey"[^>]*>([\s\S]*?)<\/p>/i;
const SIRE_PATTERN: RegExp =
  /<li\s+class="sire"[^>]*>[\s\S]*?<span[^>]*>父：<\/span>([\s\S]*?)<\/li>/i;
const DAM_PATTERN: RegExp =
  /<li\s+class="mare"[^>]*>[\s\S]*?<span[^>]*>母：<\/span>([\s\S]*?)<span\s+class="bloodmare">/i;
const DAMSIRE_PATTERN: RegExp = /<span\s+class="bloodmare">\(母の父：([\s\S]*?)\)<\/span>/i;

const decodeHtml = (value: string): string =>
  value
    .replaceAll("&amp;", "&")
    .replaceAll("&quot;", '"')
    .replaceAll("&#39;", "'")
    .replaceAll("&nbsp;", " ")
    .replaceAll("&#8544;", "Ⅰ")
    .replaceAll("&#8545;", "Ⅱ")
    .replaceAll("&#8546;", "Ⅲ");

const cleanText = (value: string): string =>
  decodeHtml(value.replace(TAG_PATTERN, " ").replace(WHITESPACE_PATTERN, " ").trim());

const requiredCapture = ({ html, pattern, fieldName }: RequiredMatchInput): string => {
  const matched: RegExpExecArray | null = pattern.exec(html);
  const value: string | undefined = matched?.[1];
  if (value === undefined) {
    throw new Error(`JRA card is missing ${fieldName}.`);
  }
  return value;
};

const requiredMatch = (input: RequiredMatchInput): string => cleanText(requiredCapture(input));

const optionalNumber = (html: string, pattern: RegExp): number | null => {
  const matched: RegExpExecArray | null = pattern.exec(html);
  const value: string | undefined = matched?.[1];
  return value === undefined ? null : Number(value);
};

const parseGrade = (raceNameWithGrade: string): RaceGrade => {
  const matched: RegExpMatchArray | null = raceNameWithGrade.match(GRADE_PATTERN);
  const grade: string | undefined = matched?.[1]?.toUpperCase();
  if (grade === "G1" || grade === "G2" || grade === "G3") {
    return grade;
  }
  return null;
};

const parseRunnerBasics = (html: string): RunnerBasics => {
  const ageAndCoat: string = requiredMatch({
    html,
    pattern: AGE_COAT_PATTERN,
    fieldName: "runner sex, age, and coat colour",
  });
  const matchedAge: RegExpMatchArray | null = ageAndCoat.match(AGE_PATTERN);
  const sex: string | undefined = matchedAge?.[1];
  const age: string | undefined = matchedAge?.[2];
  const coatColour: string | undefined = matchedAge?.[3];
  if (sex === undefined || age === undefined || coatColour === undefined) {
    throw new Error("JRA card has an invalid runner sex, age, or coat colour.");
  }

  return {
    horseNumber: Number(
      requiredMatch({ html, pattern: HORSE_NUMBER_PATTERN, fieldName: "runner horse number" }),
    ),
    gate: Number(requiredMatch({ html, pattern: GATE_PATTERN, fieldName: "runner gate" })),
    horseName: requiredMatch({ html, pattern: HORSE_NAME_PATTERN, fieldName: "runner horse name" }),
    sex,
    age: Number(age),
    coatColour,
    weightCarriedKg: Number(
      requiredMatch({ html, pattern: WEIGHT_PATTERN, fieldName: "runner carried weight" }),
    ),
  };
};

const parseRunner = (html: string): ParsedRunner => {
  const basics: RunnerBasics = parseRunnerBasics(html);
  const trainerHtml: string = requiredMatch({
    html,
    pattern: TRAINER_PATTERN,
    fieldName: "runner trainer",
  });
  const trainerCountry: string = trainerHtml.match(TRAINER_COUNTRY_PATTERN)?.[1] ?? "";

  return {
    ...basics,
    jockeyAbbrev: requiredMatch({ html, pattern: JOCKEY_PATTERN, fieldName: "runner jockey" }),
    trainerAbbrev: trainerHtml.replace(TRAINER_COUNTRY_PATTERN, "").trim(),
    trainerCountry,
    owner: requiredMatch({ html, pattern: OWNER_PATTERN, fieldName: "runner owner" }),
    winOdds: optionalNumber(html, ODDS_PATTERN),
    popularity: optionalNumber(html, POPULARITY_PATTERN),
    formRecord: requiredMatch({ html, pattern: FORM_PATTERN, fieldName: "runner form record" }),
    sire: requiredMatch({ html, pattern: SIRE_PATTERN, fieldName: "runner sire" }),
    dam: requiredMatch({ html, pattern: DAM_PATTERN, fieldName: "runner dam" }),
    damsire: requiredMatch({ html, pattern: DAMSIRE_PATTERN, fieldName: "runner damsire" }),
  };
};

export const parseJraCard = (html: string): ParsedJraRace => {
  const dateMatch: RegExpExecArray | null = DATE_PATTERN.exec(html);
  const year: string | undefined = dateMatch?.[1];
  const month: string | undefined = dateMatch?.[2];
  const day: string | undefined = dateMatch?.[3];
  const venue: string | undefined = dateMatch?.[4];
  const country: string | undefined = dateMatch?.[5];
  if (
    year === undefined ||
    month === undefined ||
    day === undefined ||
    venue === undefined ||
    country === undefined
  ) {
    throw new Error("JRA card is missing race date, venue, or country.");
  }

  const courseMatch: RegExpExecArray | null = COURSE_PATTERN.exec(html);
  const distance: string | undefined = courseMatch?.[1];
  const surface: string | undefined = courseMatch?.[2];
  const direction: string | undefined = courseMatch?.[3];
  if (distance === undefined || surface === undefined || direction === undefined) {
    throw new Error("JRA card is missing race course details.");
  }

  const startTimeMatch: RegExpExecArray | null = START_TIME_PATTERN.exec(html);
  const hour: string | undefined = startTimeMatch?.[1];
  const minute: string | undefined = startTimeMatch?.[2];
  if (hour === undefined || minute === undefined) {
    throw new Error("JRA card is missing race start time.");
  }

  const localStartTimeMatch: RegExpExecArray | null = LOCAL_START_TIME_PATTERN.exec(html);
  const localHour: string | undefined = localStartTimeMatch?.[1];
  const localMinute: string | undefined = localStartTimeMatch?.[2];
  if (localHour === undefined || localMinute === undefined) {
    throw new Error("JRA card is missing local race start time.");
  }

  const raceNameWithGrade: string = requiredMatch({
    html,
    pattern: RACE_NAME_PATTERN,
    fieldName: "race name",
  });
  const body: string = requiredCapture({ html, pattern: BODY_PATTERN, fieldName: "runner table" });
  const runnerMatches: RegExpMatchArray[] = Array.from(body.matchAll(RUNNER_PATTERN));
  if (runnerMatches.length === 0) {
    throw new Error("JRA card has no runners.");
  }

  const runners: ParsedRunner[] = runnerMatches.map((matched: RegExpMatchArray): ParsedRunner => {
    const runnerHtml: string | undefined = matched[1];
    if (runnerHtml === undefined) {
      throw new Error("JRA card has an invalid runner row.");
    }
    return parseRunner(runnerHtml);
  });

  return {
    raceName: raceNameWithGrade.replace(GRADE_PATTERN, "").trim(),
    grade: parseGrade(raceNameWithGrade),
    date: `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`,
    venue: cleanText(venue),
    country: cleanText(country),
    distanceMetres: Number(distance.replaceAll(",", "")),
    surface: cleanText(surface),
    direction: cleanText(direction),
    startTime: `${hour.padStart(2, "0")}:${minute}`,
    localStartTime: `${localHour.padStart(2, "0")}:${localMinute}`,
    runners,
  };
};
