import { existsSync } from "node:fs";
import path from "node:path";

import { TRACK_LABELS } from "./codes";
import { cleanText, formatDistance } from "./format";

const toHalfWidth = (value: string): string =>
  value.replace(/[０-９Ａ-Ｚａ-ｚ．％]/g, (char) =>
    char === "．" ? "." : char === "％" ? "%" : String.fromCharCode(char.charCodeAt(0) - 0xfee0),
  );

export interface CourseFact {
  label: string;
  value: string;
}

const normalizeLabel = (value: string): string => value.replace(/\s+/g, "");

const getFirstMatchValue = (text: string, patterns: RegExp[]): string | undefined => {
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match?.[1]) {
      return match[1];
    }
  }
  return undefined;
};

export const estimateCornerCount = (
  courseText: string,
  trackCode: string | null | undefined,
): number | null => {
  const trackLabel = TRACK_LABELS[cleanText(trackCode, "")] ?? "";
  if (trackLabel.includes("直線")) {
    return 0;
  }
  if (trackLabel.includes("2周")) {
    return 8;
  }

  const normalized = toHalfWidth(courseText);
  const firstCorner = normalized.match(/最初の([1-4])コーナー/);
  if (firstCorner?.[1]) {
    return 5 - Number(firstCorner[1]);
  }
  if (/[1-4]～[1-4]コーナー/.test(normalized)) {
    return 4;
  }

  return null;
};

const extractStandardLapFacts = (normalized: string): CourseFact[] => {
  const facts: CourseFact[] = [];
  const lapSection = normalized.match(/水準ラップ[^　]*(.*)$/)?.[1] ?? "";
  const lapPattern = /([^()､]+)\(([^()]*[0-9][^()]*)\)/g;

  for (const match of lapSection.matchAll(lapPattern)) {
    const label = normalizeLabel(match[1] ?? "");
    const value = match[2]?.trim();
    if (label && value && value !== "─" && !label.startsWith("●クラス別水準ラップ")) {
      facts.push({ label: `${label} 水準`, value });
    }
  }

  return facts;
};

export const getCourseFacts = (
  courseText: string,
  distance: string | null | undefined,
  trackCode?: string | null,
): CourseFact[] => {
  const normalized = toHalfWidth(courseText);
  const facts = [
    { label: "距離", value: formatDistance(distance) },
    {
      label: "コーナー回数",
      value:
        estimateCornerCount(courseText, trackCode) === null
          ? undefined
          : String(estimateCornerCount(courseText, trackCode)),
    },
    {
      label: "高低差",
      value: getFirstMatchValue(normalized, [
        /高低差が(?:約)?([0-9.]+)m/,
        /高低差は(?:約)?([0-9.]+)m/,
      ]),
    },
    {
      label: "最後の直線",
      value: getFirstMatchValue(normalized, [
        /最後の直線距離は(?:約)?([0-9.]+)m/,
        /直線距離は(?:約)?([0-9.]+)m/,
      ]),
    },
    {
      label: "1コーナーまで",
      value: normalized.match(/最初の1コーナーまでの距離は(?:約)?([0-9.]+)m/)?.[1],
    },
    { label: "フルゲート", value: normalized.match(/フルゲートは([0-9]+)頭/)?.[1] },
    { label: "比較対象", value: normalized.match(/JRA全([0-9]+)場/)?.[1] },
    {
      label: "良・稍重 逃げ連対率",
      value: normalized.match(/良馬場･稍重での逃げ馬の連対率は(?:約)?([0-9.]+%前後)/)?.[1],
    },
    {
      label: "重以上 逃げ連対率",
      value: normalized.match(/重馬場以上になると(?:約)?([0-9.]+%まで)/)?.[1],
    },
    {
      label: "外枠注意",
      value: normalized.match(/多頭数の([0-9]+番､[0-9]+番ゲート)/)?.[1],
    },
  ];

  const baseFacts = facts
    .map((fact) => ({
      label: fact.label,
      value:
        fact.label === "距離" ||
        fact.label === "良・稍重 逃げ連対率" ||
        fact.label === "重以上 逃げ連対率" ||
        fact.label === "外枠注意" ||
        fact.value === undefined
          ? fact.value
          : fact.label === "フルゲート"
            ? `${fact.value}頭`
            : fact.label === "コーナー回数"
              ? `${fact.value}回`
              : fact.label === "比較対象"
                ? `${fact.value}場`
                : `${fact.value}m`,
    }))
    .filter((fact): fact is CourseFact => Boolean(fact.value) && fact.value !== "-");

  return [...baseFacts, ...extractStandardLapFacts(normalized)];
};

export const formatCourseParagraphs = (courseText: string): string[] =>
  courseText
    .replaceAll("｡", "。\n")
    .replace(/\s*●/g, "\n●")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

export const getCourseImagePath = (
  keibajoCode: string,
  trackCode: string | null | undefined,
  distance: string | null | undefined,
): string | null => {
  const cleanTrackCode = cleanText(trackCode, "");
  const cleanDistance = cleanText(distance, "");
  if (!cleanTrackCode || !cleanDistance) {
    return null;
  }

  const baseName = `${keibajoCode}-${cleanTrackCode}-${cleanDistance}`;
  const publicDir = path.join(process.cwd(), "public", "courses");
  for (const extension of ["webp", "png", "jpg", "jpeg", "svg"]) {
    const fileName = `${baseName}.${extension}`;
    if (existsSync(path.join(publicDir, fileName))) {
      return `/courses/${fileName}`;
    }
  }

  return null;
};
