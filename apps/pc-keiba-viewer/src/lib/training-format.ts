import { cleanText } from "./format";

const TRACEN_LABELS: Record<string, string> = {
  "0": "美浦",
  "1": "栗東",
};

const WOOD_COURSE_LABELS: Record<string, string> = {
  "1": "A",
  "2": "B",
  "3": "C",
  "4": "D",
};

const WOOD_DIRECTION_LABELS: Record<string, string> = {
  "0": "内",
  "1": "外",
};

export const formatTrainingTime = (value: string | null | undefined): string => {
  const cleaned = cleanText(value, "");
  if (!cleaned || /^0+$/.test(cleaned) || /^9+$/.test(cleaned)) {
    return "-";
  }

  const parsed = Number(cleaned);
  if (!Number.isFinite(parsed)) {
    return "-";
  }

  return (parsed / 10).toFixed(1);
};

export const formatTracen = (value: string | null | undefined): string => {
  const cleaned = cleanText(value, "");
  return cleaned ? (TRACEN_LABELS[cleaned] ?? `トレセン${cleaned}`) : "-";
};

export const formatWoodCourse = (
  course: string | null | undefined,
  direction: string | null | undefined,
): string => {
  const cleanCourse = cleanText(course, "");
  const cleanDirection = cleanText(direction, "");
  if (!cleanCourse && !cleanDirection) {
    return "-";
  }

  const courseLabel = WOOD_COURSE_LABELS[cleanCourse] ?? cleanCourse;
  const directionLabel = WOOD_DIRECTION_LABELS[cleanDirection] ?? cleanDirection;
  return [courseLabel ? `${courseLabel}コース` : "", directionLabel].filter(Boolean).join(" / ");
};
