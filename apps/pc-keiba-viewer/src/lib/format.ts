import { BABA_LABELS, KEIBAJO_NAMES, TRACK_LABELS, WEATHER_LABELS } from "./codes";

const trim = (value: string | null | undefined): string => value?.trim() ?? "";

export const cleanText = (value: string | null | undefined, fallback = "-"): string => {
  const cleaned = trim(value);
  return cleaned.length > 0 ? cleaned : fallback;
};

export const formatDate = (year: string, monthDay: string): string => {
  const month = monthDay.slice(0, 2);
  const day = monthDay.slice(2, 4);
  return `${year}-${month}-${day}`;
};

export const formatDisplayDate = (year: string, monthDay: string): string => {
  const month = Number(monthDay.slice(0, 2));
  const day = Number(monthDay.slice(2, 4));
  return `${year}年${month}月${day}日`;
};

export const formatTime = (hhmm: string | null | undefined): string => {
  const value = trim(hhmm);
  if (value.length !== 4) {
    return "--:--";
  }
  return `${value.slice(0, 2)}:${value.slice(2, 4)}`;
};

export const formatRaceNumber = (raceNumber: string | null | undefined): string => {
  const parsed = Number(trim(raceNumber));
  return Number.isFinite(parsed) && parsed > 0 ? `${parsed}R` : "-";
};

export const formatKeibajo = (code: string): string => KEIBAJO_NAMES[code] ?? `競馬場 ${code}`;

export const formatTrack = (code: string | null | undefined): string => {
  const value = trim(code);
  return value.length > 0 ? (TRACK_LABELS[value] ?? `コース ${value}`) : "-";
};

export const getTrackSurfaceLabel = (code: string | null | undefined): string => {
  const label = formatTrack(code);
  return label === "-" ? "-" : label.split("・")[0] || label;
};

export const getTrackTurnLabel = (code: string | null | undefined): string => {
  const label = formatTrack(code);
  if (label.includes("左")) {
    return "左";
  }
  if (label.includes("右")) {
    return "右";
  }
  if (label.includes("直線")) {
    return "直線";
  }
  return "-";
};

export const formatWeather = (code: string | null | undefined): string => {
  const value = trim(code);
  return value.length > 0 ? (WEATHER_LABELS[value] ?? `天候 ${value}`) : "-";
};

export const formatBaba = (code: string | null | undefined): string => {
  const value = trim(code);
  return value.length > 0 ? (BABA_LABELS[value] ?? `馬場 ${value}`) : "-";
};

export const formatDistance = (distance: string | null | undefined): string => {
  const parsed = Number(trim(distance));
  return Number.isFinite(parsed) && parsed > 0 ? `${parsed}m` : "-";
};

export const formatCount = (count: string | number | bigint | null | undefined): string =>
  Number(count ?? 0).toLocaleString("ja-JP");
