import type { CSSProperties } from "react";

import { cleanText } from "../../../lib/format";

const VALID_FRAME_NUMBERS = new Set(["1", "2", "3", "4", "5", "6", "7", "8"]);
type HorseVisualStyle = CSSProperties & {
  "--coat-color": string;
  "--frame-color": string;
};
type CoatVisualStyle = CSSProperties & {
  "--coat-color": string;
};

export const FRAME_COLORS: Record<string, string> = {
  "1": "#ffffff",
  "2": "#111111",
  "3": "#d71920",
  "4": "#005bac",
  "5": "#ffd400",
  "6": "#009944",
  "7": "#f39800",
  "8": "#f4a3c4",
};

const COAT_COLORS: Record<string, { color: string; label: string }> = {
  "01": { color: "#b96a2c", label: "栗毛" },
  "02": { color: "#8c4b24", label: "栃栗毛" },
  "03": { color: "#6f4428", label: "鹿毛" },
  "04": { color: "#2e2723", label: "黒鹿毛" },
  "05": { color: "#171f27", label: "青鹿毛" },
  "06": { color: "#0f1720", label: "青毛" },
  "07": { color: "#c9c9c2", label: "芦毛" },
  "08": { color: "#d5a067", label: "栗粕毛" },
  "09": { color: "#a58b72", label: "鹿粕毛" },
  "10": { color: "#78828a", label: "青粕毛" },
  "11": { color: "#ffffff", label: "白毛" },
  "30": { color: "#c8874c", label: "栗駁毛" },
  "31": { color: "#8e6749", label: "鹿駁毛" },
  "32": { color: "#4b5563", label: "青駁毛" },
  "33": { color: "#c8874c", label: "駁栗毛" },
  "34": { color: "#8e6749", label: "駁鹿毛" },
  "35": { color: "#4b5563", label: "駁青毛" },
  "36": { color: "#d9c48f", label: "月毛" },
  "37": { color: "#d0b06d", label: "川原毛" },
  "38": { color: "#f1dfc5", label: "佐目毛" },
  "39": { color: "#9ca3af", label: "薄墨毛" },
};

export const getFrameColor = (value: string | null | undefined): string | null => {
  const frameNumber = cleanText(value, "");
  return FRAME_COLORS[frameNumber] ?? null;
};

const getCoatColor = (
  value: string | null | undefined,
): { color: string; label: string } | null => {
  const coatCode = cleanText(value, "").padStart(2, "0");
  return COAT_COLORS[coatCode] ?? null;
};

const getStyle = (frameNumber: string, coatCode?: string | null): HorseVisualStyle => ({
  "--coat-color": getCoatColor(coatCode)?.color ?? "transparent",
  "--frame-color": FRAME_COLORS[frameNumber] ?? "transparent",
});

export function FrameNumberBadge({ value }: { value: string | null | undefined }) {
  const frameNumber = cleanText(value, "");
  if (!VALID_FRAME_NUMBERS.has(frameNumber)) {
    return <>{frameNumber || "-"}</>;
  }

  return <span className={`frame-number-badge frame-${frameNumber}`}>{frameNumber}</span>;
}

export function HorseNumberBadge({
  coatCode,
  frameNumber,
  horseNumber,
}: {
  coatCode?: string | null;
  frameNumber: string | null | undefined;
  horseNumber: string;
}) {
  const normalizedFrame = cleanText(frameNumber, "");
  if (!VALID_FRAME_NUMBERS.has(normalizedFrame)) {
    return <span className="horse-number-badge">{horseNumber}</span>;
  }
  return (
    <span className="horse-number-badge" style={getStyle(normalizedFrame, coatCode)}>
      {horseNumber}
    </span>
  );
}

export function PlainHorseNumberBadge({ horseNumber }: { horseNumber: string }) {
  return <span className="horse-number-badge plain">{horseNumber}</span>;
}

export function HorseNameBadge({
  coatCode,
  name,
  showCoatLabel = true,
}: {
  coatCode?: string | null;
  name: string;
  showCoatLabel?: boolean;
}) {
  const coat = getCoatColor(coatCode);
  const style: CoatVisualStyle | undefined = coat ? { "--coat-color": coat.color } : undefined;

  return (
    <span
      className="horse-name-badge"
      style={style}
      title={coat ? `${name} / ${coat.label}` : name}
    >
      {coat ? <i aria-label={coat.label} /> : null}
      <span>{name}</span>
      {coat && showCoatLabel ? <small>{coat.label}</small> : null}
    </span>
  );
}

export function HorseNameWithCoatDot({
  coatCode,
  name,
}: {
  coatCode?: string | null;
  name: string;
}) {
  const coat = getCoatColor(coatCode);
  const style: CoatVisualStyle | undefined = coat ? { "--coat-color": coat.color } : undefined;

  return (
    <span
      className="horse-name-coat-dot"
      style={style}
      title={coat ? `${name} / ${coat.label}` : name}
    >
      {coat ? <i aria-label={coat.label} /> : null}
      <span>{name}</span>
    </span>
  );
}
