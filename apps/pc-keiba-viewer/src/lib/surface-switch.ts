// Run with bun (exercised via `bunx vitest run`).

export type SurfaceSwitch = "ダート替わり" | "芝替わり";

const SURFACE_SWITCH_CLASS_MAP: Record<SurfaceSwitch, string> = {
  芝替わり: "surface-turf",
  ダート替わり: "surface-dirt",
};

export const getSurfaceSwitchClassName = (value: SurfaceSwitch): string =>
  SURFACE_SWITCH_CLASS_MAP[value];

type PastSurface = "dirt" | "other" | "turf";

const TURF_TRACK_CODES: ReadonlySet<string> = new Set([
  "10",
  "11",
  "12",
  "13",
  "14",
  "15",
  "16",
  "17",
  "18",
  "19",
  "20",
  "21",
  "22",
  "51",
  "54",
  "55",
  "58",
  "59",
]);

const DIRT_TRACK_CODES: ReadonlySet<string> = new Set([
  "23",
  "24",
  "25",
  "26",
  "27",
  "28",
  "29",
  "53",
]);

const classifyPastSurface = (code: string | null | undefined): PastSurface => {
  if (code == null) {
    return "other";
  }
  if (TURF_TRACK_CODES.has(code)) {
    return "turf";
  }
  return DIRT_TRACK_CODES.has(code) ? "dirt" : "other";
};

export const classifySurfaceSwitch = (
  raceTrackCode: string | null | undefined,
  pastTrackCodes: ReadonlyArray<string | null | undefined>,
): SurfaceSwitch | null => {
  if (raceTrackCode == null) {
    return null;
  }
  const isTurf = TURF_TRACK_CODES.has(raceTrackCode);
  const isDirt = DIRT_TRACK_CODES.has(raceTrackCode);
  if (!isTurf && !isDirt) {
    return null;
  }
  if (pastTrackCodes.length === 0) {
    return null;
  }
  const pastSurfaces = pastTrackCodes.map(classifyPastSurface);
  const allDirt = pastSurfaces.every((s) => s === "dirt");
  const allTurf = pastSurfaces.every((s) => s === "turf");
  if (isTurf && allDirt) {
    return "芝替わり";
  }
  if (isDirt && allTurf) {
    return "ダート替わり";
  }
  return null;
};
