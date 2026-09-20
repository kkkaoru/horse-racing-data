// Run with bun. Optional race-detail data (currently the prev/next venue
// navigation list) must never fail the whole page. A failed read degrades to
// a documented fallback and logs a structured event; required data (race,
// runners, course info) keeps failing loudly.
export const loadOptionalRaceDetailData = async <T>(
  load: () => Promise<T>,
  fallback: T,
  logFields: Record<string, string>,
): Promise<T> => {
  try {
    return await load();
  } catch {
    console.error(JSON.stringify({ event: "race_detail_optional_data_failed", ...logFields }));
    return fallback;
  }
};
