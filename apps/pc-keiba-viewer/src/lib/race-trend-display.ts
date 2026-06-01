// Race-trend display range helper. Owned by the lib layer so the race-detail
// server component can keep its render pipeline thin while the threshold rule
// stays unit-testable.
//
// The cache layer always fetches the full 14-day window regardless; this only
// affects the DEFAULT display range surfaced to the user. R1 is the first race
// of the day at a venue so it has no today siblings yet — we keep the
// past-14-days default for it so the panel is not empty on the opening race.
// R2 and later collapse to today only so today's sibling races are the first
// thing visible, which is what the user prefers on a busy race day.
const TODAY_ONLY_TREND_RACE_BANGO_THRESHOLD = 2;

export interface TrendDisplayRestrictionParams {
  raceBango: string;
}

export const shouldRestrictTrendDisplayToToday = (params: TrendDisplayRestrictionParams): boolean =>
  Number.parseInt(params.raceBango, 10) >= TODAY_ONLY_TREND_RACE_BANGO_THRESHOLD;
