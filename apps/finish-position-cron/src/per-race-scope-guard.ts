// Run with bun. Production finish-position prediction generation is
// per-race only: every predict enqueue / container /predict dispatch must
// carry an explicit keibajoCode AND raceBango. Day-scoped ("all") generation
// is rejected outright -- not behind a config flag, and not silently
// downgraded to a whole-card run.
//
// Day-base PREWARM (/prewarm-day-base) is intentionally outside this guard:
// it builds a shared day-stable feature artifact and never scores or UPSERTs
// predictions. See day-base-prewarm.ts.

export interface PerRaceScopeFields {
  keibajoCode?: string;
  raceBango?: string;
}

export interface RequiredPerRaceScope {
  keibajoCode: string;
  raceBango: string;
}

// True only when both race-target fields are present as non-empty strings.
// Partial scope (keibajo-only or race-only) is treated as missing -- production
// must never fall through to a whole-day generation path.
export const hasRequiredPerRaceScope = (
  fields: PerRaceScopeFields,
): fields is RequiredPerRaceScope => {
  const keibajoCode = fields.keibajoCode;
  const raceBango = fields.raceBango;
  return (
    typeof keibajoCode === "string" &&
    keibajoCode.trim() !== "" &&
    typeof raceBango === "string" &&
    raceBango.trim() !== ""
  );
};

export const PER_RACE_SCOPE_REQUIRED_ERROR =
  "per-race scope required: both keibajoCode and raceBango must be set";
