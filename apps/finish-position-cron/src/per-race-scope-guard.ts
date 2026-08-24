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

const RACE_TARGET_CODE_PATTERN = /^\d{1,2}$/u;
const MIN_KEIBAJO_CODE = 1;
const MIN_RACE_BANGO = 1;
const MAX_RACE_BANGO = 12;

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

// Queue keys, R2 objects, KV entries, and viewer URLs all use the same
// zero-padded two-digit race target. One-digit legacy input is normalized;
// malformed input is rejected before it can create a poison Queue message.
// `00` is not a venue and races are 1..12.
export const hasValidPerRaceScope = (fields: PerRaceScopeFields): fields is RequiredPerRaceScope =>
  normalizePerRaceScope(fields) !== null;

export const normalizePerRaceScope = (fields: PerRaceScopeFields): RequiredPerRaceScope | null => {
  if (!hasRequiredPerRaceScope(fields)) return null;
  const keibajoCode = fields.keibajoCode.trim();
  const raceBango = fields.raceBango.trim();
  if (!RACE_TARGET_CODE_PATTERN.test(keibajoCode)) return null;
  if (Number(keibajoCode) < MIN_KEIBAJO_CODE) return null;
  if (!RACE_TARGET_CODE_PATTERN.test(raceBango)) return null;
  if (Number(raceBango) < MIN_RACE_BANGO || Number(raceBango) > MAX_RACE_BANGO) return null;
  return {
    keibajoCode: keibajoCode.padStart(2, "0"),
    raceBango: raceBango.padStart(2, "0"),
  };
};

export const PER_RACE_SCOPE_REQUIRED_ERROR =
  "per-race scope required: both keibajoCode and raceBango must be set";

export const PER_RACE_SCOPE_INVALID_ERROR =
  "invalid per-race scope: keibajoCode must be a non-zero numeric code and raceBango must be 1..12";
