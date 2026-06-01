// Run with bun. Thin wrapper that forwards to the shared
// horse-racing-realtime/wakuban derivation. The original JRA-only derivation
// was source-agnostic in practice (the umaban -> frame distribution rule is
// identical for JRA, NAR, and Ban-ei), and the trend section's frame filter
// was dropping every NAR row when wakuban was hard-pinned to null. The
// underlying helper now lives in the shared realtime package so both the
// viewer and the realtime DO can reuse the same truth table.

import {
  deriveWakuban as deriveWakubanShared,
  type DeriveWakubanInput,
} from "horse-racing-realtime/wakuban";

export interface DeriveJraWakubanParams {
  horseCount: number;
  horseNumber: number;
}

const toSharedInput = (params: DeriveJraWakubanParams): DeriveWakubanInput => ({
  horseCount: params.horseCount,
  horseNumber: params.horseNumber,
});

export const deriveJraWakuban = (params: DeriveJraWakubanParams): number | null =>
  deriveWakubanShared(toSharedInput(params));
