// Run with bun. Optional resource bindings must preserve legacy Env compatibility.

import { expectTypeOf, test } from "vitest";
import type { ContainerControlMessage, Env, FocusedFullWatchPayload } from "./types";

test("keeps the rescore binding and gates optional", () => {
  expectTypeOf<Env["FINISH_POSITION_RESCORE_CONTAINER"]>().toEqualTypeOf<
    Env["FINISH_POSITION_PREDICT_CONTAINER"] | undefined
  >();
  expectTypeOf<Env["RESCORE_CONTAINER_ENABLED"]>().toEqualTypeOf<string | undefined>();
  expectTypeOf<Env["RESCORE_CONTAINER_RACES"]>().toEqualTypeOf<string | undefined>();
});

test("supports rescore cleanup without widening focused-full execution roles", () => {
  expectTypeOf<ContainerControlMessage["role"]>().toEqualTypeOf<
    "legacy" | "race-chain" | "rescore" | undefined
  >();
  expectTypeOf<FocusedFullWatchPayload["role"]>().toEqualTypeOf<"legacy" | "race-chain">();
});
