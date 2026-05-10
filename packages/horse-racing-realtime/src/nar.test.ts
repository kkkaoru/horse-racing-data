import { describe, expect, it } from "vitest";

import {
  buildNarRaceKey,
  LOCAL_KEIBAJO_TO_NAR_BABA_CODE,
  NAR_BABA_CODE_TO_LOCAL_KEIBAJO,
  parseNarRaceKey,
} from "./nar";

describe("NAR realtime helpers", () => {
  it("maps local NAR racecourse codes to keiba.go.jp baba codes", () => {
    expect(LOCAL_KEIBAJO_TO_NAR_BABA_CODE["46"]).toBe("22");
    expect(LOCAL_KEIBAJO_TO_NAR_BABA_CODE["55"]).toBe("32");
    expect(LOCAL_KEIBAJO_TO_NAR_BABA_CODE["83"]).toBe("03");
    expect(NAR_BABA_CODE_TO_LOCAL_KEIBAJO["22"]).toBe("46");
  });

  it("builds and parses NAR race keys", () => {
    const raceKey = buildNarRaceKey("2026", "0510", "46", "9");
    expect(raceKey).toBe("nar:2026:0510:46:09");
    expect(parseNarRaceKey(raceKey)).toEqual({
      keibajoCode: "46",
      monthDay: "0510",
      raceNumber: "09",
      year: "2026",
    });
    expect(parseNarRaceKey("jra:2026:0510:05:11")).toBeNull();
  });
});
