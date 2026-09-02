// Run with bun.
import { expect, it } from "vitest";
import { probeShiftJis, probeShiftJisWith } from "./encoding";

it("decodes the JV-Data CP932 sample", () => {
  expect(probeShiftJis()).toStrictEqual({
    decoded: "競馬",
    encoding: "shift_jis",
    supported: true,
  });
});

it("reports an unavailable decoder without leaking runtime errors", () => {
  const result = probeShiftJisWith(() => {
    throw new RangeError("unsupported encoding");
  });

  expect(result).toStrictEqual({
    decoded: null,
    encoding: "shift_jis",
    supported: false,
  });
});
