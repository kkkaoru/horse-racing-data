// Run with bun. Verify the fixed-width JV-Data character encoding in a Workers isolate.

interface EncodingProbeResult {
  decoded: string | null;
  encoding: "shift_jis";
  supported: boolean;
}

export type DecodeSample = () => string;

const SAMPLE_JV_DATA_BYTES: Uint8Array = new Uint8Array([0x8b, 0xa3, 0x94, 0x6e]);

const decodeSample = (): string =>
  new TextDecoder("shift_jis", { fatal: true, ignoreBOM: false }).decode(SAMPLE_JV_DATA_BYTES);

export const probeShiftJisWith = (decode: DecodeSample): EncodingProbeResult => {
  try {
    return { decoded: decode(), encoding: "shift_jis", supported: true };
  } catch {
    return { decoded: null, encoding: "shift_jis", supported: false };
  }
};

export const probeShiftJis = (): EncodingProbeResult => probeShiftJisWith(decodeSample);
