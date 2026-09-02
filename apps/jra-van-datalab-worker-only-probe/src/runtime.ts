// Run with bun. Verify Worker-native primitives needed after an official wire contract exists.

interface RuntimeProbeResult {
  deflateDecoded: string | null;
  deflateSupported: boolean;
  webStreamsSupported: boolean;
}

export type InflateSample = () => Promise<string>;

const COMPRESSED_SAMPLE: Uint8Array = new Uint8Array([
  0x78, 0x9c, 0xf3, 0x0a, 0xd3, 0x75, 0x71, 0x0c, 0x71, 0xe4, 0x02, 0x00, 0x09, 0xa0, 0x01, 0xf2,
]);

const inflateSample = async (): Promise<string> => {
  const source = new ReadableStream<Uint8Array>({
    start(controller): void {
      controller.enqueue(COMPRESSED_SAMPLE);
      controller.close();
    },
  });
  return new Response(source.pipeThrough(new DecompressionStream("deflate"))).text();
};

export const probeRuntimeWith = async (inflate: InflateSample): Promise<RuntimeProbeResult> => {
  try {
    return {
      deflateDecoded: await inflate(),
      deflateSupported: true,
      webStreamsSupported: true,
    };
  } catch {
    return {
      deflateDecoded: null,
      deflateSupported: false,
      webStreamsSupported: false,
    };
  }
};

export const probeRuntime = (): Promise<RuntimeProbeResult> => probeRuntimeWith(inflateSample);
