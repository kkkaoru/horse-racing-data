// Run with bun.
import { expect, it } from "vitest";
import { probeRuntime, probeRuntimeWith } from "./runtime";

it("inflates the built-in sample through Web Streams", async () => {
  await expect(probeRuntime()).resolves.toStrictEqual({
    deflateDecoded: "JV-DATA\n",
    deflateSupported: true,
    webStreamsSupported: true,
  });
});

it("reports Worker-native streaming deflate support", async () => {
  await expect(probeRuntimeWith(async () => "JV-DATA\n")).resolves.toStrictEqual({
    deflateDecoded: "JV-DATA\n",
    deflateSupported: true,
    webStreamsSupported: true,
  });
});

it("reports unavailable runtime primitives without throwing", async () => {
  await expect(
    probeRuntimeWith(async () => {
      throw new Error("unsupported");
    }),
  ).resolves.toStrictEqual({
    deflateDecoded: null,
    deflateSupported: false,
    webStreamsSupported: false,
  });
});
