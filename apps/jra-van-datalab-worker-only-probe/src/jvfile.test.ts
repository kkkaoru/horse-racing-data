// Run with bun.
import { expect, it } from "vitest";
import { decodeJvFile } from "./jvfile";

const VECTOR: Uint8Array = new Uint8Array([
  32, 32, 32, 32, 32, 32, 32, 32, 32, 53, 120, 156, 155, 228, 170, 250, 112, 26, 59, 0, 9, 53, 2,
  123,
]);

it("decodes a fixed credential-free JV file vector in the private core", async () => {
  await expect(decodeJvFile(VECTOR)).resolves.toStrictEqual(
    new Uint8Array([0x4a, 0x47, 0x31, 0x0d, 0x0a]),
  );
});

it("propagates private core validation failures", async () => {
  await expect(decodeJvFile(new Uint8Array())).rejects.toThrow("shorter");
});
