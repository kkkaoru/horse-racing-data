// Run with bun. Credential-free fixtures for Worker tests.

export const LICENSE_SUCCESS_BASE64: string = "UkVUVVJOPTQ=";

export const ZIP_BASE64: string =
  "UEsDBBQAAAAIAFBsI103szkPCQAAAAcAAAAOAAAAUkEyMDI2MDkwMi50eHTzMExMSublAgBQSwECFAMUAAAACABQbCNdN7M5DwkAAAAHAAAADgAAAAAAAAAAAAAAgAEAAAAAUkEyMDI2MDkwMi50eHRQSwUGAAAAAAEAAQA8AAAANQAAAAAA";

export const bytes = (text: string): Uint8Array<ArrayBuffer> => new TextEncoder().encode(text);

export const decodeBase64 = (value: string): Uint8Array<ArrayBuffer> => {
  const decoded: string = atob(value);
  const output: Uint8Array<ArrayBuffer> = new Uint8Array(decoded.length);
  output.set(Array.from(decoded, (character: string): number => character.charCodeAt(0)));
  return output;
};

export const opaqueConfig = (): string =>
  "AQARQUJDREVGR0hJSktMTU5PUFEABTEyMzQ1AAVTQTEyMwAFU0Q0NTYACFNPRlRXQVJF";
