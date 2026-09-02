// Run with bun. Public host adapter for private JV file validation and decoding.

import { decodeJvFileInRust } from "./rust-core";

export const decodeJvFile = async (file: Uint8Array): Promise<Uint8Array> =>
  decodeJvFileInRust(file);
