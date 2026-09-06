// Run with bun. Public host adapter for private JV file validation and decoding.

import { decodeJvFileInRust } from "./rust-core";

const ORACLE_FILENAME = "JGDW2026083020260829112816.jvd";

export const decodeJvFile = async (
  file: Uint8Array,
  filename: string = ORACLE_FILENAME,
): Promise<Uint8Array> => decodeJvFileInRust(file, filename);
