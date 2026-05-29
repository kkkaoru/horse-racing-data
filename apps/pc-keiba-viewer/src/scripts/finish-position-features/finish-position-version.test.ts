// Run with: bun run --filter pc-keiba-viewer test
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, test } from "vitest";

import {
  FINISH_POSITION_VERSION,
  FINISH_POSITION_VERSION_DESCRIPTION,
} from "./finish-position-version";

const JSON_PATH = resolve(
  process.cwd(),
  "src/scripts/finish-position-features/finish-position-version.json",
);
const PY_PATH = resolve(process.cwd(), "src/scripts/finish_position_version.py");

describe("finish-position-version", () => {
  test("TS constant version equals literal v1", () => {
    expect(FINISH_POSITION_VERSION).toBe("v1");
  });

  test("TS constant description is non-empty", () => {
    expect(FINISH_POSITION_VERSION_DESCRIPTION.length > 0).toBe(true);
  });

  test("JSON SSoT file version equals literal v1", () => {
    const raw = readFileSync(JSON_PATH, "utf8");
    const parsed: { version: string; description: string } = JSON.parse(raw);
    expect(parsed.version).toBe("v1");
  });

  test("Python version constant equals literal v1", () => {
    const pySrc = readFileSync(PY_PATH, "utf8");
    const match = pySrc.match(/FINISH_POSITION_VERSION:\s*str\s*=\s*_FILE\["version"\]/);
    expect(match !== null).toBe(true);
  });
});
