// Run with: bun run --filter pc-keiba-viewer test
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, test } from "vitest";

import {
  RUNNING_STYLE_FEATURE_VERSION,
  RUNNING_STYLE_FEATURE_VERSION_DESC,
} from "./running-style-feature-version";

const JSON_PATH = resolve(
  process.cwd(),
  "src/scripts/finish-position-features/running-style-feature-version.json",
);
const PY_PATH = resolve(process.cwd(), "src/scripts/running_style_feature_version.py");

describe("running-style-feature-version", () => {
  test("TS constant version equals literal v1", () => {
    expect(RUNNING_STYLE_FEATURE_VERSION).toBe("v1");
  });

  test("TS constant description is non-empty", () => {
    expect(RUNNING_STYLE_FEATURE_VERSION_DESC.length > 0).toBe(true);
  });

  test("JSON SSoT file version equals literal v1", () => {
    const raw = readFileSync(JSON_PATH, "utf8");
    const parsed: { version: string; description: string } = JSON.parse(raw);
    expect(parsed.version).toBe("v1");
  });

  test("Python version constant equals literal v1", () => {
    const pySrc = readFileSync(PY_PATH, "utf8");
    const match = pySrc.match(/RUNNING_STYLE_FEATURE_VERSION:\s*str\s*=\s*_DATA\["version"\]/);
    expect(match !== null).toBe(true);
  });
});
