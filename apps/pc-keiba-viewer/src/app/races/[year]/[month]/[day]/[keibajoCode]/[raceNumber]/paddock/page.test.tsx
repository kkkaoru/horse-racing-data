// Run with: bun run --filter pc-keiba-viewer test
// Source-level tests for the paddock-edit page running-style timeout budget.
// The page itself is a Next.js Server Component pulling in `server-only`
// modules, so we assert the timeout constants and the empty-array fallback
// path by reading the source file as text rather than importing it.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { expect, test } from "vitest";

const PAGE_PATH = resolve(
  process.cwd(),
  "src/app/races/[year]/[month]/[day]/[keibajoCode]/[raceNumber]/paddock/page.tsx",
);

test("running-style-timeout-uses-4-second-budget", () => {
  const source = readFileSync(PAGE_PATH, "utf8");
  const match = source.match(/const RUNNING_STYLE_TOTAL_TIMEOUT_MS = (\d+);/);
  const value = match === null ? null : Number(match[1]);
  expect(value).toBe(4000);
});

test("running-style-attempt-timeout-uses-1800ms-budget", () => {
  const source = readFileSync(PAGE_PATH, "utf8");
  const match = source.match(/const RUNNING_STYLE_ATTEMPT_TIMEOUT_MS = (\d+);/);
  const value = match === null ? null : Number(match[1]);
  expect(value).toBe(1800);
});

test("paddock-page-renders-when-running-style-times-out", () => {
  const source = readFileSync(PAGE_PATH, "utf8");
  const fallbackMatch = source.match(
    /setTimeout\(\(\) => resolve\(\[\]\), RUNNING_STYLE_TOTAL_TIMEOUT_MS\)/,
  );
  expect(fallbackMatch === null).toBe(false);
});

test("running-style-total-timeout-is-not-the-old-9-second-value", () => {
  const source = readFileSync(PAGE_PATH, "utf8");
  const match = source.match(/const RUNNING_STYLE_TOTAL_TIMEOUT_MS = 9000;/);
  expect(match).toBe(null);
});

test("running-style-attempt-timeout-is-not-the-old-3500ms-value", () => {
  const source = readFileSync(PAGE_PATH, "utf8");
  const match = source.match(/const RUNNING_STYLE_ATTEMPT_TIMEOUT_MS = 3500;/);
  expect(match).toBe(null);
});

test("running-style-max-attempts-retry-count-is-preserved", () => {
  const source = readFileSync(PAGE_PATH, "utf8");
  const match = source.match(/const RUNNING_STYLE_MAX_ATTEMPTS = (\d+);/);
  const value = match === null ? null : Number(match[1]);
  expect(value).toBe(2);
});

test("running-style-retry-backoff-is-preserved", () => {
  const source = readFileSync(PAGE_PATH, "utf8");
  const match = source.match(/const RUNNING_STYLE_RETRY_BACKOFF_MS = (\d+);/);
  const value = match === null ? null : Number(match[1]);
  expect(value).toBe(200);
});
