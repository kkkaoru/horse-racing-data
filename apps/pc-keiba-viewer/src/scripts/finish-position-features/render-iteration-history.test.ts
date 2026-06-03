// Run with: bunx vitest run src/scripts/finish-position-features/render-iteration-history.test.ts
import { expect, test, vi } from "vitest";

import type { IterationInput, RenderCliOptions, RenderRunDeps } from "./render-iteration-history";
import {
  buildPlaceholderMap,
  buildUsageText,
  formatNumber,
  initialOptions,
  parseArgs,
  renderIterationHistory,
  runRenderIterationHistory,
} from "./render-iteration-history";

const baseInput = (overrides: Partial<IterationInput>): IterationInput => ({
  iteration_number: 1,
  iso_timestamp: "2026-06-04T12:00:00Z",
  based_on_iteration: 0,
  lever_id: "Lever-A1",
  status: "accept",
  quality_gate: "passed",
  model_version_jra: "jra-v8-iter1",
  model_version_nar: "nar-v8-iter1",
  metrics_jra: { top1: 0.31, place2: 0.18, place3: 0.14, top3_box: 0.42 },
  metrics_nar: { top1: 0.32, place2: 0.19, place3: 0.15, top3_box: 0.43 },
  deltas_jra: { top1: 0.01, place2: 0.005, place3: 0.003, top3_box: 0.004 },
  deltas_nar: { top1: 0.012, place2: 0.006, place3: 0.004, top3_box: 0.005 },
  composite_gain_normalized: 0.0058,
  per_bucket_worst: { jra: "keibajo=06 -0.05", nar: "kyori=2400 -0.03" },
  training_time: { jra: "PT8H", nar: "PT6H" },
  artifacts: {
    model_dir_jra: "models/jra-v8-iter1",
    model_dir_nar: "models/nar-v8-iter1",
    predictions_parquet: "tmp/v8/preds",
    bucket_eval_csv: "tmp/v8/delta.csv",
  },
  quality_gate_results: {
    tsc: "ok",
    lint: "ok",
    format: "ok",
    branches: 95.5,
    functions: 96.1,
    lines: 95.7,
    statements: 95.9,
    python_coverage: 95.4,
  },
  body_what_tried: "Applied Lever-A1 race-internal pace polish.",
  body_implementation_summary: "abcd123 viewer + ef45678 features",
  body_results: "JRA +0.01 top1 across the board.",
  body_per_bucket_findings: "keibajo=06 improved 0.04.",
  body_decision: "accept: composite gain crossed threshold.",
  body_next_iteration: "Try Lever-B2 race-pace next.",
  ...overrides,
});

test("buildUsageText renders the render-iteration-history flag list", () => {
  expect(buildUsageText()).toBe(
    "Usage:\n  bun run src/scripts/finish-position-features/render-iteration-history.ts \\\n    --input-json <input-json-path> \\\n    --template <template-path> \\\n    --output <output-md-path>",
  );
});

test("initialOptions seeds all three required paths as empty", () => {
  expect(initialOptions()).toStrictEqual({
    inputJson: "",
    template: "",
    output: "",
  } satisfies RenderCliOptions);
});

test("parseArgs reads all three flags", () => {
  expect(
    parseArgs([
      "--input-json",
      "tmp/v8/iter-data.json",
      "--template",
      "docs/x.tmpl",
      "--output",
      "docs/iter1.md",
    ]),
  ).toStrictEqual({
    inputJson: "tmp/v8/iter-data.json",
    template: "docs/x.tmpl",
    output: "docs/iter1.md",
  } satisfies RenderCliOptions);
});

test("parseArgs throws when --input-json is missing", () => {
  expect(() => parseArgs(["--template", "t", "--output", "o"])).toThrowError(
    "--input-json is required.",
  );
});

test("parseArgs throws when --template is missing", () => {
  expect(() => parseArgs(["--input-json", "i", "--output", "o"])).toThrowError(
    "--template is required.",
  );
});

test("parseArgs throws when --output is missing", () => {
  expect(() => parseArgs(["--input-json", "i", "--template", "t"])).toThrowError(
    "--output is required.",
  );
});

test("parseArgs throws on unknown argument", () => {
  expect(() =>
    parseArgs(["--input-json", "i", "--template", "t", "--output", "o", "--bogus"]),
  ).toThrowError("Unknown argument: --bogus");
});

test("parseArgs throws when a flag value is missing", () => {
  expect(() => parseArgs(["--input-json", "i", "--template", "t", "--output"])).toThrowError(
    "--output requires a value.",
  );
});

test("formatNumber returns 4-decimal precision string", () => {
  expect(formatNumber(0.123456)).toBe("0.1235");
});

test("formatNumber preserves integer-ish values without trailing zeros", () => {
  expect(formatNumber(95)).toBe("95");
});

test("formatNumber preserves negative deltas with sign", () => {
  expect(formatNumber(-0.005)).toBe("-0.005");
});

test("buildPlaceholderMap exposes both metric and body placeholders", () => {
  const map = buildPlaceholderMap(baseInput({}));
  expect(map.get("MODEL_VERSION_JRA")).toBe("jra-v8-iter1");
  expect(map.get("TOP1_JRA")).toBe("0.31");
  expect(map.get("DELTA_TOP1_JRA")).toBe("0.01");
  expect(map.get("LEVER 説明")).toBe("Applied Lever-A1 race-internal pace polish.");
});

test("renderIterationHistory substitutes every standard placeholder", () => {
  const template =
    "iteration: {N}\nlever: {LEVER_ID}\nstatus: {accept_or_reject}\ntop1 jra: {TOP1_JRA}\n";
  expect(renderIterationHistory(baseInput({}), template)).toBe(
    "iteration: 1\nlever: Lever-A1\nstatus: accept\ntop1 jra: 0.31\n",
  );
});

test("renderIterationHistory substitutes japanese body placeholder", () => {
  const template = "## What was tried\n\n{LEVER 説明}\n";
  expect(renderIterationHistory(baseInput({}), template)).toBe(
    "## What was tried\n\nApplied Lever-A1 race-internal pace polish.\n",
  );
});

test("renderIterationHistory throws on unknown placeholder", () => {
  expect(() => renderIterationHistory(baseInput({}), "{UNKNOWN_KEY}")).toThrowError(
    "Unknown placeholder: {UNKNOWN_KEY}",
  );
});

test("renderIterationHistory leaves text without placeholders untouched", () => {
  expect(renderIterationHistory(baseInput({}), "no placeholders here\n")).toBe(
    "no placeholders here\n",
  );
});

test("runRenderIterationHistory reads input and template and writes output", async () => {
  const readMock = vi
    .fn<RenderRunDeps["readFile"]>()
    .mockResolvedValueOnce(JSON.stringify(baseInput({})))
    .mockResolvedValueOnce("iteration: {N}\nlever: {LEVER_ID}\n");
  const writeMock = vi.fn<RenderRunDeps["writeFile"]>().mockResolvedValue(undefined);
  const logMock = vi.fn<(message: string) => void>();
  const rendered = await runRenderIterationHistory(
    { readFile: readMock, writeFile: writeMock, log: logMock },
    {
      inputJson: "tmp/v8/iter-data.json",
      template: "tmp/v8/template.md.tmpl",
      output: "tmp/v8/iter1.md",
    },
  );
  expect(rendered).toBe("iteration: 1\nlever: Lever-A1\n");
  expect(readMock).toHaveBeenCalledTimes(2);
  expect(writeMock).toHaveBeenCalledWith("tmp/v8/iter1.md", "iteration: 1\nlever: Lever-A1\n");
});
