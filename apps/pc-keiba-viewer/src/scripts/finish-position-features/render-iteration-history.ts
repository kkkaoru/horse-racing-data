// Run with: bun run src/scripts/finish-position-features/render-iteration-history.ts \
//   --input-json tmp/v8/iter-data.json \
//   --template docs/finish-position-accuracy/_templates/iteration.md.tmpl \
//   --output docs/finish-position-accuracy/history/iter{N}.md
//
// Stage 0B helper for v8 iterative loop plan. Pure substitution over an
// iteration markdown template: walk every `{KEY}` placeholder in the template
// and replace it with the corresponding scalar / numeric / nested value from a
// JSON input. Numbers are rendered with 4-decimal precision so accuracy values
// stay readable in the rendered history file. Unknown keys raise a descriptive
// error so that drift between the template and the input is caught at render
// time rather than producing a half-substituted markdown file.

import { readFile, writeFile } from "node:fs/promises";

export interface IterationCategoryMetrics {
  top1: number;
  place2: number;
  place3: number;
  top3_box: number;
}

export interface IterationCategoryDeltas {
  top1: number;
  place2: number;
  place3: number;
  top3_box: number;
}

export interface IterationCategoryTrainingTime {
  jra: string;
  nar: string;
}

export interface IterationArtifacts {
  model_dir_jra: string;
  model_dir_nar: string;
  predictions_parquet: string;
  bucket_eval_csv: string;
}

export interface IterationQualityGate {
  tsc: string;
  lint: string;
  format: string;
  branches: number;
  functions: number;
  lines: number;
  statements: number;
  python_coverage: number;
}

export interface IterationPerBucketWorst {
  jra: string;
  nar: string;
}

export interface IterationInput {
  iteration_number: number;
  iso_timestamp: string;
  based_on_iteration: number;
  lever_id: string;
  status: string;
  quality_gate: string;
  model_version_jra: string;
  model_version_nar: string;
  metrics_jra: IterationCategoryMetrics;
  metrics_nar: IterationCategoryMetrics;
  deltas_jra: IterationCategoryDeltas;
  deltas_nar: IterationCategoryDeltas;
  composite_gain_normalized: number;
  per_bucket_worst: IterationPerBucketWorst;
  training_time: IterationCategoryTrainingTime;
  artifacts: IterationArtifacts;
  quality_gate_results: IterationQualityGate;
  body_what_tried: string;
  body_implementation_summary: string;
  body_results: string;
  body_per_bucket_findings: string;
  body_decision: string;
  body_next_iteration: string;
}

export interface RenderCliOptions {
  inputJson: string;
  template: string;
  output: string;
}

interface ApplyArgResult {
  advanceBy: number;
}

const NUMBER_DIGITS = 4;
const PLACEHOLDER_RE = /\{([^}\n]+)\}/g;

export const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/finish-position-features/render-iteration-history.ts \\",
    "    --input-json <input-json-path> \\",
    "    --template <template-path> \\",
    "    --output <output-md-path>",
  ].join("\n");

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

export const initialOptions = (): RenderCliOptions => ({
  inputJson: "",
  template: "",
  output: "",
});

const applyArg = (
  options: RenderCliOptions,
  name: string,
  value: string | undefined,
): ApplyArgResult => {
  if (name === "--input-json") {
    options.inputJson = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--template") {
    options.template = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--output") {
    options.output = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--help" || name === "-h") {
    console.log(buildUsageText());
    process.exit(0);
  }
  throw new Error(`Unknown argument: ${name}`);
};

const consumeArgs = (
  options: RenderCliOptions,
  argv: readonly string[],
  cursor: number,
): RenderCliOptions => {
  const name = argv[cursor];
  if (name === undefined) return options;
  const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
  return consumeArgs(options, argv, cursor + advanceBy);
};

export const parseArgs = (argv: readonly string[]): RenderCliOptions => {
  const options = consumeArgs(initialOptions(), argv, 0);
  if (options.inputJson === "") throw new Error("--input-json is required.");
  if (options.template === "") throw new Error("--template is required.");
  if (options.output === "") throw new Error("--output is required.");
  return options;
};

const formatNumber = (value: number): string => Number(value.toFixed(NUMBER_DIGITS)).toString();

const buildPlaceholderMap = (input: IterationInput): Map<string, string> => {
  const entries: ReadonlyArray<readonly [string, string]> = [
    ["N", String(input.iteration_number)],
    ["ISO_TIMESTAMP", input.iso_timestamp],
    ["M", String(input.based_on_iteration)],
    ["LEVER_ID", input.lever_id],
    ["accept_or_reject", input.status],
    ["passed_or_failed", input.quality_gate],
    ["MODEL_VERSION_JRA", input.model_version_jra],
    ["MODEL_VERSION_NAR", input.model_version_nar],
    ["TOP1_JRA", formatNumber(input.metrics_jra.top1)],
    ["PLACE2_JRA", formatNumber(input.metrics_jra.place2)],
    ["PLACE3_JRA", formatNumber(input.metrics_jra.place3)],
    ["TOP3_BOX_JRA", formatNumber(input.metrics_jra.top3_box)],
    ["TOP1_NAR", formatNumber(input.metrics_nar.top1)],
    ["PLACE2_NAR", formatNumber(input.metrics_nar.place2)],
    ["PLACE3_NAR", formatNumber(input.metrics_nar.place3)],
    ["TOP3_BOX_NAR", formatNumber(input.metrics_nar.top3_box)],
    ["DELTA_TOP1_JRA", formatNumber(input.deltas_jra.top1)],
    ["DELTA_PLACE2_JRA", formatNumber(input.deltas_jra.place2)],
    ["DELTA_PLACE3_JRA", formatNumber(input.deltas_jra.place3)],
    ["DELTA_TOP3_BOX_JRA", formatNumber(input.deltas_jra.top3_box)],
    ["DELTA_TOP1_NAR", formatNumber(input.deltas_nar.top1)],
    ["DELTA_PLACE2_NAR", formatNumber(input.deltas_nar.place2)],
    ["DELTA_PLACE3_NAR", formatNumber(input.deltas_nar.place3)],
    ["DELTA_TOP3_BOX_NAR", formatNumber(input.deltas_nar.top3_box)],
    ["COMPOSITE_GAIN", formatNumber(input.composite_gain_normalized)],
    ["PER_BUCKET_WORST_JRA", input.per_bucket_worst.jra],
    ["PER_BUCKET_WORST_NAR", input.per_bucket_worst.nar],
    ["DURATION_JRA", input.training_time.jra],
    ["DURATION_NAR", input.training_time.nar],
    ["MODEL_DIR_JRA", input.artifacts.model_dir_jra],
    ["MODEL_DIR_NAR", input.artifacts.model_dir_nar],
    ["PREDICTIONS_PARQUET", input.artifacts.predictions_parquet],
    ["BUCKET_EVAL_CSV", input.artifacts.bucket_eval_csv],
    ["TSC_RESULT", input.quality_gate_results.tsc],
    ["LINT_RESULT", input.quality_gate_results.lint],
    ["FORMAT_RESULT", input.quality_gate_results.format],
    ["BR", formatNumber(input.quality_gate_results.branches)],
    ["FN", formatNumber(input.quality_gate_results.functions)],
    ["LN", formatNumber(input.quality_gate_results.lines)],
    ["ST", formatNumber(input.quality_gate_results.statements)],
    ["PY_COV", formatNumber(input.quality_gate_results.python_coverage)],
    ["LEVER 説明", input.body_what_tried],
    ["SubAgent commit hashes、 touched files", input.body_implementation_summary],
    ["per-cat top1/place2/place3/top3_box 比較表", input.body_results],
    ["weak bucket 改善 / 悪化リスト", input.body_per_bucket_findings],
    ["accept / reject + 理由", input.body_decision],
    ["Step A の次選定 hint、 lever bank の優先順位調整", input.body_next_iteration],
  ];
  return new Map<string, string>(entries.map(([k, v]) => [k, v]));
};

export const renderIterationHistory = (input: IterationInput, template: string): string => {
  const placeholderMap = buildPlaceholderMap(input);
  return template.replaceAll(PLACEHOLDER_RE, (_match, key: string) => {
    const replacement = placeholderMap.get(key);
    if (replacement === undefined) {
      throw new Error(`Unknown placeholder: {${key}}`);
    }
    return replacement;
  });
};

export interface RenderRunDeps {
  readFile: (path: string) => Promise<string>;
  writeFile: (path: string, contents: string) => Promise<void>;
  log: (message: string) => void;
}

export const parseIterationInput = (raw: string): IterationInput => {
  const parsed: IterationInput = JSON.parse(raw);
  return parsed;
};

export const runRenderIterationHistory = async (
  deps: RenderRunDeps,
  options: RenderCliOptions,
): Promise<string> => {
  const [inputText, templateText] = await Promise.all([
    deps.readFile(options.inputJson),
    deps.readFile(options.template),
  ]);
  const input = parseIterationInput(inputText);
  const rendered = renderIterationHistory(input, templateText);
  await deps.writeFile(options.output, rendered);
  deps.log(`Wrote rendered iteration markdown to ${options.output}`);
  return rendered;
};

const readFileText = (path: string): Promise<string> => readFile(path, "utf-8");

const defaultLog = (message: string): void => {
  console.log(`[render-iteration-history] ${message}`);
};

const main = async (): Promise<void> => {
  const options = parseArgs(process.argv.slice(2));
  await runRenderIterationHistory({ readFile: readFileText, writeFile, log: defaultLog }, options);
};

if (import.meta.main) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}

export { applyArg, buildPlaceholderMap, formatNumber };
