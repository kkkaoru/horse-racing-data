// Run with bun. Exact operational canary registration and legacy resource protection.

import { readFileSync } from "node:fs";
import { URL } from "node:url";
import { parseConfigFileTextToJson } from "typescript";
import { expect, expectTypeOf, test } from "vitest";
import type { FinishPositionRescoreContainer } from "./container-class";
import type { FinishPositionRescoreContainer as ExportedRescore } from "./index";

test("exports the rescore Durable Object from the deployment entrypoint", () => {
  expectTypeOf<ExportedRescore>().toEqualTypeOf<FinishPositionRescoreContainer>();
});

test("retains disabled rescore registration without downsizing legacy or race-chain", () => {
  const parsed = parseConfigFileTextToJson(
    "wrangler.jsonc",
    readFileSync(new URL("../wrangler.jsonc", import.meta.url), "utf8"),
  );
  expect(parsed.error).toBeUndefined();
  const config: unknown = parsed.config;
  expect(config).toHaveProperty("containers.2", {
    class_name: "FinishPositionRescoreContainer",
    image: "../finish-position-predict-container/Dockerfile",
    image_build_context: "../..",
    image_vars: {
      ARTIFACT_ROLLOUT_REV: "20260912-jra-confirmed-entry-filter-v2",
      SOURCE_CACHE_BUSTER: "20260912-jra-confirmed-entry-filter-v2",
    },
    instance_type: { vcpu: 1, memory_mib: 3072, disk_mb: 6000 },
    max_instances: 1,
    rollout_step_percentage: 100,
  });
  expect(config).toHaveProperty("containers.0.instance_type", "standard-4");
  expect(config).toHaveProperty("containers.1.instance_type", {
    vcpu: 2,
    memory_mib: 6144,
    disk_mb: 12000,
  });
  expect(config).toHaveProperty("durable_objects.bindings.2", {
    class_name: "FinishPositionRescoreContainer",
    name: "FINISH_POSITION_RESCORE_CONTAINER",
  });
  expect(config).toHaveProperty("migrations.3", {
    tag: "v4",
    new_sqlite_classes: ["FinishPositionRescoreContainer"],
  });
  expect(config).toHaveProperty("vars.RESCORE_CONTAINER_ENABLED", "0");
  expect(config).toHaveProperty("vars.RESCORE_CONTAINER_RACES", "");
});
