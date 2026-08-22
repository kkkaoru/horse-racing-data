// Run with bun. Tests for authoritative Container role environment merging.

import { expect, test, vi } from "vitest";

vi.mock("@cloudflare/containers", () => ({
  Container: class {},
}));

import {
  buildLegacyPredictContainerEnvVars,
  buildRaceChainPredictContainerEnvVars,
} from "./container-class";

test("buildLegacyPredictContainerEnvVars fixes the legacy role after inherited variables", () => {
  const envVars = buildLegacyPredictContainerEnvVars({
    env: {
      NEON_DATABASE_URL: "postgres://legacy-output/db",
      PREDICT_DAYS_AHEAD: "1",
    },
    inheritedEnvVars: {
      CALLER_VALUE: "preserved",
      PREDICT_CONTAINER_ROLE: "race-chain",
    },
  });

  expect(envVars.PREDICT_CONTAINER_ROLE).toBe("legacy");
  expect(envVars.CALLER_VALUE).toBe("preserved");
  expect(envVars.NEON_DATABASE_URL).toBe("postgres://legacy-output/db");
});

test("buildRaceChainPredictContainerEnvVars fixes the role after inherited variables", () => {
  const envVars = buildRaceChainPredictContainerEnvVars({
    env: {
      DAY_BASE_SPLIT_ENABLED: "jra,nar,ban-ei",
      NEON_DATABASE_URL: "postgres://race-output/db",
      PREDICT_DAYS_AHEAD: "0",
      SOURCE_DATABASE_URL: "r2-catalog://pc-keiba",
    },
    inheritedEnvVars: {
      PREDICT_CONTAINER_ROLE: "legacy",
    },
  });

  expect(envVars.PREDICT_CONTAINER_ROLE).toBe("race-chain");
  expect(envVars.DAY_BASE_SPLIT_ENABLED).toBe("jra,nar,ban-ei");
  expect(envVars.SOURCE_DATABASE_URL).toBe("r2-catalog://pc-keiba");
});

test("buildRaceChainPredictContainerEnvVars keeps production defaults fail closed", () => {
  const envVars = buildRaceChainPredictContainerEnvVars({
    env: {
      NEON_DATABASE_URL: "postgres://output/db",
      PREDICT_DAYS_AHEAD: "0",
    },
    inheritedEnvVars: {
      MODELS_DIR: "/caller-models",
      PREDICT_SERVE_MODE: "cli",
    },
  });

  expect(envVars.MODELS_DIR).toBe("/models");
  expect(envVars.PREDICT_SERVE_MODE).toBe("http");
  expect(envVars.PYTHONUNBUFFERED).toBe("1");
  expect(envVars.SOURCE_DATABASE_URL).toBe("");
});
