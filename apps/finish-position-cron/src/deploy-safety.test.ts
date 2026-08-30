// Run with bun test.

import { describe, expect, it } from "vitest";

import {
  buildDeploymentPredictionRequest,
  DEPLOYMENT_DRAIN_QUEUES,
  findLiveContainerInstances,
  findUnsafeContainerInstances,
  finishPositionContainerApplications,
  isLiveContainerState,
  parseContainerApplications,
  parseContainerInstances,
  parseDeploymentRaces,
  shouldRequeueDeploymentPredictions,
} from "./deploy-safety";

describe("deployment safety", () => {
  it("pauses only queues that can start prediction Containers", () => {
    expect(DEPLOYMENT_DRAIN_QUEUES).toEqual([
      "finish-position-predict-queue",
      "finish-position-weight-rescore-queue",
    ]);
    expect(DEPLOYMENT_DRAIN_QUEUES).not.toContain("finish-position-focused-full-completion-queue");
    expect(DEPLOYMENT_DRAIN_QUEUES).not.toContain("finish-position-container-control-queue");
  });

  it("builds the admin request with the canonical runYmd field", () => {
    expect(
      buildDeploymentPredictionRequest(
        { category: "nar", keibajoCode: "50", raceBango: "01" },
        "20260827",
      ),
    ).toStrictEqual({
      category: "nar",
      force: true,
      keibajoCode: "50",
      raceBango: "01",
      runYmd: "20260827",
    });
  });

  it("requeues current races only for an explicit model deployment", () => {
    expect(shouldRequeueDeploymentPredictions("1")).toBe(true);
    expect(shouldRequeueDeploymentPredictions(undefined)).toBe(false);
    expect(shouldRequeueDeploymentPredictions("0")).toBe(false);
    expect(shouldRequeueDeploymentPredictions("true")).toBe(false);
  });

  it("selects only finish-position Container applications", () => {
    const applications = parseContainerApplications([
      { id: "predict", name: "finish-position-cron-finishpositionpredictcontainer" },
      { id: "chain", name: "finish-position-cron-finishpositionracechaincontainer" },
      { id: "other", name: "zenz-standard-small-n5-off" },
    ]);

    expect(finishPositionContainerApplications(applications).map(({ id }) => id)).toEqual([
      "predict",
      "chain",
    ]);
  });

  it("blocks every state except inactive", () => {
    const application = {
      id: "chain",
      name: "finish-position-cron-finishpositionracechaincontainer",
    };
    const instances = parseContainerInstances([
      { name: "race-chain-predict-nar-0", state: "running" },
      { name: "race-chain-predict-nar-1", state: "starting" },
      { name: "race-chain-predict-jra-0", state: "inactive" },
      { name: "predict-jra-2", state: "stopped" },
    ]);

    expect(findUnsafeContainerInstances(application, instances)).toEqual([
      {
        applicationName: application.name,
        name: "race-chain-predict-nar-0",
        state: "running",
      },
      {
        applicationName: application.name,
        name: "race-chain-predict-nar-1",
        state: "starting",
      },
      {
        applicationName: application.name,
        name: "predict-jra-2",
        state: "stopped",
      },
    ]);
  });

  it("treats only running and starting instances as live drain blockers", () => {
    const application = {
      id: "predict",
      name: "finish-position-cron-finishpositionpredictcontainer",
    };
    const instances = parseContainerInstances([
      { name: "predict-jra-0", state: "running" },
      { name: "predict-jra-1", state: "starting" },
      { name: "predict-jra-2", state: "stopped" },
      { name: "predict-jra-3", state: "inactive" },
    ]);

    expect(findLiveContainerInstances(application, instances)).toStrictEqual([
      {
        applicationName: "finish-position-cron-finishpositionpredictcontainer",
        name: "predict-jra-0",
        state: "running",
      },
      {
        applicationName: "finish-position-cron-finishpositionpredictcontainer",
        name: "predict-jra-1",
        state: "starting",
      },
    ]);
    expect(isLiveContainerState("running")).toBe(true);
    expect(isLiveContainerState("starting")).toBe(true);
    expect(isLiveContainerState("stopped")).toBe(false);
    expect(isLiveContainerState("inactive")).toBe(false);
  });

  it("fails closed for malformed Wrangler JSON", () => {
    expect(() => parseContainerApplications({})).toThrow("invalid JSON");
    expect(() => parseContainerApplications([{ id: 1, name: "bad" }])).toThrow(
      "invalid application",
    );
    expect(() => parseContainerInstances({})).toThrow("invalid JSON");
    expect(() => parseContainerInstances([{ state: 1 }])).toThrow("invalid instance");
    expect(() => parseDeploymentRaces({})).toThrow("invalid JSON");
    expect(() => parseDeploymentRaces([{ results: {} }])).toThrow("invalid results");
    expect(() => parseDeploymentRaces([{ results: [{ source: "other" }] }])).toThrow(
      "invalid race",
    );
  });

  it("parses post-deployment races from Wrangler D1 output", () => {
    expect(
      parseDeploymentRaces([
        {
          results: [
            { keibajo_code: "05", race_bango: "01", source: "jra" },
            { keibajo_code: "43", race_bango: "02", source: "nar" },
          ],
        },
      ]),
    ).toEqual([
      { category: "jra", keibajoCode: "05", raceBango: "01" },
      { category: "nar", keibajoCode: "43", raceBango: "02" },
    ]);
  });
});
