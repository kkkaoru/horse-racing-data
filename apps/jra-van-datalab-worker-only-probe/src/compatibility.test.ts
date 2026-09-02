// Run with bun.
import { describe, expect, it } from "vitest";
import {
  JVLINK_DEPLOYED_METHODS,
  JVLINK_DEPLOYMENT_COMPATIBILITY,
  JVLINK_EVENTS,
  JVLINK_FULL_COMPATIBILITY,
  JVLINK_METHODS,
  JVLINK_PROPERTIES,
} from "./compatibility";

const OFFICIAL_METHODS_5_0_0 = [
  "JVInit",
  "JVSetUIProperties",
  "JVSetServiceKey",
  "JVSetSaveFlag",
  "JVSetSavePath",
  "JVSetPayFlag",
  "JVOpen",
  "JVRTOpen",
  "JVStatus",
  "JVRead",
  "JVGets",
  "JVSkip",
  "JVCancel",
  "JVClose",
  "JVFiledelete",
  "JVFukuFile",
  "JVFuku",
  "JVMVCheck",
  "JVMVCheckWithType",
  "JVMVPlay",
  "JVMVPlayWithType",
  "JVMVOpen",
  "JVMVRead",
  "JVCourseFile",
  "JVCourseFile2",
  "JVWatchEvent",
  "JVWatchEventClose",
] as const;

const TYPELIB_DISPIDS_5_0_0 = [
  4, 6, 13, 15, 1, 34, 7, 10, 8, 9, 22, 19, 11, 5, 12, 25, 26, 24, 36, 23, 29, 27, 28, 30, 31, 33,
  35,
] as const;

const OFFICIAL_PROPERTIES_4_9_0_1 = [
  "m_saveflag",
  "m_savepath",
  "m_servicekey",
  "m_JVLinkVersion",
  "m_TotalReadFilesize",
  "m_CurrentReadFilesize",
  "m_CurrentFileTimeStamp",
  "ParentHWnd",
  "m_payflag",
] as const;

const OFFICIAL_EVENTS_4_9_0_1 = [
  "JVEvtPay",
  "JVEvtJockeyChange",
  "JVEvtWeather",
  "JVEvtCourseChange",
  "JVEvtAvoid",
  "JVEvtTimeChange",
  "JVEvtWeight",
] as const;

describe("official JV-Link API compatibility ledger", () => {
  it("classifies every official method without omissions or additions", () => {
    expect(JVLINK_METHODS.map(({ name }) => name)).toEqual(OFFICIAL_METHODS_5_0_0);
    expect(JVLINK_METHODS.map(({ dispid }) => dispid)).toEqual(TYPELIB_DISPIDS_5_0_0);
    expect(new Set(JVLINK_METHODS.map(({ name }) => name)).size).toBe(JVLINK_METHODS.length);
  });

  it("classifies every official property and event", () => {
    expect(JVLINK_PROPERTIES.map(({ name }) => name)).toEqual(OFFICIAL_PROPERTIES_4_9_0_1);
    expect(JVLINK_EVENTS).toEqual(OFFICIAL_EVENTS_4_9_0_1);
  });

  it("guarantees every method exposed by the Worker deployment", () => {
    expect(JVLINK_DEPLOYMENT_COMPATIBILITY).toBe(true);
    expect(JVLINK_DEPLOYED_METHODS).toHaveLength(18);
    expect(
      JVLINK_DEPLOYED_METHODS.every((name) => JVLINK_METHODS.some((entry) => entry.name === name)),
    ).toBe(true);
  });

  it("cannot claim full compatibility while an oracle-backed protocol is missing", () => {
    expect(JVLINK_FULL_COMPATIBILITY).toBe(false);
    expect(
      JVLINK_METHODS.filter(({ level }) => level === "protocol-research-required").map(
        ({ name }) => name,
      ),
    ).toEqual([
      "JVFukuFile",
      "JVFuku",
      "JVMVCheck",
      "JVMVCheckWithType",
      "JVWatchEvent",
      "JVWatchEventClose",
    ]);
  });
});
