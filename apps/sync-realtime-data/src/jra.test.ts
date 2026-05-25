// run with: bun run test
import { expect, it } from "vitest";
import {
  fetchJraOddsWithPlaywright,
  fetchJraResultHtmlWithPlaywright,
  isJraScratchStatus,
} from "./jra";

it("isJraScratchStatus returns true for known scratch statuses", () => {
  expect(isJraScratchStatus("除外")).toBe(true);
  expect(isJraScratchStatus("取消")).toBe(true);
});

it("isJraScratchStatus returns false for normal status text", () => {
  expect(isJraScratchStatus("出走")).toBe(false);
  expect(isJraScratchStatus("")).toBe(false);
});

it("fetchJraOddsWithPlaywright throws when browserBinding is undefined", async () => {
  await expect(fetchJraOddsWithPlaywright(undefined, "https://x.test/race")).rejects.toThrow(
    "JRA_BROWSER binding is required",
  );
});

it("fetchJraResultHtmlWithPlaywright throws when browserBinding is undefined", async () => {
  await expect(fetchJraResultHtmlWithPlaywright(undefined, "https://x.test/result")).rejects.toThrow(
    "JRA_BROWSER binding is required",
  );
});
