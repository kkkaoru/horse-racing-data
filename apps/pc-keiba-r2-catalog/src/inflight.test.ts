// Run with bunx vitest.
import { expect, it } from "vitest";

import { coalesce, inflightSize } from "./inflight";

it("runs the builder once for concurrent callers sharing a key", async () => {
  const calls: string[] = [];
  const build = async (): Promise<string> => {
    calls.push("build");
    return "payload";
  };
  const [first, second, third] = await Promise.all([
    coalesce("shared-key-a", build),
    coalesce("shared-key-a", build),
    coalesce("shared-key-a", build),
  ]);
  expect(calls).toStrictEqual(["build"]);
  expect(first).toBe("payload");
  expect(second).toBe("payload");
  expect(third).toBe("payload");
});

it("runs the builder separately for different keys", async () => {
  const calls: string[] = [];
  const buildOne = async (): Promise<string> => {
    calls.push("one");
    return "one";
  };
  const buildTwo = async (): Promise<string> => {
    calls.push("two");
    return "two";
  };
  const [first, second] = await Promise.all([
    coalesce("distinct-key-1", buildOne),
    coalesce("distinct-key-2", buildTwo),
  ]);
  expect(calls).toStrictEqual(["one", "two"]);
  expect(first).toBe("one");
  expect(second).toBe("two");
});

it("re-executes after the previous call settles instead of replaying the result", async () => {
  const calls: string[] = [];
  const build = async (): Promise<string> => {
    calls.push("build");
    return "fresh";
  };
  await coalesce("sequential-key", build);
  await coalesce("sequential-key", build);
  expect(calls).toStrictEqual(["build", "build"]);
});

it("shares a rejection with every joined caller and clears the entry", async () => {
  const calls: string[] = [];
  const build = (): Promise<string> => {
    calls.push("build");
    return Promise.reject(new Error("upstream exploded"));
  };
  const first = coalesce("failing-key", build);
  const second = coalesce("failing-key", build);
  await expect(first).rejects.toThrow("upstream exploded");
  await expect(second).rejects.toThrow("upstream exploded");
  expect(calls).toStrictEqual(["build"]);
  expect(inflightSize()).toBe(0);
});

it("releases the key once a successful call settles", async () => {
  const build = async (): Promise<string> => "done";
  await coalesce("released-key", build);
  expect(inflightSize()).toBe(0);
});
