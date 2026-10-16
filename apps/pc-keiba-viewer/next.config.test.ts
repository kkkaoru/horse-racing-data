// Run with bun via vitest.
import { afterEach, expect, it, vi } from "vitest";

import configureNext from "./next.config";

const { initialize } = vi.hoisted(() => ({ initialize: vi.fn<() => Promise<void>>() }));
vi.mock("@opennextjs/cloudflare", () => ({ initOpenNextCloudflareForDev: initialize }));

afterEach(() => {
  vi.unstubAllEnvs();
  initialize.mockReset();
});

it("waits for development bindings before returning Next configuration", async () => {
  vi.stubEnv("NODE_ENV", "development");
  vi.stubEnv("PC_KEIBA_CLOUDFLARE_REMOTE_BINDINGS", "1");
  const ready = Promise.withResolvers<void>();
  initialize.mockReturnValue(ready.promise);
  const settled = vi.fn<() => void>();
  const pending = configureNext().then(settled);
  await vi.waitFor(() => expect(initialize).toHaveBeenCalledOnce());
  expect(initialize).toHaveBeenCalledWith({
    configPath: expect.stringMatching(/\/wrangler\.dev\.jsonc$/),
    remoteBindings: true,
  });
  expect(settled).not.toHaveBeenCalled();
  ready.resolve();
  await pending;
  expect(settled).toHaveBeenCalledOnce();
});

it("fails startup when development bindings cannot initialize", async () => {
  vi.stubEnv("NODE_ENV", "development");
  initialize.mockRejectedValue(new Error("binding initialization failed"));
  await expect(configureNext()).rejects.toThrow("binding initialization failed");
});

it("does not initialize development bindings during production builds", async () => {
  vi.stubEnv("NODE_ENV", "production");
  expect((await configureNext()).serverExternalPackages).toStrictEqual(["pg", "pg-cloudflare"]);
  expect(initialize).not.toHaveBeenCalled();
});
