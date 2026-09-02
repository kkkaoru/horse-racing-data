// Run with bun.
import { expect, it, vi } from "vitest";
import { probeJvLinkNetwork } from "./network";

it("reports HTTP responses as reachable without sending credentials", async () => {
  const fetcher = vi.fn(async () => new Response(null, { status: 403 }));

  await expect(probeJvLinkNetwork(fetcher)).resolves.toStrictEqual({
    authenticationContractImplemented: true,
    dataContractImplemented: true,
    note: "The stored-data authentication, file-list, CDN-path, and payload contracts are implemented from successful official-SDK oracle captures; initial terminal registration remains an official-UI operation.",
    targets: [
      { host: "authlab.jra-van.ne.jp", reachable: true, status: 403 },
      { host: "datalab.cdn.jra-van.ne.jp", reachable: true, status: 403 },
      { host: "reallab.jra-van.ne.jp", reachable: true, status: 403 },
    ],
    transport: "http/80",
  });
  expect(fetcher).toHaveBeenCalledTimes(3);
});

it("reports transport errors without exposing their details", async () => {
  const fetcher = vi.fn(async () => {
    throw new Error("private network detail");
  });

  await expect(probeJvLinkNetwork(fetcher)).resolves.toStrictEqual({
    authenticationContractImplemented: true,
    dataContractImplemented: true,
    note: "The stored-data authentication, file-list, CDN-path, and payload contracts are implemented from successful official-SDK oracle captures; initial terminal registration remains an official-UI operation.",
    targets: [
      { host: "authlab.jra-van.ne.jp", reachable: false, status: null },
      { host: "datalab.cdn.jra-van.ne.jp", reachable: false, status: null },
      { host: "reallab.jra-van.ne.jp", reachable: false, status: null },
    ],
    transport: "http/80",
  });
});
