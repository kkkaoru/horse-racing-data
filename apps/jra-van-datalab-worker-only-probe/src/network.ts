// Run with bun. Credential-free reachability probe for JV-Link's documented HTTP hosts.

interface ProbeTarget {
  host: string;
  url: string;
}

interface TargetResult {
  host: string;
  reachable: boolean;
  status: number | null;
}

interface NetworkProbeResult {
  authenticationContractImplemented: true;
  dataContractImplemented: true;
  note: string;
  targets: TargetResult[];
  transport: "http/80";
}

export type ProbeFetch = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

const PROBE_TIMEOUT_MS: number = 5000;
const CONTRACT_NOTE: string =
  "The stored-data authentication, file-list, CDN-path, and payload contracts are implemented from successful official-SDK oracle captures; initial terminal registration remains an official-UI operation.";
const TARGETS: ProbeTarget[] = [
  { host: "authlab.jra-van.ne.jp", url: "http://authlab.jra-van.ne.jp/" },
  { host: "datalab.cdn.jra-van.ne.jp", url: "http://datalab.cdn.jra-van.ne.jp/" },
  { host: "reallab.jra-van.ne.jp", url: "http://reallab.jra-van.ne.jp/" },
];

const probeTarget = async (target: ProbeTarget, fetcher: ProbeFetch): Promise<TargetResult> => {
  try {
    const response = await fetcher(target.url, {
      method: "GET",
      redirect: "manual",
      signal: AbortSignal.timeout(PROBE_TIMEOUT_MS),
    });
    await response.body?.cancel();
    return { host: target.host, reachable: true, status: response.status };
  } catch {
    return { host: target.host, reachable: false, status: null };
  }
};

export const probeJvLinkNetwork = async (
  fetcher: ProbeFetch = fetch,
): Promise<NetworkProbeResult> => ({
  authenticationContractImplemented: true,
  dataContractImplemented: true,
  note: CONTRACT_NOTE,
  targets: await Promise.all(TARGETS.map((target) => probeTarget(target, fetcher))),
  transport: "http/80",
});
