# Natural rescore follow-up — 2026-09-13, 03:20–03:59 UTC

Read-only telemetry query returned five successful legacy-profile resource
samples and five accepted `exact-entry-and-identity-match` cache attestations.
Venue attribution below correlates category, race, colo and nearby preceding
attestation time; these events do not expose a shared execution ID, so this
is a correlation rather than an independently joined execution trace.

| Category | Correlated venue/race | Colo | Resource event timestamp (epoch ms) | Python seconds | Samples | Guest peak bytes |
| -------- | --------------------- | ---- | ----------------------------------: | -------------: | ------: | ---------------: |
| JRA      | 09/07                 | ATL  |                       1789269733682 |         56.082 |     221 |        578035712 |
| NAR      | 36/04                 | DEN  |                       1789269903104 |         30.166 |     117 |        503111680 |
| JRA      | 06/07                 | ATL  |                       1789270755589 |         42.322 |     164 |        569630720 |
| JRA      | 09/08                 | ATL  |                       1789271586441 |         48.241 |     186 |        556228608 |
| NAR      | 36/05                 | LAX  |                       1789271842064 |         34.196 |     136 |        506744832 |

All events report `mode=rescore`, `run_date=20260913`, successful sampling
startup, a 250 ms sample interval and guest total 12,814,610,432 bytes. Native
cgroup measurement remains unavailable. Observed guest peaks span about
480–551 MiB; they are neither exact per-job maxima nor guaranteed bounds for
an entire category. These are not additional executions on the small role.

The query is sampled telemetry, not a complete census of all requests or
failures. Scoring success and accepted attestation do not independently prove
publication/cleanup for every row in this table. No production requests were
submitted to obtain these measurements. The small route remains disabled.

## What this supports, and what it does not

- Adds naturally observed JRA and NAR rescore workloads to the resource record.
- Does not compare identical workloads at two production allocations. Do not
  infer causal CPU/memory savings by comparing these races with the small 6R.
- Does not cover Ban-ei, all routing/ensemble branches, maximum fields, error
  paths, or full/day-base builds. Legacy and race-chain resources stay unchanged.
- Does not establish allocated lifetime or priced billing. Normalized complete
  Billable Usage windows and actual lifecycle attribution are still required.
- A next size expansion needs immutable complete-input comparisons and a
  bounded future-race validation, not restoration of unverified native models.

Source: bounded Cloudflare observability query, job 115, needles
`container-resource-usage` and `rescore-cache-attestation`, limit 30 each,
from 03:20 UTC to query time near 03:59 UTC. Both API responses succeeded
with empty error arrays. See the canary and native-parity review documents
for the separate functional proof and remaining correctness requirements.
