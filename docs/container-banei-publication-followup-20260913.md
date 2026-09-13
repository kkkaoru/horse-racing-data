# Ban-ei publication follow-up — 2026-09-13

## Observed, not yet repaired

While selecting an offline resource-comparison fixture, readiness at
04:28:21 UTC reported Ban-ei 1R (raw source `nar`, venue `83`, post 14:40 JST):

- Expected runners: 9. All 9 predictions existed in Neon, generation
  `2026-09-12 00:21:06.113472+00`.
- Prediction KV count: 0; pre-weight completeness false,
  `kv-generation-mismatch`. Weights had not yet been delivered.
- A separate authenticated Cloudflare KV GET returned error 10009, key not
  found, for `pred:fp:v1:20260913:83:01` in namespace
  `a013fdf6e7224c3ab0c227be3c899073`.

The readiness API uses raw `source=nar` for Ban-ei and distinguishes it by
venue 83. The initial `source=ban-ei` filter was incorrect; its empty result
was not evidence of no meeting. The subsequent completed-candidate check
stopped before any R2 download because publication completeness was false.

## Recovery attempt and authentication blocker

The existing Viewer section refresh endpoint reads published predictions
from Neon and can refill the current-day KV cache. It does not run scoring
or rewrite the prediction table. A generation-fenced request was attempted
for this one race only, using locally configured credentials without logging
their values.

- Job 124, using the Worker-facing local token name: HTTP 401.
- Source inspection found that the Viewer route expects
  `PC_KEIBA_INTERNAL_TOKEN`. This local value exists and differs from the
  Worker-facing value; job 125 with that configured Viewer value also
  returned HTTP 401.
- Neither request establishes a successful cache repair. Local token-name
  mismatch alone therefore does not explain the production authentication
  failure. Production secret values were not read, changed or rotated.

The user was informed that valid production internal authentication must be
confirmed through the normal configuration channel, not by pasting a token
into chat. No repeated credential guessing or authentication bypass is allowed.
No new prediction request, retry-budget reset, direct KV overwrite or blanket
multi-race refresh was submitted. Offline Ban-ei sizing is secondary to resolving
this publication discrepancy.

## Remaining verification

- Job 126 at **04:42:20 UTC / 13:42 JST** still reported 1R with 9 Neon rows,
  0 KV rows and no delivered weights. Its initial T-60 deadline of 13:40 JST
  had passed without readiness confirming complete publication. The user was
  informed; the gap is not repaired. Job 127 at 04:58:15 UTC / 13:58 JST
  still showed the same 9-row Neon generation, zero KV rows and no delivered
  weights for 1R, with 42 minutes remaining to post.
- Job 129 at **05:15:23 UTC / 14:15 JST** still showed 1R with 9 Neon rows,
  zero KV rows and no weight snapshot, 25 minutes before post. 2R likewise
  had 10 Neon rows, zero KV rows and no weight snapshot, 55 minutes before
  post. Both were incomplete after their respective initial T-60 deadlines
  (13:40 and 14:10 JST). The user was alerted that legitimate internal-auth
  configuration and the weight-delivery path need operator investigation.
  No recovery or successful repair has been verified.
- Job 130 at **05:28:14 UTC / 14:28 JST** again showed all 12 races with
  zero KV rows and no ready weight snapshot. 1R was 12 minutes before post;
  2R was 42 minutes before post. Races 3 and 12 still had no Neon predictions;
  the other ten had the expected Neon counts. This is the latest observation,
  not a claim about subsequent publication or actual race-start outcomes.
- All 12 Ban-ei races had zero KV rows in this check. Races 3 and 12 also
  lacked Neon predictions (their later deadlines had not passed); the other
  ten races had the expected Neon counts. This broader observation is not a
  justification for blind blanket cache writes or new prediction submissions.
- Job 128 independently confirmed both prediction and weight-rescore queues
  had `delivery_paused=false` (successful API responses, no service errors).
  The sampled resource events since 04:42 UTC showed successful JRA 10R and
  NAR 7R rescores, but no Ban-ei event. Sampling and the bounded query do not
  establish that no Ban-ei execution occurred, nor prove end-to-end queue health.
- Observe natural weight delivery and normal processing at a bounded later
  check. Independently verify any recovery and the newer generation before
  declaring success; do not assume an estimated arrival time is guaranteed.
- Cache expiration is a hypothesis, not an established cause. The old
  generation and tomorrow-cache TTL are insufficient without write/expiry
  evidence. No causal link to the Container optimization has been established.
- Existing natural weight rescores may publish a newer generation. Never
  overwrite one with an older snapshot to make a benchmark eligible.
- Keep the small Container route disabled. Any later Ban-ei benchmark must
  be offline and must not trigger historical production replay.

## Operator handoff / blocked continuation

The automated loop is paused pending operator input, not marked as successful
optimization or incident resolution. Repeated read-only checks have not found
recovery; the cache-only repair path remains authentication-blocked. No jobs,
watchers or deployments from this investigation remain active.

Required operator actions:

1. Confirm the approved production internal-auth configuration for the Viewer
   through the normal secret-management channel; do not paste credentials into
   chat. Confirm when legitimate access is available before another refresh.
2. Investigate the Ban-ei weight-delivery path separately. Unpaused queues do
   not establish that upstream data was delivered or admitted successfully.
3. After access or normal processing recovers, re-read current generations and
   independently verify Neon/KV counts and weight freshness. Do not replay an
   old generation over a newer publication or rewrite past-race predictions.

Cost optimization remains unfinished: 50% savings is a target, native parity and
admission-protocol implementation remain open, and complete-day billed savings
and authenticated MLflow UI verification are still pending. These have not been
superseded by this incident investigation.
