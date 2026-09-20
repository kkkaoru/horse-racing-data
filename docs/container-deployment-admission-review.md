# Prediction deployment admission review

## Observed gap (2026-09-13)

`scripts/deploy-with-queue-drain.ts` under `apps/finish-position-cron/`
pauses delivery queues, stops superseded Containers, then returns from
`waitForContainerDrain()` on the first empty live-Container inventory.
Pausing deliveries does not cancel or await already delivered Worker requests.
A request doing native scoring, cache/weight checks or other preflight may
start a Container after this observation. The Nakayama 5R reset during the
11:38–11:39 JST activation demonstrates this gap; normal retry recovered its
publication, independently verified. See `container-rescore-only-canary-plan.md`.

A second empty inventory, a fixed sleep, or sampled tail silence can reduce
exposure but cannot prove that every admitted Worker request has drained.
The 03:20 UTC deactivation succeeded with no additional reset observed; this
is not a general synchronization guarantee.

## Proposed protocol — design only, not implemented

1. Inventory every side-effecting prediction entry path: both delivery queues,
   direct/admin requests, scheduled work, native scoring, full builds, rescores,
   reconnects and publication/cleanup handoffs. Container inventory alone and
   claims acquired only after preflight are insufficient.
2. Introduce a durable admission coordinator with a linearizable open/closed
   epoch and execution identities. Acquire admission before any prediction
   work, including native/preflight work. Closing admission must serialize
   against acquisitions: already admitted work remains tracked; later requests
   cannot start prediction work.
3. Pause the existing delivery queues, close admission, and wait for both
   tracked executions and live Containers to drain. Preserve execution
   ownership through publication and required cleanup handoff. Deployment
   proceeds only with a closed epoch and a proven empty set.
4. Treat unknown executions, storage/API errors and expired owner heartbeats
   as fail-closed deployment blockers, not evidence that execution ended.
   A lease timeout alone must never authorize an update that could reset a
   still-running request. Bound the deployment wait and abort safely.
5. Reopen admission and resume queues in recovery/finally handling. Ensure
   failure in either recovery operation is explicit and recoverable. Rejected
   admission must use the existing retry/deadline semantics; never clone Queue
   messages, reset retry budgets, drop work, or bypass ownership checks.
6. Bootstrap in an independently quiet maintenance window: an older Worker
   does not yet register admission, so the first protocol deployment cannot
   retrospectively establish safety for its in-flight requests. No rollout of
   this proposal has been attempted during the observed racing workload.

## Required regression evidence before implementation rollout

- A request delivered before queue pause, stalled in preflight, then released
  after the first empty inventory must still block deployment.
- Acquisition racing with closure has exactly one outcome: tracked admission
  or rejection before work. Include native and direct invocation paths.
- Completion, reconnect, retry, control handoff and concurrent stop preserve
  the same ownership and terminal publication guarantees.
- Stale owners, missing state, API failure, deploy failure and signals cannot
  silently reopen early or leave delivery paused without an explicit error.
- Bootstrap and rollback preserve registrations and pending cleanup; no
  historical prediction rewriting or retry-budget resets are permitted.
- All existing coverage scopes and thresholds remain unchanged; add tests
  alongside implementation and run full type/lint/coverage gates.

## Additional platform-bound check (2026-09-16 JST)

The current [Queues limits](https://developers.cloudflare.com/queues/platform/limits/)
documentation explicitly lists a 15-minute wall-clock maximum for Queue consumers,
Cron invocations and Durable Object alarms. The same page says ordinary incoming
HTTP requests and Durable Object RPC/HTTP invocations have no hard wall-clock
limit while their caller remains connected. These are not interchangeable with
CPU limits.

A closer implementation audit found that the route named
`run-focused-full-race-direct` is now a compatibility alias for the coordinated
Queue path; its name must not be mistaken for direct execution. However,
category-specific day-base prewarm still calls `prewarmCategoryWithOutcome`
synchronously, and day-base pickup/materialization and completion callbacks are
also HTTP operations. Therefore, waiting 15 minutes after pausing delivery alone
does not establish all-path drain. That bound alone must not justify an image
rollout without independently quiescing/tracking those other entry paths.

The deployment CLI's previously forced superseded-container stop has now been
replaced with a tested `buildDeploymentStopRequest` using `overrideActive:false`.
This preserves existing leases and focused-full watches while draining. It is
not by itself the complete admission protocol above.

This design is not a cost saving by itself. Avoid further production rollouts
solely to test the race. Representative rescore sizing and normalized billing
remain separate investigations; one successful small canary does not justify
resizing day-base or claiming 50% total-cost reduction.

## September 16 authorized maintenance outcome

The user authorized the operational maintenance window. HTTP/preview ingress,
ten crons and the two start queues were closed, then independently verified
restored at September 15 18:48:13 UTC. This was not implementation or proof of the
all-path admission protocol above. The maintenance wrapper exited 1 because its
idle-only postcheck ran during progressive rollout; the exact transient health
response was not retained.

Cloudflare subsequently reported all rollouts completed at 100%: legacy v254,
chain v133 and disabled small v2, latest completion 18:51:42.699 UTC. Thus ingress
was restored before progressive rollout finished. Future deployment coordination
must distinguish CLI acceptance from completed rollout and keep maintenance gates
owned until rollout completion, rather than relying on an immediate idle check.
No repeated deployment or forced stop was used to resolve the observed transient.

At 07:04 JST, direct API checks showed all prediction instances inactive on the
new application versions, with no health errors; initial publication completeness
remained 48/48. This does not prove every potential HTTP execution drained or that
the new prediction image has yet completed a live SIGTERM lifecycle. Evidence and
remaining cost/runtime checks are in `container-cost-revalidation-20260915.md`.
