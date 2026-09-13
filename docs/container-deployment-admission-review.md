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

This design is not a cost saving by itself. Avoid further production rollouts
solely to test the race. Representative rescore sizing and normalized billing
remain separate investigations; one successful small canary does not justify
resizing day-base or claiming 50% total-cost reduction.
