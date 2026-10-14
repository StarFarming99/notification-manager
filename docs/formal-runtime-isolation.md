# Formal sender runtime contract (2026-10-14)

This capability is installed in the candidate image before any handoff. Nothing
activates formal on startup: test admission continues with its existing
credential, immutable versions and destination; formal remains paused until an
explicit authenticated prepare/activate. These runtime controls do not change
AM, PagerDuty, the stable Service or the old sender.

## Original formal behavior

The separate source-fenced `NM_FORMAL_COMPAT_LISTEN_ADDRESS` retains original
uncredentialed APIs behind verified direct socket CIDRs. Forwarded headers do
not authorize callers. No test, annotation or profile mutation route is exposed
on this listener. A guard may expose port 19093 and proxy to a loopback-only
compatibility listener; the guard must validate actual Kubernetes caller
identity before proxying. `/-/formal-ready` is independent from ordinary test
readiness, and fails unless compatibility is installed, formal is prepared and
active, all original receiver/template/credential adapters validate, and formal
storage is healthy. Preparation validates without sending a platform request.

Formal Alertmanager requests persist frozen original destinations and ACK only
a committed original-channel plan. Notification and Verify API requests reuse
the original synchronous aggregation/notifier pipeline and return actual
platform success or error/timeout, with their original successful response
strings. They do not return pretend verification success after enqueue, do not
queue a hidden retry after returning an error and remain excluded from Jev.
This covers original inline receiver/config/literal credentials. The AM durable
plans still use the existing UID-bound or encrypted-inline frozen credential
mechanism and preserve old key references.

## Queue and pacing isolation

- Every original v1 queue row retains its identity, payload, state, destination
  and frozen version. Startup rebuilds only a secondary per-lane ready index;
  it never turns delivered/unknown rows into sends.
- Formal and test use independent indexes and dedicated worker pools. Defaults
  match the existing `--worker.queue` bounded to 1–32 for each pool. Explicit
  `NM_TEST_NOTIFICATION_WORKERS` and `NM_FORMAL_NOTIFICATION_WORKERS` can set
  each to 1–32. Test cannot occupy formal workers.
- `NM_NOTIFICATION_SEND_INTERVAL` retains the existing persisted test clock,
  including pre-upgrade reservations. `NM_FORMAL_NOTIFICATION_SEND_INTERVAL`
  defaults to `0s` (original scheduling); its optional 0–1 second clock uses a
  separate persisted key. Formal never inherits the test 12 second reservation.
- Test's active-record quota defaults to its original
  `NM_NOTIFICATION_SPOOL_MAX_TARGETS`. Optional
  `NM_TEST_MAX_PENDING_TARGETS` can lower it. Original/formal work has its own
  original-sized quota, excluding historical test pending/retry/unknown rows.
  Historical test over-quota remains stored and visible; it is not deleted.
- Byte/disk health is still shared and fails closed. This is exposed separately;
  record-quota isolation cannot make a full disk healthy. Shared VM, database
  and App resource pressure still requires rollout evidence.
- The regional executor's existing global PATCH gate continues to count every
  scope, rejection and retry. Test base sends and all additional PATCH calls
  retain their separately allocated, aggregate App quota slices. Formal base
  notifications are original traffic, rather than a new sidechannel allowance.
  Jev/central failure or PATCH deferral never blocks original base delivery.

## Observability and rollback restart

`GET /internal/cutover-status` on the primary listener accepts the optional independent
`NM_CUTOVER_READ_TOKEN` Bearer (at least 32 bytes, distinct from control/intake);
existing profile-control GET access remains compatible. The read credential
never authorizes intake, another control resource or any POST mutation. The resource returns `contract_version: 1`, `profiles`, `budgets`,
`spool` and `formal_compatibility`. It contains queue states/counts and runtime
limits, listener/CIDRs and original API semantics, never resolved credentials or
credential hashes. Read-only probes cannot prepare, activate or send a card.

After the candidate process has stopped, `/notification-spool-recovery` can
persist a formal pause under the exclusive original Bolt writer lock:

```text
notification-spool-recovery --spool /path/notifications.db --action pause-profile \
  --profile formal --expected-profile-revision N --actor OPERATOR --evidence REFERENCE
notification-spool-recovery --spool /path/notifications.db --action check
```

The first command returns `{action:"pause-profile",profile_id:"formal",
state:{paused:true,revision:N+1,...},writer_stopped:true}`; revision conflict or
an active writer fails. `check` uses a read-only lock and includes persisted
profile states/counts. No platform API or replay is invoked by these operations.

Starting the same PVC with `NM_FORMAL_DISABLED=true` requires the formal ledger
already be persistently paused. Otherwise startup refuses to run. With this
fence formal readiness/intake stays disabled and activate/resume is rejected;
test, historical receipts and original test destinations remain usable. Removing
the fence is a separately reviewed startup configuration change; no rollback
operation automatically re-enables formal.

The executor uses that optional independent read credential for GET
`/internal/jev/writer-control`; POST continues to require its separate writer
control credential. Status adds `formal_disabled`. The default-disabled startup
fence `JEV_FORMAL_WRITER_DISABLED=true` blocks every installed formal scope/version
annotation/PATCH before any external intent is saved. It retains formal and test
receipts, historical cards and test annotations. Resuming the global writer gate
cannot remove this startup fence. Managed rollback stops the executor before
installing the flag; later explicit test restart may resume the global writer
while the formal flag remains true. Neither the read credential nor a successful
pause response substitutes for process exit evidence.
