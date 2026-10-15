# Local-ledger single-active releases

This is an opt-in, instance-level release protocol. It uses each generation's
own Bolt PVC. It introduces no MySQL, Redis, MQ, RWX mount, or per-alert Kubernetes
record. Delivery `test` / `formal` profile configuration remains a separate gate.
Do not restart the annotation executor merely because an NM binary is released.

## Configuration

Run NM with `--store.type=notification_durable`, an absolute
`NM_NOTIFICATION_SPOOL_PATH`, and the following independently mounted files:

| Variable | Meaning |
| --- | --- |
| `NM_INSTANCE_ID` | Stable, bounded identity of this generation's ledger |
| `NM_INSTANCE_INITIAL_MODE` | `active` for an approved initial sender; `standby` for a new generation |
| `NM_INSTANCE_READ_TOKEN_FILE` | Absolute file containing the independent read credential |
| `NM_INSTANCE_CONTROL_TOKEN_FILE` | Absolute file containing the independent write/export credential |
| `NM_INSTANCE_PROTOCOL_KEY_FILE` | Absolute file containing the shared cross-ledger HMAC key |
| `NM_INSTANCE_SYNC_PEER_ID` | Exact identity of the other generation in this release pair |
| `NM_INSTANCE_SYNC_PEER_URL` | Fixed peer generation HTTPS origin, never the stable notification Service |
| `NM_INSTANCE_SYNC_ALLOW_HTTP` | Explicit `true` only for an independently isolated HTTP network; HMAC does not encrypt payloads |

The three values must differ and contain at least 32 non-whitespace bytes.
Credentials and transfer payloads must stay out of logs, Git and shell history.
The key is retained only in process memory; its fingerprint is bound to the PVC.
Changing startup mode or removing environment configuration cannot reset a
persisted retirement fence. No instance configuration preserves the legacy path
only for a ledger that has never opted into this protocol.

A standby can be receive-ready: Alertmanager requests are durably accepted while
claiming and sending remain disabled. During a configured release, synchronous
Notification / Verify requests use the fixed other generation while it remains
the legitimate sender, or wait within the original request budget for the safe
handoff. The original token or compatibility source guard runs before forwarding.
Requests are not silently queued or reported successful before the actual platform
result. An unconfigured standby still returns an explicit `503`; this negative
case is not future blue-green API continuity acceptance. Preflight requires both
generations' `synchronous_forwarding.configured` and exact peer identity/origin.

## Authenticated HTTP resources

The deployment must put these resources behind its bounded source guard and TLS
origin. They must not be exposed by the unauthenticated legacy compatibility
listener. All responses use `Cache-Control: no-store`.

| Resource | Authority | Result |
| --- | --- | --- |
| `GET /internal/instance` | Read token only | Safe phase, revision, deadline, aggregate queue counts, unresolved synchronous count and transfer size bound |
| `POST /internal/instance/control` | Control token | CAS command and persisted operation receipt |
| `POST /internal/instance/export` | Control token | Signed snapshot; final snapshots are persisted for exact recovery |
| `GET /internal/instance/export?operation_id=…&checksum=…` | Control token | Exact previously persisted final snapshot |
| `POST /internal/instance/import` | Control token | Authenticated, idempotent merge into the successor's ledger |
| `GET /internal/instance/synchronous` | Control token | Synchronous attempt references and outcomes, without request contents |
| `POST /internal/instance/synchronous/resolve` | Control token | Explicit, audited external reconciliation; never an automatic retry |
| `POST /internal/instance/synchronous/forward` | Signed single-hop peer claims plus original profile authentication | Real Notification / Verify result under the current sender's in-flight permit |

The read token cannot export rows, import, control, reconcile, or access profile
controls; it is not accepted as a profiled intake credential. Existing legacy
API access still requires its established bounded source guard. The control token
is not the probe's read token.
Control errors contain fixed reason codes instead of caller data or payloads.

`ReleaseCommand` uses `operation_id`, `expected_revision`, `action`, `peer_id`,
`deadline` (RFC3339), `max_send_delay_millis`, `sync_remaining_millis`, `actor`,
`evidence`, and optional `retirement_proof`. The first command fixes the entire
operation's deadline and budgets. Subsequent commands cannot renew them. The
remaining synchronous budget is a release planning input. Publication expiry does
not truncate a legal sender's original worker/caller request deadline.

The synchronous peer route binds operation, source/target instance, path, original
source, authentication hash, body hash and absolute request deadline in a
purpose-separated HMAC using the mounted protocol key. It accepts only the two
original synchronous paths, rejects replay, never forwards again, and does not
follow redirects or ambient HTTP proxies. The protocol key does not grant general
read/control authority; it does grant this narrow peer capability when the local
operation, sender permit and profile gate all agree. Preserve these headers at the
independent generation guard: `X-NM-Release-Synchronous`,
`X-NM-Release-Signature`, and the original `Authorization`. Limit this route to
the explicit peer sources separately from operator control access.

A platform error, transport uncertainty or incomplete/oversized peer response is
returned once and never automatically resent. Only the peer's authenticated
never-entered response permits a bounded 100 ms routing recheck. A signed durable
retirement proof can complete activation only against the exact already-imported
final checksum, operation, peer and unused fence; it does not renew the release
deadline. This also covers an old keepalive connection after Service propagation.
Incoming/outgoing `release-forward:*` records contain reconciliation metadata, no
request body or credentials. Actual platform calls retain the synchronous attempt
fence under the release read permit. Restart marks unfinished forward references
unknown without replay. Completed references expire only after both retention and
the signed request deadline; unknown references stay available for reconciliation.

Export body: `{ "operation_id": "…", "expected_revision": 1, "final": true }`.
Import body: `{ "command": { …, "action": "import" }, "manifest": { … } }`.
Controls are limited to 32 KiB; import is limited to 64 MiB. Preflight checks the
read-only transfer size bound before starting an operation or changing a selector.

## Transition and recovery rules

The release coordinator uses `begin-active` on A and `begin-standby` on B with
one operation and frozen publication deadline, preseeds B, changes only the
sealed Service selector, then closes A's asynchronous admission. `freeze` waits
for held platform permits and disables new claims. Only actual `sending` targets
and synchronous platform calls delay the final snapshot. Pending/retry work,
unknown and dead-letter outcomes migrate unchanged; unknown attempts are never
automatically replayed. Ordering is per destination and observable lifecycle;
a completed uncertain attempt must not block unrelated future notifications.

An explicit CAS `freeze` commits the physical send fence before final export.
Final export rejects `draining`; it never silently changes phase without advancing
the control revision. Repeated final
exports return the same persisted bytes, not a new timestamp/checksum. Import is
idempotent and size/capacity bound. A's `retire` durably records a signed fence;
B's `activate` requires the exact final checksum, peer, operation, frozen deadline
and an unconsumed retirement proof. CAS, instance/PVC identity and proof replay
checks remain mandatory. A startup mode change cannot revive a retired ledger.

If B never received sending rights and A has not retired, close B's admission
and freeze its buffer export first. `cancel-import` merges that final delta and
puts A into `cancelling` in one transaction, keeping its unique sending right;
new A intake cannot overtake the known B buffer. The lower-level `cancel` remains
a same-operation recovery action, not the normal selector-first cancellation. It invalidates
an unused A export rather than retiring/reviving A or clearing historical unknown
records. The coordinator then returns the Service to A, retires B, and uses
`cancel-complete` with
that exact imported checksum and B's proof. Imported preseed/final predecessor
rows are excluded from the returned delta, so stale copies cannot overwrite A's
newer sends. A repeated cancel cannot erase an imported checksum; a completed
cancellation cannot reopen the same operation. If buffer return fails, preserve
B/PVC/tasks and report partial recovery instead of claiming selector rollback is
complete. Cancellation can also recover B beginning before A's begin succeeded.

After A has retired or B has sent, quick cancellation is forbidden. Rollback
puts retired A into `returning`, returns the Service, freezes B, imports its final
snapshot and retirement proof, then activates A. Both PVCs and original operation
remain. GET reconciles lost responses; cached command responses are not fresh
state proof. The same exact final export is recovered by checksum.

The original total deadline is never renewed. Expiry marks the publication late
or failed; it does not revoke the last legal sender's business permit or reject
buffer admission. Recovery may finish after expiry only with the same operation,
exact final snapshot and unconsumed durable retirement proof. Late recovery is
reported as late, not as an on-time release. If an unretired sender remains frozen
past the original deadline, business claiming or synchronous ingress atomically
cancels the unused snapshot and resumes that same sender. A synchronous request
can also cancel an uncommitted freeze when it reaches the operation's reserved
remaining budget. These paths serialize with retirement: a committed retirement
wins and can never be undone by expiry or a public request. The successor's
buffers still require same-operation return and reconciliation by the coordinator. A coordinator outage is not solved by
letting both instances send: unresolved sender identity remains a real failure
and requires the bounded same-operation recovery command. Retired peers keep
serving authenticated single-hop synchronous handoff while needed by existing
connections; final cleanup is a separately controlled step.

## Acceptance boundaries

The currently deployed c62 sidechannel may be replaced once by ordinary
stop-before-start using its existing local PVC. That first sidechannel replacement
may briefly return 503 or miss Jev annotations; it does not require online legacy
export/retirement or a permanent formal-only duplicate. This allowance is not a
future formal NM daily-release continuity contract. Historical missing generation
metadata is classified `legacy-unsequenced`, preserved without invented sequence
facts, and does not permanently block later releases between capable versions.

Native AM webhooks cannot always distinguish same-start content A -> B -> A from
a delayed peer. Persist each ambiguous candidate and its reception provenance,
possible duplicate references and reason. Prefer notifying over losing it;
confirmed current repeats remain deduplicated. Cross-peer scheduling order is
explicit metadata, not a fabricated observation time or AM generation. Ambiguity
counters are diagnostic and cannot block an entire operation or reject admission.
Boundary duplicates must be reconciled separately from omissions in tests.

First formal cutover uses the separate Operator exit-proof protocol. A new
profiled candidate requires `preserved_test` containing sealed independent test
Deployment / Service UIDs and selector, its own PVC and actual Ready endpoints.
Missing, shared or changed proof blocks before stopping the original sender.
Forward transitions recheck it; rollback preserves the original safe process
proof and does not edit the independent test workload. `test-restored` keeps the
retired formal candidate stopped when the separate test sender remains available.
Legacy shared-process recovery remains explicitly unverified for test continuity.

These tools are development capabilities, not production deployment, activation,
or full migration acceptance. Use the separate isolated test report for the exact
tested binaries, specifications, scenarios and remaining gates.

The inherited conservative-card test expected a fifth element left over from
removed card feedback. Its expectation now matches the existing four-element
render and verifies a frozen copy of all original confirmation/group/silence
callback values plus the detail link. This changes the test, not card rendering.
