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

The three values must differ and contain at least 32 non-whitespace bytes.
Credentials and transfer payloads must stay out of logs, Git and shell history.
The key is retained only in process memory; its fingerprint is bound to the PVC.
Changing startup mode or removing environment configuration cannot reset a
persisted retirement fence. No instance configuration preserves the legacy path
only for a ledger that has never opted into this protocol.

A standby can be receive-ready: Alertmanager requests are durably accepted while
claiming and sending remain disabled. Synchronous Notification / Verify requests
instead receive `503` with `Retry-After: 1` before any platform call. They are not
silently queued and are never reported delivered merely because buffering worked.
An active instance returns the original synchronous platform success/failure;
uncertain calls and process interruption are persisted as reconciliation fences.

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

The read token cannot export rows, import, control, reconcile, or access profile
controls; it is not accepted as a profiled intake credential. Existing legacy
API access still requires its established bounded source guard. The control token
is not the probe's read token.
Control errors contain fixed reason codes instead of caller data or payloads.

`ReleaseCommand` uses `operation_id`, `expected_revision`, `action`, `peer_id`,
`deadline` (RFC3339), `max_send_delay_millis`, `sync_remaining_millis`, `actor`,
`evidence`, and optional `retirement_proof`. The first command fixes the entire
operation's deadline and budgets. Subsequent commands cannot renew them. The
minimum remaining synchronous request budget is checked before sending; its
context is also bounded by the frozen release deadline.

Export body: `{ "operation_id": "…", "expected_revision": 1, "final": true }`.
Import body: `{ "command": { …, "action": "import" }, "manifest": { … } }`.
Controls are limited to 32 KiB; import is limited to 64 MiB. Preflight checks the
read-only transfer size bound before starting an operation or changing a selector.

## Transition and recovery rules

The release coordinator uses `begin-active` on A and `begin-standby` on B with the
same operation and frozen deadline, preseeds B, changes only the intended Service
selector, then closes A's admission. A continues its already accepted sends until
pending, retrying, sending, unknown and unreconciled failures are resolved. A final
export is imported by B. A's `retire` durably records a signed fence; B's `activate`
requires the matching authenticated final checksum and retirement proof. An
arbitrary JSON claim of retirement cannot enable sending.

After a lost response or script disconnect, GET and compare the operation,
revision, phase and original deadline before continuing. Exact duplicate command
receipts are idempotent, but an old receipt is not a current-state proof. Recover
an export with its authenticated GET instead of creating a different snapshot.
Canceled HTTP controls recheck the request context after waiting for an in-flight
send permit; they cannot later acquire the lock and apply an abandoned mutation.

Rollback first puts the retired A into `rollback-standby` (`returning`) so it can
durably receive without sending, then preseeds B's state into A. The coordinator
changes the intended Service back to A and verifies its exclusive Ready
endpoints before closing B's admission. B drains; its final state is imported by
A, B is durably retired, and only then may A activate with B's matching retirement
proof. Both ledger generations retain their immutable target/profile provenance;
the current underlying delivery profile state is handed over consistently, rather
than pausing or redirecting existing test/formal tasks to simulate an instance
transition.
Keep both PVCs, original operation and audit evidence on any conflict or unknown.
An expired operation never just resets its deadline or turns a buffered instance
into a sender. The single total deadline reserves a recovery interval in advance;
rollback must activate inside that original deadline. After expiry the coordinator
reports blocked recovery and preserves both ledgers; it cannot enable sending by
claiming a new budget.

## Acceptance boundaries

The deployed c62 binary has no online instance handoff protocol. This implementation
does not make its first migration seamless: capability checks must reject that
boundary and leave the existing test sender running. Native Alertmanager messages
also cannot always distinguish a delayed old generation from a legitimate return
to identical content across a handoff. Ambiguous admission fails with an explicit
retryable response and fixed diagnostic code; it is not silently ACKed or claimed
as zero-loss acceptance.

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
