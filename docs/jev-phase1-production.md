# Phase-one original sender and independent executor

Long-term main repository: **StarFarming99/notification-manager**, explicitly
selected by the user on 2026-10-09. Current full-region validation and same-NM
promotion contract: [full-region-promotion.md](full-region-promotion.md).
The static-isolated test mode described below is not the production rollout path.

Source baseline: production rc7 is byte-identical to Linkwei master
`a9a0bfdd77b1f26a1fef5e8afe6ec424c22dfa3d`. The production binary SHA256 is
`ac28d0c192d2b1ba043b57e0876b3fbc558538bd1c908b2899f23bc7db0b6b62`.
This branch integrates that baseline with the existing UAT adaptation. It is a
candidate for controlled validation, not a production acceptance result.

One image contains `/notification-manager` (default entrypoint),
`/jev-annotation-executor` (independent process, HTTP :19095), and
`/notification-spool-recovery` (offline exclusive-owned recovery).
Build/push linux/amd64 and deploy only the verified immutable Harbor index digest.
The user separately builds the AIOps image. No deployment is authorized by this document.

Original store opt-in: `--store.type=notification_durable`,
`NM_NOTIFICATION_SPOOL_PATH=/var/lib/nm-original/notifications.db`, and
`NM_SHUTDOWN_MODE=drain` or `handoff`. Use a private single-writer PVC with a
verified Recreate strategy. The authoritative NotificationManager CR exposes
env/volumes/volumeMounts but not strategy; validate the actual Operator/GitOps
strategy before release. Never replace its generated Deployment as the source of truth.
Leave the existing provider/configuration in place until G0 full-channel parity
and first-rc7 drain/ownership checks pass.

The durable provider freezes the complete post-route/filter/silence/aggregation
plan before accepting HTTP, commits every target together, and persists sending
before platform I/O. Literal credential values are rejected; use registered
Secret/config references. Feishu chat/user/department/bot and webhook targets
have separate outcomes. Proven pre-send credential/token/render failures retry
the same frozen target. Token HTTP calls never count as notification sends.
HTTP 429/401/403 and parsed platform rejections are retryable; invalid-request
HTTP 400/404/405/410/413/422 are dead letters. Ambiguous transport/HTTP 5xx/restarts become
unknown. Delivered/unknown targets are never automatically replayed. Original
history depends on confirmed original success. Unsupported notifier types refuse
the whole intake with 503; implement and golden-test any production channel before
enabling the provider. Data retention and stopped-store compaction require an
audited export/restore procedure; do not delete records to bypass capacity.

Upstream Idempotency-Key binds the exact request and frozen target set. AM 0.23
does not generally provide this header. Without it, the sender retains baseline
at-least-once intake behavior and records a new attempt; identical-body retry and
legitimate repeat cannot be distinguished. Exactly-once delivery and an automatic
content-dedupe window are not claimed. This ambiguity must be reconciled against
AM/platform receipts during G0/G2 and the first old-version drain.

The original Feishu success hook only offers a pointer to a bounded 128-slot
channel. It performs no hash, clone, marshal, disk/network wait or goroutine spawn.
Overflow offers reconciliation keys to a separate bounded 1,024-slot gap channel.
`/metrics` exposes `nm_jev_relay_*` accepted/dropped/failed/queued/gap_overflow/
journal_errors counters and gauges; `/status` includes instance, sequence, the
last 256 gaps and journal health. Every recorded gap is structured-logged with
receiver, destination, app and message ID, without card content or credentials.
Set `JEV_RELAY_GAP_PATH` to an absolute private PVC JSONL path; by default the
durable provider uses `jev-relay-gaps.jsonl` next to its spool. The async writer
fsyncs each entry, rotates at 32 MiB and retains the current and one previous file
(64 MiB total). It refuses writes below 128 MiB free space, protecting the original
spool's 64 MiB reserve. Prefer a separate bounded volume. Archive journal files
through log collection before rotation. After restart, reconcile both files with
the original spool and platform message receipts; live recent gaps are per-process.
`gap_overflow` or `journal_errors` means reconciliation records are incomplete;
never interpret missing individual keys as zero loss. An unset journal path also
reports reconciliation incomplete. Poll status and scrape metrics independently.
The independent AM observer and original spool are the reconciliation inputs.
Executor/API/DB/model/feedback failure must
not withdraw original NM readiness. Relay failure is an observable capture gap,
never an original send failure.

The nil HTTP client retains the caller's deadline; no global five-second timeout
overrides original Feishu behavior. The durable worker retains its configured
budget (30 seconds by default). Dedicated relay HTTP calls remain bounded at 3 seconds.

The 2026-10-09 review rejected the first candidate. Replacement code and image
evidence live in `alert/artifacts/production-framework-20261008/acceptance/phase1/devfix-20261009`.
These regressions do not satisfy the real-alert, full-channel or deployment gates.

Executor owns separate fenced receipt and card-state directories. Receipt records
retain exact observations and successful completion tombstones; 50,000 records
or insufficient disk stop extension capture and require reconciliation. It first
posts a canonical observation and then its exact occurrence receipt. Production
service principals bind source/domain/owner/app/chat/execution domain. Receipt,
annotation, feedback and relay credentials are independent. Do not reuse bearer
credentials across hops. No secret values are stored in a frozen original payload.

Production PATCH and feedback stay off until original card status/ACK/callback
writers are identified and their coordination is verified. A flag is a gate,
not proof of coordination. The process must not become the competing consumer of
production button events during validation. Use static-isolated mode only with a
dedicated test app/chat and approved mock hosts; it rejects production mode and
does not create a Kubernetes controller/cache.

Recovery commands are described by `/notification-spool-recovery --help`:
check/export are offline; export creates a new private file. Resolve and exact
replay require actor/evidence; replay also requires `--permit-platform-send`.
Resolving unknown is a separate decision based on platform receipts. No whole
spool replay, delivered replay, live dual ownership or automatic destructive
schema downgrade is allowed. Keep all PVCs and audit across rollback.

Tests in this branch include race checks, bounded-hook P99, exact frozen replay,
atomic capacity rejection, partial success/restart/unknown fencing, target-only
recovery, history dependencies, canonical Python/Go fixtures and static isolation.
Real receiver/card parity, original callbacks, first rc7 drain, 1,000 platform
sends, 30-minute peak recovery and 72-hour independent coverage remain release
gates. See the alert workspace phase-one acceptance matrix for exact status.
