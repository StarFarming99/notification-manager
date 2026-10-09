# Original sender and independent executor: implementation reference

The current rollout and acceptance contract is
[Scoped parallel NM delivery and future stable-Service promotion](full-region-promotion.md).
That document supersedes the earlier full-region/1,000-send/30-minute/72-hour
rollout checklist. Phase one copies only the designated production critical AM
Receiver to the designated test chat; live deployment requires the current
fault matrix and separate user approval. Full formal promotion remains gated.

NM's long-term repository is StarFarming99/notification-manager. Linkwei source
`a9a0bfdd77b1f26a1fef5e8afe6ec424c22dfa3d` is the verified rc7 baseline; production
binary SHA256 is `ac28d0c192d2b1ba043b57e0876b3fbc558538bd1c908b2899f23bc7db0b6b62`.
Historical image hashes are previous build evidence, never acceptance of the
current profile/scope implementation.

One reproducible Go 1.23.12 linux/amd64 image bundles `/notification-manager`
(default), `/jev-annotation-executor` (:19095), `/notification-spool-recovery`
(offline exclusive recovery), and `/notification-manager-operator` (future
managed stable-Service handoff). Initial test rollout leaves the existing
Operator workload unchanged. Future promotion needs its reviewed command/image
upgrade; adding a CR annotation without the upgraded consumer is insufficient.

Use `--store.type=notification_durable` and private single-writer
`NM_NOTIFICATION_SPOOL_PATH`, Recreate strategy, and controlled shutdown
(`NM_SHUTDOWN_MODE=drain|handoff`). The frozen plan is committed before HTTP ACK;
sending is persisted before notification I/O. Known pre-send errors and explicit
platform refusal retry the same target. Ambiguous transport/send interruption
becomes unknown and requires receipt-based reconciliation. The caller's original
deadline is preserved; no global five-second HTTP cutoff is introduced.

The independent relay remains bounded at 128 queued success offers. Overflow
uses a separate 1,024-slot gap queue and private `JEV_RELAY_GAP_PATH` JSONL
(default beside the durable spool), fsynced/rotated at 32MiB with one previous
file. Scrape `/metrics` and `/status`; dropped/failed/gap_overflow/journal_errors
and missing journal durability are reconciliation gaps. Jev/executor/model
failure never requests re-sending an already-successful original card.

Executor stores are process-fenced and separate from the original sender. Each
profile/version scope retains its card binding/outbox and immutable public
owner/domain/app/chat contract. Receipt completion tombstones retain exact
replay protection; its current 50,000-record storage budget remains explicit
and requires extension maintenance/reconciliation when exhausted. New NM spool
active-capacity, 48-hour payload cleanup and repeat-aware canonical dedupe are
described in the current contract above; legacy non-profile behavior is retained.

Recovery check/export/resolve/exact-replay uses
`/notification-spool-recovery --help`. Export creates a private new file;
resolve/replay requires actor/evidence and replay requires
`--permit-platform-send`. Unknown is a separate receipt-based decision. No whole
spool replay, delivered replay, dual writer, implicit historical test-to-formal
rewrite or destructive schema downgrade is allowed. Preserve PVC/audit across
rollback. The static-isolated UAT controller remains a test facility and is not
the production named-CR runtime.
