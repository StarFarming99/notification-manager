# Full-region parallel validation and promotion

Updated 2026-10-09. The user selected **StarFarming99/notification-manager** as
NM's long-term main repository. Linkwei/notification-manager remains the verified
production rc7 baseline, not the destination for future NM changes.

Business code: [zilliz-cloud #10086](https://github.com/zilliztech/zilliz-cloud/pull/10086).
Deployment design: [vdc-deploy-prod #2360](https://github.com/zilliztech/vdc-deploy-prod/pull/2360),
`jev-alert-center-sidechannel/FULL_REGION_PLAN.md`.

## Required runtime

The latest user decision is to **reuse the existing AM, not deploy another AM**.
Keep source notifier targets and the existing route tree, receiver names, grouping,
repeat timing, muting, continue behavior, original integration indices and formal
outputs. Append Observer then test-NM webhooks with send_resolved=true to every
reachable receiver, including PD-only and root fallback. This requires a reviewed
AM config/auth-mount overlay, not an assertion that all AM configuration stays
unchanged. The same infra-alerts app keeps old NM production groups and sends new
NM cards only to test chat `oc_fbb2c50fed6ea3bd0f0777bea6f35358`.

AM v0.23 uses per-integration dedup/retry/success logs, but its concurrent Fanout
waits for branches and shares resources/group deadlines. Prove original NM/PD
behavior with local mock 503/timeout/outage/reload tests before rollout. Appending
integrations preserves original indices; do not insert or reorder original ones.
Cross-receiver duplicates, legitimate AM repeat, request retries and Observer vs
success-receipt intake need explicit reconciliation, not a permanent fingerprint
plus startsAt dedupe claim. AM does not generally supply an Idempotency-Key.

Notification webhooks do not guarantee all silenced/inhibited/short-lived raw
events. Full raw regional coverage requires a persistent event outlet before
muting/grouping, separately wired to Observer per source. This is missing runtime
implementation. Do not call sampled snapshots or old NM success receipts complete
raw event capture. Notification and raw-event coverage are separate gates.
References: [v0.23 notification pipeline](https://github.com/prometheus/alertmanager/blob/v0.23.0/notify/notify.go)
and [group dispatch](https://github.com/prometheus/alertmanager/blob/v0.23.0/dispatch/dispatch.go).

The new NM must begin with the production-compatible durable sender, independent
single-writer PVC, original template/channel/filter/silence/batching semantics,
reference-based credentials and retained card/receipt ledger. A test destination
profile is an instance-local overlay, not a write to shared production Receiver
CRs. Preserve original logical receiver identity and profile version. Freeze the
profile and destination in each queued plan; old test tasks never become formal
sends just because the active profile changes.

## Implementation boundaries of this PR

This PR contains the existing phase-one adapter, durable original sender,
independent executor, relay metrics/gaps, safe pre-send retry classification,
original deadline compatibility, receipt/card fencing and static test controller.
It does **not** implement or validate all of the following promotion requirements:

- Named NotificationManager configuration binding and isolated test/formal
  destination overlay. Current dynamic-cr observes all cluster-scoped NM CRs;
  creating a second CR can reconfigure the old controller. Namespace separation
  does not solve this. The test/uat-only static-isolated mode cannot be promoted.
- Complete equivalence for all actual production channels and receiver variants.
  The durable provider supports Feishu and webhook plans and refuses unsupported
  notifier kinds. Inventory and implement any other actual channel before rollout.
- A single infra-alerts callback consumer/router with old/new message ownership,
  original ACK/claim/recovery writers and coordinated suggestion PATCH. Do not
  start two competing WebSocket consumers. Original card state and old callbacks
  must remain supported after the old sender stops, requiring a verified owner
  handoff or legacy-handler mode rather than discarding their state.
- Actual all-receiver AM overlay and v0.23 failure/reload evidence, cross-receiver
  duplicate/retry/repeat reconciliation and pre-mute/pre-group raw event wiring.

The deployment PR has retired its memory/static snapshot runtime and rejects
all attempts to enable it. These requirements are development/deployment gates,
not a boolean approval switch or evidence from earlier static fixture tests.
Validation session instructions: deployment PR
`jev-alert-center-sidechannel/VERIFICATION_HANDOFF.md`. Report code regression,
design consistency and deployment readiness separately; A01-A17 remain pending.

## Promotion of the same NM

After full-region dual-group acceptance and separate user approval:

1. Remove test-NM webhooks while retaining Observer/original slots. Stop only test
   intake and drain/reconcile frozen test plans while old NM serves production.
2. Ready and verify the same new NM's formal endpoint/profile, credentials and
   callback ownership. Keep image/PVC/identities and all formal receivers.
3. Switch the original AM NM URL in its existing slot to the verified new formal
   endpoint. Verify new intake and that old NM receives no new work; do not stop
   the old endpoint or pause the original notification path before this switch.
4. Drain/reconcile old accepted/in-flight/unknown work, then stop old sender via
   authoritative CR/Operator/GitOps. Verify rc7 drain capability in advance. HA
   reload is not an atomic cluster-wide switch: target ownership and retry
   reconciliation across both ledgers are required before approval. During drain,
   old sender only finishes work accepted before intake cutover.
5. Verify receipts, original receiver/card parity and old-card callbacks. PD/Vector
   and Observer continue. Old AM logs never become fabricated new NM receipts;
   frozen test plans remain test sends and existing cards cannot cross chats.

Rollback follows the same safe order: ready/verify the previous formal endpoint,
switch the AM URL back, verify intake, then drain/reconcile/stop the outgoing
sender. Never point AM at a stopped endpoint or replay the entire spool. Each
notification target must retain one owner across the handoff.

Named configuration isolation is a hard precondition, not optional follow-up.
Do not create another cluster-scoped NM CR until implementation and evidence
prove the original controller cannot be reconfigured by it. That review blocker
remains open; the disabled chart does not count as implementing isolation.

## Provenance and release ownership

The PR is based on StarFarming99 master `5bb4646764670c0a2320804b5ff1be60fa07c81b`.
Before documentation changes, its complete reconstructed tree was
`61f0627fdd20031031981d1891b915f5b9f58dbf`, identical to tested candidate
`db9c7b89df393ba851bf33dccaa03cc52b9e53ef`. Documentation-only changes do not
change the candidate's executable inputs. Existing image:
`harbor.op.zillizcloud.com/devops/notification-manager@sha256:169d03edeb422241fc72608e78e737058430eb342ceca5f8df01baba57b7049b`.
It proves that candidate build/pull, not completion of the missing promotion gates.

Codex tests/builds/pushes NM/executor, Jev and Console as linux/amd64 immutable
index digests to harbor.op.zillizcloud.com. The user builds AIOps. Required runtime
changes need new tested images before their manifests can be enabled. No cluster,
production DB or real chat is modified by this PR. A01–A17 remain pending.


## 2026-10-09 acceptance repairs and AM research correction

The durable AM handler now passes the pipeline's unnamed alert slice. Frozen
History receives a stable internal ledger identity without changing legacy memory
History rendering; full HTTP/atomic-plan/dependency tests cover both paths.
Relay Close stops admission and joins cancelled deliveries and fsynced gap records
within a two-second budget. Timeout is explicitly incomplete, and reconciliation
scope is the current process's shutdown gaps. Immediate-exit reproduction journals
all 101 accepted receipts. These repairs do not close pending profile/config/card
ownership or duplicate/repeat gates, and nothing has been deployed.

Stock AM's short-interval fault fixture delayed the next resolved delivery because
Fanout waits for retries, but the single-instance 60s production-timer fixture did
not show extra latency. The 9.75s number is not measured production impact. The
user retained reuse-AM and asked to eliminate the effect before rollout. A local
optional AM binary study isolates side aggregation while preserving primary keys;
it is not yet a production choice. Three-replica HA, contents/counts and resource
cost remain unverified. See deployment REPAIR_HANDOFF.md and business
infra/jev-alert-shadow/deploy/production/alertmanager-study/README.md.

Fixed NM/executor/recovery image (linux/amd64):
harbor.op.zillizcloud.com/devops/notification-manager@sha256:8bdc7d032e49d8a2e8e237b6fba4f7179a3eb10279e6e60af61369d347ce2368
Build revision: b7df11e3b00a5e7fa987fd4e05ca9d45c3839df5.
Previous runtime digest remains historical evidence, not this repair candidate.
