# Full-region parallel validation and promotion

Updated 2026-10-09. The user selected **StarFarming99/notification-manager** as
NM's long-term main repository. Linkwei/notification-manager remains the verified
production rc7 baseline, not the destination for future NM changes.

Business code: [zilliz-cloud #10086](https://github.com/zilliztech/zilliz-cloud/pull/10086).
Deployment design: [vdc-deploy-prod #2360](https://github.com/zilliztech/vdc-deploy-prod/pull/2360),
`jev-alert-center-sidechannel/FULL_REGION_PLAN.md`.

## Required runtime

The region's complete producer stream must independently reach its existing AM
and a separate AM. The existing AM/NM/PD/Vector routes keep their behavior during
parallel validation. The separate AM drives a persistent Observer and this new
NM; the same infra-alerts application sends new NM cards only to the approved test
chat `oc_fbb2c50fed6ea3bd0f0777bea6f35358`. Current AM routes include PD-only
branches, so an old-NM success relay alone cannot prove complete regional intake.
Inventory and prove every producer's target/retry isolation before enabling it.

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
- Complete live regional source wiring and outage/short-alert coverage.

The deployment PR has retired its memory/static snapshot runtime and rejects
all attempts to enable it. These requirements are development/deployment gates,
not a boolean approval switch or evidence from earlier static fixture tests.

## Promotion of the same NM

After full-region dual-group acceptance and separate user approval:

1. Stop parallel AM's test sending to the new NM while Observer intake continues.
   Drain/reconcile the test queue, retain unknown/dead-letter and frozen test plans.
2. Pause the existing AM-to-old-NM handoff safely with upstream retry retained;
   verify old sender drain, unknown outcomes and in-flight cards. Stop it through
   authoritative CR/Operator/GitOps so reconciliation cannot restart a second sender.
3. Change the **same** new NM to the formal destination profile and direct the
   existing AM NM webhook to it. Keep its verified image, PVC, card_owner,
   execution_domain, source identities and Jev episodes. Preserve all production
   receivers, not just the infra critical chat. PD/Vector remain on the existing AM.
4. Verify sole formal sender, endpoint, original receiver/card parity, callback
   ownership and precise request/receipt reconciliation across the handoff.

This is a sender/profile/intake ownership handoff, not merely a chat ID edit or
another rebuilt NM. Existing cards cannot receive cross-chat updates. Rollback
first stops and drains the new formal sender, then restores the old intake/owner;
never activate both formal senders or replay the whole spool.

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
