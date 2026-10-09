# Scoped parallel NM delivery and future stable-Service promotion

Updated 2026-10-09. This description follows the latest development plan. It
supersedes the previous all-receiver/full-region rollout description in this file.
NM's main repository is **StarFarming99/notification-manager**. Linkwei's rc7 is
only the verified production baseline. Business code lives in zilliz-cloud;
deployment overlays live in vdc-deploy-prod. Codex builds NM/executor, Jev and
Console for `harbor.op.zillizcloud.com`; the user builds AIOps when needed.

## Phase-one boundary

Reuse the two existing stock AM instances. The deployment overlay copies only
critical notifications belonging to `feishu-critical-receiver` and production
chat `oc_6c65c3f33939cb742e54cd633f95d3a2`, to test chat
`oc_fbb2c50fed6ea3bd0f0777bea6f35358`. Preserve all original AM branches,
integration indices, timing, muting and destination configuration. No warning,
other chat, user, department, chatbot, history webhook or Notification API
request is folded into the test copy. New NM keeps the original silence,
routing, filtering, aggregation and template pipeline, then applies its local
copy overlay. Original action buttons and explicit card config remain intact.

This change does not guarantee all raw regional events before AM grouping or
muting. The test sibling has its own stock-AM aggregation group and receiver, so its
failure/retry does not join the original bot receiver Fanout. The earlier design
that appended a test integration inside the original bot receiver had that coupling
and is withdrawn. PD remains the existing direct branch. Shared AM resources and
infra-app quotas still require the measured fault and live capacity checks. The final two-peer real-AM/rc7/new-NM fault matrix must
pass before a live test route is enabled. Live wiring, quota, permissions,
feedback and real PD receipt are separate acceptance gates. User approval is
required for deployment; no cluster or real Feishu write is part of code tests.

## Runtime configuration and control

The candidate reads the **existing unique** NotificationManager CR through
`NM_CONFIGURATION_NAME=notification-manager`; it creates no second CR and never
writes shared Receiver/Config credentials. Omission preserves the legacy global
controller behavior. The named instance's Secret/ConfigMap cache is restricted
to `NAMESPACE`; cross-namespace references require a reviewed compatibility/RBAC
change. Cluster-scoped notification CRs and namespace metadata remain read-only.

`NM_DELIVERY_PROFILES_FILE` is strict JSON with version 1, `initial_test_version`,
`retry_dedupe_window` (2m), `repeat_interval` (12h), and exactly two `profiles`:

- `test`: immutable `version`, `card_owner_id`, `receiver`, `source_chat_id`,
  `test_chat_id`; the two chats must be the IDs above.
- `formal`: immutable `version`, `card_owner_id`; original destinations remain
  in the frozen plan. Formal starts unprepared/inactive.

`NM_TEST_INTAKE_TOKEN`, `NM_FORMAL_INTAKE_TOKEN`, `NM_PROFILE_CONTROL_TOKEN` are
independent credentials of at least 32 bytes. `/api/v2/test/alerts` accepts only
the test token. Test `/verify` and `/notifications` reject requests. Existing
`/api/v2/alerts`, `/verify`, `/notifications` require the formal token while
profiles are enabled; an inactive/paused formal profile returns 503.

GET `/internal/delivery-profiles` reports persistent revision/state and target
counts by lane. POST `/internal/delivery-profiles/{test|formal}/{prepare|activate|pause|resume}`
requires the control token and JSON `expected_revision`, `version`, `actor`,
`evidence`. Prepare checks the actual enabled Receiver selector, original chat,
credentials, original template and durable adapter. The first test activation
performs these checks at startup. CAS rejects races/stale revisions; restart
retains state. Pause stops intake and future claims. To drain, first remove the
AM test route, verify no new intake, leave test sending active until delivered
or reconciled, then pause. In-flight external sends need explicit reconciliation.

Accepted plans freeze profile ID/version/owner, actual destination, original
production destination, rendered content, Receiver and transport options.
`content_revision` hashes that actual snapshot; profile version identifies the
delivery overlay, not every live CR edit. Changing the active formal profile
never rewrites historical test targets. Literal Feishu appSecret values remain
only in the original Config: the spool stores Config name/UID/public appID and
resolves the credential at send time. Same-app secret rotation is supported;
Config replacement/app identity drift is a known pre-send rejection. Preparation,
freeze and send also check the public appID against the executor's scope.

## Preinstalled receipt and card scopes

Both NM and executor mount `JEV_DELIVERY_SCOPES_FILE`. Strict JSON contains
`version: 1` and `profiles` entries with `profile_id` (`test` or `formal`),
`profile_version`, `card_owner_id`, `execution_domain`, `sender_app` (real appID),
`receivers` and `destinations` allowlists. Each entry names
`receipt_token_env`, `annotation_token_env`, `feedback_token_env`.
NM parses public bindings without reading those six credential values; executor
resolves independent credentials, all at least 32 bytes. Jev must preload the
matching principals and card owners, including each receipt principal's profile
ID, destinations, domain, owner and app. Upgrade/migrate Jev's optional profile
receipt contract before enabling the NM producer.

The executor selects the scope from **frozen profile ID/version and owner**.
Each scope has its own receipt principal/outbox/card binding directory, under
one process-level fenced root. `scope-binding.json` persists the public identity;
reusing the same ID/version with different owner/domain/app/destination is
rejected. Credential values never enter that file. Keep older scope entries
installed for historical cards/pending work. Startup validates all registered
NM profile versions, not just the newly active version. When no scope file is configured, empty-profile legacy receipts keep their
existing global behavior. Scoped mode rejects missing profile provenance and
requires only the six scope credentials, not extra global receipt/annotation/
feedback tokens or a global owner/domain.

Scope config and credentials are read on controlled restart; no file hot reload
is implemented. After restart/formal activation, old test receipts, annotations
and feedback continue to use test credentials and destinations. Cross-lane
annotation tokens and cross-owner/destination success envelopes are rejected.
The executor hosts `/internal/jev/successful-deliveries` with independent
`JEV_EXECUTOR_TOKEN`; it never starts a production WebSocket consumer.

New feedback buttons require `JEV_SHADOW_FEEDBACK_ENABLED=true`,
`JEV_EXISTING_CALLBACK_INTEGRATED=true` and verified writer coordination. The
existing callback must POST the seven verified snake_case fields (`app_id`,
`source_event_id`, `actor_id`, `chat_id`, `message_id`, `action_reference`,
`correct_label`) to `/internal/jev/card-feedback` using independent
`JEV_CALLBACK_RELAY_TOKEN`. The relay is bounded to two seconds and rejects
redirects; actual message binding selects the historical lane. Until the real
callback helper is installed and validated, keep the gate off. Original buttons
remain; invalid new feedback actions are omitted.

## Durable retry, capacity and quota

AM normally supplies no Idempotency-Key. Canonical identity includes trusted
profile/version, transport receiver, groupKey and sorted alert status/labels/
annotations/startsAt plus resolved EndsAt; peer externalURL and rolling firing
EndsAt do not create another card. Concurrent/ACK-loss retries reuse the durable
plan through the configured **12h repeat interval**, not only the declared 2m
retry window. Pending/retryable/sending/unknown and unreconciled dead-letter
work never creates another automatic copy merely because time elapsed. Changed
content/status creates a new intent; completed identical content can repeat at
the configured AM repeat boundary. Canonical history survives restart.

`NM_NOTIFICATION_SEND_INTERVAL` defaults to 2s for profile instances and bounds
all workers and retries together (at most 30 send attempts/minute). Legacy
instances keep their existing behavior. This is a sender budget; real shared
infra-app quota headroom still needs live verification.

`NM_NOTIFICATION_SPOOL_MAX_TARGETS`, `MAX_BYTES`, `RESERVE_BYTES` (all with the
full `NM_NOTIFICATION_SPOOL_` prefix) preserve defaults 100000 active targets,
8GiB file budget, 64MiB reserve. Only pending/retryable/sending/unknown count
against active capacity. Confirmed completed payloads are cleaned after 48h,
at most 200 intakes per minute. Unknown/unreconciled work is retained. Compact
intake/dedupe tombstones retain explicit-key replay protection; metadata/audits
and retained uncertainty still use the physical byte budget. Bolt reuses freed
pages; file compaction/export is an offline maintenance step and never deletes
uncertain ownership. Byte exhaustion or commit failure returns 503/readiness
failure rather than accepting unpersisted notifications. `/status` and `/metrics`
expose state counts, active/file limits and sanitized storage failure counters;
a successful durable write clears a storage-outage health signal. The relay's
existing gap journal/metrics expose missing receipt correlation.

## Future stable-Service handoff and current limitations

No AM/notification-api DNS replacement is needed for future promotion. The new
same NM image can enable an additional **default-disabled** formal compatibility
listener (`NM_FORMAL_COMPAT_LISTEN_ADDRESS=:19093`) while authenticated test/
control stays at :19094. `NM_FORMAL_COMPAT_SOURCE_CIDRS` must contain explicit
canonical source ranges, never /0. The listener checks direct RemoteAddr and
ignores X-Forwarded-For, serves only legacy APIs, has no test/control/annotation
routes and returns 503 until formal is prepared/active. Deployment NetworkPolicy
must also restrict actual AM/notification-api identities; real CNI source-IP
behavior is an unverified promotion gate.

An upgraded Operator consumes the annotation
`notification.kubesphere.io/sender-handoff` on the existing unique CR:
`operation_id`, `phase` (active/retired/rollback), candidate `deployment`,
`deployment_uid`, disjoint `selector`, and `drain_evidence` for retirement.
It continually reconciles the stable Service, preserves ClusterIP and recreates
it directly on the selected Ready backend. The candidate must explicitly enable
compatibility on :19093, keep its primary listener on :19094, and use
`/-/formal-ready` on :19094 as its readinessProbe. That endpoint checks enabled
compatibility, prepared/active formal configuration and durable storage. A test
`/-/ready` probe cannot authorize takeover. Switch the probe through a controlled
rollout and wait for formal readiness before submitting handoff intent. Active retains the old sender for
verified drain. Retired keeps the owned original Deployment at zero, including
recreation. Rollback readies the original before switching the Service back.
Removing an active annotation is rejected as an implicit unsafe rollback.

Test rollout leaves the current Operator unchanged. Future handoff requires
the bundled `/notification-manager-operator` consumer enabled by a separately
reviewed Operator workload upgrade, actual old rc7 queue/in-flight
reconciliation evidence, real source fencing, original callback ownership, and
complete receiver/channel/API parity. Literal custom chatbot credentials and
other notifier adapters still block complete formal preparation. The annotation
cannot prove old drain by itself. Fake-client reconciliation tests do not close
these live gates. A01–A17 are not automatically passed by runtime unit tests.
