# Scoped parallel NM delivery and future stable-Service promotion

Updated 2026-10-12. This description follows the latest development plan. It
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
content/status advances the current receiver/group generation; returning to
previously delivered content also creates a new intent (the prior global
content-hash ledger incorrectly suppressed this). Same-generation peer retries
reuse the accepted intent; historical unfinished/unknown intents stay fenced
until reconciliation. Completed identical current content can repeat at the
configured AM repeat boundary. Canonical indexes survive restart.

An AM webhook has no monotonic group sequence: delivered A → delivered B →
late old A is indistinguishable from legitimate A → B → A. The sender preserves
legitimate updates and cannot promise exactly-once under arbitrary delivered-peer
reordering. Formal takeover remains gated on a verified single ordered admission
path or an explicitly reviewed accounting/cutover strategy; do not silently
restore global historical-hash suppression to hide this protocol limit.

Upgrading a populated legacy ledger requires an explicit paused migration. Old
records contain only the content identity and cannot reconstruct the last AM
group automatically. Pause the profile, stop the only writer, export/check the
ledger, prove each group’s last accepted intake, then run
`notification-spool-recovery --action seed-am-group` with exact profile/version,
expected paused revision, group hash, request hash, intake, actor and evidence.
This operation only seeds indexes and audits evidence; it never changes target
outcomes or sends. Missing proof keeps the upgrade blocked. Keep the original
ledger backup for rollback; running both formats against one live spool is forbidden.

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
routes and returns 503 until formal is prepared/active. The current production CNI does not enforce the declared NetworkPolicy and the
user requested no NetworkPolicy change. Direct source-CIDR application filtering
must therefore be independently verified for real AM/Notification API callers;
use the narrowest proven source identities and do not treat an inert policy as
isolation evidence. Other callers must fail before formal intake.

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
complete receiver/channel/API parity. All original notifier adapters now have frozen formal plans; CR literal credentials
are referenced by original UID and inline API credentials require the independent
keyring described below. This code compatibility does not close actual
provider parity or old-queue reconciliation. The annotation
cannot prove old drain by itself. Fake-client reconciliation tests do not close
these live gates. A01–A17 are not automatically passed by runtime unit tests.


## Formal channel and credential compatibility added on 2026-10-12

Formal retains Email, SMS, Slack, Telegram, Discord, DingTalk, WeChat,
Pushover, Feishu and Webhook. Each original destination has its own immutable
plan/outcome. Feishu/Webhook preserve the accepted rendered content; other
channels freeze the original template catalogue, language/dictionary, data and
options and reuse their original notifier (including HTML/title/splitting).
Only the registered critical chat is eligible for Jev capture. Unregistered
original chats and channels still deliver. Notification/Verify API requests
retain their original receiver/config semantics and explicitly exclude Jev
capture. The test profile remains restricted to its single approved critical
receiver and test group; it still rejects Notification/Verify requests.

Literal Credential values matching the original Receiver/Config CR are replaced
with name/UID/field references. Same-owner secret rotation remains recoverable;
replacement UID, sender-app drift, or chatbot destination drift fails before
send. No accepted task can redirect itself to a newly configured endpoint.

Inline Notification/Verify credentials have no CR owner. Set
`NM_FROZEN_CREDENTIAL_ACTIVE_KEY` to a key ID and
`NM_FROZEN_CREDENTIAL_KEYS` to a JSON map from key ID to base64 32-byte key.
Mount these from a newly generated, independent Kubernetes Secret; never reuse
Jev signing/model keys or an app secret. Codex generates the random key during
a separately authorized rollout; no user needs to paste a credential. AES-256-GCM
binds ciphertext to its frozen field path and key ID; nonces are fresh per
snapshot. Sensitive URL userinfo/query fields and Pushover user keys also use
sealed scalar references. Restart retains ciphertext usability. Rotation selects
a new active key while all old keys remain until their queues have drained or
been audited/reconciled. Losing an old key is a known pre-send retryable failure,
not permission to send with new credentials or to discard uncertain work.

Real POST/SMTP/SMS boundaries retain the worker delivery-attempt marker. Token
acquisition is a credential preflight and cannot imply notification delivery.
A transport/response loss after notification started is `unknown` and is never
automatically resent; a credential failure before notification started is
retryable. Notifier deadlines remain their original per-channel settings and the
worker deadline; no global five-second HTTP client cap is introduced.

Admission failures expose low-cardinality fixed reason classes through
`nm_notification_admission_failures_total` and `/status.admission_failures`.
Logs contain only fixed classes and the canonical AM group hash, never payloads,
URLs or arbitrary credential-bearing error strings. These counters start with the
new process and cannot retrospectively explain the five prior production 503s.

The upgraded executor adds authenticated read-only
`GET /internal/jev/annotations/{message_id}?profile_id=test&profile_version=v1&sender_app={app_id}&annotation_revision={revision}`.
Use the exact historical profile's annotation Bearer credential. It verifies the
profile/app/message/destination binding before returning durable base/patch state,
revision/hash/request ID and `confirmed`, with no card content or credentials.
A missing/overwritten durable proof is conservatively unconfirmed. This route
creates no callback consumer and changes no card. The old deployed executor
cannot supply this evidence until its image is upgraded.

Code and mock tests cannot prove old rc7's volatile queue is drained, a live
SMTP/SMS provider delivered, actual source-CIDR fences work, platform PATCH
changed the same message, or original buttons worked. Keep old NM and PD until
these separately recorded gates pass. The sole original callback stays owned
by the original consumer throughout takeover and rollback.
