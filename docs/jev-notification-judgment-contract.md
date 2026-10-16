# Notification-level ordinary judgments and silent recovery

This is a development contract. It does not activate a profile or change AM,
PD, production sender ownership, or notification routing.

The existing authenticated `PUT /internal/jev/group-annotations/{message_id}`
keeps schema `3` and capability `group-judgment-v1`. Final NM snapshot,
scope, member/episode identities, base hash/revision, and lifecycle CAS remain
mandatory. Upgrade this consumer before enabling a new producer.

## Ordinary notification result

`kind: notification_judgment` carries `notification_result` and a null
`group_result`. The detail URL is exactly
`/apps/alert-center/traces/{assessment_id}?domain={permission_domain}`.
`notification_result` uses the existing ordinary `policy-facts-v1` semantics:

| Field | Meaning |
| --- | --- |
| `routing` | Actual choice and probabilities for `pool`, `oncall` |
| `pool_reason` | Actual choice and probabilities for `recurring`, `same_incident`, `non_urgent`, `unclear` |
| `policy_proposal`, `policy_reason_code` | Independent policy recommendation and its reason |
| `pool_min_probability` | Applied threshold, or null when inapplicable |
| `decision_source` | `model_policy`, `conservative_policy`, `operator_rule`, or `model_unavailable` |
| `threshold_applied`, `threshold_source` | Actual threshold facts |
| `model_status`, `model_error_code` | Actual availability/failure |
| `context_reduced` | Whether evidence was reduced |

The model distributions are never inferred from old four-choice results or
failures. Unavailable results have both question answers null and retain
original routing. Both Pool and Oncall display the returned classification.
The base content and existing buttons are preserved; one annotation block
and one ordinary Trace link are appended. Pool remains a suggestion and does
not suppress any original notification or PD.

Legacy `group_judgment` render inputs and confirmed four-choice histories stay
readable and reproducible. They are not rewritten as ordinary probabilities.

## Silent terminal confirmation

`kind: lifecycle_only` has null model results, an increasing lifecycle
revision, and `annotation_revision == expected_annotation_revision` (zero
is permitted). Its observed visible revision may lag a concurrent successful
risk PATCH. Base and exact routing/member identities still must match.
Lifecycle revision itself remains a monotonic CAS.

HTTP 200 returns `request_id`, `lifecycle_only: true`, `lifecycle_revision`,
and `visible_annotation_revision`. This confirms durable lifecycle facts,
not a platform PATCH or a visible annotation. Read-only `group-card-v1`
projection adds `lifecycle_fence` and `lifecycle_fence_request_id`; existing
`annotation_revision`/`group_component` still describe the last confirmed
visible update. Pending/unknown intents and their payloads are retained.

Terminal facts are persisted atomically under the existing card-state root's
`lifecycles/` directory, separately from visible PATCH state. The existing
asynchronous trusted NM receipt path also records exact scope/episode
terminal facts, so a new resolved MID can fence an older firing MID without
waiting for MySQL or model work. A true new `startsAt`/episode is unaffected.
Do not discard this directory during state backup/transfer.

The lifecycle lock covers only local reads/fsync/atomic rename. It is never
held across platform HTTP. Thus an in-flight slow or unknown PATCH cannot
prevent same-MID terminal confirmation or another scope's lifecycle facts.
Risk sends recheck base/visible/lifecycle revisions and terminal facts at the
final send gate under the message lock. A confirmed fence rejects later
gated risks, including after restart; it does not retract an external PATCH
already past its final gate/in flight. Async fence transport and the
non-atomic final local gate to external network boundary remain documented
race windows. The platform is not transactionally coupled to this ledger.

New legacy `lifecycle` requests are accepted as silent control facts as well;
old immutable lifecycle render snapshots remain readable. This does not add
same-MID base-card replacement. If a trusted resolved receipt describes an
incompatible same-MID base update, terminal facts persist first and the base
replacement is rejected, preventing later stale risk annotation.
