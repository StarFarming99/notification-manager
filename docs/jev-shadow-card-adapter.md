# Jev shadow card adapter

This optional adapter is for an isolated shadow-mode receiver. It preserves the original
Feishu send result: receipt delivery and Jev failures do not turn a successful original
notification into a failure.

The supported path is a Feishu application receiver using `chatids` with an interactive
card. Custom webhook robots and batch sends are intentionally excluded because they do
not provide the same updateable application-message contract.

## Runtime configuration

The feature is disabled unless `JEV_SHADOW_ENABLED=true`. When enabled, the variables in
the following table are required except the two storage paths, which have documented defaults.

| Variable | Purpose |
| --- | --- |
| `JEV_SHADOW_RECEIPT_URL` | Full Jev `POST /v1/delivery-receipts` URL |
| `JEV_SHADOW_TOKEN` | Bearer token shared with Jev and the internal card-host endpoint |
| `JEV_SHADOW_RECEIVER_ALLOWLIST` | Comma-separated actual Feishu Receiver names |
| `JEV_SHADOW_DESTINATION_ALLOWLIST` | Comma-separated Feishu chat IDs |
| `JEV_SHADOW_LOGICAL_SOURCE` | Stable Notification Manager cluster identity |
| `JEV_SHADOW_ENVIRONMENT` | Environment recorded in receipts |
| `JEV_SHADOW_SOURCE_REGION` | Source region recorded in receipts |
| `JEV_SHADOW_PERMISSION_DOMAIN` | Permission boundary recorded in receipts |
| `JEV_SHADOW_OBSERVATION_RECEIVER` | Name of the parallel Jev webhook receiver used to derive the same delivery ID |
| `JEV_SHADOW_SENDER_APP` | Non-secret application identity recorded in receipts |
| `JEV_SHADOW_EXPIRES_AT` | Absolute RFC3339 UAT cutoff; restart does not extend it |
| `JEV_SHADOW_RECEIPT_OUTBOX_DIR` | Receipt outbox path; mount it on persistent storage to survive pod replacement (default `/tmp/notification-manager/jev-shadow-receipts`) |
| `JEV_SHADOW_CARD_STATE_DIR` | Base card, binding, component, revision, idempotency, and patch-state path (default `<receipt-outbox>/card-state`) |
| `JEV_SHADOW_FEEDBACK_ENABLED` | Explicitly start the allowlisted Feishu application's `card.action.trigger` long-connection consumer and relay verified overall feedback to Jev (default `false`) |

The retry policy can be tuned with `JEV_SHADOW_RECEIPT_MAX_ATTEMPTS` (default `12`),
`JEV_SHADOW_RECEIPT_RETRY_MIN` (default `1s`), `JEV_SHADOW_RECEIPT_RETRY_MAX`
(default `5m`), and `JEV_SHADOW_RECEIPT_DRAIN_TIMEOUT` (default `5s`). Durations use Go
duration syntax.

When an allowlisted application-card send succeeds, Notification Manager records the
returned `message_id`, retains the base card, and asynchronously posts a delivery receipt.
At the start of each notify-stage fanout, Notification Manager creates one random attempt
identity and derives each group `deliveryID` from that attempt plus the stable group labels.
Webhook and Feishu therefore keep the same delivery envelope even when their selectors yield
different member sets (for example A+B versus A). The receipt still lists only the members
actually present in the sent card; `(delivery_id, fingerprint, starts_at)` is the immutable
per-member occurrence identity. Jev must fail closed if a listed member is absent or ambiguous.
Before returning from the capture hook, the adapter atomically persists the receipt with mode
`0600`. The sender recovers pending entries after restart, applies exponential backoff, and
moves exhausted or permanent failures to the `dead-letter` directory instead of discarding
them. Shutdown makes one bounded drain attempt for each pending entry; anything still pending
remains on disk for the next process. Receipt endpoint failures remain fail-open for the
already-successful Feishu notification.
If the outbox cannot be initialized, the optional Jev adapter disables itself and logs the
error; Notification Manager continues serving the original notification path.
Jev submits components to:

```text
PUT /internal/jev/annotations/{message_id}
Authorization: Bearer <JEV_SHADOW_TOKEN>
Idempotency-Key: <stable payload key>
```

The adapter serializes updates for each card, validates base and annotation revisions,
preserves the original card elements and actions, and uses the same Feishu application
credentials to patch the message. Reusing an idempotency key with different content or
submitting a stale revision is rejected.

Context v2 decoder bounds come from Jev's generated
`annotation-component-v2.schema.json`. Notification Manager vendors that schema together with
the generated boundary-case fixture under `pkg/jevshadow/testdata`; tests run those Python-valid
and Python-invalid payloads through the Go decoder. String limits count Unicode code points, not
UTF-8 bytes.

The base card, writer identity, annotation components, global annotation revision,
idempotency records, and confirmed/failed/unknown patch state are atomically persisted. A
per-message advisory filesystem lock serializes processes sharing the same volume. Before a
PATCH, the desired rendered card and its hash are persisted as an intent. A confirmed HTTP
200 plus Feishu `code=0` commits the revision; a confirmed rejection can retry, while a
timeout, HTTP 202/5xx, broken response, or crash between PATCH and commit remains `unknown`
and fails closed instead of replaying a stale full card. After restart, Notification Manager
resolves the original application writer from the live receiver plus destination; missing or
ambiguous matches return a retryable writer-unavailable response.

The compact classification component appends `准确` and `不准确` buttons. Both use the
`jev_feedback` action and carry only an overall verdict plus Jev's signed action reference. When
feedback is explicitly enabled, Notification Manager connects as the same allowlisted Feishu
application that sent the card. It takes operator/chat/message identity only from the verified
`card.action.trigger` event and relays it to Jev `POST /v1/feedback`; identity fields in the button
are never accepted. Jev deduplicates redelivery by Feishu event ID and treats a later click by the
same actor as an audited opinion revision.

The Feishu application must use long-connection callback delivery and have
`card.action.trigger` enabled in the developer console. A successful WebSocket connection alone
does not prove this console-side subscription; UAT acceptance requires a real user click, a
success toast, and the matching feedback row in Jev.

## UAT boundary

The Jev writer state now survives restart when its configured path survives, and concurrent
Jev writers sharing that path are serialized. This is still **not the complete production
single-writer design**: feedback callbacks only return a toast and do not rewrite the card. This
fork still contains no ACK, recovery, silence, or other card-action writer routed through the same
state machine. If another service changes the same
Feishu message, Notification Manager cannot fetch and merge that newer remote base; a later
full-card PATCH could otherwise restore stale firing/unacknowledged content. Production and
any UAT receiver with an active external card-mutating callback must therefore remain blocked
until that writer is integrated or a verified remote-card reconciliation path is added.

For production executors with versioned delivery scopes, verified coordination is a
separate gate from writer activation. `JEV_ANNOTATION_ENABLED_PROFILES=test:v1`
enables only cards already bound to that installed profile/version; the default
keeps every production profile closed. Other installed scopes retain receipts and
history but return 503 for annotation even with their own valid credential. A scope
name or version absent from the installed registry fails startup. Development and
UAT annotation behavior is unchanged. The existing executor coordination gate
must also remain closed until the deployed callback's actual write paths are audited.

Receipt and card state persist at their configured paths, but a container-local filesystem
does not survive Kubernetes pod replacement. UAT must mount both paths on the same writable,
single-writer or advisory-lock-capable persistent volume when pod-level recovery is required.
Dead-letter entries and card bindings are retained for inspection, so the deployment also
needs an operator-owned retention policy.

The adapter never enables suppression, silence, PagerDuty changes, or alternate routing.
Disable it by removing the variables or setting `JEV_SHADOW_ENABLED=false`.
