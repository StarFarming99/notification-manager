# Jev shadow card adapter

This optional adapter is for an isolated shadow-mode receiver. It preserves the original
Feishu send result: receipt delivery and Jev failures do not turn a successful original
notification into a failure.

The supported path is a Feishu application receiver using `chatids` with an interactive
card. Custom webhook robots and batch sends are intentionally excluded because they do
not provide the same updateable application-message contract.

## Runtime configuration

The feature is disabled unless `JEV_SHADOW_ENABLED=true`. When enabled, all variables in
the following table are required.

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

When an allowlisted application-card send succeeds, Notification Manager records the
returned `message_id`, retains the base card, and asynchronously posts a delivery receipt.
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

The compact classification component appends `准确` and `不准确` buttons. Both use the
`jev_feedback` action and carry only an overall verdict plus Jev's signed action reference. The
Feishu callback service must take the verified operator/chat/message identity from the callback
event and relay it to Jev `POST /v1/feedback`; it must not trust identity fields from the button.

## UAT boundary

Card bindings are bounded and held in memory. A Notification Manager restart therefore
stops further annotation of cards sent before the restart; it does not affect the original
alert. This is acceptable for the first time-limited UAT run, but it is not the production
single-writer design. Production requires persistent card state and coordination with the
existing callback writer before the feature is enabled beyond an isolated receiver.

The adapter never enables suppression, silence, PagerDuty changes, or alternate routing.
Disable it by removing the variables or setting `JEV_SHADOW_ENABLED=false`.
