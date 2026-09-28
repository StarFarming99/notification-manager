package jevshadow

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/pkg/template"
)

func TestDeriveDeliveryIDMatchesJevAdapter(t *testing.T) {
	startsAt := time.Date(2026, 9, 23, 10, 11, 12, 123456000, time.UTC)
	data := &template.Data{
		GroupLabels: template.KV{"alertname": "NodeMemoryHigh"},
		Alerts: template.Alerts{
			&template.Alert{
				ID:     "alert-id-1",
				Status: "firing",
				Labels: template.KV{
					"alertname": "NodeMemoryHigh",
					"cluster":   "infra-dev",
					"receiver":  "jev-shadow-feishu-uat",
				},
				Annotations: template.KV{"summary": "Memory <high> 告警"},
				StartsAt:    startsAt,
			},
		},
	}
	deliveryID, err := deriveDeliveryID(data, "jev-shadow-uat")
	if err != nil {
		t.Fatal(err)
	}
	if want := "nm_d2d31254f60126734f26278907d77922"; deliveryID != want {
		t.Fatalf("delivery ID mismatch: got %s, want %s", deliveryID, want)
	}
	data.Alerts[0].NotificationTime = time.Date(2026, 9, 23, 10, 15, 0, 0, time.UTC)
	nextDeliveryID, err := deriveDeliveryID(data, "jev-shadow-uat")
	if err != nil {
		t.Fatal(err)
	}
	if nextDeliveryID == deliveryID {
		t.Fatal("a later notification dispatch must receive a distinct delivery ID")
	}
	data.Alerts[0].NotificationTime = time.Time{}
	data.Alerts[0].Labels["channel_specific"] = "feishu"
	data.Alerts[0].Annotations["rendered_by"] = "card-template"
	channelDeliveryID, err := deriveDeliveryID(data, "jev-shadow-uat")
	if err != nil {
		t.Fatal(err)
	}
	if channelDeliveryID != deliveryID {
		t.Fatalf("channel-specific content changed delivery ID: got %s, want %s", channelDeliveryID, deliveryID)
	}
}

func TestStampedDeliveryIDSurvivesChannelSpecificMutation(t *testing.T) {
	service := &Service{config: testConfig("http://jev.example/receipts")}
	data := &template.Data{
		GroupLabels: template.KV{"alertname": "NodeMemoryHigh"},
		Alerts: template.Alerts{
			&template.Alert{
				ID:               "alert-id-1",
				Status:           "firing",
				Labels:           template.KV{"alertname": "NodeMemoryHigh", "cluster": "infra-dev"},
				StartsAt:         time.Date(2026, 9, 24, 1, 0, 0, 0, time.UTC),
				NotificationTime: time.Date(2026, 9, 24, 1, 5, 0, 0, time.UTC),
			},
		},
	}
	if err := service.StampDeliveryID(data); err != nil {
		t.Fatal(err)
	}
	want := data.DeliveryID

	channelData := data.Clone()
	channelData.GroupLabels["channel"] = "feishu"
	channelData.Alerts[0].NotificationTime = channelData.Alerts[0].NotificationTime.Add(time.Second)
	channelData.Alerts[0].Labels["rendered_by"] = "interactive-card"
	receipt, err := service.buildReceipt(channelData, "receiver-a", "chat-a", "om-a", "0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if receipt.DeliveryID != want {
		t.Fatalf("channel mutation changed stamped delivery ID: got %s, want %s", receipt.DeliveryID, want)
	}
}

func TestReceiptAndAnnotationAreFailOpenAndIdempotent(t *testing.T) {
	receiptCh := make(chan deliveryReceipt, 1)
	receiptServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("unexpected authorization header %q", got)
		}
		defer r.Body.Close()
		var receipt deliveryReceipt
		if err := json.NewDecoder(r.Body).Decode(&receipt); err != nil {
			t.Errorf("decode receipt: %v", err)
		}
		receiptCh <- receipt
		w.WriteHeader(http.StatusAccepted)
	}))
	defer receiptServer.Close()

	config := testConfig(receiptServer.URL)
	config.ReceiptOutboxDir = t.TempDir()
	service, err := New(log.NewNopLogger(), config, receiptServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	var patchCalls atomic.Int32
	var patchedCard map[string]interface{}
	data := &template.Data{
		GroupLabels: template.KV{"alertname": "Watchdog"},
		Alerts: template.Alerts{
			&template.Alert{
				ID:          "alert-id-watchdog",
				Status:      "firing",
				Labels:      template.KV{"alertname": "Watchdog", "receiver": "jev-shadow-feishu-uat"},
				Annotations: template.KV{"summary": "Watchdog"},
				StartsAt:    time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC),
			},
		},
	}
	baseCard := map[string]interface{}{
		"config": map[string]interface{}{"update_multi": true},
		"elements": []interface{}{
			map[string]interface{}{
				"tag": "action",
				"actions": []interface{}{
					map[string]interface{}{"tag": "button", "value": map[string]interface{}{"action": "ack"}},
				},
			},
		},
	}
	service.CaptureSuccessfulDelivery(
		data,
		"jev-shadow-feishu-uat",
		"oc_test",
		"om_test",
		baseCard,
		func(_ context.Context, messageID string, card map[string]interface{}) error {
			if messageID != "om_test" {
				t.Errorf("unexpected message ID %s", messageID)
			}
			patchCalls.Add(1)
			patchedCard = card
			return nil
		},
	)

	select {
	case receipt := <-receiptCh:
		if receipt.MessageID != "om_test" || receipt.Receiver != "jev-shadow-uat" {
			t.Fatalf("unexpected receipt: %+v", receipt)
		}
		if len(receipt.Members) != 1 || receipt.Members[0].Fingerprint != "alert-id-watchdog" {
			t.Fatalf("unexpected receipt members: %+v", receipt.Members)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delivery receipt")
	}

	payload := AnnotationComponent{
		SchemaVersion:        "1",
		AnnotationRevision:   1,
		ExpectedBaseRevision: 1,
		JudgmentRefs:         []string{"J-1"},
		MemberEventIDs:       []string{"event-1"},
		Title:                "Jev 旁路观察",
		Recommendation:       "保持原通知",
		EvidenceLines:        []string{"当前无自动抑制"},
		Footer:               "shadow mode",
	}
	assertAnnotationResponse(t, service, payload, http.StatusUnauthorized, "", "idem-123")
	assertAnnotationResponse(t, service, payload, http.StatusOK, "test-token", "idem-123")
	assertAnnotationResponse(t, service, payload, http.StatusOK, "test-token", "idem-123")
	if got := patchCalls.Load(); got != 1 {
		t.Fatalf("idempotent update patched %d times", got)
	}
	elements, ok := patchedCard["elements"].([]interface{})
	if !ok || len(elements) != 3 {
		t.Fatalf("base elements were not preserved: %#v", patchedCard)
	}
}

func TestDisabledConfigIsBackwardCompatible(t *testing.T) {
	t.Setenv(envEnabled, "false")
	config, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(log.NewNopLogger(), config, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	if service.Enabled() {
		t.Fatal("disabled adapter unexpectedly enabled")
	}
	if service.ShouldCapture("any", "any") {
		t.Fatal("disabled adapter captured a delivery")
	}
}

func TestRenderCompactClassificationWithFeedbackButtons(t *testing.T) {
	actionReference := "signed-action-reference"
	detailURL := "https://aiops.example.com/apps/alert-center/traces/J-1"
	card, err := renderCard(
		map[string]interface{}{"elements": []interface{}{}},
		map[string]AnnotationComponent{
			"event-1": {
				SchemaVersion:        "1",
				AnnotationRevision:   1,
				ExpectedBaseRevision: 1,
				JudgmentRefs:         []string{"J-1"},
				MemberEventIDs:       []string{"event-1"},
				Title:                "Jev 告警分类",
				Category:             "repeat",
				Route:                "pool",
				RepeatCount:          3,
				SameKindCount:        1,
				Recommendation:       "重复告警｜第 3 次｜进入告警池",
				EvidenceLines: []string{
					"重复概率：**同次重复通知 90%**｜新问题 7%｜不确定 2%｜同问题复发 1%",
				},
				Footer:          "影子模式：未实际改变告警路由",
				ActionReference: &actionReference,
				DetailURL:       &detailURL,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	elements := card["elements"].([]interface{})
	if len(elements) != 4 {
		t.Fatalf("expected divider, summary, feedback and detail actions, got %#v", elements)
	}
	summary := elements[1].(map[string]interface{})["text"].(map[string]interface{})["content"].(string)
	for _, expected := range []string{
		"Jev 告警分类（Shadow）",
		"结论：重复告警｜第 3 次｜进入告警池",
		"重复概率：**同次重复通知 90%**｜新问题 7%",
	} {
		if !strings.Contains(summary, expected) {
			t.Fatalf("missing %q in compact summary: %s", expected, summary)
		}
	}
	actions := elements[2].(map[string]interface{})["actions"].([]interface{})
	if len(actions) != 2 {
		t.Fatalf("expected two feedback buttons, got %#v", actions)
	}
	value := actions[0].(map[string]interface{})["value"].(map[string]interface{})
	if value["action"] != "jev_feedback" || value["correct_label"] != "accurate" {
		t.Fatalf("unexpected feedback value: %#v", value)
	}
	detail := elements[3].(map[string]interface{})["actions"].([]interface{})[0].(map[string]interface{})
	if detail["url"] != detailURL {
		t.Fatalf("unexpected detail URL: %#v", detail)
	}
	if detail["text"].(map[string]interface{})["content"] != "查看判断详情" {
		t.Fatalf("unexpected detail label: %#v", detail)
	}
}

func TestRenderV2PoolClassificationExactlyAtCardBottom(t *testing.T) {
	component := validV2Component()
	card, err := renderCard(
		map[string]interface{}{
			"header": map[string]interface{}{"title": "original title"},
			"elements": []interface{}{
				map[string]interface{}{"tag": "action", "name": "original actions"},
			},
		},
		map[string]AnnotationComponent{"event-1": component},
	)
	if err != nil {
		t.Fatal(err)
	}
	if card["header"].(map[string]interface{})["title"] != "original title" {
		t.Fatalf("original header changed: %#v", card)
	}
	elements := card["elements"].([]interface{})
	if len(elements) != 5 || elements[0].(map[string]interface{})["name"] != "original actions" {
		t.Fatalf("v2 annotation did not append after the original elements: %#v", elements)
	}
	summary := elements[2].(map[string]interface{})["text"].(map[string]interface{})["content"].(string)
	want := "**Pool 92%** · Oncall 8%\n" +
		"**Recurring**\n" +
		"Recurring 80% · Same incident 12%\n" +
		"Non-urgent 6% · Unclear 2%"
	if summary != want {
		t.Fatalf("unexpected v2 pool summary:\n%s\nwant:\n%s", summary, want)
	}
	actions := elements[3].(map[string]interface{})["actions"].([]interface{})
	if got := actions[0].(map[string]interface{})["value"].(map[string]interface{})["action_reference"]; got != *component.ActionReference {
		t.Fatalf("feedback reference changed: %#v", got)
	}
	detail := elements[4].(map[string]interface{})["actions"].([]interface{})[0].(map[string]interface{})
	if detail["url"] != *component.TraceURL {
		t.Fatalf("unexpected trace URL: %#v", detail)
	}
}

func TestRenderV2OncallHidesHypotheticalPoolReason(t *testing.T) {
	component := validV2Component()
	component.Routing = &ChoiceAnswer{
		Choice:        "oncall",
		Probabilities: map[string]float64{"pool": 0.07, "oncall": 0.93},
	}
	component.PolicyProposal = "oncall"
	component.PolicyReasonCode = "routing_oncall"

	want := "**Oncall 93%** · Pool 7%"
	if got := component.markdown(); got != want {
		t.Fatalf("unexpected v2 oncall summary: %q, want %q", got, want)
	}
}

func TestRenderV2PolicyOverrideReason(t *testing.T) {
	tests := []struct {
		name       string
		reasonCode string
		threshold  *float64
		wantLine   string
	}{
		{
			name:       "threshold",
			reasonCode: "pool_probability_below_threshold",
			threshold:  floatPointer(0.95),
			wantLine:   "Policy: Oncall · Pool threshold 95%",
		},
		{
			name:       "first notification",
			reasonCode: "first_notification_unconfirmed",
			wantLine:   "Policy: Oncall · First notification unconfirmed",
		},
		{
			name:       "routing tie",
			reasonCode: "routing_tie",
			wantLine:   "Policy: Oncall · Routing tie",
		},
		{
			name:       "model selected oncall",
			reasonCode: "model_selected_oncall",
			wantLine:   "Policy: Oncall · Model selected Oncall",
		},
		{
			name:       "pool reason unclear",
			reasonCode: "pool_reason_unclear",
			wantLine:   "Policy: Oncall · Pool reason unclear",
		},
		{
			name:       "pool reason tie",
			reasonCode: "pool_reason_tie",
			wantLine:   "Policy: Oncall · Pool reason tie",
		},
		{
			name:       "critical evidence missing",
			reasonCode: "critical_evidence_missing",
			wantLine:   "Policy: Oncall · Critical evidence missing",
		},
		{
			name:       "material risk present",
			reasonCode: "material_risk_present",
			wantLine:   "Policy: Oncall · Material risk present",
		},
		{
			name:       "recurring episode unverified",
			reasonCode: "recurring_episode_unverified",
			wantLine:   "Policy: Oncall · Recurring episode unverified",
		},
		{
			name:       "incident binding unconfirmed",
			reasonCode: "incident_binding_unconfirmed",
			wantLine:   "Policy: Oncall · Incident binding unconfirmed",
		},
		{
			name:       "safe unknown-code fallback",
			reasonCode: "future_policy_reason",
			wantLine:   "Policy: Oncall",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			component := validV2Component()
			component.PolicyProposal = "oncall"
			component.PolicyReasonCode = test.reasonCode
			component.PoolMinProbability = test.threshold
			if err := component.validate(); err != nil {
				t.Fatalf("valid policy override rejected: %v", err)
			}
			lines := strings.Split(component.markdown(), "\n")
			if got := lines[len(lines)-1]; got != test.wantLine {
				t.Fatalf("unexpected policy line %q, want %q", got, test.wantLine)
			}
		})
	}
}

func TestRenderV2ExceptionalStatusWithoutFabricatedProbabilities(t *testing.T) {
	tests := []struct {
		reason string
		want   string
	}{
		{reason: "protected_scope", want: "Protected · Original routing retained"},
		{reason: "invalid_response", want: "Jev unavailable · Original routing retained"},
	}
	for _, test := range tests {
		t.Run(test.reason, func(t *testing.T) {
			component := validV2Component()
			component.JudgmentID = ""
			component.Routing = nil
			component.PoolReason = nil
			component.PolicyProposal = "original"
			component.PolicyReasonCode = test.reason
			component.TraceURL = nil
			if err := component.validate(); err != nil {
				t.Fatalf("valid exceptional component rejected: %v", err)
			}
			encoded, err := json.Marshal(component)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeAnnotation(bytes.NewReader(encoded))
			if err != nil {
				t.Fatalf("strict decoder rejected explicit null exceptional answers: %v", err)
			}
			if got := decoded.markdown(); got != test.want || strings.Contains(got, "%") {
				t.Fatalf("unexpected exceptional summary: %q", got)
			}
		})
	}
}

func TestDecodeAnnotationIsStrictPerSchema(t *testing.T) {
	component := validV2Component()
	wire := annotationComponentV2{
		SchemaVersion:        component.SchemaVersion,
		JudgmentID:           component.JudgmentID,
		MemberEventIDs:       component.MemberEventIDs,
		AnnotationRevision:   component.AnnotationRevision,
		ExpectedBaseRevision: component.ExpectedBaseRevision,
		Routing:              component.Routing,
		PoolReason:           component.PoolReason,
		PolicyProposal:       component.PolicyProposal,
		PolicyReasonCode:     component.PolicyReasonCode,
		ExecutionMode:        component.ExecutionMode,
		TraceURL:             component.TraceURL,
		ActionReference:      component.ActionReference,
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeAnnotation(bytes.NewReader(encoded)); err != nil {
		t.Fatalf("valid v2 component rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(map[string]interface{})
	}{
		{
			name: "v1 field in v2",
			mutate: func(payload map[string]interface{}) {
				payload["recommendation"] = "must be rejected"
			},
		},
		{
			name: "unknown nested field",
			mutate: func(payload map[string]interface{}) {
				payload["routing"].(map[string]interface{})["confidence"] = 0.99
			},
		},
		{
			name: "missing required field",
			mutate: func(payload map[string]interface{}) {
				delete(payload, "policy_reason_code")
			},
		},
		{
			name: "incomplete fixed options",
			mutate: func(payload map[string]interface{}) {
				delete(payload["routing"].(map[string]interface{})["probabilities"].(map[string]interface{}), "oncall")
			},
		},
		{
			name: "invalid probability sum",
			mutate: func(payload map[string]interface{}) {
				probabilities := payload["routing"].(map[string]interface{})["probabilities"].(map[string]interface{})
				probabilities["pool"] = 0.70
				probabilities["oncall"] = 0.10
			},
		},
		{
			name: "choice is not a highest-probability option",
			mutate: func(payload map[string]interface{}) {
				probabilities := payload["routing"].(map[string]interface{})["probabilities"].(map[string]interface{})
				probabilities["pool"] = 0.40
				probabilities["oncall"] = 0.60
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var payload map[string]interface{}
			if err := json.Unmarshal(encoded, &payload); err != nil {
				t.Fatal(err)
			}
			test.mutate(payload)
			invalid, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeAnnotation(bytes.NewReader(invalid)); err == nil {
				t.Fatal("invalid cross-schema or incomplete payload was accepted")
			}
		})
	}

	v1 := AnnotationComponent{
		SchemaVersion:        "1",
		AnnotationRevision:   1,
		ExpectedBaseRevision: 1,
		JudgmentRefs:         []string{"J-1"},
		MemberEventIDs:       []string{"event-1"},
		Title:                "Jev",
		Category:             "repeat",
		Route:                "pool",
		RepeatCount:          2,
		SameKindCount:        1,
		Recommendation:       "legacy",
		EvidenceLines:        []string{"legacy evidence"},
		Footer:               "shadow",
	}
	v1Encoded, err := json.Marshal(v1)
	if err != nil {
		t.Fatal(err)
	}
	var v1Payload map[string]interface{}
	if err := json.Unmarshal(v1Encoded, &v1Payload); err != nil {
		t.Fatal(err)
	}
	v1Payload["judgment_id"] = "v2-field"
	v1Encoded, err = json.Marshal(v1Payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeAnnotation(bytes.NewReader(v1Encoded)); err == nil {
		t.Fatal("v1 payload with a v2-only field was accepted")
	}
}

func TestDecodeAnnotationPreservesBoundedRoundingWithoutNormalization(t *testing.T) {
	component := validV2Component()
	component.Routing.Probabilities = map[string]float64{"pool": 0.93, "oncall": 0.08}
	component.PoolReason.Probabilities = map[string]float64{
		"recurring": 0.79, "same_incident": 0.12, "non_urgent": 0.06, "unclear": 0.02,
	}
	wire := annotationComponentV2{
		SchemaVersion:        component.SchemaVersion,
		JudgmentID:           component.JudgmentID,
		MemberEventIDs:       component.MemberEventIDs,
		AnnotationRevision:   component.AnnotationRevision,
		ExpectedBaseRevision: component.ExpectedBaseRevision,
		Routing:              component.Routing,
		PoolReason:           component.PoolReason,
		PolicyProposal:       component.PolicyProposal,
		PolicyReasonCode:     component.PolicyReasonCode,
		ExecutionMode:        component.ExecutionMode,
		TraceURL:             component.TraceURL,
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeAnnotation(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("bounded two-decimal rounding was rejected: %v", err)
	}
	if decoded.Routing.Probabilities["pool"] != 0.93 || decoded.Routing.Probabilities["oncall"] != 0.08 {
		t.Fatalf("probabilities were normalized: %#v", decoded.Routing.Probabilities)
	}
}

func TestV2UpdateStablyReplacesSameMemberComponent(t *testing.T) {
	var patchedCards []map[string]interface{}
	service := &Service{
		cards: map[string]*cardBinding{
			"om-v2": {
				baseCard: map[string]interface{}{
					"elements": []interface{}{map[string]interface{}{"tag": "div", "original": true}},
				},
				baseRevision: 1,
				annotations:  make(map[string]AnnotationComponent),
				appliedKeys:  make(map[string]appliedRequest),
				patcher: func(_ context.Context, _ string, card map[string]interface{}) error {
					patchedCards = append(patchedCards, card)
					return nil
				},
			},
		},
	}
	first := validV2Component()
	first.MemberEventIDs = []string{"event-b", "event-a"}
	if _, err := service.applyAnnotation(context.Background(), "om-v2", "idem-v2-first", first); err != nil {
		t.Fatal(err)
	}
	second := validV2Component()
	second.MemberEventIDs = []string{"event-a", "event-b"}
	second.AnnotationRevision = 2
	second.Routing = &ChoiceAnswer{
		Choice:        "pool",
		Probabilities: map[string]float64{"pool": 0.96, "oncall": 0.04},
	}
	if _, err := service.applyAnnotation(context.Background(), "om-v2", "idem-v2-second", second); err != nil {
		t.Fatal(err)
	}
	if len(patchedCards) != 2 {
		t.Fatalf("expected two revisions to patch twice, got %d", len(patchedCards))
	}
	elements := patchedCards[1]["elements"].([]interface{})
	if len(elements) != 5 {
		t.Fatalf("same member set appended a second component: %#v", elements)
	}
	summary := elements[2].(map[string]interface{})["text"].(map[string]interface{})["content"].(string)
	if !strings.Contains(summary, "Pool 96%") || strings.Contains(summary, "Pool 92%") {
		t.Fatalf("latest component did not replace the prior revision: %s", summary)
	}
}

func TestAnnotationRevisionGapIsRetryableAndDoesNotSkip(t *testing.T) {
	var patchCalls atomic.Int32
	service := &Service{
		config: Config{Enabled: true, Token: "test-token"},
		cards: map[string]*cardBinding{
			"om-v2": {
				baseCard: map[string]interface{}{
					"elements": []interface{}{map[string]interface{}{"tag": "div", "original": true}},
				},
				baseRevision: 1,
				annotations:  make(map[string]AnnotationComponent),
				appliedKeys:  make(map[string]appliedRequest),
				patcher: func(_ context.Context, _ string, _ map[string]interface{}) error {
					patchCalls.Add(1)
					return nil
				},
			},
		},
	}
	request := func(component AnnotationComponent, idempotencyKey string) *httptest.ResponseRecorder {
		body, err := json.Marshal(component)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(
			http.MethodPut,
			"/internal/jev/annotations/om-v2",
			bytes.NewReader(body),
		)
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Idempotency-Key", idempotencyKey)
		response := httptest.NewRecorder()
		service.HandleAnnotation(response, req, "om-v2")
		return response
	}

	first := validV2Component()
	if response := request(first, "revision-1"); response.Code != http.StatusOK {
		t.Fatalf("revision 1 failed: %d %s", response.Code, response.Body.String())
	}

	third := validV2Component()
	third.AnnotationRevision = 3
	response := request(third, "revision-3")
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("revision gap returned %d, want 429: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Retry-After") != "1" {
		t.Fatalf("revision gap did not advertise an immediate retry: %#v", response.Header())
	}
	var conflict struct {
		Retryable                  bool `json:"retryable"`
		ExpectedAnnotationRevision int  `json:"expected_annotation_revision"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &conflict); err != nil {
		t.Fatal(err)
	}
	if !conflict.Retryable || conflict.ExpectedAnnotationRevision != 2 {
		t.Fatalf("unexpected revision conflict response: %+v", conflict)
	}
	if got := patchCalls.Load(); got != 1 {
		t.Fatalf("out-of-order revision patched the card %d times", got)
	}

	second := validV2Component()
	second.AnnotationRevision = 2
	if response := request(second, "revision-2"); response.Code != http.StatusOK {
		t.Fatalf("revision 2 failed after rejected revision 3: %d %s", response.Code, response.Body.String())
	}
	if got := patchCalls.Load(); got != 2 {
		t.Fatalf("expected revisions 1 and 2 to patch exactly twice, got %d", got)
	}
}

func TestRenderV2GroupAttributesDifferentJudgmentsToRedactedMembers(t *testing.T) {
	pool := validV2Component()
	pool.MemberEventIDs = []string{"event-sensitive-alpha"}
	oncall := validV2Component()
	oncall.MemberEventIDs = []string{"event-sensitive-beta"}
	oncall.Routing = &ChoiceAnswer{
		Choice:        "oncall",
		Probabilities: map[string]float64{"pool": 0.09, "oncall": 0.91},
	}
	oncall.PolicyProposal = "oncall"
	oncall.PolicyReasonCode = "routing_oncall"

	card, err := renderCard(
		map[string]interface{}{"elements": []interface{}{}},
		map[string]AnnotationComponent{"a-pool": pool, "b-oncall": oncall},
	)
	if err != nil {
		t.Fatal(err)
	}
	elements := card["elements"].([]interface{})
	poolSummary := elements[1].(map[string]interface{})["text"].(map[string]interface{})["content"].(string)
	oncallSummary := elements[5].(map[string]interface{})["text"].(map[string]interface{})["content"].(string)
	if !strings.Contains(poolSummary, "Member: m-5f3c41c5eb85") ||
		!strings.Contains(poolSummary, "**Pool 92%**") {
		t.Fatalf("pool judgment lost its member attribution: %s", poolSummary)
	}
	if !strings.Contains(oncallSummary, "Member: m-f71a3243ce0f") ||
		!strings.Contains(oncallSummary, "**Oncall 91%**") {
		t.Fatalf("oncall judgment lost its member attribution: %s", oncallSummary)
	}
	for _, summary := range []string{poolSummary, oncallSummary} {
		if strings.Contains(summary, "event-sensitive-alpha") ||
			strings.Contains(summary, "event-sensitive-beta") {
			t.Fatalf("raw member identifier leaked into card text: %s", summary)
		}
	}
}

func TestV2MemberAttributionIsBounded(t *testing.T) {
	component := validV2Component()
	component.MemberEventIDs = []string{"event-a", "event-b", "event-c", "event-d"}
	want := "Members: m-acbf3e162e0d · m-d4fd80fd2778 · m-38dbf366343b · +1 more"
	if got := component.memberScopeLine(); got != want {
		t.Fatalf("unexpected bounded member attribution: %q, want %q", got, want)
	}
}

func validV2Component() AnnotationComponent {
	return AnnotationComponent{
		SchemaVersion:        "2",
		JudgmentID:           "J-v2-1",
		MemberEventIDs:       []string{"event-1"},
		AnnotationRevision:   1,
		ExpectedBaseRevision: 1,
		Routing: &ChoiceAnswer{
			Choice:        "pool",
			Probabilities: map[string]float64{"pool": 0.92, "oncall": 0.08},
		},
		PoolReason: &ChoiceAnswer{
			Choice: "recurring",
			Probabilities: map[string]float64{
				"recurring": 0.80, "same_incident": 0.12, "non_urgent": 0.06, "unclear": 0.02,
			},
		},
		PolicyProposal:   "pool",
		PolicyReasonCode: "pool_allowed",
		ExecutionMode:    "shadow",
		TraceURL:         stringPointer("https://aiops.example.com/apps/alert-center/traces/J-v2-1"),
		ActionReference:  stringPointer("signed-action-reference"),
	}
}

func stringPointer(value string) *string {
	return &value
}

func floatPointer(value float64) *float64 {
	return &value
}

func testConfig(receiptURL string) Config {
	return Config{
		Enabled:              true,
		ReceiptURL:           receiptURL,
		Token:                "test-token",
		ReceiverAllowlist:    map[string]struct{}{"jev-shadow-feishu-uat": {}},
		DestinationAllowlist: map[string]struct{}{"oc_test": {}},
		LogicalSource:        "notification-manager-uat",
		Environment:          "uat",
		SourceRegion:         "us-west-2",
		PermissionDomain:     "sre-uat",
		ObservationReceiver:  "jev-shadow-uat",
		SenderApp:            "infra-alerts",
		ExpiresAt:            time.Now().Add(time.Hour),
		MaxCards:             8,
	}
}

func assertAnnotationResponse(t *testing.T, service *Service, payload AnnotationComponent, status int, token, idempotencyKey string) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPut, "/internal/jev/annotations/om_test", bytes.NewReader(body))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	request.Header.Set("Idempotency-Key", idempotencyKey)
	response := httptest.NewRecorder()
	service.HandleAnnotation(response, request, "om_test")
	result := response.Result()
	defer result.Body.Close()
	if result.StatusCode != status {
		contents, _ := io.ReadAll(result.Body)
		t.Fatalf("unexpected status %d, want %d: %s", result.StatusCode, status, contents)
	}
}
