package jevshadow

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
				Footer:               "影子模式：未实际改变告警路由",
				ActionReference:      &actionReference,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	elements := card["elements"].([]interface{})
	if len(elements) != 3 {
		t.Fatalf("expected divider, summary and feedback actions, got %#v", elements)
	}
	actions := elements[2].(map[string]interface{})["actions"].([]interface{})
	if len(actions) != 2 {
		t.Fatalf("expected two feedback buttons, got %#v", actions)
	}
	value := actions[0].(map[string]interface{})["value"].(map[string]interface{})
	if value["action"] != "jev_feedback" || value["correct_label"] != "accurate" {
		t.Fatalf("unexpected feedback value: %#v", value)
	}
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
		ReceiptQueueSize:     8,
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
