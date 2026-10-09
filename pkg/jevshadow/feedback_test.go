package jevshadow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRelayCardFeedbackUsesVerifiedCallbackIdentity(t *testing.T) {
	var received map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/feedback" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("unexpected authorization %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"id":"feedback-1","duplicate":false}`))
	}))
	defer server.Close()

	service := &Service{
		config: Config{
			Enabled:              true,
			FeedbackEnabled:      true,
			ReceiptURL:           server.URL + "/v1/delivery-receipts",
			Token:                "test-token",
			ReceiverAllowlist:    map[string]struct{}{"jev-shadow-uat-receiver": {}},
			DestinationAllowlist: map[string]struct{}{"oc_uat": {}},
			ExpiresAt:            time.Now().Add(time.Hour),
		},
		client: server.Client(),
	}
	result, err := service.RelayCardFeedback(context.Background(), CardFeedback{
		SourceEventID:   "event-1",
		ActorID:         "ou_operator",
		ChatID:          "oc_uat",
		MessageID:       "om_message",
		ActionReference: "signed-action-reference",
		CorrectLabel:    "accurate",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ID != "feedback-1" || result.Duplicate {
		t.Fatalf("unexpected result %#v", result)
	}
	for key, want := range map[string]interface{}{
		"source_event_id": "event-1",
		"actor_id":        "ou_operator",
		"chat_id":         "oc_uat",
		"message_id":      "om_message",
		"dimension":       "overall",
		"correct_label":   "accurate",
	} {
		if got := received[key]; got != want {
			t.Fatalf("%s=%#v, want %#v", key, got, want)
		}
	}
}

func TestExistingCallbackRelayRequiresAuthenticatedMatchingAppAndChat(t *testing.T) {
	var calls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(202)
		_, _ = w.Write([]byte(`{"id":"feedback","duplicate":true}`))
	}))
	defer upstream.Close()
	cfg := testConfig(upstream.URL + "/v1/delivery-receipts")
	cfg.Environment = "production"
	cfg.FeedbackEnabled = true
	cfg.ExistingCallbackIntegrated = true
	s := &Service{config: cfg, client: upstream.Client()}
	token := strings.Repeat("c", 32)
	body := `{"app_id":"infra-alerts","source_event_id":"event","actor_id":"actor","chat_id":"oc_test","message_id":"message","action_reference":"signed","correct_label":"accurate"}`
	for _, tc := range []struct {
		body, token string
		want        int
	}{{body, "", 401}, {strings.Replace(body, "infra-alerts", "wrong-app", 1), token, 403}, {strings.Replace(body, "oc_test", "other-chat", 1), token, 403}, {strings.TrimSuffix(body, "}") + `,"unexpected":"value"}`, token, 400}, {body + `{}`, token, 400}, {body, token, 202}} {
		r := httptest.NewRequest("POST", "/internal/jev/card-feedback", strings.NewReader(tc.body))
		r.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		s.HandleCardFeedback(token)(w, r)
		if w.Code != tc.want {
			t.Fatal(tc.want, w.Code, w.Body.String())
		}
	}
	if calls != 1 {
		t.Fatal("rejected callback reached upstream", calls)
	}
	s.config.ExistingCallbackIntegrated = false
	r := httptest.NewRequest("POST", "/internal/jev/card-feedback", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	s.HandleCardFeedback(token)(w, r)
	if w.Code != 503 || s.FeedbackEnabled() {
		t.Fatal("uninstalled production callback claimed ready", w.Code)
	}
}

func TestRelayCardFeedbackRejectsUntrustedValues(t *testing.T) {
	service := &Service{
		config: Config{
			Enabled:              true,
			FeedbackEnabled:      true,
			ReceiptURL:           "http://example.invalid/v1/delivery-receipts",
			Token:                "test-token",
			ReceiverAllowlist:    map[string]struct{}{"receiver": {}},
			DestinationAllowlist: map[string]struct{}{"oc_uat": {}},
			ExpiresAt:            time.Now().Add(time.Hour),
		},
		client: http.DefaultClient,
	}
	for name, feedback := range map[string]CardFeedback{
		"wrong chat":  {SourceEventID: "event", ActorID: "ou", ChatID: "oc_other", MessageID: "om", ActionReference: "signed", CorrectLabel: "accurate"},
		"wrong label": {SourceEventID: "event", ActorID: "ou", ChatID: "oc_uat", MessageID: "om", ActionReference: "signed", CorrectLabel: "oncall"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := service.RelayCardFeedback(context.Background(), feedback); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}
