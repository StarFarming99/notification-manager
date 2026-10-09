package jevshadow

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/pkg/template"
)

func installedScopes(t *testing.T) map[string]DeliveryScope {
	t.Helper()
	rows := []DeliveryScope{}
	for i, id := range []string{"test", "formal"} {
		scope := DeliveryScope{ProfileID: id, ProfileVersion: "v1", CardOwnerID: id + "-owner", ExecutionDomain: id + "-execution", SenderApp: "infra-alerts", Receivers: []string{"critical"}, Destinations: []string{"oc_" + id}, ReceiptTokenEnv: "JEV_" + strings.ToUpper(id) + "_RECEIPT_TOKEN", AnnotationTokenEnv: "JEV_" + strings.ToUpper(id) + "_ANNOTATION_TOKEN", FeedbackTokenEnv: "JEV_" + strings.ToUpper(id) + "_FEEDBACK_TOKEN"}
		for j, name := range []string{scope.ReceiptTokenEnv, scope.AnnotationTokenEnv, scope.FeedbackTokenEnv} {
			t.Setenv(name, strings.Repeat(string(rune('a'+i*3+j)), 32))
		}
		rows = append(rows, scope)
	}
	file := filepath.Join(t.TempDir(), "scopes.json")
	raw, _ := json.Marshal(map[string]interface{}{"version": 1, "profiles": rows})
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JEV_DELIVERY_SCOPES_FILE", file)
	scopes, err := loadDeliveryScopes(true)
	if err != nil {
		t.Fatal(err)
	}
	return scopes
}

func TestPreinstalledScopesKeepHistoricalTestOwnerTokensAndCardAfterRestart(t *testing.T) {
	type received struct {
		path, token string
		body        map[string]interface{}
	}
	calls := make(chan received, 16)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		calls <- received{r.URL.Path, r.Header.Get("Authorization"), body}
		w.WriteHeader(202)
		_, _ = w.Write([]byte(`{"id":"feedback","duplicate":true}`))
	}))
	defer upstream.Close()
	cfg := testConfig(upstream.URL + "/v1/delivery-receipts")
	cfg.Environment = "production"
	cfg.ObservationURL = upstream.URL + "/v1/observations"
	cfg.CardOwnerID = "legacy-owner"
	cfg.ExecutionDomain = "legacy-execution"
	// Scoped production requires six scope credentials, without three extra
	// global credentials or a legacy global owner/execution identity.
	cfg.Token = ""
	cfg.AnnotationToken = ""
	cfg.FeedbackToken = ""
	cfg.CardOwnerID = ""
	cfg.ExecutionDomain = ""
	cfg.DeliveryScopes = installedScopes(t)
	cfg.FeedbackEnabled = true
	cfg.ExistingCallbackIntegrated = true
	t.Setenv("JEV_CARD_STATE_COORDINATED", "true")
	root := t.TempDir()
	cfg.CardStateDir = filepath.Join(root, "cards")
	cfg.ReceiptOutboxDir = filepath.Join(root, "receipts")
	service, err := New(log.NewNopLogger(), cfg, upstream.Client())
	if err != nil {
		t.Fatal(err)
	}
	patcher := func(context.Context, string, map[string]interface{}) error { return nil }
	service.SetCardPatcherResolver(func(context.Context, string, string) (CardPatcher, error) { return patcher, nil })
	token := strings.Repeat("x", 32)
	delivery := func(id, message, chat, owner string) int {
		data := &template.Data{ProfileID: id, ProfileVersion: "v1", CardOwnerID: owner, OriginalDestination: "chat:oc_production", Alerts: template.Alerts{{Status: "firing", Labels: template.KV{"alertname": "fixture", "severity": "critical", "cluster": "us"}, StartsAt: time.Date(2026, 10, 9, 8, 0, 0, 0, time.UTC)}}}
		raw, _ := json.Marshal(SuccessfulDelivery{Data: data, Receiver: "critical", Destination: chat, MessageID: message, SenderApp: "infra-alerts", BaseCard: map[string]interface{}{"elements": []interface{}{}}})
		r := httptest.NewRequest("POST", "/internal/jev/successful-deliveries", bytes.NewReader(raw))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		service.HandleSuccessfulDelivery(token)(w, r)
		return w.Code
	}
	if code := delivery("test", "om_test", "oc_test", "test-owner"); code != 202 {
		t.Fatal(code)
	}
	if code := delivery("test", "om_wrong", "oc_formal", "test-owner"); code != 400 {
		t.Fatal("cross-destination scope accepted", code)
	}
	if code := delivery("test", "om_wrong", "oc_test", "formal-owner"); code != 400 {
		t.Fatal("cross-owner scope accepted", code)
	}
	if code := delivery("formal", "om_formal", "oc_formal", "formal-owner"); code != 202 {
		t.Fatal(code)
	}
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case call := <-calls:
			if call.path == "/v1/delivery-receipts" {
				id := call.body["profile_id"].(string)
				scope := cfg.DeliveryScopes[scopeKey(id, "v1")]
				if call.token != "Bearer "+scope.receiptToken || call.body["card_owner_id"] != scope.CardOwnerID || call.body["execution_domain"] != scope.ExecutionDomain || call.body["profile_version"] != "v1" {
					t.Fatal("receipt lost immutable lane credential/ownership", call)
				}
				seen[id] = true
			}
		case <-time.After(3 * time.Second):
			t.Fatal("scoped receipts missing")
		}
	}
	service.Close()
	// Restart both installed scopes; choosing formal for new work does not
	// change the restored test card's annotation or feedback credential.
	service, err = New(log.NewNopLogger(), cfg, upstream.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	service.SetCardPatcherResolver(func(context.Context, string, string) (CardPatcher, error) { return patcher, nil })
	body, _ := json.Marshal(validV2Component())
	for _, tc := range []struct {
		credential string
		want       int
	}{{cfg.DeliveryScopes["formal:v1"].annotationToken, 401}, {cfg.DeliveryScopes["test:v1"].annotationToken, 200}} {
		r := httptest.NewRequest("PUT", "/internal/jev/annotations/om_test", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+tc.credential)
		r.Header.Set("Idempotency-Key", "historical-test-annotation")
		w := httptest.NewRecorder()
		service.HandleAnnotation(w, r, "om_test")
		if w.Code != tc.want {
			t.Fatal(tc.want, w.Code, w.Body.String())
		}
	}
	feedback := `{"app_id":"infra-alerts","source_event_id":"old-test-click","actor_id":"actor","chat_id":"oc_test","message_id":"om_test","action_reference":"signed","correct_label":"accurate"}`
	r := httptest.NewRequest("POST", "/internal/jev/card-feedback", strings.NewReader(feedback))
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	service.HandleCardFeedback(token)(w, r)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	select {
	case call := <-calls:
		if call.path != "/v1/feedback" || call.token != "Bearer "+cfg.DeliveryScopes["test:v1"].feedbackToken {
			t.Fatal("old test feedback changed principal", call)
		}
	case <-time.After(time.Second):
		t.Fatal("feedback not relayed")
	}
}

func TestExecutorScopeVersionCannotBeReboundOrExposeTokensOnDisk(t *testing.T) {
	scopes := installedScopes(t)
	scope := scopes["test:v1"]
	root := t.TempDir()
	dir, err := bindScopeDirectory(root, scope)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "scope-binding.json"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(scope.receiptToken)) || bytes.Contains(raw, []byte(scope.feedbackToken)) {
		t.Fatal("scope persisted credential value")
	}
	scope.receiptToken = "rotated-credential"
	if _, err := bindScopeDirectory(root, scope); err != nil {
		t.Fatal("credential rotation changed public binding", err)
	}
	for _, change := range []func(*DeliveryScope){func(s *DeliveryScope) { s.CardOwnerID = "different-owner" }, func(s *DeliveryScope) { s.ExecutionDomain = "different-domain" }, func(s *DeliveryScope) { s.SenderApp = "different-app" }, func(s *DeliveryScope) { s.Destinations = []string{"oc_formal"} }} {
		changed := scope
		change(&changed)
		if _, err := bindScopeDirectory(root, changed); err == nil {
			t.Fatal("immutable scope silently rebound", changed)
		}
	}
}
