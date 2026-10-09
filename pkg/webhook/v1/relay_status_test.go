package v1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/controller"
	"github.com/kubesphere/notification-manager/pkg/jevshadow"
	"github.com/kubesphere/notification-manager/pkg/store"
	"github.com/kubesphere/notification-manager/pkg/template"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRelayFailuresAreVisibleThroughLiveMetricsAndStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	t.Setenv("JEV_SHADOW_ENABLED", "true")
	t.Setenv("JEV_SHADOW_SENDER_APP", "fixture-app")
	t.Setenv("JEV_SHADOW_RECEIVER_ALLOWLIST", "fixture-receiver")
	t.Setenv("JEV_SHADOW_DESTINATION_ALLOWLIST", "fixture-chat")
	t.Setenv("JEV_EXECUTOR_SUCCESS_URL", server.URL)
	t.Setenv("JEV_EXECUTOR_TOKEN", strings.Repeat("t", 32))
	shadow, err := jevshadow.NewNMFromEnv(log.NewNopLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	u := "http://unused.invalid"
	ctl, err := controller.NewStatic(context.Background(), log.NewNopLogger(), controller.StaticConfiguration{Environment: "test", Cluster: "fixture", Receivers: []v2beta2.Receiver{{ObjectMeta: metav1.ObjectMeta{Name: "fixture"}, Spec: v2beta2.ReceiverSpec{Webhook: &v2beta2.WebhookReceiver{URL: &u}}}}})
	if err != nil {
		t.Fatal(err)
	}
	ctl.SetJevShadow(shadow)
	handler := New(log.NewNopLogger(), time.Second, ctl, &store.AlertStore{})
	shadow.CaptureSuccessfulDelivery(&template.Data{Alerts: template.Alerts{{ID: "fixture", Labels: template.KV{"alertname": "fixture"}}}}, "fixture-receiver", "fixture-chat", "fixture-message", nil, nil)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if shadow.RelayStatus()["failed"] > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	metrics := httptest.NewRecorder()
	handler.ServeMetrics(metrics, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(metrics.Body.String(), "nm_jev_relay_failed 1") {
		t.Fatal(metrics.Body.String())
	}
	status := httptest.NewRecorder()
	handler.ServeStatus(status, httptest.NewRequest("GET", "/status", nil))
	var body map[string]interface{}
	if json.Unmarshal(status.Body.Bytes(), &body) != nil || body["jev_relay"] == nil {
		t.Fatal(status.Body.String())
	}
	ready := httptest.NewRecorder()
	handler.ServeReadinessCheck(ready, httptest.NewRequest("GET", "/ready", nil))
	if ready.Code != 200 {
		t.Fatal("Jev failure withdrew original readiness")
	}
}
