package v1

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/controller"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Exercise the actual AM handler through silence, routing, filter, aggregation,
// frozen rendering and the atomic ledger, rather than calling Submit directly.
func TestDurableAMIngressAndHistoryDependency(t *testing.T) {
	for _, history := range []bool{false, true} {
		t.Run(map[bool]string{false: "original", true: "original-and-history"}[history], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			endpoint := "http://mock.invalid/original"
			historyEndpoint := "http://mock.invalid/history"
			config := controller.StaticConfiguration{Environment: "test", Cluster: "fixture", Receivers: []v2beta2.Receiver{{ObjectMeta: metav1.ObjectMeta{Name: "fixture-original"}, Spec: v2beta2.ReceiverSpec{Webhook: &v2beta2.WebhookReceiver{URL: &endpoint}}}}}
			if history {
				config.History = &v2beta2.HistoryReceiver{Webhook: &v2beta2.WebhookReceiver{URL: &historyEndpoint}}
			}
			ctl, err := controller.NewStatic(ctx, log.NewNopLogger(), config)
			if err != nil {
				t.Fatal(err)
			}
			if err = ctl.Run(); err != nil {
				t.Fatal(err)
			}
			db, err := spool.Open(filepath.Join(t.TempDir(), "notifications.db"), spool.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			h := New(log.NewNopLogger(), time.Second, ctl, &store.AlertStore{Durable: db})
			body := `{"receiver":"bot","groupKey":"fixture","alerts":[{"status":"firing","labels":{"alertname":"Critical","severity":"critical"},"startsAt":"2026-10-09T08:00:00Z"}]}`
			for attempt := 0; attempt < 2; attempt++ {
				request := httptest.NewRequest("POST", "/api/v2/alerts", strings.NewReader(body))
				request.Header.Set("Idempotency-Key", "fixture-http-attempt")
				response := httptest.NewRecorder()
				h.Alert(response, request)
				if response.Code != 200 || response.Header().Get("X-NM-Intake-ID") == "" {
					t.Fatal(response.Code, response.Body.String())
				}
			}
			intakes, targets, err := db.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			expected := 1
			if history {
				expected = 2
			}
			if len(intakes) != 1 || len(targets) != expected {
				t.Fatalf("intakes=%d targets=%d", len(intakes), len(targets))
			}
			claim, err := db.Claim(time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if claim.Target.Receiver == "__nm_internal_history_webhook__" {
				t.Fatal("history claimed before original success")
			}
			if _, err = db.Claim(time.Now()); err != spool.ErrEmpty {
				t.Fatal("unexpected extra send before success", err)
			}
			if err = db.Finish(claim, spool.Delivered, "confirmed", "fixture", time.Now()); err != nil {
				t.Fatal(err)
			}
			if history {
				claim, err = db.Claim(time.Now())
				if err != nil {
					t.Fatal(err)
				}
				if claim.Target.Receiver != "__nm_internal_history_webhook__" || len(claim.Target.DependencyRevisions) != 1 {
					t.Fatal("history dependency missing", claim.Target)
				}
				if ctl.GetHistoryReceivers()[0].GetName() != "" {
					t.Fatal("legacy history identity changed")
				}
			}
		})
	}
}
