package jevshadow

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/pkg/template"
)

func TestFormalWriterStartupFenceKeepsAllVersionsReceiptsAndTestPatches(t *testing.T) {
	receipts := make(chan string, 8)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		if r.URL.Path == "/v1/delivery-receipts" {
			receipts <- payload["profile_id"].(string) + ":" + payload["profile_version"].(string)
		}
		w.WriteHeader(202)
	}))
	defer upstream.Close()
	cfg := testConfig(upstream.URL + "/v1/delivery-receipts")
	cfg.CardStateDir = filepath.Join(t.TempDir(), "cards")
	cfg.ReceiptOutboxDir = filepath.Join(t.TempDir(), "receipts")
	cfg.DeliveryScopes = installedScopes(t)
	for i, id := range []string{"test", "formal"} {
		scope := cfg.DeliveryScopes[scopeKey(id, "v1")]
		scope.ProfileVersion = "v2"
		scope.receiptToken = strings.Repeat(string(rune('g'+i*3)), 32)
		scope.annotationToken = strings.Repeat(string(rune('h'+i*3)), 32)
		scope.feedbackToken = strings.Repeat(string(rune('i'+i*3)), 32)
		cfg.DeliveryScopes[scopeKey(id, "v2")] = scope
	}
	cfg.FormalWriterDisabled = true
	cfg.CutoverReadToken = strings.Repeat("r", 32)
	cfg.WriterControlToken = strings.Repeat("w", 32)
	s, err := New(log.NewNopLogger(), cfg, upstream.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var patches atomic.Int32
	s.SetCardPatcherResolver(func(context.Context, string, string) (CardPatcher, error) {
		return func(context.Context, string, map[string]interface{}) error { patches.Add(1); return nil }, nil
	})
	relay := strings.Repeat("x", 32)
	for _, id := range []string{"test", "formal"} {
		for _, version := range []string{"v1", "v2"} {
			scope := cfg.DeliveryScopes[scopeKey(id, version)]
			mid := "om_" + id + "_" + version
			data := &template.Data{ProfileID: id, ProfileVersion: version, CardOwnerID: scope.CardOwnerID, OriginalDestination: "chat:oc_production", Alerts: template.Alerts{{Status: "firing", Labels: template.KV{"alertname": "fixture", "severity": "critical"}, StartsAt: time.Now().UTC()}}}
			raw, _ := json.Marshal(SuccessfulDelivery{Data: data, Receiver: "critical", Destination: scope.Destinations[0], MessageID: mid, SenderApp: scope.SenderApp, BaseCard: map[string]interface{}{"elements": []interface{}{}}})
			request := httptest.NewRequest("POST", "/internal/jev/successful-deliveries", bytes.NewReader(raw))
			request.Header.Set("Authorization", "Bearer "+relay)
			response := httptest.NewRecorder()
			s.HandleSuccessfulDelivery(relay)(response, request)
			if response.Code != 202 {
				t.Fatal("static formal PATCH fence blocked receipt", id, version, response.Code, response.Body.String())
			}
		}
	}
	received := map[string]bool{}
	for len(received) < 4 {
		select {
		case key := <-receipts:
			received[key] = true
		case <-time.After(3 * time.Second):
			t.Fatal("receipt lost under formal fence", received)
		}
	}
	// A global resume succeeds for test but cannot change the startup formal fence.
	control := func(action string, revision uint64) {
		payload, _ := json.Marshal(map[string]interface{}{"action": action, "expected_revision": revision, "permission_domain": cfg.PermissionDomain, "actor": "fixture", "evidence": "test resume after rollback"})
		r := httptest.NewRequest("POST", "/internal/jev/writer-control", bytes.NewReader(payload))
		r.Header.Set("Authorization", "Bearer "+cfg.WriterControlToken)
		w := httptest.NewRecorder()
		s.HandleWriterControl(cfg.WriterControlToken)(w, r)
		if w.Code != 200 {
			t.Fatal(action, w.Code, w.Body.String())
		}
	}
	control("pause", 1)
	control("resume", 2)
	component, _ := json.Marshal(validV2Component())
	for _, id := range []string{"formal", "test"} {
		for _, version := range []string{"v1", "v2"} {
			scope := cfg.DeliveryScopes[scopeKey(id, version)]
			mid := "om_" + id + "_" + version
			r := httptest.NewRequest("PUT", "/internal/jev/annotations/"+mid, bytes.NewReader(component))
			r.Header.Set("Authorization", "Bearer "+scope.annotationToken)
			r.Header.Set("Idempotency-Key", "fence-test-"+id+"-"+version)
			w := httptest.NewRecorder()
			s.HandleAnnotation(w, r, mid)
			want := 200
			if id == "formal" {
				want = 503
			}
			if w.Code != want {
				t.Fatal("formal fence or test independence violated", id, version, w.Code, w.Body.String())
			}
			if id == "formal" {
				state, err := s.scopeServices[scopeKey(id, version)].cardStore.Load(mid)
				if err != nil || state.PendingPatch != nil || state.AnnotationRevision != 0 {
					t.Fatal("formal fence persisted external intent", state, err)
				}
			}
		}
	}
	if patches.Load() != 2 {
		t.Fatal("formal PATCH escaped fence or test PATCH missing", patches.Load())
	}
	r := httptest.NewRequest("GET", "/internal/jev/writer-control", nil)
	r.Header.Set("Authorization", "Bearer "+cfg.CutoverReadToken)
	w := httptest.NewRecorder()
	s.HandleWriterControl(cfg.WriterControlToken)(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"formal_disabled":true`) {
		t.Fatal("read-only status did not show static formal fence", w.Code, w.Body.String())
	}
}
