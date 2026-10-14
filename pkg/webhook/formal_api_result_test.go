package webhook

import (
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
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/controller"
	"github.com/kubesphere/notification-manager/pkg/deliveryprofiles"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestFormalOriginalAPIsReturnActualSendFailureTimeoutAndSuccess(t *testing.T) {
	var sends atomic.Int32
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sends.Add(1)
		switch r.URL.Path {
		case "/failure":
			w.WriteHeader(503)
		case "/slow":
			select {
			case <-r.Context().Done():
			case <-time.After(300 * time.Millisecond):
				w.WriteHeader(200)
			}
		default:
			w.WriteHeader(200)
		}
	}))
	defer mock.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u := mock.URL + "/success"
	ctl, err := controller.NewStatic(ctx, log.NewNopLogger(), controller.StaticConfiguration{Environment: "test", Cluster: "fixture", Receivers: []v2beta2.Receiver{{ObjectMeta: metav1.ObjectMeta{Name: "original"}, Spec: v2beta2.ReceiverSpec{Webhook: &v2beta2.WebhookReceiver{URL: &u}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = ctl.Run(); err != nil {
		t.Fatal(err)
	}
	db, err := spool.Open(filepath.Join(t.TempDir(), "spool.db"), spool.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m, err := deliveryprofiles.New(db, deliveryprofiles.File{Version: 1, RetryDedupeWindow: "2m", RepeatInterval: "12h", Profiles: []spool.DeliveryProfile{{ID: "test", Version: "v1", CardOwnerID: "test-owner", Receiver: "critical", SourceChatID: deliveryprofiles.ProductionChat, TestChatID: deliveryprofiles.TestChat}, {ID: "formal", Version: "v1", CardOwnerID: "formal-owner"}}}, strings.Repeat("t", 32), strings.Repeat("f", 32), strings.Repeat("c", 32))
	if err != nil {
		t.Fatal(err)
	}
	ctl.DeliveryProfiles = m
	if _, err = db.ControlProfile("formal", "prepare", "v1", 1, "fixture", "offline prepare"); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ControlProfile("formal", "activate", "v1", 2, "fixture", "offline activate"); err != nil {
		t.Fatal(err)
	}
	h := New(log.NewNopLogger(), ctl, &store.AlertStore{Durable: db}, &Options{ListenAddress: ":19094", FormalCompatListenAddress: ":19093", FormalCompatSourceCIDRs: []string{"10.0.0.9/32"}, WebhookTimeout: time.Second, WorkerTimeout: 50 * time.Millisecond})
	for _, api := range []string{"/api/v2/verify", "/api/v2/notifications"} {
		for _, path := range []string{"/success", "/failure", "/slow"} {
			body, _ := json.Marshal(map[string]interface{}{"receiver": map[string]interface{}{"metadata": map[string]interface{}{"name": "inline", "labels": map[string]string{"type": "global"}}, "spec": map[string]interface{}{"webhook": map[string]string{"url": mock.URL + path}}}, "alert": map[string]interface{}{"alerts": []interface{}{map[string]interface{}{"labels": map[string]string{"alertname": "fixture"}}}}})
			r := httptest.NewRequest("POST", api, strings.NewReader(string(body)))
			r.RemoteAddr = "10.0.0.9:2222"
			w := httptest.NewRecorder()
			started := time.Now()
			h.compatRouter.ServeHTTP(w, r)
			want := 400
			if path == "/success" {
				want = 200
			}
			if w.Code != want {
				t.Fatal(api, path, w.Code, w.Body.String())
			}
			if path == "/success" {
				var result map[string]interface{}
				if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				expected := "Send alerts successfully"
				if api == "/api/v2/verify" {
					expected = "Verify successfully"
				}
				if result["Message"] != expected {
					t.Fatal("original API response changed", result)
				}
			}
			if strings.Contains(w.Body.String(), "accepted for durable") {
				t.Fatal("pretend synchronous success")
			}
			if path == "/slow" && time.Since(started) > 200*time.Millisecond {
				t.Fatal("original API timeout was ignored")
			}
		}
	}
	if sends.Load() != 6 {
		t.Fatal("unexpected sends", sends.Load())
	}
	_, rows, _ := db.Snapshot()
	if len(rows) != 0 {
		t.Fatal("API failures secretly queued for retry", len(rows))
	}
	// Pause fences synchronous APIs just as AM, before touching a platform.
	if _, err = db.ControlProfile("formal", "pause", "", 3, "fixture", "offline rollback"); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/api/v2/verify", strings.NewReader(`{}`))
	r.RemoteAddr = "10.0.0.9:2222"
	w := httptest.NewRecorder()
	h.compatRouter.ServeHTTP(w, r)
	if w.Code != 503 || sends.Load() != 6 {
		t.Fatal("pause bypassed", w.Code, sends.Load())
	}
}
