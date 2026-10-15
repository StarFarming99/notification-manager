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
	"github.com/kubesphere/notification-manager/pkg/instancecontrol"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func instanceFixture(t *testing.T, mode, endpoint string) (*Webhook, *spool.Store) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ctl, err := controller.NewStatic(ctx, log.NewNopLogger(), controller.StaticConfiguration{Environment: "test", Cluster: "fixture", Receivers: []v2beta2.Receiver{{ObjectMeta: metav1.ObjectMeta{Name: "original"}, Spec: v2beta2.ReceiverSpec{Webhook: &v2beta2.WebhookReceiver{URL: &endpoint}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = ctl.Run(); err != nil {
		t.Fatal(err)
	}
	db, err := spool.Open(filepath.Join(t.TempDir(), "ledger.db"), spool.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	initial := "active"
	if mode == "standby" {
		initial = mode
	}
	instance, err := instancecontrol.New(db, "blue", initial, strings.Repeat("r", 32), strings.Repeat("c", 32), []byte(strings.Repeat("p", 32)))
	if err != nil {
		t.Fatal(err)
	}
	if mode == "draining" || mode == "retired" {
		command := spool.ReleaseCommand{OperationID: "fixture-release", ExpectedRevision: 1, Action: "begin-active", PeerID: "green", Deadline: time.Now().UTC().Add(time.Minute), MaxSendDelayMillis: 60000, SyncRemainingMillis: 1000, Actor: "fixture", Evidence: "offline compatibility"}
		state, err := db.ControlRelease(command)
		if err != nil {
			t.Fatal(err)
		}
		command.ExpectedRevision, command.Action = state.Revision, "close-admission"
		state, err = db.ControlRelease(command)
		if err != nil {
			t.Fatal(err)
		}
		if mode == "retired" {
			command.ExpectedRevision, command.Action = state.Revision, "freeze"
			state, err = db.ControlRelease(command)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.ExportRelease(command.OperationID, state.Revision, true); err != nil {
				t.Fatal(err)
			}
			state, _ = db.ReleaseStatus()
			command.ExpectedRevision, command.Action = state.Revision, "retire"
			if _, err = db.ControlRelease(command); err != nil {
				t.Fatal(err)
			}
		}
	}
	return New(log.NewNopLogger(), ctl, &store.AlertStore{Durable: db}, &Options{ListenAddress: ":19094", WebhookTimeout: time.Second, WorkerTimeout: 60 * time.Millisecond, InstanceControl: instance}), db
}

func TestInstanceStandbyReceivesAMButNeverReportsSynchronousBufferAsSuccess(t *testing.T) {
	var sends atomic.Int32
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { sends.Add(1); w.WriteHeader(200) }))
	defer mock.Close()
	for _, mode := range []string{"active", "standby", "draining", "retired"} {
		t.Run(mode, func(t *testing.T) {
			h, db := instanceFixture(t, mode, mock.URL)
			before := sends.Load()
			for _, api := range []string{"verify", "notifications"} {
				body, _ := json.Marshal(map[string]interface{}{"receiver": map[string]interface{}{"metadata": map[string]interface{}{"name": "inline", "labels": map[string]string{"type": "global"}}, "spec": map[string]interface{}{"webhook": map[string]string{"url": mock.URL}}}, "alert": map[string]interface{}{"alerts": []interface{}{map[string]interface{}{"labels": map[string]string{"alertname": "fixture"}}}}})
				w := httptest.NewRecorder()
				h.router.ServeHTTP(w, httptest.NewRequest("POST", "/api/v2/"+api, strings.NewReader(string(body))))
				want := 503
				if mode == "active" || mode == "draining" {
					want = 200
				}
				if w.Code != want || strings.Contains(w.Body.String(), "accepted for durable") {
					t.Fatal(api, w.Code, w.Body.String())
				}
				if want == 503 && w.Header().Get("Retry-After") != "1" {
					t.Fatal("gate did not give bounded explicit retry response")
				}
			}
			_, rows, _ := db.Snapshot()
			if len(rows) != 0 {
				t.Fatal("synchronous API secretly persisted an unsent job", rows)
			}
			if (mode == "active" || mode == "draining") && sends.Load()-before != 2 || (mode != "active" && mode != "draining") && sends.Load() != before {
				t.Fatal("instance send fence was bypassed", mode, sends.Load()-before)
			}
			w := httptest.NewRecorder()
			h.router.ServeHTTP(w, httptest.NewRequest("POST", "/api/v2/alerts", strings.NewReader(`{"receiver":"fixture","groupKey":"fixture","alerts":[{"status":"firing","labels":{"alertname":"fixture","severity":"critical"},"startsAt":"2026-10-15T08:00:00Z"}]}`)))
			want := 503
			if mode == "active" || mode == "standby" {
				want = 200
			}
			if w.Code != want {
				t.Fatal("AM admission gate", mode, w.Code, w.Body.String())
			}
			ready := httptest.NewRecorder()
			h.router.ServeHTTP(ready, httptest.NewRequest("GET", "/-/ready", nil))
			if ready.Code != want {
				t.Fatal("receive-only standby readiness", mode, ready.Code, ready.Body.String())
			}
			if mode == "standby" {
				_, rows, _ = db.Snapshot()
				if len(rows) != 1 {
					t.Fatal("standby failed durable admission", rows)
				}
				if _, err := db.Claim(time.Now()); err == nil {
					t.Fatal("standby claimed a platform send")
				}
			}
		})
	}
}

func TestInstanceAPIsRetainActualFailureAndTimeoutWithoutProfiles(t *testing.T) {
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			time.Sleep(200 * time.Millisecond)
		}
		w.WriteHeader(503)
	}))
	defer mock.Close()
	h, db := instanceFixture(t, "active", mock.URL)
	for _, api := range []string{"verify", "notifications"} {
		for _, path := range []string{"/failure", "/slow"} {
			body := `{"receiver":{"metadata":{"name":"inline","labels":{"type":"global"}},"spec":{"webhook":{"url":"` + mock.URL + path + `"}}},"alert":{"alerts":[{"labels":{"alertname":"fixture"}}]}}`
			started := time.Now()
			w := httptest.NewRecorder()
			h.router.ServeHTTP(w, httptest.NewRequest("POST", "/api/v2/"+api, strings.NewReader(body)))
			if w.Code != 400 || time.Since(started) > 180*time.Millisecond {
				t.Fatal("platform result or original timeout changed", api, path, w.Code, time.Since(started))
			}
		}
	}
	_, rows, _ := db.Snapshot()
	if len(rows) != 0 {
		t.Fatal("platform failures were queued after returning a synchronous result")
	}
}

func TestInstanceEndpointsAreIndependentOfProfileAndProbeCredentials(t *testing.T) {
	h, _ := instanceFixture(t, "standby", "http://mock.invalid")
	for _, item := range []struct {
		method, path, token string
		want                int
	}{
		{"GET", "/internal/instance", strings.Repeat("r", 32), 200},
		{"GET", "/internal/instance", strings.Repeat("c", 32), 401},
		{"GET", "/internal/instance/export?operation_id=x&checksum=x", strings.Repeat("r", 32), 401},
		{"POST", "/internal/instance/control", strings.Repeat("r", 32), 401},
		{"POST", "/internal/instance/export", strings.Repeat("r", 32), 401},
		{"POST", "/internal/instance/import", strings.Repeat("r", 32), 401},
		{"GET", "/internal/instance/synchronous", strings.Repeat("r", 32), 401},
		{"POST", "/internal/instance/synchronous/resolve", strings.Repeat("r", 32), 401},
		{"DELETE", "/internal/instance", strings.Repeat("c", 32), 405},
	} {
		r := httptest.NewRequest(item.method, item.path, strings.NewReader(`{}`))
		r.Header.Set("Authorization", "Bearer "+item.token)
		w := httptest.NewRecorder()
		h.router.ServeHTTP(w, r)
		if w.Code != item.want {
			t.Fatal(item.method, item.path, w.Code, w.Body.String())
		}
	}
}

func TestSynchronousUncertainPlatformResultIsIsolatedAndAuditedWithoutReplay(t *testing.T) {
	var sends atomic.Int32
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sends.Add(1)
		w.WriteHeader(503)
	}))
	defer mock.Close()
	h, db := instanceFixture(t, "active", mock.URL)
	body := `{"receiver":{"metadata":{"name":"inline","labels":{"type":"global"}},"spec":{"webhook":{"url":"` + mock.URL + `"}}},"alert":{"alerts":[{"labels":{"alertname":"fixture"}}]}}`
	w := httptest.NewRecorder()
	h.router.ServeHTTP(w, httptest.NewRequest("POST", "/api/v2/notifications", strings.NewReader(body)))
	if w.Code != 400 || sends.Load() != 1 {
		t.Fatal("synchronous platform failure contract changed", w.Code, sends.Load())
	}
	state, _ := db.ReleaseStatus()
	if state.SynchronousUnresolved != 1 {
		t.Fatal("uncertain synchronous platform result disappeared", state)
	}
	command := spool.ReleaseCommand{OperationID: "fixture-release", ExpectedRevision: state.Revision, Action: "begin-active", PeerID: "green", Deadline: time.Now().UTC().Add(time.Minute), MaxSendDelayMillis: 60000, SyncRemainingMillis: 1000, Actor: "fixture", Evidence: "offline unknown check"}
	raw, _ := json.Marshal(command)
	r := httptest.NewRequest("POST", "/internal/instance/control", strings.NewReader(string(raw)))
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("c", 32))
	w = httptest.NewRecorder()
	h.router.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal("historical unknown blocked unrelated release", w.Code, w.Body.String())
	}
	r = httptest.NewRequest("GET", "/internal/instance/synchronous", nil)
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("c", 32))
	w = httptest.NewRecorder()
	h.router.ServeHTTP(w, r)
	var response struct {
		Attempts []spool.SynchronousAttempt `json:"attempts"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || len(response.Attempts) != 1 {
		t.Fatal(err, w.Body.String())
	}
	resolve, _ := json.Marshal(map[string]interface{}{"attempt_id": response.Attempts[0].ID, "actor": "fixture-reviewer", "evidence": "mock-platform-log:confirmed-not-accepted", "delivered": false})
	r = httptest.NewRequest("POST", "/internal/instance/synchronous/resolve", strings.NewReader(string(resolve)))
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("c", 32))
	w = httptest.NewRecorder()
	h.router.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal("audited reconciliation failed", w.Code, w.Body.String())
	}
	state, _ = db.ReleaseStatus()
	_, queued, _ := db.Snapshot()
	if state.SynchronousUnresolved != 0 || sends.Load() != 1 || len(queued) != 0 {
		t.Fatal("reconciliation secretly retried a synchronous notification", state, sends.Load(), queued)
	}
}
