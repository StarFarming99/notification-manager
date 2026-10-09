package webhook

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
	"github.com/kubesphere/notification-manager/pkg/deliveryprofiles"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestFormalCompatibilitySourceFenceAndLegacyAlertFreezesFormal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u := "http://mock.invalid/original"
	ctl, err := controller.NewStatic(ctx, log.NewNopLogger(), controller.StaticConfiguration{Environment: "test", Cluster: "fixture", Receivers: []v2beta2.Receiver{{ObjectMeta: metav1.ObjectMeta{Name: "original"}, Spec: v2beta2.ReceiverSpec{Webhook: &v2beta2.WebhookReceiver{URL: &u}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ctl.Run(); err != nil {
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
	h := New(log.NewNopLogger(), ctl, &store.AlertStore{Durable: db}, &Options{ListenAddress: ":19094", FormalCompatListenAddress: ":19093", FormalCompatSourceCIDRs: []string{"10.0.0.0/24"}, WebhookTimeout: time.Second, WorkerTimeout: time.Second})
	if h.compatError != nil {
		t.Fatal(h.compatError)
	}
	formalReady := func() int {
		r := httptest.NewRequest("GET", "/-/formal-ready", nil)
		w := httptest.NewRecorder()
		h.router.ServeHTTP(w, r)
		return w.Code
	}
	if code := formalReady(); code != 503 {
		t.Fatal("unprepared formal was Ready", code)
	}
	call := func(path, body, remote, xff string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", path, strings.NewReader(body))
		r.RemoteAddr = remote
		r.Header.Set("X-Forwarded-For", xff)
		w := httptest.NewRecorder()
		h.compatRouter.ServeHTTP(w, r)
		return w
	}
	if w := call("/api/v2/alerts", `{}`, "192.0.2.1:20", "10.0.0.9"); w.Code != 403 {
		t.Fatal("spoofed forwarded source admitted", w.Code)
	}
	if w := call("/api/v2/alerts", `{}`, "10.0.0.9:20", ""); w.Code != 503 {
		t.Fatal("unprepared formal accepted", w.Code)
	}
	if _, err := db.ControlProfile("formal", "prepare", "v1", 1, "fixture", "verified fixture"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ControlProfile("formal", "activate", "v1", 2, "fixture", "verified fixture"); err != nil {
		t.Fatal(err)
	}
	// This isolated fixture has no tenant selector: explicit notification/verify
	// reach the original validator (400 unknown tenant), rather than bypassing
	// formal admission. Full real-CR API send parity is a separate promotion gate.
	if code := formalReady(); code != 200 {
		t.Fatal("prepared active formal was not Ready", code)
	}
	saved := h.FormalCompatListenAddress
	h.FormalCompatListenAddress = ""
	if code := formalReady(); code != 503 {
		t.Fatal("disabled compat claimed formal Ready", code)
	}
	h.FormalCompatListenAddress = saved
	receiver := `{"metadata":{"name":"explicit","labels":{"user":"fixture-tenant"}},"spec":{"webhook":{"url":"http://mock.invalid/explicit"}}}`
	for _, tc := range []struct {
		path, body string
		want       int
	}{{"/api/v2/alerts", `{"receiver":"original","groupKey":"g","alerts":[{"status":"firing","labels":{"alertname":"fixture"},"startsAt":"2026-10-09T08:00:00Z"}]}`, 200}, {"/api/v2/verify", `{"receiver":` + receiver + `}`, 400}, {"/api/v2/notifications", `{"receiver":` + receiver + `,"alert":{"alerts":[{"labels":{"alertname":"fixture"},"annotations":{"message":"original-context"}}]}}`, 400}} {
		if w := call(tc.path, tc.body, "10.0.0.9:20", ""); w.Code != tc.want {
			t.Fatal(tc.path, w.Code, w.Body.String())
		}
	}
	_, targets, err := db.Snapshot()
	if err != nil || len(targets) != 1 {
		t.Fatal(targets, err)
	}
	for _, target := range targets {
		if target.ProfileID != "formal" || target.ProfileVersion != "v1" || target.CardOwnerID != "formal-owner" || !strings.Contains(string(target.Payload), "http://mock.invalid/original") {
			t.Fatal("compatibility folded or lost frozen formal identity", target)
		}
	}
	for _, path := range []string{"/api/v2/test/alerts", "/internal/delivery-profiles/test/pause", "/internal/jev/annotations/message"} {
		if w := call(path, `{}`, "10.0.0.9:20", ""); w.Code != 404 {
			t.Fatal("compatibility exposed privileged lane", path, w.Code)
		}
	}
	if _, err := db.ControlProfile("formal", "pause", "", 3, "fixture", "pause"); err != nil {
		t.Fatal(err)
	}
	if w := call("/api/v2/alerts", `{}`, "10.0.0.9:20", ""); w.Code != 503 {
		t.Fatal("paused compatibility bypassed fence", w.Code)
	}
	if code := formalReady(); code != 503 {
		t.Fatal("paused formal remained Ready", code)
	}
}

func TestFormalCompatibilityRejectsBroadOrAmbiguousSourceConfiguration(t *testing.T) {
	for _, cidrs := range [][]string{nil, {"0.0.0.0/0"}, {"10.0.0.1/24"}, {"invalid"}} {
		h := &Webhook{Options: &Options{ListenAddress: ":19094", FormalCompatListenAddress: ":19093", FormalCompatSourceCIDRs: cidrs}}
		if _, err := h.compatibilityRouter(&deliveryprofiles.Manager{}); err == nil {
			t.Fatal("unsafe source CIDRs admitted", cidrs)
		}
	}
}
