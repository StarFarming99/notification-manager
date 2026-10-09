package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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
	"github.com/kubesphere/notification-manager/pkg/internal"
	webhooktype "github.com/kubesphere/notification-manager/pkg/internal/webhook"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/notify"
	"github.com/kubesphere/notification-manager/pkg/store"
	"github.com/kubesphere/notification-manager/pkg/template"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDurableDispatcherAcknowledgesTargetsSeparately(t *testing.T) {
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/success":
			w.WriteHeader(200)
		case "/rate":
			w.WriteHeader(429)
		default:
			w.WriteHeader(503)
		}
	}))
	defer mock.Close()
	u := mock.URL + "/success"
	ctl, err := controller.NewStatic(context.Background(), log.NewNopLogger(), controller.StaticConfiguration{Environment: "test", Cluster: "fixture", Template: `{{define "fixture"}}original{{end}}`, Receivers: []v2beta2.Receiver{{ObjectMeta: metav1.ObjectMeta{Name: "fixture"}, Spec: v2beta2.ReceiverSpec{Webhook: &v2beta2.WebhookReceiver{URL: &u}}}}})
	if err != nil {
		t.Fatal(err)
	}
	groups := map[internal.Receiver][]*template.Data{}
	for _, path := range []string{"/success", "/rate", "/uncertain"} {
		r := &webhooktype.Receiver{Common: &internal.Common{Name: path, Type: "webhook", Template: internal.Template{TmplName: "fixture"}}, URL: mock.URL + path}
		groups[r] = []*template.Data{{Alerts: template.Alerts{{ID: "alert-1", Labels: template.KV{"alertname": "fixture"}}}}}
	}
	plan, err := notify.Freeze(log.NewNopLogger(), ctl, groups)
	if err != nil {
		t.Fatal(err)
	}
	s, err := spool.Open(filepath.Join(t.TempDir(), "notifications.db"), spool.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, _, err := s.Submit("fixture", "body", plan, ""); err != nil {
		t.Fatal(err)
	}
	d := New(log.NewNopLogger(), ctl, &store.AlertStore{Durable: s}, time.Second, time.Second, 2)
	for i := 0; i < 3; i++ {
		c, err := s.Claim(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		d.sendClaim(c)
	}
	_, targets, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	states := map[string]string{}
	for _, v := range targets {
		states[v.Receiver] = v.State
	}
	if states["/success"] != spool.Delivered || states["/rate"] != spool.Retryable || states["/uncertain"] != spool.Unknown {
		t.Fatalf("target outcomes %+v", states)
	}
}

type fixtureTransport func(*http.Request) (*http.Response, error)

func (f fixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTokenFailureRetriesSameFrozenTargetAfterRestart(t *testing.T) {
	var recovered atomic.Bool
	var messages atomic.Int32
	original := http.DefaultTransport
	http.DefaultTransport = fixtureTransport(func(r *http.Request) (*http.Response, error) {
		body := `{"code":0,"tenant_access_token":"synthetic-token","expire":3600}`
		if strings.Contains(r.URL.Path, "tenant_access_token") {
			if !recovered.Load() {
				return nil, errors.New("synthetic token outage")
			}
		} else {
			messages.Add(1)
			body = `{"code":0,"data":{"message_id":"fixture-message"}}`
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})
	defer func() { http.DefaultTransport = original }()
	u := "http://unused.invalid"
	ctl, err := controller.NewStatic(context.Background(), log.NewNopLogger(), controller.StaticConfiguration{Environment: "test", Cluster: "fixture", Receivers: []v2beta2.Receiver{{ObjectMeta: metav1.ObjectMeta{Name: "fixture"}, Spec: v2beta2.ReceiverSpec{Webhook: &v2beta2.WebhookReceiver{URL: &u}}}}})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(notify.FrozenNotification{Version: spool.Version, Receiver: json.RawMessage(`{"frozen":true,"name":"fixture-token","type":"feishu","template":{"tmplType":"text"},"chatids":["fixture-chat"],"appID":{"value":"synthetic-app"},"appSecret":{"value":"synthetic-secret"}}`), Data: &template.Data{Alerts: template.Alerts{{ID: "fixture", Labels: template.KV{"alertname": "fixture"}}}}, Content: "frozen-original"})
	target := spool.Target{Receiver: "fixture-token", Channel: "feishu", Destination: "fixture-chat", ContentRevision: spool.Hash(payload), Payload: payload}
	path := filepath.Join(t.TempDir(), "spool.db")
	s, err := spool.Open(path, spool.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.Submit("token-retry", "request", []spool.Target{target}, "")
	if err != nil {
		t.Fatal(err)
	}
	d := New(log.NewNopLogger(), ctl, &store.AlertStore{Durable: s}, time.Second, time.Second, 1)
	claim, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	d.sendClaim(claim)
	_, rows, _ := s.Snapshot()
	if rows[0].State != spool.Retryable || messages.Load() != 0 {
		t.Fatalf("pre-send state=%s messages=%d", rows[0].State, messages.Load())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = spool.Open(path, spool.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	recovered.Store(true)
	d.alerts.Durable = s
	claim, err = s.Claim(time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	d.sendClaim(claim)
	_, rows, _ = s.Snapshot()
	if rows[0].State != spool.Delivered || messages.Load() != 1 {
		t.Fatalf("recovered state=%s messages=%d", rows[0].State, messages.Load())
	}
	if string(rows[0].Payload) != string(payload) {
		t.Fatal("retry changed frozen target")
	}
}
