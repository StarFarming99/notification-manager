package dispatcher

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/controller"
	"github.com/kubesphere/notification-manager/pkg/deliveryprofiles"
	"github.com/kubesphere/notification-manager/pkg/internal"
	feishutype "github.com/kubesphere/notification-manager/pkg/internal/feishu"
	webhooktype "github.com/kubesphere/notification-manager/pkg/internal/webhook"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/notify"
	"github.com/kubesphere/notification-manager/pkg/store"
	"github.com/kubesphere/notification-manager/pkg/template"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestFormalSendDoesNotWaitBlockedTestWorkerOrTwelveSecondClock(t *testing.T) {
	t.Setenv("NM_FROZEN_CREDENTIAL_ACTIVE_KEY", "fixture-key")
	t.Setenv("NM_FROZEN_CREDENTIAL_KEYS", `{"fixture-key":"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="}`)
	testStarted, releaseTest := make(chan struct{}), make(chan struct{})
	original := http.DefaultTransport
	http.DefaultTransport = fixtureTransport(func(r *http.Request) (*http.Response, error) {
		body := `{"code":0,"tenant_access_token":"synthetic-isolation-token","expire":3600}`
		if !strings.Contains(r.URL.Path, "tenant_access_token") {
			select {
			case testStarted <- struct{}{}:
			default:
			}
			select {
			case <-releaseTest:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
			body = `{"code":0,"data":{"message_id":"synthetic-test-mid"}}`
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	defer func() { http.DefaultTransport = original }()
	formalSent := make(chan time.Time, 4)
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { formalSent <- time.Now(); w.WriteHeader(200) }))
	defer mock.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u := mock.URL
	ctl, err := controller.NewStatic(ctx, log.NewNopLogger(), controller.StaticConfiguration{Environment: "test", Cluster: "fixture", Template: `{{define "fixture"}}original{{end}}`, Receivers: []v2beta2.Receiver{{ObjectMeta: metav1.ObjectMeta{Name: "original"}, Spec: v2beta2.ReceiverSpec{Webhook: &v2beta2.WebhookReceiver{URL: &u}}}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("NM_NOTIFICATION_SPOOL_PATH", filepath.Join(t.TempDir(), "spool.db"))
	t.Setenv("NM_SHUTDOWN_MODE", "handoff")
	alerts, err := store.NewCheckedAlertStore("notification_durable")
	if err != nil {
		t.Fatal(err)
	}
	db := alerts.Durable
	defer db.Close()
	m, err := deliveryprofiles.New(db, deliveryprofiles.File{Version: 1, RetryDedupeWindow: "2m", RepeatInterval: "12h", Profiles: []spool.DeliveryProfile{{ID: "test", Version: "v1", CardOwnerID: "test-owner", Receiver: "critical", SourceChatID: deliveryprofiles.ProductionChat, TestChatID: deliveryprofiles.TestChat}, {ID: "formal", Version: "v1", CardOwnerID: "formal-owner"}}}, strings.Repeat("t", 32), strings.Repeat("f", 32), strings.Repeat("c", 32))
	if err != nil {
		t.Fatal(err)
	}
	m.SendInterval = 12 * time.Second
	m.FormalWorkers = 1
	m.TestWorkers = 1
	ctl.DeliveryProfiles = m
	for _, id := range []string{"test", "formal"} {
		if _, err = db.ControlProfile(id, "prepare", "v1", 1, "fixture", "offline"); err != nil {
			t.Fatal(err)
		}
		if _, err = db.ControlProfile(id, "activate", "v1", 2, "fixture", "offline"); err != nil {
			t.Fatal(err)
		}
	}
	snap, _ := db.ProfileSnapshot("test")
	receiver := &feishutype.Receiver{Common: &internal.Common{Name: "critical", Type: "feishu", Template: internal.Template{TmplName: "fixture", TmplType: "text"}}, ChatIDs: []string{deliveryprofiles.TestChat}, Config: &feishutype.Config{AppID: &v2beta2.Credential{Value: "synthetic-isolation-app"}, AppSecret: &v2beta2.Credential{Value: "synthetic-secret"}}}
	plan, err := notify.Freeze(log.NewNopLogger(), ctl, map[internal.Receiver][]*template.Data{receiver: {{Alerts: template.Alerts{{ID: "fixture", Labels: template.KV{"alertname": "fixture", "severity": "critical"}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err = notify.BindFrozenProfile(plan, snap)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 256; i++ {
		key := fmt.Sprintf("backlog-%03d", i)
		if _, _, err = db.SubmitProfile(key, key, plan, "", snap, 0); err != nil {
			t.Fatal(err)
		}
	}
	d := New(log.NewNopLogger(), ctl, alerts, time.Second, 3*time.Second, 1)
	exited := make(chan error, 1)
	go func() { exited <- d.Run() }()
	select {
	case <-testStarted:
	case <-time.After(time.Second):
		close(releaseTest)
		_ = alerts.Close()
		<-exited
		t.Fatal("test worker never entered mocked platform")
	}
	formal, _ := db.ProfileSnapshot("formal")
	fReceiver := &webhooktype.Receiver{Common: &internal.Common{Name: "original", Type: "webhook", Template: internal.Template{TmplName: "fixture"}}, URL: mock.URL}
	fPlan, err := notify.Freeze(log.NewNopLogger(), ctl, map[internal.Receiver][]*template.Data{fReceiver: {{Alerts: template.Alerts{{ID: "formal", Labels: template.KV{"alertname": "formal"}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	fPlan, err = notify.BindFrozenProfile(fPlan, formal)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("formal-%d", i)
		if _, _, err = db.SubmitProfile(key, key, fPlan, "", formal, 0); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case sent := <-formalSent:
			t.Logf("formal send %d delay=%s while test worker is blocked", i+1, sent.Sub(started))
		case <-time.After(time.Second):
			close(releaseTest)
			_ = alerts.Close()
			<-exited
			t.Fatal("formal waited for test backlog or 12 second clock")
		}
	}
	close(releaseTest)
	if err = alerts.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-exited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dispatcher did not exit after bounded stop")
	}
}
