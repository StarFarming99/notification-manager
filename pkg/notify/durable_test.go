package notify

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/controller"
	"github.com/kubesphere/notification-manager/pkg/internal"
	feishutype "github.com/kubesphere/notification-manager/pkg/internal/feishu"
	webhooktype "github.com/kubesphere/notification-manager/pkg/internal/webhook"
	"github.com/kubesphere/notification-manager/pkg/template"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func testController(t *testing.T, render string) *controller.Controller {
	t.Helper()
	url := "http://mock.invalid/notifications"
	c, err := controller.NewStatic(context.Background(), log.NewNopLogger(), controller.StaticConfiguration{Environment: "test", Cluster: "fixture", Template: render, Receivers: []v2beta2.Receiver{{ObjectMeta: metav1.ObjectMeta{Name: "fixture"}, Spec: v2beta2.ReceiverSpec{Webhook: &v2beta2.WebhookReceiver{URL: &url}}}}})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestFrozenRenderDestinationAndRecoveryDoNotUseNewConfiguration(t *testing.T) {
	received := make(chan string, 1)
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		received <- string(raw)
		w.WriteHeader(200)
	}))
	defer mock.Close()
	ctl := testController(t, `{{define "frozen"}}original:{{(index .Alerts 0).Labels.alertname}}{{end}}`)
	receiver := &webhooktype.Receiver{Common: &internal.Common{Name: "webhook-test", Type: "webhook", Template: internal.Template{TmplName: "frozen"}}, URL: mock.URL}
	data := &template.Data{Alerts: template.Alerts{{ID: "alert-1", Labels: template.KV{"alertname": "firing"}}}}
	plan, err := Freeze(log.NewNopLogger(), ctl, map[internal.Receiver][]*template.Data{receiver: {data}})
	if err != nil || len(plan) != 1 {
		t.Fatal("freeze", err)
	}
	receiver.URL = "http://must-not-contact.invalid/"
	data.Alerts[0].Labels["alertname"] = "mutated"
	ctl = testController(t, `{{define "frozen"}}changed-template{{end}}`)
	if err := SendFrozen(context.Background(), log.NewNopLogger(), ctl, plan[0]); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if got != "original:firing" {
			t.Fatalf("render changed: %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("frozen target not called")
	}
}

func TestFeishuFreezeSplitsEveryOriginalPathAndRejectsPlaintext(t *testing.T) {
	ctl := testController(t, `{{define "frozen"}}{"elements":[],"header":{"title":{"tag":"plain_text","content":"original"}}}{{end}}`)
	ref := &v2beta2.Credential{ValueFrom: &v2beta2.ValueSource{SecretKeyRef: &v2beta2.SecretKeySelector{Name: "test-reference-only", Namespace: "fixture", Key: "secret"}}}
	receiver := &feishutype.Receiver{Common: &internal.Common{Name: "critical", Type: "feishu", Template: internal.Template{TmplName: "frozen", TmplType: "interactive"}}, ChatIDs: []string{"test-chat-1", "test-chat-2"}, User: []string{"fixture-user"}, Department: []string{"fixture-department"}, ChatBot: &feishutype.ChatBot{Webhook: ref}, Config: &feishutype.Config{AppID: &v2beta2.Credential{Value: "fixture-public-app-id"}, AppSecret: ref}}
	data := &template.Data{Alerts: template.Alerts{{ID: "alert-1", Labels: template.KV{"alertname": "fixture"}}}}
	plan, err := Freeze(log.NewNopLogger(), ctl, map[internal.Receiver][]*template.Data{receiver: {data}})
	if err != nil || len(plan) != 5 {
		t.Fatal("original paths missing", len(plan), err)
	}
	for _, target := range plan {
		var frozen FrozenNotification
		var r feishutype.Receiver
		if err := json.Unmarshal(target.Payload, &frozen); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(frozen.Receiver, &r); err != nil {
			t.Fatal(err)
		}
		paths := len(r.ChatIDs) + len(r.User) + len(r.Department)
		if r.ChatBot != nil {
			paths++
		}
		if paths != 1 || !r.Frozen || r.Common.Name != "critical" || r.Config == nil || r.AppSecret.ValueFrom == nil {
			t.Fatal("target not isolated/frozen", string(frozen.Receiver))
		}
	}
	receiver.AppSecret = &v2beta2.Credential{Value: "synthetic-plaintext-must-not-persist"}
	if _, err := Freeze(log.NewNopLogger(), ctl, map[internal.Receiver][]*template.Data{receiver: {data}}); err == nil {
		t.Fatal("plaintext secret accepted into spool")
	}
	if strings.Contains(string(plan[0].Payload), "synthetic-plaintext") {
		t.Fatal("immutable plan mutated")
	}
}
