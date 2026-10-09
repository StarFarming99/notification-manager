package notify

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/controller"
	"github.com/kubesphere/notification-manager/pkg/internal"
	feishutype "github.com/kubesphere/notification-manager/pkg/internal/feishu"
	webhooktype "github.com/kubesphere/notification-manager/pkg/internal/webhook"
	"github.com/kubesphere/notification-manager/pkg/jevshadow"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/template"
	"github.com/kubesphere/notification-manager/pkg/utils"
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

func TestProfileWrongApplicationFailsBeforeFreezeAndSend(t *testing.T) {
	file := filepath.Join(t.TempDir(), "public-scopes.json")
	raw := `{"version":1,"profiles":[{"profile_id":"test","profile_version":"v1","card_owner_id":"test-owner","execution_domain":"test-domain","sender_app":"expected-app","receivers":["critical"],"destinations":["test-chat"],"receipt_token_env":"TEST_RECEIPT","annotation_token_env":"TEST_ANNOTATION","feedback_token_env":"TEST_FEEDBACK"},{"profile_id":"formal","profile_version":"v1","card_owner_id":"formal-owner","execution_domain":"formal-domain","sender_app":"expected-app","receivers":["critical"],"destinations":["production-chat"],"receipt_token_env":"FORMAL_RECEIPT","annotation_token_env":"FORMAL_ANNOTATION","feedback_token_env":"FORMAL_FEEDBACK"}]}`
	if err := os.WriteFile(file, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{"JEV_SHADOW_ENABLED": "true", "JEV_DELIVERY_SCOPES_FILE": file, "JEV_SHADOW_SENDER_APP": "expected-app", "JEV_SHADOW_RECEIVER_ALLOWLIST": "critical", "JEV_SHADOW_DESTINATION_ALLOWLIST": "test-chat", "JEV_EXECUTOR_SUCCESS_URL": "http://unused.invalid/successful-deliveries", "JEV_EXECUTOR_TOKEN": strings.Repeat("x", 32)} {
		t.Setenv(key, value)
	}
	shadow, err := jevshadow.NewNMFromEnv(log.NewNopLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	ctl := testController(t, `{{define "frozen"}}{"elements":[]}{{end}}`)
	ctl.SetJevShadow(shadow)
	ref := &v2beta2.Credential{ValueFrom: &v2beta2.ValueSource{SecretKeyRef: &v2beta2.SecretKeySelector{Name: "never-read", Key: "secret"}}}
	receiver := &feishutype.Receiver{Common: &internal.Common{Name: "critical", Type: "feishu", Template: internal.Template{TmplName: "frozen", TmplType: "interactive"}}, ChatIDs: []string{"test-chat"}, Config: &feishutype.Config{AppID: &v2beta2.Credential{Value: "different-app"}, AppSecret: ref}}
	data := &template.Data{ProfileID: "test", ProfileVersion: "v1", CardOwnerID: "test-owner", Alerts: template.Alerts{{ID: "fixture", Labels: template.KV{"alertname": "fixture"}}}}
	if _, err := Freeze(log.NewNopLogger(), ctl, map[internal.Receiver][]*template.Data{receiver: {data}}); err == nil {
		t.Fatal("wrong app reached durable admission")
	}
	receiver.AppID.Value = "expected-app"
	plan, err := Freeze(log.NewNopLogger(), ctl, map[internal.Receiver][]*template.Data{receiver: {data}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err = BindFrozenProfile(plan, spool.ProfileSnapshot{DeliveryProfile: spool.DeliveryProfile{ID: "test", Version: "v1", CardOwnerID: "test-owner", Receiver: "critical", SourceChatID: "production-chat", TestChatID: "test-chat"}})
	if err != nil {
		t.Fatal(err)
	}
	var frozen FrozenNotification
	_ = json.Unmarshal(plan[0].Payload, &frozen)
	var mutated feishutype.Receiver
	_ = json.Unmarshal(frozen.Receiver, &mutated)
	mutated.AppID.Value = "different-app"
	frozen.Receiver, _ = json.Marshal(mutated)
	plan[0].Payload, _ = json.Marshal(frozen)
	if err := SendFrozen(context.Background(), log.NewNopLogger(), ctl, plan[0]); err == nil {
		t.Fatal("wrong app sent")
	} else if state, _ := utils.DeliveryOutcome(err); state != spool.Retryable {
		t.Fatal("known pre-send rejection became unknown", err)
	}
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
