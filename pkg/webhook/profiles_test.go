package webhook

import (
	"context"
	"encoding/json"
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
	"github.com/kubesphere/notification-manager/pkg/notify"
	"github.com/kubesphere/notification-manager/pkg/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestProfiledHTTPIntakeScopeEveryAutomaticEntryAndHAIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	secret := &v2beta2.Credential{Value: "synthetic-before-freeze"}
	app := &v2beta2.Credential{Value: "synthetic-app"}
	tmpl := "fixture"
	kind := "interactive"
	webhookURL := "http://mock.invalid/other"
	historyURL := "http://mock.invalid/history"
	config := controller.StaticConfiguration{Environment: "test", Cluster: "original-cluster", Template: `{{define "fixture"}}{"header":{"title":{"tag":"plain_text","content":"original"}},"elements":[{"tag":"action","actions":[{"tag":"button","value":{"action":"acknowledge"}},{"tag":"button","value":{"action":"create_group"}},{"tag":"button","value":{"action":"silence"}}]}]}{{end}}`, Configs: []v2beta2.Config{{ObjectMeta: metav1.ObjectMeta{Name: "infra"}, Spec: v2beta2.ConfigSpec{Feishu: &v2beta2.FeishuConfig{AppID: app, AppSecret: secret}}}}, Receivers: []v2beta2.Receiver{{ObjectMeta: metav1.ObjectMeta{Name: "critical"}, Spec: v2beta2.ReceiverSpec{Feishu: &v2beta2.FeishuReceiver{AlertSelector: &v2beta2.LabelSelector{MatchLabels: map[string]string{"severity": "critical"}}, ChatIDs: []string{deliveryprofiles.ProductionChat, "other-chat"}, User: []string{"other-user"}, Department: []string{"other-department"}, ChatBot: &v2beta2.FeishuChatBot{Webhook: &v2beta2.Credential{Value: "mock-bot"}}, Template: &tmpl, TmplType: &kind}}}, {ObjectMeta: metav1.ObjectMeta{Name: "unrelated"}, Spec: v2beta2.ReceiverSpec{Feishu: &v2beta2.FeishuReceiver{ChatIDs: []string{deliveryprofiles.ProductionChat}, Template: &tmpl, TmplType: &kind}}}, {ObjectMeta: metav1.ObjectMeta{Name: "webhook"}, Spec: v2beta2.ReceiverSpec{Webhook: &v2beta2.WebhookReceiver{URL: &webhookURL}}}}, History: &v2beta2.HistoryReceiver{Webhook: &v2beta2.WebhookReceiver{URL: &historyURL}}}
	ctl, err := controller.NewStatic(ctx, log.NewNopLogger(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := ctl.Run(); err != nil {
		t.Fatal(err)
	}
	// Static construction uses synthetic credentials only. Freeze sees the same
	// production-style reference contract; this unit test performs no sends.
	secret.Value = ""
	secret.ValueFrom = &v2beta2.ValueSource{SecretKeyRef: &v2beta2.SecretKeySelector{Name: "fixture-secret", Key: "app-secret"}}
	db, err := spool.Open(filepath.Join(t.TempDir(), "spool.db"), spool.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	token, formal, control := strings.Repeat("t", 32), strings.Repeat("f", 32), strings.Repeat("c", 32)
	manager, err := deliveryprofiles.New(db, deliveryprofiles.File{Version: 1, InitialTestVersion: "v1", RetryDedupeWindow: "1s", RepeatInterval: "12h", Profiles: []spool.DeliveryProfile{{ID: "test", Version: "v1", CardOwnerID: "test-owner", Receiver: "critical", SourceChatID: deliveryprofiles.ProductionChat, TestChatID: deliveryprofiles.TestChat}, {ID: "formal", Version: "v1", CardOwnerID: "formal-owner"}}}, token, formal, control)
	if err != nil {
		t.Fatal(err)
	}
	ctl.DeliveryProfiles = manager
	if _, err := db.ControlProfile("test", "prepare", "v1", 1, "fixture", "verified fixture"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ControlProfile("test", "activate", "v1", 2, "fixture", "verified fixture"); err != nil {
		t.Fatal(err)
	}
	h := New(log.NewNopLogger(), ctl, &store.AlertStore{Durable: db}, &Options{WorkerTimeout: time.Second})
	call := func(path, key, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+key)
		w := httptest.NewRecorder()
		h.router.ServeHTTP(w, r)
		return w
	}
	body := `{"profile_id":"formal","receiver":"jev-nm-test","groupKey":"critical-group","externalURL":"http://am-peer-a","alerts":[{"status":"firing","labels":{"alertname":"fixture","severity":"critical","cluster":"real-us","namespace":"real-ns"},"annotations":{"description":"original-context"},"startsAt":"2026-10-09T08:00:00Z","endsAt":"2040-01-01T00:00:00Z"}]}`
	first := call("/api/v2/test/alerts", token, body)
	if first.Code != 200 {
		t.Fatal(first.Code, first.Body.String())
	}
	peer := strings.ReplaceAll(strings.ReplaceAll(body, "am-peer-a", "am-peer-b"), "2040-01-01", "2040-01-02")
	second := call("/api/v2/test/alerts", token, peer)
	if second.Code != 200 || first.Header().Get("X-NM-Intake-ID") != second.Header().Get("X-NM-Intake-ID") {
		t.Fatal("HA/ACK-loss retry created another intake", second.Code, second.Body.String())
	}
	for _, tc := range []struct {
		path, credential string
		status           int
	}{{"/api/v2/test/verify", token, 403}, {"/api/v2/test/notifications", token, 403}, {"/api/v2/alerts", token, 401}, {"/api/v2/alerts", formal, 503}, {"/api/v2/notifications", formal, 503}, {"/api/v2/verify", formal, 503}, {"/api/v2/test/alerts", formal, 401}} {
		response := call(tc.path, tc.credential, body)
		if response.Code != tc.status {
			t.Fatal(tc, response.Code, response.Body.String())
		}
	}
	intakes, targets, err := db.Snapshot()
	if err != nil || len(intakes) != 1 || len(targets) != 1 {
		t.Fatal("scope duplicated receivers/channels/history", len(intakes), len(targets), err)
	}
	target := targets[0]
	if target.Destination != "chat:"+deliveryprofiles.TestChat || target.Receiver != "critical" || target.ProfileID != "test" {
		t.Fatal("wrong frozen target", target)
	}
	var frozen notify.FrozenNotification
	_ = json.Unmarshal(target.Payload, &frozen)
	if len(frozen.Data.Alerts) != 1 || frozen.Data.Alerts[0].Labels["cluster"] != "real-us" || frozen.Data.Alerts[0].Annotations["description"] != "original-context" {
		t.Fatal("profile altered source context", frozen.Data)
	}
	for _, action := range []string{"acknowledge", "create_group", "silence"} {
		if !strings.Contains(frozen.Content, action) {
			t.Fatal("original action lost", action)
		}
	}
	for _, receiver := range config.Receivers {
		if receiver.Name == "critical" && receiver.Spec.Feishu.ChatIDs[0] != deliveryprofiles.ProductionChat {
			t.Fatal("shared original receiver mutated")
		}
	}
	warning := strings.ReplaceAll(body, `"severity":"critical"`, `"severity":"warning"`)
	response := call("/api/v2/test/alerts", token, warning)
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	_, after, err := db.Snapshot()
	if err != nil || len(after) != 1 {
		t.Fatal("warning created test card", after, err)
	}
}
