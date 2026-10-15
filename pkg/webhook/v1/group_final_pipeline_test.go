package v1

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/controller"
	"github.com/kubesphere/notification-manager/pkg/deliveryprofiles"
	"github.com/kubesphere/notification-manager/pkg/jevshadow"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/notify"
	"github.com/kubesphere/notification-manager/pkg/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Test the actual authenticated HTTP admission/pipeline, not a manually built
// post-aggregation Data. Group metadata must describe each final target after
// silence, receiver filtering, route deduplication, aggregation and test scope.
func TestGroupFinalHTTPPipelineMembersMatchLegacyWhenModeDisabled(t *testing.T) {
	t.Setenv("NM_FROZEN_CREDENTIAL_ACTIVE_KEY", "fixture-key")
	t.Setenv("NM_FROZEN_CREDENTIAL_KEYS", `{"fixture-key":"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="}`)
	directory := t.TempDir()
	scopes := filepath.Join(directory, "scopes.json")
	if err := os.WriteFile(scopes, []byte(`{"version":1,"profiles":[{"profile_id":"test","profile_version":"v1","card_owner_id":"test-owner","execution_domain":"test-domain","sender_app":"fixture-public-app","receivers":["critical"],"destinations":["`+deliveryprofiles.TestChat+`"],"receipt_token_env":"TEST_RECEIPT","annotation_token_env":"TEST_ANNOTATION","feedback_token_env":"TEST_FEEDBACK"},{"profile_id":"formal","profile_version":"v1","card_owner_id":"formal-owner","execution_domain":"formal-domain","sender_app":"fixture-public-app","receivers":["critical"],"destinations":["`+deliveryprofiles.ProductionChat+`"],"receipt_token_env":"FORMAL_RECEIPT","annotation_token_env":"FORMAL_ANNOTATION","feedback_token_env":"FORMAL_FEEDBACK"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{"JEV_SHADOW_ENABLED": "true", "JEV_DELIVERY_SCOPES_FILE": scopes, "JEV_SHADOW_SENDER_APP": "fixture-public-app", "JEV_SHADOW_RECEIVER_ALLOWLIST": "critical", "JEV_SHADOW_DESTINATION_ALLOWLIST": deliveryprofiles.TestChat, "JEV_EXECUTOR_SUCCESS_URL": "http://unused.invalid/successful-deliveries", "JEV_EXECUTOR_TOKEN": strings.Repeat("x", 32)} {
		t.Setenv(key, value)
	}
	templateName, templateKind := "fixture", "interactive"
	var legacy map[string]interface{}
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "group-disabled", true: "group-enabled"}[enabled], func(t *testing.T) {
			t.Setenv("JEV_GROUP_ENABLED_PROFILES", map[bool]string{false: "", true: "test:v1"}[enabled])
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			shadow, err := jevshadow.NewNMFromEnv(log.NewNopLogger())
			if err != nil {
				t.Fatal(err)
			}
			defer shadow.Close()
			selector := func(severity string) *v2beta2.LabelSelector {
				return &v2beta2.LabelSelector{MatchLabels: map[string]string{"severity": severity}}
			}
			config := controller.StaticConfiguration{Environment: "test", Cluster: "fixture", GroupLabels: []string{"alertname", "cluster"},
				Template: `{{define "fixture"}}{"elements":[{"tag":"div","text":{"tag":"plain_text","content":"{{range .Alerts}}{{.Labels.pod}},{{end}}"}}]}{{end}}`,
				Receivers: []v2beta2.Receiver{
					{ObjectMeta: metav1.ObjectMeta{Name: "critical"}, Spec: v2beta2.ReceiverSpec{Feishu: &v2beta2.FeishuReceiver{AlertSelector: selector("critical"), ChatIDs: []string{deliveryprofiles.ProductionChat, "unrelated-original-chat"}, Template: &templateName, TmplType: &templateKind}}},
					{ObjectMeta: metav1.ObjectMeta{Name: "warning"}, Spec: v2beta2.ReceiverSpec{Feishu: &v2beta2.FeishuReceiver{AlertSelector: selector("warning"), ChatIDs: []string{"unrelated-warning-chat"}, Template: &templateName, TmplType: &templateKind}}},
				},
				Configs:  []v2beta2.Config{{ObjectMeta: metav1.ObjectMeta{Name: "fixture-feishu", Labels: map[string]string{"type": "default"}}, Spec: v2beta2.ConfigSpec{Feishu: &v2beta2.FeishuConfig{AppID: &v2beta2.Credential{Value: "fixture-public-app"}, AppSecret: &v2beta2.Credential{Value: "synthetic-local-only"}}}}},
				Silences: []v2beta2.Silence{{Spec: v2beta2.SilenceSpec{Matcher: &v2beta2.LabelSelector{MatchLabels: map[string]string{"silenced": "yes"}}}}},
				// The matching route also selects critical; route+global overlap
				// must not duplicate this alert or create another final target.
				Routers: []v2beta2.Router{{Spec: v2beta2.RouterSpec{AlertSelector: &v2beta2.LabelSelector{MatchLabels: map[string]string{"pod": "keep-one"}}, Receivers: v2beta2.ReceiverSelector{Name: []string{"critical"}, Type: "feishu"}}}},
			}
			ctl, err := controller.NewStatic(ctx, log.NewNopLogger(), config)
			if err != nil {
				t.Fatal(err)
			}
			ctl.SetJevShadow(shadow)
			if err := ctl.Run(); err != nil {
				t.Fatal(err)
			}
			db, err := spool.Open(filepath.Join(t.TempDir(), "spool.db"), spool.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			manager, err := deliveryprofiles.New(db, deliveryprofiles.File{Version: 1, RetryDedupeWindow: "2m", RepeatInterval: "12h", Profiles: []spool.DeliveryProfile{{ID: "test", Version: "v1", CardOwnerID: "test-owner", Receiver: "critical", SourceChatID: deliveryprofiles.ProductionChat, TestChatID: deliveryprofiles.TestChat}, {ID: "formal", Version: "v1", CardOwnerID: "formal-owner"}}}, strings.Repeat("t", 32), strings.Repeat("f", 32), strings.Repeat("c", 32))
			if err != nil {
				t.Fatal(err)
			}
			ctl.DeliveryProfiles = manager
			if _, err := db.ControlProfile("test", "prepare", "v1", 1, "fixture", "isolated HTTP pipeline"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ControlProfile("test", "activate", "v1", 2, "fixture", "isolated HTTP pipeline"); err != nil {
				t.Fatal(err)
			}
			h := New(log.NewNopLogger(), time.Second, ctl, &store.AlertStore{Durable: db})
			body := `{"receiver":"original-am-route","groupKey":"original-am-group","groupLabels":{"cluster":"fixture"},"externalURL":"http://am.fixture","alerts":[{"status":"firing","labels":{"alertname":"GroupA","severity":"critical","pod":"keep-one"},"startsAt":"2026-10-15T01:00:00Z"},{"status":"firing","labels":{"alertname":"GroupA","severity":"critical","pod":"keep-two"},"startsAt":"2026-10-15T01:00:00Z"},{"status":"firing","labels":{"alertname":"GroupB","severity":"critical","pod":"separate-group"},"startsAt":"2026-10-15T01:00:00Z"},{"status":"firing","labels":{"alertname":"GroupA","severity":"critical","pod":"must-not-appear-silenced","silenced":"yes"},"startsAt":"2026-10-15T01:00:00Z"},{"status":"firing","labels":{"alertname":"GroupA","severity":"warning","pod":"must-not-appear-filtered"},"startsAt":"2026-10-15T01:00:00Z"}]}`
			request := httptest.NewRequest("POST", "/api/v2/test/alerts", strings.NewReader(body))
			request.Header.Set("Authorization", "Bearer "+strings.Repeat("t", 32))
			response := httptest.NewRecorder()
			manager.Guard("test", h.TestAlert)(response, request)
			if response.Code != 200 {
				t.Fatal(response.Code, response.Body.String())
			}
			_, targets, err := db.Snapshot()
			if err != nil || len(targets) != 2 {
				t.Fatal("wrong actual frozen target count", len(targets), err)
			}
			actual := map[string]interface{}{}
			for _, target := range targets {
				var frozen notify.FrozenNotification
				if err := json.Unmarshal(target.Payload, &frozen); err != nil {
					t.Fatal(err)
				}
				if target.Receiver != "critical" || target.Destination != "chat:"+deliveryprofiles.TestChat {
					t.Fatal("unrelated destination leaked into test target", target.Receiver, target.Destination)
				}
				pods, identities := []string{}, []string{}
				for _, alert := range frozen.Data.Alerts {
					pods = append(pods, alert.Labels["pod"])
					identities = append(identities, alert.ID)
				}
				sort.Strings(pods)
				sort.Strings(identities)
				group := frozen.Data.GroupLabels["alertname"]
				expected := map[string][]string{"GroupA": {"keep-one", "keep-two"}, "GroupB": {"separate-group"}}[group]
				if !reflect.DeepEqual(pods, expected) {
					t.Fatal("raw/filtered/other group member entered final target", group, pods)
				}
				if enabled {
					refs := frozen.Data.AMInputRefs
					if len(refs) != 1 || refs[0].GroupKey != "original-am-group" || len(refs[0].EntryIndexes) != len(expected) {
						t.Fatal("metadata does not map exact final members", refs)
					}
				} else if len(frozen.Data.AMInputRefs) != 0 {
					t.Fatal("disabled profile acquired group metadata")
				}
				var content interface{}
				if err := json.Unmarshal([]byte(frozen.Content), &content); err != nil {
					t.Fatal(err)
				}
				actual[group] = map[string]interface{}{"pods": pods, "alert_ids": identities, "content": content, "destination": target.Destination}
			}
			if !enabled {
				legacy = actual
			} else if !reflect.DeepEqual(actual, legacy) {
				t.Fatal("group mode changed original notification output", actual, legacy)
			}
			t.Logf("group_mode=%v actual_target_members=%v", enabled, actual)
		})
	}
}
