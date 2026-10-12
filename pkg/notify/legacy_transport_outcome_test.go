package notify

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/constants"
	"github.com/kubesphere/notification-manager/pkg/internal"
	dingtalktype "github.com/kubesphere/notification-manager/pkg/internal/dingtalk"
	wechattype "github.com/kubesphere/notification-manager/pkg/internal/wechat"
	"github.com/kubesphere/notification-manager/pkg/template"
	"github.com/kubesphere/notification-manager/pkg/utils"
)

type legacyOutcomeTransport func(*http.Request) (*http.Response, error)

func (f legacyOutcomeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Every notification transport must retain SendFrozen's attempt marker and the
// worker deadline. A lost response after POST cannot be retried automatically.
func TestLegacyFrozenNotificationResponseLossIsUnknown(t *testing.T) {
	t.Setenv("NM_FROZEN_CREDENTIAL_ACTIVE_KEY", "synthetic")
	t.Setenv("NM_FROZEN_CREDENTIAL_KEYS", `{"synthetic":"`+base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", 32)))+`"}`)
	ctl := testController(t, `{{define "body"}}original:{{(index .Alerts 0).Labels.alertname}}{{end}}`)
	originalTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = originalTransport }()
	for _, route := range []string{"dingtalk-chatbot", "dingtalk-conversation", "wechat-chatbot"} {
		for _, failure := range []string{"response-loss", "deadline"} {
			t.Run(route+"/"+failure, func(t *testing.T) {
				common := &internal.Common{Name: "legacy", Type: constants.DingTalk, Template: internal.Template{TmplName: "body", TmplType: constants.Text}}
				literal := func(s string) *v2beta2.Credential { return &v2beta2.Credential{Value: s} }
				var receiver internal.Receiver
				switch route {
				case "dingtalk-chatbot":
					receiver = &dingtalktype.Receiver{Common: common, ChatBot: &dingtalktype.ChatBot{Webhook: literal("https://mock.invalid/ding?credential=synthetic")}}
				case "dingtalk-conversation":
					receiver = &dingtalktype.Receiver{Common: common, ChatIDs: []string{"synthetic-chat"}, Config: &dingtalktype.Config{AppKey: literal("synthetic-app"), AppSecret: literal("synthetic-" + failure)}}
				case "wechat-chatbot":
					common.Type = constants.WeChat
					receiver = &wechattype.Receiver{Common: common, ChatBot: &wechattype.ChatBot{Webhook: literal("https://mock.invalid/wechat?credential=synthetic")}}
				}
				data := &template.Data{Alerts: template.Alerts{{ID: "fixture", Labels: template.KV{"alertname": "fixture"}}}}
				targets, err := Freeze(log.NewNopLogger(), ctl, map[internal.Receiver][]*template.Data{receiver: {data}})
				if err != nil || len(targets) != 1 {
					t.Fatal("freeze", err, len(targets))
				}
				var posts atomic.Int32
				http.DefaultTransport = legacyOutcomeTransport(func(r *http.Request) (*http.Response, error) {
					if _, ok := r.Context().Deadline(); !ok {
						t.Error("transport lost worker deadline")
					}
					if r.Method == http.MethodGet {
						return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"errcode":0,"access_token":"synthetic-token"}`)), Header: make(http.Header)}, nil
					}
					posts.Add(1)
					if failure == "deadline" {
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
					return nil, io.ErrUnexpectedEOF
				})
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				err = SendFrozen(ctx, log.NewNopLogger(), ctl, targets[0])
				state, reason := utils.DeliveryOutcome(err)
				if err == nil || state != "unknown" || reason != "platform_result_uncertain" || posts.Load() != 1 {
					t.Fatal("response loss must not automatically retry", state, reason, posts.Load(), err)
				}
			})
		}
	}
}

func TestLegacyFrozenTokenFailureRemainsRetryableBeforeSend(t *testing.T) {
	t.Setenv("NM_FROZEN_CREDENTIAL_ACTIVE_KEY", "synthetic")
	t.Setenv("NM_FROZEN_CREDENTIAL_KEYS", `{"synthetic":"`+base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", 32)))+`"}`)
	ctl := testController(t, `{{define "body"}}original{{end}}`)
	receiver := &dingtalktype.Receiver{Common: &internal.Common{Name: "legacy", Type: constants.DingTalk, Template: internal.Template{TmplName: "body", TmplType: constants.Text}}, ChatIDs: []string{"synthetic-chat"}, Config: &dingtalktype.Config{AppKey: &v2beta2.Credential{Value: "preflight-only-app"}, AppSecret: &v2beta2.Credential{Value: "preflight-only-secret"}}}
	targets, err := Freeze(log.NewNopLogger(), ctl, map[internal.Receiver][]*template.Data{receiver: {{Alerts: template.Alerts{{ID: "fixture"}}}}})
	if err != nil || len(targets) != 1 {
		t.Fatal("freeze", err)
	}
	old := http.DefaultTransport
	defer func() { http.DefaultTransport = old }()
	http.DefaultTransport = legacyOutcomeTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet {
			t.Error("notification escaped failed token preflight")
		}
		if _, ok := r.Context().Deadline(); !ok {
			t.Error("token acquisition lost worker deadline")
		}
		return nil, errors.New("synthetic token unavailable")
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = SendFrozen(ctx, log.NewNopLogger(), ctl, targets[0])
	state, reason := utils.DeliveryOutcome(err)
	if state != "retryable" || reason != "notification_not_sent" {
		t.Fatal("token failure classification", state, reason, err)
	}
}
