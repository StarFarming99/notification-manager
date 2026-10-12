package notify

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/constants"
	"github.com/kubesphere/notification-manager/pkg/internal"
	dingtalktype "github.com/kubesphere/notification-manager/pkg/internal/dingtalk"
	discordtype "github.com/kubesphere/notification-manager/pkg/internal/discord"
	emailtype "github.com/kubesphere/notification-manager/pkg/internal/email"
	feishutype "github.com/kubesphere/notification-manager/pkg/internal/feishu"
	pushovertype "github.com/kubesphere/notification-manager/pkg/internal/pushover"
	slacktype "github.com/kubesphere/notification-manager/pkg/internal/slack"
	smstype "github.com/kubesphere/notification-manager/pkg/internal/sms"
	telegramtype "github.com/kubesphere/notification-manager/pkg/internal/telegram"
	webhooktype "github.com/kubesphere/notification-manager/pkg/internal/webhook"
	wechattype "github.com/kubesphere/notification-manager/pkg/internal/wechat"
	"github.com/kubesphere/notification-manager/pkg/template"
)

func TestAllOriginalChannelsFreezePerDestinationWithOriginalTemplates(t *testing.T) {
	t.Setenv("NM_FROZEN_CREDENTIAL_ACTIVE_KEY", "synthetic")
	t.Setenv("NM_FROZEN_CREDENTIAL_KEYS", `{"synthetic":"`+base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", 32)))+`"}`)
	ctl := testController(t, `{{define "body"}}original:{{(index .Alerts 0).Labels.alertname}}{{end}}{{define "title"}}original-title{{end}}{{define "card"}}{"elements":[]}{{end}}`)
	common := func(channel string) *internal.Common {
		return &internal.Common{Name: "legacy-" + channel, Type: channel, Template: internal.Template{TmplName: "body", TitleTmplName: "title", TmplType: constants.Text}}
	}
	ref := &v2beta2.Credential{ValueFrom: &v2beta2.ValueSource{SecretKeyRef: &v2beta2.SecretKeySelector{Name: "fake", Key: "credential"}}}
	user1, user2 := "synthetic-user-one", "synthetic-user-two"
	cases := []struct {
		receiver internal.Receiver
		count    int
	}{
		{&emailtype.Receiver{Common: common(constants.Email), To: []string{"a@example.test", "b@example.test"}, Config: &emailtype.Config{From: "sender@example.test", SmartHost: v2beta2.HostPort{Host: "mock.invalid", Port: 25}, AuthPassword: ref}}, 2},
		{&smstype.Receiver{Common: common(constants.SMS), PhoneNumbers: []string{"+1234567890", "+1234567891"}, Config: &smstype.Config{DefaultProvider: "aws", Providers: &v2beta2.Providers{AWS: &v2beta2.AWSSMS{AccessKeyId: ref, SecretAccessKey: ref, Region: "us-west-2"}}}}, 2},
		{&slacktype.Receiver{Common: common(constants.Slack), Channels: []string{"ch-a", "ch-b"}, Config: &slacktype.Config{Token: ref}}, 2},
		{&telegramtype.Receiver{Common: common(constants.Telegram), Channels: []string{"tg-a", "tg-b"}, Config: &telegramtype.Config{Token: ref}}, 2},
		{&discordtype.Receiver{Common: common(constants.Discord), Webhook: ref}, 1},
		{&dingtalktype.Receiver{Common: common(constants.DingTalk), ChatIDs: []string{"ding-a", "ding-b"}, ChatBot: &dingtalktype.ChatBot{Webhook: ref}, Config: &dingtalktype.Config{AppKey: ref, AppSecret: ref}}, 3},
		{&wechattype.Receiver{Common: common(constants.WeChat), ToUser: []string{"wx-user"}, ToParty: []string{"wx-party"}, ToTag: []string{"wx-tag"}, ChatBot: &wechattype.ChatBot{Webhook: ref}, Config: &wechattype.Config{CorpID: "corp", AgentID: "agent", APISecret: ref}}, 4},
		{&pushovertype.Receiver{Common: common(constants.Pushover), Profiles: []*v2beta2.PushoverUserProfile{{UserKey: &user1}, {UserKey: &user2}}, Config: &pushovertype.Config{Token: ref}}, 2},
		{&feishutype.Receiver{Common: common(constants.Feishu), ChatIDs: []string{"other-group-a", "other-group-b"}, ChatBot: &feishutype.ChatBot{Webhook: ref}, Config: &feishutype.Config{AppID: &v2beta2.Credential{Value: "other-app"}, AppSecret: ref}}, 3},
		{&webhooktype.Receiver{Common: common(constants.Webhook), URL: "http://mock.invalid/original"}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.receiver.GetType(), func(t *testing.T) {
			data := &template.Data{ProfileID: "formal", ProfileVersion: "v1", CardOwnerID: "formal-owner", Alerts: template.Alerts{{ID: "fixture", Labels: template.KV{"alertname": "all-critical-types", "severity": "critical"}}}}
			plan, err := Freeze(log.NewNopLogger(), ctl, map[internal.Receiver][]*template.Data{tc.receiver: {data}})
			if err != nil || len(plan) != tc.count {
				t.Fatal("original channel unavailable", len(plan), err)
			}
			seen := map[string]bool{}
			for _, target := range plan {
				if target.Channel != tc.receiver.GetType() || seen[target.Destination] {
					t.Fatal("target isolation lost", target.Channel, target.Destination)
				}
				seen[target.Destination] = true
				var frozen FrozenNotification
				if err := json.Unmarshal(target.Payload, &frozen); err != nil {
					t.Fatal(err)
				}
				if target.Channel != constants.Feishu && target.Channel != constants.Webhook {
					if frozen.TemplateSnapshot == nil {
						t.Fatal("legacy catalogue not frozen")
					}
					restored, err := template.FromSnapshot(*frozen.TemplateSnapshot)
					if err != nil {
						t.Fatal(err)
					}
					body, err := restored.Text("body", frozen.Data)
					if err != nil || body != "original:all-critical-types" {
						t.Fatal("original template changed", body, err)
					}
				}
			}
		})
	}
}
