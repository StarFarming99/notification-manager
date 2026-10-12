package v1

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/controller"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestFormalPreparesOriginalEmailSMSChatbotWithoutJevBindings(t *testing.T) {
	t.Setenv("NM_FROZEN_CREDENTIAL_ACTIVE_KEY", "synthetic")
	t.Setenv("NM_FROZEN_CREDENTIAL_KEYS", `{"synthetic":"`+base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", 32)))+`"}`)
	enabled := true
	body := "body"
	title := "title"
	text := "text"
	credential := func(s string) *v2beta2.Credential { return &v2beta2.Credential{Value: s} }
	selector := &v2beta2.LabelSelector{MatchLabels: map[string]string{"type": "default"}}
	cfg := controller.StaticConfiguration{Environment: "test", Cluster: "fixture", Template: `{{define "body"}}original:{{(index .Alerts 0).Labels.alertname}}{{end}}{{define "title"}}original-title{{end}}`, Configs: []v2beta2.Config{{ObjectMeta: metav1.ObjectMeta{Name: "original-default", Labels: map[string]string{"type": "default"}}, Spec: v2beta2.ConfigSpec{Email: &v2beta2.EmailConfig{From: "sender@example.test", SmartHost: v2beta2.HostPort{Host: "mock.invalid", Port: 25}, AuthPassword: credential("synthetic-smtp-secret")}, Sms: &v2beta2.SmsConfig{DefaultProvider: "aws", Providers: &v2beta2.Providers{AWS: &v2beta2.AWSSMS{AccessKeyId: credential("synthetic-id"), SecretAccessKey: credential("synthetic-aws-secret"), Region: "us-west-2"}}}}}}, Receivers: []v2beta2.Receiver{{ObjectMeta: metav1.ObjectMeta{Name: "original-global", Labels: map[string]string{"type": "global"}}, Spec: v2beta2.ReceiverSpec{Email: &v2beta2.EmailReceiver{Enabled: &enabled, EmailConfigSelector: selector, Template: &body, SubjectTemplate: &title, TmplType: &text, To: []string{"oncall@example.test"}}, Sms: &v2beta2.SmsReceiver{Enabled: &enabled, SmsConfigSelector: selector, Template: &body, PhoneNumbers: []string{"+1234567890"}}, Feishu: &v2beta2.FeishuReceiver{Enabled: &enabled, Template: &body, TmplType: &text, ChatBot: &v2beta2.FeishuChatBot{Webhook: credential("https://mock.invalid/hook/synthetic-secret")}}}}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctl, err := controller.NewStatic(ctx, log.NewNopLogger(), cfg)
	if err != nil {
		t.Fatal("legacy original configuration rejected", err)
	}
	if err = ctl.Run(); err != nil {
		t.Fatal(err)
	}
	h := New(log.NewNopLogger(), 30*time.Second, ctl, nil)
	if err = h.validateProfile(spool.DeliveryProfile{ID: "formal", Version: "v1", CardOwnerID: "formal-owner"}); err != nil {
		t.Fatal("formal needs no Jev binding for original non-annotation channels", err)
	}
}
