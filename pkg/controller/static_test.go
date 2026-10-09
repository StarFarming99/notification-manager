package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/internal/feishu"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestStaticFeishuResolvesExplicitConfiguration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := StaticConfiguration{Environment: "test", Cluster: "sidechannel", Configs: []v2beta2.Config{
		{ObjectMeta: metav1.ObjectMeta{Name: "sidechannel-app"}, Spec: v2beta2.ConfigSpec{Feishu: &v2beta2.FeishuConfig{
			AppID: &v2beta2.Credential{Value: "fixture-app"}, AppSecret: &v2beta2.Credential{Value: "fixture-secret"},
		}}},
	}, Receivers: []v2beta2.Receiver{
		{ObjectMeta: metav1.ObjectMeta{Name: "sidechannel-us"}, Spec: v2beta2.ReceiverSpec{Feishu: &v2beta2.FeishuReceiver{ChatIDs: []string{"fixture-chat"}}}},
	}}
	c, err := NewStatic(ctx, log.NewNopLogger(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Run(); err != nil {
		t.Fatal(err)
	}
	receivers := c.RcvsFromNs("sidechannel", nil)
	if len(receivers) != 1 {
		t.Fatalf("receivers=%d", len(receivers))
	}
	receiver, ok := receivers[0].(*feishu.Receiver)
	if !ok || receiver.Config == nil || receiver.Config.AppID.Value != "fixture-app" {
		t.Fatal("static global receiver did not resolve its explicit credentials")
	}
	if c.cache != nil {
		t.Fatal("static credential resolution must not create a Kubernetes client")
	}
}

func TestStaticControllerHasNoKubernetesDependencies(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	endpoint := "http://mock.test/notifications"
	config := StaticConfiguration{Environment: "test", Cluster: "fixture", Receivers: []v2beta2.Receiver{
		{ObjectMeta: metav1.ObjectMeta{Name: "receiver-test"}, Spec: v2beta2.ReceiverSpec{Webhook: &v2beta2.WebhookReceiver{URL: &endpoint}}},
	}}
	c, err := NewStatic(ctx, log.NewNopLogger(), config)
	if err != nil {
		t.Fatal(err)
	}
	if c.cache != nil {
		t.Fatal("static controller created a Kubernetes cache")
	}
	if err := c.Run(); err != nil {
		t.Fatal(err)
	}
	done := make(chan int, 1)
	go func() { done <- len(c.RcvsFromNs("fixture", nil)) }()
	select {
	case count := <-done:
		if count != 1 {
			t.Fatalf("receivers=%d", count)
		}
	case <-time.After(time.Second):
		t.Fatal("static read blocked")
	}
	if c.GetCluster() != "fixture" {
		t.Fatal("cluster not static")
	}
	if _, err := c.GetConfigmap(&v2beta2.ConfigmapKeySelector{Name: "prod"}); err == nil {
		t.Fatal("cluster reference accepted")
	}
	if _, err := c.GetActiveRouters(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetActiveSilences(ctx, ""); err != nil {
		t.Fatal(err)
	}
	config.Environment = "production"
	if _, err := NewStatic(ctx, log.NewNopLogger(), config); err == nil {
		t.Fatal("production static mode accepted")
	}
}
