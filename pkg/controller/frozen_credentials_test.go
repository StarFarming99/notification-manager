package controller

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/internal"
	feishutype "github.com/kubesphere/notification-manager/pkg/internal/feishu"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type credentialCache struct {
	cache.Cache
	reader client.Client
}

func (c credentialCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.reader.Get(ctx, key, obj, opts...)
}

func TestOriginalLiteralCredentialReferenceRotationAndIdentityFence(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = v2beta2.AddToScheme(scheme)
	obj := &v2beta2.Config{ObjectMeta: metav1.ObjectMeta{Name: "original-feishu", UID: "original-uid"}, Spec: v2beta2.ConfigSpec{Feishu: &v2beta2.FeishuConfig{AppID: &v2beta2.Credential{Value: "public-app"}, AppSecret: &v2beta2.Credential{Value: "synthetic-secret-never-in-ledger"}}}}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(obj).Build()
	c := &Controller{ctx: ctx, cache: credentialCache{reader: reader}}
	config := &feishutype.Config{Common: &internal.Common{Name: obj.Name}, AppID: obj.Spec.Feishu.AppID, AppSecret: obj.Spec.Feishu.AppSecret}
	ref, err := c.FreezeFeishuCredentialReference(config)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(ref)
	if strings.Contains(string(raw), "synthetic-secret") {
		t.Fatal("secret copied into frozen reference")
	}
	secret, err := c.ResolveFeishuCredentialReference(ref)
	if err != nil || secret != obj.Spec.Feishu.AppSecret.Value {
		t.Fatal("original credential unresolved", err)
	}
	var live v2beta2.Config
	_ = reader.Get(ctx, client.ObjectKey{Name: obj.Name}, &live)
	live.Spec.Feishu.AppSecret.Value = "rotated-synthetic-secret"
	if err := reader.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	secret, err = c.ResolveFeishuCredentialReference(ref)
	if err != nil || secret != "rotated-synthetic-secret" {
		t.Fatal("same-owner credential rotation was rejected", err)
	}
	_ = reader.Get(ctx, client.ObjectKey{Name: obj.Name}, &live)
	live.Spec.Feishu.AppID.Value = "different-app"
	_ = reader.Update(ctx, &live)
	if _, err := c.ResolveFeishuCredentialReference(ref); err == nil {
		t.Fatal("app identity drift bypassed fence")
	}
	_ = reader.Get(ctx, client.ObjectKey{Name: obj.Name}, &live)
	live.Spec.Feishu.AppID.Value = "public-app"
	live.UID = "replacement-uid"
	_ = reader.Update(ctx, &live)
	if _, err := c.ResolveFeishuCredentialReference(ref); err == nil {
		t.Fatal("replacement config UID bypassed fence")
	}
}
