package controller

import (
	"bytes"
	"context"
	"encoding/base64"
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

func TestGenericCRLiteralChatbotReferenceAndDestinationFence(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = v2beta2.AddToScheme(scheme)
	obj := &v2beta2.Receiver{ObjectMeta: metav1.ObjectMeta{Name: "legacy-chatbot", UID: "receiver-uid"}, Spec: v2beta2.ReceiverSpec{Feishu: &v2beta2.FeishuReceiver{ChatBot: &v2beta2.FeishuChatBot{Webhook: &v2beta2.Credential{Value: "https://mock.invalid/hook/synthetic-secret"}, Secret: &v2beta2.Credential{Value: "synthetic-signing-secret"}}}}}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(obj).Build()
	c := &Controller{ctx: ctx, cache: credentialCache{reader: reader}}
	raw := json.RawMessage(`{"chatbot":{"webhook":{"value":"https://mock.invalid/hook/synthetic-secret"},"secret":{"value":"synthetic-signing-secret"}}}`)
	safe, refs, err := c.FreezeCredentialReferences(raw, obj.Name, "", "feishu")
	if err != nil || len(refs) != 2 {
		t.Fatal(refs, err)
	}
	if strings.Contains(string(safe), "synthetic") {
		t.Fatal("plaintext credential in frozen receiver")
	}
	encoded, _ := json.Marshal(refs)
	if strings.Contains(string(encoded), "synthetic") {
		t.Fatal("plaintext in reference")
	}
	restored, err := c.ResolveCredentialReferences(safe, refs)
	if err != nil || !strings.Contains(string(restored), "synthetic-secret") {
		t.Fatal(err)
	}
	var live v2beta2.Receiver
	_ = reader.Get(ctx, client.ObjectKey{Name: obj.Name}, &live)
	live.Spec.Feishu.ChatBot.Secret.Value = "rotated-synthetic-signature"
	_ = reader.Update(ctx, &live)
	restored, err = c.ResolveCredentialReferences(safe, refs)
	if err != nil || !strings.Contains(string(restored), "rotated-synthetic-signature") {
		t.Fatal("safe signature rotation rejected", err)
	}
	_ = reader.Get(ctx, client.ObjectKey{Name: obj.Name}, &live)
	live.Spec.Feishu.ChatBot.Webhook.Value = "https://different.invalid/synthetic"
	_ = reader.Update(ctx, &live)
	if _, err = c.ResolveCredentialReferences(safe, refs); err == nil {
		t.Fatal("accepted destination silently redirected")
	}
	_ = reader.Get(ctx, client.ObjectKey{Name: obj.Name}, &live)
	live.UID = "replacement"
	_ = reader.Update(ctx, &live)
	if _, err = c.ResolveCredentialReferences(safe, refs); err == nil {
		t.Fatal("replacement UID accepted")
	}
}

func TestInlineCredentialEncryptionRestartRotationAndTamper(t *testing.T) {
	key1 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
	key2 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{2}, 32))
	t.Setenv("NM_FROZEN_CREDENTIAL_ACTIVE_KEY", "k1")
	t.Setenv("NM_FROZEN_CREDENTIAL_KEYS", `{"k1":"`+key1+`"}`)
	c := &Controller{staticIsolated: true}
	raw := json.RawMessage(`{"token":{"value":"api-inline-synthetic-secret"}}`)
	safe, refs, err := c.FreezeCredentialReferences(raw, "inline", "", "slack")
	if err != nil || len(refs) != 1 {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(refs)
	if strings.Contains(string(safe)+string(encoded), "api-inline-synthetic-secret") {
		t.Fatal("plaintext persisted")
	}
	serialized, _ := json.Marshal(refs)
	var restoredRefs []CredentialReference
	_ = json.Unmarshal(serialized, &restoredRefs)
	fresh := &Controller{staticIsolated: true}
	out, err := fresh.ResolveCredentialReferences(safe, restoredRefs)
	if err != nil || !strings.Contains(string(out), "api-inline-synthetic-secret") {
		t.Fatal("restart unresolved", err)
	}
	t.Setenv("NM_FROZEN_CREDENTIAL_ACTIVE_KEY", "k2")
	t.Setenv("NM_FROZEN_CREDENTIAL_KEYS", `{"k1":"`+key1+`","k2":"`+key2+`"}`)
	if _, err = fresh.ResolveCredentialReferences(safe, restoredRefs); err != nil {
		t.Fatal("retained old key did not resolve", err)
	}
	_, newRefs, err := fresh.FreezeCredentialReferences(raw, "inline", "", "slack")
	if err != nil || newRefs[0].KeyID != "k2" {
		t.Fatal("rotation did not select new key", err)
	}
	restoredRefs[0].TargetPath = []string{"different"}
	if _, err = fresh.ResolveCredentialReferences(safe, restoredRefs); err == nil {
		t.Fatal("AEAD path tamper accepted")
	}
	_ = json.Unmarshal(serialized, &restoredRefs)
	t.Setenv("NM_FROZEN_CREDENTIAL_KEYS", `{"k2":"`+key2+`"}`)
	if _, err = fresh.ResolveCredentialReferences(safe, restoredRefs); err == nil || strings.Contains(err.Error(), "synthetic") {
		t.Fatal("missing key should fail safely", err)
	}
}

func TestScalarSensitiveURLsAndPushoverKeyAreEncrypted(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
	t.Setenv("NM_FROZEN_CREDENTIAL_ACTIVE_KEY", "k1")
	t.Setenv("NM_FROZEN_CREDENTIAL_KEYS", `{"k1":"`+key+`"}`)
	ctl := &Controller{staticIsolated: true}
	raw := json.RawMessage(`{"url":"https://synthetic-user:synthetic-password@mock.invalid/hook?token=synthetic-token","httpConfig":{"proxyUrl":"https://proxy-user:proxy-secret@mock.invalid"},"profiles":[{"userKey":"synthetic-pushover-secret"}]}`)
	safe, refs, err := ctl.FreezeCredentialReferences(raw, "inline", "", "webhook")
	if err != nil || len(refs) != 3 {
		t.Fatal("sensitive scalar freeze", len(refs), err)
	}
	encoded, _ := json.Marshal(refs)
	if strings.Contains(string(safe)+string(encoded), "synthetic-") || strings.Contains(string(safe)+string(encoded), "proxy-secret") {
		t.Fatal("scalar secret persisted")
	}
	out, err := ctl.ResolveCredentialReferences(safe, refs)
	if err != nil || !strings.Contains(string(out), "synthetic-pushover-secret") || !strings.Contains(string(out), "synthetic-password") || !strings.Contains(string(out), "proxy-secret") {
		t.Fatal("scalar restore", err)
	}
}

func TestGenericConfigReferencePinsSenderAndUIDWhileSecretRotates(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = v2beta2.AddToScheme(scheme)
	obj := &v2beta2.Config{ObjectMeta: metav1.ObjectMeta{Name: "original", UID: "original-uid"}, Spec: v2beta2.ConfigSpec{Feishu: &v2beta2.FeishuConfig{AppID: &v2beta2.Credential{Value: "original-app"}, AppSecret: &v2beta2.Credential{Value: "synthetic-original-secret"}}}}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(obj).Build()
	c := &Controller{ctx: ctx, cache: credentialCache{reader: reader}}
	raw := json.RawMessage(`{"appID":{"value":"original-app"},"appSecret":{"value":"synthetic-original-secret"}}`)
	safe, refs, err := c.FreezeCredentialReferences(raw, "", "original", "feishu")
	if err != nil || len(refs) != 1 || refs[0].SenderApp != "original-app" {
		t.Fatal("sender not pinned", refs, err)
	}
	var live v2beta2.Config
	_ = reader.Get(ctx, client.ObjectKey{Name: obj.Name}, &live)
	live.Spec.Feishu.AppSecret.Value = "synthetic-rotated-secret"
	_ = reader.Update(ctx, &live)
	restored, err := c.ResolveCredentialReferences(safe, refs)
	if err != nil || !strings.Contains(string(restored), "synthetic-rotated-secret") {
		t.Fatal("rotation", err)
	}
	_ = reader.Get(ctx, client.ObjectKey{Name: obj.Name}, &live)
	live.Spec.Feishu.AppID.Value = "different-app"
	_ = reader.Update(ctx, &live)
	if _, err := c.ResolveCredentialReferences(safe, refs); err == nil {
		t.Fatal("sender drift accepted")
	}
	_ = reader.Get(ctx, client.ObjectKey{Name: obj.Name}, &live)
	live.Spec.Feishu.AppID.Value = "original-app"
	live.UID = "replacement-uid"
	_ = reader.Update(ctx, &live)
	if _, err := c.ResolveCredentialReferences(safe, refs); err == nil {
		t.Fatal("UID replacement accepted")
	}
}
