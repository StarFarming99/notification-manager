package v1

import (
	"context"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/controller"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/pkg/deliveryprofiles"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/store"
)

func TestBrokenTestReceiverDoesNotRemoveActiveFormalBackend(t *testing.T) {
	db, err := spool.Open(filepath.Join(t.TempDir(), "spool.db"), spool.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m, err := deliveryprofiles.New(db, deliveryprofiles.File{Version: 1, RetryDedupeWindow: "2m", RepeatInterval: "12h", Profiles: []spool.DeliveryProfile{{ID: "test", Version: "v1", CardOwnerID: "test-owner", Receiver: "missing-critical", SourceChatID: deliveryprofiles.ProductionChat, TestChatID: deliveryprofiles.TestChat}, {ID: "formal", Version: "v1", CardOwnerID: "formal-owner"}}}, strings.Repeat("t", 32), strings.Repeat("f", 32), strings.Repeat("c", 32))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u := "http://mock.invalid/original"
	ctl, err := controller.NewStatic(ctx, log.NewNopLogger(), controller.StaticConfiguration{Environment: "test", Cluster: "fixture", Receivers: []v2beta2.Receiver{{ObjectMeta: metav1.ObjectMeta{Name: "original"}, Spec: v2beta2.ReceiverSpec{Webhook: &v2beta2.WebhookReceiver{URL: &u}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ctl.Run(); err != nil {
		t.Fatal(err)
	}
	ctl.DeliveryProfiles = m
	for _, id := range []string{"test", "formal"} {
		if _, err = db.ControlProfile(id, "prepare", "v1", 1, "fixture", "offline"); err != nil {
			t.Fatal(err)
		}
		if _, err = db.ControlProfile(id, "activate", "v1", 2, "fixture", "offline"); err != nil {
			t.Fatal(err)
		}
	}
	h := New(log.NewNopLogger(), time.Second, ctl, &store.AlertStore{Durable: db})
	w := httptest.NewRecorder()
	h.ServeReadinessCheck(w, httptest.NewRequest("GET", "/-/ready", nil))
	if w.Code != 200 {
		t.Fatal("test drift removed formal backend", w.Code, w.Body.String())
	}
	if _, err = db.ControlProfile("formal", "pause", "", 3, "fixture", "offline rollback"); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	h.ServeReadinessCheck(w, httptest.NewRequest("GET", "/-/ready", nil))
	if w.Code != 503 {
		t.Fatal("test-only readiness stopped validating scoped receiver", w.Code)
	}
}
