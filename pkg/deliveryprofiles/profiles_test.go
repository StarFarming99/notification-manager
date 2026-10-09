package deliveryprofiles

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
)

func TestLaneCredentialsAndFormalPreparation(t *testing.T) {
	db, err := spool.Open(filepath.Join(t.TempDir(), "spool.db"), spool.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	config := File{Version: 1, InitialTestVersion: "v1", RetryDedupeWindow: "30s", RepeatInterval: "12h", Profiles: []spool.DeliveryProfile{{ID: "test", Version: "v1", CardOwnerID: "test-owner", Receiver: "critical", SourceChatID: ProductionChat, TestChatID: TestChat}, {ID: "formal", Version: "v1", CardOwnerID: "formal-owner"}}}
	test, formal, control := strings.Repeat("t", 32), strings.Repeat("f", 32), strings.Repeat("c", 32)
	m, err := New(db, config, test, formal, control)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ControlProfile("test", "prepare", "v1", 1, "fixture", "verified fixture"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ControlProfile("test", "activate", "v1", 2, "fixture", "verified fixture"); err != nil {
		t.Fatal(err)
	}
	next := func(w http.ResponseWriter, r *http.Request) {
		snap, ok := Snapshot(r.Context())
		if !ok || snap.ID != "test" {
			t.Fatal("request lane missing")
		}
		w.WriteHeader(202)
	}
	for _, tc := range []struct {
		lane, token string
		want        int
	}{{"test", test, 202}, {"test", formal, 401}, {"test", control, 401}, {"formal", test, 401}, {"formal", formal, 503}} {
		r := httptest.NewRequest("POST", "/", strings.NewReader(`{"profile_id":"formal"}`))
		r.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		m.Guard(tc.lane, next)(w, r)
		if w.Code != tc.want {
			t.Fatal(tc, w.Code)
		}
	}
	if _, err := New(db, config, test, test, control); err == nil {
		t.Fatal("shared lane credential accepted")
	}
	config.RetryDedupeWindow = "12h"
	if _, err := New(db, config, test, formal, control); err == nil {
		t.Fatal("repeat-suppressing dedupe window accepted")
	}
}
