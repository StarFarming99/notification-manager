package deliveryprofiles

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
)

func TestCutoverReadCredentialCannotWriteOrSelectIntake(t *testing.T) {
	db, err := spool.Open(filepath.Join(t.TempDir(), "spool.db"), spool.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	config := File{Version: 1, RetryDedupeWindow: "30s", RepeatInterval: "12h", Profiles: []spool.DeliveryProfile{{ID: "test", Version: "v1", CardOwnerID: "test-owner", Receiver: "critical", SourceChatID: ProductionChat, TestChatID: TestChat}, {ID: "formal", Version: "v1", CardOwnerID: "formal-owner"}}}
	test, formal, control, read := strings.Repeat("t", 32), strings.Repeat("f", 32), strings.Repeat("c", 32), strings.Repeat("r", 32)
	m, err := New(db, config, test, formal, control, read)
	if err != nil {
		t.Fatal(err)
	}
	next := func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }
	for _, tc := range []struct {
		method, path, token string
		handler             http.HandlerFunc
		want                int
	}{
		{"GET", "/internal/cutover-status", read, m.CutoverReadGuard(next), 200},
		{"GET", "/internal/cutover-status", control, m.CutoverReadGuard(next), 200},
		{"GET", "/internal/cutover-status", test, m.CutoverReadGuard(next), 401},
		{"POST", "/internal/delivery-profiles/formal/pause", read, m.ControlGuard(next), 401},
		{"POST", "/internal/delivery-profiles/formal/activate", read, m.ControlGuard(next), 401},
		{"GET", "/internal/delivery-profiles", read, m.ControlGuard(next), 401},
		{"POST", "/api/v2/alerts", read, m.Guard("formal", next), 401},
		{"POST", "/api/v2/test/alerts", read, m.Guard("test", next), 401},
		{"POST", "/internal/cutover-status", read, m.CutoverReadGuard(next), 405},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		r.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		tc.handler(w, r)
		if w.Code != tc.want {
			t.Fatal(tc.method, tc.path, w.Code)
		}
	}
	for _, invalid := range []string{"too-short", test, formal, control} {
		if _, err := New(db, config, test, formal, control, invalid); err == nil {
			t.Fatal("invalid read credential accepted")
		}
	}
	old, err := New(db, config, test, formal, control)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/internal/cutover-status", nil)
	r.Header.Set("Authorization", "Bearer "+control)
	w := httptest.NewRecorder()
	old.CutoverReadGuard(next)(w, r)
	if w.Code != 200 {
		t.Fatal("old control GET compatibility lost")
	}
}

func TestCutoverReadEnvIndependentCredential(t *testing.T) {
	db, err := spool.Open(filepath.Join(t.TempDir(), "spool.db"), spool.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	config := File{Version: 1, RetryDedupeWindow: "30s", RepeatInterval: "12h", Profiles: []spool.DeliveryProfile{{ID: "test", Version: "v1", CardOwnerID: "test-owner", Receiver: "critical", SourceChatID: ProductionChat, TestChatID: TestChat}, {ID: "formal", Version: "v1", CardOwnerID: "formal-owner"}}}
	file := filepath.Join(t.TempDir(), "profiles.json")
	raw, _ := json.Marshal(config)
	if err = os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	for k, v := range map[string]string{"NM_DELIVERY_PROFILES_FILE": file, "NM_CONFIGURATION_NAME": "fixture", "NM_TEST_INTAKE_TOKEN": strings.Repeat("t", 32), "NM_FORMAL_INTAKE_TOKEN": strings.Repeat("f", 32), "NM_PROFILE_CONTROL_TOKEN": strings.Repeat("c", 32), "NM_CUTOVER_READ_TOKEN": strings.Repeat("r", 32)} {
		t.Setenv(k, v)
	}
	m, err := FromEnv(db)
	if err != nil || m.cutoverReadToken != strings.Repeat("r", 32) {
		t.Fatal("read env contract failed", err)
	}
	t.Setenv("NM_CUTOVER_READ_TOKEN", strings.Repeat("c", 32))
	if _, err = FromEnv(db); err == nil {
		t.Fatal("read/control collision from env accepted")
	}
}
