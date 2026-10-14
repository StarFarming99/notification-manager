package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
)

func captureRecovery(t *testing.T, args ...string) (string, error) {
	t.Helper()
	original := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	result := runArgs(args)
	_ = w.Close()
	os.Stdout = original
	raw, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil {
		t.Fatal(err)
	}
	return string(raw), result
}
func TestOfflineFormalPauseRequiresStoppedWriterAndPreservesLedger(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notifications.db")
	s, err := spool.Open(path, spool.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	profiles := []spool.DeliveryProfile{{ID: "test", Version: "v1", CardOwnerID: "test-owner", Receiver: "critical", SourceChatID: "production", TestChatID: "test"}, {ID: "formal", Version: "v1", CardOwnerID: "formal-owner"}}
	if err = s.RegisterProfiles(profiles, ""); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ControlProfile("formal", "prepare", "v1", 1, "fixture", "offline prepare"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ControlProfile("formal", "activate", "v1", 2, "fixture", "offline activate"); err != nil {
		t.Fatal(err)
	}
	snap, _ := s.ProfileSnapshot("formal")
	target := spool.Target{ProfileID: "formal", ProfileVersion: "v1", CardOwnerID: "formal-owner", Receiver: "critical", Channel: "feishu", Destination: "chat:production", OriginalDestination: "chat:production", ContentRevision: "frozen", Payload: json.RawMessage(`{"message_id":"synthetic-preserved-mid"}`)}
	if _, _, err = s.SubmitProfile("existing", "existing", []spool.Target{target}, "", snap, 0); err != nil {
		t.Fatal(err)
	}
	args := []string{"--spool", path, "--action", "pause-profile", "--profile", "formal", "--expected-profile-revision", "3", "--actor", "fixture", "--evidence", "offline rollback"}
	started := time.Now()
	if _, err = captureRecovery(t, args...); err == nil {
		t.Fatal("live writer bypassed offline fence")
	}
	if time.Since(started) > time.Second {
		t.Fatal("writer lock check unbounded")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := captureRecovery(t, args...)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output, `"paused":true`) || !strings.Contains(output, `"revision":4`) || !strings.Contains(output, `"writer_stopped":true`) {
		t.Fatal(output)
	}
	if _, err = captureRecovery(t, args...); err == nil {
		t.Fatal("offline stale CAS blindly repeated")
	}
	output, err = captureRecovery(t, "--spool", path, "--action", "check")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output, `"profiles"`) || !strings.Contains(output, `"paused":true`) || strings.Contains(output, "synthetic-preserved-mid") {
		t.Fatal("check lost pause or disclosed payload", output)
	}
	s, err = spool.Open(path, spool.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.DisableFormal(); err != nil {
		t.Fatal(err)
	}
	_, rows, err := s.Snapshot()
	if err != nil || len(rows) != 1 || rows[0].State != spool.Pending || !strings.Contains(string(rows[0].Payload), "synthetic-preserved-mid") {
		t.Fatal("offline pause altered/replayed ledger", rows, err)
	}
	if _, err = s.ClaimProfile("formal", time.Now()); err == nil {
		t.Fatal("offline-paused ledger resumed send")
	}
}
