package notification_spool

import (
	"errors"
	"testing"
	"time"
)

func TestCompletedTargetsReleaseCapacityAndRestartRetainsUnknownFence(t *testing.T) {
	s, path := openTest(t, Limits{MaxTargets: 1})
	if _, _, err := s.Submit("one", "one", []Target{target("one")}, ""); err != nil {
		t.Fatal(err)
	}
	c, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Submit("two", "two", []Target{target("two")}, ""); !errors.Is(err, ErrCapacity) {
		t.Fatal("sending must reserve capacity", err)
	}
	if err := s.Finish(c, Delivered, "confirmed", "receipt", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Ready(); err != nil {
		t.Fatal("success permanently blocked readiness", err)
	}
	if _, _, err := s.Submit("two", "two", []Target{target("two")}, ""); err != nil {
		t.Fatal("completed payload held active capacity", err)
	}
	if _, err := s.Claim(time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Limits{MaxTargets: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, _, err := s.Submit("three", "three", []Target{target("three")}, ""); !errors.Is(err, ErrCapacity) {
		t.Fatal("recovered unknown lost its capacity fence", err)
	}
	status, err := s.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status["active_targets"] != uint64(1) || status["counts"].(map[string]int)[Unknown] != 1 {
		t.Fatal(status)
	}
}

func TestCompletedCleanupRetainsDedupeTombstoneAndUncertainWork(t *testing.T) {
	s, path := openTest(t, Limits{})
	i, _, err := s.Submit("stable-key", "body", []Target{target("one")}, "")
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(c, Delivered, "confirmed", "receipt", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Submit("unknown-key", "unknown-body", []Target{target("unknown")}, ""); err != nil {
		t.Fatal(err)
	}
	u, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(u, Unknown, "ambiguous", "case", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CleanupCompleted(time.Hour, 100, time.Now().Add(72*time.Hour)); err == nil {
		t.Fatal("unsafe short retention accepted")
	}
	n, err := s.CleanupCompleted(48*time.Hour, 100, time.Now().Add(72*time.Hour))
	if err != nil || n != 1 {
		t.Fatal(n, err)
	}
	rows, targets, err := s.Snapshot()
	if err != nil || len(rows) != 2 || len(targets) != 1 || targets[0].State != Unknown {
		t.Fatal(rows, targets, err)
	}
	if err := s.Check(); err != nil {
		t.Fatal("cleanup corrupted indexes", err)
	}
	if _, err := s.Claim(time.Now().Add(72 * time.Hour)); !errors.Is(err, ErrEmpty) {
		t.Fatal("unknown became automatic work", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	again, duplicate, err := s.Submit("stable-key", "body", []Target{target("changed-live-config")}, "")
	if err != nil || !duplicate || again.ID != i.ID || len(again.TargetIDs) != 0 || again.TerminalReason != "confirmed_completed_tombstone" {
		t.Fatal("cleanup allowed blind replay", again, duplicate, err)
	}
}

func TestWriteFailureHealthIsSeparateFromCapacityAndRejectedPlans(t *testing.T) {
	s, _ := openTest(t, Limits{MaxTargets: 1})
	if _, _, err := s.Submit("one", "body", []Target{target("one")}, ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Submit("two", "body", []Target{target("two")}, ""); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	if s.lastStorageError != "" || s.storageWriteFailures != 0 {
		t.Fatal("logical capacity rejection classified as I/O outage")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Submit("three", "body", []Target{target("three")}, ""); err == nil {
		t.Fatal("closed writer acknowledged")
	}
	if s.lastStorageError != "storage_write_failed" || s.storageWriteFailures != 1 || s.Ready() == nil {
		t.Fatal("failed persistence did not fail readiness")
	}
}
