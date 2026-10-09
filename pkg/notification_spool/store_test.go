package notification_spool

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func target(destination string) Target {
	return Target{Receiver: "critical", Channel: "feishu", Destination: destination, ContentRevision: "revision-1", Payload: json.RawMessage(`{"credential_ref":"secret/test/key","render":"frozen"}`)}
}
func openTest(t *testing.T, limits Limits) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "notifications.db")
	s, err := Open(path, limits)
	if err != nil {
		t.Fatal(err)
	}
	return s, path
}

func TestAtomicIntakeDedupeAndCapacity(t *testing.T) {
	s, path := openTest(t, Limits{MaxTargets: 2})
	defer s.Close()
	i, dupe, err := s.Submit("request-1", "body-1", []Target{target("chat:one"), target("chat:two")}, "")
	if err != nil || dupe || len(i.TargetIDs) != 2 {
		t.Fatalf("intake: %+v %v %v", i, dupe, err)
	}
	j, dupe, err := s.Submit("request-1", "body-1", []Target{target("different-config")}, "")
	if err != nil || !dupe || j.ID != i.ID {
		t.Fatal("retry changed frozen target plan", err)
	}
	if _, _, err = s.Submit("request-1", "other-body", nil, "filtered"); err == nil {
		t.Fatal("conflicting retry accepted")
	}
	if _, _, err = s.Submit("request-2", "body-2", []Target{target("third")}, ""); !errors.Is(err, ErrCapacity) {
		t.Fatal("capacity failure missing", err)
	}
	intakes, targets, err := s.Snapshot()
	if err != nil || len(intakes) != 1 || len(targets) != 2 {
		t.Fatal("failed intake partially committed", err)
	}
	fi, err := os.Stat(path)
	if err != nil || fi.Mode().Perm() != 0600 {
		t.Fatal("spool file is not private")
	}
}

func TestRestartKeepsPartialSuccessAndFencesUnknown(t *testing.T) {
	s, path := openTest(t, Limits{})
	_, _, err := s.Submit("request", "body", []Target{target("chat:one"), target("chat:two"), target("bot:three")}, "")
	if err != nil {
		t.Fatal(err)
	}
	c1, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(c1, Delivered, "confirmed", "receipt:one", time.Time{}); err != nil {
		t.Fatal(err)
	}
	c2, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if other, err := Open(path, Limits{}); err == nil {
		other.Close()
		t.Fatal("second writer obtained ownership")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Finish(c2, Delivered, "stale", "", time.Time{}); !errors.Is(err, ErrFence) {
		t.Fatal("stale owner completed attempt", err)
	}
	_, all, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, v := range all {
		counts[v.State]++
	}
	if counts[Delivered] != 1 || counts[Unknown] != 1 || counts[Pending] != 1 {
		t.Fatalf("restart states %+v", counts)
	}
	c3, err := s.Claim(time.Now())
	if err != nil || c3.Target.ID == c1.Target.ID || c3.Target.ID == c2.Target.ID {
		t.Fatal("delivered/unknown replayed", err)
	}
	if _, err = s.Claim(time.Now()); !errors.Is(err, ErrEmpty) {
		t.Fatal("unexpected claim", err)
	}
	if err = s.Resolve(c2.Target.ID, Retryable, "", "receipt-checked"); err == nil {
		t.Fatal("unaudited reconciliation accepted")
	}
	if err = s.Resolve(c2.Target.ID, Retryable, "operator:test", "case:test-only-no-platform-receipt"); err != nil {
		t.Fatal(err)
	}
	c4, err := s.Claim(time.Now())
	if err != nil || c4.Target.ID != c2.Target.ID || c4.Target.Attempts != 2 {
		t.Fatal("target-only recovery failed", err)
	}
	if err = s.Finish(c4, Delivered, "confirmed", "receipt:two", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(c4, Delivered, "repeat", "receipt:two", time.Time{}); !errors.Is(err, ErrFence) {
		t.Fatal("double Ack accepted", err)
	}
}

func TestRetryScheduleDiskFailureAndReadOnlyInspection(t *testing.T) {
	s, path := openTest(t, Limits{})
	if _, _, err := s.Submit("a", "hash", []Target{target("one")}, ""); err != nil {
		t.Fatal(err)
	}
	c, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	next := time.Now().Add(time.Hour)
	if err = s.Finish(c, Retryable, "rate-limit", "attempt:1", next); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Claim(time.Now()); !errors.Is(err, ErrEmpty) {
		t.Fatal("retry ignored backoff")
	}
	if _, err = s.Claim(next.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = s.Check(); err != nil {
		t.Fatal(err)
	}
	s.Close()
	ro, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	_, records, err := ro.Snapshot()
	if err != nil || records[0].State != Sending {
		t.Fatal("read-only inspection mutated sending", err)
	}
	ro.Close()
	s, err = Open(path, Limits{ReserveBytes: ^uint64(0)})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, _, err = s.Submit("new", "hash2", []Target{target("two")}, ""); !errors.Is(err, ErrCapacity) {
		t.Fatal("disk reserve failure was acknowledged", err)
	}
	intakes, records, err := s.Snapshot()
	if err != nil || len(intakes) != 1 || records[0].State != Unknown {
		t.Fatal("recovery/capacity atomicity failed")
	}
}

func TestFilteredIntakeIsNotDelivered(t *testing.T) {
	s, _ := openTest(t, Limits{})
	defer s.Close()
	i, _, err := s.Submit("filtered", "body", nil, "global_silence")
	if err != nil || i.TerminalReason != "global_silence" {
		t.Fatal(err)
	}
	if _, err = s.Claim(time.Now()); !errors.Is(err, ErrEmpty) {
		t.Fatal("filtered intake counted as a delivered target")
	}
	if _, _, err = s.Submit("invalid", "body", nil, ""); err == nil {
		t.Fatal("empty unaccounted plan accepted")
	}
}

func TestHistoryDependsOnActualOriginalSuccessAndExactRecovery(t *testing.T) {
	s, _ := openTest(t, Limits{})
	defer s.Close()
	primary := target("chat:original")
	history := target("webhook:history")
	history.ContentRevision = "history-revision"
	history.DependencyRevisions = []string{primary.ContentRevision}
	intake, _, err := s.Submit("history-test", "body", []Target{primary, history}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimForRecovery(intake.TargetIDs[1], "operator:test", "case:test", time.Now()); err == nil {
		t.Fatal("history sent before original success")
	}
	c, err := s.ClaimForRecovery(intake.TargetIDs[0], "operator:test", "case:test", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(c, Delivered, "confirmed", "receipt:one", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimForRecovery(c.Target.ID, "operator:test", "case:test", time.Now()); err == nil {
		t.Fatal("recovery replayed a delivered original")
	}
	next, err := s.Claim(time.Now())
	if err != nil || next.Target.ID != intake.TargetIDs[1] {
		t.Fatal("history did not become ready", err)
	}
	history.DependencyRevisions = []string{"unrelated-intake"}
	if _, _, err := s.Submit("invalid-history", "body", []Target{history}, ""); err == nil {
		t.Fatal("unresolvable history dependency accepted")
	}
}
