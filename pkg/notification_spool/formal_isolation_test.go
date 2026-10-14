package notification_spool

import (
	"errors"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"testing"
	"time"
)

func activateFormalFixture(t *testing.T, s *Store) ProfileSnapshot {
	t.Helper()
	if _, err := s.ControlProfile("formal", "prepare", "v1", 1, "fixture", "offline preparation"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ControlProfile("formal", "activate", "v1", 2, "fixture", "offline activation"); err != nil {
		t.Fatal(err)
	}
	snap, err := s.ProfileSnapshot("formal")
	if err != nil {
		t.Fatal(err)
	}
	return snap
}
func TestFormalClaimsAndClockIgnoreHistoricalTestBacklog(t *testing.T) {
	s, path, _ := profileFixture(t)
	test, _ := s.ProfileSnapshot("test")
	formal := activateFormalFixture(t, s)
	for i := 0; i < 256; i++ {
		key := fmt.Sprintf("backlog-%03d", i)
		if _, _, err := s.SubmitProfile(key, key, []Target{profileTarget(test)}, "", test, 0); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	first, err := s.ClaimProfile("test", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.WithSendPermit(first, 12*time.Second, now, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(first, Unknown, "lost response", "synthetic", time.Time{}); err != nil {
		t.Fatal(err)
	}
	t2, err := s.ClaimProfile("test", now)
	if err != nil {
		t.Fatal(err)
	}
	err = s.WithSendPermit(t2, 12*time.Second, now, func() error { t.Fatal("test bypassed budget"); return nil })
	var deferred *SendBudgetDeferred
	if !errors.As(err, &deferred) {
		t.Fatal(err)
	}
	if err := s.Finish(t2, Retryable, "budget", "synthetic", deferred.AvailableAt); err != nil {
		t.Fatal(err)
	}
	ft := profileTarget(formal)
	ft.Destination = "chat:production"
	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("formal-%d", i)
		if _, _, err := s.SubmitProfile(key, key, []Target{ft}, "", formal, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 2; i++ {
		c, err := s.ClaimProfile("formal", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if c.Target.ProfileID != "formal" || c.Target.Destination != "chat:production" {
			t.Fatal("claimed test or redirected", c)
		}
		if err := s.WithSendPermit(c, 0, now.Add(time.Millisecond), func() error { return nil }); err != nil {
			t.Fatal("test clock delayed formal", err)
		}
		if err := s.Finish(c, Delivered, "confirmed", "synthetic", time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	_, rows, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	unknown := 0
	for _, row := range rows {
		if row.ProfileID == "test" && row.State == Unknown {
			unknown++
		}
		if row.ProfileID == "test" && row.Destination != "chat:test" {
			t.Fatal("history redirected")
		}
	}
	if unknown != 1 {
		t.Fatal("unknown replayed or lost", unknown)
	}
}
func TestFormalPauseDoesNotWaitTestSendAndStartupFenceRequiresDurablePause(t *testing.T) {
	s, _, _ := profileFixture(t)
	defer s.Close()
	activateFormalFixture(t, s)
	if err := s.DisableFormal(); err == nil {
		t.Fatal("active formal accepted startup fence")
	}
	test, _ := s.ProfileSnapshot("test")
	if _, _, err := s.SubmitProfile("test-flight", "hash", []Target{profileTarget(test)}, "", test, 0); err != nil {
		t.Fatal(err)
	}
	c, err := s.ClaimProfile("test", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- s.WithSendPermit(c, 12*time.Second, time.Now(), func() error { close(started); <-release; return nil })
	}()
	<-started
	paused := make(chan error, 1)
	go func() { _, err := s.ControlProfile("formal", "pause", "", 3, "fixture", "rollback"); paused <- err }()
	select {
	case err := <-paused:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("formal pause waited for test")
	}
	if err := s.DisableFormal(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ControlProfile("formal", "resume", "", 4, "fixture", "unsafe restore"); !errors.Is(err, ErrProfilePaused) {
		t.Fatal("startup fence resumed formal", err)
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}
func TestTestCapacityReservesFormalSlots(t *testing.T) {
	s, _, _ := profileFixture(t)
	defer s.Close()
	if err := s.ConfigureTestCapacity(1); err != nil {
		t.Fatal(err)
	}
	test, _ := s.ProfileSnapshot("test")
	formal := activateFormalFixture(t, s)
	if _, _, err := s.SubmitProfile("first", "first", []Target{profileTarget(test)}, "", test, 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.SubmitProfile("overlimit", "overlimit", []Target{profileTarget(test)}, "", test, 0); !errors.Is(err, ErrCapacity) {
		t.Fatal("test not bounded", err)
	}
	ft := profileTarget(formal)
	ft.Destination = "chat:production"
	if _, _, err := s.SubmitProfile("formal", "formal", []Target{ft}, "", formal, 0); err != nil {
		t.Fatal("test blocked formal capacity", err)
	}
}

func TestFormalQuotaSurvivesFullHistoricalTestAndV1IndexUpgrade(t *testing.T) {
	s, path, profiles := profileFixture(t)
	test, _ := s.ProfileSnapshot("test")
	for i := 0; i < 12; i++ {
		key := fmt.Sprintf("old-%02d", i)
		if _, _, err := s.SubmitProfile(key, key, []Target{profileTarget(test)}, "", test, 0); err != nil {
			t.Fatal(err)
		}
	}
	// Keep delivered history, retry/unknown records and pending work; simulate the
	// original v1 ledger without our derived lane index or profile counters.
	for i := 0; i < 6; i++ {
		c, err := s.Claim(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		state := Delivered
		if i == 0 {
			state = Unknown
		}
		if i == 1 {
			state = Retryable
		}
		if err = s.Finish(c, state, "historical", "fixture", time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte("ready-lanes")); err != nil {
			return err
		}
		return tx.Bucket([]byte("meta")).Delete([]byte("active-targets-profile:test"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Eight historical test active records already fill/exceed this deployment's
	// seven-slot original quota. New formal work must still have its own quota.
	s, err := Open(path, Limits{MaxTargets: 7})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.RegisterProfiles(profiles, "v1"); err != nil {
		t.Fatal(err)
	}
	if err = s.ConfigureTestCapacity(7); err != nil {
		t.Fatal(err)
	}
	formal := activateFormalFixture(t, s)
	if err = s.ReadyProfile("formal"); err != nil {
		t.Fatal("old test rows block formal readiness", err)
	}
	if err = s.ReadyProfile("test"); !errors.Is(err, ErrCapacity) {
		t.Fatal("historical test overlimit not visible", err)
	}
	ft := profileTarget(formal)
	ft.Destination = "chat:production"
	if _, _, err = s.SubmitProfile("formal-new", "formal-new", []Target{ft}, "", formal, 0); err != nil {
		t.Fatal("formal quota consumed by test", err)
	}
	c, err := s.ClaimProfile("formal", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.WithSendPermit(c, 0, time.Now(), func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(c, Delivered, "confirmed", "fixture", time.Time{}); err != nil {
		t.Fatal(err)
	}
	_, rows, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 13 {
		t.Fatal("history altered or replayed", len(rows))
	}
	states := map[string]int{}
	for _, row := range rows {
		if row.ProfileID == "test" {
			states[row.State]++
		}
	}
	if states[Unknown] != 1 || states[Retryable] != 1 || states[Delivered] != 4 || states[Pending] != 6 {
		t.Fatal("historical outcomes changed", states)
	}
}
