package notification_spool

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProfileBudgetAndClaimDeferralPersistAcrossRestart(t *testing.T) {
	s, path, _ := profileFixture(t)
	snap, _ := s.ProfileSnapshot("test")
	for _, key := range []string{"budget-one", "budget-two"} {
		if _, _, err := s.SubmitProfile(key, key, []Target{profileTarget(snap)}, "", snap, 0); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	first, err := s.Claim(now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.WithSendPermit(first, 12*time.Second, now, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(first, Delivered, "confirmed", "receipt", time.Time{}); err != nil {
		t.Fatal(err)
	}
	second, err := s.Claim(now)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	err = s.WithSendPermit(second, 12*time.Second, now.Add(time.Millisecond), func() error { called = true; return nil })
	var deferred *SendBudgetDeferred
	if !errors.As(err, &deferred) || called {
		t.Fatal("parallel claim bypassed persistent budget", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, rows, _ := s.Snapshot()
	for _, row := range rows {
		if row.ID == second.Target.ID && row.State != Retryable {
			t.Fatal("unsent claim became uncertain", row.State)
		}
	}
	second, err = s.Claim(time.Now().UTC().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.WithSendPermit(second, 12*time.Second, now.Add(time.Second), func() error { called = true; return nil }); !errors.As(err, &deferred) || called {
		t.Fatal("restart reset send gate", err)
	}
	if err := s.WithSendPermit(second, 12*time.Second, now.Add(13*time.Second), func() error { called = true; return nil }); err != nil || !called {
		t.Fatal("budget never reopened", err)
	}
}

func TestProfilePauseStopsAlreadyClaimedUnsentWorkAndDrainsStartedSend(t *testing.T) {
	s, _, _ := profileFixture(t)
	defer s.Close()
	snap, _ := s.ProfileSnapshot("test")
	if _, _, err := s.SubmitProfile("before-pause", "before-pause", []Target{profileTarget(snap)}, "", snap, 0); err != nil {
		t.Fatal(err)
	}
	claim, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	st, err := s.ControlProfile("test", "pause", "", 3, "operator", "withdraw AP")
	if err != nil {
		t.Fatal(err)
	}
	called := false
	if err := s.WithSendPermit(claim, 12*time.Second, time.Now(), func() error { called = true; return nil }); !errors.Is(err, ErrProfilePaused) || called {
		t.Fatal("claimed work crossed paused profile", err)
	}
	if _, err := s.ControlProfile("test", "resume", "", st.Revision, "operator", "resume AP"); err != nil {
		t.Fatal(err)
	}
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- s.WithSendPermit(claim, 12*time.Second, time.Now(), func() error { close(started); <-release; return nil })
	}()
	<-started
	paused := make(chan error, 1)
	go func() {
		_, err := s.ControlProfile("test", "pause", "", st.Revision+1, "operator", "drain AP")
		paused <- err
	}()
	select {
	case err := <-paused:
		t.Fatal("pause returned before bounded send ended", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if err := <-paused; err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(claim, Delivered, "confirmed", "receipt", time.Time{}); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyTargetsKeepExistingSendThroughput(t *testing.T) {
	s, _ := openTest(t, Limits{})
	defer s.Close()
	for _, key := range []string{"legacy-one", "legacy-two"} {
		if _, _, err := s.Submit(key, key, []Target{target(key)}, ""); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		claim, err := s.Claim(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := s.WithSendPermit(claim, 12*time.Second, time.Now(), func() error { return nil }); err != nil {
			t.Fatal("new sidechannel budget altered legacy send", err)
		}
		if err := s.Finish(claim, Delivered, "confirmed", "receipt", time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConcurrentProfileClaimsShareOnePersistentReservation(t *testing.T) {
	s, _, _ := profileFixture(t)
	defer s.Close()
	snap, _ := s.ProfileSnapshot("test")
	var claims []Claim
	for i := 0; i < 8; i++ {
		key := string(rune('a' + i))
		if _, _, err := s.SubmitProfile(key, key, []Target{profileTarget(snap)}, "", snap, 0); err != nil {
			t.Fatal(err)
		}
		claim, err := s.Claim(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		claims = append(claims, claim)
	}
	var calls atomic.Int32
	var wg sync.WaitGroup
	now := time.Now().UTC()
	for _, claim := range claims {
		claim := claim
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.WithSendPermit(claim, 12*time.Second, now, func() error { calls.Add(1); return nil })
			var deferred *SendBudgetDeferred
			if err != nil && !errors.As(err, &deferred) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatal("concurrent workers exceeded regional budget", calls.Load())
	}
}
