package notification_spool

import (
	"context"
	"errors"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestReleaseSafetyExpiryCancelsOnlyUnretiredFrozenSender(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	beginPair(t, a, b)
	controlRelease(t, a, "close-admission")
	before := controlRelease(t, a, "freeze")
	if err := a.RefreshReleaseSafety(context.Background()); err != nil {
		t.Fatal(err)
	}
	unchanged, _ := a.ReleaseStatus()
	if unchanged.Revision != before.Revision || unchanged.Phase != "frozen" {
		t.Fatal("early refresh mutated release", unchanged)
	}
	changed := false
	if err := a.update(func(tx *bolt.Tx) error {
		var err error
		changed, err = recoverExpiredSender(tx, before.Deadline.Add(time.Millisecond))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	current, _ := a.ReleaseStatus()
	if !changed || current.Phase != "cancelling" || current.Revision != before.Revision+1 || !current.Deadline.Equal(before.Deadline) || !current.DeadlineExceeded || current.ExportedChecksum != "" || current.ImportedChecksum != "" {
		t.Fatal(current)
	}
	called := false
	if err := a.WithInstancePermit(true, func() error { called = true; return nil }); err != nil || !called {
		t.Fatal("resumed sender cannot send", err)
	}
	if err := a.update(func(tx *bolt.Tx) error {
		changed, err := recoverExpiredSender(tx, before.Deadline.Add(time.Hour))
		if changed {
			t.Fatal("repeated expiry reset cancellation")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReleaseSafetyRetirementAndResumeHaveExactlyOneWinner(t *testing.T) {
	for i := 0; i < 8; i++ {
		a, _ := releaseFixture(t, "old", "active")
		b, _ := releaseFixture(t, "new", "standby")
		beginPair(t, a, b)
		controlRelease(t, a, "close-admission")
		controlRelease(t, a, "freeze")
		m := exportRelease(t, a, true)
		importRelease(t, b, m)
		retireCommand := commandFor(t, a, "retire")
		start := make(chan struct{})
		retired := make(chan error, 1)
		go func() { <-start; _, err := a.ControlRelease(retireCommand); retired <- err }()
		type resumeResult struct {
			changed bool
			err     error
		}
		resumed := make(chan resumeResult, 1)
		go func() {
			<-start
			changed, err := a.ResumeUnretiredSender(context.Background(), retireCommand.OperationID, retireCommand.PeerID, time.Now().Add(50*time.Millisecond))
			resumed <- resumeResult{changed, err}
		}()
		close(start)
		retireErr, resume := <-retired, <-resumed
		st, _ := a.ReleaseStatus()
		if resume.err != nil {
			t.Fatal(resume.err)
		}
		if st.Phase == "retired" {
			if retireErr != nil || resume.changed || st.RetirementProof == nil {
				t.Fatal("retirement did not remain authoritative", retireErr, resume, st)
			}
			activateRelease(t, b, st.RetirementProof)
		} else if st.Phase == "cancelling" {
			if !resume.changed || retireErr == nil || st.RetirementProof != nil {
				t.Fatal("resume allowed a concurrent retirement proof", retireErr, resume, st)
			}
		} else {
			t.Fatal("unexpected concurrent outcome", st)
		}
		active := 0
		for _, s := range []*Store{a, b} {
			if err := s.WithInstancePermit(true, func() error { active++; return nil }); err != nil && !errors.Is(err, ErrReleaseGate) {
				t.Fatal(err)
			}
		}
		if active != 1 {
			t.Fatal("concurrent resume/retire admitted multiple or zero senders", active, st)
		}
		if err := a.update(func(tx *bolt.Tx) error {
			changed, err := recoverExpiredSender(tx, retireCommand.Deadline.Add(time.Second))
			if changed {
				t.Fatal("expiry revived a retired/cancelling sender")
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		_ = a.Close()
		_ = b.Close()
	}
}

func TestReleaseSafetyOriginalRequestReserveDoesNotRenewDeadline(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	beginPair(t, a, b)
	controlRelease(t, a, "close-admission")
	before := controlRelease(t, a, "freeze")
	if changed, err := a.ResumeUnretiredSender(context.Background(), before.OperationID, before.PeerID, time.Now().Add(time.Second)); err != nil || changed {
		t.Fatal("unused caller budget cancelled normal handoff", changed, err)
	}
	if changed, err := a.ResumeUnretiredSender(context.Background(), "wrong-operation", before.PeerID, time.Now().Add(20*time.Millisecond)); err != nil || changed {
		t.Fatal("foreign operation cancelled release", changed, err)
	}
	if changed, err := a.ResumeUnretiredSender(context.Background(), before.OperationID, before.PeerID, time.Now().Add(20*time.Millisecond)); err != nil || !changed {
		t.Fatal("original request reserve did not resume old sender", changed, err)
	}
	after, _ := a.ReleaseStatus()
	if after.Phase != "cancelling" || after.Revision != before.Revision+1 || !after.Deadline.Equal(before.Deadline) {
		t.Fatal(after)
	}
}
