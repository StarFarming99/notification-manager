package notification_spool

import (
	"encoding/json"
	"errors"
	bolt "go.etcd.io/bbolt"
	"testing"
	"time"
)

func beginTimedPair(t *testing.T, a, b *Store, duration time.Duration) time.Time {
	t.Helper()
	as, _ := a.ReleaseStatus()
	bs, _ := b.ReleaseStatus()
	deadline := time.Now().UTC().Add(duration)
	for _, row := range []struct {
		s            *Store
		state        ReleaseState
		action, peer string
	}{{a, as, "begin-active", bs.InstanceID}, {b, bs, "begin-standby", as.InstanceID}} {
		_, err := row.s.ControlRelease(ReleaseCommand{OperationID: "release-test", ExpectedRevision: row.state.Revision, Action: row.action, PeerID: row.peer, Deadline: deadline, MaxSendDelayMillis: duration.Milliseconds(), SyncRemainingMillis: 1, Actor: "test", Evidence: "isolated fixed deadline"})
		if err != nil {
			t.Fatal(err)
		}
	}
	return deadline
}

func TestReleaseCancelReturnsBufferWithoutRetiringUnknownOwner(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	bad, _ := submitGroup(t, a, "uncertain", "A", time.Hour)
	claim, err := a.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Finish(claim, Unknown, "platform_timeout", "original-unknown-attempt", time.Time{}); err != nil {
		t.Fatal(err)
	}
	deadline := beginTimedPair(t, a, b, 100*time.Millisecond)
	preseed(t, a, b)
	buffered, _ := submitGroup(t, b, "buffered", "B", time.Hour)
	// A's copy advances after preseed; B must not restore that stale copy.
	local, _ := submitGroup(t, a, "local", "C", time.Hour)
	controlRelease(t, a, "close-admission")
	controlRelease(t, a, "freeze")
	time.Sleep(time.Until(deadline) + 5*time.Millisecond)
	controlRelease(t, a, "cancel")
	if err = a.AdmissionAllowed(false); err != nil {
		t.Fatal("last sender did not reopen", err)
	}
	if got := finishOne(t, a); got.Target.IntakeID != local.ID {
		t.Fatal("old pending task stopped")
	}
	controlRelease(t, b, "close-admission")
	returned := exportRelease(t, b, true)
	if !returned.BufferedOnly || len(returned.Buckets["intakes"]) != 1 || returned.Buckets["intakes"][buffered.ID] == nil || returned.Buckets["intakes"][bad.ID] != nil {
		t.Fatal("buffer return includes stale predecessor", returned)
	}
	importRelease(t, a, returned)
	proof := controlRelease(t, b, "retire").RetirementProof
	cmd := commandFor(t, a, "cancel-complete")
	cmd.Proof = proof
	if _, err = a.ControlRelease(cmd); err != nil {
		t.Fatal(err)
	}
	if got := finishOne(t, a); got.Target.IntakeID != buffered.ID {
		t.Fatal("buffer return missing")
	}
	_, targets, _ := a.Snapshot()
	for _, target := range targets {
		if target.IntakeID == bad.ID && (target.State != Unknown || target.Attempts != claim.Target.Attempts || target.AttemptID != claim.AttemptID) {
			t.Fatal("unknown replayed or rewritten", target)
		}
	}
	state, _ := a.ReleaseStatus()
	if state.Phase != "active" || !state.DeadlineExceeded || !state.Deadline.Equal(deadline) || state.RetirementProof != nil {
		t.Fatal("cancel changed sending right or deadline", state)
	}
	if _, err = b.Claim(time.Now()); !errors.Is(err, ErrReleaseGate) {
		t.Fatal("cancelled successor can send", err)
	}
	if _, err = a.Claim(time.Now()); !errors.Is(err, ErrEmpty) {
		t.Fatal("unknown or stale copy scheduled", err)
	}
	if _, err = a.ControlRelease(commandFor(t, a, "cancel")); err == nil {
		t.Fatal("completed cancellation reopened old transfer")
	}
}

func TestReleaseTransferPreservesHistoricalFailuresAndReadyWork(t *testing.T) {
	for _, state := range []string{Pending, Retryable, Unknown, DeadLetter} {
		t.Run(state, func(t *testing.T) {
			a, _ := releaseFixture(t, "old", "active")
			defer a.Close()
			b, _ := releaseFixture(t, "new", "standby")
			defer b.Close()
			in, _ := submitGroup(t, a, "old-work", "A", time.Hour)
			var old Claim
			if state != Pending {
				var err error
				old, err = a.Claim(time.Now())
				if err != nil {
					t.Fatal(err)
				}
				if err = a.Finish(old, state, "historical-failure", "preserve-evidence", time.Now().Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
			}
			beginPair(t, a, b)
			preseed(t, a, b)
			finalHandoff(t, a, b)
			_, targets, _ := b.Snapshot()
			found := false
			for _, target := range targets {
				if target.IntakeID == in.ID {
					found = true
					if target.State != state {
						t.Fatal("state rewritten", target)
					}
					if state == Unknown && target.AttemptID != old.AttemptID {
						t.Fatal("attempt changed")
					}
				}
			}
			if !found {
				t.Fatal("historical fact lost")
			}
			if state == Pending {
				finishOne(t, b)
			}
			fresh, _ := submitGroup(t, b, "new-work", "fresh", time.Hour)
			if got := finishOne(t, b); got.Target.IntakeID != fresh.ID {
				t.Fatal("failure stopped unrelated notification")
			}
		})
	}
}

func TestReleaseLateProofRecoversWithoutResettingDeadline(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	deadline := beginTimedPair(t, a, b, 100*time.Millisecond)
	preseed(t, a, b)
	controlRelease(t, a, "close-admission")
	importRelease(t, b, exportRelease(t, a, true))
	proof := controlRelease(t, a, "retire").RetirementProof
	time.Sleep(time.Until(deadline) + 5*time.Millisecond)
	activateRelease(t, b, proof)
	state, _ := b.ReleaseStatus()
	if !state.Deadline.Equal(deadline) || !state.DeadlineExceeded {
		t.Fatal("late recovery invented a fresh budget", state)
	}
	submitGroup(t, b, "fresh", "notification", time.Hour)
	finishOne(t, b)
	if err := a.AdmissionAllowed(false); !errors.Is(err, ErrReleaseGate) {
		t.Fatal("retired sender revived", err)
	}
	forged := *proof
	forged.FenceRevision++
	command := commandFor(t, a, "cancel")
	command.Proof = &forged
	if _, err := a.ControlRelease(command); err == nil {
		t.Fatal("retired sender cancelled")
	}
}

func TestReleaseUnknownDoesNotBlockNewStartsOrOtherDestinations(t *testing.T) {
	s, _ := releaseFixture(t, "old", "active")
	defer s.Close()
	first, _ := submitGroup(t, s, "same-group", "first", time.Hour)
	claim, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(claim, Unknown, "unknown", "original", time.Time{}); err != nil {
		t.Fatal(err)
	}
	second, _ := submitGroup(t, s, "same-group", "changed", time.Hour)
	if got := finishOne(t, s); got.Target.IntakeID != second.ID || got.Target.IntakeID == first.ID {
		t.Fatal("historical unknown blocked new notification")
	}
	// Active pending/sending ordering is local to a destination.
	pending, _ := submitGroup(t, s, "scope-group", "pending", time.Hour)
	next, _ := submitGroup(t, s, "scope-group", "next", time.Hour)
	c1, err := s.Claim(time.Now())
	if err != nil || c1.Target.IntakeID != pending.ID {
		t.Fatal(err)
	}
	if err = s.update(func(tx *bolt.Tx) error {
		var nextTarget Target
		if err := json.Unmarshal(tx.Bucket([]byte("targets")).Get([]byte(next.TargetIDs[0])), &nextTarget); err != nil {
			return err
		}
		blocked, err := groupDeliveryBlocked(tx, nextTarget)
		if err != nil {
			return err
		}
		if !blocked {
			t.Fatal("same destination overtook active prior attempt")
		}
		nextTarget.Destination = "independent-destination"
		blocked, err = groupDeliveryBlocked(tx, nextTarget)
		if err != nil {
			return err
		}
		if blocked {
			t.Fatal("other destination blocked")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReleaseRepeatedCancelDoesNotLoseReturnedBufferProof(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	beginPair(t, a, b)
	preseed(t, a, b)
	submitGroup(t, b, "one", "buffer", time.Hour)
	controlRelease(t, a, "cancel")
	controlRelease(t, b, "close-admission")
	manifest := exportRelease(t, b, true)
	importRelease(t, a, manifest)
	controlRelease(t, a, "cancel")
	state, _ := a.ReleaseStatus()
	if state.ImportedChecksum != manifest.SnapshotChecksum {
		t.Fatal("repeat cancel erased imported proof")
	}
	repeated := exportRelease(t, b, true)
	if repeated.SnapshotChecksum != manifest.SnapshotChecksum || !repeated.ExportedAt.Equal(manifest.ExportedAt) {
		t.Fatal("second final export drifted")
	}
	proof := controlRelease(t, b, "retire").RetirementProof
	c := commandFor(t, a, "cancel-complete")
	c.Proof = proof
	if _, err := a.ControlRelease(c); err != nil {
		t.Fatal(err)
	}
	finishOne(t, a)
}

func TestReleaseCancelCanRecoverPeerBeginBeforeOldBegin(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	bs, _ := b.ReleaseStatus()
	deadline := time.Now().UTC().Add(time.Second)
	cmd := ReleaseCommand{OperationID: "partial-begin", ExpectedRevision: bs.Revision, Action: "begin-standby", PeerID: "old", Deadline: deadline, MaxSendDelayMillis: 1000, SyncRemainingMillis: 10, Actor: "test", Evidence: "old begin response not issued"}
	if _, err := b.ControlRelease(cmd); err != nil {
		t.Fatal(err)
	}
	submitGroup(t, b, "one", "buffer", time.Hour)
	as, _ := a.ReleaseStatus()
	cmd.ExpectedRevision = as.Revision
	cmd.Action = "cancel"
	cmd.PeerID = "new"
	if _, err := a.ControlRelease(cmd); err != nil {
		t.Fatal(err)
	}
	controlRelease(t, b, "close-admission")
	importRelease(t, a, exportRelease(t, b, true))
	proof := controlRelease(t, b, "retire").RetirementProof
	finish := commandFor(t, a, "cancel-complete")
	finish.Proof = proof
	if _, err := a.ControlRelease(finish); err != nil {
		t.Fatal(err)
	}
	finishOne(t, a)
}

func TestReleaseAtomicCancelImportPreservesLocalOutcomeAndRejectsAttempts(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	beginPair(t, a, b)
	preseed(t, a, b)
	in, _ := submitGroup(t, b, "one", "buffer", time.Hour)
	controlRelease(t, b, "close-admission")
	manifest := exportRelease(t, b, true)
	// Even a correctly authenticated manifest cannot return an attempted row.
	alteredBytes, _ := json.Marshal(manifest)
	var bad ReleaseManifest
	if err := json.Unmarshal(alteredBytes, &bad); err != nil {
		t.Fatal(err)
	}
	var target Target
	if err := json.Unmarshal(bad.Buckets["targets"][in.TargetIDs[0]], &target); err != nil {
		t.Fatal(err)
	}
	target.Attempts = 1
	target.State = Unknown
	target.AttemptID = "foreign-uncertain"
	bad.Buckets["targets"][target.ID], _ = json.Marshal(target)
	bad.SnapshotChecksum = manifestChecksum(bad)
	bad.MAC = ""
	bad.MAC = b.releaseMAC(bad)
	before, _ := a.ReleaseStatus()
	cancel := commandFor(t, a, "cancel-import")
	if _, err := a.ImportRelease(bad, cancel); err == nil {
		t.Fatal("attempted buffer returned")
	}
	after, _ := a.ReleaseStatus()
	if after.Phase != before.Phase || after.Revision != before.Revision {
		t.Fatal("failed atomic return changed old sender")
	}
	if _, err := a.ImportRelease(manifest, cancel); err != nil {
		t.Fatal(err)
	}
	sent := finishOne(t, a)
	if _, err := a.ImportRelease(manifest, commandFor(t, a, "import")); err != nil {
		t.Fatal(err)
	}
	_, targets, _ := a.Snapshot()
	for _, row := range targets {
		if row.ID == sent.Target.ID && (row.State != Delivered || row.AttemptID != sent.AttemptID || row.Attempts != 1) {
			t.Fatal("buffer retry reset local outcome", row)
		}
	}
	proof := controlRelease(t, b, "retire").RetirementProof
	finish := commandFor(t, a, "cancel-complete")
	finish.Proof = proof
	if _, err := a.ControlRelease(finish); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Claim(time.Now()); !errors.Is(err, ErrEmpty) {
		t.Fatal("returned buffer replayed", err)
	}
}
