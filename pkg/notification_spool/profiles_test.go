package notification_spool

import (
	"encoding/json"
	"errors"
	bolt "go.etcd.io/bbolt"
	"path/filepath"
	"testing"
	"time"
)

func profileFixture(t *testing.T) (*Store, string, []DeliveryProfile) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spool.db")
	s, err := Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	profiles := []DeliveryProfile{{ID: "test", Version: "v1", CardOwnerID: "test-owner", Receiver: "critical", SourceChatID: "production", TestChatID: "test"}, {ID: "formal", Version: "v1", CardOwnerID: "formal-owner"}}
	if err := s.RegisterProfiles(profiles, "v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ControlProfile("test", "prepare", "v1", 1, "fixture", "verified fixture"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ControlProfile("test", "activate", "v1", 2, "fixture", "verified fixture"); err != nil {
		t.Fatal(err)
	}
	return s, path, profiles
}
func profileTarget(snap ProfileSnapshot) Target {
	return Target{ProfileID: snap.ID, ProfileVersion: snap.Version, CardOwnerID: snap.CardOwnerID, OriginalDestination: "chat:production", Receiver: "critical", Channel: "feishu", Destination: "chat:test", ContentRevision: "frozen", Payload: json.RawMessage(`{"frozen":true}`)}
}
func TestProfileCASPauseRestartAndFrozenHistory(t *testing.T) {
	s, path, profiles := profileFixture(t)
	snap, err := s.ProfileSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	intake, _, err := s.SubmitProfile("intent", "hash", []Target{profileTarget(snap)}, "", snap, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	st, err := s.ControlProfile("test", "pause", "", 3, "operator", "freeze test lane")
	if err != nil || !st.Paused {
		t.Fatal(st, err)
	}
	if _, _, err := s.SubmitProfile("late", "late", []Target{profileTarget(snap)}, "", snap, 0); !errors.Is(err, ErrProfilePaused) {
		t.Fatal("pause racing intake admitted", err)
	}
	if _, err := s.Claim(time.Now()); !errors.Is(err, ErrEmpty) {
		t.Fatal("paused work claimed", err)
	}
	if _, err := s.ClaimForRecovery(intake.TargetIDs[0], "operator", "recovery", time.Now()); err == nil {
		t.Fatal("recovery bypassed pause")
	}
	if _, err := s.ControlProfile("test", "resume", "", 3, "operator", "stale"); !errors.Is(err, ErrProfileConflict) {
		t.Fatal("CAS did not reject stale control", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.RegisterProfiles(profiles, "v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProfileSnapshot("test"); !errors.Is(err, ErrProfilePaused) {
		t.Fatal("restart reset pause", err)
	}
	if _, err := s.ControlProfile("formal", "activate", "v1", 1, "operator", "not prepared"); err == nil {
		t.Fatal("unprepared formal activated")
	}
	if _, err := s.ControlProfile("formal", "prepare", "v1", 1, "operator", "offline compatibility"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ControlProfile("formal", "activate", "v1", 2, "operator", "formal ready"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ControlProfile("test", "resume", "", 4, "operator", "finish historical test"); err != nil {
		t.Fatal(err)
	}
	claim, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if claim.Target.ProfileID != "test" || claim.Target.Destination != "chat:test" || claim.Target.ProfileVersion != "v1" {
		t.Fatal("promotion rewrote historical target", claim.Target)
	}
}
func TestProfileImmutableVersionAndFence(t *testing.T) {
	s, _, profiles := profileFixture(t)
	defer s.Close()
	profiles[0].TestChatID = "production"
	if err := s.RegisterProfiles(profiles, "v1"); err == nil {
		t.Fatal("profile version was rewritten")
	}
	snap, err := s.ProfileSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	target := profileTarget(snap)
	target.Destination = "chat:production"
	if _, _, err := s.SubmitProfile("bad", "bad", []Target{target}, "", snap, 0); err == nil {
		t.Fatal("destination fence bypassed")
	}
	_, targets, err := s.Snapshot()
	if err != nil || len(targets) != 0 {
		t.Fatal("rejected plan partially persisted", targets, err)
	}
}
func TestBoundedIntentDedupeSurvivesRestartAndAllowsRepeat(t *testing.T) {
	s, path, profiles := profileFixture(t)
	snap, _ := s.ProfileSnapshot("test")
	first, dup, err := s.SubmitProfile("am:stable", "stable", []Target{profileTarget(snap)}, "", snap, 80*time.Millisecond)
	if err != nil || dup {
		t.Fatal(err)
	}
	claim, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(claim, Delivered, "confirmed", "mock", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.RegisterProfiles(profiles, "v1"); err != nil {
		t.Fatal(err)
	}
	snap, _ = s.ProfileSnapshot("test")
	second, dup, err := s.SubmitProfile("am:stable", "stable", []Target{profileTarget(snap)}, "", snap, 80*time.Millisecond)
	if err != nil || !dup || second.ID != first.ID {
		t.Fatal("restart/ACK loss duplicated intent", second, dup, err)
	}
	time.Sleep(100 * time.Millisecond)
	third, dup, err := s.SubmitProfile("am:stable", "stable", []Target{profileTarget(snap)}, "", snap, 80*time.Millisecond)
	if err != nil || dup || third.ID == first.ID {
		t.Fatal("legitimate repeat swallowed", third, dup, err)
	}
}

func TestProlongedACKLossIsNotARepeatAndUncertainWorkNeverDuplicates(t *testing.T) {
	s, _, _ := profileFixture(t)
	defer s.Close()
	snap, _ := s.ProfileSnapshot("test")
	first, _, err := s.SubmitProfile("am:lost", "canonical", []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(claim, Delivered, "confirmed", "mock", time.Now()); err != nil {
		t.Fatal(err)
	}
	age := func(d time.Duration) {
		t.Helper()
		if err := s.db.Update(func(tx *bolt.Tx) error {
			first.AcceptedAt = time.Now().Add(-d)
			return put(tx.Bucket([]byte("intakes")), first.ID, first)
		}); err != nil {
			t.Fatal(err)
		}
	}
	age(5 * time.Minute)
	again, dup, err := s.SubmitProfile("am:lost", "canonical", []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
	if err != nil || !dup || again.ID != first.ID {
		t.Fatal("ACK loss beyond short retry window duplicated card", again, dup, err)
	}
	age(12*time.Hour + time.Second)
	repeated, dup, err := s.SubmitProfile("am:lost", "canonical", []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
	if err != nil || dup || repeated.ID == first.ID {
		t.Fatal("configured legitimate repeat swallowed", repeated, dup, err)
	}
	claim, err = s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(claim, Unknown, "response_lost", "mock", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		repeated.AcceptedAt = time.Now().Add(-24 * time.Hour)
		return put(tx.Bucket([]byte("intakes")), repeated.ID, repeated)
	}); err != nil {
		t.Fatal(err)
	}
	uncertain, dup, err := s.SubmitProfile("am:lost", "canonical", []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
	if err != nil || !dup || uncertain.ID != repeated.ID {
		t.Fatal("unknown target blindly opened another send", uncertain, dup, err)
	}
}
