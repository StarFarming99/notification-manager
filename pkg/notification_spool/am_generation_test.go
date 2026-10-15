package notification_spool

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestAMGroupDeliveredStateReturnCreatesNewGeneration(t *testing.T) {
	s, path, _ := profileFixture(t)
	snap, _ := s.ProfileSnapshot("test")
	seen := map[string]bool{}
	for _, content := range []string{"AB", "BC", "BC-resolved", "AB"} {
		intake, dup, err := s.SubmitAMProfile("test:v1:am-group:g", content, []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
		if err != nil || dup || seen[intake.ID] {
			t.Fatalf("valid group change swallowed: content=%s dup=%v err=%v", content, dup, err)
		}
		seen[intake.ID] = true
		peer, dup, err := s.SubmitAMProfile("test:v1:am-group:g", content, []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
		if err != nil || !dup || peer.ID != intake.ID {
			t.Fatal("same-generation peer duplicated", dup, err)
		}
		claim, err := s.Claim(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Finish(claim, Delivered, "confirmed", "mock", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	again, dup, err := s.SubmitAMProfile("test:v1:am-group:g", "AB", []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
	if err != nil || !dup || !seen[again.ID] {
		t.Fatal("current generation lost after restart", dup, err)
	}
}
func TestAMGroupInterleavedUnknownReturnIsNewCandidateNotOldAttemptReplay(t *testing.T) {
	s, _, _ := profileFixture(t)
	defer s.Close()
	snap, _ := s.ProfileSnapshot("test")
	a, _, _ := s.SubmitAMProfile("group", "A", []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
	b, dup, err := s.SubmitAMProfile("group", "B", []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
	if err != nil || dup || a.ID == b.ID {
		t.Fatal(err)
	}
	late, dup, err := s.SubmitAMProfile("group", "A", []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
	if err != nil || dup || late.ID == a.ID || late.AmbiguityReason != "content_return_or_delayed_peer" || len(late.PossibleDuplicateOf) != 1 || late.PossibleDuplicateOf[0] != a.ID {
		t.Fatal("ambiguous return was not independently accepted with provenance", dup, err, late)
	}
	peer, dup, err := s.SubmitAMProfile("group", "A", []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
	if err != nil || !dup || peer.ID != late.ID {
		t.Fatal("certain current duplicate was not deduplicated", dup, err)
	}
	claim, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(claim, Unknown, "response_lost", "mock", time.Now()); err != nil {
		t.Fatal(err)
	}
	var sends []string
	for range 2 {
		c, err := s.Claim(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		sends = append(sends, c.Target.IntakeID)
		if err := s.Finish(c, Delivered, "confirmed", "new-candidate-receipt", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if sends[0] == a.ID || sends[1] == a.ID || sends[0] == sends[1] {
		t.Fatal("old unknown attempt replayed", sends)
	}
	if _, err := s.Claim(time.Now()); !errors.Is(err, ErrEmpty) {
		t.Fatal("extra send queued", err)
	}
}

func TestAMConcurrentPeersUseOneGenerationAndSeparateGroups(t *testing.T) {
	s, _, _ := profileFixture(t)
	defer s.Close()
	snap, _ := s.ProfileSnapshot("test")
	var wait sync.WaitGroup
	ids := make(chan string, 20)
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			intake, _, err := s.SubmitAMProfile("group-one", "A", []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
			ids <- intake.ID
			errs <- err
		}()
	}
	wait.Wait()
	close(ids)
	close(errs)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatal("concurrent peer duplicated")
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	other, dup, err := s.SubmitAMProfile("group-two", "A", []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
	if err != nil || dup || other.ID == first {
		t.Fatal("group identity merged", dup, err)
	}
}

func TestLegacyAMGenerationSeedRequiresPausedCASAndRetainsExactWork(t *testing.T) {
	s, _, _ := profileFixture(t)
	defer s.Close()
	snap, _ := s.ProfileSnapshot("test")
	hash, group := Hash([]byte("original")), Hash([]byte("group"))
	intake, _, err := s.SubmitProfile("test:v1:am:"+hash, hash, []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SeedAMGroup("test", "v1", group, intake.ID, hash, snap.Revision, "operator", "exact saved last group receipt"); !errors.Is(err, ErrProfileConflict) {
		t.Fatal("unpaused migration admitted", err)
	}
	state, err := s.ControlProfile("test", "pause", "", snap.Revision, "operator", "upgrade pause")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SeedAMGroup("test", "v1", group, intake.ID, Hash([]byte("wrong")), state.Revision, "operator", "wrong"); err == nil {
		t.Fatal("wrong intent migration admitted")
	}
	if err := s.SeedAMGroup("test", "v1", group, intake.ID, hash, state.Revision, "operator", "exact saved last group receipt"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ControlProfile("test", "resume", "", state.Revision, "operator", "indexed without replay"); err != nil {
		t.Fatal(err)
	}
	snap, _ = s.ProfileSnapshot("test")
	again, dup, err := s.SubmitAMProfile("test:v1:am-group:"+group, hash, []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
	if err != nil || !dup || again.ID != intake.ID {
		t.Fatal("upgrade duplicated accepted intent", dup, err)
	}
	claim, err := s.Claim(time.Now())
	if err != nil || claim.Target.IntakeID != intake.ID {
		t.Fatal("seed changed original work", err)
	}
}

// AM v0.23 webhook has no monotonically increasing group sequence. Once A and
// B are both delivered, a late peer's old A has exactly the same observable
// request as a valid unsilence/state-return A. Preserve the valid update; this
// test documents that arbitrary delayed delivered-peer exactly-once cannot be
// proven. The accepted return is retained as a separately auditable candidate;
// a possible boundary duplicate does not block unrelated notifications.
func TestDeliveredLatePeerIndistinguishableFromLegitimateReturn(t *testing.T) {
	s, _, _ := profileFixture(t)
	defer s.Close()
	snap, _ := s.ProfileSnapshot("test")
	var first Intake
	for _, content := range []string{"A", "B"} {
		intake, dup, err := s.SubmitAMProfile("group", content, []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
		if err != nil || dup {
			t.Fatal(err)
		}
		if content == "A" {
			first = intake
		}
		claim, err := s.Claim(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Finish(claim, Delivered, "confirmed", "fixture", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	late, dup, err := s.SubmitAMProfile("group", "A", []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
	if err != nil || dup || late.ID == first.ID {
		t.Fatal("valid return would be swallowed", dup, err)
	}
}

func TestLegacyUpgradePreservesHistoricalUnknownAndAcceptsIndependentReturn(t *testing.T) {
	s, _, _ := profileFixture(t)
	defer s.Close()
	snap, _ := s.ProfileSnapshot("test")
	aHash, bHash, groupHash := Hash([]byte("A")), Hash([]byte("B")), Hash([]byte("group"))
	a, _, err := s.SubmitProfile("test:v1:am:"+aHash, aHash, []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	aClaim, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(aClaim, Unknown, "response_loss", "old exact A", time.Now()); err != nil {
		t.Fatal(err)
	}
	b, _, err := s.SubmitProfile("test:v1:am:"+bHash, bHash, []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	bClaim, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(bClaim, Delivered, "confirmed", "old exact B", time.Now()); err != nil {
		t.Fatal(err)
	}
	paused, err := s.ControlProfile("test", "pause", "", snap.Revision, "operator", "upgrade proof")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SeedAMGroup("test", "v1", groupHash, b.ID, bHash, paused.Revision, "operator", "proven latest B"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ControlProfile("test", "resume", "", paused.Revision, "operator", "seed complete"); err != nil {
		t.Fatal(err)
	}
	snap, _ = s.ProfileSnapshot("test")
	fresh, dup, err := s.SubmitAMProfile("test:v1:am-group:"+groupHash, aHash, []Target{profileTarget(snap)}, "", snap, 12*time.Hour)
	if err != nil || dup || fresh.ID == a.ID || len(fresh.PossibleDuplicateOf) != 1 || fresh.PossibleDuplicateOf[0] != a.ID {
		t.Fatal("valid legacy return swallowed or provenance lost", dup, err)
	}
	c, err := s.Claim(time.Now())
	if err != nil || c.Target.IntakeID != fresh.ID {
		t.Fatal("new candidate not independently sendable", err)
	}
	if err = s.Finish(c, Delivered, "confirmed", "new return", time.Now()); err != nil {
		t.Fatal(err)
	}
	_, rows, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.IntakeID == a.ID && (row.State != Unknown || row.Attempts != 1 || row.AttemptID != aClaim.AttemptID) {
			t.Fatal("legacy unknown facts mutated", row)
		}
	}
}
