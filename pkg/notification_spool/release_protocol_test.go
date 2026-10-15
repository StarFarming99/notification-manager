package notification_spool

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kubesphere/notification-manager/pkg/utils"
	bolt "go.etcd.io/bbolt"
)

var releaseTestKey = []byte("isolated-bluegreen-test-key-32bytes-minimum")

func releaseFixture(t *testing.T, id, mode string) (*Store, string) {
	t.Helper()
	s, path, _ := profileFixture(t)
	if err := s.ConfigureRelease(id, mode, releaseTestKey); err != nil {
		t.Fatal(err)
	}
	return s, path
}
func commandFor(t *testing.T, s *Store, action string) ReleaseCommand {
	t.Helper()
	st, err := s.ReleaseStatus()
	if err != nil {
		t.Fatal(err)
	}
	return ReleaseCommand{OperationID: st.OperationID, ExpectedRevision: st.Revision, Action: action, PeerID: st.PeerID, Deadline: st.Deadline, MaxSendDelayMillis: st.MaxSendDelayMillis, SyncRemainingMillis: st.SyncRemainingMillis, Actor: "isolated-test", Evidence: "mock-platform-evidence"}
}
func controlRelease(t *testing.T, s *Store, action string) ReleaseState {
	t.Helper()
	st, err := s.ControlRelease(commandFor(t, s, action))
	if err != nil {
		t.Fatalf("%s: %v", action, err)
	}
	return st
}
func beginPair(t *testing.T, a, b *Store) {
	t.Helper()
	as, _ := a.ReleaseStatus()
	bs, _ := b.ReleaseStatus()
	deadline := time.Now().UTC().Add(10 * time.Second)
	for _, entry := range []struct {
		s            *Store
		state        ReleaseState
		peer, action string
	}{{a, as, bs.InstanceID, "begin-active"}, {b, bs, as.InstanceID, "begin-standby"}} {
		_, err := entry.s.ControlRelease(ReleaseCommand{OperationID: "release-test", ExpectedRevision: entry.state.Revision, Action: entry.action, PeerID: entry.peer, Deadline: deadline, MaxSendDelayMillis: 9000, SyncRemainingMillis: 100, Actor: "isolated-test", Evidence: "frozen test operation"})
		if err != nil {
			t.Fatal(err)
		}
	}
}
func exportRelease(t *testing.T, s *Store, final bool) ReleaseManifest {
	t.Helper()
	st, _ := s.ReleaseStatus()
	if final && st.Phase == "draining" {
		controlRelease(t, s, "freeze")
		st, _ = s.ReleaseStatus()
	}
	m, err := s.ExportRelease(st.OperationID, st.Revision, final)
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func importRelease(t *testing.T, s *Store, m ReleaseManifest) {
	t.Helper()
	if _, err := s.ImportRelease(m, commandFor(t, s, "import")); err != nil {
		t.Fatal(err)
	}
}
func activateRelease(t *testing.T, s *Store, proof *RetirementProof) {
	t.Helper()
	c := commandFor(t, s, "activate")
	c.Proof = proof
	if _, err := s.ControlRelease(c); err != nil {
		t.Fatal(err)
	}
}
func submitGroup(t *testing.T, s *Store, key, hash string, window time.Duration) (Intake, bool) {
	t.Helper()
	snap, err := s.ProfileSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	in, dup, err := s.SubmitAMProfile(key, hash, []Target{profileTarget(snap)}, "", snap, window)
	if err != nil {
		t.Fatal(err)
	}
	return in, dup
}
func finishOne(t *testing.T, s *Store) Claim {
	t.Helper()
	c, err := s.Claim(time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.WithSendPermit(c, 0, time.Now().UTC(), func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(c, Delivered, "platform_confirmed", "mock-receipt:"+c.Target.IntakeID, time.Time{}); err != nil {
		t.Fatal(err)
	}
	return c
}
func preseed(t *testing.T, a, b *Store) { t.Helper(); importRelease(t, b, exportRelease(t, a, false)) }
func finalHandoff(t *testing.T, a, b *Store) *RetirementProof {
	t.Helper()
	controlRelease(t, a, "close-admission")
	m := exportRelease(t, a, true)
	importRelease(t, b, m)
	st := controlRelease(t, a, "retire")
	activateRelease(t, b, st.RetirementProof)
	return st.RetirementProof
}
func TestReleaseStandbyRetirementRestartAndOriginalProfileIsolation(t *testing.T) {
	a, path := releaseFixture(t, "old", "active")
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	original, _ := a.ProfileSnapshot("test")
	submitGroup(t, a, "group", "initial", time.Hour)
	finishOne(t, a)
	beginPair(t, a, b)
	preseed(t, a, b)
	in, _ := submitGroup(t, b, "group", "resolved", time.Hour)
	if err := b.AdmissionAllowed(true); !errors.Is(err, ErrReleaseGate) {
		t.Fatal("standby acknowledged synchronous send", err)
	}
	if _, err := b.Claim(time.Now()); !errors.Is(err, ErrReleaseGate) {
		t.Fatal("standby claimed", err)
	}
	proof := finalHandoff(t, a, b)
	c := finishOne(t, b)
	if c.Target.IntakeID != in.ID {
		t.Fatal("buffered lifecycle missing")
	}
	nowSnap, _ := a.ProfileSnapshot("test")
	if nowSnap != original {
		t.Fatal("instance control changed profile")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	a, err = Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err = a.Claim(time.Now()); !errors.Is(err, ErrReleaseGate) {
		t.Fatal("retirement lost after restart", err)
	}
	if err = a.AdmissionAllowed(false); !errors.Is(err, ErrReleaseGate) {
		t.Fatal("retired accepted", err)
	}
	if err = a.ConfigureRelease("old", "active", []byte(strings.Repeat("x", 32))); !errors.Is(err, ErrReleaseConflict) {
		t.Fatal("key changed", err)
	}
	if proof == nil || proof.MAC == "" {
		t.Fatal("unsigned retirement")
	}
}
func TestReleaseFrozenDeadlineCASReplayAndExportReadback(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	beginPair(t, a, b)
	preseed(t, a, b)
	c := commandFor(t, a, "close-admission")
	first, err := a.ControlRelease(c)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := a.ControlRelease(c)
	if err != nil || replay.Revision != first.Revision {
		t.Fatal("lost command response was not idempotent", err)
	}
	changed := commandFor(t, a, "close-admission")
	changed.Deadline = changed.Deadline.Add(time.Minute)
	if _, err := a.ControlRelease(changed); !errors.Is(err, ErrReleaseConflict) {
		t.Fatal("deadline reset", err)
	}
	controlRelease(t, a, "freeze")
	st, _ := a.ReleaseStatus()
	m, err := a.ExportRelease(st.OperationID, st.Revision, true)
	if err != nil {
		t.Fatal(err)
	}
	repeat, err := a.ExportRelease(st.OperationID, st.Revision, true)
	if err != nil || m.SnapshotChecksum != repeat.SnapshotChecksum || !m.ExportedAt.Equal(repeat.ExportedAt) {
		t.Fatal("export retry drifted", err)
	}
	readback, err := a.ReadReleaseExport(st.OperationID, m.SnapshotChecksum)
	if err != nil || readback.MAC != m.MAC {
		t.Fatal("exact export unavailable", err)
	}
}
func TestReleaseVerifiedTailOverlapDoesNotReplayAndPreservesLaterGeneration(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	submitGroup(t, a, "group", "base", time.Hour)
	finishOne(t, a)
	beginPair(t, a, b)
	preseed(t, a, b)
	submitGroup(t, a, "group", "tail", time.Hour)
	finishOne(t, a)
	tail, _ := submitGroup(t, b, "group", "tail", time.Hour)
	later, _ := submitGroup(t, b, "group", "resolved", time.Hour)
	finalHandoff(t, a, b)
	_, rows, _ := b.Snapshot()
	found := false
	for _, row := range rows {
		if row.IntakeID == tail.ID {
			found = row.State == Delivered && row.Outcome == "handoff_confirmed_duplicate" && row.Evidence != ""
		}
	}
	if !found {
		t.Fatal("exact duplicate was not tied to predecessor receipt")
	}
	c := finishOne(t, b)
	if c.Target.IntakeID != later.ID {
		t.Fatal("tail replayed or later generation swallowed")
	}
	if _, err := b.Claim(time.Now()); !errors.Is(err, ErrEmpty) {
		t.Fatal("extra send", err)
	}
}
func TestReleaseAmbiguousBufferedReturnIsDurablyAcceptedWithEvidence(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	first, _ := submitGroup(t, a, "group", "A", time.Hour)
	finishOne(t, a)
	beginPair(t, a, b)
	preseed(t, a, b)
	submitGroup(t, b, "group", "B", time.Hour)
	before, _, _ := b.Snapshot()
	returned, dup := submitGroup(t, b, "group", "A", time.Hour)
	after, _, _ := b.Snapshot()
	if dup || len(after) != len(before)+1 || returned.AmbiguityReason == "" || len(returned.PossibleDuplicateOf) != 1 || returned.PossibleDuplicateOf[0] != first.ID {
		t.Fatal("ambiguous candidate not durably accepted", returned, dup)
	}
	finalHandoff(t, a, b)
	finishOne(t, b)
	if c := finishOne(t, b); c.Target.IntakeID != returned.ID {
		t.Fatal("accepted return lost")
	}
}

func TestReleaseUnprovenTailOrderingRetainsBothCandidatesWithoutBlocking(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	submitGroup(t, a, "group", "base", time.Hour)
	finishOne(t, a)
	beginPair(t, a, b)
	preseed(t, a, b)
	submitGroup(t, a, "group", "old-tail", time.Hour)
	finishOne(t, a)
	local, _ := submitGroup(t, b, "group", "new-tail", time.Hour)
	finalHandoff(t, a, b)
	if c := finishOne(t, b); c.Target.IntakeID != local.ID {
		t.Fatal("uncertain tail was swallowed")
	}
	intakes, _, _ := b.Snapshot()
	for _, in := range intakes {
		if in.ID == local.ID && in.AmbiguityReason != "cross_instance_order_unproven" {
			t.Fatal("missing uncertain ordering provenance", in)
		}
	}
}

func TestReleaseOnlyPhysicalInFlightCannotRetire(t *testing.T) {
	for _, state := range []string{Pending, Retryable, Unknown, Sending} {
		t.Run(state, func(t *testing.T) {
			a, _ := releaseFixture(t, "old", "active")
			defer a.Close()
			b, _ := releaseFixture(t, "new", "standby")
			defer b.Close()
			beginPair(t, a, b)
			preseed(t, a, b)
			submitGroup(t, a, "group", "body", time.Hour)
			if state != Pending {
				c, err := a.Claim(time.Now())
				if err != nil {
					t.Fatal(err)
				}
				if state != Sending {
					if err = a.Finish(c, state, "injected_platform_result", "mock-failure", time.Now().Add(time.Hour)); err != nil {
						t.Fatal(err)
					}
				}
			}
			controlRelease(t, a, "close-admission")
			controlRelease(t, a, "freeze")
			st, _ := a.ReleaseStatus()
			_, err := a.ExportRelease(st.OperationID, st.Revision, true)
			if state == Sending {
				if err == nil {
					t.Fatal("physical in-flight exported")
				}
				if _, err = a.ControlRelease(commandFor(t, a, "retire")); err == nil {
					t.Fatal("physical in-flight retired")
				}
			} else {
				if err != nil {
					t.Fatal("historical work blocked transfer", err)
				}
				controlRelease(t, a, "retire")
			}
		})
	}
}
func TestReleaseReverseHandoffPreservesAllBufferedIntakes(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	submitGroup(t, a, "one", "already-sent", time.Hour)
	finishOne(t, a)
	beginPair(t, a, b)
	preseed(t, a, b)
	pending, _ := submitGroup(t, b, "two", "buffered", time.Hour)
	controlRelease(t, a, "close-admission")
	importRelease(t, b, exportRelease(t, a, true))
	controlRelease(t, a, "retire")
	controlRelease(t, b, "close-admission")
	reverse := exportRelease(t, b, true)
	bp := controlRelease(t, b, "retire")
	controlRelease(t, a, "rollback-standby")
	if err := a.AdmissionAllowed(false); err != nil {
		t.Fatal("rollback receiver unavailable", err)
	}
	if err := a.AdmissionAllowed(true); !errors.Is(err, ErrReleaseGate) {
		t.Fatal("rollback syncbuffer false success", err)
	}
	importRelease(t, a, reverse)
	activateRelease(t, a, bp.RetirementProof)
	c := finishOne(t, a)
	if c.Target.IntakeID != pending.ID {
		t.Fatal("reverse transfer lost buffered target")
	}
	if _, err := a.Claim(time.Now()); !errors.Is(err, ErrEmpty) {
		t.Fatal("old delivered replayed", err)
	}
}
func TestReleaseManifestTamperProtocolAndForgedProofRejected(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	beginPair(t, a, b)
	m := exportRelease(t, a, false)
	m.Buckets["dedupe"]["tamper"] = []byte("id")
	if _, err := b.ImportRelease(m, commandFor(t, b, "import")); err == nil {
		t.Fatal("tampered manifest accepted")
	}
	preseed(t, a, b)
	controlRelease(t, a, "close-admission")
	m = exportRelease(t, a, true)
	importRelease(t, b, m)
	p := controlRelease(t, a, "retire").RetirementProof
	p.MAC = strings.Repeat("0", 64)
	c := commandFor(t, b, "activate")
	c.Proof = p
	if _, err := b.ControlRelease(c); err == nil {
		t.Fatal("forged retirement accepted")
	}
}
func TestReleasePersistentSynchronousUncertaintyMigratesWithoutReplay(t *testing.T) {
	a, path := releaseFixture(t, "old", "active")
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	if err := a.WithInstancePermit(true, func() error { return &utils.TransportError{Err: errors.New("mock timeout")} }); err == nil {
		t.Fatal("sync failure became success")
	}
	rows, _ := a.SynchronousAttempts()
	if len(rows) != 1 || rows[0].State != Unknown {
		t.Fatal("uncertainty missing", rows)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	a, err = Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err = a.ConfigureRelease("old", "active", releaseTestKey); err != nil {
		t.Fatal(err)
	}
	as, _ := a.ReleaseStatus()
	if as.SynchronousUnresolved != 1 {
		t.Fatal("sync unknown lost on restart")
	}
	deadline := time.Now().Add(time.Second)
	if _, err = a.ControlRelease(ReleaseCommand{OperationID: "transfer-unknown", ExpectedRevision: as.Revision, Action: "begin-active", PeerID: "new", Deadline: deadline, MaxSendDelayMillis: 900, SyncRemainingMillis: 100, Actor: "test", Evidence: "mock"}); err != nil {
		t.Fatal("historical sync unknown blocked future release", err)
	}
	if err = a.ResolveSynchronousAttempt(rows[0].ID, "test", "mock platform receipt manually checked", true); err != nil {
		t.Fatal(err)
	}
	if attempts, _ := a.SynchronousAttempts(); len(attempts) != 1 || attempts[0].ID != rows[0].ID {
		t.Fatal("original synchronous attempt disappeared")
	}
}
func TestReleaseSameGroupDeliveryDoesNotOvertakeFiring(t *testing.T) {
	s, _ := releaseFixture(t, "active", "active")
	defer s.Close()
	first, _ := submitGroup(t, s, "group", "firing", time.Hour)
	second, _ := submitGroup(t, s, "group", "resolved", time.Hour)
	c, err := s.Claim(time.Now())
	if err != nil || c.Target.IntakeID != first.ID {
		t.Fatal("first lifecycle missing", err)
	}
	if _, err = s.Claim(time.Now()); !errors.Is(err, ErrEmpty) {
		t.Fatal("resolved overtook firing", err)
	}
	if err = s.Finish(c, Delivered, "confirmed", "mock", time.Time{}); err != nil {
		t.Fatal(err)
	}
	next := finishOne(t, s)
	if next.Target.IntakeID != second.ID {
		t.Fatal("resolved not released")
	}
}
func TestReleaseExpiredBudgetDoesNotRevokeBusinessPermits(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	beginPair(t, a, b)
	if err := b.update(func(tx *bolt.Tx) error {
		st, _ := releaseRead(tx)
		st.StartedAt = time.Now().Add(-20 * time.Second)
		return releasePut(tx, st)
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.AdmissionAllowed(false); err != nil {
		t.Fatal("budget revoked durable admission", err)
	}
	if state, _ := b.ReleaseStatus(); !state.DeadlineExceeded {
		t.Fatal("budget miss not visible")
	}
	if err := a.update(func(tx *bolt.Tx) error {
		st, _ := releaseRead(tx)
		st.Deadline = time.Now().Add(30 * time.Millisecond)
		return releasePut(tx, st)
	}); err != nil {
		t.Fatal(err)
	}
	if err := a.AdmissionAllowed(true); err != nil {
		t.Fatal("release budget revoked legitimate sender", err)
	}
}
func TestReleaseFreshRepeatAfterWindowIsRetained(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	first, _ := submitGroup(t, a, "group", "same", time.Millisecond)
	finishOne(t, a)
	beginPair(t, a, b)
	preseed(t, a, b)
	time.Sleep(3 * time.Millisecond)
	repeat, duplicate := submitGroup(t, b, "group", "same", time.Millisecond)
	if duplicate || repeat.ID == first.ID {
		t.Fatal("legitimate repeat suppressed")
	}
	finalHandoff(t, a, b)
	if c := finishOne(t, b); c.Target.IntakeID != repeat.ID {
		t.Fatal("repeat lost")
	}
}
func TestReleaseCompletedSyncCleanupKeepsUncertainMarker(t *testing.T) {
	s, _ := releaseFixture(t, "old", "active")
	defer s.Close()
	if err := s.WithInstancePermit(true, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	_ = s.WithInstancePermit(true, func() error { return errors.New("unknown") })
	if err := s.update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("meta")).ForEach(func(k, v []byte) error {
			if !strings.HasPrefix(string(k), "release-sync:") {
				return nil
			}
			var a SynchronousAttempt
			if err := json.Unmarshal(v, &a); err != nil {
				return err
			}
			a.UpdatedAt = time.Now().Add(-72 * time.Hour)
			return put(tx.Bucket([]byte("meta")), string(k), a)
		})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CleanupCompleted(48*time.Hour, 200, time.Now()); err != nil {
		t.Fatal(err)
	}
	rows, _ := s.SynchronousAttempts()
	if len(rows) != 1 || rows[0].State != Unknown {
		t.Fatal("uncertain record deleted or success retained", rows)
	}
}
func TestReleaseRetirementProofCannotReactivateAfterReverseHandoff(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	beginPair(t, a, b)
	preseed(t, a, b)
	controlRelease(t, a, "close-admission")
	oldManifest := exportRelease(t, a, true)
	importRelease(t, b, oldManifest)
	oldProof := controlRelease(t, a, "retire").RetirementProof
	activateRelease(t, b, oldProof)
	controlRelease(t, b, "close-admission")
	reverse := exportRelease(t, b, true)
	reverseProof := controlRelease(t, b, "retire").RetirementProof
	controlRelease(t, a, "rollback-standby")
	importRelease(t, a, reverse)
	activateRelease(t, a, reverseProof)
	controlRelease(t, b, "rollback-standby")
	importRelease(t, b, oldManifest)
	c := commandFor(t, b, "activate")
	c.Proof = oldProof
	if _, err := b.ControlRelease(c); err == nil {
		t.Fatal("consumed retirement proof reactivated second sender")
	}
}
func TestReleaseCannotRecycleUnactivatedRetiredStandbyWithUnusedOldProof(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	beginPair(t, a, b)
	preseed(t, a, b)
	controlRelease(t, a, "close-admission")
	importRelease(t, b, exportRelease(t, a, true))
	controlRelease(t, a, "retire")
	controlRelease(t, b, "close-admission")
	reverse := exportRelease(t, b, true)
	p := controlRelease(t, b, "retire").RetirementProof
	controlRelease(t, a, "rollback-standby")
	importRelease(t, a, reverse)
	activateRelease(t, a, p)
	if _, err := b.ControlRelease(commandFor(t, b, "rollback-standby")); err == nil {
		t.Fatal("never-active retired successor recycled with unused predecessor fence")
	}
	if _, err := a.ControlRelease(commandFor(t, a, "begin-active")); err == nil {
		t.Fatal("same operation restarted budget")
	}
}
func TestReleaseLegacyGrouplessAMManifestIsCarriedWithoutInventingGeneration(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	submitGroup(t, a, "test:v1:am-group:g", "A", time.Hour)
	finishOne(t, a)
	beginPair(t, a, b)
	m := exportRelease(t, a, false)
	for id, raw := range m.Buckets["intakes"] {
		var in Intake
		if err := json.Unmarshal(raw, &in); err != nil {
			t.Fatal(err)
		}
		in.AMGroup = false
		in.DedupeKey = ""
		in.Generation = 0
		in.IdentityOrigin = ""
		in.ReceptionSequence = 0
		in.ReceptionInstance = ""
		in.DeliveryOrder = 0
		raw, _ = json.Marshal(in)
		m.Buckets["intakes"][id] = raw
	}
	m.MAC = ""
	m.SnapshotChecksum = manifestChecksum(m)
	m.MAC = a.releaseMAC(m)
	importRelease(t, b, m)
	intakes, targets, err := b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(intakes) != 1 || len(targets) != 1 || intakes[0].IdentityOrigin != "legacy-unsequenced" || intakes[0].Generation != 0 || intakes[0].AMGroup || intakes[0].ReceptionSequence != 0 || intakes[0].DedupeKey != "" || targets[0].State != Delivered {
		t.Fatal("legacy facts invented or discarded", intakes, targets)
	}
}

func TestReleaseHistoricalAmbiguityDiagnosticDoesNotBlockLaterHandoff(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	submitGroup(t, a, "group", "A", time.Hour)
	finishOne(t, a)
	beginPair(t, a, b)
	preseed(t, a, b)
	submitGroup(t, b, "group", "B", time.Hour)
	returned, _ := submitGroup(t, b, "group", "A", time.Hour)
	// Keep a previous-version rejected-admission fact as historical evidence.
	if err := b.update(func(tx *bolt.Tx) error {
		return put(tx.Bucket([]byte("meta")), "release-ambiguity:release-test", uint64(1))
	}); err != nil {
		t.Fatal(err)
	}
	finalHandoff(t, a, b)
	finishOne(t, b)
	if c := finishOne(t, b); c.Target.IntakeID != returned.ID {
		t.Fatal("ambiguity fact blocked unrelated or new work")
	}
	st, _ := b.ReleaseStatus()
	if st.AmbiguousAdmissions != 1 {
		t.Fatal("old diagnostic was erased", st)
	}
}

func TestReleaseImportCannotOverwriteFormalStartupFence(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	if _, err := a.ControlProfile("formal", "prepare", "v1", 1, "test", "fixture"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ControlProfile("formal", "activate", "v1", 2, "test", "fixture"); err != nil {
		t.Fatal(err)
	}
	if err := b.DisableFormal(); err != nil {
		t.Fatal(err)
	}
	beginPair(t, a, b)
	if _, err := b.ImportRelease(exportRelease(t, a, false), commandFor(t, b, "import")); err == nil {
		t.Fatal("local formal fence overwritten")
	}
	if err := b.DisableFormal(); err != nil {
		t.Fatal("failed import changed formal state", err)
	}
}
func TestReleaseImportQuotaAndTransferEstimateAreEnforced(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	beginPair(t, a, b)
	preseed(t, a, b)
	submitGroup(t, b, "one", "A", time.Hour)
	submitGroup(t, b, "two", "B", time.Hour)
	estimate, err := b.EstimateReleaseTransferBytes()
	if err != nil {
		t.Fatal(err)
	}
	controlRelease(t, a, "close-admission")
	importRelease(t, b, exportRelease(t, a, true))
	controlRelease(t, a, "retire")
	controlRelease(t, b, "close-admission")
	m := exportRelease(t, b, true)
	raw, _ := json.Marshal(m)
	if int64(len(raw)) > estimate {
		t.Fatal("transfer estimate is not an upper bound", estimate, len(raw))
	}
	controlRelease(t, b, "retire")
	controlRelease(t, a, "rollback-standby")
	a.limits.MaxTargets = 1
	if _, err := a.ImportRelease(m, commandFor(t, a, "import")); !errors.Is(err, ErrCapacity) {
		t.Fatal("merged active quota was not enforced", err)
	}
	_, rows, _ := a.Snapshot()
	if len(rows) != 0 {
		t.Fatal("failed quota import partially committed")
	}
}
func TestReleaseCleanupPrunesOnlyOldCachesAndKeepsProofFences(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	beginPair(t, a, b)
	preseed(t, a, b)
	controlRelease(t, a, "close-admission")
	m := exportRelease(t, a, true)
	if err := a.update(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("meta"))
		m.ExportedAt = time.Now().Add(-72 * time.Hour)
		if err := put(meta, "release-export:"+m.OperationID+":"+m.SnapshotChecksum, m); err != nil {
			return err
		}
		st, _ := releaseRead(tx)
		st.OperationID = "current-operation"
		st.Phase = "active"
		if err := releasePut(tx, st); err != nil {
			return err
		}
		current := m
		current.OperationID = st.OperationID
		if err := put(meta, "release-export:"+current.OperationID+":current", current); err != nil {
			return err
		}
		return put(meta, "release-peer-proof:new", uint64(10))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.CleanupCompleted(48*time.Hour, 200, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ReadReleaseExport("release-test", m.SnapshotChecksum); err == nil {
		t.Fatal("old large export cache retained")
	}
	if _, err := a.ReadReleaseExport("current-operation", "current"); err != nil {
		t.Fatal("current recovery snapshot removed")
	}
	if err := a.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("meta")).Get([]byte("release-peer-proof:new")) == nil {
			return errors.New("proof consumption fence removed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReleaseCanceledHTTPWaitCannotApplyLateMutation(t *testing.T) {
	s, _ := releaseFixture(t, "old", "active")
	defer s.Close()
	st, _ := s.ReleaseStatus()
	c := ReleaseCommand{OperationID: "ctx-operation", ExpectedRevision: st.Revision, Action: "begin-active", PeerID: "new", Deadline: time.Now().Add(time.Second), MaxSendDelayMillis: 800, SyncRemainingMillis: 50, Actor: "test", Evidence: "isolated canceled request"}
	ctx, cancel := context.WithCancel(context.Background())
	s.releaseMu.Lock()
	done := make(chan error, 1)
	go func() { _, err := s.ControlReleaseContext(ctx, c); done <- err }()
	cancel()
	s.releaseMu.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("canceled control mutated late", err)
	}
	after, _ := s.ReleaseStatus()
	if after.Revision != st.Revision || after.OperationID != "" {
		t.Fatal("late state mutation", after)
	}
	if _, err := s.ExportReleaseContext(ctx, "ctx-operation", st.Revision, true); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled export", err)
	}
	if _, err := s.ImportReleaseContext(ctx, ReleaseManifest{}, c); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled import", err)
	}
	if err := s.ResolveSynchronousAttemptContext(ctx, "missing", "test", "fixture", false); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled resolution", err)
	}
}
func TestReleaseSeenOperationCannotResetBudgetAfterAnotherRelease(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	beginPair(t, a, b)
	if err := a.update(func(tx *bolt.Tx) error {
		st, _ := releaseRead(tx)
		st.OperationID = "another-completed-release"
		st.Phase = "active"
		return releasePut(tx, st)
	}); err != nil {
		t.Fatal(err)
	}
	c := commandFor(t, a, "begin-active")
	c.OperationID = "release-test"
	c.Deadline = time.Now().Add(time.Hour)
	if _, err := a.ControlRelease(c); !errors.Is(err, ErrReleaseConflict) {
		t.Fatal("old operation reused with new deadline", err)
	}
}
func TestReleaseExpiredReturningCanUseExactUnconsumedProof(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	deadline := beginTimedPair(t, a, b, 100*time.Millisecond)
	preseed(t, a, b)
	finalHandoff(t, a, b)
	controlRelease(t, b, "close-admission")
	reverse := exportRelease(t, b, true)
	proof := controlRelease(t, b, "retire").RetirementProof
	controlRelease(t, a, "rollback-standby")
	importRelease(t, a, reverse)
	time.Sleep(time.Until(deadline) + 5*time.Millisecond)
	if err := a.AdmissionAllowed(false); err != nil {
		t.Fatal(err)
	}
	activateRelease(t, a, proof)
	state, _ := a.ReleaseStatus()
	if !state.DeadlineExceeded || !state.Deadline.Equal(deadline) {
		t.Fatal("expired recovery reset deadline")
	}
	submitGroup(t, a, "after-return", "new", time.Hour)
	finishOne(t, a)
}
