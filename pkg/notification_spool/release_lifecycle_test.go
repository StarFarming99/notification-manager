package notification_spool

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func lifecycleTestTarget(snap ProfileSnapshot, alerts ...map[string]interface{}) Target {
	target := profileTarget(snap)
	target.Payload, _ = json.Marshal(map[string]interface{}{"data": map[string]interface{}{"alerts": alerts}})
	target.ContentRevision = Hash(target.Payload)
	return target
}
func lifecycleTestAlert(name, status string, start int) map[string]interface{} {
	alert := map[string]interface{}{"labels": map[string]string{"alertname": name, "namespace": "isolated", "severity": "critical"}, "startsAt": time.Date(2026, 10, 15, 0, 0, start, 0, time.UTC).Format(time.RFC3339Nano), "status": status}
	// The production HTTP handler hashes the whole alert, so a status or
	// annotation update has a different ID despite the same lifecycle.
	raw, _ := json.Marshal(alert)
	alert["id"] = Hash(raw)
	return alert
}

func submitLifecycle(t *testing.T, s *Store, group string, alerts ...map[string]interface{}) Intake {
	t.Helper()
	snap, err := s.ProfileSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	target := lifecycleTestTarget(snap, alerts...)
	in, _, err := s.SubmitAMProfile(group, Hash(target.Payload), []Target{target}, "", snap, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return in
}
func reconcileLifecycle(t *testing.T, s *Store) {
	t.Helper()
	if err := s.update(func(tx *bolt.Tx) error {
		if err := reconcileLifecycleOrder(tx); err != nil {
			return err
		}
		return rebuildReleaseIndexes(tx)
	}); err != nil {
		t.Fatal(err)
	}
}
func targetByIntake(t *testing.T, s *Store, id string) Target {
	t.Helper()
	_, rows, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.IntakeID == id {
			return row
		}
	}
	t.Fatal("target missing", id)
	return Target{}
}

func TestLifecycleQueuedFiringIsOrderedBeforeSameStartResolutionWithoutRewritingGeneration(t *testing.T) {
	s, _ := releaseFixture(t, "old", "active")
	defer s.Close()
	resolved := submitLifecycle(t, s, "group", lifecycleTestAlert("canary", "resolved", 0))
	firing := submitLifecycle(t, s, "group", lifecycleTestAlert("canary", "firing", 0))
	reconcileLifecycle(t, s)
	if c := finishOne(t, s); c.Target.IntakeID != firing.ID {
		t.Fatal("queued resolved preceded known firing", c.Target)
	}
	if c := finishOne(t, s); c.Target.IntakeID != resolved.ID {
		t.Fatal("resolution lost", c.Target)
	}
	intakes, _, _ := s.Snapshot()
	for _, in := range intakes {
		if in.ID == firing.ID && in.Generation != firing.Generation {
			t.Fatal("observed generation rewritten")
		}
		if in.ID == resolved.ID && in.Generation != resolved.Generation {
			t.Fatal("observed generation rewritten")
		}
	}
}

func TestLifecycleDeliveredResolutionSupersedesLateUnsentFiringWithoutFakeSuccess(t *testing.T) {
	s, _ := releaseFixture(t, "old", "active")
	defer s.Close()
	resolved := submitLifecycle(t, s, "group", lifecycleTestAlert("canary", "resolved", 0))
	proof := finishOne(t, s)
	firing := submitLifecycle(t, s, "group", lifecycleTestAlert("canary", "firing", 0))
	original := targetByIntake(t, s, firing.ID)
	reconcileLifecycle(t, s)
	row := targetByIntake(t, s, firing.ID)
	if row.State != DeadLetter || row.Outcome != terminalSupersededOutcome || row.Attempts != 0 || row.ID != original.ID || row.ContentRevision != original.ContentRevision || string(row.Payload) != string(original.Payload) || row.Evidence == "" {
		t.Fatal("late firing falsely sent or facts changed", row)
	}
	if _, err := s.Claim(time.Now()); !errors.Is(err, ErrEmpty) {
		t.Fatal("terminal firing remained active", err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket([]byte("audit")).Get([]byte("terminal-superseded:" + row.ID))
		var audit struct {
			PlatformSent bool     `json:"platform_sent"`
			IDs          []string `json:"resolved_receipt_target_ids"`
		}
		if err := json.Unmarshal(raw, &audit); err != nil {
			return err
		}
		if audit.PlatformSent || len(audit.IDs) != 1 || audit.IDs[0] != proof.Target.ID {
			t.Fatal("missing exact terminal receipt", string(raw))
		}
		if string(tx.Bucket([]byte("dedupe")).Get([]byte("group"))) != resolved.ID {
			t.Fatal("terminal firing displaced recovered current pointer")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLifecycleDifferentStartAndMixedUnrecoveredCardAreRetained(t *testing.T) {
	for _, kind := range []string{"new-start", "mixed"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := releaseFixture(t, "old", "active")
			defer s.Close()
			submitLifecycle(t, s, "group", lifecycleTestAlert("canary", "resolved", 0))
			finishOne(t, s)
			alerts := []map[string]interface{}{lifecycleTestAlert("canary", "firing", 1)}
			if kind == "mixed" {
				alerts = []map[string]interface{}{lifecycleTestAlert("canary", "firing", 0), lifecycleTestAlert("other", "firing", 0)}
			}
			in := submitLifecycle(t, s, "group", alerts...)
			reconcileLifecycle(t, s)
			if row := targetByIntake(t, s, in.ID); row.State != Pending || row.Outcome == terminalSupersededOutcome {
				t.Fatal("unproved target swallowed", row)
			}
			if c := finishOne(t, s); c.Target.IntakeID != in.ID {
				t.Fatal("retained target not sendable")
			}
		})
	}
}

func TestLifecycleUnknownAttemptIsNeverRewrittenByResolution(t *testing.T) {
	s, _ := releaseFixture(t, "old", "active")
	defer s.Close()
	first := submitLifecycle(t, s, "group", lifecycleTestAlert("canary", "firing", 0))
	c, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(c, Unknown, "lost", "original-proof", time.Now()); err != nil {
		t.Fatal(err)
	}
	submitLifecycle(t, s, "group", lifecycleTestAlert("canary", "resolved", 0))
	finishOne(t, s)
	reconcileLifecycle(t, s)
	row := targetByIntake(t, s, first.ID)
	if row.State != Unknown || row.AttemptID != c.AttemptID || row.Attempts != 1 || row.Evidence != "original-proof" {
		t.Fatal("historical unknown rewritten", row)
	}
}

func TestLifecycleMixedCardsWithCycleStayExplicitlyPartialAndSendable(t *testing.T) {
	s, _ := releaseFixture(t, "old", "active")
	defer s.Close()
	first := submitLifecycle(t, s, "group", lifecycleTestAlert("one", "firing", 0), lifecycleTestAlert("two", "resolved", 0))
	second := submitLifecycle(t, s, "group", lifecycleTestAlert("one", "resolved", 0), lifecycleTestAlert("two", "firing", 0))
	reconcileLifecycle(t, s)
	if err := s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("audit")).Get([]byte("lifecycle-partial:"+Hash([]byte("group")))) == nil {
			t.Fatal("mixed cycle missing explicit partial evidence")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, wanted := range []string{first.ID, second.ID} {
		if c := finishOne(t, s); c.Target.IntakeID != wanted {
			t.Fatal("partial mixed card blocked or discarded")
		}
	}
}

func TestLifecycleClaimProbeIsScopedAndStaleIndexDoesNotSuppress(t *testing.T) {
	s, _ := releaseFixture(t, "old", "active")
	defer s.Close()
	resolved := submitLifecycle(t, s, "group", lifecycleTestAlert("canary", "resolved", 0))
	c := finishOne(t, s)
	in := submitLifecycle(t, s, "group", lifecycleTestAlert("canary", "firing", 0))
	if err := s.update(func(tx *bolt.Tx) error {
		if err := recordDeliveredLifecycle(tx, c.Target); err != nil {
			return err
		}
		// Normal completed payload retention may leave an intake tombstone.
		var prior Intake
		if err := json.Unmarshal(tx.Bucket([]byte("intakes")).Get([]byte(resolved.ID)), &prior); err != nil {
			return err
		}
		prior.TargetIDs = nil
		prior.TerminalReason = "confirmed_completed_tombstone"
		if err := put(tx.Bucket([]byte("intakes")), prior.ID, prior); err != nil {
			return err
		}
		return tx.Bucket([]byte("targets")).Delete([]byte(c.Target.ID))
	}); err != nil {
		t.Fatal(err)
	}
	if c := finishOne(t, s); c.Target.IntakeID != in.ID {
		t.Fatal("stale resolution index swallowed notification")
	}
}

func TestLifecycleCancelBufferAfterDeliveredResolutionIsTerminalSuperseded(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	beginPair(t, a, b)
	preseed(t, a, b)
	firing := submitLifecycle(t, b, "group", lifecycleTestAlert("canary", "firing", 0))
	controlRelease(t, a, "cancel")
	submitLifecycle(t, a, "group", lifecycleTestAlert("canary", "resolved", 0))
	finishOne(t, a)
	controlRelease(t, b, "close-admission")
	importRelease(t, a, exportRelease(t, b, true))
	row := targetByIntake(t, a, firing.ID)
	if row.State != DeadLetter || row.Outcome != terminalSupersededOutcome {
		t.Fatal("cancel imported an active stale firing", row)
	}
	if _, err := a.Claim(time.Now()); !errors.Is(err, ErrEmpty) {
		t.Fatal("cancel sent firing after resolution", err)
	}
}

func TestLifecycleAtomicCancelImportOrdersBufferedFiringBeforeQueuedResolution(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	beginPair(t, a, b)
	preseed(t, a, b)
	firing := submitLifecycle(t, b, "group", lifecycleTestAlert("canary", "firing", 0))
	resolved := submitLifecycle(t, a, "group", lifecycleTestAlert("canary", "resolved", 0))
	controlRelease(t, b, "close-admission")
	manifest := exportRelease(t, b, true)
	command := commandFor(t, a, "cancel-import")
	if _, err := a.ImportRelease(manifest, command); err != nil {
		t.Fatal(err)
	}
	for _, wanted := range []string{firing.ID, resolved.ID} {
		if c := finishOne(t, a); c.Target.IntakeID != wanted {
			t.Fatal("atomic cancel lifecycle order incorrect", c.Target.IntakeID, wanted)
		}
	}
}

func TestLifecycleTerminalOriginalAlsoSettlesOnlyItsUnstartedDependentHistory(t *testing.T) {
	s, _ := releaseFixture(t, "old", "active")
	defer s.Close()
	submitLifecycle(t, s, "group", lifecycleTestAlert("canary", "resolved", 0))
	finishOne(t, s)
	snap, _ := s.ProfileSnapshot("test")
	original := lifecycleTestTarget(snap, lifecycleTestAlert("canary", "firing", 0))
	history := profileTarget(snap)
	history.Payload = json.RawMessage(`{"data":{"alerts":[]},"content":"history"}`)
	history.ContentRevision = Hash(history.Payload)
	history.DependencyRevisions = []string{original.ContentRevision}
	in, _, err := s.SubmitAMProfile("group", Hash([]byte("firing-with-dependent-history")), []Target{original, history}, "", snap, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(time.Now()); !errors.Is(err, ErrEmpty) {
		t.Fatal("terminal original/history attempted platform send", err)
	}
	_, rows, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.IntakeID == in.ID {
			count++
			if row.State != DeadLetter || row.Outcome != terminalSupersededOutcome || row.Attempts != 0 {
				t.Fatal("dependent history remained pending or was fake-delivered", row)
			}
		}
	}
	if count != 2 {
		t.Fatal("original/history facts dropped", count)
	}
	status, err := s.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status["active_targets"].(uint64) != 0 {
		t.Fatal("terminal history permanently occupied capacity", status)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("blocked")).Stats().KeyN != 0 {
			t.Fatal("terminal history still blocked")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLifecycleRuntimeTerminalKeepsCertainResolvedRetryDeduplicated(t *testing.T) {
	s, _ := releaseFixture(t, "old", "active")
	defer s.Close()
	resolved := submitLifecycle(t, s, "group", lifecycleTestAlert("canary", "resolved", 0))
	finishOne(t, s)
	submitLifecycle(t, s, "group", lifecycleTestAlert("canary", "firing", 0))
	if _, err := s.Claim(time.Now()); !errors.Is(err, ErrEmpty) {
		t.Fatal("late firing attempted", err)
	}
	snap, _ := s.ProfileSnapshot("test")
	target := lifecycleTestTarget(snap, lifecycleTestAlert("canary", "resolved", 0))
	in, dup, err := s.SubmitAMProfile("group", Hash(target.Payload), []Target{target}, "", snap, time.Hour)
	if err != nil || !dup || in.ID != resolved.ID {
		t.Fatal("certain recovered retry became a new notification", dup, err, in)
	}
}
