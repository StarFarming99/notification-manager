package notification_spool

import (
	"encoding/json"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestSynchronousImportedSendingIsObservationAndFinalCompletionSupersedesIt(t *testing.T) {
	s, _ := releaseFixture(t, "new", "standby")
	defer s.Close()
	original := SynchronousAttempt{ID: "actual-request", InstanceID: "old", ProfileID: "formal", State: Sending, Outcome: "platform_call_entered", UpdatedAt: time.Now().UTC()}
	key := "release-sync:" + original.ID
	raw, _ := json.Marshal(original)
	if err := s.update(func(tx *bolt.Tx) error {
		merged, err := mergeSynchronousReference(tx, key, raw, "new")
		if err != nil {
			return err
		}
		return tx.Bucket([]byte("meta")).Put([]byte(key), merged)
	}); err != nil {
		t.Fatal(err)
	}
	rows, _ := s.SynchronousAttempts()
	if len(rows) != 1 || rows[0].State != Unknown || rows[0].SnapshotState != Sending || rows[0].ImportedFrom != "old" || rows[0].InstanceID != "old" || rows[0].SnapshotUpdatedAt == nil || !rows[0].SnapshotUpdatedAt.Equal(original.UpdatedAt) {
		t.Fatal(rows)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		n, err := syncSending(tx)
		if n != 0 {
			t.Fatal("foreign preseed counted as local physical request", n)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	completed := original
	completed.State = Delivered
	completed.Outcome = "platform_confirmed"
	completed.UpdatedAt = original.UpdatedAt.Add(time.Second)
	raw, _ = json.Marshal(completed)
	if err := s.update(func(tx *bolt.Tx) error {
		merged, err := mergeSynchronousReference(tx, key, raw, "new")
		if err != nil {
			return err
		}
		return tx.Bucket([]byte("meta")).Put([]byte(key), merged)
	}); err != nil {
		t.Fatal(err)
	}
	rows, _ = s.SynchronousAttempts()
	if rows[0].State != Delivered || rows[0].Outcome != "platform_confirmed" || rows[0].SnapshotState != "" {
		t.Fatal("final real completion lost", rows)
	}
}

func TestSynchronousMergeKeepsLocalOwnerAndDoesNotInventLegacyOwnership(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		s, _ := releaseFixture(t, "local", "active")
		original := SynchronousAttempt{ID: "original", InstanceID: "local", State: Sending, Outcome: "platform_call_entered", UpdatedAt: time.Now().UTC()}
		if legacy {
			original.InstanceID = ""
		}
		key := "release-sync:" + original.ID
		if !legacy {
			if err := s.update(func(tx *bolt.Tx) error { return put(tx.Bucket([]byte("meta")), key, original) }); err != nil {
				t.Fatal(err)
			}
		}
		incoming := original
		if !legacy {
			incoming.State = Delivered
			incoming.UpdatedAt = original.UpdatedAt.Add(time.Hour)
		}
		raw, _ := json.Marshal(incoming)
		var merged []byte
		if err := s.update(func(tx *bolt.Tx) error {
			var err error
			merged, err = mergeSynchronousReference(tx, key, raw, "local")
			return err
		}); err != nil {
			t.Fatal(err)
		}
		var got SynchronousAttempt
		if err := json.Unmarshal(merged, &got); err != nil {
			t.Fatal(err)
		}
		if !legacy && (got.State != Sending || got.Outcome != original.Outcome || got.ImportedAt != nil) {
			t.Fatal("import overwrote local physical request", got)
		}
		if legacy && (got.State != Unknown || got.InstanceID != "" || got.ImportedFrom != "" || got.SnapshotState != Sending) {
			t.Fatal("legacy ownership invented", got)
		}
		_ = s.Close()
	}
}
