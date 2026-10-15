package notification_spool

import (
	"encoding/json"
	"errors"
	bolt "go.etcd.io/bbolt"
	"strings"
	"time"
)

// inheritedIntakes includes every imported snapshot, not just the preseed.
// A never-active successor may return only locally admitted buffers; it must
// never overwrite the predecessor with its stale copy of that predecessor.
func inheritedIntakes(tx *bolt.Tx, st ReleaseState) (map[string]bool, error) {
	ids := map[string]bool{}
	meta := tx.Bucket([]byte("meta"))
	for _, prefix := range []string{"release-base:", "release-inherited:"} {
		if raw := meta.Get([]byte(prefix + st.OperationID)); raw != nil {
			var rows map[string]bool
			if err := json.Unmarshal(raw, &rows); err != nil {
				return nil, err
			}
			for id := range rows {
				ids[id] = true
			}
		}
	}
	return ids, nil
}

func filterBufferedManifest(tx *bolt.Tx, m *ReleaseManifest, st ReleaseState) error {
	inherited, err := inheritedIntakes(tx, st)
	if err != nil {
		return err
	}
	for id := range inherited {
		delete(m.Buckets["intakes"], id)
	}
	for id, raw := range m.Buckets["targets"] {
		var target Target
		if err := json.Unmarshal(raw, &target); err != nil {
			return err
		}
		if inherited[target.IntakeID] {
			delete(m.Buckets["targets"], id)
			delete(m.Buckets["blocked"], id)
		}
	}
	for key, id := range m.Buckets["dedupe"] {
		if m.Buckets["intakes"][string(id)] == nil {
			delete(m.Buckets["dedupe"], key)
		}
	}
	// Profiles and old synchronous outcomes are the predecessor's authority.
	// Only this successor's immutable forwarding references are returned.
	for key, raw := range m.Buckets["meta"] {
		if strings.HasPrefix(key, "release-forward:") {
			var forward ForwardAttempt
			if err := json.Unmarshal(raw, &forward); err != nil {
				return err
			}
			if forward.SourceID == st.InstanceID {
				continue
			}
		}
		delete(m.Buckets["meta"], key)
	}
	return nil
}

// Preparing cancellation and importing the frozen successor delta can be one
// transaction. No new old-generation intake can overtake its known buffers.
func prepareCancellation(tx *bolt.Tx, st *ReleaseState, c ReleaseCommand, now time.Time) error {
	if tx.Bucket([]byte("meta")).Get([]byte("release-cancel-complete:"+c.OperationID)) != nil {
		return ErrReleaseConflict
	}
	if st.Phase == "cancelling" {
		return nil
	}
	if st.Phase == "active" && st.OperationID == c.OperationID && !st.ActivatedAt.IsZero() && !st.ActivatedAt.Before(st.StartedAt) {
		return ErrReleaseConflict
	}
	// This instance has not retired, so no successor can hold its valid
	// retirement proof for this operation. Keep its unique sending right.
	if st.Phase != "active" && st.Phase != "releasing" && st.Phase != "draining" && st.Phase != "frozen" && st.Phase != "cancelling" {
		return ErrReleaseConflict
	}
	if st.OperationID != c.OperationID {
		if st.Phase != "active" || !validReleaseID(c.PeerID) || c.PeerID == st.InstanceID || c.Deadline.IsZero() || c.MaxSendDelayMillis <= 0 || c.SyncRemainingMillis <= 0 || c.MaxSendDelayMillis > (24*time.Hour).Milliseconds() || c.SyncRemainingMillis > (24*time.Hour).Milliseconds() || c.Deadline.Sub(now) > 24*time.Hour || tx.Bucket([]byte("meta")).Get([]byte("release-operation:"+c.OperationID)) != nil {
			return ErrReleaseConflict
		}
		st.OperationID, st.PeerID, st.Deadline = c.OperationID, c.PeerID, c.Deadline.UTC()
		st.MaxSendDelayMillis, st.SyncRemainingMillis, st.StartedAt = c.MaxSendDelayMillis, c.SyncRemainingMillis, now
		if err := put(tx.Bucket([]byte("meta")), "release-operation:"+c.OperationID, map[string]interface{}{"peer_id": st.PeerID, "deadline": st.Deadline}); err != nil {
			return err
		}
	}
	st.Phase = "cancelling"
	st.ExportedChecksum, st.ImportedChecksum = "", ""
	st.RetirementProof = nil
	return nil
}

func validateCancelledBuffer(m ReleaseManifest) error {
	if !m.Final || !m.BufferedOnly {
		return errors.New("cancellation requires a final never-active buffer")
	}
	for _, raw := range m.Buckets["targets"] {
		var target Target
		if err := json.Unmarshal(raw, &target); err != nil {
			return err
		}
		if target.Attempts != 0 || target.AttemptID != "" || target.Owner != "" || (target.State != Pending && target.State != DeadLetter) {
			return errors.New("cancel buffer contains a prior attempt or uncertainty")
		}
	}
	return nil
}

func sameCancelledIntake(a, b Intake) bool {
	a.DeliveryOrder, b.DeliveryOrder = 0, 0
	a.AmbiguityReason, b.AmbiguityReason = "", ""
	a.PossibleDuplicateOf, b.PossibleDuplicateOf = nil, nil
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return string(left) == string(right)
}
