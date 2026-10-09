package notification_spool

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	bolt "go.etcd.io/bbolt"
)

func activeState(state string) bool {
	return state == Pending || state == Retryable || state == Sending || state == Unknown
}
func activeCount(tx *bolt.Tx) uint64 {
	raw := tx.Bucket([]byte("meta")).Get([]byte("active_targets"))
	if len(raw) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(raw)
}
func setActiveCount(tx *bolt.Tx, value uint64) error {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], value)
	return tx.Bucket([]byte("meta")).Put([]byte("active_targets"), raw[:])
}
func changeActiveCount(tx *bolt.Tx, delta int) error {
	count := activeCount(tx)
	if delta < 0 {
		if count < uint64(-delta) {
			return errors.New("active notification counter underflow")
		}
		count -= uint64(-delta)
	} else {
		count += uint64(delta)
	}
	return setActiveCount(tx, count)
}

// CleanupCompleted bounds each transaction and removes confirmed completed
// payloads only after retention. Dedupe keys keep compact intake tombstones;
// unknown/pending/retryable/dead-letter work requiring reconciliation remains.
// Tombstones and audit records use the independently visible physical byte
// budget, so storage exhaustion fails readiness instead of silently replaying.
func (s *Store) CleanupCompleted(retention time.Duration, batch int, now time.Time) (int, error) {
	if retention < 24*time.Hour || batch < 1 || batch > 1000 {
		return 0, errors.New("completed retention must be at least one day and batch at most 1000")
	}
	cleaned := 0
	err := s.update(func(tx *bolt.Tx) error {
		meta, intakes, targets := tx.Bucket([]byte("meta")), tx.Bucket([]byte("intakes")), tx.Bucket([]byte("targets"))
		cursor := intakes.Cursor()
		last := meta.Get([]byte("cleanup_cursor"))
		key, raw := cursor.First()
		if len(last) > 0 {
			key, raw = cursor.Seek(last)
			if key != nil {
				key, raw = cursor.Next()
			}
		}
		if key == nil {
			key, raw = cursor.First()
		}
		type entry struct {
			id      string
			intake  Intake
			targets []Target
		}
		var changes []entry
		var lastSeen []byte
		for examined := 0; key != nil && examined < batch; examined++ {
			lastSeen = append(lastSeen[:0], key...)
			var intake Intake
			if err := json.Unmarshal(raw, &intake); err != nil {
				return err
			}
			if len(intake.TargetIDs) > 0 && intake.AcceptedAt.Add(retention).Before(now) {
				complete := true
				var rows []Target
				for _, id := range intake.TargetIDs {
					var target Target
					if err := json.Unmarshal(targets.Get([]byte(id)), &target); err != nil {
						return err
					}
					if target.UpdatedAt.Add(retention).After(now) || (target.State != Delivered && (target.State != DeadLetter || target.Outcome != "operator_reconciled")) {
						complete = false
						break
					}
					rows = append(rows, target)
				}
				if complete {
					changes = append(changes, entry{string(key), intake, rows})
				}
			}
			key, raw = cursor.Next()
		}
		for _, change := range changes {
			for _, target := range change.targets {
				if err := targets.Delete([]byte(target.ID)); err != nil {
					return err
				}
				if err := tx.Bucket([]byte("ready")).Delete(queueKey(target)); err != nil {
					return err
				}
				if err := tx.Bucket([]byte("blocked")).Delete([]byte(target.ID)); err != nil {
					return err
				}
			}
			change.intake.TargetIDs = nil
			change.intake.TerminalReason = "confirmed_completed_tombstone"
			if err := put(intakes, change.id, change.intake); err != nil {
				return err
			}
			cleaned++
		}
		if err := meta.Put([]byte("cleanup_cursor"), lastSeen); err != nil {
			return err
		}
		if cleaned > 0 {
			id, err := ID()
			if err != nil {
				return err
			}
			return put(tx.Bucket([]byte("audit")), id, map[string]interface{}{"action": "cleanup_confirmed_completed", "intakes": cleaned, "retention_seconds": retention.Seconds(), "at": now.UTC()})
		}
		return nil
	})
	return cleaned, err
}
