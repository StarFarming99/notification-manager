package notification_spool

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

func laneKey(t Target) []byte { return append([]byte(t.ProfileID+":"), queueKey(t)...) }
func putReady(tx *bolt.Tx, t Target) error {
	if err := tx.Bucket([]byte("ready")).Put(queueKey(t), []byte(t.ID)); err != nil {
		return err
	}
	return tx.Bucket([]byte("ready-lanes")).Put(laneKey(t), []byte(t.ID))
}
func deleteReady(tx *bolt.Tx, t Target) error {
	if err := tx.Bucket([]byte("ready")).Delete(queueKey(t)); err != nil {
		return err
	}
	return tx.Bucket([]byte("ready-lanes")).Delete(laneKey(t))
}
func rebuildLaneIndex(tx *bolt.Tx) error {
	if err := tx.DeleteBucket([]byte("ready-lanes")); err != nil {
		return err
	}
	b, err := tx.CreateBucket([]byte("ready-lanes"))
	if err != nil {
		return err
	}
	return tx.Bucket([]byte("ready")).ForEach(func(_, id []byte) error {
		var t Target
		if err := json.Unmarshal(tx.Bucket([]byte("targets")).Get(id), &t); err != nil {
			return err
		}
		return b.Put(laneKey(t), id)
	})
}
func profileActiveCount(tx *bolt.Tx, id string) (int, error) {
	raw := tx.Bucket([]byte("meta")).Get([]byte("active-targets-profile:" + id))
	if raw == nil {
		return 0, nil
	}
	var count int
	err := json.Unmarshal(raw, &count)
	return count, err
}
func changeProfileActiveCount(tx *bolt.Tx, id string, delta int) error {
	count, err := profileActiveCount(tx, id)
	if err != nil {
		return err
	}
	count += delta
	if count < 0 {
		return errors.New("invalid profile active counter")
	}
	return put(tx.Bucket([]byte("meta")), "active-targets-profile:"+id, count)
}
func (s *Store) profileSendMutex(id string) *sync.RWMutex {
	switch id {
	case "test":
		return &s.testSendMu
	case "formal":
		return &s.formalSendMu
	default:
		return &s.sendMu
	}
}

// ConfigureTestCapacity bounds test admission independently from the original
// production quota. This setting never changes legacy memory/durable stores.
func (s *Store) ConfigureTestCapacity(limit int) error {
	if limit <= 0 || limit > s.limits.MaxTargets {
		return errors.New("test target limit exceeds its original queue quota")
	}
	s.testActiveLimit = limit
	return nil
}
func (s *Store) DefaultTestCapacity() int { return s.limits.MaxTargets }

// ReadyProfile applies independent lane record quotas. Existing test unknown or
// retry rows cannot consume the original production quota; disk health remains
// a common fail-closed requirement and is separately exposed in /status.
func (s *Store) ReadyProfile(id string) error {
	s.healthMu.Lock()
	failed := s.lastStorageError != ""
	s.healthMu.Unlock()
	if failed {
		return errors.New("notification storage write unavailable")
	}
	if err := s.capacity(); err != nil {
		return err
	}
	return s.db.View(func(tx *bolt.Tx) error {
		test, err := profileActiveCount(tx, "test")
		if err != nil {
			return err
		}
		if id == "test" {
			limit := s.testActiveLimit
			if limit == 0 {
				limit = s.limits.MaxTargets
			}
			if test >= limit {
				return ErrCapacity
			}
			return nil
		}
		total := activeCount(tx)
		if total < uint64(test) {
			return errors.New("invalid profile quota counter")
		}
		if total-uint64(test) >= uint64(s.limits.MaxTargets) {
			return ErrCapacity
		}
		return nil
	})
}

// DisableFormal is a startup fence for restarting test after manual rollback.
// It cannot turn an active ledger into a paused ledger: the stopped-writer
// offline control must first persist that pause and its audit evidence.
func (s *Store) DisableFormal() error {
	err := s.db.View(func(tx *bolt.Tx) error {
		st, err := readProfileState(tx, "formal")
		if err != nil {
			return err
		}
		if !st.Paused {
			return errors.New("formal startup fence requires a persisted pause")
		}
		return nil
	})
	if err == nil {
		s.formalDisabled = true
	}
	return err
}
func (s *Store) FormalDisabled() bool { return s.formalDisabled }

// WithProfilePermit preserves synchronous original API results and closes the
// pause race. It never enters the test clock and does not claim a queued send.
func (s *Store) WithProfilePermit(snap ProfileSnapshot, send func() error) error {
	s.releaseMu.RLock()
	defer s.releaseMu.RUnlock()
	if err := s.db.View(func(tx *bolt.Tx) error {
		if err := releaseAdmission(tx, true, time.Now().UTC()); err != nil {
			return err
		}
		return releaseSending(tx, time.Now().UTC())
	}); err != nil {
		return err
	}
	mu := s.profileSendMutex(snap.ID)
	mu.RLock()
	defer mu.RUnlock()
	if snap.ID == "formal" && s.formalDisabled {
		return ErrProfilePaused
	}
	if err := s.db.View(func(tx *bolt.Tx) error { return checkProfileAdmission(tx, &snap) }); err != nil {
		return err
	}
	return s.synchronousAttempt(snap.ID, send)
}
