package notification_spool

import (
	"encoding/json"
	"errors"
	"time"

	bolt "go.etcd.io/bbolt"
)

// SendBudgetDeferred is a pre-send deferral, never an uncertain delivery.
type SendBudgetDeferred struct{ AvailableAt time.Time }

func (e *SendBudgetDeferred) Error() string { return "notification send budget not yet available" }

// WithSendPermit persists the reservation and send boundary before invoking
// the bounded platform call. Each profile has its own persisted clock;
// unprofiled legacy sends keep their existing throughput. Pause waits for any
// call already inside this boundary and prevents every later claimed send.
func (s *Store) WithSendPermit(c Claim, interval time.Duration, now time.Time, send func() error) error {
	mu := s.profileSendMutex(c.Target.ProfileID)
	mu.RLock()
	defer mu.RUnlock()
	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("targets"))
		var t Target
		if err := json.Unmarshal(b.Get([]byte(c.Target.ID)), &t); err != nil {
			return err
		}
		if t.State != Sending || t.Owner != s.owner || c.Owner != s.owner || t.AttemptID != c.AttemptID {
			return ErrFence
		}
		if t.ProfileID != "" && !t.SendPrepared {
			return errors.New("notification platform send already started")
		}
		paused, err := profileDeliveryPaused(tx, t)
		if err != nil {
			return err
		}
		if paused {
			return ErrProfilePaused
		}
		if t.ProfileID != "" && interval > 0 {
			meta := tx.Bucket([]byte("meta"))
			key := []byte("profile-send-next-at")
			// Retain the existing test clock across upgrades; formal never inherits it.
			if t.ProfileID == "formal" {
				key = []byte("formal-send-next-at")
			}
			if raw := meta.Get(key); raw != nil {
				var next time.Time
				if err := json.Unmarshal(raw, &next); err != nil {
					return err
				}
				if now.Before(next) {
					return &SendBudgetDeferred{AvailableAt: next}
				}
			}
			if err := put(meta, string(key), now.UTC().Add(interval)); err != nil {
				return err
			}
		}
		t.SendPrepared = false
		return put(b, t.ID, t)
	})
	if err != nil {
		return err
	}
	return send()
}
